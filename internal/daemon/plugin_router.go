package daemon

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

// pluginRegistry tracks connected plugins by name.
type pluginRegistry struct {
	bus        *daemonBus
	manager    *plugins.Manager
	requests   chan<- pluginRequest
	resume     chan *pluginSession
	mu         sync.RWMutex
	conns      map[string]*pluginSession
	registered chan struct{} // closed at the next hello, if callers are waiting
}

func (r *pluginRegistry) set(name string, conn peerConn) *pluginSession {
	s := &pluginSession{name: name, conn: conn, inFlight: make(map[string]*selectAction)}
	if r.manager != nil {
		r.manager.RLock()
		if p := r.manager.Plugins[name]; p != nil {
			s.tagged = slices.Contains(p.Config.Capabilities, "action_id")
		}
		r.manager.RUnlock()
	}
	r.mu.Lock()
	if r.conns == nil {
		r.conns = make(map[string]*pluginSession)
	}
	old := r.conns[name]
	var actions []*selectAction
	if old != nil {
		actions = old.retire()
	}
	r.conns[name] = s
	if r.registered != nil {
		close(r.registered)
		r.registered = nil
	}
	r.mu.Unlock()
	if old != nil {
		old.close(actions)
	}
	return s
}

func (r *pluginRegistry) remove(name string, conn peerConn) {
	r.mu.Lock()
	s := r.conns[name]
	if s == nil || s.conn != conn {
		r.mu.Unlock()
		return // the restarted plugin already registered a new connection
	}
	actions := s.retire()
	delete(r.conns, name)
	r.mu.Unlock()
	s.close(actions)
}

func (r *pluginRegistry) isConnected(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.conns[name]
	return ok
}

// Names returns a sorted list of currently connected plugin names.
func (r *pluginRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.conns))
	for name := range r.conns {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *pluginRegistry) getConn(name string) (peerConn, bool) {
	s := r.session(name)
	if s == nil {
		return nil, false
	}
	return s.conn, true
}

func (r *pluginRegistry) session(name string) *pluginSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.conns[name]
}

// Wait for an on-demand child's hello, bounded by the query or action
// lifetime. Subscribe under the registry lock so a fast hello cannot be lost.
func (r *pluginRegistry) waitConn(ctx context.Context, name string, done <-chan struct{}) (peerConn, bool) {
	for {
		r.mu.Lock()
		if session := r.conns[name]; session != nil {
			r.mu.Unlock()
			return session.conn, true
		}
		if r.registered == nil {
			r.registered = make(chan struct{})
		}
		registered := r.registered
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false
		case <-done:
			return nil, false
		case <-registered:
		}
	}
}

// Requests routed to plugins.
type pluginRequest struct {
	name          string
	pluginContext string
	queryID       string
	text          string
	conn          peerConn
	end           bool
	selectAction  *selectAction
}

func startPluginRouter(ctx context.Context, store *aggregateStore) (chan<- pluginRequest, *pluginRegistry) {
	reqChan := make(chan pluginRequest, 256)
	registry := &pluginRegistry{requests: reqChan, resume: make(chan *pluginSession)}
	writeReady := func(s *pluginSession) {
		if err := s.writeReady(); err != nil {
			log.Printf("[PLUGINS] send select to %s failed: %v", s.name, err)
			registry.remove(s.name, s.conn)
		}
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case s := <-registry.resume:
				writeReady(s)
			case req := <-reqChan:
				s := registry.session(req.name)
				if s == nil || s.conn != req.conn {
					if req.selectAction != nil {
						req.selectAction.fail("Plugin connection no longer available.")
					}
					log.Printf("[PLUGINS] drop request: plugin %s not connected", req.name)
					continue
				}
				if req.selectAction != nil {
					s.enqueue(req.selectAction)
					writeReady(s)
					continue
				}
				conn := s.conn
				var frame any
				if req.end {
					frame = map[string]string{"type": wire.MsgEnd, "query_id": req.queryID}
				} else {
					frame = &wire.PluginRequest{Type: wire.MsgRequest, QueryID: req.queryID, Text: req.text, Context: req.pluginContext}
				}
				data, err := wire.EncodeMsg(frame)
				if err != nil {
					if !req.end {
						store.beginQueryWrite(req, err)
					}
					continue
				}
				_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
				if !req.end && !store.beginQueryWrite(req, nil) {
					_ = conn.SetWriteDeadline(time.Time{})
					continue // rejected, ended or retired while queued
				}
				n, err := conn.Write(data)
				_ = conn.SetWriteDeadline(time.Time{})
				if err == nil && n != len(data) {
					err = io.ErrShortWrite
				}
				if err != nil {
					log.Printf("[PLUGINS] send to %s failed: %v", req.name, err)
					registry.remove(req.name, conn)
					continue
				}
			}
		}
	}()

	return reqChan, registry
}

func handlePluginConn(ctx context.Context, conn peerConn, registry *pluginRegistry, store *aggregateStore, ui *uiRegistry) {
	scanner := wire.NewScanner(conn)
	var hello wire.PluginHello
	if err := wire.ReadMsg(scanner, &hello); err != nil {
		log.Printf("[PLUGINS] failed hello read: %v", err)
		_ = conn.Close()
		return
	}
	if hello.Type != wire.MsgHello || hello.Name == "" || hello.Name == plugins.OverviewID {
		log.Printf("[PLUGINS] invalid hello: type=%q name=%q", hello.Type, hello.Name)
		_ = conn.Close()
		return
	}

	session := registry.set(hello.Name, conn)
	log.Printf("[PLUGINS] plugin connected: %s", hello.Name)

	defer func() {
		registry.remove(hello.Name, conn)
		store.retireQueries(hello.Name, conn)
		log.Printf("[PLUGINS] plugin disconnected: %s", hello.Name)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !scanner.Scan() {
			return
		}
		raw := scanner.Bytes()
		var resp wire.PluginResponse
		if err := json.Unmarshal(raw, &resp); err != nil {
			continue
		}

		switch resp.Type {
		case wire.MsgResponse, wire.MsgLiveUpdate:
			if store.receiveQuery(hello.Name, conn, resp) && session.lateLiveResponse(resp) {
				queryEnd{
					requests: registry.requests, shutdown: ctx.Done(),
					req: pluginRequest{name: hello.Name, conn: conn, queryID: resp.QueryID, end: true},
				}.send()
			}

		case wire.MsgSelectResponse:
			type terminal wire.SelectResponse
			var reply struct {
				terminal
				Next json.RawMessage `json:"next"`
			}
			if json.Unmarshal(raw, &reply) != nil {
				continue
			}
			resp := wire.SelectResponse(reply.terminal)
			resp.Next = decodeActionNext(reply.Next)
			if session.final(resp) {
				select {
				case registry.resume <- session:
				case <-ctx.Done():
					return
				}
			}
		case wire.MsgActionProgress:
			var progress wire.ActionProgress
			if json.Unmarshal(raw, &progress) == nil {
				session.progress(progress)
			}
		}
	}
}
