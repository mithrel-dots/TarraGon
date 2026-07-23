package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/mithrel-dots/tarragon/internal/wire"
)

// pluginRegistry tracks connected plugins by name.
type pluginRegistry struct {
	mu       sync.RWMutex
	conns    map[string]net.Conn
	scanners map[string]*bufio.Scanner
}

func (r *pluginRegistry) set(name string, conn net.Conn, scanner *bufio.Scanner) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conns == nil {
		r.conns = make(map[string]net.Conn)
	}
	if r.scanners == nil {
		r.scanners = make(map[string]*bufio.Scanner)
	}
	if old, ok := r.conns[name]; ok {
		_ = old.Close()
	}
	r.conns[name] = conn
	r.scanners[name] = scanner
}

func (r *pluginRegistry) remove(name string, conn net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conns[name] != conn {
		return // the restarted plugin already registered a new connection
	}
	_ = conn.Close()
	delete(r.conns, name)
	delete(r.scanners, name)
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

func (r *pluginRegistry) getConn(name string) (net.Conn, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.conns[name]
	return c, ok
}

// Requests routed to plugins.
type pluginRequest struct {
	name     string
	queryID  string
	text     string
	resultID string
	action   string
	msgType  string
}

func startPluginListener(ctx context.Context, store *aggregateStore, ui *uiRegistry) (chan<- pluginRequest, *pluginRegistry) {
	socketPath := wire.ResolvePluginsSocketPath()
	ln, err := wire.ListenUnix(socketPath)
	if err != nil {
		log.Fatalf("[PLUGINS] Failed to listen on %s: %v", socketPath, err)
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close()
		_ = wire.CleanupSocket(socketPath)
	}()

	log.Printf("[PLUGINS] listening on %s", socketPath)
	registry := &pluginRegistry{conns: make(map[string]net.Conn), scanners: make(map[string]*bufio.Scanner)}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[PLUGINS] accept error: %v", err)
				continue
			}
			go handlePluginConn(ctx, conn, registry, store, ui)
		}
	}()

	reqChan := make(chan pluginRequest, 256)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-reqChan:
				conn, ok := registry.getConn(req.name)
				if !ok {
					log.Printf("[PLUGINS] drop request: plugin %s not connected", req.name)
					continue
				}
				msgType := req.msgType
				if msgType == "" {
					msgType = wire.MsgRequest
				}
				if msgType == wire.MsgRequest {
					q := store.query(req.queryID)
					if q == nil || q.ctx.Err() != nil {
						continue // evicted or superseded while queued
					}
					q.mu.Lock()
					if q.started == nil {
						q.started = make(map[string]time.Time)
					}
					q.started[req.name] = time.Now()
					q.mu.Unlock()
				}
				_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
				var err error
				if msgType == wire.MsgSelect {
					err = wire.WriteMsg(conn, &wire.SelectRequest{
						Type:     wire.MsgSelect,
						QueryID:  req.queryID,
						ResultID: req.resultID,
						Action:   req.action,
					})
				} else {
					err = wire.WriteMsg(conn, &wire.PluginRequest{Type: msgType, QueryID: req.queryID, Text: req.text})
				}
				_ = conn.SetWriteDeadline(time.Time{})
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

func handlePluginConn(ctx context.Context, conn net.Conn, registry *pluginRegistry, store *aggregateStore, ui *uiRegistry) {
	scanner := wire.NewScanner(conn)
	var hello wire.PluginHello
	if err := wire.ReadMsg(scanner, &hello); err != nil {
		log.Printf("[PLUGINS] failed hello read: %v", err)
		_ = conn.Close()
		return
	}
	if hello.Type != wire.MsgHello || hello.Name == "" {
		log.Printf("[PLUGINS] invalid hello: type=%q name=%q", hello.Type, hello.Name)
		_ = conn.Close()
		return
	}

	registry.set(hello.Name, conn, scanner)
	log.Printf("[PLUGINS] plugin connected: %s", hello.Name)

	defer func() {
		registry.remove(hello.Name, conn)
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
		case wire.MsgResponse:
			q := store.query(resp.QueryID)
			if q == nil {
				continue
			}
			q.mu.Lock()
			start := q.started[hello.Name]
			delete(q.started, hello.Name)
			q.mu.Unlock()
			var elapsed float64
			if !start.IsZero() {
				elapsed = float64(time.Since(start).Microseconds()) / 1000
			}
			store.SubmitResponse(hello.Name, resp, elapsed, ui.publishSnapshot)

		case wire.MsgSelectResponse:
			var resp wire.SelectResponse
			if json.Unmarshal(raw, &resp) != nil {
				continue
			}
			if resp.Type == "" {
				resp.Type = wire.MsgSelectResponse
			}
			ui.publish(&resp)
		}
	}
}
