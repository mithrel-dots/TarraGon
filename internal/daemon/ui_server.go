package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mithrel-dots/tarragon/internal/config"
	"github.com/mithrel-dots/tarragon/internal/db"
	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

var querySeq atomic.Uint64

// uiClient tracks a connected UI.
type uiClient struct {
	conn     peerConn
	uid      string
	clientID string
	out      chan any
	done     chan struct{}
	stopOnce sync.Once
	actions  sync.WaitGroup // terminal replies not yet queued
	queueMu  sync.Mutex
	pending  map[string]*queuedSnapshot
}

type writeBarrier chan struct{}
type queuedSnapshot struct{ latest *wire.UpdateMessage }

func (c *uiClient) stop() {
	c.stopOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

func (c *uiClient) send(msg any) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	select {
	case <-c.done:
		return net.ErrClosed
	default:
	}
	if update, ok := msg.(*wire.UpdateMessage); ok {
		if slot := c.pending[update.QueryID]; slot != nil {
			slot.latest = update
			return nil
		}
		slot := &queuedSnapshot{latest: update}
		c.pending[update.QueryID] = slot
		msg = slot
	} else {
		// A control reply or flush barrier must not be overtaken by a later update.
		clear(c.pending)
	}
	select {
	case c.out <- msg:
		return nil
	case <-c.done:
		return net.ErrClosed
	default:
		// A stalled frontend must not hold up every plugin or another UI.
		c.stop()
		return net.ErrClosed
	}
}

func (c *uiClient) takeSnapshot(slot *queuedSnapshot) *wire.UpdateMessage {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	update := slot.latest
	if c.pending[update.QueryID] == slot {
		delete(c.pending, update.QueryID)
	}
	return update
}

// uiRegistry tracks active UI clients.
type uiRegistry struct {
	mu      sync.RWMutex
	clients map[string]*uiClient
	store   *aggregateStore
	// Serialize config changes through the resulting process stops/starts,
	// without holding the query path's manager lock while waiting for exit.
	management    sync.Mutex
	actionTimeout time.Duration
	actionSlots   chan struct{}
}

func newUIRegistry(store *aggregateStore) *uiRegistry {
	return &uiRegistry{
		clients:       make(map[string]*uiClient),
		store:         store,
		actionTimeout: 3 * time.Second,
		actionSlots:   make(chan struct{}, 256),
	}
}

func (r *uiRegistry) add(c *uiClient) {
	c.uid = fmt.Sprintf("ui-%d", querySeq.Add(1))
	c.out = make(chan any, 128)
	c.pending = make(map[string]*queuedSnapshot)
	c.done = make(chan struct{})
	r.mu.Lock()
	r.clients[c.uid] = c
	r.mu.Unlock()
	go func() {
		defer r.remove(c)
		for {
			select {
			case <-c.done:
				return
			case msg := <-c.out:
				if slot, ok := msg.(*queuedSnapshot); ok {
					msg = c.takeSnapshot(slot)
				}
				if barrier, ok := msg.(writeBarrier); ok {
					close(barrier)
					continue
				}
				_ = c.conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
				if err := writeUIFrame(c.conn, msg); err != nil {
					return
				}
			}
		}
	}()
}

const oversizedActionMessage = "Action message exceeds the 8 MiB payload limit."

func writeUIFrame(w io.Writer, msg any) error {
	err := wire.WriteMsg(w, msg)
	if !errors.Is(err, wire.ErrMessageTooLarge) {
		return err
	}
	// The limit is checked before any bytes are written. A valid handler
	// reply can exceed it after adding the owner's correlation fields.
	switch m := msg.(type) {
	case *wire.SelectResponse:
		short := *m
		short.Message = oversizedActionMessage
		if short.Code == "completion_unknown" {
			short.Message += " The effect may already have occurred."
		}
		if err := wire.WriteMsg(w, &short); !errors.Is(err, wire.ErrMessageTooLarge) {
			return err
		}
		short.RequestID = ""
		return wire.WriteMsg(w, &short)
	case *wire.SelectAck:
		return wire.WriteMsg(w, &wire.SelectAck{Type: m.Type, ActionID: m.ActionID})
	case *wire.ActionProgress:
		return nil // optional notification; the final can still be delivered
	case *wire.UpdateMessage:
		return wire.WriteMsg(w, map[string]string{"type": "error", "error": err.Error(), "query_id": m.QueryID})
	default:
		return wire.WriteMsg(w, map[string]string{"type": "error", "error": err.Error()})
	}
}

func (r *uiRegistry) remove(c *uiClient) {
	r.mu.Lock()
	delete(r.clients, c.uid)
	r.mu.Unlock()
	c.stop()
}

func handleUIClient(ctx context.Context, conn peerConn, mgr *plugins.Manager, reqOut chan<- pluginRequest, pluginsReg *pluginRegistry, store *aggregateStore, ui *uiRegistry, frecencyDB *db.DB) {
	var history sync.WaitGroup
	defer history.Wait()
	scanner := wire.NewScanner(conn)
	client := &uiClient{conn: conn}
	ui.add(client)
	uid := client.uid
	session, endSession := context.WithCancel(ctx)
	defer endSession()
	stopOnShutdown := context.AfterFunc(ctx, client.stop)
	defer stopOnShutdown()
	defer ui.remove(client)
	defer store.removeByOwner(uid)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var parsed wire.UIRequest
		if err := wire.ReadMsg(scanner, &parsed); err != nil {
			if eof, ok := err.(unixInputEOF); ok {
				go func() {
					eof.waitForDisconnect()
					endSession()
				}()
				client.actions.Wait()
				err = io.EOF
			}
			if err == io.EOF {
				flushed := make(writeBarrier)
				if client.send(flushed) == nil {
					select {
					case <-flushed:
					case <-client.done:
					}
				}
			}
			return
		}

		if parsed.ClientID != "" {
			client.clientID = parsed.ClientID
		}

		switch parsed.Type {
		case "detach":
			store.removeByOwner(uid)
			_ = client.send(map[string]any{"type": "ok"})
			continue
		case "select":
			a := ui.acceptSelect(session, client, parsed)
			if a != nil {
				if frecencyDB != nil && a.producer != "" && a.resultID != "" {
					history.Add(1)
					go func() {
						defer history.Done()
						recordSelect(frecencyDB, store, a.producer, a.resultID)
					}()
				}
				ui.routeSelect(ctx, mgr, pluginsReg, reqOut, a)
			}
			continue
		case "status":
			_ = client.send(pluginStatus(mgr, pluginsReg))
			continue
		case "reload":
			ui.management.Lock()
			err := reloadPlugins(ctx, mgr, config.ReloadConfig)
			ui.management.Unlock()
			msg := "configuration reloaded"
			if err != nil {
				msg = err.Error()
			}
			_ = client.send(&wire.ReloadResponse{Type: "reload_response", Success: err == nil, Message: msg})
			continue
		case "restart":
			ui.management.Lock()
			resp := restartPlugins(ctx, mgr, parsed.Plugin)
			ui.management.Unlock()
			_ = client.send(resp)
			continue
		case "query", "":
			// handled below
		default:
			_ = client.send(map[string]any{"type": "error", "error": "unknown request type"})
			continue
		}

		input := parsed.Text
		if input == "" {
			continue
		}
		qid := fmt.Sprintf("q-%d-%d", time.Now().UnixNano(), querySeq.Add(1))
		store.startQuery(ctx, qid, client.clientID, input, uid, parsed.QueryStream)
		if err := client.send(&wire.AckMessage{Type: "ack", QueryID: qid}); err != nil {
			return
		}

		mgr.RLock()
		targetName, targetText, hasTarget := resolvePrefixTarget(input, mgr)
		if !hasTarget {
			targetText = input
		}
		// A query beginning with the routing symbol is an explicit routing
		// attempt, even when its prefix is only partially typed or unknown.
		prefixTargeted := hasTarget || mgr.HasRoutingPrefix(input)
		mgr.RUnlock()

		go dispatchQuery(ctx, targetText, qid, mgr, reqOut, pluginsReg, store, ui, prefixTargeted, targetName)
	}
}

func dispatchQuery(ctx context.Context, queryText, qid string, mgr *plugins.Manager, reqOut chan<- pluginRequest, pluginsReg *pluginRegistry, store *aggregateStore, ui *uiRegistry, hasTarget bool, targetName string) {
	query := store.query(qid)
	if query == nil {
		return
	}
	route := &resolvedQuery{q: query, text: queryText, daemonCtx: ctx, mgr: mgr, registry: pluginsReg, requests: reqOut, store: store, ui: ui}
	producers := make(map[string]*producerDispatch)
	var targets []string
	mgr.RLock()
	for name, p := range mgr.Plugins {
		if !p.Config.Enabled || !isEligibleForDispatch(name, p.Config, hasTarget, targetName) {
			continue
		}
		producers[name] = &producerDispatch{resolvedQuery: route, name: name, outstanding: true}
		targets = append(targets, name)
	}
	mgr.RUnlock()
	query.mu.Lock()
	query.producers = producers
	query.mu.Unlock()
	if snap, ok := store.setExpectedPlugins(qid, targets); ok {
		ui.publishSnapshot(&wire.UpdateMessage{Type: "update", QueryID: qid, Payload: snap}, nil)
	}
	var pending sync.WaitGroup
	for _, producer := range producers {
		pending.Add(1)
		go func() {
			defer pending.Done()
			producer.run(initialDispatch)
		}()
	}
	pending.Wait()
}

// run invokes exactly the recorded producer. A refresh never re-resolves the
// prefix or rediscovers the eligible set, even if the configuration changed.
func (d *producerDispatch) run(reason dispatchReason) {
	if d.q.ctx.Err() != nil {
		return
	}
	start := time.Now()
	d.mgr.RLock()
	plugin := d.eligiblePluginLocked(reason)
	if plugin == nil {
		d.mgr.RUnlock()
		if reason == initialDispatch {
			d.fail(start, "plugin disabled")
		} else {
			d.complete()
		}
		return
	}
	lifecycle := plugin.Config.Lifecycle
	var onCall *plugins.Plugin
	if lifecycle == plugins.LifecycleOnCall && d.name != plugins.OverviewID {
		onCall = &plugins.Plugin{Dir: plugin.Dir, Config: plugin.Config}
	}
	d.mgr.RUnlock()
	if d.name == plugins.OverviewID {
		data, err := queryOverview(d.mgr, d.registry, d.text)
		d.result(start, data, err)
		return
	}
	if onCall != nil {
		// A lifecycle change can leave the old persistent connection registered.
		d.q.mu.Lock()
		d.updatesFrom = nil
		d.q.mu.Unlock()
		ctx, cancel := context.WithTimeout(d.q.ctx, 3*time.Second)
		data, err := d.registry.bus.callPlugin(ctx, onCall, wire.UIRequest{Type: "query", QueryID: d.q.QueryID, Text: d.text})
		cancel()
		if err == nil && !json.Valid(data) {
			d.fail(start, "invalid on-call query response JSON")
			return
		}
		if err != nil && reason == initialDispatch {
			// Preserve the initial error payload expected by legacy clients.
			// A refresh failure instead keeps the last valid data visible.
			data, err = json.Marshal(map[string]string{"error": err.Error()})
		}
		d.result(start, data, err)
		return
	}
	if lifecycle == plugins.LifecycleOnDemandPersistent && !d.registry.isConnected(d.name) {
		if err := d.mgr.StartOnDemand(d.daemonCtx, d.name, wire.ResolvePluginsSocketPath()); err != nil {
			d.fail(start, err.Error())
			return
		}
		ctx, cancel := context.WithTimeout(d.q.ctx, 2*time.Second)
		_, connected := d.registry.waitConn(ctx, d.name, nil)
		cancel()
		if !connected {
			d.fail(start, "plugin did not connect")
			return
		}
	}
	// Pin before enqueue, under the same query lock used by retirement.
	// The router marks the actual write start; an earlier reply is unsolicited.
	d.q.mu.Lock()
	conn, ok := d.registry.getConn(d.name)
	d.conn, d.reason = conn, reason
	d.q.mu.Unlock()
	if !ok {
		d.fail(start, "plugin not connected")
		return
	}
	select {
	case d.requests <- pluginRequest{name: d.name, queryID: d.q.QueryID, text: d.text, conn: conn}:
	case <-d.q.ctx.Done():
	}
}

func isEligibleForDispatch(name string, cfg plugins.PluginConfig, hasTarget bool, targetName string) bool {
	if hasTarget {
		return name == targetName
	}
	if cfg.RequirePrefix {
		return false
	}
	return cfg.ProvidesGeneral
}

// The caller holds the manager read lock for the routing decision.
func resolvePrefixTarget(input string, mgr *plugins.Manager) (string, string, bool) {
	text := strings.TrimSpace(input)
	if text == "" {
		return "", "", false
	}
	var targetName string
	var targetPrefix string
	for name, p := range mgr.Plugins {
		if !p.Config.Enabled {
			continue
		}
		prefix := strings.TrimSpace(p.Config.ResolvedPrefix)
		if prefix == "" {
			continue
		}
		if !strings.HasPrefix(text, prefix) {
			continue
		}
		// Longest prefix wins. Equal-length prefixes are a config collision,
		// so fall back to id order rather than Go's random map iteration.
		if len(prefix) > len(targetPrefix) || (len(prefix) == len(targetPrefix) && name < targetName) {
			targetName = name
			targetPrefix = prefix
		}
	}
	if targetName == "" {
		return "", "", false
	}
	trimmed := strings.TrimSpace(strings.TrimPrefix(text, targetPrefix))
	return targetName, trimmed, true
}
