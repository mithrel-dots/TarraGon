package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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
	management sync.Mutex
}

func newUIRegistry(store *aggregateStore) *uiRegistry {
	return &uiRegistry{
		clients: make(map[string]*uiClient),
		store:   store,
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

func (r *uiRegistry) publish(msg any) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, c := range r.clients {
		_ = c.send(msg)
	}
}

func handleUIClient(ctx context.Context, conn peerConn, mgr *plugins.Manager, reqOut chan<- pluginRequest, pluginsReg *pluginRegistry, store *aggregateStore, ui *uiRegistry, frecencyDB *db.DB) {
	scanner := wire.NewScanner(conn)
	client := &uiClient{conn: conn}
	ui.add(client)
	uid := client.uid
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
			if err == io.EOF {
				// A client may close its write half after a control request.
				// Deliver the replies already accepted by its writer first.
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
			selectedID := parsed.ID
			if selectedID == "" {
				selectedID = parsed.ResultID
			}
			if selectedID == "" {
				selectedID = parsed.Text
			}
			if frecencyDB != nil && parsed.Plugin != "" && selectedID != "" {
				go func(plugin, id string) {
					if err := frecencyDB.RecordSelection(context.Background(), plugin, id); err != nil {
						log.Printf("[UI] failed to record selection %s:%s: %v", plugin, id, err)
						return
					}
					if err := store.RefreshFrecencyCache(context.Background()); err != nil {
						log.Printf("[UI] failed to refresh frecency cache: %v", err)
					}
				}(parsed.Plugin, selectedID)
			}
			if parsed.Plugin == plugins.OverviewID {
				if err := client.send(map[string]any{"type": "ok"}); err != nil {
					return
				}
				msg, err := ui.selectOverview(ctx, mgr, pluginsReg, selectedID, parsed.Action)
				if err != nil {
					msg = err.Error()
				}
				_ = client.send(&wire.SelectResponse{Type: wire.MsgSelectResponse, Success: err == nil, Message: msg})
				switch parsed.Action {
				case "enable", "disable", "restart":
					// Re-read effective state even when starting the process failed
					// after its configuration was successfully changed.
					if q := store.query(parsed.QueryID); q != nil && q.session == uid {
						if target, filter, ok := resolvePrefixTarget(q.Input, mgr); ok && target == plugins.OverviewID {
							publishOverview(mgr, pluginsReg, store, ui, parsed.QueryID, filter)
						}
					}
				}
				continue
			}
			if parsed.Plugin != "" && parsed.QueryID != "" && pluginsReg.isConnected(parsed.Plugin) {
				select {
				case reqOut <- pluginRequest{name: parsed.Plugin, queryID: parsed.QueryID, resultID: selectedID, action: parsed.Action, msgType: wire.MsgSelect}:
				case <-ctx.Done():
					return
				case <-client.done:
					return
				}
			} else if parsed.Plugin != "" {
				mgr.RLock()
				plug, ok := mgr.Plugins[parsed.Plugin]
				var snapDir string
				var snapCfg plugins.PluginConfig
				if ok {
					snapDir = plug.Dir
					snapCfg = plug.Config
				}
				mgr.RUnlock()

				if ok && snapCfg.Enabled && snapCfg.Lifecycle == plugins.LifecycleOnCall && selectedID != "" {
					pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
					data, err := pluginsReg.bus.callPlugin(pctx, &plugins.Plugin{Dir: snapDir, Config: snapCfg}, wire.UIRequest{Type: wire.MsgSelect, ResultID: selectedID, Action: parsed.Action})
					cancel()
					var resp wire.SelectResponse
					if err == nil {
						err = json.Unmarshal(data, &resp)
					}
					if err != nil {
						resp = wire.SelectResponse{Type: wire.MsgSelectResponse, Success: false, Message: err.Error()}
					}
					_ = client.send(&resp)
				}
			}
			_ = client.send(map[string]any{"type": "ok"})
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

		targetName, targetText, hasTarget := resolvePrefixTarget(input, mgr)
		if !hasTarget {
			targetText = input
		}
		// A query beginning with the routing symbol is an explicit routing
		// attempt, even when its prefix is only partially typed or unknown.
		mgr.RLock()
		prefixTargeted := hasTarget || startsWithRoutingPrefix(input)
		mgr.RUnlock()

		go dispatchQuery(ctx, targetText, qid, mgr, reqOut, pluginsReg, store, ui, prefixTargeted, targetName)
	}
}

func dispatchQuery(ctx context.Context, queryText, qid string, mgr *plugins.Manager, reqOut chan<- pluginRequest, pluginsReg *pluginRegistry, store *aggregateStore, ui *uiRegistry, hasTarget bool, targetName string) {
	query := store.query(qid)
	if query == nil {
		return
	}
	queryCtx := query.ctx
	type pluginSnapshot struct {
		name string
		dir  string
		cfg  plugins.PluginConfig
	}

	mgr.RLock()
	snaps := make([]pluginSnapshot, 0, len(mgr.Plugins))
	for name, p := range mgr.Plugins {
		snaps = append(snaps, pluginSnapshot{name: name, dir: p.Dir, cfg: p.Config})
	}
	mgr.RUnlock()

	targets := make([]string, 0, len(snaps))
	for _, snap := range snaps {
		if queryCtx.Err() != nil {
			return
		}
		if !snap.cfg.Enabled {
			continue
		}
		if !isEligibleForDispatch(snap.name, snap.cfg, hasTarget, targetName) {
			continue
		}
		targets = append(targets, snap.name)
	}
	if snap, ok := store.setExpectedPlugins(qid, targets); ok {
		ui.publishSnapshot(&wire.UpdateMessage{Type: "update", QueryID: qid, Payload: snap}, nil)
	}
	if targetName == plugins.OverviewID && len(targets) == 1 {
		publishOverview(mgr, pluginsReg, store, ui, qid, queryText)
		return
	}

	for _, snap := range snaps {
		if queryCtx.Err() != nil {
			return
		}
		if !snap.cfg.Enabled {
			continue
		}
		if snap.cfg.Lifecycle == plugins.LifecycleOnCall {
			continue
		}
		if !isEligibleForDispatch(snap.name, snap.cfg, hasTarget, targetName) {
			continue
		}

		if snap.cfg.Lifecycle == plugins.LifecycleOnDemandPersistent && !pluginsReg.isConnected(snap.name) {
			if err := mgr.StartOnDemand(ctx, snap.name, wire.ResolvePluginsSocketPath()); err != nil {
				log.Printf("[UI] failed to start on-demand plugin %s: %v", snap.name, err)
				store.submitError(qid, snap.name, 0, err.Error(), ui.publishSnapshot)
				continue
			}

			connected := false
			for i := 0; i < 40; i++ {
				if pluginsReg.isConnected(snap.name) {
					connected = true
					break
				}
				select {
				case <-time.After(50 * time.Millisecond):
				case <-queryCtx.Done():
					return
				}
			}

			if !connected {
				log.Printf("[UI] on-demand plugin %s did not connect within 2s; skipping query %s", snap.name, qid)
				store.submitError(qid, snap.name, 2000, "plugin did not connect", ui.publishSnapshot)
				continue
			}
		}

		if pluginsReg.isConnected(snap.name) {
			select {
			case reqOut <- pluginRequest{name: snap.name, queryID: qid, text: queryText}:
			case <-queryCtx.Done():
				return
			}
		} else {
			store.submitError(qid, snap.name, 0, "plugin not connected", ui.publishSnapshot)
		}
	}

	for _, snap := range snaps {
		if queryCtx.Err() != nil {
			return
		}
		if !snap.cfg.Enabled || snap.cfg.Lifecycle != plugins.LifecycleOnCall {
			continue
		}
		if !isEligibleForDispatch(snap.name, snap.cfg, hasTarget, targetName) {
			continue
		}
		t0 := time.Now()
		pctx, cancel := context.WithTimeout(queryCtx, 3*time.Second)
		raw, err := pluginsReg.bus.callPlugin(pctx, &plugins.Plugin{Dir: snap.dir, Config: snap.cfg}, wire.UIRequest{Type: "query", Text: queryText})
		cancel()
		if err != nil {
			raw = json.RawMessage([]byte(fmt.Sprintf(`{"error":"%s"}`, escapeJSONString(err.Error()))))
		}
		store.SubmitResponse(snap.name, wire.PluginResponse{QueryID: qid, Data: raw}, float64(time.Since(t0).Microseconds())/1000.0, ui.publishSnapshot)
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

func startsWithRoutingPrefix(input string) bool {
	text := strings.TrimSpace(input)
	symbol := strings.TrimSpace(plugins.PrefixSymbol())
	return symbol != "" && strings.HasPrefix(text, symbol)
}

func resolvePrefixTarget(input string, mgr *plugins.Manager) (string, string, bool) {
	text := strings.TrimSpace(input)
	if text == "" {
		return "", "", false
	}
	var targetName string
	var targetPrefix string
	mgr.RLock()
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
	mgr.RUnlock()
	if targetName == "" {
		return "", "", false
	}
	trimmed := strings.TrimSpace(strings.TrimPrefix(text, targetPrefix))
	return targetName, trimmed, true
}
