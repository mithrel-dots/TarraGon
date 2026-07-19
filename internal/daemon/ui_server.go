package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mithrel-dots/tarragon/internal/config"
	"github.com/mithrel-dots/tarragon/internal/db"
	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

var querySeq uint64

// uiClient tracks a connected UI.
type uiClient struct {
	conn     net.Conn
	clientID string
	out      chan any
	done     chan struct{}
	stopOnce sync.Once
}

type writeBarrier chan struct{}

func (c *uiClient) stop() {
	c.stopOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

func (c *uiClient) send(msg any) error {
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

// uiRegistry tracks active UI clients.
type uiRegistry struct {
	mu      sync.RWMutex
	clients map[string]*uiClient
}

func newUIRegistry() *uiRegistry {
	return &uiRegistry{clients: make(map[string]*uiClient)}
}

func (r *uiRegistry) add(c *uiClient) {
	c.out = make(chan any, 128)
	c.done = make(chan struct{})
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.clients[c.clientID]; ok && old != c {
		old.stop()
	}
	r.clients[c.clientID] = c
	go func() {
		defer r.remove(c)
		for {
			select {
			case <-c.done:
				return
			case msg := <-c.out:
				if barrier, ok := msg.(writeBarrier); ok {
					close(barrier)
					continue
				}
				_ = c.conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
				if err := wire.WriteMsg(c.conn, msg); err != nil {
					return
				}
			}
		}
	}()
}

func (r *uiRegistry) updateClientID(c *uiClient, newID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clients[c.clientID] != c {
		return
	}
	delete(r.clients, c.clientID)
	c.clientID = newID
	if old, ok := r.clients[newID]; ok && old != c {
		old.stop()
	}
	r.clients[newID] = c
}

func (r *uiRegistry) remove(c *uiClient) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clients[c.clientID] == c {
		delete(r.clients, c.clientID)
	}
	c.stop()
}

func (r *uiRegistry) publish(msg any) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, c := range r.clients {
		_ = c.send(msg)
	}
}

func (r *uiRegistry) publishSnapshot(msg *wire.UpdateMessage, _ []string) { r.publish(msg) }

func startUIServer(ctx context.Context, mgr *plugins.Manager, reqOut chan<- pluginRequest, pluginsReg *pluginRegistry, store *aggregateStore, ui *uiRegistry, frecencyDB *db.DB) {
	socketPath := wire.ResolveUISocketPath()
	ln, err := wire.ListenUnix(socketPath)
	if err != nil {
		log.Fatalf("[UI] failed to listen on %s: %v", socketPath, err)
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close()
		_ = wire.CleanupSocket(socketPath)
	}()
	log.Printf("[UI] listening on %s", socketPath)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[UI] accept error: %v", err)
			continue
		}
		go handleUIClient(ctx, conn, mgr, reqOut, pluginsReg, store, ui, frecencyDB)
	}
}

func handleUIClient(ctx context.Context, conn net.Conn, mgr *plugins.Manager, reqOut chan<- pluginRequest, pluginsReg *pluginRegistry, store *aggregateStore, ui *uiRegistry, frecencyDB *db.DB) {
	scanner := wire.NewScanner(conn)
	uid := fmt.Sprintf("ui-%d-%d", time.Now().UnixNano(), atomicAdd(&querySeq, 1))
	client := &uiClient{conn: conn, clientID: uid}
	ui.add(client)
	stopOnShutdown := context.AfterFunc(ctx, client.stop)
	defer stopOnShutdown()
	defer ui.remove(client)
	defer store.removeStreams(uid)

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

		if parsed.ClientID != "" && parsed.ClientID != client.clientID {
			ui.updateClientID(client, parsed.ClientID)
		}

		switch parsed.Type {
		case "detach":
			targetClient := parsed.ClientID
			if targetClient == "" {
				targetClient = client.clientID
			}
			_ = store.removeByClient(targetClient)
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
			if parsed.Plugin != "" && parsed.QueryID != "" && pluginsReg.isConnected(parsed.Plugin) {
				reqOut <- pluginRequest{name: parsed.Plugin, queryID: parsed.QueryID, resultID: selectedID, action: parsed.Action, msgType: wire.MsgSelect}
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
					resp, err := invokeOnCallSelect(pctx, &plugins.Plugin{Dir: snapDir, Config: snapCfg}, selectedID, parsed.Action)
					cancel()
					if err != nil {
						resp = wire.SelectResponse{Type: wire.MsgSelectResponse, Success: false, Message: err.Error()}
					}
					ui.publish(&resp)
				}
			}
			_ = client.send(map[string]any{"type": "ok"})
			continue
		case "status":
			connected := pluginsReg.Names()
			total := 0
			mgr.RLock()
			pluginInfos := make([]wire.PluginInfo, 0, len(mgr.Plugins))
			for _, p := range mgr.Plugins {
				if p.Config.Enabled {
					total++
				}
				pluginInfos = append(pluginInfos, wire.PluginInfo{
					ID:              p.Config.ID,
					Name:            p.Config.Name,
					Description:     p.Config.Description,
					Source:          p.Config.Source,
					Enabled:         p.Config.Enabled,
					Connected:       pluginsReg.isConnected(p.Config.ID),
					Lifecycle:       string(p.Config.Lifecycle),
					Prefix:          p.Config.ResolvedPrefix,
					RequirePrefix:   p.Config.RequirePrefix,
					ProvidesGeneral: p.Config.ProvidesGeneral,
					Capabilities:    p.Config.Capabilities,
					Icon:            p.Config.Icon,
				})
			}
			mgr.RUnlock()
			sort.Slice(pluginInfos, func(i, j int) bool {
				return pluginInfos[i].ID < pluginInfos[j].ID
			})
			_ = client.send(&wire.StatusResponse{
				Type:      "status",
				Connected: connected,
				Total:     total,
				Plugins:   pluginInfos,
			})
			continue
		case "reload":
			if err := config.ReloadConfig(); err != nil {
				_ = client.send(&wire.ReloadResponse{Type: "reload_response", Success: false, Message: fmt.Sprintf("failed to read config: %v", err)})
				continue
			}

			startPersistent := false
			mgr.Lock()
			beforeEnabled := make(map[string]bool, len(mgr.Plugins))
			beforeLifecycle := make(map[string]plugins.LifecycleMode, len(mgr.Plugins))
			for name, p := range mgr.Plugins {
				beforeEnabled[name] = p.Config.Enabled
				beforeLifecycle[name] = p.Config.Lifecycle
			}

			if err := mgr.DiscoverNew(); err != nil {
				mgr.Unlock()
				_ = client.send(&wire.ReloadResponse{
					Type:    "reload_response",
					Success: false,
					Message: fmt.Sprintf("failed to discover new plugins: %v", err),
				})
				continue
			}

			if err := mgr.ApplyOverrides(); err != nil {
				mgr.Unlock()
				_ = client.send(&wire.ReloadResponse{
					Type:    "reload_response",
					Success: false,
					Message: fmt.Sprintf("failed to apply config overrides: %v", err),
				})
				continue
			}

			var toStop []string
			for name, p := range mgr.Plugins {
				wasEnabled := beforeEnabled[name]
				wasLifecycle := beforeLifecycle[name]
				isEnabled := p.Config.Enabled
				isLifecycle := p.Config.Lifecycle

				if !isEnabled {
					if p.Running() {
						toStop = append(toStop, name)
					}
					continue
				}

				if wasLifecycle != isLifecycle && p.Running() {
					toStop = append(toStop, name)
				}

				if (!wasEnabled && isEnabled && isLifecycle == plugins.LifecycleDaemon) ||
					(wasLifecycle != isLifecycle && isLifecycle == plugins.LifecycleDaemon) {
					startPersistent = true
				}
			}
			mgr.Unlock()

			// Stop after releasing the lock: plugins now get a grace period to
			// exit, and holding the manager lock through it would block every
			// concurrent query.
			if len(toStop) > 0 {
				mgr.StopPlugins(toStop)
			}

			if startPersistent {
				if err := mgr.StartPersistent(ctx, wire.ResolvePluginsSocketPath()); err != nil {
					_ = client.send(&wire.ReloadResponse{Type: "reload_response", Success: false, Message: fmt.Sprintf("reload applied, but failed to start daemon plugins: %v", err)})
					continue
				}
			}

			_ = client.send(&wire.ReloadResponse{Type: "reload_response", Success: true, Message: "configuration reloaded"})
			continue
		case "restart":
			// Re-read manifests and overrides first so a restart also picks up
			// plugins installed or reconfigured since the daemon started.
			if err := config.ReloadConfig(); err != nil {
				log.Printf("[UI] restart: could not re-read config: %v", err)
			}
			mgr.Lock()
			discoverErr := mgr.RefreshConfigs()
			overrideErr := mgr.ApplyOverrides()
			mgr.Unlock()
			if discoverErr != nil {
				log.Printf("[UI] restart: plugin discovery failed: %v", discoverErr)
			}
			if overrideErr != nil {
				_ = client.send(&wire.RestartResponse{
					Type:    "restart_response",
					Success: false,
					Message: fmt.Sprintf("failed to apply config overrides: %v", overrideErr),
				})
				continue
			}

			var targets []string
			if parsed.Plugin != "" {
				targets = []string{parsed.Plugin}
			}

			results := mgr.Restart(ctx, wire.ResolvePluginsSocketPath(), targets)

			wireResults := make([]wire.RestartResult, 0, len(results))
			failed := 0
			restarted := 0
			for _, r := range results {
				if r.Status == plugins.RestartStatusError {
					failed++
				}
				if r.Status == plugins.RestartStatusRestarted {
					restarted++
				}
				wireResults = append(wireResults, wire.RestartResult{
					Name:    r.Name,
					Status:  r.Status,
					Message: r.Message,
				})
			}

			msg := fmt.Sprintf("restarted %d plugin(s)", restarted)
			if failed > 0 {
				msg = fmt.Sprintf("%s, %d failed", msg, failed)
			}
			_ = client.send(&wire.RestartResponse{
				Type:    "restart_response",
				Success: failed == 0,
				Message: msg,
				Results: wireResults,
			})
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
		clientID := client.clientID
		if parsed.ClientID != "" {
			clientID = parsed.ClientID
		}

		qid := fmt.Sprintf("q-%d-%d", time.Now().UnixNano(), atomicAdd(&querySeq, 1))
		store.startQuery(ctx, qid, clientID, input, uid, parsed.QueryStream)
		if err := client.send(&wire.AckMessage{Type: "ack", QueryID: qid}); err != nil {
			return
		}

		targetName, targetText, hasTarget := resolvePrefixTarget(input, mgr)
		if !hasTarget {
			targetText = input
		}
		// A query beginning with the routing symbol is an explicit routing
		// attempt, even when its prefix is only partially typed or unknown.
		prefixTargeted := hasTarget || startsWithRoutingPrefix(input)

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
		ui.publish(&wire.UpdateMessage{Type: "update", QueryID: qid, Payload: snap})
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
		raw, err := invokeOnCallQuery(pctx, &plugins.Plugin{Dir: snap.dir, Config: snap.cfg}, queryText)
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

// atomicAdd wraps sync/atomic.AddUint64 for clarity.
func atomicAdd(ptr *uint64, delta uint64) uint64 { return atomic.AddUint64(ptr, delta) }

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
