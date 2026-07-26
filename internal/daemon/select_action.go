package daemon

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/mithrel-dots/tarragon/internal/db"
	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

// selectAction lives independently of the query that offered the row. Its
// owner is the original peer, even if a new peer registers the same label.
type selectAction struct {
	id        string
	requestID string
	owner     string
	queryID   string
	producer  string
	resultID  string
	handler   string
	action    string

	client        *uiClient
	wantsProgress bool
	received      time.Time
	timeout       time.Duration
	done          chan struct{}

	mu         sync.Mutex
	deadline   time.Time
	timer      *time.Timer
	dispatched bool
	terminal   bool
	cleanup    []func()
}

func (ui *uiRegistry) acceptSelect(session context.Context, client *uiClient, req wire.UIRequest) *selectAction {
	row := req.ID
	if row == "" {
		row = req.ResultID
	}
	if row == "" {
		row = req.Text
	}
	a := &selectAction{
		id:            "a-" + rand.Text(),
		requestID:     req.RequestID,
		owner:         client.uid,
		queryID:       req.QueryID,
		producer:      req.Plugin,
		handler:       req.Plugin,
		resultID:      row,
		action:        req.Action,
		wantsProgress: slices.Contains(req.UIFeatures, wire.MsgActionProgress),
		client:        client,
		received:      time.Now(),
		timeout:       ui.actionTimeout,
		done:          make(chan struct{}),
	}
	a.deadline = a.received.Add(a.timeout)
	if err := client.send(&wire.SelectAck{Type: "ok", ActionID: a.id, RequestID: a.requestID}); err != nil {
		return nil
	}
	select {
	case ui.actionSlots <- struct{}{}:
	default:
		a.notRun("Too many pending actions.")
		return nil
	}
	a.mu.Lock()
	client.actions.Add(1)
	stop := context.AfterFunc(session, func() { a.fail("UI session closed.") })
	a.cleanup = append(a.cleanup, func() {
		stop()
		<-ui.actionSlots
		client.actions.Done()
	})
	a.timer = time.AfterFunc(time.Until(a.deadline), a.expire)
	a.mu.Unlock()
	return a
}

func (a *selectAction) pending() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pendingLocked()
}

func (a *selectAction) pendingLocked() bool {
	return !a.terminal && time.Now().Before(a.deadline)
}

func (a *selectAction) onTerminal(fn func()) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.terminal {
		return false
	}
	a.cleanup = append(a.cleanup, fn)
	return true
}

// Mark the point at which the persistent frame may be delivered, just before
// writing it. A timeout racing the write must assume an effect is possible.
func (a *selectAction) beginDispatch() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.dispatched || !a.pendingLocked() {
		return false
	}
	a.dispatched = true
	return true
}

// Start and the transition to "possibly ran" are indivisible for a local
// child. Waiting for the child happens outside this lock and the UI lifetime.
func (a *selectAction) startProcess(start func() error) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.dispatched || !a.pendingLocked() {
		return false, nil
	}
	if err := start(); err != nil {
		return false, err
	}
	a.dispatched = true
	return true, nil
}

func (a *selectAction) progress(message string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.dispatched || !a.pendingLocked() {
		return false
	}
	a.deadline = time.Now().Add(a.timeout)
	a.timer.Reset(a.timeout)
	if a.wantsProgress {
		// Serialize queue insertion with completion so progress cannot follow
		// the final. The peer writer, not this goroutine, performs the write.
		_ = a.client.send(&wire.ActionProgress{Type: wire.MsgActionProgress, ActionID: a.id, RequestID: a.requestID, Message: message})
	}
	return true
}

func (a *selectAction) expire() {
	a.mu.Lock()
	if a.terminal || time.Now().Before(a.deadline) {
		a.mu.Unlock()
		return // a progress frame already reset this timer
	}
	a.failLocked("Action deadline expired.")
}

func (a *selectAction) finish(success bool, message string) bool {
	a.mu.Lock()
	if !a.terminal && !time.Now().Before(a.deadline) {
		a.failLocked("Action deadline expired.")
		return false
	}
	return a.completeLocked(wire.SelectResponse{Success: success, Message: message})
}

func (a *selectAction) notRun(message string) {
	a.mu.Lock()
	a.completeLocked(wire.SelectResponse{Code: "not_run", Message: message})
}

func (a *selectAction) fail(message string) {
	a.mu.Lock()
	a.failLocked(message)
}

func (a *selectAction) failLocked(message string) {
	code := "not_run"
	if a.dispatched {
		code = "completion_unknown"
		message += " The effect may already have occurred."
	}
	a.completeLocked(wire.SelectResponse{Code: code, Message: message})
}

// completeLocked consumes the lock. Exactly one caller can close done,
// publish a terminal, retire its connection slot and run its continuation.
func (a *selectAction) completeLocked(resp wire.SelectResponse) bool {
	if a.terminal {
		a.mu.Unlock()
		return false
	}
	a.terminal = true
	if a.timer != nil {
		a.timer.Stop()
	}
	close(a.done)
	cleanup := a.cleanup
	a.cleanup = nil
	a.mu.Unlock()

	resp.Type, resp.ActionID, resp.RequestID = wire.MsgSelectResponse, a.id, a.requestID
	_ = a.client.send(&resp)
	for _, fn := range cleanup {
		fn()
	}
	return true
}

func (ui *uiRegistry) routeSelect(ctx context.Context, mgr *plugins.Manager, reg *pluginRegistry, out chan<- pluginRequest, a *selectAction) {
	if a.resultID == "" || a.handler == "" {
		a.notRun("Select requires a plugin and result ID.")
		return
	}
	mgr.RLock()
	p := mgr.Plugins[a.handler]
	var plugin *plugins.Plugin
	if p != nil && p.Config.Enabled {
		plugin = &plugins.Plugin{Config: p.Config, Dir: p.Dir}
	}
	mgr.RUnlock()
	if plugin == nil {
		a.notRun(fmt.Sprintf("Plugin %q is unknown or disabled.", a.handler))
		return
	}
	if a.handler == plugins.OverviewID {
		go func() {
			msg, refresh, err := ui.selectOverview(ctx, mgr, reg, a)
			if err != nil {
				msg = err.Error()
			}
			if a.finish(err == nil, msg) && refresh {
				if q := ui.store.query(a.queryID); q != nil && q.session == a.owner {
					if target, filter, ok := resolvePrefixTarget(q.Input, mgr); ok && target == plugins.OverviewID {
						publishOverview(mgr, reg, ui.store, ui, a.queryID, filter)
					}
				}
			}
		}()
		return
	}
	if plugin.Config.Lifecycle == plugins.LifecycleOnCall {
		if reg.bus == nil {
			a.notRun("On-call responder is unavailable.")
			return
		}
		go reg.bus.callPluginAction(ctx, plugin, a)
		return
	}
	if conn, ok := reg.getConn(a.handler); ok {
		a.enqueue(ctx, out, conn)
		return
	}
	if plugin.Config.Lifecycle != plugins.LifecycleOnDemandPersistent {
		a.notRun(fmt.Sprintf("Plugin %q is not connected.", a.handler))
		return
	}
	go func() {
		// The process serves future queries too: its context belongs to the
		// daemon; only this wait for registration belongs to the select.
		if !a.pending() {
			return
		}
		if err := mgr.StartOnDemand(ctx, a.handler, wire.ResolvePluginsSocketPath()); err != nil {
			a.notRun(err.Error())
			return
		}
		if conn, ok := reg.waitConn(ctx, a.handler, a.done); ok {
			a.enqueue(ctx, out, conn)
		}
	}()
}

func (a *selectAction) enqueue(ctx context.Context, out chan<- pluginRequest, conn peerConn) {
	req := pluginRequest{name: a.handler, conn: conn, selectAction: a}
	select {
	case out <- req:
	case <-a.done:
	case <-ctx.Done():
		a.fail("Daemon is shutting down.")
	case <-a.client.done:
		a.fail("UI session closed.")
	}
}

func recordSelect(database *db.DB, store *aggregateStore, plugin, row string) {
	ctx := context.Background()
	if err := database.RecordSelection(ctx, plugin, row); err != nil {
		log.Printf("[UI] failed to record selection %s:%s: %v", plugin, row, err)
		return
	}
	if err := store.RefreshFrecencyCache(ctx); err != nil {
		log.Printf("[UI] failed to refresh frecency cache: %v", err)
	}
}
