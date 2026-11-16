package daemon

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

func TestPersistentQueryRejectsOversizedFrameLocally(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.configure("p", plugins.LifecycleDaemon, true, "")
	f.mgr.Plugins["p"].Config.Capabilities = []string{wire.MsgEnd}
	p := f.connectPlugin(t, "p")
	held := make(chan pluginRequest, 2)
	owner, other := f.connectUI(t, held), f.connectUI(t, held)
	oversized := strings.Repeat("<", wire.MaxMessageSize/6+1)
	forwardOversized := func(id string) {
		t.Helper()
		select {
		case req := <-held:
			if req.queryID != id || req.end {
				t.Fatalf("expected request for %s, got %+v", id, req)
			}
			// Exercise the routed envelope without making the UI snapshot too large.
			req.text = oversized
			f.requests <- req
		case <-time.After(time.Second):
			t.Fatal("query never reached the queue")
		}
	}
	expectError := func(ui admissionPeer, id string) aggregate {
		t.Helper()
		snap := refreshSnapshot(t, func(v any) { ui.read(t, v) }, id, func(q aggregate) bool {
			return q.Plugins["p"].State == "error"
		})
		if err := snap.Plugins["p"].Error; err != wire.ErrMessageTooLarge.Error() {
			t.Fatalf("unexpected error: %s", err)
		}
		return snap
	}

	live := watchQuery(t, owner, true)
	f.requests <- <-held
	watchRequest(t, p, live)
	watchFrame(t, p, live, wire.MsgResponse, `["original"]`, true, false)
	watchRows(t, owner, live, "original")

	rejected := pendingEndTestQuery(t, other, "reject initial", "")
	forwardOversized(rejected)
	expectError(other, rejected)
	endTestQuery(t, other, rejected)

	q := f.store.query(live)
	action := &selectAction{owner: q.session, queryID: live, producer: "p", handler: "p", resultID: "original", offered: true}
	f.store.refreshAction(action)
	forwardOversized(live)
	if snap := expectError(owner, live); len(snap.List) != 1 || snap.List[0].ID != "original" {
		t.Fatalf("failed refresh replaced prior rows: %+v", snap.List)
	}

	fence := pendingEndTestQuery(t, other, "fence", "")
	f.requests <- <-held
	watchRequest(t, p, fence) // neither local rejection sent a request or end
	watchFrame(t, p, live, wire.MsgLiveUpdate, `["pushed"]`, false, false)
	watchRows(t, owner, live, "pushed")
	watchFrame(t, p, fence, wire.MsgResponse, `["fence"]`, false, false)
	watchRows(t, other, fence, "fence")

	action.resultID = "pushed"
	f.store.refreshAction(action)
	select {
	case req := <-held:
		f.requests <- req
	case <-time.After(time.Second):
		t.Fatal("rejected refresh did not release its slot")
	}
	watchRequest(t, p, live)
	watchFrame(t, p, live, wire.MsgResponse, `["refreshed"]`, true, false)
	watchRows(t, owner, live, "refreshed")
}

func TestPersistentQueryRetiresTransportOnWriteError(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.configure("p", plugins.LifecycleDaemon, true, "")
	server, p := admissionSocket(t)
	// Even the size sentinel is a transport failure if Write returned it.
	writer := &failingSelectPeer{Conn: server, err: wire.ErrMessageTooLarge}
	go handlePluginConn(t.Context(), writer, f.registry, f.store, f.ui)
	p.send(t, wire.PluginHello{Type: wire.MsgHello, Name: "p"})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, ok := f.registry.waitConn(ctx, "p", nil); !ok {
		t.Fatal("plugin did not register")
	}
	owner := f.connectUI(t, f.requests)
	id := watchQuery(t, owner, false)
	snap := refreshSnapshot(t, func(v any) { owner.read(t, v) }, id, func(q aggregate) bool {
		return q.Plugins["p"].State == "error"
	})
	if err := snap.Plugins["p"].Error; err != "plugin disconnected" || f.registry.isConnected("p") {
		t.Fatalf("transport failure did not retire the plugin: %q", err)
	}
}

type gatedQueryDeadline struct {
	peerConn
	arm     chan struct{}
	started chan struct{}
	release chan struct{}
}

func (p *gatedQueryDeadline) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		select {
		case <-p.arm:
			close(p.started)
			<-p.release
			deadline = time.Now().Add(time.Second)
		default:
		}
	}
	return p.peerConn.SetWriteDeadline(deadline)
}

func TestPersistentInitialQueryRechecksEnabledAtWrite(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.store.limit = 1
	f.configure("p", plugins.LifecycleDaemon, true, "")
	f.mgr.Plugins["p"].Config.Capabilities = []string{wire.MsgEnd}
	server, p := admissionSocket(t)
	writer := &gatedQueryDeadline{server, make(chan struct{}, 1), make(chan struct{}), make(chan struct{})}
	release := sync.OnceFunc(func() { close(writer.release) })
	defer release()
	f.registry.set("p", writer)
	owner, watcher := f.connectUI(t, f.requests), f.connectUI(t, f.requests)

	old := pendingEndTestQuery(t, owner, "waiting for its response", "")
	watchRequest(t, p, old)
	writer.arm <- struct{}{}
	id := watchQuery(t, watcher, true)
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("initial query did not reach the router")
	}
	f.mgr.Lock()
	f.mgr.Plugins["p"].Config.Enabled = false
	f.mgr.Unlock()
	for range cap(f.requests) {
		f.requests <- pluginRequest{name: "p", conn: writer, queryID: "expired"}
	}
	release()

	snap := refreshSnapshot(t, func(v any) { watcher.read(t, v) }, id, func(q aggregate) bool {
		return q.Plugins["p"].State == "error"
	})
	if err := snap.Plugins["p"].Error; err != "plugin disabled" {
		t.Fatalf("unexpected error: %q", err)
	}
	// The rejected watch demotes and evicts the old query. Its end must
	// drain through the full router queue without a write for the watch.
	expectPluginQueryFrame(t, p, wire.MsgEnd, old)
	endTestQuery(t, watcher, id)
	f.mgr.Lock()
	f.mgr.Plugins["p"].Config.Enabled = true
	f.mgr.Unlock()
	fence := pendingEndTestQuery(t, owner, "fence", "")
	watchRequest(t, p, fence)
}
