package daemon

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

func pendingEndTestQuery(t *testing.T, ui admissionPeer, text, stream string) string {
	t.Helper()
	ui.send(t, wire.UIRequest{Type: "query", Text: text, QueryStream: stream})
	var ack wire.AckMessage
	ui.read(t, &ack)
	if ack.Type != "ack" || ack.QueryID == "" {
		t.Fatalf("expected query acknowledgement, got %+v", ack)
	}
	var update wire.UpdateMessage
	ui.read(t, &update)
	if update.Type != "update" || update.QueryID != ack.QueryID {
		t.Fatalf("expected initial snapshot for %s, got %s for %s", ack.QueryID, update.Type, update.QueryID)
	}
	return ack.QueryID
}

func endTestQuery(t *testing.T, ui admissionPeer, id string) {
	t.Helper()
	ui.send(t, wire.UIRequest{Type: wire.MsgEnd, QueryID: id})
	var ack wire.AckMessage
	ui.read(t, &ack)
	if ack.Type != "ok" || ack.QueryID != id {
		t.Fatalf("expected end receipt for %s, got %+v", id, ack)
	}
}

func expectPluginQueryFrame(t *testing.T, plugin admissionPeer, kind, id string) {
	t.Helper()
	var frame map[string]json.RawMessage
	plugin.read(t, &frame)
	if readStringField(frame, "type") != kind || readStringField(frame, "query_id") != id || (kind == wire.MsgEnd && len(frame) != 2) {
		t.Fatalf("expected %s for %s, got %s", kind, id, frame)
	}
}

func TestEndQueryOnlyDispatchedParticipants(t *testing.T) {
	f := newAdmissionFixture(t, 2*time.Second)
	for _, name := range []string{"pending", "legacy", "finished", "live"} {
		f.configure(name, plugins.LifecycleDaemon, true, "")
		if name == "pending" || name == "finished" {
			f.mgr.Plugins[name].Config.Capabilities = []string{wire.MsgEnd}
		}
	}
	pending, legacy, finished := f.connectPlugin(t, "pending"), f.connectPlugin(t, "legacy"), f.connectPlugin(t, "finished")
	live := f.connectPlugin(t, "live")
	participants := []admissionPeer{pending, legacy, finished, live}
	owner, foreign := f.connectUI(t, f.requests), f.connectUI(t, f.requests)
	read := func(v any) { owner.read(t, v) }
	query := func(text string) string {
		t.Helper()
		id := pendingEndTestQuery(t, owner, text, "")
		for _, p := range participants {
			expectPluginQueryFrame(t, p, wire.MsgRequest, id)
		}
		return id
	}
	first, second := query("one"), query("two")
	respond := func(p admissionPeer, id string) {
		t.Helper()
		p.send(t, wire.PluginResponse{Type: wire.MsgResponse, QueryID: id, Data: json.RawMessage(`["row"]`)})
	}
	respond(finished, first)
	live.send(t, wire.PluginResponse{Type: wire.MsgResponse, QueryID: first, Data: json.RawMessage(`["streaming"]`), Live: true})
	refreshSnapshot(t, read, first, func(q aggregate) bool {
		return q.Plugins["finished"].State == "done" && q.Plugins["live"].State == "done"
	})
	// The advertised capability belongs to each dispatch, even across reloads.
	f.mgr.Lock()
	f.mgr.Plugins["pending"].Config.Capabilities = nil
	f.mgr.Plugins["legacy"].Config.Capabilities = []string{wire.MsgEnd}
	f.mgr.Unlock()
	endTestQuery(t, foreign, first)
	endTestQuery(t, foreign, "unknown")
	if f.store.query(first) == nil {
		t.Fatal("foreign end removed the owner's query")
	}
	endTestQuery(t, owner, first)
	endTestQuery(t, owner, first)
	expectPluginQueryFrame(t, pending, wire.MsgEnd, first)
	expectPluginQueryFrame(t, live, wire.MsgEnd, first)
	// On each plugin reader the late frame precedes the valid response. By
	// the time the second view completes, all the late frames were handled.
	for _, p := range participants {
		respond(p, first)
		respond(p, second)
	}
	refreshSnapshot(t, read, second, func(q aggregate) bool {
		for _, p := range q.Plugins {
			if p.State != "done" {
				return false
			}
		}
		return true
	})
	foreign.expectStatus(t)
	query("writer fence") // catches a duplicate or an end on legacy/finished
}

func TestEndQuerySupersessionAndEviction(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.store.limit = 1
	f.configure("p", plugins.LifecycleDaemon, true, "")
	f.mgr.Plugins["p"].Config.Capabilities = []string{wire.MsgEnd}
	p := f.connectPlugin(t, "p")
	u := f.connectUI(t, f.requests)
	first := pendingEndTestQuery(t, u, "one", "search")
	expectPluginQueryFrame(t, p, wire.MsgRequest, first)
	second := pendingEndTestQuery(t, u, "two", "search")
	expectPluginQueryFrame(t, p, wire.MsgEnd, first)
	expectPluginQueryFrame(t, p, wire.MsgRequest, second)
	third := pendingEndTestQuery(t, u, "three", "")
	expectPluginQueryFrame(t, p, wire.MsgEnd, second)
	expectPluginQueryFrame(t, p, wire.MsgRequest, third)
	if f.store.query(first) != nil || f.store.query(second) != nil || f.store.query(third) == nil {
		t.Fatal("unexpected retained queries after supersession and eviction")
	}
}

func TestEndQueryBeforeDispatch(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.configure("p", plugins.LifecycleDaemon, true, "")
	f.mgr.Plugins["p"].Config.Capabilities = []string{wire.MsgEnd}
	p := f.connectPlugin(t, "p")
	held := make(chan pluginRequest, 2)
	u := f.connectUI(t, held)
	id := pendingEndTestQuery(t, u, "held", "")
	old := <-held
	endTestQuery(t, u, id)
	f.requests <- old
	fence := pendingEndTestQuery(t, u, "fence", "")
	req := <-held
	if req.queryID != fence || req.end {
		t.Fatalf("end queued for a request that never reached the writer: %+v", req)
	}
	f.requests <- req
	expectPluginQueryFrame(t, p, wire.MsgRequest, fence)
}

type gatedQueryWriter struct {
	peerConn
	started chan struct{}
	release chan struct{}
}

func (p *gatedQueryWriter) Write(frame []byte) (int, error) {
	select {
	case <-p.release:
	default:
		close(p.started)
		<-p.release
		// The artificial gate is outside the transport's write deadline.
		_ = p.SetWriteDeadline(time.Now().Add(time.Second))
	}
	return p.peerConn.Write(frame)
}

func TestEndQueryDuringWrite(t *testing.T) {
	for _, name := range []string{"same connection", "replaced connection"} {
		t.Run(name, func(t *testing.T) {
			f := newAdmissionFixture(t, time.Second)
			f.configure("p", plugins.LifecycleDaemon, true, "")
			f.mgr.Plugins["p"].Config.Capabilities = []string{wire.MsgEnd}
			server, plugin := admissionSocket(t)
			writer := &gatedQueryWriter{server, make(chan struct{}), make(chan struct{})}
			release := sync.OnceFunc(func() { close(writer.release) })
			defer release()
			f.registry.set("p", writer)
			owner := f.connectUI(t, f.requests)
			other := f.connectUI(t, f.requests)
			id := pendingEndTestQuery(t, owner, "held write", "")
			select {
			case <-writer.started:
			case <-time.After(time.Second):
				t.Fatal("request did not reach writer")
			}
			if name == "same connection" {
				for range cap(f.requests) {
					f.requests <- pluginRequest{name: "p", conn: writer, queryID: "expired"}
				}
			}
			q := f.store.query(id)
			owner.send(t, wire.UIRequest{Type: wire.MsgEnd, QueryID: id})
			select {
			case <-q.ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("saturated writer blocked query cancellation")
			}
			endTestQuery(t, other, "unknown") // needs the store lock even while end is waiting for queue space
			if name == "replaced connection" {
				var newServer peerConn
				newServer, plugin = admissionSocket(t)
				f.registry.set("p", newServer)
			}
			release()
			var receipt wire.AckMessage
			owner.read(t, &receipt)
			if receipt.Type != "ok" || receipt.QueryID != id {
				t.Fatalf("invalid end receipt: %+v", receipt)
			}
			if name == "same connection" {
				expectPluginQueryFrame(t, plugin, wire.MsgRequest, id)
				expectPluginQueryFrame(t, plugin, wire.MsgEnd, id)
			}
			fence := pendingEndTestQuery(t, owner, "fence", "")
			expectPluginQueryFrame(t, plugin, wire.MsgRequest, fence)
		})
	}
}

func TestEndQueryCancelsOnCallQueryButNotSelect(t *testing.T) {
	f := newAdmissionFixture(t, 3*time.Second)
	b := testBus(t, t.Context())
	f.registry.bus = b
	dir, actionDir := t.TempDir(), t.TempDir()
	gate := openOnCallTestGate(t, actionDir)
	query := writeScript(t, dir, "query", "#!/bin/sh\necho $$ > \"${0%/*}/pid\"\nexec sleep 30\n")
	action := writeScript(t, actionDir, "action", `#!/bin/sh
cd "${0%/*}" || exit 1
echo $$ > pid
read -r value < gate
echo "$value" > state
`)
	f.configure("slow", plugins.LifecycleOnCall, true, query)
	f.configure("action", plugins.LifecycleOnCall, true, action)
	f.mgr.Plugins["action"].Config.ProvidesGeneral = false
	u := f.connectUI(t, f.requests)
	id := pendingEndTestQuery(t, u, "slow query", "")
	pid := onCallChildPID(t, dir)
	ack := u.selectRow(t, wire.UIRequest{Plugin: "action", QueryID: id, ResultID: "row"})
	onCallChildPID(t, actionDir)
	endTestQuery(t, u, id)
	if _, err := gate.WriteString("committed\n"); err != nil {
		t.Fatal(err)
	}
	u.outcome(t, ack, true, "")
	if data, err := os.ReadFile(filepath.Join(actionDir, "state")); err != nil || string(data) != "committed\n" {
		t.Fatalf("select did not finish after end: %q, %v", data, err)
	}
	// The ordinary query deadline is 3s; explicit end must reap it sooner.
	until := time.Now().Add(time.Second)
	for !errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
		if time.Now().After(until) {
			t.Fatalf("ended query process %d still running", pid)
		}
		time.Sleep(time.Millisecond)
	}
	u.expectStatus(t)
}
