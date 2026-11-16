package daemon

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
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
		if p.Conn == pending.Conn || p.Conn == live.Conn {
			watchFrame(t, p, first, wire.MsgResponse, `["late live"]`, true, false)
		} else {
			respond(p, first)
		}
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

func TestEndQueryBeforeFirstLiveResponse(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.configure("p", plugins.LifecycleDaemon, true, "")
	p := persistentPlugin(t, f, "p")
	u := f.connectUI(t, f.requests)
	removed := func(text string) string {
		t.Helper()
		id := pendingEndTestQuery(t, u, text, "")
		expectPluginQueryFrame(t, p, wire.MsgRequest, id)
		endTestQuery(t, u, id)
		if f.store.query(id) != nil {
			t.Fatal("released response before query removal")
		}
		return id
	}
	live, ordinary := removed("late live"), removed("late ordinary")
	watchFrame(t, p, ordinary, wire.MsgResponse, `["ordinary"]`, false, false)
	watchFrame(t, p, ordinary, wire.MsgResponse, `["unsolicited"]`, true, false)
	watchFrame(t, p, live, wire.MsgLiveUpdate, `["push is not an opt-in"]`, true, false)
	p.readBarrier(t)
	fence := pendingEndTestQuery(t, u, "before opt-in", "")
	expectPluginQueryFrame(t, p, wire.MsgRequest, fence)

	watchFrame(t, p, live, wire.MsgResponse, `["started late"]`, true, false)
	expectPluginQueryFrame(t, p, wire.MsgEnd, live)
	watchFrame(t, p, live, wire.MsgResponse, `["duplicate"]`, true, false)
	p.readBarrier(t)
	fence = pendingEndTestQuery(t, u, "after stop", "")
	expectPluginQueryFrame(t, p, wire.MsgRequest, fence)
}

func TestLateLiveResponseCannotStopReplacement(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.configure("p", plugins.LifecycleDaemon, true, "")
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close() })
	t.Cleanup(func() { client.Close() })
	old := admissionPeer{Conn: client, scanner: wire.NewScanner(client)}
	go handlePluginConn(t.Context(), bufferedRetiredPeer{server}, f.registry, f.store, f.ui)
	old.send(t, wire.PluginHello{Type: wire.MsgHello, Name: "p"})
	old.readBarrier(t)
	u := f.connectUI(t, f.requests)
	id := pendingEndTestQuery(t, u, "original connection", "")
	expectPluginQueryFrame(t, old, wire.MsgRequest, id)
	endTestQuery(t, u, id)

	replacement := persistentPlugin(t, f, "p")
	watchFrame(t, old, id, wire.MsgResponse, `["buffered old"]`, true, false)
	old.readBarrier(t)
	watchFrame(t, replacement, id, wire.MsgResponse, `["unsolicited replacement"]`, true, false)
	replacement.readBarrier(t)
	fence := pendingEndTestQuery(t, u, "replacement fence", "")
	expectPluginQueryFrame(t, replacement, wire.MsgRequest, fence)
	old.expectNoWrite(t)
}

func TestLateLiveResponseWhileTerminalWatchPublishes(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.configure("p", plugins.LifecycleDaemon, true, "")
	p := persistentPlugin(t, f, "p")
	u := f.connectUI(t, f.requests)
	id := watchQuery(t, u, true)
	watchRequest(t, p, id)
	refreshSnapshot(t, func(v any) { u.read(t, v) }, id, func(q aggregate) bool { return q.Plugins["p"].State == "pending" })
	f.store.WaitForPublications()
	q := f.store.query(id)
	publishing, release := make(chan struct{}), make(chan struct{})
	done := sync.OnceFunc(func() { close(release) })
	defer done()
	go func() {
		q.mu.Lock()
		f.store.stopWatchLocked(q, "test budget error", func(update *wire.UpdateMessage, changed []string) {
			close(publishing)
			<-release
			f.ui.publishSnapshot(update, changed)
		})
	}()
	select {
	case <-publishing:
	case <-time.After(time.Second):
		t.Fatal("terminal publisher did not start")
	}
	if got := f.store.query(id); got != q || q.ctx.Err() == nil {
		t.Fatal("expected a canceled query still retained during terminal publication")
	}
	watchFrame(t, p, id, wire.MsgResponse, `["started late"]`, true, false)
	p.readBarrier(t)
	done()
	watchError(t, u, id, "test budget error")
	expectPluginQueryFrame(t, p, wire.MsgEnd, id)
	fence := pendingEndTestQuery(t, u, "writer fence", "")
	expectPluginQueryFrame(t, p, wire.MsgRequest, fence)
}

func TestEndQueryBackpressurePreservesPluginSession(t *testing.T) {
	f := newAdmissionFixture(t, 2*time.Second)
	f.configure("p", plugins.LifecycleDaemon, true, "")
	p := persistentPlugin(t, f, "p")
	u := f.connectUI(t, f.requests)
	owner := f.connectUI(t, f.requests)
	live := watchQuery(t, owner, true)
	watchRequest(t, p, live)
	watchFrame(t, p, live, wire.MsgResponse, `["live"]`, true, false)
	watchRows(t, owner, live, "live")
	selection := owner.selectRow(t, click("selected"))
	p.expectSelect(t, "selected", "")

	// Both requests are on the wire before the first removal reaches the limit.
	first := pendingEndTestQuery(t, u, "at limit", "")
	expectPluginQueryFrame(t, p, wire.MsgRequest, first)
	second := pendingEndTestQuery(t, u, "already dispatched", "")
	expectPluginQueryFrame(t, p, wire.MsgRequest, second)
	session := f.registry.session("p")
	for i := range maxUnansweredRemovedQueries - 1 {
		session.rememberRemovedQuery(strconv.Itoa(i))
	}
	endTestQuery(t, u, first)
	endTestQuery(t, u, second)
	expectRejected := func() {
		t.Helper()
		id := pendingEndTestQuery(t, u, "backpressure", "")
		refreshSnapshot(t, func(v any) { u.read(t, v) }, id, func(q aggregate) bool {
			return q.Plugins["p"].Error == errUnansweredRemovedQueries.Error()
		})
	}
	expectRejected()

	p.reply(t, "", "completed", true)
	owner.outcome(t, selection, true, "")
	watchFrame(t, p, live, wire.MsgLiveUpdate, `["still live"]`, false, false)
	watchRows(t, owner, live, "still live")

	watchFrame(t, p, second, wire.MsgResponse, `["late opt-in"]`, true, false)
	expectPluginQueryFrame(t, p, wire.MsgEnd, second)
	watchFrame(t, p, second, wire.MsgResponse, `["duplicate"]`, true, false)
	p.readBarrier(t)
	expectRejected() // this reply cleared only one ID, leaving the count at the limit

	watchFrame(t, p, first, wire.MsgResponse, `["ordinary reply"]`, false, false)
	p.readBarrier(t)
	recovered := pendingEndTestQuery(t, u, "recovered", "")
	expectPluginQueryFrame(t, p, wire.MsgRequest, recovered)
	watchFrame(t, p, recovered, wire.MsgResponse, `["recovered"]`, false, false)
	watchRows(t, u, recovered, "recovered")
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
