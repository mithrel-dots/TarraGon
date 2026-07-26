package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

func persistentFixture(t *testing.T, tagged bool, timeout time.Duration) *admissionFixture {
	t.Helper()
	f := newAdmissionFixture(t, timeout)
	f.configure("p", plugins.LifecycleDaemon, true, "")
	if tagged {
		f.mgr.Plugins["p"].Config.Capabilities = []string{"action_id"}
	}
	f.registry.manager = f.mgr
	return f
}

// A synchronous pipe on the plugin side lets the test establish when the
// reader has finished processing a reply, including a reply that was dropped.
func persistentPlugin(t *testing.T, f *admissionFixture, name string) admissionPeer {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { client.Close() })
	go handlePluginConn(t.Context(), server, f.registry, f.store, f.ui)
	peer := admissionPeer{Conn: client, scanner: wire.NewScanner(client)}
	peer.send(t, wire.PluginHello{Type: wire.MsgHello, Name: name})
	peer.readBarrier(t)
	return peer
}

func (p admissionPeer) readBarrier(t *testing.T) {
	t.Helper()
	// net.Pipe doesn't buffer writes. The reader can't consume this line
	// until it has finished the preceding hello, final or progress frame.
	p.send(t, map[string]string{"type": "test_barrier"})
}

func click(row string) wire.UIRequest {
	return wire.UIRequest{Plugin: "p", ResultID: row, RequestID: row, Action: "open"}
}

func (p admissionPeer) expectSelect(t *testing.T, row, id string) {
	t.Helper()
	var req wire.SelectRequest
	p.read(t, &req)
	if req.Type != wire.MsgSelect || req.ResultID != row || req.ActionID != id || req.Action != "open" || req.QueryID != "" {
		t.Fatalf("want select row=%q, action_id=%q without a query; got %+v", row, id, req)
	}
}

func (p admissionPeer) reply(t *testing.T, id, message string, success bool) {
	t.Helper()
	p.send(t, wire.SelectResponse{Type: wire.MsgSelectResponse, ActionID: id, Message: message, Success: success})
}

// A complete query is also a barrier on the plugin writer: any accidental
// extra select ahead of it fails the exact frame check.
func checkPersistentQuery(t *testing.T, f *admissionFixture, p admissionPeer) {
	t.Helper()
	ui := f.connectUI(t, f.requests)
	ui.send(t, wire.UIRequest{Type: "query", Text: "still searchable"})
	var ack wire.AckMessage
	ui.read(t, &ack)
	var req wire.PluginRequest
	p.read(t, &req)
	if req.Type != wire.MsgRequest || req.QueryID != ack.QueryID || req.Text != "still searchable" {
		t.Fatalf("expected only a query on the plugin writer, got %+v", req)
	}
	p.send(t, wire.PluginResponse{Type: wire.MsgResponse, QueryID: req.QueryID, Data: json.RawMessage(`["here"]`)})
	for {
		var update wire.UpdateMessage
		ui.read(t, &update)
		var snap aggregate
		if update.QueryID != req.QueryID || json.Unmarshal(update.Payload, &snap) != nil {
			t.Fatalf("unexpected query update: %+v", update)
		}
		if snap.Plugins["p"].State != "pending" {
			if len(snap.List) != 1 || snap.List[0].ID != "here" {
				t.Fatalf("query failed: %+v", snap)
			}
			return
		}
	}
}

func TestPersistentLegacyTimeout(t *testing.T) {
	for _, recovery := range []string{"late final", "replacement"} {
		t.Run(recovery, func(t *testing.T) {
			const timeout = 300 * time.Millisecond
			f := persistentFixture(t, false, timeout)
			plugin := persistentPlugin(t, f, "p")
			first, second := f.connectUI(t, f.requests), f.connectUI(t, f.requests)
			a := first.selectRow(t, click("a"))
			plugin.expectSelect(t, "a", "")
			// Leave B a distinct receipt deadline. Its rejection must be due
			// to A's unanswered slot, not expiry of B's own timer.
			time.Sleep(timeout / 3)
			b := second.selectRow(t, click("b"))
			if r := first.outcome(t, a, false, "completion_unknown"); !strings.Contains(r.Message, "may already have occurred") {
				t.Fatal(r)
			}
			if r := second.outcome(t, b, false, "not_run"); r.Message != unansweredAction {
				t.Fatal(r)
			}
			blocked := first.selectRow(t, click("blocked"))
			if r := first.outcome(t, blocked, false, "not_run"); r.Message != unansweredAction {
				t.Fatal(r)
			}
			checkPersistentQuery(t, f, plugin)
			// A query reply does not drain the unanswered action slot.
			stillBlocked := second.selectRow(t, click("still blocked"))
			second.outcome(t, stillBlocked, false, "not_run")
			if recovery == "late final" {
				// A click admitted during quarantine can also still be in
				// the shared router queue when the late reply arrives.
				held := make(chan pluginRequest, 1)
				waiter := f.connectUI(t, held)
				beforeDrain := waiter.selectRow(t, click("delayed"))
				delayed := <-held
				plugin.reply(t, "", "late a", true)
				plugin.readBarrier(t)
				f.requests <- delayed
				if r := waiter.outcome(t, beforeDrain, false, "not_run"); r.Message != unansweredAction {
					t.Fatal(r)
				}
			} else {
				plugin = persistentPlugin(t, f, "p")
			}
			c := second.selectRow(t, click("c"))
			plugin.expectSelect(t, "c", "")
			plugin.reply(t, "", "fresh c", true)
			if r := second.outcome(t, c, true, ""); r.Message != "fresh c" {
				t.Fatal(r)
			}
			first.expectStatus(t)
			second.expectStatus(t)
		})
	}
}

func TestPersistentLegacyQueueIsBoundedAndFIFO(t *testing.T) {
	f := persistentFixture(t, false, 5*time.Second)
	plugin := persistentPlugin(t, f, "p")
	first, second := f.connectUI(t, f.requests), f.connectUI(t, f.requests)
	a := first.selectRow(t, click("a"))
	plugin.expectSelect(t, "a", "")
	queued := make([]wire.SelectAck, maxPersistentActions)
	for i := range queued {
		queued[i] = second.selectRow(t, click(fmt.Sprintf("queued-%02d", i)))
	}
	full := second.selectRow(t, click("overflow"))
	if r := second.outcome(t, full, false, "not_run"); !strings.Contains(r.Message, "full") {
		t.Fatal(r)
	}
	plugin.reply(t, "", "done a", true)
	first.outcome(t, a, true, "")
	for _, ack := range queued {
		plugin.expectSelect(t, ack.RequestID, "")
		plugin.reply(t, "", ack.RequestID, true)
		if r := second.outcome(t, ack, true, ""); r.Message != ack.RequestID {
			t.Fatal(r)
		}
	}
	checkPersistentQuery(t, f, plugin)
}

func TestPersistentLegacyQueuedExpiry(t *testing.T) {
	f := persistentFixture(t, false, 2*time.Second)
	p := persistentPlugin(t, f, "p")
	active := f.connectUI(t, f.requests)
	a := active.selectRow(t, click("active"))
	p.expectSelect(t, "active", "")

	// Hold an older UI's request on its way to the router. Give it a
	// shorter receipt deadline than A to exercise expiry in the FIFO.
	held := make(chan pluginRequest, 1)
	other := f.connectUI(t, held)
	b := other.selectRow(t, click("older"))
	req := <-held
	req.selectAction.mu.Lock()
	req.selectAction.deadline = time.Now().Add(150 * time.Millisecond)
	req.selectAction.timer.Reset(time.Until(req.selectAction.deadline))
	req.selectAction.mu.Unlock()
	f.requests <- req
	c := active.selectRow(t, click("next"))
	checkPersistentQuery(t, f, p)
	other.outcome(t, b, false, "not_run")

	p.reply(t, "", "active", true)
	active.outcome(t, a, true, "")
	p.expectSelect(t, "next", "")
	p.reply(t, "", "next", true)
	active.outcome(t, c, true, "")
	other.expectStatus(t)
}

func TestPersistentPinnedRequestAfterReplacement(t *testing.T) {
	f := persistentFixture(t, false, time.Second)
	persistentPlugin(t, f, "p")
	held := make(chan pluginRequest, 1)
	ui := f.connectUI(t, held)
	a := ui.selectRow(t, click("old target"))
	req := <-held
	replacement := persistentPlugin(t, f, "p")
	f.requests <- req
	ui.outcome(t, a, false, "not_run")
	checkPersistentQuery(t, f, replacement)
	ui.expectStatus(t)
}

// Keep the test reader alive across replacement to model an old frame that
// was already buffered in the transport when the old peer was retired.
type bufferedRetiredPeer struct{ net.Conn }

func (p bufferedRetiredPeer) Close() error { return nil }

func TestPersistentRetiredReaderCannotMatchReplacement(t *testing.T) {
	for _, tagged := range []bool{false, true} {
		t.Run(fmt.Sprintf("tagged=%v", tagged), func(t *testing.T) {
			f := persistentFixture(t, tagged, time.Second)
			server, client := net.Pipe()
			defer client.Close()
			defer server.Close()
			old := admissionPeer{Conn: client, scanner: wire.NewScanner(client)}
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				handlePluginConn(t.Context(), bufferedRetiredPeer{server}, f.registry, f.store, f.ui)
			}()
			old.send(t, wire.PluginHello{Type: wire.MsgHello, Name: "p"})
			old.readBarrier(t)
			owner := f.connectUI(t, f.requests)
			newPlugin := persistentPlugin(t, f, "p")
			a := owner.selectRow(t, click("replacement"))
			var id string
			if tagged {
				id = a.ActionID
			}
			newPlugin.expectSelect(t, "replacement", id)
			old.reply(t, id, "wrong session", false)
			old.Close()
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("old reader did not retire")
			}
			newPlugin.reply(t, id, "right session", true)
			if r := owner.outcome(t, a, true, ""); r.Message != "right session" {
				t.Fatal(r)
			}
			owner.expectStatus(t)
		})
	}
}

func TestPersistentTaggedReplyMatching(t *testing.T) {
	f := persistentFixture(t, true, time.Second)
	f.configure("other", plugins.LifecycleDaemon, true, "")
	f.mgr.Plugins["other"].Config.Capabilities = []string{"action_id"}
	other := persistentPlugin(t, f, "other")
	plugin := persistentPlugin(t, f, "p")
	first, second := f.connectUI(t, f.requests), f.connectUI(t, f.requests)
	a := first.selectRow(t, click("a"))
	plugin.expectSelect(t, "a", a.ActionID)
	// An effective manifest change must not reinterpret this live connection.
	f.mgr.Lock()
	f.mgr.Plugins["p"].Config.Capabilities = nil
	f.mgr.Unlock()
	b := second.selectRow(t, click("b"))
	plugin.expectSelect(t, "b", b.ActionID)
	other.reply(t, b.ActionID, "wrong connection", true)
	other.readBarrier(t)
	plugin.reply(t, "", "untagged", true)
	plugin.reply(t, "unknown", "unmatched", true)
	plugin.reply(t, b.ActionID, "b failed", false)
	if r := second.outcome(t, b, false, ""); r.Message != "b failed" {
		t.Fatal(r)
	}
	plugin.reply(t, b.ActionID, "duplicate", true)
	plugin.reply(t, a.ActionID, "a succeeded", true)
	if r := first.outcome(t, a, true, ""); r.Message != "a succeeded" {
		t.Fatal(r)
	}
	second.expectStatus(t)
	first.expectStatus(t)
	plugin = persistentPlugin(t, f, "p")
	c := second.selectRow(t, click("c"))
	plugin.expectSelect(t, "c", "")
	plugin.reply(t, "", "new legacy connection", true)
	second.outcome(t, c, true, "")
}

func TestPersistentTaggedAdmissionBound(t *testing.T) {
	f := persistentFixture(t, true, 5*time.Second)
	p := persistentPlugin(t, f, "p")
	ui := f.connectUI(t, f.requests)
	var accepted []wire.SelectAck
	for i := range maxPersistentActions {
		ack := ui.selectRow(t, click(fmt.Sprintf("row-%d", i)))
		p.expectSelect(t, ack.RequestID, ack.ActionID)
		accepted = append(accepted, ack)
	}
	full := ui.selectRow(t, click("full"))
	ui.outcome(t, full, false, "not_run")
	for _, ack := range accepted {
		p.reply(t, ack.ActionID, "done", true)
		ui.outcome(t, ack, true, "")
	}
	checkPersistentQuery(t, f, p)
	ui.expectStatus(t)
}

func TestPersistentConnectionLoss(t *testing.T) {
	for _, lost := range []string{"exit", "replacement"} {
		t.Run(lost, func(t *testing.T) {
			f := persistentFixture(t, false, 2*time.Second)
			p := persistentPlugin(t, f, "p")
			first, second := f.connectUI(t, f.requests), f.connectUI(t, f.requests)
			a := first.selectRow(t, click("sent"))
			p.expectSelect(t, "sent", "")
			b := second.selectRow(t, click("waiting"))
			// A query on the same router proves the previously admitted B
			// remained queued, even though queries can still flow.
			second.expectStatus(t)
			checkPersistentQuery(t, f, p)
			if lost == "exit" {
				p.Close()
			} else {
				persistentPlugin(t, f, "p")
			}
			first.outcome(t, a, false, "completion_unknown")
			second.outcome(t, b, false, "not_run")
			second.expectStatus(t)
			first.expectStatus(t)
		})
	}
}

type failingSelectPeer struct {
	net.Conn
	beforeWrite bool
	n           int
	err         error
}

func (p *failingSelectPeer) SetWriteDeadline(t time.Time) error {
	if p.beforeWrite {
		return net.ErrClosed
	}
	return p.Conn.SetWriteDeadline(t)
}

func (p *failingSelectPeer) Write([]byte) (int, error) {
	return p.n, p.err
}

func TestPersistentWriteFailure(t *testing.T) {
	for _, tc := range []struct {
		name, code  string
		beforeWrite bool
		n           int
		err         error
	}{
		{name: "before write", code: "not_run", beforeWrite: true},
		{name: "zero-byte error", code: "not_run", err: net.ErrClosed},
		{name: "zero-byte short write", code: "not_run"},
		{name: "partial write", code: "completion_unknown", n: 1, err: errors.New("injected partial write")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := persistentFixture(t, false, time.Second)
			server, client := net.Pipe()
			defer client.Close()
			f.registry.set("p", &failingSelectPeer{server, tc.beforeWrite, tc.n, tc.err})
			ui := f.connectUI(t, f.requests)
			a := ui.selectRow(t, click("a"))
			ui.outcome(t, a, false, tc.code)
			ui.expectStatus(t)
			if f.registry.isConnected("p") {
				t.Fatal("failed connection still registered")
			}
		})
	}
}

type blockedSelectPeer struct {
	net.Conn
	started, release, retired chan struct{}
}

func (p *blockedSelectPeer) Write([]byte) (int, error) {
	close(p.started)
	<-p.release
	return 0, net.ErrClosed
}

func (p *blockedSelectPeer) Close() error {
	close(p.retired)
	return p.Conn.Close()
}

func TestPersistentWriteTimeoutRace(t *testing.T) {
	f := persistentFixture(t, false, 150*time.Millisecond)
	server, client := net.Pipe()
	defer client.Close()
	p := &blockedSelectPeer{server, make(chan struct{}), make(chan struct{}), make(chan struct{})}
	f.registry.set("p", p)
	owner := f.connectUI(t, f.requests)
	ack := owner.selectRow(t, click("a"))
	select {
	case <-p.started:
	case <-time.After(time.Second):
		t.Fatal("router did not begin write")
	}
	// Whether the pending write accepted bytes is still unknown at expiry.
	owner.outcome(t, ack, false, "completion_unknown")
	close(p.release)
	select {
	case <-p.retired:
	case <-time.After(time.Second):
		t.Fatal("failed writer was not retired")
	}
	owner.expectStatus(t)
}
