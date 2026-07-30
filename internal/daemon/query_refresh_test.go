package daemon

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/nats-io/nats.go"
)

func refreshSnapshot(t *testing.T, read func(any), qid string, ready func(aggregate) bool) aggregate {
	t.Helper()
	for {
		var update wire.UpdateMessage
		read(&update)
		if update.Type != "update" || update.QueryID != qid {
			t.Fatalf("expected an update for %s, got %s for %s", qid, update.Type, update.QueryID)
		}
		var snap aggregate
		if err := json.Unmarshal(update.Payload, &snap); err != nil {
			t.Fatal(err)
		}
		if ready(snap) {
			return snap
		}
	}
}

func TestProducerRefreshUnixAndNative(t *testing.T) {
	dir := t.TempDir()
	// Record each call at the executables themselves: a snapshot may coalesce
	// any number of responses and cannot be used as a dispatch counter.
	marked := writeScript(t, dir, "marked", `#!/bin/sh
cd "$(dirname "$0")" || exit 1
echo "$1:$2:$#:$3:$4" >> marked.log
case "$2" in
  query)
    label=Before
    [ ! -f changed ] || label=After
    printf '[{"id":"row","label":"%s","actions":[{"name":"mark","type":"keep_open"}]}]\n' "$label"
    ;;
  select)
    touch changed
    # Valid stdout under 8 MiB; JSON-escaping the message exceeds the broker limit.
    printf '{"success":true,"message":"'
    head -c 1500000 /dev/zero | tr '\000' '<'
    printf '","next":{"type":"refresh"}}\n'
    ;;
esac
`)
	other := writeScript(t, dir, "other", `#!/bin/sh
cd "$(dirname "$0")" || exit 1
echo "$1:$2:$#:$3:$4" >> other.log
echo '["unrelated"]'
`)
	f := newAdmissionFixture(t, 2*time.Second)
	f.configure("marked", plugins.LifecycleOnCall, true, marked)
	f.configure("other", plugins.LifecycleOnCall, true, other)
	b := testBus(t, t.Context())
	f.registry.bus = b
	handleUI := func(c peerConn) { handleUIClient(b.ctx, c, f.mgr, f.requests, f.registry, f.store, f.ui, nil) }
	if err := b.listenNative(handleUI, nil); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "ipc", "u")
	ln, err := b.listenUnix(path, handleUI)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	socket, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	unix := admissionPeer{Conn: socket, scanner: wire.NewScanner(socket)}
	readUnix := func(v any) { unix.read(t, v) }

	nc, err := nats.Connect(nats.DefaultURL, nats.InProcessServer(b.server))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	session, err := nc.Request("tarragon.ui.open", nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	base := string(session.Data)
	sub, err := nc.SubscribeSync(base + ".out")
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	readNative := func(v any) {
		t.Helper()
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(msg.Data, v); err != nil {
			t.Fatal(err)
		}
	}
	complete := func(q aggregate) bool {
		return q.Plugins["marked"].State == "done" && q.Plugins["other"].State == "done"
	}
	if err := nc.Publish(base+".in", []byte(`{"type":"query","text":"B terms"}`)); err != nil {
		t.Fatal(err)
	}
	var bAck wire.AckMessage
	readNative(&bAck)
	if bAck.Type != "ack" || bAck.QueryID == "" {
		t.Fatalf("bad native ack %+v", bAck)
	}
	refreshSnapshot(t, readNative, bAck.QueryID, complete)
	unix.send(t, wire.UIRequest{Type: "query", Text: "A terms"})
	var aAck wire.AckMessage
	unix.read(t, &aAck)
	if aAck.Type != "ack" || aAck.QueryID == "" || aAck.QueryID == bAck.QueryID {
		t.Fatalf("bad Unix ack %+v", aAck)
	}
	before := refreshSnapshot(t, readUnix, aAck.QueryID, complete)
	if !slices.ContainsFunc(before.List, func(row wire.ResultItem) bool {
		return row.Plugin == "marked" && row.ID == "row" && row.Label == "Before" &&
			len(row.Actions) == 1 && row.Actions[0].Type == wire.ActionTypeKeepOpen
	}) {
		t.Fatalf("keep-open row not offered: %+v", before.List)
	}
	ack := unix.selectRow(t, wire.UIRequest{QueryID: aAck.QueryID, Plugin: "marked", ResultID: "row", Action: "mark"})
	if resp := unix.outcome(t, ack, true, ""); resp.Next != nil || resp.Message != oversizedActionMessage {
		t.Fatal("expected a shortened terminal message with the refresh consumed by the daemon")
	}
	after := refreshSnapshot(t, readUnix, aAck.QueryID, complete)
	if !slices.ContainsFunc(after.List, func(row wire.ResultItem) bool { return row.Plugin == "marked" && row.Label == "After" }) ||
		!slices.ContainsFunc(after.List, func(row wire.ResultItem) bool { return row.Plugin == "other" && row.ID == "unrelated" }) {
		t.Fatalf("wrong producer replacements: %+v", after.List)
	}
	for file, want := range map[string]string{
		"marked.log": "tarragon:query:3:B terms:\ntarragon:query:3:A terms:\ntarragon:select:4:row:mark\ntarragon:query:3:A terms:\n",
		"other.log":  "tarragon:query:3:B terms:\ntarragon:query:3:A terms:\n",
	} {
		data, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil || string(data) != want {
			t.Fatalf("%s got %q (%v), want %q", file, data, err, want)
		}
	}
	f.store.WaitForPublications()
	if err := nc.Publish(base+".in", []byte(`{"type":"status"}`)); err != nil {
		t.Fatal(err)
	}
	var status wire.StatusResponse
	readNative(&status)
	if status.Type != "status" {
		t.Fatalf("native observer received foreign %q instead of status", status.Type)
	}
}

func TestPersistentRefreshCoalescesAndStopsAtSupersession(t *testing.T) {
	for _, supersede := range []bool{false, true} {
		name := "followup"
		if supersede {
			name = "supersede"
		}
		t.Run(name, func(t *testing.T) {
			f := newAdmissionFixture(t, time.Second)
			f.configure("p", plugins.LifecycleDaemon, true, "")
			f.mgr.Plugins["p"].Config.ResolvedPrefix = "@old"
			p := f.connectPlugin(t, "p")
			u := f.connectUI(t, f.requests)
			readUI := func(v any) { u.read(t, v) }
			query := func(input string) string {
				t.Helper()
				u.send(t, wire.UIRequest{Type: "query", Text: input, QueryStream: "typing"})
				var ack wire.AckMessage
				u.read(t, &ack)
				if ack.Type != "ack" {
					t.Fatalf("expected query ack, got %+v", ack)
				}
				return ack.QueryID
			}
			expectQuery := func(id, text string) {
				t.Helper()
				var req wire.PluginRequest
				p.read(t, &req)
				if req.Type != wire.MsgRequest || req.QueryID != id || req.Text != text {
					t.Fatalf("wrong replay: %+v; want %s %q", req, id, text)
				}
			}
			replyQuery := func(id, data string) {
				t.Helper()
				p.send(t, wire.PluginResponse{Type: wire.MsgResponse, QueryID: id, Data: json.RawMessage(data)})
			}
			id := query("@old  literal input")
			expectQuery(id, "literal input")
			replyQuery(id, `["row"]`)
			refreshSnapshot(t, readUI, id, func(q aggregate) bool { return q.Plugins["p"].State == "done" })
			f.mgr.Lock()
			f.mgr.Plugins["p"].Config.ResolvedPrefix = "@new"
			f.mgr.Unlock()
			refresh := func() {
				t.Helper()
				ack := u.selectRow(t, wire.UIRequest{QueryID: id, Plugin: "p", ResultID: "row"})
				var sel wire.SelectRequest
				p.read(t, &sel)
				if sel.Type != wire.MsgSelect || sel.QueryID != id || sel.ResultID != "row" {
					t.Fatalf("expected one select; got %+v", sel)
				}
				p.send(t, wire.SelectResponse{Type: wire.MsgSelectResponse, Success: false, Message: "partial", Next: &wire.ActionNext{Type: "refresh"}})
				u.outcome(t, ack, false, "")
			}
			refresh()
			expectQuery(id, "literal input")
			for range 3 {
				refresh()
			}
			if supersede {
				newID := query("@new another")
				expectQuery(newID, "another")
				replyQuery(id, `["stale"]`)
				replyQuery(newID, `["new"]`)
				snap := refreshSnapshot(t, readUI, newID, func(q aggregate) bool { return q.Plugins["p"].State == "done" })
				if len(snap.List) != 1 || snap.List[0].ID != "new" {
					t.Fatalf("stale results reached new stream: %+v", snap.List)
				}
			} else {
				replyQuery(id, `["updated"]`)
				expectQuery(id, "literal input")
				replyQuery(id, `[]`)
				snap := refreshSnapshot(t, readUI, id, func(q aggregate) bool { return q.Plugins["p"].State == "empty" })
				if len(snap.List) != 0 {
					t.Fatalf("empty replay did not clear the producer: %+v", snap.List)
				}
			}
			// A new query on the same writer fences any unwanted extra replay.
			expectQuery(query("@new fence"), "fence")
		})
	}
}

func TestRefreshRequiresOwnedOfferedRowAndConsent(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.configure("p", plugins.LifecycleDaemon, true, "")
	server, client := net.Pipe()
	defer client.Close()
	f.registry.set("p", server)
	defer f.registry.remove("p", server)
	out := make(chan pluginRequest, 1)
	f.store.startQuery(t.Context(), "q", "label", "text", "owner", "")
	dispatchQuery(t.Context(), "text", "q", f.mgr, out, f.registry, f.store, f.ui, false, "")
	req := <-out
	replacement := wire.PluginResponse{QueryID: "q", Data: json.RawMessage(`["row","extra"]`)}
	f.store.receiveQuery("p", server, replacement)
	if f.store.offeredRow("owner", "q", "p", "extra") {
		t.Fatal("accepted rows before the first query write")
	}
	if !f.store.beginQueryWrite(req, server) {
		t.Fatal("initial write lost")
	}
	premature := &selectAction{owner: "owner", queryID: "q", resultID: "row", producer: "p", handler: "p"}
	premature.offered = f.store.offeredRow("owner", "q", "p", "row")
	f.store.receiveQuery("p", server, wire.PluginResponse{QueryID: "q", Data: json.RawMessage(`["row"]`)})
	f.store.refreshAction(premature) // The row did not exist at receipt.
	q := f.store.query("q")
	check := func(owner, id, row, handler string, accepted bool) {
		t.Helper()
		a := &selectAction{owner: owner, queryID: id, resultID: row, producer: "p", handler: handler}
		a.offered = f.store.offeredRow(owner, id, "p", row)
		f.store.refreshAction(a)
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.producers["p"].outstanding != accepted {
			t.Fatalf("unexpected dispatch for %q/%q/%q from %q", owner, id, row, handler)
		}
	}
	check("foreign", "q", "row", "p", false)
	check("owner", "unknown", "row", "p", false)
	check("owner", "q", "not offered", "p", false)
	check("owner", "q", "row", "consumer", false)
	f.mgr.Lock()
	f.mgr.Plugins["p"].Config.Enabled = false
	f.mgr.Unlock()
	check("owner", "q", "row", "p", false)
	f.mgr.Lock()
	f.mgr.Plugins["p"].Config.Enabled = true
	f.mgr.Plugins["p"].Config.Capabilities = []string{"replay"}
	f.mgr.Unlock()
	check("owner", "q", "row", "consumer", true)
	select {
	case req = <-out:
	case <-time.After(2 * time.Second):
		t.Fatal("consenting producer never received its refresh")
	}
	if req.conn != server || req.queryID != "q" || req.text != "text" {
		t.Fatalf("changed route: %+v", req)
	}
	f.store.receiveQuery("p", server, replacement)
	if !f.store.offeredRow("owner", "q", "p", "extra") {
		t.Fatal("lost incremental rows while refresh was queued")
	}
	q.mu.Lock()
	stillPending := q.producers["p"].outstanding
	q.mu.Unlock()
	if !stillPending {
		t.Fatal("incremental rows completed a refresh before its write")
	}
	if !f.store.beginQueryWrite(req, server) {
		t.Fatal("refresh write lost")
	}
	check("owner", "q", "row", "consumer", true)
	f.mgr.Lock()
	f.mgr.Plugins["p"].Config.Capabilities = nil
	f.mgr.Unlock()
	f.store.receiveQuery("p", server, wire.PluginResponse{QueryID: "q", Data: json.RawMessage(`["row"]`)})
	deadline := time.After(time.Second)
	for {
		select {
		case unexpected := <-out:
			t.Fatalf("dirty follow-up ignored revoked replay consent: %+v", unexpected)
		case <-deadline:
			t.Fatal("ineligible follow-up still outstanding")
		default:
		}
		q.mu.Lock()
		pending := q.producers["p"].outstanding
		q.mu.Unlock()
		if !pending {
			break
		}
		time.Sleep(time.Millisecond)
	}
	check("owner", "q", "row", "p", true)
	select {
	case req = <-out:
	case <-time.After(time.Second):
		t.Fatal("refresh did not reach the writer")
	}
	f.mgr.Lock()
	f.mgr.Plugins["p"].Config.Enabled = false
	f.mgr.Unlock()
	if f.store.beginQueryWrite(req, server) {
		t.Fatal("queued refresh was written after its producer was disabled")
	}
}

func TestRefreshRetiresReplacedProducer(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.configure("p", plugins.LifecycleDaemon, true, "")
	p := f.connectPlugin(t, "p")
	oldConn, _ := f.registry.getConn("p")
	u := f.connectUI(t, f.requests)
	readUI := func(v any) { u.read(t, v) }
	u.send(t, wire.UIRequest{Type: "query", Text: "search"})
	var ack wire.AckMessage
	u.read(t, &ack)
	readQuery := func(peer admissionPeer) {
		t.Helper()
		var req wire.PluginRequest
		peer.read(t, &req)
		if req.Type != wire.MsgRequest || req.QueryID != ack.QueryID || req.Text != "search" {
			t.Fatalf("unrecognized dispatch: %+v", req)
		}
	}
	readQuery(p)
	reply := wire.PluginResponse{Type: wire.MsgResponse, QueryID: ack.QueryID, Data: json.RawMessage(`["original"]`)}
	p.send(t, reply)
	refreshSnapshot(t, readUI, ack.QueryID, func(q aggregate) bool { return q.Plugins["p"].State == "done" })
	q := f.store.query(ack.QueryID)
	a := &selectAction{owner: q.session, queryID: q.QueryID, producer: "p", resultID: "original", handler: "p", offered: true}
	f.store.refreshAction(a)
	readQuery(p)
	f.store.refreshAction(a) // pending on the old connection, dirty once
	replacement := f.connectPlugin(t, "p")
	readQuery(replacement)
	prior := refreshSnapshot(t, readUI, ack.QueryID, func(q aggregate) bool { return q.Plugins["p"].State == "error" })
	if len(prior.List) != 1 || prior.List[0].ID != "original" {
		t.Fatalf("lost old rows at retirement: %+v", prior.List)
	}
	reply.Data = json.RawMessage(`["late"]`)
	f.store.receiveQuery("p", oldConn, reply) // buffered reply from retired connection
	if f.store.offeredRow(q.session, ack.QueryID, "p", "late") {
		t.Fatal("retired connection replaced the last valid rows")
	}
	reply.Data = json.RawMessage(`["replacement"]`)
	replacement.send(t, reply)
	now := refreshSnapshot(t, readUI, ack.QueryID, func(q aggregate) bool { return q.Plugins["p"].State == "done" })
	if len(now.List) != 1 || now.List[0].ID != "replacement" {
		t.Fatalf("replacement did not own the only reply slot: %+v", now.List)
	}
}

func TestOnCallRefreshWaitsForTimedOutInvocation(t *testing.T) {
	dir := t.TempDir()
	entry := writeScript(t, dir, "slow", `#!/bin/sh
cd "$(dirname "$0")" || exit 1
if mkdir initial 2>/dev/null; then
  echo '["row"]'
elif mkdir slow-started 2>/dev/null; then
  # Keep stdout open briefly *after* the request deadline kills the direct
  # child, so the responder is still in cmd.Wait when the NATS inbox expires.
  parent=$$
  (while kill -0 "$parent" 2>/dev/null; do sleep .01; done
   sleep .15
   touch drained) &
  exec sleep 10
else
  if [ -e drained ]; then echo retired; else echo overlap; fi > outcome.tmp
  mv outcome.tmp outcome
  echo '["row"]'
fi
`)
	f := newAdmissionFixture(t, time.Second)
	f.configure("p", plugins.LifecycleOnCall, true, entry)
	f.registry.bus = testBus(t, t.Context())
	f.store.startQuery(t.Context(), "q", "label", "terms", "owner", "")
	dispatchQuery(t.Context(), "terms", "q", f.mgr, f.requests, f.registry, f.store, f.ui, false, "")
	a := &selectAction{owner: "owner", producer: "p", handler: "p", resultID: "row", queryID: "q", offered: true}
	f.store.refreshAction(a)
	f.store.refreshAction(a)
	deadline := time.Now().Add(6 * time.Second)
	for {
		data, err := os.ReadFile(filepath.Join(dir, "outcome"))
		if err == nil {
			if string(data) != "retired\n" {
				t.Fatalf("dirty follow-up raced the timed-out invocation: %q", data)
			}
			return
		}
		if !os.IsNotExist(err) || time.Now().After(deadline) {
			t.Fatalf("did not observe the follow-up invocation: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRefreshInvalidOnCallJSONRetainsRows(t *testing.T) {
	dir := t.TempDir()
	entry := writeScript(t, dir, "p", `#!/bin/sh
cd "$(dirname "$0")" || exit 1
if mkdir first 2>/dev/null; then echo '["row"]'; else echo 'malformed'; fi
`)
	f := newAdmissionFixture(t, time.Second)
	f.configure("p", plugins.LifecycleOnCall, true, entry)
	f.registry.bus = testBus(t, t.Context())
	u := f.connectUI(t, f.requests)
	read := func(v any) { u.read(t, v) }
	u.send(t, wire.UIRequest{Type: "query", Text: "terms"})
	var ack wire.AckMessage
	read(&ack)
	refreshSnapshot(t, read, ack.QueryID, func(q aggregate) bool { return q.Plugins["p"].State == "done" })
	q := f.store.query(ack.QueryID)
	f.store.refreshAction(&selectAction{owner: q.session, queryID: q.QueryID, resultID: "row", producer: "p", handler: "p", offered: true})
	snap := refreshSnapshot(t, read, ack.QueryID, func(q aggregate) bool { return q.Plugins["p"].State == "error" })
	if len(snap.List) != 1 || snap.List[0].ID != "row" || string(snap.Results["p"].Data) != `["row"]` {
		t.Fatalf("invalid JSON displaced the last valid result: %+v", snap)
	}
}
