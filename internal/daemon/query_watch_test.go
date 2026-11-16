package daemon

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

func watchQuery(t *testing.T, ui admissionPeer, watch bool) string {
	t.Helper()
	ui.send(t, wire.UIRequest{Type: "query", Plugin: "p", Watch: watch})
	var ack wire.AckMessage
	ui.read(t, &ack)
	if ack.Type != "ack" || ack.QueryID == "" {
		t.Fatalf("expected query ack, got %+v", ack)
	}
	return ack.QueryID
}

func watchRequest(t *testing.T, p admissionPeer, id string) {
	t.Helper()
	var req wire.PluginRequest
	p.read(t, &req)
	if req.Type != wire.MsgRequest || req.QueryID != id {
		t.Fatalf("expected request for %s, got %+v", id, req)
	}
}

func watchFrame(t *testing.T, p admissionPeer, id, typ, data string, live, final bool) {
	t.Helper()
	p.send(t, wire.PluginResponse{Type: typ, QueryID: id, Live: live, Final: final, Data: json.RawMessage(data)})
}

func watchRows(t *testing.T, ui admissionPeer, id, row string) aggregate {
	t.Helper()
	return refreshSnapshot(t, func(v any) { ui.read(t, v) }, id, func(q aggregate) bool {
		return len(q.List) == 1 && q.List[0].ID == row
	})
}

func watchError(t *testing.T, ui admissionPeer, id, want string) aggregate {
	t.Helper()
	snap := refreshSnapshot(t, func(v any) { ui.read(t, v) }, id, func(q aggregate) bool { return q.WatchError != "" })
	if !strings.Contains(snap.WatchError, want) {
		t.Fatalf("watch_error %q, want %q", snap.WatchError, want)
	}
	return snap
}

func TestWatchCountAndHistory(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.store.limit = 1
	f.store.watches.perOwner.count = 1
	f.store.watches.global.count = 2
	f.configure("p", plugins.LifecycleDaemon, true, "")
	p := f.connectPlugin(t, "p")
	a, b, c := f.connectUI(t, f.requests), f.connectUI(t, f.requests), f.connectUI(t, f.requests)
	first := watchQuery(t, a, true)
	watchRequest(t, p, first)
	// Consume the pending frame before sending another request on A.
	refreshSnapshot(t, func(v any) { a.read(t, v) }, first, func(q aggregate) bool { return q.Plugins["p"].State == "pending" })
	watchError(t, a, watchQuery(t, a, true), "owner watch count")
	second := watchQuery(t, b, true)
	watchRequest(t, p, second)
	watchError(t, c, watchQuery(t, c, true), "global watch count")
	// Legacy clients can display live pushes but never reserve capacity.
	legacy := watchQuery(t, c, false)
	watchRequest(t, p, legacy)
	watchFrame(t, p, legacy, wire.MsgResponse, `["legacy"]`, true, false)
	watchRows(t, c, legacy, "legacy")
	watchFrame(t, p, legacy, wire.MsgLiveUpdate, `["pushed"]`, false, false)
	watchRows(t, c, legacy, "pushed")
	newer := watchQuery(t, c, false)
	var end wire.AckMessage
	p.read(t, &end)
	if end.Type != wire.MsgEnd || end.QueryID != legacy || f.store.query(legacy) != nil {
		t.Fatalf("legacy subscription survived ordinary eviction: %+v", end)
	}
	watchRequest(t, p, newer)
	watchFrame(t, p, legacy, wire.MsgLiveUpdate, `["late"]`, false, false)
	watchFrame(t, p, newer, wire.MsgResponse, `["newer"]`, false, false)
	watchRows(t, c, newer, "newer")
	watchFrame(t, p, first, wire.MsgResponse, `["first"]`, true, false)
	watchRows(t, a, first, "first")
	watchFrame(t, p, second, wire.MsgResponse, `["ordinary"]`, false, false)
	watchRows(t, b, second, "ordinary")
	f.store.WaitForPublications()
	if totalWatchUsage(f.store).count != 1 || f.store.query(first) == nil || f.store.query(second) == nil || f.store.query(newer) != nil {
		t.Fatal("non-live completion did not return just B's watch to ordinary history")
	}
	b.expectStatus(t)
	c.expectStatus(t)
	a.send(t, wire.UIRequest{Type: wire.MsgEnd, QueryID: first})
	a.read(t, &end)
	if end.Type != "ok" || end.QueryID != first || totalWatchUsage(f.store) != (watchUsage{}) {
		t.Fatalf("explicit end leaked watch usage: %+v", end)
	}
	p.read(t, &end)
	if end.Type != wire.MsgEnd || end.QueryID != first {
		t.Fatalf("active subscription did not get end: %+v", end)
	}
}

func TestWatchByteLimits(t *testing.T) {
	for _, limit := range []string{"owner", "global"} {
		t.Run(limit, func(t *testing.T) {
			f := newAdmissionFixture(t, time.Second)
			// Either result fits on its own; the raw frames also fit together.
			// Two normalized replacements plus both raw payloads do not.
			if limit == "owner" {
				f.store.watches.perOwner.bytes = 1400
			} else {
				f.store.watches.global.bytes = 1400
			}
			f.configure("p", plugins.LifecycleDaemon, true, "")
			p := f.connectPlugin(t, "p")
			a := f.connectUI(t, f.requests)
			b := a
			if limit == "global" {
				b = f.connectUI(t, f.requests)
			}
			id := watchQuery(t, a, true)
			watchRequest(t, p, id)
			row := strings.Repeat("r", 220)
			data := fmt.Sprintf(`["%s"]`, row)
			watchFrame(t, p, id, wire.MsgResponse, data, true, false)
			watchRows(t, a, id, row)
			usage := totalWatchUsage(f.store)
			if usage.bytes < int64(len(data)+2*len(row)) {
				t.Fatalf("missing raw/normalized byte charges: %+v", usage)
			}
			watchFrame(t, p, id, wire.MsgLiveUpdate, data, false, false)
			watchRows(t, a, id, row)
			if got := totalWatchUsage(f.store); got != usage {
				t.Fatalf("charged a replacement twice: %+v (before %+v)", got, usage)
			}
			id2 := watchQuery(t, b, true)
			watchRequest(t, p, id2)
			watchFrame(t, p, id2, wire.MsgResponse, `["old"]`, true, false)
			watchRows(t, b, id2, "old")
			watchFrame(t, p, id2, wire.MsgLiveUpdate, data, false, false)
			snap := watchError(t, b, id2, limit+" watch byte")
			if len(snap.List) != 1 || snap.List[0].ID != "old" || string(snap.Results["p"].Data) != `["old"]` {
				t.Fatal("over-budget replacement destroyed the last valid snapshot")
			}
			var end wire.AckMessage
			p.read(t, &end)
			if end.Type != wire.MsgEnd || end.QueryID != id2 || f.store.query(id2) != nil || totalWatchUsage(f.store) != usage {
				t.Fatalf("wrong watch ended after byte exhaustion: %+v, usage %+v", end, totalWatchUsage(f.store))
			}
			watchFrame(t, p, id2, wire.MsgLiveUpdate, `["late"]`, false, false)
			watchFrame(t, p, id, wire.MsgLiveUpdate, `["small"]`, false, false)
			watchRows(t, a, id, "small")
			if got := totalWatchUsage(f.store); got.count != 1 || got.bytes >= usage.bytes {
				t.Fatalf("shrinking a live window did not return bytes: before %+v, after %+v", usage, got)
			}
			watchFrame(t, p, id, wire.MsgLiveUpdate, `["last"]`, false, true)
			watchRows(t, a, id, "last")
			f.store.WaitForPublications()
			if totalWatchUsage(f.store) != (watchUsage{}) || f.store.query(id) == nil {
				t.Fatal("producer's final push did not leave its rows in ordinary history")
			}
			b.expectStatus(t)
		})
	}
}

func TestWatchChargesRawDataWithNoNormalizedRows(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.store.watches.perOwner.bytes = 1024
	f.configure("p", plugins.LifecycleDaemon, true, "")
	p := f.connectPlugin(t, "p")
	a := f.connectUI(t, f.requests)
	id := watchQuery(t, a, true)
	watchRequest(t, p, id)
	data := fmt.Sprintf(`{"results":[],"extra":"%s"}`, strings.Repeat("x", 1024))
	watchFrame(t, p, id, wire.MsgResponse, data, true, false)
	snap := watchError(t, a, id, "owner watch byte")
	if len(snap.Results) != 0 {
		t.Fatal("raw payload was retained despite exceeding the watch budget")
	}
	var end wire.AckMessage
	p.read(t, &end)
	if end.Type != wire.MsgEnd || end.QueryID != id || totalWatchUsage(f.store) != (watchUsage{}) {
		t.Fatalf("oversized first live response leaked a subscription: %+v", end)
	}
}

func TestWatchPendingTimeout(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.store.watches.timeout = 300 * time.Millisecond
	f.configure("p", plugins.LifecycleDaemon, true, "")
	f.mgr.Plugins["p"].Config.Capabilities = []string{wire.MsgEnd}
	p := f.connectPlugin(t, "p")
	a := f.connectUI(t, f.requests)
	id := watchQuery(t, a, true)
	watchRequest(t, p, id)
	watchFrame(t, p, id, wire.MsgLiveUpdate, `["not opted in"]`, true, false)
	snap := watchError(t, a, id, "timed out")
	if len(snap.List) != 0 {
		t.Fatal("push before a solicited live response was accepted")
	}
	var end wire.AckMessage
	p.read(t, &end)
	if end.Type != wire.MsgEnd || end.QueryID != id || totalWatchUsage(f.store) != (watchUsage{}) {
		t.Fatalf("initial timeout leaked watch: %+v", end)
	}
	watchFrame(t, p, id, wire.MsgResponse, `["late"]`, true, false)
	a.expectStatus(t)
}

func TestWatchChargesQueryMetadataAtAdmission(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.store.watches.perOwner.bytes = 512
	f.configure("p", plugins.LifecycleDaemon, true, "")
	p := f.connectPlugin(t, "p")
	a := f.connectUI(t, f.requests)
	value := strings.Repeat("x", 1024)
	for _, req := range []wire.UIRequest{
		{Text: value}, {Context: value}, {UIFeatures: []string{value}},
		{QueryStream: value}, {UIFeatures: make([]string, 128)},
	} {
		req.Type, req.Plugin, req.Watch = "query", "p", true
		a.send(t, req)
		var ack wire.AckMessage
		a.read(t, &ack)
		watchError(t, a, ack.QueryID, "owner watch byte")
		if ack.Type != "ack" || f.store.query(ack.QueryID) != nil || totalWatchUsage(f.store) != (watchUsage{}) {
			t.Fatalf("rejected request leaked reservation: %+v", ack)
		}
	}
	p.expectNoWrite(t)
}

func TestWatchMultipleProducersAndDisconnect(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.configure("p", plugins.LifecycleDaemon, true, "")
	f.configure("logs", plugins.LifecycleDaemon, true, "")
	f.configure("silent", plugins.LifecycleDaemon, true, "")
	p, logs := f.connectPlugin(t, "p"), f.connectPlugin(t, "logs")
	silent := f.connectPlugin(t, "silent")
	a := f.connectUI(t, f.requests)
	a.send(t, wire.UIRequest{Type: "query", Text: "all", Watch: true})
	var ack wire.AckMessage
	a.read(t, &ack)
	id := ack.QueryID
	watchRequest(t, p, id)
	watchRequest(t, logs, id)
	watchRequest(t, silent, id)
	watchFrame(t, p, id, wire.MsgResponse, `["stable"]`, false, false)
	watchRows(t, a, id, "stable")
	if totalWatchUsage(f.store).count != 1 {
		t.Fatal("non-live producer released reservation while logs was pending")
	}
	watchFrame(t, logs, id, wire.MsgResponse, `["old"]`, true, false)
	watchFrame(t, logs, id, wire.MsgLiveUpdate, `["new"]`, false, false)
	read := func(v any) { a.read(t, v) }
	refreshSnapshot(t, read, id, func(s aggregate) bool {
		return string(s.Results["logs"].Data) == `["new"]` && string(s.Results["p"].Data) == `["stable"]` && len(s.List) == 2
	})
	f.store.expireInitialWatch(f.store.query(id))
	if totalWatchUsage(f.store).count != 1 {
		t.Fatal("silent producer expired a confirmed live feed")
	}
	// Another connection using the same plugin ID cannot push to this query.
	wrong, _ := f.registry.getConn("p")
	f.store.receiveQuery("logs", wrong, wire.PluginResponse{Type: wire.MsgLiveUpdate, QueryID: id, Data: json.RawMessage(`["wrong"]`)})
	logs.Close()
	snap := refreshSnapshot(t, read, id, func(s aggregate) bool { return s.Plugins["logs"].State == "error" })
	f.store.WaitForPublications()
	if totalWatchUsage(f.store) != (watchUsage{}) || string(snap.Results["logs"].Data) != `["new"]` || len(snap.List) != 2 {
		t.Fatal("losing the only live producer must release its reservation and retain the latest valid rows")
	}
	a.expectStatus(t)
}

func TestLivePushDoesNotCompleteRefresh(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.store.limit = 1
	f.configure("p", plugins.LifecycleDaemon, true, "")
	f.configure("silent", plugins.LifecycleDaemon, true, "")
	p := f.connectPlugin(t, "p")
	silent := f.connectPlugin(t, "silent")
	other := f.connectPlugin(t, "foreign")
	a := f.connectUI(t, f.requests)
	a.send(t, wire.UIRequest{Type: "query", Text: "all", Watch: true})
	var ack wire.AckMessage
	a.read(t, &ack)
	id := ack.QueryID
	watchRequest(t, p, id)
	watchRequest(t, silent, id)
	watchFrame(t, p, id, wire.MsgResponse, `["row"]`, true, false)
	watchRows(t, a, id, "row")
	q := f.store.query(id)
	action := &selectAction{queryID: id, owner: q.session, producer: "p", handler: "p", resultID: "row", offered: true}
	f.store.refreshAction(action)
	watchRequest(t, p, id)
	f.store.refreshAction(action)
	watchFrame(t, other, id, wire.MsgLiveUpdate, `["foreign"]`, false, false)
	watchFrame(t, p, id, wire.MsgLiveUpdate, `["push"]`, false, true)
	watchRows(t, a, id, "push")
	f.store.WaitForPublications()
	for _, ordinary := range []string{"older", "newer"} {
		f.store.create(ordinary, "other", "ordinary history")
	}
	if f.store.query(id) != q || totalWatchUsage(f.store).count != 1 {
		t.Fatal("final push demoted the watch while a refresh was running")
	}
	q.mu.Lock()
	d := q.producers["p"]
	held := d.outstanding && !d.started.IsZero() && d.again == producerRefresh
	q.mu.Unlock()
	if !held {
		t.Fatal("push acknowledged the outstanding refresh or cleared its dirty bit")
	}
	watchFrame(t, p, id, wire.MsgResponse, `["answer"]`, false, false)
	watchRequest(t, p, id) // exactly one queued replay
	watchRows(t, a, id, "answer")
	f.store.WaitForPublications()
	f.store.create("another", "other", "ordinary history")
	if f.store.query(id) != q || totalWatchUsage(f.store).count != 1 {
		t.Fatal("watch demoted before the queued follow-up could renew it")
	}
	watchFrame(t, p, id, wire.MsgResponse, `["renewed"]`, true, false)
	watchRows(t, a, id, "renewed")
	watchFrame(t, p, id, wire.MsgLiveUpdate, `["back live"]`, false, false)
	watchRows(t, a, id, "back live")
	action.resultID = "back live"
	f.store.refreshAction(action)
	watchRequest(t, p, id)
	watchFrame(t, p, id, wire.MsgLiveUpdate, `["last"]`, false, true)
	watchRows(t, a, id, "last")
	watchFrame(t, p, id, wire.MsgResponse, `["complete"]`, false, false)
	watchRows(t, a, id, "complete")
	f.store.WaitForPublications()
	if totalWatchUsage(f.store) != (watchUsage{}) {
		t.Fatal("non-live refresh answer kept its reservation")
	}
	watchFrame(t, p, id, wire.MsgLiveUpdate, `["after completion"]`, false, false)
	// A fresh query on the same producer fences both rejected pushes.
	next := watchQuery(t, a, false)
	watchRequest(t, p, next)
	watchFrame(t, p, next, wire.MsgResponse, `["next"]`, false, false)
	watchRows(t, a, next, "next")
	a.expectStatus(t)
}

func TestWatchRenewalBeforeWriter(t *testing.T) {
	for _, outcome := range []string{"renew", "disconnect", "timeout"} {
		t.Run(outcome, func(t *testing.T) { testQueuedWatchRenewal(t, outcome) })
	}
}

func testQueuedWatchRenewal(t *testing.T, outcome string) {
	t.Helper()
	f := newAdmissionFixture(t, time.Second)
	f.store.limit = 1
	if outcome == "timeout" {
		f.store.watches.timeout = 250 * time.Millisecond
	}
	f.configure("p", plugins.LifecycleDaemon, true, "")
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	f.registry.set("p", server)
	defer f.registry.remove("p", server)
	out := make(chan pluginRequest, 1)
	next := func() pluginRequest {
		t.Helper()
		select {
		case req := <-out:
			return req
		case <-time.After(time.Second):
			t.Fatal("no request queued")
			return pluginRequest{}
		}
	}
	updates := make(chan *wire.UpdateMessage, 4)
	if reason := f.store.startUIQuery(t.Context(), "q", "label", "owner", wire.UIRequest{Watch: true}, "p"); reason != "" {
		t.Fatal(reason)
	}
	f.store.armWatch("q", func(update *wire.UpdateMessage, _ []string) { updates <- update })
	dispatchQuery(t.Context(), "text", "q", f.mgr, out, f.registry, f.store, f.ui, true, "p")
	if !f.store.beginQueryWrite(next(), server) {
		t.Fatal("initial write lost")
	}
	f.store.receiveQuery("p", server, wire.PluginResponse{QueryID: "q", Data: json.RawMessage(`["row"]`), Live: true})
	f.store.refreshAction(&selectAction{queryID: "q", owner: "owner", producer: "p", handler: "p", resultID: "row", offered: true})
	req := next() // final push arrives before the queued refresh reaches the writer
	final := wire.PluginResponse{Type: wire.MsgLiveUpdate, QueryID: "q", Data: json.RawMessage(`["last"]`), Final: true}
	f.store.receiveQuery("p", server, final)
	q := f.store.query("q")
	f.store.create("ordinary-1", "other", "older")
	f.store.create("ordinary-2", "other", "newer")
	q.mu.Lock()
	d := q.producers["p"]
	timer := d.renewal
	pending := q.watch != nil && d.outstanding && d.started.IsZero()
	q.mu.Unlock()
	if !pending || timer == nil || f.store.query("q") != q || totalWatchUsage(f.store).count != 1 {
		t.Fatal("refresh queued before the writer did not hold the reservation")
	}
	final.Data = json.RawMessage(`["unsolicited"]`)
	f.store.receiveQuery("p", server, final)
	q.mu.Lock()
	reset := d.renewal != timer
	q.mu.Unlock()
	if reset {
		t.Fatal("unsolicited push changed the renewal timer")
	}
	switch outcome {
	case "renew":
		if !f.store.beginQueryWrite(req, server) {
			t.Fatal("refresh write lost")
		}
		f.store.receiveQuery("p", server, wire.PluginResponse{QueryID: "q", Data: json.RawMessage(`["renewed"]`), Live: true})
		f.store.create("ordinary-3", "other", "newest")
		q.mu.Lock()
		renewed := d.live && d.renewal == nil && !d.outstanding
		q.mu.Unlock()
		if !renewed || !f.store.offeredRow("owner", "q", "p", "renewed") || totalWatchUsage(f.store).count != 1 {
			t.Fatal("solicited live reply did not renew the watch")
		}
		final.Data = json.RawMessage(`["finished"]`)
		f.store.receiveQuery("p", server, final)
		if totalWatchUsage(f.store) != (watchUsage{}) || f.store.query("q") != q {
			t.Fatal("final push without a refresh did not demote the renewed watch")
		}
		return
	case "disconnect":
		f.registry.remove("p", server)
		f.store.retireQueries("p", server)
		f.store.WaitForPublications()
		q.mu.Lock()
		disconnected := !d.outstanding && d.renewal == nil && q.Plugins["p"].Error == "plugin disconnected"
		q.mu.Unlock()
		if !disconnected || totalWatchUsage(f.store) != (watchUsage{}) || f.store.query("q") != q {
			t.Fatal("disconnect did not retire the queued refresh and demote the watch")
		}
		return
	}
	snap := refreshSnapshot(t, func(v any) {
		select {
		case update := <-updates:
			*v.(*wire.UpdateMessage) = *update
		case <-time.After(time.Second):
			t.Fatal("watch did not publish its renewal timeout")
		}
	}, "q", func(s aggregate) bool { return s.WatchError != "" })
	f.store.WaitForPublications()
	if !strings.Contains(snap.WatchError, "refresh response") || len(snap.List) != 1 || snap.List[0].ID != "last" ||
		totalWatchUsage(f.store) != (watchUsage{}) || f.store.query("q") != nil || f.store.beginQueryWrite(req, server) {
		t.Fatalf("renewal timeout did not end the queued refresh and release the watch: %+v", snap)
	}
}

func TestWatchEnvelopeLimit(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.store.watches.perOwner.bytes = 16 << 20
	f.configure("p", plugins.LifecycleDaemon, true, "")
	p := f.connectPlugin(t, "p")
	a, observer := f.connectUI(t, f.requests), f.connectUI(t, f.requests)
	id := watchQuery(t, a, true)
	watchRequest(t, p, id)
	refreshSnapshot(t, func(v any) { a.read(t, v) }, id, func(q aggregate) bool { return q.Plugins["p"].State == "pending" })
	// Raw data, normalized ID, and label each contribute ~2.1 MiB. The
	// aggregate fits in 8 MiB before base64, but the UI envelope cannot.
	data := `["` + strings.Repeat("x", 2200000) + `"]`
	watchFrame(t, p, id, wire.MsgResponse, data, true, false)
	var errMsg map[string]string
	a.read(t, &errMsg)
	if errMsg["type"] != "error" || errMsg["query_id"] != id || errMsg["error"] != wire.ErrMessageTooLarge.Error() {
		t.Fatalf("oversized watch did not get the scoped wire error: %s %s", errMsg["type"], errMsg["error"])
	}
	if u := totalWatchUsage(f.store); u.count != 1 || u.bytes < int64(len(data))*2 {
		t.Fatalf("wire error affected retention: %+v", u)
	}
	a.expectStatus(t)
	observer.expectStatus(t)
}
