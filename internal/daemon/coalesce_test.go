package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/wire"
)

func TestPublishWhileEarlierSnapshotIsInFlight(t *testing.T) {
	s := NewTransientQueryStore()
	s.PrepareQuery("q", "ui", "search", []string{"a", "b", "c"})
	snapshots := make(chan aggregate, 4)
	unblock := make(chan struct{})
	first := true
	publish := func(update *wire.UpdateMessage, _ []string) {
		var ag aggregate
		if err := json.Unmarshal(update.Payload, &ag); err != nil {
			t.Error(err)
		}
		snapshots <- ag
		if first {
			first = false
			<-unblock
		}
	}
	submit := func(plugin, data string) {
		s.SubmitResponse(plugin, wire.PluginResponse{QueryID: "q", Data: json.RawMessage(data)}, 1, publish)
	}
	submit("a", `["old"]`)
	if ag := <-snapshots; len(ag.List) != 1 || ag.List[0].ID != "old" {
		t.Fatal("first result did not stream immediately")
	}
	submit("a", `["new"]`)
	submit("b", `[]`)
	s.submitError("q", "c", 3, "offline", publish)
	close(unblock)
	s.WaitForPublications()
	if len(snapshots) != 1 {
		t.Fatalf("expected one coalesced snapshot, got %d", len(snapshots))
	}
	ag := <-snapshots
	if len(ag.List) != 1 || ag.List[0].ID != "new" || ag.Plugins["b"].State != "empty" || ag.Plugins["c"].State != "error" {
		t.Fatalf("incomplete final snapshot: %+v", ag)
	}
}

func TestSupersedeOnlyMatchingQueryStream(t *testing.T) {
	s := NewTransientQueryStore()
	start := func(id, conn, stream string) {
		s.startQuery(context.Background(), id, "ui", "bench", conn, stream)
	}
	start("old", "one", "search")
	old := s.query("old").ctx
	start("independent", "one", "")
	start("other-box", "one", "filter")
	start("other-ui", "two", "search")
	start("new", "one", "search")
	if old.Err() == nil || s.SubmitResponse("p", wire.PluginResponse{QueryID: "old"}, 0, nil) {
		t.Fatal("superseded query accepted a late reply")
	}
	for _, id := range []string{"independent", "other-box", "other-ui", "new"} {
		if q := s.query(id); q == nil || q.ctx.Err() != nil {
			t.Fatalf("cancelled unrelated query %s", id)
		}
	}
	s.removeByOwner("one")
	if s.query("other-ui") == nil || s.query("independent") != nil || s.query("new") != nil || s.query("other-box") != nil {
		t.Fatal("disconnect affected the wrong queries")
	}
}

// Keep the slow writer blocked until the queue limit is reached, independent
// of wall-clock timing on a loaded test runner.
type stalledConn struct{ net.Conn }

func (stalledConn) SetWriteDeadline(time.Time) error { return nil }

func TestOversizedSnapshotReportsQueryErrorAndKeepsSession(t *testing.T) {
	server, reader := net.Pipe()
	defer reader.Close()
	_ = reader.SetReadDeadline(time.Now().Add(5 * time.Second))
	store := NewTransientQueryStore()
	reg := newUIRegistry(store)
	client := &uiClient{conn: stalledConn{server}, clientID: "ui"}
	reg.add(client)
	defer reg.remove(client)
	store.startQuery(t.Context(), "q", "ui", "", client.uid, "")
	// Base64 expansion pushes the encoded envelope over the limit.
	reg.publishSnapshot(&wire.UpdateMessage{Type: "update", QueryID: "q", Payload: make([]byte, wire.MaxMessageSize*3/4)}, nil)
	_ = client.send(&wire.AckMessage{Type: "ack", QueryID: "next"})
	scanner := wire.NewScanner(reader)
	var response map[string]string
	if err := wire.ReadMsg(scanner, &response); err != nil || response["query_id"] != "q" || response["type"] != "error" || response["error"] != wire.ErrMessageTooLarge.Error() {
		t.Fatalf("oversize response: %v, %v", response, err)
	}
	var ack wire.AckMessage
	if err := wire.ReadMsg(scanner, &ack); err != nil || ack.QueryID != "next" {
		t.Fatalf("next reply: %+v, %v", ack, err)
	}
}

func TestUIWriterCoalescesOnlyBetweenControlMessages(t *testing.T) {
	server, reader := net.Pipe()
	defer reader.Close()
	r := newUIRegistry(NewTransientQueryStore())
	c := &uiClient{conn: stalledConn{server}, clientID: "ui"}
	r.add(c)
	defer r.remove(c)
	_ = c.send(1) // The writer cannot drain more until the reader starts.
	for i := range 300 {
		_ = c.send(&wire.UpdateMessage{Type: "update", QueryID: "a", Payload: []byte{byte(i % 100)}})
		_ = c.send(&wire.UpdateMessage{Type: "update", QueryID: "b", Payload: []byte{byte(i % 100)}})
	}
	_ = c.send(&wire.SelectResponse{Type: wire.MsgSelectResponse, Success: true})
	_ = c.send(&wire.UpdateMessage{Type: "update", QueryID: "a", Payload: []byte{100}})
	flushed := make(writeBarrier)
	_ = c.send(flushed)
	_ = c.send(&wire.UpdateMessage{Type: "update", QueryID: "a", Payload: []byte{101}})
	sc := wire.NewScanner(reader)
	_ = reader.SetReadDeadline(time.Now().Add(time.Second))
	control := func(want int) {
		t.Helper()
		var got int
		if err := wire.ReadMsg(sc, &got); err != nil || got != want {
			t.Fatalf("control %d: got %d, %v", want, got, err)
		}
	}
	snapshot := func(id string, last byte) {
		t.Helper()
		var u wire.UpdateMessage
		if err := wire.ReadMsg(sc, &u); err != nil || u.QueryID != id || !bytes.Equal(u.Payload, []byte{last}) {
			t.Fatalf("snapshot %s: %+v, %v", id, u, err)
		}
	}
	control(1)
	snapshot("a", 99)
	snapshot("b", 99)
	var terminal wire.SelectResponse
	if err := wire.ReadMsg(sc, &terminal); err != nil || terminal.Type != wire.MsgSelectResponse || !terminal.Success {
		t.Fatalf("terminal: %+v, %v", terminal, err)
	}
	snapshot("a", 100)
	snapshot("a", 101)
	select {
	case <-flushed:
	default:
		t.Fatal("last snapshot overtook the flush barrier")
	}
}

func TestStalledUIIsIndependentOfOtherClients(t *testing.T) {
	store := NewTransientQueryStore()
	r := newUIRegistry(store)
	blocked, unread := net.Pipe()
	defer unread.Close()
	slow := &uiClient{conn: stalledConn{blocked}, clientID: "slow"}
	r.add(slow)
	defer r.remove(slow)
	server, reader := net.Pipe()
	defer reader.Close()
	fast := &uiClient{conn: server, clientID: "fast"}
	r.add(fast)
	defer r.remove(fast)
	store.startQuery(t.Context(), "fast-query", "", "", fast.uid, "")
	// No reader on the slow connection. The healthy client's frames must
	// still arrive in order, without waiting for that writer to time out.
	scanner := wire.NewScanner(reader)
	_ = reader.SetReadDeadline(time.Now().Add(time.Second))
	for i := range cap(slow.out) + 3 {
		_ = slow.send(i)
		payload := []byte{byte(i)}
		r.publishSnapshot(&wire.UpdateMessage{Type: "update", QueryID: "fast-query", Payload: payload}, nil)
		var got wire.UpdateMessage
		if err := wire.ReadMsg(scanner, &got); err != nil || got.QueryID != "fast-query" || !bytes.Equal(got.Payload, payload) {
			t.Fatalf("read frame %d: got %+v, error %v", i, got, err)
		}
	}
	select {
	case <-slow.done:
	default:
		t.Fatal("stalled UI remained connected after exceeding its queue limit")
	}
}
