package daemon

import (
	"net"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/wire"
)

func TestPublishSnapshotRequiresLiveQueryAndOwner(t *testing.T) {
	store := NewTransientQueryStore()
	reg := newUIRegistry(store)
	server, reader := net.Pipe()
	defer reader.Close()
	_ = reader.SetReadDeadline(time.Now().Add(time.Second))
	client := &uiClient{conn: server, clientID: "cli"}
	reg.add(client)
	defer reg.remove(client)

	store.startQuery(t.Context(), "old", "cli", "", client.uid, "")
	store.removeByOwner(client.uid)
	store.startQuery(t.Context(), "missing-owner", "cli", "", "disconnected", "")
	store.startQuery(t.Context(), "current", "cli", "", client.uid, "")
	for _, qid := range []string{"old", "missing-owner", "current"} {
		reg.publishSnapshot(&wire.UpdateMessage{Type: "update", QueryID: qid, Payload: []byte(`{"x":1}`)}, []string{"producer"})
	}
	var update wire.UpdateMessage
	if err := wire.ReadMsg(wire.NewScanner(reader), &update); err != nil || update.QueryID != "current" || string(update.Payload) != `{"x":1}` {
		t.Fatalf("expected only the live owner's snapshot, got %+v, %v", update, err)
	}
}
