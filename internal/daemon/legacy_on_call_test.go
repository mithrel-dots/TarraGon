package daemon

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

func TestLegacyOnCallPluginE2E(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "plain")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	const manifest = `id = "plain"
entrypoint = "plain.sh"
lifecycle_mode = "on_call"
enabled = true
provides_general_suggestions = true
`
	if err := os.WriteFile(filepath.Join(dir, "plugin.toml"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	writeScript(t, dir, "plain.sh", `#!/bin/sh
case "$1:$2:$#" in
  tarragon:query:3) printf '%s\n' '["one","two"]' ;;
  tarragon:select:3|tarragon:select:4) exit 0 ;;
  *) exit 2 ;;
esac
`)
	mgr := plugins.NewManager(root)
	if err := mgr.Discover(); err != nil {
		t.Fatal(err)
	}
	store := NewTransientQueryStore()
	requests, registry := startPluginRouter(t.Context(), store)
	testUIListener(t, t.Context(), mgr, requests, registry, store, newUIRegistry(store), nil)
	conn, err := net.Dial("unix", wire.ResolveUISocketPath())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	scanner := wire.NewScanner(conn)
	send := func(request string) {
		t.Helper()
		if err := wire.WriteMsg(conn, json.RawMessage(request)); err != nil {
			t.Fatal(err)
		}
	}
	read := func(frame any) {
		t.Helper()
		if err := wire.ReadMsg(scanner, frame); err != nil {
			t.Fatal(err)
		}
	}

	send(`{"type":"query","text":"two words"}`)
	var ack wire.AckMessage
	read(&ack)
	if ack.Type != "ack" || ack.QueryID == "" {
		t.Fatalf("unexpected query acknowledgement: %+v", ack)
	}
	for {
		var update wire.UpdateMessage
		read(&update)
		if update.Type != "update" || update.QueryID != ack.QueryID {
			t.Fatalf("expected update for %s, got %+v", ack.QueryID, update)
		}
		var snap aggregate
		if err := json.Unmarshal(update.Payload, &snap); err != nil {
			t.Fatal(err)
		}
		state := snap.Plugins["plain"]
		if state.State == "pending" {
			continue
		}
		if state.State != "done" {
			t.Fatalf("expected plain query to complete: %+v", state)
		}
		if len(snap.List) != 2 || snap.List[0].ID != "one" || snap.List[1].ID != "two" {
			t.Fatalf("unexpected rows from plain: %+v", snap.List)
		}
		break
	}

	send(`{"type":"select","plugin":"plain","result_id":"one"}`)
	var ok wire.AckMessage
	read(&ok)
	if ok.Type != "ok" {
		t.Fatalf("first select frame = %q, want ok", ok.Type)
	}
	var result wire.SelectResponse
	read(&result)
	if result.Type != wire.MsgSelectResponse || !result.Success {
		t.Fatalf("expected success from empty select stdout: %+v", result)
	}
	// A second queued select frame would be read before the status reply.
	send(`{"type":"status"}`)
	var status wire.StatusResponse
	read(&status)
	if status.Type != "status" {
		t.Fatalf("unexpected frame after terminal select response: %s", status.Type)
	}
}
