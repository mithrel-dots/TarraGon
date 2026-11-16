package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/db"
	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

func TestFrecencyOptOutUsesSelectedAction(t *testing.T) {
	actions := []wire.Action{{Name: "inspect"}, {Name: "save", NoFrecency: true, Default: true}}
	for name, want := range map[string]bool{"": true, "save": true, "inspect": false, "unknown": false} {
		if got := skipsFrecency(actions, name); got != want {
			t.Errorf("action %q: no_frecency = %v, want %v", name, got, want)
		}
	}
	actions[1].Default = false
	if skipsFrecency(actions, "") {
		t.Fatal("secondary action opted the primary action out of history")
	}
}

func TestRenamePromptCommitsOnlyOnSelect(t *testing.T) {
	dir := t.TempDir()
	entry := writeScript(t, dir, "files", `#!/bin/sh
cd "${0%/*}" || exit 1
case "$2:$TARRAGON_CONTEXT" in
  query:)
    echo '[{"id":"file-17","actions":[{"name":"Rename","type":"open","destination":{"plugin":"files","context":"rename:file-17","text":""},"placeholder":"New name"}]}]' ;;
  query:rename:file-17)
    echo "$3" >> edits
    case "$3" in
      n|ne|new.txt) printf '[{"id":"%s","actions":[{"name":"save","default":true,"no_frecency":true}]}]\n' "$3" ;;
      *) echo '[]' ;;
    esac ;;
  select:rename:file-17) echo "$TARRAGON_CONTEXT:$3:$4" >> commits ;;
  select:) exit 0 ;;
  *) exit 1 ;;
esac`)
	database, err := db.Open(filepath.Join(dir, "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	f := newAdmissionFixture(t, 3*time.Second)
	f.configure("files", plugins.LifecycleOnCall, true, entry)
	f.registry.bus = testBus(t, t.Context())
	server, ui := admissionSocket(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleUIClient(ctx, server, f.mgr, f.requests, f.registry, f.store, f.ui, database)
	}()
	stop := func() { cancel(); <-done } // also drains selection history writes
	defer stop()
	query := func(req wire.UIRequest) (string, wire.ResultItem) {
		t.Helper()
		req.Type = "query"
		req.QueryStream = "view"
		req.UIFeatures = []string{"navigation"}
		ui.send(t, req)
		var ack wire.AckMessage
		ui.read(t, &ack)
		snap := refreshSnapshot(t, func(v any) { ui.read(t, v) }, ack.QueryID, func(s aggregate) bool {
			return s.Plugins["files"].State == "done"
		})
		if len(snap.List) != 1 || len(snap.List[0].Actions) != 1 {
			t.Fatalf("expected one candidate and action: %+v", snap.List)
		}
		return ack.QueryID, snap.List[0]
	}
	_, root := query(wire.UIRequest{Plugin: "files"})
	open := root.Actions[0]
	if open.Type != "open" || open.Destination == nil || open.Placeholder != "New name" {
		t.Fatalf("missing rename prompt: %+v", open)
	}
	var last string
	for visit := range 2 {
		for _, edit := range []string{"n", "ne", "new.txt"} {
			qid, row := query(wire.UIRequest{Plugin: open.Destination.Plugin, Context: open.Destination.Context, Text: edit})
			if row.ID != edit || !row.Actions[0].NoFrecency {
				t.Fatalf("wrong candidate for %q: %+v", edit, row)
			}
			last = qid
		}
		if _, err := os.Stat(filepath.Join(dir, "commits")); !os.IsNotExist(err) {
			t.Fatalf("edits committed before select: %v", err)
		}
		if visit == 0 {
			// Back releases this view and restores the cached root without a select.
			ui.send(t, wire.UIRequest{Type: "end", QueryID: last})
			var ended wire.AckMessage
			ui.read(t, &ended)
			if ended.Type != "ok" || ended.QueryID != last {
				t.Fatalf("unexpected end acknowledgement: %+v", ended)
			}
		}
	}
	for _, req := range []wire.UIRequest{
		{QueryID: last, Plugin: "files", ResultID: "new.txt", Action: "save"},
		{QueryID: "evicted", Plugin: "files", ResultID: "opted-out", NoFrecency: true},
		{Plugin: "files", ResultID: "ordinary"},
	} {
		ack := ui.selectRow(t, req)
		ui.outcome(t, ack, true, "")
	}
	stop()
	for name, want := range map[string]string{"edits": "n\nne\nnew.txt\nn\nne\nnew.txt\n", "commits": "rename:file-17:new.txt:save\n"} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(got) != want {
			t.Fatalf("%s = %q (%v), want %q", name, got, err, want)
		}
	}
	scores, err := database.GetFrecencyScores(t.Context())
	if err != nil || len(scores) != 1 || scores["files:ordinary"] <= 0 {
		t.Fatalf("expected only ordinary selection in history, got %v (%v)", scores, err)
	}
}
