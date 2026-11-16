package daemon

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

// A wire-level model of what the two frontend generations do on activation.
// Frames and cached selection belong to the frontend, even after daemon eviction.
type navigationView struct {
	destination                         wire.Destination // Text is the current input, including any edits
	issuer, title, placeholder          string
	queryID, selectedPlugin, selectedID string
	scroll                              int
	rows                                []wire.ResultItem
}

type navigationUI struct {
	features []string // nil models the baseline frontend, which only recognizes query_replace
	views    []navigationView
	replay   map[string]bool // current enabled targets with a replay manifest capability
}

func (u *navigationUI) current() *navigationView { return &u.views[len(u.views)-1] }

func (u *navigationUI) query() *wire.UIRequest {
	d := u.current().destination
	return &wire.UIRequest{Type: "query", Plugin: d.Plugin, Text: d.Text, Context: d.Context, UIFeatures: u.features}
}

func (u *navigationUI) open(d wire.Destination, issuer, title, placeholder string) *wire.UIRequest {
	u.views = append(u.views, navigationView{destination: d, issuer: issuer, title: title, placeholder: placeholder})
	return u.query()
}

func (u *navigationUI) back() *wire.UIRequest {
	if len(u.views) == 1 {
		return nil
	}
	u.views = u.views[:len(u.views)-1]
	v := u.current()
	if v.rows == nil && v.destination.Plugin != "" && (v.destination.Plugin == v.issuer || u.replay[v.destination.Plugin]) {
		return u.query()
	}
	return nil
}

func (u *navigationUI) activate(row wire.ResultItem, action wire.Action) *wire.UIRequest {
	if action.Type == wire.ActionTypeQueryReplace {
		*u.current() = navigationView{destination: wire.Destination{Text: action.Query}}
		return u.query()
	}
	if slices.Contains(u.features, navigationFeature) && action.Type == wire.ActionTypeOpen {
		return u.open(*action.Destination, row.Plugin, action.Title, action.Placeholder)
	}
	return &wire.UIRequest{Type: "select", QueryID: u.current().queryID, Plugin: row.Plugin, ResultID: row.ID, Action: action.Name}
}

func (u *navigationUI) outcome(invokingQuery string, resp wire.SelectResponse) *wire.UIRequest {
	if u.current().queryID != invokingQuery || !resp.Success || resp.Next == nil {
		return nil
	}
	switch n := resp.Next; n.Type {
	case wire.ActionTypeOpen:
		return u.open(*n.Destination, n.Issuer, n.Title, n.Placeholder)
	case "back":
		return u.back()
	}
	return nil
}

func navigationSnapshot(t *testing.T, peer admissionPeer, req *wire.UIRequest, producer string) aggregate {
	t.Helper()
	if req == nil || req.Type != "query" {
		t.Fatalf("expected a query, got %+v", req)
	}
	peer.send(t, req)
	var ack wire.AckMessage
	peer.read(t, &ack)
	if ack.Type != "ack" || ack.QueryID == "" {
		t.Fatalf("bad query acknowledgement: %+v", ack)
	}
	return refreshSnapshot(t, func(v any) { peer.read(t, v) }, ack.QueryID, func(a aggregate) bool {
		return a.Plugins[producer].State != "pending"
	})
}

func TestNavigationFrontends(t *testing.T) {
	dir := t.TempDir()
	rows, err := os.ReadFile("testdata/navigation.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "rows.json"), rows, 0600); err != nil {
		t.Fatal(err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, rows); err != nil {
		t.Fatal(err)
	}
	entry := writeScript(t, dir, "catalog", `#!/bin/sh
cd "${0%/*}" || exit 1
echo "$2:$3:$4:$TARRAGON_CONTEXT" >> calls
case "$2:$4" in
  query:) cat rows.json ;;
  select:) echo '{"success":true,"message":"Selected"}' ;;
  select:Mark|select:Continue) echo '{"success":true,"message":"Saved","next":{"type":"open","issuer":"forged","destination":{"plugin":"catalog","context":"episode:7","text":""},"title":"Episode 7"}}' ;;
  select:Done) echo '{"success":true,"next":{"type":"back"}}' ;;
  *) exit 1 ;;
esac
`)
	f := newAdmissionFixture(t, 2*time.Second)
	f.store.limit = 1
	f.configure("catalog", plugins.LifecycleOnCall, true, entry)
	f.configure("consumer", plugins.LifecycleOnCall, true, "")
	f.mgr.Plugins["consumer"].BaseConfig.Handlers = []plugins.Handler{{Plugin: "consumer", Action: "copy", Label: "Copy", Accepts: "text"}}
	if err := f.mgr.ApplyOverrides(); err != nil {
		t.Fatal(err)
	}
	f.mgr.Plugins["catalog"].Config.ResolvedPrefix = "@cat"
	f.registry.bus = testBus(t, t.Context())
	for _, features := range [][]string{nil, {navigationFeature}} {
		peer := f.connectUI(t, f.requests)
		u := navigationUI{views: []navigationView{{destination: wire.Destination{Text: "@cat root"}}}, features: features}
		query := func(req *wire.UIRequest) wire.ResultItem {
			t.Helper()
			snap := navigationSnapshot(t, peer, req, "catalog")
			want := []string{"show", "legacy"}
			if len(features) != 0 {
				want = []string{"show", "private-view", "unsafe-open", "unsafe-required", "legacy"}
			}
			var ids []string
			for _, row := range snap.List {
				ids = append(ids, row.ID)
			}
			if !bytes.Equal(snap.Results["catalog"].Data, compact.Bytes()) || !slices.Equal(ids, want) || snap.Plugins["catalog"].Count != len(want) {
				t.Fatalf("wrong raw payload, visible rows or count: %+v", snap)
			}
			u.current().queryID, u.current().rows = snap.QueryID, snap.List
			return snap.List[0]
		}
		selectRow := func(req *wire.UIRequest) wire.SelectResponse {
			t.Helper()
			if req == nil || req.Type != "select" {
				t.Fatalf("expected a select, got %+v", req)
			}
			ack := peer.selectRow(t, *req)
			return peer.outcome(t, ack, true, "")
		}
		root := query(u.query())
		reject := func(id, name string) {
			t.Helper()
			ack := peer.selectRow(t, wire.UIRequest{QueryID: u.current().queryID, Plugin: root.Plugin, ResultID: id, Action: name})
			peer.outcome(t, ack, false, "not_run")
		}
		// A faulty client sending select for a pure navigation choice must not run it.
		reject(root.ID, "")
		reject(root.ID, "Search")
		reject(root.ID, "Episodes")
		reject("private-view", "Open")
		if len(features) == 0 {
			// A client that treats unknown action types as select sees none.
			if len(root.Actions) != 2 || !root.Actions[0].Default || root.Actions[0].Name != "Search" || root.Actions[0].Type != "query_replace" ||
				root.Actions[0].Query != "@cat summary" || root.Actions[1].Name != "Mark" {
				t.Fatalf("baseline menu: %+v", root.Actions)
			}
			legacy := u.current().rows[1]
			if len(legacy.Actions) != 0 {
				t.Fatalf("baseline legacy actionless row received actions: %+v", legacy)
			}
			reject("private-view", "")
			reject("unsafe-open", "")
			reject("unsafe-open", "Open")
			reject("unsafe-required", "")
			reject("unsafe-required", "Continue")
			reject(root.ID, "Continue")
			cachedSelect := u.activate(legacy, wire.Action{})
			row := query(u.activate(root, root.Actions[0]))
			if resp := selectRow(u.activate(row, row.Actions[1])); resp.Next != nil || resp.Message != "Saved" {
				t.Fatalf("wrong baseline outcome: %+v", resp)
			}
			if f.store.query(cachedSelect.QueryID) != nil {
				t.Fatal("expected the baseline view to be evicted")
			}
			selectRow(cachedSelect)
			continue
		}
		if len(root.Actions) != 6 || root.Actions[0].Type != "open" || root.Actions[3].Name != "Continue" {
			t.Fatalf("navigation menu: %+v", root.Actions)
		}
		if actions := u.current().rows[2].Actions; len(actions) != 3 || actions[1].Name != "Delete" || !actions[2].Secondary {
			t.Fatalf("lost native or secondary choices from the navigation view: %+v", actions)
		}
		child := query(u.activate(root, root.Actions[0]))
		view := u.current()
		firstID := view.queryID
		if view.title != "Episodes" || view.placeholder != "Filter episodes" || view.issuer != "catalog" {
			t.Fatalf("lost opener metadata: %+v", view)
		}
		view.selectedPlugin, view.selectedID, view.scroll = child.Plugin, child.ID, 37
		view.rows = nil // simulate a bounded frontend cache; the query is about to be evicted too
		next := selectRow(u.activate(child, child.Actions[3]))
		episode := query(u.outcome(firstID, next))
		if u.current().issuer != "catalog" || u.current().title != "Episode 7" || f.store.query(firstID) != nil {
			t.Fatal("expected a handler-issued destination and an evicted source")
		}
		if u.outcome(firstID, next) != nil || len(u.views) != 3 {
			t.Fatal("a late outcome navigated away from the current view")
		}
		back := selectRow(u.activate(episode, episode.Actions[5]))
		query(u.outcome(u.current().queryID, back))
		view = u.current()
		if view.queryID == firstID || view.selectedPlugin != "catalog" || view.selectedID != "show" || view.scroll != 37 {
			t.Fatalf("Back failed to restore the evicted view: %+v", view)
		}
		if u.back() != nil || len(u.current().rows) != 5 || u.back() != nil || len(u.views) != 1 {
			t.Fatal("Back must restore cached root state, then do nothing at the root")
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "calls"))
	want := []string{
		"query:root::", "query:summary::", "select:show:Mark:", "select:legacy::",
		"query:root::", "query:::show:42", "select:show:Continue:show:42",
		"query:::episode:7", "select:show:Done:episode:7", "query:::show:42",
	}
	if err != nil || !slices.Equal(strings.Split(strings.TrimSuffix(string(data), "\n"), "\n"), want) {
		t.Fatalf("unexpected plugin calls:\n%s\nwant:\n%s (%v)", data, strings.Join(want, "\n"), err)
	}
}

func TestNavigationBackWithoutCachedRows(t *testing.T) {
	for _, tc := range []struct {
		name, target, issuer string
		replay, wantQuery    bool
	}{
		{"arbitrary typed legacy query", "", "", false, false},
		{"overview opened another plugin", "catalog", "plugins", false, false},
		{"target issued its own view", "catalog", "catalog", false, true},
		{"target separately promised replay", "catalog", "plugins", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := navigationUI{views: []navigationView{
				{destination: wire.Destination{Plugin: tc.target, Text: "typed text"}, issuer: tc.issuer},
				{},
			}, replay: map[string]bool{"catalog": tc.replay}}
			if got := u.back(); (got != nil) != tc.wantQuery || got != nil && (got.Plugin != tc.target || got.Text != "typed text") {
				t.Fatalf("unexpected replay: %+v", got)
			}
		})
	}
}

func TestNavigationOversizedContinuationKeepsOutcome(t *testing.T) {
	resp := &wire.SelectResponse{Type: wire.MsgSelectResponse, Success: true, RequestID: "r", ActionID: "a",
		Next: &wire.ActionNext{Type: "open", Destination: &wire.Destination{Plugin: "catalog", Text: strings.Repeat("<", wire.MaxMessageSize/6)}}}
	var out bytes.Buffer
	if err := writeUIFrame(&out, resp); err != nil {
		t.Fatal(err)
	}
	var got wire.SelectResponse
	if err := json.Unmarshal(out.Bytes(), &got); err != nil || !got.Success || got.Next != nil ||
		got.Type != wire.MsgSelectResponse || got.ActionID != "a" || got.RequestID != "r" {
		t.Fatalf("lost the correlated terminal outcome: %+v (%v)", got, err)
	}
}
