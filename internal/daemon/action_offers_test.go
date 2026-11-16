package daemon

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

var wallpaperRule = plugins.Handler{Plugin: "wallpaper", Action: "set", Label: "Set as wallpaper", Accepts: "file", MIME: "image/*"}

func TestMalformedRowActions(t *testing.T) {
	f := newAdmissionFixture(t, 5*time.Second)
	f.configure("finder", plugins.LifecycleDaemon, true, "")
	f.configure("wallpaper", plugins.LifecycleDaemon, true, "")
	f.mgr.Plugins["wallpaper"].BaseConfig.Handlers = []plugins.Handler{wallpaperRule}
	if err := f.mgr.ApplyOverrides(); err != nil {
		t.Fatal(err)
	}
	finder := f.connectPlugin(t, "finder")
	owner := f.connectUI(t, f.requests)
	owner.send(t, wire.UIRequest{Type: "query", Plugin: "finder", UIFeatures: []string{navigationFeature, actionOffersFeature}})
	var ack wire.AckMessage
	owner.read(t, &ack)
	var query wire.PluginRequest
	finder.read(t, &query)

	const ref = `"ref":{"type":"file","value":"/pictures/sky.png","mime":"image/png"}`
	const raw = `{"ordered":true,"items":[` +
		`{"id":"opens",` + ref + `,"actions":[{"name":"open","type":"open","destination":{"plugin":"finder","context":"child"},"default":true,"secondary":true,"offer_id":"forged","future":{"flag":true}},{"name":"bad","requires":"navigation"},{"name":"bad-context","type":"open","destination":{"plugin":"finder","context":"\ud800"}}]},` +
		`{"id":"bad-default",` + ref + `,"actions":[{"name":"open","type":"open","destination":{"plugin":"finder"},"default":true,"requires":42},{"name":"delete"}]},` +
		`{"id":"bad-sole",` + ref + `,"actions":[{"name":"open","type":"open","destination":{"plugin":"finder"},"requires":"navigation"}]},` +
		`{"id":"bad-context-default",` + ref + `,"actions":[{"name":"delete"},{"name":"open","type":"open","default":true,"destination":{"plugin":"finder","context":"\ud800"}}]},` +
		`{"id":"bad-context-sole",` + ref + `,"actions":[{"name":"open","type":"open","destination":{"plugin":"finder","context":42}}]},` +
		`{"id":"null-first",` + ref + `,"actions":[null,{"name":"delete"}]},` +
		`{"id":"null-only",` + ref + `,"actions":[null]},` +
		`{"id":"wrong-entry",` + ref + `,"actions":["delete"]},` +
		`{"id":"wrong-container",` + ref + `,"actions":42},` +
		`{"id":"legacy","actions":[null,{"default":true}]}]}`
	finder.send(t, wire.PluginResponse{Type: wire.MsgResponse, QueryID: query.QueryID, Data: json.RawMessage(raw)})
	snap := refreshSnapshot(t, func(v any) { owner.read(t, v) }, ack.QueryID, func(a aggregate) bool {
		return a.Plugins["finder"].State == "done"
	})
	if len(snap.List) != 2 || snap.List[0].ID != "opens" || snap.List[1].ID != "legacy" ||
		string(snap.Results["finder"].Data) != raw {
		t.Fatalf("changed rows or raw data: %+v", snap)
	}
	actions := snap.List[0].Actions
	if len(actions) != 2 || !actions[1].Secondary || actions[1].OfferID == "" || actions[1].Name != wallpaperRule.Label {
		t.Fatalf("wanted the valid Open and a matching offer: %+v", actions)
	}
	open := actions[0]
	if open.Name != "open" || open.Type != wire.ActionTypeOpen || !open.Default || open.Secondary ||
		open.OfferID != "" || open.Destination == nil || open.Destination.Context != "child" {
		t.Fatalf("Open changed or forged an offer: %+v", open)
	}
	if actions := snap.List[1].Actions; len(actions) != 1 || actions[0].Name != "" || !actions[0].Default {
		t.Fatalf("lost the legacy unnamed default: %+v", actions)
	}
	for _, blocked := range []struct{ row, name string }{
		{"opens", "bad"}, {"bad-default", "open"}, {"bad-sole", "open"},
		{"bad-default", ""}, {"bad-sole", ""}, {"null-first", ""}, {"null-only", ""}, {"wrong-entry", ""}, {"wrong-container", ""},
		{"opens", "bad-context"}, {"bad-context-default", "open"}, {"bad-context-default", ""}, {"bad-context-sole", "open"}, {"bad-context-sole", ""},
	} {
		choice := wire.UIRequest{QueryID: ack.QueryID, Plugin: "finder", ResultID: blocked.row, Action: blocked.name}
		owner.outcome(t, owner.selectRow(t, choice), false, "not_run")
	}
	finder.expectNoWrite(t)
}

func TestMalformedPrimaryActionMarksImplicitSelectBlocked(t *testing.T) {
	const raw = `[
		{"id":"default","actions":[{"name":"delete"},{"name":"open","type":"open","default":true,"requires":42}]},
		{"id":"first","actions":[{"name":"open","type":"open","requires":"navigation"},{"name":"delete"}]},
		{"id":"sole","actions":[{"name":"open","type":"open","requires":42}]},
		{"id":"later-default","actions":[{"name":"open","type":"open","requires":42},{"name":"delete","default":true}]}
	]`
	res, _ := parseResult("finder", json.RawMessage(raw), 0)
	want := []string{"open", ""}
	for _, row := range []string{"default", "first", "sole"} {
		if got := res.blockedSelects[row]; !slices.Equal(got, want) {
			t.Errorf("%s blocked %q, want %q", row, got, want)
		}
	}
	if got := res.blockedSelects["later-default"]; !slices.Equal(got, []string{"open"}) {
		t.Errorf("valid explicit default was blocked: %q", got)
	}
}

func TestActionOfferProjection(t *testing.T) {
	mgr := plugins.NewManager(t.TempDir())
	mgr.Plugins["wallpaper"] = &plugins.Plugin{BaseConfig: plugins.PluginConfig{Enabled: true, Handlers: []plugins.Handler{wallpaperRule}}}
	if err := mgr.ApplyOverrides(); err != nil {
		t.Fatal(err)
	}
	const raw = `[
	{"id":"image","ref":{"type":"file","value":"/sky.png","mime":"image/png"},"actions":[{"name":"open","default":true,"secondary":true,"offer_id":"forged"}]},
	{"id":"actionless","ref":{"type":"file","value":"/sky.png","mime":"image/png"}},
	{"id":"collision","ref":{"type":"file","value":"/sky.png","mime":"image/png"},"actions":[{"name":"Set as wallpaper"}]},
	{"id":"duplicate","ref":{"type":"file","value":"/a.png","mime":"image/png"}},
	{"id":"duplicate","ref":{"type":"file","value":"/b.png","mime":"image/png"}},
	{"id":"wrong MIME","ref":{"type":"file","value":"/a","mime":"text/plain"}},
	{"id":"missing MIME","ref":{"type":"file","value":"/a"}},
	{"id":"wildcard ref","ref":{"type":"file","value":"/a","mime":"image/*"}},
	{"id":"bad value","ref":{"type":"file","value":"/a\u0000b","mime":"image/png"}}
	]`
	q := &queryState{uiFeatures: []string{actionOffersFeature}}
	res, _ := parseResult("file_finder", json.RawMessage(raw), 0)
	q.addOffersLocked(res.items, mgr)
	rows := res.items
	if len(rows) != 9 || string(res.Data) != raw {
		t.Fatalf("altered producer results: %+v", res)
	}
	if open := rows[0].Actions[0]; len(rows[0].Actions) != 2 || open.Name != "open" || !open.Default || open.Secondary || open.OfferID != "" {
		t.Fatalf("native action changed or forged an offer: %+v", rows[0])
	}
	secondary := rows[1].Actions
	if len(secondary) != 1 || !secondary[0].Secondary || secondary[0].Default || secondary[0].OfferID == "" || secondary[0].OfferID == rows[0].Actions[1].OfferID {
		t.Fatalf("actionless row's secondary offer: %+v", secondary)
	}
	if len(rows[2].Actions) != 1 || rows[2].Actions[0].Secondary {
		t.Fatalf("native name collision: %+v", rows[2])
	}
	for _, row := range rows[3:] {
		if len(row.Actions) != 0 {
			t.Fatalf("ineligible row received offer: %+v", row)
		}
	}

	q.uiFeatures = nil
	res, _ = parseResult("file_finder", json.RawMessage(raw), 0)
	q.addOffersLocked(res.items, mgr)
	if len(res.items[1].Actions) != 0 || !legacyOfferName(res.items[0].Actions[1].Name) {
		t.Fatalf("unexpected legacy projection: %+v", res.items)
	}
	alias := res.items[0].Actions[1].Name
	res, _ = parseResult("file_finder", json.RawMessage(strings.Replace(raw, `"name":"open"`, `"name":"`+alias+`"`, 1)), 0)
	q.addOffersLocked(res.items, mgr)
	if actions := res.items[0].Actions; len(actions) != 1 || actions[0].Secondary || !actions[0].Default {
		t.Fatalf("legacy name collision: %+v", actions)
	}
	conflicting := wallpaperRule
	conflicting.Plugin = "another"
	mgr.Plugins["another"] = &plugins.Plugin{BaseConfig: plugins.PluginConfig{Enabled: true, Handlers: []plugins.Handler{conflicting}}}
	if err := mgr.ApplyOverrides(); err != nil {
		t.Fatal(err)
	}
	q.uiFeatures = []string{actionOffersFeature}
	res, _ = parseResult("file_finder", json.RawMessage(raw), 0)
	q.addOffersLocked(res.items, mgr)
	if len(res.items[0].Actions) != 1 || len(res.items[1].Actions) != 0 {
		t.Fatalf("different handlers with the same displayed name: %+v", res.items)
	}
}

func TestActionOffersBindRouteAndRefresh(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mode   plugins.LifecycleMode
		replay bool
	}{
		{"capable scoped cold persistent", plugins.LifecycleOnDemandPersistent, false},
		{"legacy global on-call replay", plugins.LifecycleOnCall, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("TARRAGON_CONTEXT", "inherited private context")
			t.Setenv("OFFER_LOG", filepath.Join(dir, "calls"))
			entry := writeScript(t, dir, "wallpaper", `#!/bin/sh
printf '%s|%s|%s|%s|%s|%s\n' "$#" "$1" "$2" "$3" "$4" "${TARRAGON_CONTEXT+present}" >> "$OFFER_LOG"
if [ "$#" = 0 ]; then exec sleep 20; fi
echo '{"success":true,"next":{"type":"refresh"}}'
`)
			f := newAdmissionFixture(t, 2*time.Second)
			f.registry.bus = testBus(t, t.Context())
			f.configure("file_finder", plugins.LifecycleDaemon, true, "")
			f.configure("other", plugins.LifecycleDaemon, true, "")
			f.configure("wallpaper", tc.mode, true, entry)
			h := f.mgr.Plugins["wallpaper"]
			h.BaseConfig.Handlers = []plugins.Handler{wallpaperRule}
			h.BaseConfig.ProvidesGeneral = false
			if tc.replay {
				f.mgr.Plugins["file_finder"].BaseConfig.Capabilities = []string{"replay"}
			}
			if err := f.mgr.ApplyOverrides(); err != nil {
				t.Fatal(err)
			}
			finder := f.connectPlugin(t, "file_finder")
			other := f.connectPlugin(t, "other")
			owner, observer := f.connectUI(t, f.requests), f.connectUI(t, f.requests)
			query := wire.UIRequest{Type: "query", Text: "sky"}
			capable := tc.mode == plugins.LifecycleOnDemandPersistent
			if capable {
				query.Plugin, query.Context = "file_finder", "private finder cache"
				query.UIFeatures = []string{actionOffersFeature}
			}
			owner.send(t, query)
			var ack wire.AckMessage
			owner.read(t, &ack)
			var initial wire.PluginRequest
			finder.read(t, &initial)
			if initial.Type != wire.MsgRequest || initial.QueryID != ack.QueryID || initial.Context != query.Context {
				t.Fatalf("initial producer request: %+v", initial)
			}
			if query.Plugin == "" {
				var req wire.PluginRequest
				other.read(t, &req)
				other.send(t, wire.PluginResponse{Type: "response", QueryID: req.QueryID, Data: json.RawMessage(`["unrelated"]`)})
			}
			const sky = `{"id":"f17","ref":{"type":"file","value":"/pictures/sky.png","mime":"image/png"},"actions":[{"name":"open","default":true}]}`
			reply := wire.PluginResponse{Type: "response", QueryID: ack.QueryID, Data: json.RawMessage(sky)}
			finder.send(t, reply)
			readOffer := func() wire.UIRequest {
				t.Helper()
				snap := refreshSnapshot(t, func(v any) { owner.read(t, v) }, ack.QueryID, func(s aggregate) bool {
					return s.Plugins["file_finder"].State == "done" && (query.Plugin != "" || s.Plugins["other"].State == "done")
				})
				for _, row := range snap.List {
					if row.ID == "f17" && len(row.Actions) == 2 && row.Actions[0].Name == "open" && row.Actions[0].Default {
						a := row.Actions[1]
						if !a.Secondary || a.Default || (a.OfferID != "") != capable || (!capable && !legacyOfferName(a.Name)) {
							t.Fatalf("unexpected injected action: %+v", a)
						}
						return wire.UIRequest{QueryID: ack.QueryID, Plugin: row.Plugin, ResultID: row.ID, Action: a.Name, OfferID: a.OfferID, Context: "caller override"}
					}
				}
				t.Fatalf("offer missing after projection: %+v", snap.List)
				return wire.UIRequest{}
			}
			click := readOffer()
			reject := func(peer admissionPeer, req wire.UIRequest) {
				t.Helper()
				peer.outcome(t, peer.selectRow(t, req), false, "not_run")
			}
			reject(observer, click)
			wrongRow := click
			wrongRow.ResultID = "/some/other/file.png"
			reject(owner, wrongRow)
			forged := click
			forged.OfferID = "unknown"
			reject(owner, forged)
			if capable {
				missing := click
				missing.OfferID = ""
				reject(owner, missing)
				wrongName := click
				wrongName.Action = "open"
				reject(owner, wrongName)
			}
			f.mgr.Lock()
			h.Config.Enabled = false
			f.mgr.Unlock()
			reject(owner, click)
			f.mgr.Lock()
			h.Config.Enabled, h.Config.Handlers = true, nil
			f.mgr.Unlock()
			reject(owner, click)
			f.mgr.Lock()
			h.Config.Handlers = h.BaseConfig.Handlers
			f.mgr.Unlock()

			// The producer changes its reference but keeps the row ID. Both the
			// capable token and the legacy name must reject the stale click.
			reply.Data = json.RawMessage(strings.Replace(sky, "sky.png", "new sky.png", 1))
			// A producer cannot capture a stale click by repeating its old
			// visible name and even its daemon-only metadata as a native action.
			copied, err := json.Marshal(wire.Action{Name: click.Action, OfferID: click.OfferID, Secondary: true})
			if err != nil {
				t.Fatal(err)
			}
			impostor := reply
			impostor.Data = json.RawMessage(strings.Replace(string(reply.Data), `{"name":"open","default":true}`, string(copied), 1))
			f.store.SubmitResponse("file_finder", impostor, 0, f.ui.publishSnapshot)
			refreshSnapshot(t, func(v any) { owner.read(t, v) }, ack.QueryID, func(s aggregate) bool {
				return string(s.Results["file_finder"].Data) == string(impostor.Data)
			})
			reject(owner, click)
			f.store.SubmitResponse("file_finder", reply, 0, f.ui.publishSnapshot)
			fresh := readOffer()
			reject(owner, click)
			if _, err := os.Stat(os.Getenv("OFFER_LOG")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("handler ran during discovery or a rejected select: %v", err)
			}
			receipt := owner.selectRow(t, fresh)
			if capable {
				deadline := time.Now().Add(time.Second)
				for !f.mgr.IsRunning("wallpaper") {
					if time.Now().After(deadline) {
						t.Fatal("verified select did not start the cold handler")
					}
					time.Sleep(time.Millisecond)
				}
				handler := f.connectPlugin(t, "wallpaper")
				var sel wire.SelectRequest
				handler.read(t, &sel)
				if sel.Type != wire.MsgSelect || sel.Action != "set" || sel.ResultID != "/pictures/new sky.png" || sel.QueryID != "" || sel.Context != "" {
					t.Fatalf("first frame to cold handler: %+v", sel)
				}
				handler.send(t, wire.SelectResponse{Type: wire.MsgSelectResponse, Success: true, Next: &wire.ActionNext{Type: "refresh"}})
				handler.expectNoWrite(t)
			}
			owner.outcome(t, receipt, true, "")
			if tc.replay {
				var refreshed wire.PluginRequest
				finder.read(t, &refreshed)
				if refreshed != initial {
					t.Fatalf("refresh changed source: %+v, want %+v", refreshed, initial)
				}
				finder.send(t, reply)
				readOffer()
				data, err := os.ReadFile(os.Getenv("OFFER_LOG"))
				if err != nil || string(data) != "4|tarragon|select|/pictures/new sky.png|set|\n" {
					t.Fatalf("unexpected on-call argv/env or invocation count: %q (%v)", data, err)
				}
			}
			finder.expectNoWrite(t)
			other.expectNoWrite(t)
			observer.expectStatus(t)
			owner.send(t, wire.UIRequest{Type: "detach"})
			var detached wire.SelectAck
			owner.read(t, &detached)
			reject(owner, fresh)
		})
	}
}
