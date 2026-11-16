package daemon

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/config"
	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/spf13/viper"
)

func TestPluginOverviewOverUIConnection(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	root := t.TempDir()
	path := filepath.Join(root, "tarragon.toml")
	if err := os.WriteFile(path, []byte("prefix_symbol = ':'\n[plugins.example]\nprefix = 'ex'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	viper.SetConfigFile(path)
	if err := config.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "example")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	const manifest = "name='Example Tools'\nenabled=false\nlifecycle_mode='on_demand_persistent'\nprefix='example'\n"
	if err := os.WriteFile(filepath.Join(dir, "plugin.toml"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	broken := filepath.Join(root, "broken")
	if err := os.Mkdir(broken, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(broken, "plugin.toml"), []byte("name='Broken'\nenabled=false\nlifecycle_mode='daemon'\nentrypoint='missing'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	mgr := plugins.NewManager(root)
	if err := mgr.Discover(); err != nil {
		t.Fatal(err)
	}
	if err := mgr.ApplyOverrides(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		store := NewTransientQueryStore()
		handleUIClient(ctx, server, mgr, nil, &pluginRegistry{}, store, newUIRegistry(store), nil)
	}()
	t.Cleanup(func() { cancel(); client.Close(); <-done })
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	scanner := wire.NewScanner(client)
	send := func(request wire.UIRequest) {
		t.Helper()
		if err := wire.WriteMsg(client, request); err != nil {
			t.Fatal(err)
		}
	}
	read := func(msg any) {
		t.Helper()
		if err := wire.ReadMsg(scanner, msg); err != nil {
			t.Fatal(err)
		}
	}
	var ack wire.AckMessage
	query := func(input string) {
		t.Helper()
		send(wire.UIRequest{Type: "query", Text: input})
		read(&ack)
		if ack.Type != "ack" || ack.QueryID == "" {
			t.Fatalf("invalid query ack: %+v", ack)
		}
	}
	results := func() []wire.ResultItem {
		t.Helper()
		for {
			var update wire.UpdateMessage
			read(&update)
			if update.Type != "update" || update.QueryID != ack.QueryID {
				t.Fatalf("unexpected update %+v", update)
			}
			var ag aggregate
			if err := json.Unmarshal(update.Payload, &ag); err != nil {
				t.Fatal(err)
			}
			state := ag.Plugins[plugins.OverviewID].State
			if state == "done" || state == "empty" {
				return ag.List
			}
			if state != "pending" {
				t.Fatalf("unexpected overview state: %+v", ag.Plugins)
			}
		}
	}
	selectAction := func(id, action string, wantSuccess bool) string {
		t.Helper()
		send(wire.UIRequest{Type: "select", Plugin: plugins.OverviewID, QueryID: ack.QueryID, ResultID: id, Action: action})
		var ok wire.AckMessage
		read(&ok)
		var resp wire.SelectResponse
		read(&resp)
		if ok.Type != "ok" || resp.Type != wire.MsgSelectResponse || resp.Success != wantSuccess || resp.Message == "" {
			t.Fatalf("%s %s: %+v, %+v", action, id, ok, resp)
		}
		return resp.Message
	}

	query(":plugins TOOLs DISABLED")
	rows := results()
	if len(rows) != 1 || rows[0].ID != "example" || rows[0].Plugin != plugins.OverviewID || !strings.Contains(rows[0].Description, ":ex") {
		t.Fatalf("expected searchable disabled plugin with its effective prefix: %+v", rows)
	}
	selectAction("example", "enable", true)
	if rows := results(); len(rows) != 0 {
		t.Fatalf("enabled plugin still matched disabled filter: %+v", rows)
	}
	query(":plugins example")
	rows = results()
	if len(rows) != 1 || rows[0].Actions[0].Type != wire.ActionTypeQueryReplace || rows[0].Actions[0].Query != ":ex " {
		t.Fatalf("expected open to target the effective prefix: %+v", rows)
	}
	if msg := selectAction("example", "restart", true); !strings.Contains(msg, "next matching query") {
		t.Fatalf("incorrect on-demand restart outcome: %q", msg)
	}
	results()
	selectAction("example", "disable", true)
	rows = results()
	if len(rows) != 1 || !strings.Contains(rows[0].Description, "disabled") {
		t.Fatalf("disable did not refresh the overview: %+v", rows)
	}
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), "enabled = false") || !strings.Contains(string(data), `prefix = "ex"`) {
		t.Fatalf("expected persisted change preserving prefix: %s (%v)", data, err)
	}
	selectAction("example", "restart", false)
	results()
	selectAction("plugins", "disable", false)
	selectAction("", "restart", false)
	selectAction("ghost", "enable", false)
	selectAction("example", "uninstall", false)

	dropIns := filepath.Join(root, "tarragon.d")
	if err := os.Mkdir(dropIns, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dropIns, "local.toml"), []byte("[plugins.example]\nenabled = false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if msg := selectAction("example", "enable", false); !strings.Contains(msg, "higher-precedence") {
		t.Fatalf("did not report the effective drop-in override: %q", msg)
	}
	if rows := results(); len(rows) != 1 || rows[0].ID != "example" || !strings.Contains(rows[0].Description, "disabled") {
		t.Fatalf("drop-in did not remain effective: %+v", rows)
	}
	query(":plugins broken")
	if rows := results(); len(rows) != 1 || !strings.Contains(rows[0].Description, "disabled") {
		t.Fatalf("unexpected broken plugin before enabling: %+v", rows)
	}
	if msg := selectAction("broken", "enable", false); !strings.Contains(msg, "failed to start") {
		t.Fatalf("lost the partial failure: %q", msg)
	}
	if rows := results(); len(rows) != 1 || rows[0].ID != "broken" || !strings.Contains(rows[0].Description, "enabled, disconnected") {
		t.Fatalf("effective saved state missing after failed startup: %+v", rows)
	}
}

func TestOverviewActionsRespectRoutingAndLifecycle(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	entry := writeScript(t, t.TempDir(), "probe", "#!/bin/sh\nprintf '[\"%s\"]' \"$TARRAGON_PLUGIN_ID\"\n")
	f.configure("a", plugins.LifecycleDaemon, true, "")
	f.configure("b", plugins.LifecycleOnCall, true, entry)
	f.configure("c", plugins.LifecycleDaemon, false, "")
	f.configure("d", plugins.LifecycleOnCall, true, entry)
	f.configure(plugins.OverviewID, plugins.LifecycleOnCall, true, "")
	f.mgr.Plugins["a"].Config.ResolvedPrefix = "@x"
	f.mgr.Plugins["b"].Config.ResolvedPrefix = "@x"
	f.mgr.Plugins[plugins.OverviewID].Config.ResolvedPrefix = "@plugins"
	f.registry.bus = testBus(t, t.Context())
	for _, features := range [][]string{nil, {navigationFeature}} {
		peer := f.connectUI(t, f.requests)
		u := navigationUI{views: []navigationView{{destination: wire.Destination{Text: "@plugins"}}}, features: features}
		overview := navigationSnapshot(t, peer, u.query(), plugins.OverviewID)
		if len(overview.List) != 5 {
			t.Fatalf("expected five overview rows, got %+v", overview.List)
		}
		for _, row := range overview.List {
			if row.ID == plugins.OverviewID {
				continue
			}
			want := []string{"details", "disable"}
			switch row.ID {
			case "a":
				want = []string{"details", "restart", "disable"}
			case "c":
				want = []string{"details", "enable"}
			}
			if row.ID == "a" || len(features) > 0 && row.ID != "c" {
				want = append([]string{"open"}, want...)
				open := row.Actions[0]
				if len(features) > 0 {
					if open.Type != "open" || open.Destination == nil || open.Destination.Plugin != row.ID {
						t.Fatalf("expected scoped Open for %s: %+v", row.ID, open)
					}
					if row.ID == "b" || row.ID == "d" {
						snap := navigationSnapshot(t, peer, u.activate(row, open), row.ID)
						if len(snap.Plugins) != 1 || len(snap.List) != 1 || snap.List[0].ID != row.ID {
							t.Fatalf("Open failed to reach %s: %+v", row.ID, snap.List)
						}
						u.back()
					}
				} else if open.Type != "query_replace" || open.Query != "@x " {
					t.Fatalf("bad legacy replacement: %+v", open)
				}
			}
			var names []string
			for _, action := range row.Actions {
				names = append(names, action.Name)
			}
			if !row.Actions[0].Default || !slices.Equal(names, want) {
				t.Fatalf("%s, features %v: unexpected actions %+v", row.ID, features, row.Actions)
			}
		}
	}
}
