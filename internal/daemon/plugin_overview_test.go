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
	results()
	selectAction("", "restart", false)
	results()
	selectAction("ghost", "enable", false)
	results()
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
}

func TestOverviewActionsRespectRoutingAndLifecycle(t *testing.T) {
	rows := overviewItems([]wire.PluginInfo{
		{ID: "a", Enabled: true, Prefix: "@x", Lifecycle: "on_call"},
		{ID: "b", Enabled: true, Prefix: "@x", Lifecycle: "daemon", Connected: true},
		{ID: "c", Enabled: false, Lifecycle: "daemon"},
	}, "")
	want := [][]string{{"open", "details", "disable"}, {"details", "restart", "disable"}, {"details", "enable"}}
	for i, row := range rows {
		var actions []string
		for _, action := range row.Actions {
			actions = append(actions, action.Name)
		}
		if !row.Actions[0].Default || !slices.Equal(actions, want[i]) {
			t.Fatalf("%s: unexpected actions %+v", row.ID, row.Actions)
		}
	}
}
