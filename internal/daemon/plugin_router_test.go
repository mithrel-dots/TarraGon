package daemon

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

func TestPluginRegistryBasic(t *testing.T) {
	r := &pluginRegistry{}
	if r.isConnected("p1") {
		t.Fatalf("unexpected connected before set")
	}
	c1, c2 := net.Pipe()
	defer func() { _ = c2.Close() }()
	r.set("p1", c1)
	if !r.isConnected("p1") {
		t.Fatalf("expected connected")
	}
	c, ok := r.getConn("p1")
	if !ok || c == nil {
		t.Fatalf("unexpected conn: %v %v", c, ok)
	}
	restarted, peer := net.Pipe()
	defer peer.Close()
	r.set("p1", restarted)
	r.remove("p1", c1)
	if conn, _ := r.getConn("p1"); conn != restarted {
		t.Fatal("old connection removed the restarted plugin")
	}
	r.remove("p1", restarted)
	if r.isConnected("p1") {
		t.Fatalf("expected disconnected after remove")
	}
}

func TestHandlePluginConn_DropsUnsolicitedSelectResponse(t *testing.T) {
	f := persistentFixture(t, false, time.Second)
	plugin := persistentPlugin(t, f, "p")
	owner, observer := f.connectUI(t, f.requests), f.connectUI(t, f.requests)
	plugin.reply(t, "", "unsolicited", true)
	plugin.readBarrier(t)
	owner.expectStatus(t)
	observer.expectStatus(t)

	a := owner.selectRow(t, click("clicked"))
	plugin.expectSelect(t, "clicked", "")
	plugin.reply(t, "", "executed", true)
	if resp := owner.outcome(t, a, true, ""); resp.Message != "executed" {
		t.Fatal(resp)
	}
	observer.expectStatus(t)
	owner.expectStatus(t)
}

func TestPluginListener_SendSelectRequestShape(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newAggregateStore(10, "global", nil, 0.3)
	uiReg := newUIRegistry(store)
	reqOut, reg := testPluginListener(t, ctx, store, uiReg)

	conn, err := dialUnixWithRetry(wire.ResolvePluginsSocketPath(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial plugin socket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := wire.WriteMsg(conn, &wire.PluginHello{Type: wire.MsgHello, Name: "plug_select"}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for !reg.isConnected("plug_select") {
		if time.Now().After(deadline) {
			t.Fatal("plugin did not become connected in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	scanner := wire.NewScanner(conn)
	// A queued query can be superseded before the router reads it. A select
	// for a result the UI already showed must still reach the plugin.
	store.startQuery(ctx, "q-123", "ui", "old", "session", "search")
	store.startQuery(ctx, "q-124", "ui", "new", "session", "search")
	registered, _ := reg.getConn("plug_select")
	reqOut <- pluginRequest{name: "plug_select", queryID: "q-123", text: "old", conn: registered}
	mgr := plugins.NewManager("")
	mgr.Plugins["plug_select"] = &plugins.Plugin{Config: plugins.PluginConfig{Enabled: true, Lifecycle: plugins.LifecycleDaemon}}
	server, ui := admissionSocket(t)
	go handleUIClient(ctx, server, mgr, reqOut, reg, store, uiReg, nil)
	ui.selectRow(t, wire.UIRequest{Plugin: "plug_select", QueryID: "q-123", ResultID: "r-42", Action: "open"})

	var raw map[string]json.RawMessage
	if err := wire.ReadMsg(scanner, &raw); err != nil {
		t.Fatalf("read select request: %v", err)
	}

	var typ, qid, rid, action string
	_ = json.Unmarshal(raw["type"], &typ)
	_ = json.Unmarshal(raw["query_id"], &qid)
	_ = json.Unmarshal(raw["result_id"], &rid)
	_ = json.Unmarshal(raw["action"], &action)

	if typ != wire.MsgSelect || qid != "q-123" || rid != "r-42" || action != "open" {
		t.Fatalf("unexpected select request payload: type=%q query_id=%q result_id=%q action=%q", typ, qid, rid, action)
	}
	if _, tagged := raw["action_id"]; tagged {
		t.Fatal("legacy select carried an action_id")
	}
}
