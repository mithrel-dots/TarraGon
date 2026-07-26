package daemon

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/db"
	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/nats-io/nats.go"
)

func testBus(t testing.TB, ctx context.Context) *daemonBus {
	t.Helper()
	b, err := startNATS(ctx, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.close)
	return b
}

// A socket-only exchange must also be observable on the broker. A working
// socket round trip alone would not catch an accidental direct-to-core path.
func TestBrokerCarriesUnixAndNativeRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, err := startNATS(ctx, -1, "test-token")
	if err != nil {
		t.Fatal(err)
	}
	defer b.close()
	ui := newUIRegistry()
	store := NewTransientQueryStore()
	requests, registry := startPluginRouter(b.ctx, store)
	registry.bus = b
	manager := plugins.NewManager(t.TempDir())
	manager.Plugins["native"] = &plugins.Plugin{Config: plugins.PluginConfig{
		Name: "native", Enabled: true, ProvidesGeneral: true, Lifecycle: plugins.LifecycleDaemon,
	}}
	handleUI := func(c peerConn) { handleUIClient(b.ctx, c, manager, requests, registry, store, ui, nil) }
	handlePlugin := func(c peerConn) { handlePluginConn(b.ctx, c, registry, store, ui) }
	if err := b.listenNative(handleUI, handlePlugin); err != nil {
		t.Fatal(err)
	}
	client, err := nats.Connect(b.server.ClientURL(), nats.Token("test-token"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	watch, err := client.SubscribeSync("tarragon.session.*.*")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ui.sock")
	ln, err := b.listenUnix(path, handleUI)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	socket, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	_ = socket.SetDeadline(time.Now().Add(3 * time.Second))
	if err := wire.WriteMsg(socket, wire.UIRequest{Type: "status"}); err != nil {
		t.Fatal(err)
	}
	if !wire.NewScanner(socket).Scan() {
		t.Fatal("no socket status reply")
	}
	request, err := watch.NextMsgWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := watch.NextMsgWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(request.Subject, ".in") || reply.Subject != strings.TrimSuffix(request.Subject, ".in")+".out" {
		t.Fatalf("unexpected routes %s → %s", request.Subject, reply.Subject)
	}

	openSession := func(role string) (string, *nats.Subscription) {
		t.Helper()
		open, err := client.RequestWithContext(ctx, "tarragon."+role+".open", nil)
		if err != nil {
			t.Fatal(err)
		}
		base := string(open.Data)
		sub, err := client.SubscribeSync(base + ".out")
		if err != nil {
			t.Fatal(err)
		}
		return base + ".in", sub
	}
	receive := func(sub *nats.Subscription, v any) {
		t.Helper()
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(msg.Data, v); err != nil {
			t.Fatal(err)
		}
	}
	pluginIn, pluginOut := openSession("plugin")
	if err := client.Publish(pluginIn, []byte(`{"type":"hello","name":"native"}`)); err != nil {
		t.Fatal(err)
	}
	for !registry.isConnected("native") {
		select {
		case <-ctx.Done():
			t.Fatal("native plugin did not register")
		case <-time.After(time.Millisecond):
		}
	}
	uiIn, uiOut := openSession("ui")
	if err := client.Publish(uiIn, []byte(`{"type":"query","text":"hello"}`)); err != nil {
		t.Fatal(err)
	}
	var ack wire.AckMessage
	receive(uiOut, &ack)
	var req wire.PluginRequest
	receive(pluginOut, &req)
	if ack.Type != "ack" || ack.QueryID == "" || req.QueryID != ack.QueryID || req.Text != "hello" {
		t.Fatalf("ack %+v, plugin request %+v", ack, req)
	}
	data, err := json.Marshal(wire.PluginResponse{Type: wire.MsgResponse, QueryID: req.QueryID, Data: json.RawMessage(`{"id":"match","label":"Hello"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(pluginIn, data); err != nil {
		t.Fatal(err)
	}
	for {
		var update wire.UpdateMessage
		receive(uiOut, &update)
		var snap aggregate
		if err := json.Unmarshal(update.Payload, &snap); err != nil || update.Type != "update" || update.QueryID != ack.QueryID {
			t.Fatalf("invalid update for %s: %v", update.QueryID, err)
		}
		if snap.Plugins["native"].State == "pending" {
			continue
		}
		if snap.Plugins["native"].State != "done" || len(snap.List) != 1 || snap.List[0].ID != "match" || snap.List[0].Plugin != "native" {
			t.Fatalf("unexpected final snapshot: %+v", snap)
		}
		break
	}
	for _, in := range []string{uiIn, pluginIn} {
		if err := client.Publish(in, []byte("close")); err != nil {
			t.Fatal(err)
		}
	}
}

func TestShutdownReleasesBlockedSelect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, client := net.Pipe()
	defer client.Close()
	plugin, other := net.Pipe()
	defer other.Close()
	defer plugin.Close()
	registry := &pluginRegistry{}
	registry.set("p", plugin, wire.NewScanner(plugin))
	requests := make(chan pluginRequest)
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleUIClient(ctx, server, nil, requests, registry, NewTransientQueryStore(), newUIRegistry(), nil)
	}()
	if err := wire.WriteMsg(client, wire.UIRequest{Type: "select", Plugin: "p", QueryID: "q", ResultID: "r"}); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("session waited for a stopped router")
	}
}

func testPluginListener(t testing.TB, ctx context.Context, store *aggregateStore, ui *uiRegistry) (chan<- pluginRequest, *pluginRegistry) {
	t.Helper()
	requests, registry := startPluginRouter(ctx, store)
	registry.bus = testBus(t, ctx)
	ln, err := registry.bus.listenUnix(wire.ResolvePluginsSocketPath(), func(c peerConn) { handlePluginConn(ctx, c, registry, store, ui) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return requests, registry
}

func testUIListener(t testing.TB, ctx context.Context, mgr *plugins.Manager, requests chan<- pluginRequest, registry *pluginRegistry, store *aggregateStore, ui *uiRegistry, database *db.DB) {
	t.Helper()
	if registry.bus == nil {
		registry.bus = testBus(t, ctx)
	}
	ln, err := registry.bus.listenUnix(wire.ResolveUISocketPath(), func(c peerConn) { handleUIClient(ctx, c, mgr, requests, registry, store, ui, database) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
}
