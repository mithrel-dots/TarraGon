package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/db"
	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
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

func TestNativeListenerRejectsInvalidPorts(t *testing.T) {
	defer viper.Reset()
	viper.Set("nats_tcp", true)
	for _, raw := range []any{0, -1, 65536, 1.5, true, "invalid"} {
		viper.Set("nats_port", raw)
		if port, err := natsListenPort(); err == nil {
			t.Errorf("accepted %v as port %d", raw, port)
		}
	}
}

func TestNativeListenerWithToken(t *testing.T) {
	b, err := startNATS(t.Context(), -1, "test-token")
	if err != nil {
		t.Fatal(err)
	}
	defer b.close()
	for _, token := range []string{"", "wrong-token"} {
		client, err := nats.Connect(b.server.ClientURL(), nats.Token(token))
		if err == nil {
			client.Close()
			t.Fatal("listener accepted a client without the configured token")
		}
		if !errors.Is(err, nats.ErrAuthorization) {
			t.Fatalf("expected authorization error, got %v", err)
		}
	}
	client, err := nats.Connect(b.server.ClientURL(), nats.Token("test-token"))
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
}

// A socket-only exchange must also be observable on the broker. A working
// socket round trip alone would not catch an accidental direct-to-core path.
func TestBrokerCarriesUnixAndNativeRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, err := startNATS(ctx, -1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer b.close()
	store := NewTransientQueryStore()
	ui := newUIRegistry(store)
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
	client, err := nats.Connect(b.server.ClientURL())
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
	deadline, _ := ctx.Deadline()
	_ = socket.SetDeadline(deadline)
	type peer struct {
		send func(any)
		read func(any)
	}
	scanner := wire.NewScanner(socket)
	unix := peer{
		send: func(v any) {
			t.Helper()
			if err := wire.WriteMsg(socket, v); err != nil {
				t.Fatal(err)
			}
		},
		read: func(v any) {
			t.Helper()
			if err := wire.ReadMsg(scanner, v); err != nil {
				t.Fatal(err)
			}
		},
	}
	status := func(p peer) {
		t.Helper()
		p.send(wire.UIRequest{Type: "status", ClientID: "cli"})
		var reply wire.StatusResponse
		p.read(&reply)
		if reply.Type != "status" {
			t.Fatalf("expected status, received %q", reply.Type)
		}
	}
	status(unix)
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

	openSession := func(role string) (peer, func()) {
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
		if err := client.Flush(); err != nil {
			t.Fatal(err)
		}
		next := func() []byte {
			t.Helper()
			msg, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				t.Fatal(err)
			}
			return msg.Data
		}
		send := func(data []byte) {
			t.Helper()
			if err := client.Publish(base+".in", data); err != nil {
				t.Fatal(err)
			}
		}
		return peer{
			send: func(v any) {
				t.Helper()
				data, err := json.Marshal(v)
				if err != nil {
					t.Fatal(err)
				}
				send(data)
			},
			read: func(v any) {
				t.Helper()
				if err := json.Unmarshal(next(), v); err != nil {
					t.Fatal(err)
				}
			},
		}, func() {
			t.Helper()
			send([]byte("close"))
			if got := string(next()); got != "close" {
				t.Fatalf("expected session close, got %s", got)
			}
		}
	}
	plugin, _ := openSession("plugin")
	plugin.send(wire.PluginHello{Type: wire.MsgHello, Name: "native"})
	for !registry.isConnected("native") {
		select {
		case <-ctx.Done():
			t.Fatal("native plugin did not register")
		case <-time.After(time.Millisecond):
		}
	}
	native, closeNative := openSession("ui")
	observer, _ := openSession("ui")
	status(observer)
	readSnapshot := func(p peer, qid, input, state string) aggregate {
		t.Helper()
		var update wire.UpdateMessage
		p.read(&update)
		var snap aggregate
		if err := json.Unmarshal(update.Payload, &snap); err != nil {
			t.Fatal(err)
		}
		if update.Type != "update" || update.QueryID != qid || snap.QueryID != qid || snap.Input != input || snap.Plugins["native"].State != state {
			t.Fatalf("want %s / %q / %s, received %s %+v", qid, input, state, update.Type, snap)
		}
		return snap
	}
	query := func(p peer, text, stream string) string {
		t.Helper()
		p.send(wire.UIRequest{Type: "query", Text: text, ClientID: "cli", QueryStream: stream})
		var ack wire.AckMessage
		p.read(&ack)
		if ack.Type != "ack" || ack.QueryID == "" {
			t.Fatalf("expected query ack, got %+v", ack)
		}
		readSnapshot(p, ack.QueryID, text, "pending")
		var req wire.PluginRequest
		plugin.read(&req)
		if req.Type != wire.MsgRequest || req.QueryID != ack.QueryID || req.Text != text {
			t.Fatalf("unexpected plugin request: %+v", req)
		}
		return req.QueryID
	}
	respond := func(qid, data string) {
		plugin.send(wire.PluginResponse{Type: wire.MsgResponse, QueryID: qid, Data: json.RawMessage(data)})
	}

	// Hold replies until both peers have pending lists with the same label
	// and stream. The third peer shares the native UI's broker connection.
	unixQuery := query(unix, "unix query", "search")
	nativeQuery := query(native, "hello", "search")
	status(observer)
	respond(nativeQuery, `{"id":"match","label":"Hello"}`)
	respond(unixQuery, `[]`)
	snap := readSnapshot(native, nativeQuery, "hello", "done")
	if len(snap.List) != 1 || snap.List[0].ID != "match" || snap.List[0].Plugin != "native" {
		t.Fatalf("unexpected native results: %+v", snap.List)
	}
	readSnapshot(unix, unixQuery, "unix query", "empty")

	keep := query(unix, "still live", "")
	detached := query(native, "detach me", "")
	native.send(wire.UIRequest{Type: "detach", ClientID: "renamed"})
	var ack wire.AckMessage
	native.read(&ack)
	if ack.Type != "ok" || store.query(detached) != nil || store.query(nativeQuery) != nil {
		t.Fatalf("detach did not end the owner's queries: %+v", ack)
	}
	closed := query(native, "close me", "")
	closeNative()
	if store.query(closed) != nil {
		t.Fatal("closed native session retained its non-stream query")
	}
	replacement, _ := openSession("ui")
	status(replacement)
	respond(detached, `["late detach reply"]`)
	respond(closed, `["late close reply"]`)
	respond(keep, `{"error":"offline"}`)
	readSnapshot(unix, keep, "still live", "error")
	store.WaitForPublications()
	status(unix)
	status(replacement)
	status(observer)
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
	store := NewTransientQueryStore()
	go func() {
		defer close(done)
		handleUIClient(ctx, server, nil, requests, registry, store, newUIRegistry(store), nil)
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
