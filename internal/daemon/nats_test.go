package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		Name: "native", Enabled: true, ProvidesGeneral: true, Lifecycle: plugins.LifecycleDaemon, Capabilities: []string{wire.MsgEnd},
	}}
	dir := t.TempDir()
	actionGate := openOnCallTestGate(t, dir)
	action := writeScript(t, dir, "effect", `#!/bin/sh
cd "${0%/*}" || exit 1
echo $$ > pid
read -r value < gate
echo "$value" > "$3.done"
`)
	manager.Plugins["effect"] = &plugins.Plugin{Config: plugins.PluginConfig{
		Name: "effect", Enabled: true, Lifecycle: plugins.LifecycleOnCall, Entrypoint: action,
	}}
	handleUI := func(c peerConn) { handleUIClient(b.ctx, c, manager, requests, registry, store, ui, nil) }
	handlePlugin := func(c peerConn) { handlePluginConn(b.ctx, c, registry, store, ui) }
	var shortIdle atomic.Bool
	if err := b.listenNative(func(c peerConn) {
		if shortIdle.Swap(false) {
			c.(*subjectPeer).idleTimeout = 500 * time.Millisecond
		}
		handleUI(c)
	}, handlePlugin); err != nil {
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
	path := filepath.Join(t.TempDir(), "ipc", "ui.sock")
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

	openSession := func(role string) (peer, func(bool)) {
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
		}, func(closeInput bool) {
			t.Helper()
			if closeInput {
				send([]byte("close"))
			}
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
	query := func(p peer, text, stream string, watch bool) string {
		t.Helper()
		p.send(wire.UIRequest{Type: "query", Text: text, ClientID: "cli", QueryStream: stream, Watch: watch})
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
	unixQuery := query(unix, "unix query", "search", false)
	nativeQuery := query(native, "hello", "search", false)
	status(observer)
	respond(nativeQuery, `{"id":"match","label":"Hello"}`)
	respond(unixQuery, `[]`)
	snap := readSnapshot(native, nativeQuery, "hello", "done")
	if len(snap.List) != 1 || snap.List[0].ID != "match" || snap.List[0].Plugin != "native" {
		t.Fatalf("unexpected native results: %+v", snap.List)
	}
	readSnapshot(unix, unixQuery, "unix query", "empty")
	respond(unixQuery, `["updated"]`)
	snap = readSnapshot(unix, unixQuery, "unix query", "done")
	if len(snap.List) != 1 || snap.List[0].ID != "updated" {
		t.Fatalf("lost replacement from the original producer: %+v", snap.List)
	}

	keep := query(unix, "still live", "", true)
	plugin.send(wire.PluginResponse{Type: wire.MsgResponse, QueryID: keep, Live: true, Data: json.RawMessage(`["unix"]`)})
	readSnapshot(unix, keep, "still live", "done")
	unixUsage := totalWatchUsage(store)
	detached := query(native, "detach me", "", true)
	native.send(wire.UIRequest{Type: "detach", ClientID: "renamed"})
	var ack wire.AckMessage
	native.read(&ack)
	if ack.Type != "ok" || store.query(detached) != nil || store.query(nativeQuery) != nil {
		t.Fatalf("detach did not end the owner's queries: %+v", ack)
	}
	var end wire.AckMessage
	plugin.read(&end)
	if end.Type != wire.MsgEnd || end.QueryID != detached {
		t.Fatalf("unexpected plugin frame on detach: %+v", end)
	}
	for _, reason := range []string{"close", "idle"} {
		if reason == "idle" {
			shortIdle.Store(true)
			native, closeNative = openSession("ui")
		}
		streamed := query(native, reason+" streamed", "search", true)
		unstreamed := query(native, reason+" unstreamed", "", true)
		plugin.send(wire.PluginResponse{Type: wire.MsgResponse, QueryID: unstreamed, Live: true, Data: json.RawMessage(`["native"]`)})
		readSnapshot(native, unstreamed, reason+" unstreamed", "done")
		if u := totalWatchUsage(store); u.count != 3 || u.bytes <= unixUsage.bytes {
			t.Fatalf("expected two native watches and one Unix watch, got %+v", u)
		}
		native.send(wire.UIRequest{Type: wire.MsgSelect, QueryID: unstreamed, Plugin: "effect", ResultID: reason})
		var actionAck wire.SelectAck
		native.read(&actionAck)
		if actionAck.Type != "ok" || actionAck.ActionID == "" {
			t.Fatalf("bad select receipt: %+v", actionAck)
		}
		onCallChildPID(t, dir)
		closeNative(reason == "close")
		if u := totalWatchUsage(store); u != unixUsage {
			t.Fatalf("native %s left watch usage %+v, want Unix usage %+v", reason, u, unixUsage)
		}
		want := map[string]bool{streamed: true, unstreamed: true}
		for len(want) != 0 {
			plugin.read(&end)
			if end.Type != wire.MsgEnd || !want[end.QueryID] || store.query(end.QueryID) != nil {
				t.Fatalf("unexpected plugin frame or retained query on native %s: %+v", reason, end)
			}
			delete(want, end.QueryID)
			respond(end.QueryID, `["late"]`)
			plugin.send(wire.PluginResponse{Type: wire.MsgLiveUpdate, QueryID: end.QueryID, Data: json.RawMessage(`["late push"]`)})
		}
		if _, err := actionGate.WriteString("finished\n"); err != nil {
			t.Fatal(err)
		}
		for {
			data, _ := os.ReadFile(filepath.Join(dir, reason+".done"))
			if string(data) == "finished\n" {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("native %s cancelled the select process", reason)
			case <-time.After(time.Millisecond):
			}
		}
		if err := os.Remove(filepath.Join(dir, "pid")); err != nil {
			t.Fatal(err)
		}
	}
	replacement, _ := openSession("ui")
	status(replacement)
	respond(detached, `["late detach reply"]`)
	plugin.send(wire.PluginResponse{Type: wire.MsgLiveUpdate, QueryID: keep, Data: json.RawMessage(`["still watching"]`)})
	readSnapshot(unix, keep, "still live", "done")
	store.WaitForPublications()
	status(unix)
	status(replacement)
	status(observer)
	socket.Close()
	plugin.read(&end)
	if end.Type != wire.MsgEnd || end.QueryID != keep || totalWatchUsage(store) != (watchUsage{}) {
		t.Fatalf("Unix disconnect did not release its watch: %+v %+v", end, totalWatchUsage(store))
	}
}

func totalWatchUsage(s *aggregateStore) watchUsage {
	s.watches.mu.Lock()
	defer s.watches.mu.Unlock()
	return s.watches.total
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
	registry.set("p", plugin)
	manager := plugins.NewManager(t.TempDir())
	manager.Plugins["p"] = &plugins.Plugin{Config: plugins.PluginConfig{
		ID: "p", Name: "p", Enabled: true, Lifecycle: plugins.LifecycleDaemon,
	}}
	requests := make(chan pluginRequest)
	done := make(chan struct{})
	store := NewTransientQueryStore()
	ui := newUIRegistry(store)
	go func() {
		defer close(done)
		handleUIClient(ctx, server, manager, requests, registry, store, ui, nil)
	}()
	if err := wire.WriteMsg(client, wire.UIRequest{Type: "select", Plugin: "p", QueryID: "q", ResultID: "r"}); err != nil {
		t.Fatal(err)
	}
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var ack wire.SelectAck
	if err := wire.ReadMsg(wire.NewScanner(client), &ack); err != nil || ack.Type != "ok" || ack.ActionID == "" {
		t.Fatalf("expected select admission before shutdown: %+v (%v)", ack, err)
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
	t.Setenv("TARRAGON_PLUGINS_SOCKET", filepath.Join(t.TempDir(), "ipc", "p"))
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
	t.Setenv("TARRAGON_UI_SOCKET", filepath.Join(t.TempDir(), "ipc", "u"))
	if registry.bus == nil {
		registry.bus = testBus(t, ctx)
	}
	ln, err := registry.bus.listenUnix(wire.ResolveUISocketPath(), func(c peerConn) { handleUIClient(ctx, c, mgr, requests, registry, store, ui, database) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
}
