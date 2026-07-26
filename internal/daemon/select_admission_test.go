package daemon

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

type admissionFixture struct {
	mgr      *plugins.Manager
	store    *aggregateStore
	ui       *uiRegistry
	requests chan<- pluginRequest
	registry *pluginRegistry
}

func newAdmissionFixture(t *testing.T, timeout time.Duration) *admissionFixture {
	t.Helper()
	store := NewTransientQueryStore()
	ui := newUIRegistry(store)
	ui.actionTimeout = timeout
	requests, registry := startPluginRouter(t.Context(), store)
	mgr := plugins.NewManager(t.TempDir())
	registry.manager = mgr
	t.Cleanup(mgr.StopAll)
	return &admissionFixture{mgr: mgr, store: store, ui: ui, requests: requests, registry: registry}
}

func (f *admissionFixture) configure(id string, mode plugins.LifecycleMode, enabled bool, entry string) {
	cfg := plugins.PluginConfig{ID: id, Name: id, Lifecycle: mode, Enabled: enabled, Entrypoint: entry, ProvidesGeneral: true}
	f.mgr.Plugins[id] = &plugins.Plugin{Config: cfg, BaseConfig: cfg}
}

type admissionPeer struct {
	net.Conn
	scanner *bufio.Scanner
}

// A buffered socket lets a UI's writer deliver a frame while the test is
// reading the other UI or the plugin, just as it can with separate clients.
func admissionSocket(t *testing.T) (net.Conn, admissionPeer) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	client, err := net.Dial("tcp4", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return server, admissionPeer{Conn: client, scanner: wire.NewScanner(client)}
}

func (f *admissionFixture) connectUI(t *testing.T, requests chan<- pluginRequest) admissionPeer {
	t.Helper()
	server, peer := admissionSocket(t)
	go handleUIClient(t.Context(), server, f.mgr, requests, f.registry, f.store, f.ui, nil)
	peer.expectStatus(t) // establishes that this session can already receive replies
	return peer
}

func (f *admissionFixture) connectPlugin(t *testing.T, id string) admissionPeer {
	t.Helper()
	server, peer := admissionSocket(t)
	go handlePluginConn(t.Context(), server, f.registry, f.store, f.ui)
	peer.send(t, wire.PluginHello{Type: wire.MsgHello, Name: id})
	until := time.Now().Add(2 * time.Second)
	for !f.registry.isConnected(id) {
		if time.Now().After(until) {
			t.Fatalf("plugin %q did not register", id)
		}
		time.Sleep(time.Millisecond)
	}
	return peer
}

func (p admissionPeer) send(t *testing.T, msg any) {
	t.Helper()
	if err := p.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := wire.WriteMsg(p.Conn, msg); err != nil {
		t.Fatalf("send %+v: %v", msg, err)
	}
}

func (p admissionPeer) read(t *testing.T, msg any) {
	t.Helper()
	if err := p.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := wire.ReadMsg(p.scanner, msg); err != nil {
		t.Fatalf("read %T: %v", msg, err)
	}
}

// Status is a barrier on this session's writer: a duplicate or foreign
// select_response queued before the status reply must be seen first.
func (p admissionPeer) expectStatus(t *testing.T) {
	t.Helper()
	p.send(t, wire.UIRequest{Type: "status"})
	var msg wire.StatusResponse
	p.read(t, &msg)
	if msg.Type != wire.MsgStatus {
		t.Fatalf("expected status, got %s", msg.Type)
	}
}

// Use only after the last expected plugin frame. A timeout proves the live
// connection received no more bytes; EOF would also hide a broken router.
func (p admissionPeer) expectNoWrite(t *testing.T) {
	t.Helper()
	if err := p.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if p.scanner.Scan() || !errors.Is(p.scanner.Err(), os.ErrDeadlineExceeded) {
		t.Fatalf("expected an idle plugin connection, got %q (%v)", p.scanner.Text(), p.scanner.Err())
	}
}

func (p admissionPeer) selectRow(t *testing.T, req wire.UIRequest) wire.SelectAck {
	t.Helper()
	req.Type = wire.MsgSelect
	p.send(t, req)
	var ack wire.SelectAck
	p.read(t, &ack)
	if ack.Type != "ok" || ack.ActionID == "" || ack.RequestID != req.RequestID {
		t.Fatalf("invalid acknowledgement for %+v: %+v", req, ack)
	}
	return ack
}

func (p admissionPeer) outcome(t *testing.T, ack wire.SelectAck, success bool, code string) wire.SelectResponse {
	t.Helper()
	var resp wire.SelectResponse
	p.read(t, &resp)
	if resp.Type != wire.MsgSelectResponse || resp.ActionID != ack.ActionID || resp.RequestID != ack.RequestID ||
		resp.Success != success || resp.Code != code || (!success && resp.Message == "") {
		t.Fatalf("expected terminal success=%v code=%q for %+v, got %+v", success, code, ack, resp)
	}
	return resp
}

func TestSelectAdmissionRejectsUndispatchableRows(t *testing.T) {
	// Long enough to distinguish rejection from silently waiting for a timeout.
	f := newAdmissionFixture(t, 5*time.Second)
	f.configure("ready", plugins.LifecycleDaemon, true, "")
	f.configure("disabled", plugins.LifecycleDaemon, false, "")
	f.configure("offline", plugins.LifecycleDaemon, true, "")
	f.configure("broken", plugins.LifecycleOnDemandPersistent, true, filepath.Join(t.TempDir(), "missing"))
	// Even a live connection cannot authorize a missing or disabled manifest.
	ready := f.connectPlugin(t, "ready")
	unknown := f.connectPlugin(t, "unconfigured")
	disabled := f.connectPlugin(t, "disabled")
	owner := f.connectUI(t, f.requests)
	observer := f.connectUI(t, f.requests)
	seen := make(map[string]bool)
	for _, tc := range []struct{ name, plugin, row string }{
		{"missing ID", "ready", ""},
		{"missing target", "", "row"},
		{"unknown target", "unconfigured", "row"},
		{"disabled target", "disabled", "row"},
		{"daemon disconnected", "offline", "row"},
		{"failed on-demand spawn", "broken", "row"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ack := owner.selectRow(t, wire.UIRequest{RequestID: "ui-token", QueryID: "old-query", Plugin: tc.plugin, ResultID: tc.row})
			if seen[ack.ActionID] {
				t.Fatalf("reused daemon action_id %q", ack.ActionID)
			}
			seen[ack.ActionID] = true
			owner.outcome(t, ack, false, "not_run")
			owner.expectStatus(t)
			observer.expectStatus(t)
		})
	}
	for _, plugin := range []admissionPeer{ready, unknown, disabled} {
		plugin.expectNoWrite(t)
	}
}

func TestUnixSelectAfterCloseWrite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"keep reading", 500 * time.Millisecond},
		{"disconnect", time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := persistentFixture(t, true, tc.timeout)
			plugin := persistentPlugin(t, f, "p")
			held := make(chan pluginRequest, 1)
			path := filepath.Join(t.TempDir(), "u")
			t.Setenv("TARRAGON_UI_SOCKET", path)
			testUIListener(t, t.Context(), f.mgr, held, f.registry, f.store, f.ui, nil)
			conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Net: "unix", Name: path})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ui := admissionPeer{Conn: conn, scanner: wire.NewScanner(conn)}
			ack := ui.selectRow(t, click("held"))
			req := <-held
			f.requests <- req
			plugin.expectSelect(t, "held", ack.ActionID)
			if err := conn.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			if tc.name == "disconnect" {
				conn.Close()
				select {
				case <-req.selectAction.done:
				case <-time.After(2 * time.Second):
					t.Fatal("full disconnect waited for the action deadline")
				}
				return
			}
			ui.outcome(t, ack, false, "completion_unknown")
			var extra wire.SelectResponse
			if err := wire.ReadMsg(ui.scanner, &extra); err != io.EOF {
				t.Fatalf("expected EOF after terminal reply, got %+v (%v)", extra, err)
			}
		})
	}
}

func TestSelectAdmissionQueueExpiry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		capacity int
	}{{"blocked enqueue", 0}, {"awaiting router", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAdmissionFixture(t, 200*time.Millisecond)
			f.configure("ready", plugins.LifecycleDaemon, true, "")
			plugin := f.connectPlugin(t, "ready")
			held := make(chan pluginRequest, tc.capacity)
			owner := f.connectUI(t, held)
			observer := f.connectUI(t, f.requests)

			start := time.Now()
			ack := owner.selectRow(t, wire.UIRequest{Plugin: "ready", ResultID: "expired", RequestID: "first"})
			// No one reads held, and neither daemon nor plugin is shutting down.
			owner.outcome(t, ack, false, "not_run")
			if elapsed := time.Since(start); elapsed < f.ui.actionTimeout {
				t.Fatalf("select failed before its queue deadline (%v)", elapsed)
			}
			if len(held) != tc.capacity {
				t.Fatalf("expected %d held router entries before restoring reads, got %d", tc.capacity, len(held))
			}
			owner.expectStatus(t)
			observer.expectStatus(t)

			ctx := t.Context()
			go func() {
				for {
					select {
					case req := <-held:
						select {
						case f.requests <- req:
						case <-ctx.Done():
							return
						}
					case <-ctx.Done():
						return
					}
				}
			}()
			fresh := owner.selectRow(t, wire.UIRequest{Plugin: "ready", ID: "live", Action: "open", RequestID: "second"})
			var req wire.SelectRequest
			plugin.read(t, &req)
			if req.Type != wire.MsgSelect || req.ResultID != "live" || req.Action != "open" || fresh.ActionID == ack.ActionID {
				t.Fatalf("after restoring the router, expected only the fresh action, got %+v (ack %+v)", req, fresh)
			}
			plugin.send(t, wire.SelectResponse{Type: wire.MsgSelectResponse, Success: true, ActionID: req.ActionID})
			owner.outcome(t, fresh, true, "")
			owner.expectStatus(t)
			observer.expectStatus(t)
			plugin.expectNoWrite(t)
		})
	}
}

func TestSelectAdmissionSupersededRow(t *testing.T) {
	f := newAdmissionFixture(t, time.Second)
	f.configure("producer", plugins.LifecycleDaemon, true, "")
	plugin := f.connectPlugin(t, "producer")
	owner := f.connectUI(t, f.requests)
	query := func(row string) (string, wire.ResultItem) {
		t.Helper()
		owner.send(t, wire.UIRequest{Type: "query", QueryStream: "launcher", Text: row})
		var ack wire.AckMessage
		owner.read(t, &ack)
		var req wire.PluginRequest
		plugin.read(t, &req)
		if ack.Type != "ack" || ack.QueryID == "" || req.Type != wire.MsgRequest || req.QueryID != ack.QueryID {
			t.Fatalf("query acknowledgement %+v, dispatch %+v", ack, req)
		}
		data, err := json.Marshal([]string{row})
		if err != nil {
			t.Fatal(err)
		}
		plugin.send(t, wire.PluginResponse{Type: wire.MsgResponse, QueryID: ack.QueryID, Data: data})
		for {
			var update wire.UpdateMessage
			owner.read(t, &update)
			if update.Type != "update" || update.QueryID != ack.QueryID {
				t.Fatalf("unexpected update: %+v", update)
			}
			var snap aggregate
			if err := json.Unmarshal(update.Payload, &snap); err != nil {
				t.Fatal(err)
			}
			if snap.Plugins["producer"].State == "pending" {
				continue
			}
			if len(snap.List) != 1 || snap.List[0].ID != row || snap.List[0].Plugin != "producer" {
				t.Fatalf("expected %q from producer, got %+v", row, snap.List)
			}
			return ack.QueryID, snap.List[0]
		}
	}
	oldID, oldRow := query("earlier")
	newID, _ := query("later")
	if oldID == newID || f.store.query(oldID) != nil {
		t.Fatalf("earlier query %q still active after %q", oldID, newID)
	}
	observer := f.connectUI(t, f.requests)
	ack := owner.selectRow(t, wire.UIRequest{QueryID: oldID, Plugin: oldRow.Plugin, ResultID: oldRow.ID, Action: "open"})
	var req wire.SelectRequest
	plugin.read(t, &req)
	if req.Type != wire.MsgSelect || req.QueryID != oldID || req.ResultID != oldRow.ID || req.Action != "open" {
		t.Fatalf("old row was not dispatched intact: %+v", req)
	}
	// The handler, rather than the daemon, decides whether this old row works.
	plugin.send(t, wire.SelectResponse{Type: wire.MsgSelectResponse, ActionID: req.ActionID, Success: false, Message: "row no longer available"})
	resp := owner.outcome(t, ack, false, "")
	if resp.Message != "row no longer available" {
		t.Fatalf("lost handler's outcome: %+v", resp)
	}
	owner.expectStatus(t)
	observer.expectStatus(t)
}

func (f *admissionFixture) onDemandListener(t *testing.T) *net.UnixListener {
	t.Helper()
	dir := t.TempDir()
	endpoint := filepath.Join(dir, "p.sock")
	t.Setenv("TARRAGON_PLUGINS_SOCKET", endpoint)
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Net: "unix", Name: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	if err := ln.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	starts := filepath.Join(dir, "starts")
	t.Setenv("TARRAGON_ADMISSION_TEST_EXE", exe)
	t.Setenv("TARRAGON_ADMISSION_STARTS", starts)
	entry := writeScript(t, dir, "plugin", `#!/bin/sh
echo started >> "$TARRAGON_ADMISSION_STARTS"
exec "$TARRAGON_ADMISSION_TEST_EXE" -test.run='^TestSelectAdmissionPluginProcess$'
`)
	f.configure("ondemand", plugins.LifecycleOnDemandPersistent, true, entry)
	t.Cleanup(func() {
		data, err := os.ReadFile(starts)
		if err != nil || string(data) != "started\n" {
			t.Errorf("expected one process start, got %q (%v)", data, err)
		}
	})
	return ln
}

func TestSelectAdmissionOnDemandRegistration(t *testing.T) {
	f := newAdmissionFixture(t, 2*time.Second)
	ln := f.onDemandListener(t)
	owner := f.connectUI(t, f.requests)
	observer := f.connectUI(t, f.requests)
	ack := owner.selectRow(t, wire.UIRequest{Plugin: "ondemand", ResultID: "later", RequestID: "first-click"})
	// This is the process started by the outstanding select; there was no
	// connected plugin when the router made its dispatch decision.
	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	go handlePluginConn(t.Context(), conn, f.registry, f.store, f.ui)
	if resp := owner.outcome(t, ack, true, ""); resp.Message != "later" {
		t.Fatalf("first select did not reach the newly registered plugin: %+v", resp)
	}
	observer.expectStatus(t)
	owner.expectStatus(t)
}

func TestSelectAdmissionOnDemandOutlivesDeadline(t *testing.T) {
	f := newAdmissionFixture(t, 500*time.Millisecond)
	ln := f.onDemandListener(t)
	first := f.connectUI(t, f.requests)
	second := f.connectUI(t, f.requests)
	req := wire.UIRequest{Plugin: "ondemand", ResultID: "expired", RequestID: "click"}
	start := time.Now()
	a := first.selectRow(t, req)
	b := second.selectRow(t, req)
	if a.ActionID == b.ActionID {
		t.Fatalf("same daemon ID for two independent clicks: %q", a.ActionID)
	}
	conn, err := ln.Accept()
	if err != nil {
		t.Fatalf("on-demand process did not connect: %v", err)
	}
	defer conn.Close()
	// Hold this exact process's hello unread until both actions time out, then
	// let it register while another select is waiting.
	first.outcome(t, a, false, "not_run")
	if elapsed := time.Since(start); elapsed < f.ui.actionTimeout {
		t.Fatalf("select did not wait for registration (%v)", elapsed)
	}
	second.outcome(t, b, false, "not_run")
	req.ResultID, req.Text = "", "later" // legacy select using text as its row ID
	c := first.selectRow(t, req)
	go handlePluginConn(t.Context(), conn, f.registry, f.store, f.ui)
	resp := first.outcome(t, c, true, "")
	if resp.Message != "later" {
		t.Fatalf("process replied for the wrong select: %+v", resp)
	}
	first.expectStatus(t)
	second.expectStatus(t)
}

// Re-executed by the on-demand manager as a real, persistent plugin.
func TestSelectAdmissionPluginProcess(t *testing.T) {
	if os.Getenv("TARRAGON_ADMISSION_TEST_EXE") == "" {
		t.Skip("on-demand plugin subprocess")
	}
	conn, err := net.Dial("unix", os.Getenv("TARRAGON_PLUGINS_ENDPOINT"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	p := admissionPeer{Conn: conn, scanner: wire.NewScanner(conn)}
	p.send(t, wire.PluginHello{Type: wire.MsgHello, Name: os.Getenv("TARRAGON_PLUGIN_ID")})
	var req wire.SelectRequest
	p.read(t, &req)
	if req.Type != wire.MsgSelect || req.ResultID != "later" {
		t.Fatalf("plugin received expired or unexpected action: %+v", req)
	}
	p.send(t, wire.SelectResponse{Type: wire.MsgSelectResponse, Success: true, ActionID: req.ActionID, Message: req.ResultID})
	// Stay attached for the remainder of the daemon's lifetime.
	_ = conn.SetReadDeadline(time.Time{})
	if p.scanner.Scan() {
		t.Fatalf("extra plugin write: %s", p.scanner.Text())
	}
}
