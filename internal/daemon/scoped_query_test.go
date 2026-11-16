package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/nats-io/nats.go"
)

func TestScopedOnCallRouting(t *testing.T) {
	t.Setenv("TARRAGON_CONTEXT", "leaked")
	entry := writeScript(t, t.TempDir(), "record", `#!/bin/sh
printf '%s|%s|%s|%s|%s|%s|%s\n' "$TARRAGON_PLUGIN_ID" "$#" "$1" "$2" "$3" "$4" "${TARRAGON_CONTEXT-unset}" >> "$0.log"
case "$2" in
  query) echo '["row"]' ;;
  select) if [ "$4" = mark ]; then echo '{"next":{"type":"refresh"}}'; fi ;;
esac
`)
	f := newAdmissionFixture(t, 2*time.Second)
	f.configure("general", plugins.LifecycleOnCall, true, entry)
	f.mgr.Plugins["general"].Config.ResolvedPrefix = "@g"
	f.configure("catalog", plugins.LifecycleOnCall, true, entry)
	f.mgr.Plugins["catalog"].Config.ProvidesGeneral = false
	f.configure("disabled", plugins.LifecycleOnCall, false, entry)
	testUIListener(t, t.Context(), f.mgr, f.requests, f.registry, f.store, f.ui, nil)
	socket, err := net.Dial("unix", wire.ResolveUISocketPath())
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	u := admissionPeer{Conn: socket, scanner: wire.NewScanner(socket)}
	read := func(v any) { u.read(t, v) }
	var scopedID string
	var wantCalls []string
	for _, tc := range []struct{ name, id, text, context, producer, pluginText string }{
		{"global", "", "  words  ", "", "general", "  words  "},
		{"prefix", "", " @g  terms ", "", "general", "terms"},
		{"prefixless scoped target", "catalog", "", "show:42", "catalog", ""},
		{"literal scoped text", "catalog", "  @g words  ", "  opaque = ✓  ", "catalog", "  @g words  "},
		{"disabled", "disabled", "x", "", "", ""},
		{"unknown", "missing", "x", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u.send(t, wire.UIRequest{Type: "query", Plugin: tc.id, Text: tc.text, Context: tc.context})
			var ack wire.AckMessage
			u.read(t, &ack)
			if ack.Type != "ack" || ack.QueryID == "" {
				t.Fatalf("bad ack: %+v", ack)
			}
			if tc.producer == "" {
				snap := refreshSnapshot(t, read, ack.QueryID, func(q aggregate) bool { return q.Plugins[tc.id].State == "error" })
				if len(snap.Plugins) != 1 || len(snap.List) != 0 || len(snap.Results) != 0 || snap.Plugins[tc.id].Error == "" {
					t.Fatalf("bad unavailable target snapshot: %+v", snap)
				}
				return
			}
			snap := refreshSnapshot(t, read, ack.QueryID, func(q aggregate) bool { return q.Plugins[tc.producer].State == "done" })
			if snap.Input != tc.text || len(snap.Plugins) != 1 || len(snap.List) != 1 || snap.List[0].Plugin != tc.producer {
				t.Fatalf("wrong dispatch: %+v", snap)
			}
			env := tc.context
			if env == "" {
				env = "unset"
			}
			wantCalls = append(wantCalls, tc.producer+"|3|tarragon|query|"+tc.pluginText+"||"+env)
			if tc.context == "show:42" {
				scopedID = ack.QueryID
			}
		})
	}
	// A context-free empty query is still ignored. The status fences its input.
	u.send(t, wire.UIRequest{Type: "query"})
	u.expectStatus(t)
	for _, req := range []wire.UIRequest{
		{Text: "global", Context: "private"}, {Text: "@g", Context: "private"},
		{Plugin: "catalog", Context: "bad\x00value"}, {Plugin: "catalog", Context: strings.Repeat("é", 2049)},
	} {
		req.Type = "query"
		u.send(t, req)
		var msg map[string]string
		u.read(t, &msg)
		if msg["type"] != "error" || msg["error"] == "" {
			t.Fatalf("invalid query accepted: %v", msg)
		}
	}
	if scopedID == "" {
		t.Fatal("missing scoped query")
	}
	ack := u.selectRow(t, wire.UIRequest{QueryID: scopedID, Plugin: "catalog", ResultID: "row", Action: "mark", Context: "override"})
	u.outcome(t, ack, true, "")
	refreshSnapshot(t, read, scopedID, func(q aggregate) bool { return q.Plugins["catalog"].State == "done" })
	wantCalls = append(wantCalls, "catalog|4|tarragon|select|row|mark|show:42", "catalog|3|tarragon|query|||show:42")
	for _, tc := range []struct{ context, env string }{{"direct:7", "direct:7"}, {"", "unset"}} {
		ack := u.selectRow(t, wire.UIRequest{Plugin: "catalog", ResultID: "standalone", Context: tc.context})
		u.outcome(t, ack, true, "")
		wantCalls = append(wantCalls, "catalog|3|tarragon|select|standalone||"+tc.env)
	}
	bad := u.selectRow(t, wire.UIRequest{Plugin: "catalog", ResultID: "row", Context: "bad\x00value"})
	u.outcome(t, bad, false, "not_run")
	got, err := os.ReadFile(entry + ".log")
	if err != nil || string(got) != strings.Join(wantCalls, "\n")+"\n" {
		t.Fatalf("plugin argv/env:\n%s\nwant:\n%s\nerror: %v", got, strings.Join(wantCalls, "\n"), err)
	}
}

func TestScopedContextDecoding(t *testing.T) {
	for _, tc := range []struct {
		token string
		value string
		valid bool
	}{
		{"", "", true},
		{`null`, "", true},
		{`""`, "", true},
		{`42`, "", false},
		{`{}`, "", false},
		{`[]`, "", false},
		{`false`, "", false},
		{`"�\uFFfd\\ufffd\uD83C\uDF31"`, "��\\ufffd🌱", true},
		{`"\ud803"`, "", false},
		{`"\\ufffd\udc03"`, "", false},
		{`"\u0000"`, "", false},
		{"\"\xff\"", "", false},
		{`"` + strings.Repeat("é", 2048) + `"`, strings.Repeat("é", 2048), true},
		{`"` + strings.Repeat("é", 2048) + `a"`, "", false},
		{`"` + strings.Repeat(`\u0061`, 4096) + `"`, strings.Repeat("a", 4096), true},
		{`"` + strings.Repeat(`\u0061`, 4097) + `"`, "", false},
	} {
		frame := `{"plugin":"p","text":"literal"`
		if tc.token != "" {
			frame += `,"context":` + tc.token
		}
		frame += `}`
		var req incomingUIRequest
		if err := json.Unmarshal([]byte(frame), &req); err != nil {
			t.Fatal(err)
		}
		if valid := !req.Context.Invalid; valid != tc.valid || valid && req.Context.Value != tc.value {
			t.Errorf("request context token %.30q: got %.30q (valid=%v), want %.30q (valid=%v)",
				tc.token, req.Context.Value, valid, tc.value, tc.valid)
		}
		var next wire.ActionNext
		err := json.Unmarshal([]byte(`{"type":"open","destination":`+frame+`}`), &next)
		if !tc.valid {
			if !errors.Is(err, wire.ErrInvalidContext) {
				t.Errorf("destination context token %.30q: got %v, want invalid context", tc.token, err)
			}
			continue
		}
		if want := (wire.Destination{Plugin: "p", Text: "literal", Context: tc.value}); err != nil || next.Destination == nil || *next.Destination != want {
			t.Errorf("destination context token %.30q: decoded incorrectly (%v)", tc.token, err)
		}
	}
}

func TestInvalidContextKeepsUISessionAndWatch(t *testing.T) {
	f := persistentFixture(t, true, 5*time.Second)
	p := f.connectPlugin(t, "p")
	u := f.connectUI(t, f.requests)
	u.send(t, json.RawMessage(`{"type":"query","plugin":"p","query_stream":"view","watch":true,"context":null}`))
	var watch wire.AckMessage
	u.read(t, &watch)
	if watch.Type != "ack" || watch.QueryID == "" {
		t.Fatalf("bad watch ack: %+v", watch)
	}
	watchRequest(t, p, watch.QueryID)
	watchFrame(t, p, watch.QueryID, wire.MsgResponse, `["first"]`, true, false)
	watchRows(t, u, watch.QueryID, "first")

	pending := u.selectRow(t, click("pending"))
	p.expectSelect(t, "pending", pending.ActionID)
	u.send(t, json.RawMessage(`{"type":"query","plugin":"p","query_stream":"view","watch":true,"context":42}`))
	var rejection map[string]string
	u.read(t, &rejection)
	if rejection["type"] != "error" || rejection["error"] != wire.ErrInvalidContext.Error() {
		t.Fatalf("bad query rejection: %v", rejection)
	}
	u.expectStatus(t)

	u.send(t, json.RawMessage(`{"type":"select","plugin":"p","result_id":"invalid","request_id":"bad-context","context":42}`))
	var receipt wire.SelectAck
	u.read(t, &receipt)
	if receipt.Type != "ok" || receipt.ActionID == "" || receipt.RequestID != "bad-context" {
		t.Fatalf("bad select receipt: %+v", receipt)
	}
	u.outcome(t, receipt, false, "not_run")
	u.expectStatus(t)

	p.reply(t, pending.ActionID, "still running", true)
	u.outcome(t, pending, true, "")
	watchFrame(t, p, watch.QueryID, wire.MsgLiveUpdate, `["second"]`, false, false)
	watchRows(t, u, watch.QueryID, "second")
	p.expectNoWrite(t)
}

func TestScopedPersistentNativeFrames(t *testing.T) {
	f := newAdmissionFixture(t, 2*time.Second)
	f.configure("catalog", plugins.LifecycleDaemon, true, "")
	b := testBus(t, t.Context())
	err := b.listenNative(
		func(c peerConn) { handleUIClient(b.ctx, c, f.mgr, f.requests, f.registry, f.store, f.ui, nil) },
		func(c peerConn) { handlePluginConn(b.ctx, c, f.registry, f.store, f.ui) },
	)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(nats.DefaultURL, nats.InProcessServer(b.server))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	open := func(role string) (send func(any), read func(any)) {
		t.Helper()
		m, err := nc.Request("tarragon."+role+".open", nil, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		base := string(m.Data)
		sub, err := nc.SubscribeSync(base + ".out")
		if err != nil {
			t.Fatal(err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatal(err)
		}
		return func(v any) {
				t.Helper()
				data, err := json.Marshal(v)
				if err != nil {
					t.Fatal(err)
				}
				if err := nc.Publish(base+".in", data); err != nil {
					t.Fatal(err)
				}
			}, func(v any) {
				t.Helper()
				m, err := sub.NextMsg(2 * time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(m.Data, v); err != nil {
					t.Fatal(err)
				}
			}
	}
	sendPlugin, readPlugin := open("plugin")
	sendPlugin(wire.PluginHello{Type: wire.MsgHello, Name: "catalog"})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if _, ok := f.registry.waitConn(ctx, "catalog", nil); !ok {
		t.Fatal("catalog did not register")
	}
	checkFrame := func(want map[string]string) {
		t.Helper()
		var got map[string]string
		readPlugin(&got)
		if !maps.Equal(got, want) {
			t.Fatalf("plugin frame %v, want %v", got, want)
		}
	}
	send, read := open("ui")
	send(wire.UIRequest{Type: "query", ClientID: "shared", Plugin: "catalog", Context: "show:42"})
	var ack wire.AckMessage
	read(&ack)
	if ack.Type != "ack" || ack.QueryID == "" {
		t.Fatalf("bad query ack: %+v", ack)
	}
	id := ack.QueryID
	checkFrame(map[string]string{"type": "request", "query_id": id, "text": "", "context": "show:42"})
	sendPlugin(wire.PluginResponse{Type: wire.MsgResponse, QueryID: id, Data: json.RawMessage(`["row"]`)})
	refreshSnapshot(t, read, id, func(q aggregate) bool { return q.Plugins["catalog"].State == "done" })
	otherSend, otherRead := open("ui")
	otherSend(wire.UIRequest{Type: wire.MsgSelect, ClientID: "shared", Plugin: "catalog", QueryID: id, ResultID: "row", Action: "mark", RequestID: "foreign"})
	var receipt wire.SelectAck
	otherRead(&receipt)
	if receipt.Type != "ok" || receipt.ActionID == "" || receipt.RequestID != "foreign" {
		t.Fatalf("bad receipt for foreign select: %+v", receipt)
	}
	var rejected wire.SelectResponse
	otherRead(&rejected)
	if rejected.Type != wire.MsgSelectResponse || rejected.ActionID != receipt.ActionID || rejected.RequestID != receipt.RequestID ||
		rejected.Success || rejected.Code != "not_run" {
		t.Fatalf("bad outcome for foreign select: %+v", rejected)
	}
	// Fence the foreign select before checking the next plugin frame.
	otherSend(wire.UIRequest{Type: wire.MsgStatus})
	var status wire.StatusResponse
	otherRead(&status)
	if status.Type != wire.MsgStatus {
		t.Fatalf("unexpected frame after foreign select outcome: %+v", status)
	}
	for _, tc := range []struct {
		send                        func(any)
		read                        func(any)
		queryID, supplied, expected string
	}{
		{send, read, id, "override", "show:42"},
		{otherSend, otherRead, "", "direct", "direct"},
	} {
		tc.send(wire.UIRequest{Type: wire.MsgSelect, ClientID: "shared", Plugin: "catalog", QueryID: tc.queryID, ResultID: "row", Action: "mark", Context: tc.supplied})
		var receipt wire.SelectAck
		tc.read(&receipt)
		if receipt.Type != "ok" || receipt.ActionID == "" {
			t.Fatalf("bad select receipt: %+v", receipt)
		}
		want := map[string]string{"type": "select", "query_id": tc.queryID, "result_id": "row", "action": "mark"}
		if tc.expected != "" {
			want["context"] = tc.expected
		}
		checkFrame(want)
		sendPlugin(wire.SelectResponse{Type: wire.MsgSelectResponse, Success: true})
		var final wire.SelectResponse
		tc.read(&final)
		if !final.Success || final.ActionID != receipt.ActionID {
			t.Fatalf("bad select outcome: %+v", final)
		}
	}
}
