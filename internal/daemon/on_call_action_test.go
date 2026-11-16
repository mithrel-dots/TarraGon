package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

// Keep the UI on a separate socket so bus.sessions.Wait is a barrier for the
// execution responder alone, including its cmd.Wait and broker reply.
func openOnCallTestUI(t *testing.T, b *daemonBus, dir string, replyTimeout time.Duration) admissionPeer {
	t.Helper()
	f := newAdmissionFixture(t, replyTimeout)
	f.configure("plain", plugins.LifecycleOnCall, true, filepath.Join(dir, "plain.sh"))
	f.registry.bus = b
	return f.connectUI(t, f.requests)
}

func onCallChildPID(t *testing.T, dir string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(filepath.Join(dir, "pid"))
		if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid > 0 {
			return pid
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("on-call executable did not signal that it started")
	return 0
}

func waitForOnCallResponder(t *testing.T, b *daemonBus) {
	t.Helper()
	done := make(chan struct{})
	go func() { b.sessions.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("on-call responder did not finish")
	}
}

func openOnCallTestGate(t *testing.T, dir string) *os.File {
	t.Helper()
	gatePath := filepath.Join(dir, "gate")
	if err := syscall.Mkfifo(gatePath, 0600); err != nil {
		t.Fatal(err)
	}
	gate, err := os.OpenFile(gatePath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Unblock any reader still alive after an assertion failed.
		_, _ = gate.WriteString("release\n")
		_ = gate.Close()
	})
	return gate
}

func TestOnCallActionOutlivesReplyDeadline(t *testing.T) {
	dir := t.TempDir()
	gate := openOnCallTestGate(t, dir)
	writeScript(t, dir, "plain.sh", `#!/bin/sh
cd "${0%/*}" || exit 1
printf '%s:%s\n' "$2" "$3" >> calls
case "$2" in
  query) printf '["one"]' ;;
  select)
    echo $$ > pid
    read -r value < gate
    echo "$value" > state
    printf '{"message":"late","next":{"type":"refresh"}}' ;;
esac
`)
	b := testBus(t, t.Context())
	b.actionSlots = make(chan struct{}, 1)
	ui := openOnCallTestUI(t, b, dir, 500*time.Millisecond)
	ui.send(t, wire.UIRequest{Type: "query", Text: "two words"})
	var query wire.AckMessage
	ui.read(t, &query)
	if query.Type != "ack" || query.QueryID == "" {
		t.Fatalf("unexpected query ack: %+v", query)
	}
	snap := refreshSnapshot(t, func(v any) { ui.read(t, v) }, query.QueryID, func(snap aggregate) bool {
		return snap.Plugins["plain"].State != "pending"
	})
	if len(snap.List) != 1 || snap.List[0].ID != "one" {
		t.Fatalf("expected row one from plain: %+v", snap)
	}

	ack := ui.selectRow(t, wire.UIRequest{Plugin: "plain", QueryID: query.QueryID, ResultID: "one", RequestID: "mark"})
	pid := onCallChildPID(t, dir)
	ui.expectStatus(t) // before timeout and while the child is blocked
	ui.outcome(t, ack, false, "completion_unknown")
	// The timed-out child still occupies the only execution slot.
	ack = ui.selectRow(t, wire.UIRequest{Plugin: "plain", ResultID: "two"})
	ui.outcome(t, ack, false, "not_run")
	if _, err := gate.WriteString("applied\n"); err != nil {
		t.Fatal(err)
	}
	waitForOnCallResponder(t, b)
	if data, err := os.ReadFile(filepath.Join(dir, "state")); err != nil || string(data) != "applied\n" {
		t.Fatalf("late effect: got %q, err=%v", data, err)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("completed child %d not reaped: %v", pid, err)
	}
	ui.expectStatus(t) // a duplicate terminal reply or refresh update would precede this
	if data, err := os.ReadFile(filepath.Join(dir, "calls")); err != nil || string(data) != "query:two words\nselect:one\n" {
		t.Fatalf("unexpected dispatches (late refresh or duplicate select): %q, err=%v", data, err)
	}
}

func TestOnCallActionReapedAfterTimeoutOrShutdown(t *testing.T) {
	for _, reason := range []string{"safety limit", "shutdown"} {
		t.Run(reason, func(t *testing.T) {
			dir := t.TempDir()
			writeScript(t, dir, "plain.sh", "#!/bin/sh\necho $$ > \"${0%/*}/pid\"\nexec sleep 30\n")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			b := testBus(t, ctx)
			if reason == "safety limit" {
				b.actionSafetyLimit = 800 * time.Millisecond
			}
			ui := openOnCallTestUI(t, b, dir, 300*time.Millisecond)
			ack := ui.selectRow(t, wire.UIRequest{Plugin: "plain", ResultID: "one"})
			pid := onCallChildPID(t, dir)
			ui.outcome(t, ack, false, "completion_unknown")
			if err := syscall.Kill(pid, 0); err != nil {
				t.Fatalf("child already gone at UI timeout: %v", err)
			}
			if reason == "shutdown" {
				cancel()
			}
			waitForOnCallResponder(t, b)
			if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
				t.Fatalf("child %d not reaped after %s: %v", pid, reason, err)
			}
		})
	}
}

func TestOnCallActionStartAndExitFailures(t *testing.T) {
	for _, tc := range []struct {
		name, script, code, message string
	}{
		{"unstartable", "#!/nonexistent/tarragon-test-interpreter\n", "not_run", "plain.sh"},
		{"reported failure", "#!/bin/sh\necho '{\"success\":false,\"message\":\"stale\"}'\n", "", "stale"},
		{"failed exit", "#!/bin/sh\necho 'failed to apply' >&2\nexit 2\n", "", "failed to apply"},
		{"oversized failed exit", "#!/bin/sh\nhead -c 1500000 /dev/zero | tr '\\000' '<' >&2\nexit 2\n", "", "[truncated]"},
		{"oversized reply", "#!/bin/sh\nhead -c 8388610 /dev/zero\n", "completion_unknown", wire.ErrMessageTooLarge.Error()},
		{"invalid reply", "#!/bin/sh\necho bad-json\n", "completion_unknown", "invalid select response JSON"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeScript(t, dir, "plain.sh", tc.script)
			ui := openOnCallTestUI(t, testBus(t, t.Context()), dir, 2*time.Second)
			ack := ui.selectRow(t, wire.UIRequest{Plugin: "plain", ResultID: "one"})
			resp := ui.outcome(t, ack, false, tc.code)
			if !strings.Contains(resp.Message, tc.message) {
				t.Fatalf("expected %q in %+v", tc.message, resp)
			}
			ui.expectStatus(t)
		})
	}
}

func TestOnCallActionExpiredBeforeStart(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "plain.sh", "#!/bin/sh\ntouch \"${0%/*}/ran\"\n")
	plugin := &plugins.Plugin{Dir: dir, Config: plugins.PluginConfig{ID: "plain", Entrypoint: "plain.sh"}}
	b := testBus(t, t.Context())
	store := NewTransientQueryStore()
	ui := newUIRegistry(store)
	owner := &uiClient{out: make(chan any, 2), done: make(chan struct{})}
	a := ui.acceptSelect(b.ctx, owner, wire.UIRequest{Type: wire.MsgSelect, Plugin: "plain", ResultID: "one"}, nil)
	<-owner.out // receipt ack

	// Simulate a late scheduler: the deadline has passed, but the UI timer's
	// callback has not run. The responder must recheck at cmd.Start itself.
	a.mu.Lock()
	a.deadline = time.Now().Add(-time.Millisecond)
	a.mu.Unlock()
	b.callPluginAction(b.ctx, plugin, a)
	resp := (<-owner.out).(*wire.SelectResponse)
	if resp.Code != "not_run" || resp.Success {
		t.Fatalf("expired call completed with %+v", resp)
	}
	if _, err := os.Stat(filepath.Join(dir, "ran")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired call was executed: stat error %v", err)
	}
}
