package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUIAckRoundTrip(t *testing.T) {
	ack := AckMessage{Type: "ack", QueryID: "q-1"}
	b, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got AckMessage
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Type != "ack" || got.QueryID != "q-1" {
		t.Fatalf("unexpected ack: %+v", got)
	}
}

func TestPluginReqRespRoundTrip(t *testing.T) {
	req := PluginRequest{Type: MsgRequest, QueryID: "q-2", Text: "hello"}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal req: %v", err)
	}
	var req2 PluginRequest
	if err := json.Unmarshal(b, &req2); err != nil {
		t.Fatalf("unmarshal req: %v", err)
	}
	if req2.Type != MsgRequest || req2.QueryID != "q-2" || req2.Text != "hello" {
		t.Fatalf("unexpected req2: %+v", req2)
	}

	resp := PluginResponse{Type: MsgResponse, QueryID: "q-2", Data: json.RawMessage(`{"ok":true}`)}
	b2, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal resp: %v", err)
	}
	var resp2 PluginResponse
	if err := json.Unmarshal(b2, &resp2); err != nil {
		t.Fatalf("unmarshal resp: %v", err)
	}
	if resp2.Type != MsgResponse || resp2.QueryID != "q-2" || string(resp2.Data) != `{"ok":true}` {
		t.Fatalf("unexpected resp2: %+v", resp2)
	}
}

func TestWriteReadMsgRoundTrip(t *testing.T) {
	buf := &bytes.Buffer{}
	in := &UIRequest{Type: "query", ClientID: "c1", Text: "hello"}
	if err := WriteMsg(buf, in); err != nil {
		t.Fatalf("write: %v", err)
	}

	s := NewScanner(buf)
	var out UIRequest
	if err := ReadMsg(s, &out); err != nil {
		t.Fatalf("read: %v", err)
	}
	if out.Type != in.Type || out.ClientID != in.ClientID || out.Text != in.Text {
		t.Fatalf("unexpected roundtrip: %+v", out)
	}

	if err := ReadMsg(s, &out); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestMessageSizeBoundary(t *testing.T) {
	value := strings.Repeat("x", MaxMessageSize-2) // two JSON quotes
	var buf bytes.Buffer
	if err := WriteMsg(&buf, value); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := ReadMsg(NewScanner(&buf), &got); err != nil || got != value {
		t.Fatalf("maximum-size message did not round trip: %v", err)
	}
	if err := WriteMsg(&buf, value+"x"); err != ErrMessageTooLarge || buf.Len() != 0 {
		t.Fatalf("oversized write: %d bytes, %v", buf.Len(), err)
	}
}

func TestQueryReplacementActionRoundTrip(t *testing.T) {
	in := &Action{Name: "Episodes", Type: ActionTypeQueryReplace, Query: "@anime episodes 154587"}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal action: %v", err)
	}

	var out Action
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal action: %v", err)
	}
	if out != *in {
		t.Fatalf("action changed across wire round trip: got %+v, want %+v", out, *in)
	}
}

func TestKeepOpenActionRoundTrip(t *testing.T) {
	in := &Action{Name: "Copy", Type: ActionTypeKeepOpen, Default: true}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal action: %v", err)
	}

	var out Action
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal action: %v", err)
	}
	if out != *in {
		t.Fatalf("action changed across wire round trip: got %+v, want %+v", out, *in)
	}
}

func TestListenUnixKeepsLiveListener(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "x.sock")
	ln, err := ListenUnix(path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	if other, err := ListenUnix(path); err == nil {
		other.Close()
		t.Fatal("replaced live listener")
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("original listener no longer reachable: %v", err)
	}
	conn.Close()
}

func TestListenUnixLeavesSharedDirectoryAlone(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if ln, err := ListenUnix(filepath.Join(dir, "l.sock")); err == nil {
		ln.Close()
		t.Fatal("unexpected listener in non-private directory")
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o755 {
		t.Fatalf("parent directory changed: %v, %v", fi, err)
	}
}

func TestListenUnixCreatesDirWithSecureMode(t *testing.T) {
	dir := t.TempDir()
	sockDir := filepath.Join(dir, "tarragon")
	path := filepath.Join(sockDir, "ui.sock")

	ln, err := ListenUnix(path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	fi, err := os.Stat(sockDir)
	if err != nil {
		t.Fatalf("stat socket dir: %v", err)
	}
	if !fi.IsDir() {
		t.Fatalf("expected %s to be a directory", sockDir)
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Fatalf("expected socket dir mode 0700, got %o", perm)
	}
}

func TestListenUnixRemovesStaleSocket(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "stale.sock")

	// Simulate a stale socket file left behind by a crashed daemon: a real
	// unix listener whose socket file is not unlinked on close.
	addr, err := net.ResolveUnixAddr("unix", path)
	if err != nil {
		t.Fatalf("resolve addr: %v", err)
	}
	stale, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatalf("create stale listener: %v", err)
	}
	stale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatalf("close stale listener: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected stale socket file to remain, stat err=%v", err)
	}

	type attempt struct {
		ln  net.Listener
		err error
	}
	results := make(chan attempt, 2)
	for range 2 {
		go func() {
			ln, err := ListenUnix(path)
			results <- attempt{ln, err}
		}()
	}
	var winners int
	for range 2 {
		if result := <-results; result.err == nil {
			defer result.ln.Close()
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("expected one listener to claim the stale socket, got %d", winners)
	}
}

func TestResolveUISocketPath(t *testing.T) {
	t.Setenv(envUISocket, "")
	t.Setenv(envXDGRuntimeDir, "")

	t.Run("env override wins", func(t *testing.T) {
		t.Setenv(envUISocket, "/custom/ui.sock")
		if got := ResolveUISocketPath(); got != "/custom/ui.sock" {
			t.Fatalf("expected override path, got %q", got)
		}
	})

	t.Run("xdg runtime dir when no override", func(t *testing.T) {
		t.Setenv(envUISocket, "")
		t.Setenv(envXDGRuntimeDir, "/run/user/1000")
		want := "/run/user/1000/tarragon/ui.sock"
		if got := ResolveUISocketPath(); got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})

	t.Run("fallback to euid tmp dir", func(t *testing.T) {
		t.Setenv(envUISocket, "")
		t.Setenv(envXDGRuntimeDir, "")
		want := fmt.Sprintf("/tmp/tarragon-%d/ui.sock", os.Geteuid())
		if got := ResolveUISocketPath(); got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})
}

func TestResolvePluginsSocketPath(t *testing.T) {
	t.Setenv(envPluginsSocket, "")
	t.Setenv(envXDGRuntimeDir, "")

	t.Run("env override wins", func(t *testing.T) {
		t.Setenv(envPluginsSocket, "/custom/plugins.sock")
		if got := ResolvePluginsSocketPath(); got != "/custom/plugins.sock" {
			t.Fatalf("expected override path, got %q", got)
		}
	})

	t.Run("xdg runtime dir when no override", func(t *testing.T) {
		t.Setenv(envPluginsSocket, "")
		t.Setenv(envXDGRuntimeDir, "/run/user/1000")
		want := "/run/user/1000/tarragon/plugins.sock"
		if got := ResolvePluginsSocketPath(); got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})

	t.Run("fallback to euid tmp dir", func(t *testing.T) {
		t.Setenv(envPluginsSocket, "")
		t.Setenv(envXDGRuntimeDir, "")
		want := fmt.Sprintf("/tmp/tarragon-%d/plugins.sock", os.Geteuid())
		if got := ResolvePluginsSocketPath(); got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})
}
