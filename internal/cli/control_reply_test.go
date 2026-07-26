package cli

import (
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/wire"
)

func TestControlCommandsIgnoreUnrelatedUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "u")
	t.Setenv("TARRAGON_UI_SOCKET", path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	replies := map[string]string{
		"restart": `{"type":"restart_response","success":true,"message":"restarted"}`,
		"reload":  `{"type":"reload_response","success":true,"message":"reloaded"}`,
		"status":  `{"type":"status","plugins":[{"id":"example"}]}`,
	}
	done := make(chan error, 1)
	go func() {
		for range replies {
			conn, err := ln.Accept()
			if err != nil {
				done <- err
				return
			}
			var req wire.UIRequest
			err = wire.ReadMsg(wire.NewScanner(conn), &req)
			if err == nil {
				// One write also checks that buffered frames survive the read loop.
				_, err = io.WriteString(conn, `{"type":"update","query_id":"other","payload":"e30="}`+"\n"+replies[req.Type]+"\n")
			}
			conn.Close()
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	resp, err := sendRestart("example")
	if err != nil || !resp.Success || resp.Message != "restarted" {
		t.Fatalf("restart: %+v, %v", resp, err)
	}
	success, msg, err := sendReload()
	if err != nil || !success || msg != "reloaded" {
		t.Fatalf("reload: %t %q, %v", success, msg, err)
	}
	loaded, ok := loadedPluginsFromDaemon(time.Second)
	if !ok || loaded["example"].ID != "example" {
		t.Fatalf("status: %+v, available=%t", loaded, ok)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestControlReplyStopsOnErrorOrEOF(t *testing.T) {
	const update = `{"type":"update"}` + "\n"
	var reply wire.StatusResponse
	err := readControlReply(strings.NewReader(update+`{"type":"error","error":"refused"}`), wire.MsgStatus, &reply)
	if err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("expected daemon error, got %v", err)
	}
	if err := readControlReply(strings.NewReader(update), wire.MsgStatus, &reply); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}
