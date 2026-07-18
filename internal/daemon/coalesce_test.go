package daemon

import (
	"net"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/wire"
)

// Keep the slow writer blocked until the queue limit is reached, independent
// of wall-clock timing on a loaded test runner.
type stalledConn struct{ net.Conn }

func (stalledConn) SetWriteDeadline(time.Time) error { return nil }

func TestStalledUIIsIndependentOfOtherClients(t *testing.T) {
	r := newUIRegistry()
	blocked, unread := net.Pipe()
	defer unread.Close()
	slow := &uiClient{conn: stalledConn{blocked}, clientID: "slow"}
	r.add(slow)
	defer r.remove(slow)
	server, reader := net.Pipe()
	defer reader.Close()
	fast := &uiClient{conn: server, clientID: "fast"}
	r.add(fast)
	defer r.remove(fast)
	// No reader on the slow connection. The healthy client's frames must
	// still arrive in order, without waiting for that writer to time out.
	scanner := wire.NewScanner(reader)
	_ = reader.SetReadDeadline(time.Now().Add(time.Second))
	for i := range cap(slow.out) + 3 {
		r.publish(i)
		var got int
		if err := wire.ReadMsg(scanner, &got); err != nil || got != i {
			t.Fatalf("read frame %d: got %d, error %v", i, got, err)
		}
	}
	select {
	case <-slow.done:
	default:
		t.Fatal("stalled UI remained connected after exceeding its queue limit")
	}
}
