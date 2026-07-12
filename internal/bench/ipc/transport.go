package ipc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const (
	direct  = "uds"
	tcp     = "nats-tcp"
	bridged = "nats-uds-bridge"
	adapter = "uds-adapter"
)

// Each link carries whole JSON messages. Only the framing changes.
type link interface {
	read() ([]byte, error)
	write([]byte) error
	close()
}

func send(l link, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return l.write(data)
}

func receive(l link, v any) error {
	data, err := l.read()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

type socketLink struct {
	net.Conn
	scanner *bufio.Scanner
	timeout time.Duration
}

func socket(conn net.Conn, timeout time.Duration) *socketLink {
	return &socketLink{conn, wire.NewScanner(conn), timeout}
}

func (l *socketLink) read() ([]byte, error) {
	if err := l.SetReadDeadline(time.Now().Add(l.timeout)); err != nil {
		return nil, err
	}
	if l.scanner.Scan() {
		return l.scanner.Bytes(), nil
	}
	if err := l.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (l *socketLink) write(data []byte) error {
	if err := l.SetWriteDeadline(time.Now().Add(l.timeout)); err != nil {
		return err
	}
	_, err := l.Write(append(data, '\n'))
	return err
}

func (l *socketLink) close() { _ = l.Close() }

type busLink struct {
	conn    *nats.Conn
	sub     *nats.Subscription
	target  string
	timeout time.Duration
}

func bus(url, token, inbound, outbound string, timeout time.Duration, extra ...nats.Option) (*busLink, error) {
	opts := []nats.Option{nats.Token(token), nats.NoReconnect(), nats.Timeout(timeout)}
	conn, err := nats.Connect(url, append(opts, extra...)...)
	if err != nil {
		return nil, err
	}
	sub, err := conn.SubscribeSync(inbound)
	if err == nil {
		err = conn.FlushTimeout(timeout) // subscription must exist before a peer publishes
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &busLink{conn, sub, outbound, timeout}, nil
}

func (l *busLink) read() ([]byte, error) {
	msg, err := l.sub.NextMsg(l.timeout)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func (l *busLink) write(data []byte) error { return l.conn.Publish(l.target, data) }
func (l *busLink) close()                  { l.conn.Close() }

type unixDialer struct {
	path    string
	timeout time.Duration
}

func (d unixDialer) Dial(_, _ string) (net.Conn, error) {
	return net.DialTimeout("unix", d.path, d.timeout)
}

// Pinned nats-server exposes InProcessConn but no Unix client listener.
// Forward NATS protocol bytes, without a TCP fallback or JSON translation.
func bridge(client net.Conn, broker *server.Server) (func(), error) {
	upstream, err := broker.InProcessConn()
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	closeBoth := func() {
		_ = client.Close()
		_ = upstream.Close()
	}
	copyBytes := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		closeBoth()
	}
	go copyBytes(client, upstream)
	go copyBytes(upstream, client)
	return closeBoth, nil
}

// Forward NDJSON frames to the fixture's request/response subjects, and put
// the NDJSON delimiter back on messages coming from the broker.
func adapt(legacy, native link) {
	copyMessages := func(dst, src link) {
		for {
			data, err := src.read()
			// A socket writer appends a delimiter; neither an inbound NATS
			// message nor a Scanner token owns the bytes past its end.
			if err != nil || dst.write(data[:len(data):len(data)]) != nil {
				return
			}
		}
	}
	go copyMessages(native, legacy)
	go copyMessages(legacy, native)
}

func subjects(role string) (toPeer, fromPeer string) {
	return "bench." + role + ".in", "bench." + role + ".out"
}

func openFixtureLink(cfg fixtureConfig) (link, error) {
	toPeer, fromPeer := subjects(cfg.Name)
	switch cfg.Path {
	case direct, adapter:
		conn, err := net.DialTimeout("unix", cfg.Address, cfg.Timeout)
		if err != nil {
			return nil, err
		}
		return socket(conn, cfg.Timeout), nil
	case tcp:
		return bus(cfg.Address, cfg.Token, toPeer, fromPeer, cfg.Timeout)
	case bridged:
		return bus(nats.DefaultURL, cfg.Token, toPeer, fromPeer, cfg.Timeout,
			nats.SetCustomDialer(unixDialer{cfg.Address, cfg.Timeout}))
	default:
		return nil, fmt.Errorf("unknown path %q", cfg.Path)
	}
}

func isTimeout(err error) bool {
	var timeout net.Error
	return errors.Is(err, nats.ErrTimeout) || (errors.As(err, &timeout) && timeout.Timeout())
}
