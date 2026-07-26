package daemon

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Both core handlers read one JSON line at a time and write complete frames.
type peerConn interface {
	io.ReadWriteCloser
	SetWriteDeadline(time.Time) error
}

type daemonBus struct {
	server   *server.Server
	conn     *nats.Conn
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	sessions sync.WaitGroup
}

// Port zero disables the TCP listener; Unix adapters use in-process NATS.
func startNATS(parent context.Context, port int, token string) (*daemonBus, error) {
	if port != 0 && token == "" {
		return nil, errors.New("set TARRAGON_NATS_TOKEN before enabling nats_tcp")
	}
	broker, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: port, DontListen: port == 0, Authorization: token,
		NoSigs: true, NoLog: true, MaxPayload: 8 << 20,
	})
	if err != nil {
		return nil, err
	}
	broker.Start()
	if !broker.ReadyForConnections(3 * time.Second) {
		broker.Shutdown()
		broker.WaitForShutdown()
		return nil, fmt.Errorf("embedded NATS failed to start (port %d)", port)
	}
	conn, err := nats.Connect(nats.DefaultURL, nats.InProcessServer(broker), nats.Token(token), nats.NoReconnect(), nats.SyncQueueLen(512))
	if err != nil {
		broker.Shutdown()
		broker.WaitForShutdown()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	return &daemonBus{server: broker, conn: conn, ctx: ctx, cancel: cancel}, nil
}

func (b *daemonBus) listenNative(ui, plugin func(peerConn)) error {
	for role, handler := range map[string]func(peerConn){"ui": ui, "plugin": plugin} {
		_, err := b.conn.Subscribe("tarragon."+role+".open", func(m *nats.Msg) {
			if m.Reply == "" {
				return
			}
			base := newSessionID()
			if err := b.startSession(base, true, handler); err != nil {
				_ = m.Respond([]byte("error: cannot open session"))
				return
			}
			_ = m.Respond([]byte(base))
		})
		if err != nil {
			return err
		}
	}
	return b.conn.FlushTimeout(3 * time.Second)
}

func (b *daemonBus) close() {
	b.mu.Lock()
	b.cancel()
	b.mu.Unlock()
	b.sessions.Wait()
	b.conn.Close()
	b.server.Shutdown()
	b.server.WaitForShutdown()
}

func newSessionID() string { return "tarragon.session." + rand.Text() }

func (b *daemonBus) startSession(base string, native bool, handle func(peerConn)) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ctx.Err(); err != nil {
		return err
	}
	sub, err := b.conn.SubscribeSync(base + ".in")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(b.ctx)
	p := &subjectPeer{bus: b.conn, inbox: sub, out: base + ".out", ctx: ctx, cancel: cancel, native: native}
	b.sessions.Add(1)
	go func() {
		defer b.sessions.Done()
		defer p.Close()
		handle(p)
	}()
	return nil
}

// The subscription supplies the reader for the existing NDJSON decoder. An
// outbound write is exactly one wire.WriteMsg frame, hence one NATS message.
type subjectPeer struct {
	bus      *nats.Conn
	inbox    *nats.Subscription
	out      string
	ctx      context.Context
	cancel   context.CancelFunc
	native   bool
	line     bytes.Buffer
	writeMu  sync.Mutex
	deadline time.Time
}

func (p *subjectPeer) Read(dst []byte) (int, error) {
	for p.line.Len() == 0 {
		ctx := p.ctx
		var cancel context.CancelFunc
		if p.native {
			ctx, cancel = context.WithTimeout(ctx, 90*time.Second)
		}
		msg, err := p.inbox.NextMsgWithContext(ctx)
		if cancel != nil {
			cancel()
		}
		if err != nil {
			return 0, err
		}
		if len(msg.Data) == 0 { // native keepalive
			continue
		}
		if bytes.Equal(msg.Data, []byte("close")) {
			return 0, io.EOF
		}
		if err := json.Compact(&p.line, msg.Data); err != nil {
			return 0, err
		}
		p.line.WriteByte('\n')
	}
	return p.line.Read(dst)
}

func (p *subjectPeer) SetWriteDeadline(d time.Time) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.deadline = d
	return nil
}

func (p *subjectPeer) Write(frame []byte) (int, error) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if p.ctx.Err() != nil {
		return 0, net.ErrClosed
	}
	if !p.deadline.IsZero() && time.Now().After(p.deadline) {
		return 0, context.DeadlineExceeded
	}
	if err := p.bus.Publish(p.out, bytes.TrimSuffix(frame, []byte{'\n'})); err != nil {
		return 0, err
	}
	return len(frame), nil
}

func (p *subjectPeer) Close() error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if p.inbox.IsValid() {
		p.cancel()
		_ = p.inbox.Unsubscribe()
		return p.bus.Publish(p.out, []byte("close"))
	}
	return nil
}

// Translate both directions, including a Unix half-close: finish pending
// replies before closing the socket, as required by the CLI control commands.
func (b *daemonBus) adaptUnix(conn net.Conn, handler func(peerConn)) {
	defer conn.Close()
	base := newSessionID()
	sub, err := b.conn.SubscribeSync(base + ".out")
	if err != nil {
		return
	}
	defer sub.Unsubscribe()
	if err := b.startSession(base, false, handler); err != nil {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer conn.Close()
		for {
			msg, err := sub.NextMsgWithContext(b.ctx)
			if err != nil || bytes.Equal(msg.Data, []byte("close")) {
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
			if _, err := conn.Write(append(msg.Data[:len(msg.Data):len(msg.Data)], '\n')); err != nil {
				return
			}
		}
	}()
	scanner := wire.NewScanner(conn)
	for scanner.Scan() {
		// Raw controls belong to the subject protocol, never to a Unix peer.
		if !json.Valid(scanner.Bytes()) {
			break
		}
		if err := b.conn.Publish(base+".in", scanner.Bytes()); err != nil {
			break
		}
	}
	_ = b.conn.Publish(base+".in", []byte("close"))
	<-done
}

func (b *daemonBus) listenUnix(path string, handle func(peerConn)) (net.Listener, error) {
	ln, err := wire.ListenUnix(path)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(b.ctx, func() { _ = ln.Close() })
	go func() {
		defer stop()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go b.adaptUnix(conn, handle)
		}
	}()
	return ln, nil
}
