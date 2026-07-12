package ipc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type peer struct {
	link
	cmd            *exec.Cmd
	cancel         context.CancelFunc
	stdout, stderr bytes.Buffer
	cleanups       []func()
}

func (p *peer) stop() {
	p.cancel()
	for i := len(p.cleanups) - 1; i >= 0; i-- {
		p.cleanups[i]()
	}
	if p.cmd != nil && p.cmd.Process != nil && p.cmd.ProcessState == nil {
		_ = p.cmd.Wait()
	}
}

func (p *peer) trace() (fixtureTrace, error) {
	if err := p.cmd.Wait(); err != nil {
		return fixtureTrace{}, fmt.Errorf("fixture exited: %w: %s", err, p.stderr.String())
	}
	var marks fixtureTrace
	err := json.Unmarshal(p.stdout.Bytes(), &marks)
	return marks, err
}

// The parent is the TarraGon core. Each peer is a separate invocation of
// tarragon so that both measured legs cross real process boundaries.
func startPeer(ctx context.Context, dir string, broker *server.Server, cfg fixtureConfig) (_ *peer, err error) {
	ctx, cancel := context.WithCancel(ctx)
	p := &peer{cancel: cancel}
	defer func() {
		if err != nil {
			p.stop()
		}
	}()
	toPeer, fromPeer := subjects(cfg.Name)
	if cfg.Path != direct {
		p.link, err = bus(nats.DefaultURL, cfg.Token, fromPeer, toPeer, cfg.Timeout, nats.InProcessServer(broker))
		if err != nil {
			return nil, err
		}
		p.cleanups = append(p.cleanups, p.link.close)
	}
	var listener net.Listener
	if cfg.Path == tcp {
		cfg.Address = broker.ClientURL()
	} else {
		cfg.Address = filepath.Join(dir, cfg.Name+".sock")
		listener, err = wire.ListenUnix(cfg.Address)
		if err != nil {
			return nil, err
		}
		defer listener.Close()
	}

	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	config, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	p.cmd = exec.CommandContext(ctx, exe, "bench", "ipc", "fixture")
	p.cmd.Stdin = bytes.NewReader(config)
	p.cmd.Stdout, p.cmd.Stderr = &p.stdout, &p.stderr
	if err := p.cmd.Start(); err != nil {
		return nil, err
	}
	if listener == nil {
		return p, nil
	}
	if err := listener.(*net.UnixListener).SetDeadline(time.Now().Add(cfg.Timeout)); err != nil {
		return nil, err
	}
	conn, err := listener.Accept()
	if err != nil {
		return nil, err
	}
	local := socket(conn, cfg.Timeout)
	p.cleanups = append(p.cleanups, local.close)
	switch cfg.Path {
	case direct:
		p.link = local
	case bridged:
		stop, err := bridge(conn, broker)
		if err != nil {
			return nil, err
		}
		p.cleanups = append(p.cleanups, stop)
	}
	return p, nil
}
