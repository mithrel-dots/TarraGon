package daemon

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/nats-io/nats.go"
)

const execPrefix = "tarragon.exec."

// The daemon supplies the manifest and query lifetime. Only the operation and
// its arguments cross the broker; a subject can be claimed just once.
type pluginCall struct {
	ctx    context.Context
	plugin *plugins.Plugin
}

func (b *daemonBus) callPlugin(ctx context.Context, plugin *plugins.Plugin, req wire.UIRequest) (json.RawMessage, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(b.ctx, cancel)
	defer stop()
	subject := execPrefix + rand.Text()
	b.mu.Lock()
	b.calls[subject] = pluginCall{ctx, plugin}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.calls, subject)
		b.mu.Unlock()
	}()

	response, err := b.conn.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, err
	}
	if response.Header.Get("Tarragon-Error") != "" {
		return nil, errors.New(string(response.Data))
	}
	return response.Data, nil
}

func (b *daemonBus) handlePluginCall(msg *nats.Msg) {
	b.mu.Lock()
	call, ok := b.calls[msg.Subject]
	if !ok || msg.Reply == "" || b.ctx.Err() != nil {
		b.mu.Unlock()
		return
	}
	delete(b.calls, msg.Subject)
	b.sessions.Add(1)
	b.mu.Unlock()

	go func() {
		defer b.sessions.Done()
		result, err := call.run(msg.Data)
		response := nats.NewMsg(msg.Reply)
		response.Data = result
		if err != nil {
			response.Header.Set("Tarragon-Error", "true")
			response.Data = []byte(err.Error())
		}
		err = b.conn.PublishMsg(response)
		if errors.Is(err, nats.ErrMaxPayload) {
			response.Header.Set("Tarragon-Error", "true")
			response.Data = []byte("on-call reply: " + wire.ErrMessageTooLarge.Error())
			err = b.conn.PublishMsg(response)
		}
		if err != nil {
			log.Printf("publish on-call reply: %v", err)
		}
	}()
}

func (c pluginCall) run(data []byte) (json.RawMessage, error) {
	var req wire.UIRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	switch req.Type {
	case "query":
		return invokeOnCallQuery(c.ctx, c.plugin, req.Text)
	case wire.MsgSelect:
		response, err := invokeOnCallSelect(c.ctx, c.plugin, req.ResultID, req.Action)
		if err != nil {
			return nil, err
		}
		return json.Marshal(response)
	default:
		return nil, errors.New("unknown on-call request")
	}
}
