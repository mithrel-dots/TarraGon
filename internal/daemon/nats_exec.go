package daemon

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log"
	"os/exec"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/nats-io/nats.go"
)

const execPrefix = "tarragon.exec."

// The daemon supplies the manifest and lifetime. Only the operation and
// its arguments cross the broker; a subject can be claimed just once.
type pluginCall struct {
	ctx    context.Context
	plugin *plugins.Plugin
	action *selectAction
	reaped chan struct{} // synchronous calls wait for a claimed invocation to finish
}

func (b *daemonBus) callPlugin(ctx context.Context, plugin *plugins.Plugin, req wire.UIRequest) (json.RawMessage, error) {
	return b.requestPlugin(ctx, pluginCall{plugin: plugin, reaped: make(chan struct{})}, req)
}

// callPluginAction waits in the UI's dispatch goroutine. Completing the action
// releases its reply inbox without interrupting a process the responder started.
func (b *daemonBus) callPluginAction(ctx context.Context, plugin *plugins.Plugin, a *selectAction) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if !a.onTerminal(cancel) {
		return
	}

	data, err := b.requestPlugin(ctx, pluginCall{plugin: plugin, action: a}, wire.UIRequest{
		Type: wire.MsgSelect, ResultID: a.resultID, Action: a.action,
	})
	var response wire.SelectResponse
	if err == nil {
		err = json.Unmarshal(data, &response)
	}
	if err != nil {
		a.fail(err.Error())
		return
	}
	a.finish(response.Success, response.Message, response.Next)
}

func (b *daemonBus) requestPlugin(ctx context.Context, call pluginCall, req wire.UIRequest) (json.RawMessage, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(b.ctx, cancel)
	subject := execPrefix + rand.Text()
	b.mu.Lock()
	call.ctx = ctx
	b.calls[subject] = call
	b.mu.Unlock()
	defer func() {
		stop()
		cancel()
		b.mu.Lock()
		_, unclaimed := b.calls[subject]
		delete(b.calls, subject)
		b.mu.Unlock()
		if !unclaimed && call.reaped != nil {
			// A refresh can dispatch as soon as the synchronous query returns.
			// A claimed command must finish cmd.Wait before releasing that slot.
			<-call.reaped
		}
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
		result, err := b.runPluginCall(call, msg.Data)
		if call.reaped != nil {
			close(call.reaped)
		}
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

func (b *daemonBus) runPluginCall(c pluginCall, data []byte) (json.RawMessage, error) {
	var req wire.UIRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	switch req.Type {
	case "query":
		return invokeOnCallQuery(c.ctx, c.plugin, req.Text)
	case wire.MsgSelect:
		ctx := c.ctx
		if c.action != nil {
			select {
			case b.actionSlots <- struct{}{}:
				defer func() { <-b.actionSlots }()
			default:
				return nil, errors.New("too many on-call actions in progress")
			}
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(b.ctx, b.actionSafetyLimit)
			defer cancel()
		}
		response, err := runOnCallSelect(ctx, c.plugin, req.ResultID, req.Action, c.action)
		if err != nil {
			// A normal nonzero exit is a definitive plugin-reported failure.
			// A killed process or unreadable reply has no definitive outcome.
			var exit *exec.ExitError
			if c.action == nil || !errors.As(err, &exit) || exit.ExitCode() < 0 {
				return nil, err
			}
			response = wire.SelectResponse{Type: wire.MsgSelectResponse, Message: err.Error()}
		}
		data, err := json.Marshal(response)
		if len(data) > wire.MaxMessageSize {
			response.Message = oversizedActionMessage
			if response.Next != nil && response.Next.Type != "refresh" {
				response.Next = nil
			}
			return json.Marshal(response)
		}
		return data, err
	default:
		return nil, errors.New("unknown on-call request")
	}
}
