package daemon

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/nats-io/nats.go"
)

// Each invocation has its own responder so cancellation follows the query
// and the executable comes from the resolved manifest, never a bus payload.
func (b *daemonBus) callPlugin(ctx context.Context, plugin *plugins.Plugin, req wire.UIRequest) (json.RawMessage, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	subject := "tarragon.exec." + nats.NewInbox()
	sub, err := b.conn.SubscribeSync(subject)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(b.ctx, cancel)
	defer stop()
	go func() {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			return
		}
		var request wire.UIRequest
		var result json.RawMessage
		err = json.Unmarshal(msg.Data, &request)
		if err == nil {
			switch request.Type {
			case "query":
				result, err = invokeOnCallQuery(ctx, plugin, request.Text)
			case wire.MsgSelect:
				var response wire.SelectResponse
				response, err = invokeOnCallSelect(ctx, plugin, request.ResultID, request.Action)
				if err == nil {
					result, err = json.Marshal(response)
				}
			default:
				err = errors.New("unknown on-call request")
			}
		}
		response := nats.NewMsg(msg.Reply)
		response.Data = result
		if err != nil {
			response.Header.Set("Tarragon-Error", "true")
			response.Data = []byte(err.Error())
		}
		_ = b.conn.PublishMsg(response)
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
