package daemon

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mithrel-dots/tarragon/internal/wire"
)

// A frame that fit on the plugin link can grow when the daemon adds IDs.
// An oversized explanation must not turn a definitive outcome into "error".
func TestSelectFramePreservesOutcomeAtWireLimit(t *testing.T) {
	result := wire.SelectResponse{Type: wire.MsgSelectResponse, Success: true, Message: ""}
	empty, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	result.Message = strings.Repeat("x", wire.MaxMessageSize-len(empty)-len(`,"message":""`))
	if err := wire.WriteMsg(&bytes.Buffer{}, &result); err != nil {
		t.Fatalf("plugin reply must fit: %v", err)
	}
	result.ActionID, result.RequestID = "action", "request"
	var frame bytes.Buffer
	if err := writeUIFrame(&frame, &result); err != nil {
		t.Fatal(err)
	}
	var got wire.SelectResponse
	if err := wire.ReadMsg(wire.NewScanner(&frame), &got); err != nil {
		t.Fatal(err)
	}
	if got.Type != result.Type || !got.Success || got.ActionID != result.ActionID || got.RequestID != result.RequestID ||
		!strings.Contains(got.Message, "payload limit") {
		t.Fatalf("lost terminal outcome: %+v", got)
	}
}
