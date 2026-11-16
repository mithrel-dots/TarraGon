package daemon

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/wire"
)

func TestOrderedResultsApplyOnlyToTargetedReplacement(t *testing.T) {
	const rows = `[{"id":"ep2","score":0.1},{"id":"ep10","score":0.9}]`
	for _, mode := range []string{"global", "grouped"} {
		t.Run(mode, func(t *testing.T) {
			f := persistentFixture(t, false, time.Second)
			f.store.orderingMode, f.store.frecencyW = mode, 0.5
			f.store.frecencyCache["p:ep10"] = 3
			f.mgr.Plugins["p"].Config.ResolvedPrefix = "@episodes"
			plugin := persistentPlugin(t, f, "p")
			ui := f.connectUI(t, f.requests)
			for _, request := range []wire.UIRequest{
				{Type: "query", Plugin: "p"},
				{Type: "query", Text: "@episodes"},
				{Type: "query", Text: "general"},
			} {
				ui.send(t, request)
				var ack wire.AckMessage
				ui.read(t, &ack)
				var req wire.PluginRequest
				plugin.read(t, &req)
				if req.Type != wire.MsgRequest || ack.QueryID != req.QueryID {
					t.Fatalf("query %+v: got %+v, %+v", request, ack, req)
				}
				for _, data := range []string{`{"ordered":true,"results":` + rows + `}`, rows} {
					plugin.send(t, wire.PluginResponse{Type: wire.MsgResponse, QueryID: req.QueryID, Data: json.RawMessage(data)})
					got := refreshSnapshot(t, func(v any) { ui.read(t, v) }, req.QueryID, func(s aggregate) bool {
						return s.Plugins["p"].State == "done"
					})
					if string(got.Results["p"].Data) != data || len(got.List) != 2 {
						t.Fatalf("lost raw data or rows: %+v", got)
					}
					first, frecency := "ep2", 3.0
					if request.Text != "general" && data != rows {
						frecency = 0 // producer order also bypasses blending
					} else if mode == "global" {
						first = "ep10"
					}
					for _, row := range got.List {
						if row.ID == "ep10" && row.FrecencyScore != frecency {
							t.Fatalf("query %+v, data %s: wrong frecency %+v", request, data, row)
						}
					}
					if got.List[0].ID != first {
						t.Fatalf("query %+v, data %s: want %s first, got %+v", request, data, first, got.List)
					}
				}
			}
		})
	}
}
