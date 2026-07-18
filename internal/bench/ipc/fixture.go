package ipc

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/mithrel-dots/tarragon/internal/wire"
	"golang.org/x/sys/unix"
)

const pluginName = "bench-plugin"

type fixtureConfig struct {
	Role, Name, Path, Address, Token string
	Count, Warmup, Plugins, Burst    int
	Timeout                          time.Duration
}

type fixtureTrace struct {
	Marks                   []stamp
	QueryStarts             []int64 `json:",omitempty"` // UI only, before sending each query
	Snapshots, PayloadBytes int     `json:",omitempty"`
}

type stamp struct {
	ID string
	NS int64
}

// Go's serialized time.Time drops the monotonic component. CLOCK_MONOTONIC
// has the same origin across the three local processes.
func now() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		panic(err)
	}
	return ts.Nano()
}

func sampleID(i int) string { return fmt.Sprintf("bench-%08d", i) }

func pluginID(i, count int) string {
	if count == 1 {
		return pluginName
	}
	return fmt.Sprintf("%s-%02d", pluginName, i+1)
}

// RunFixture is the private subprocess entry point for the headless UI/plugin.
// Traces go back to the parent only after the measured exchanges.
func RunFixture(in io.Reader, out io.Writer) error {
	var cfg fixtureConfig
	if err := json.NewDecoder(in).Decode(&cfg); err != nil {
		return err
	}
	if cfg.Count <= 0 || cfg.Timeout <= 0 || cfg.Plugins <= 0 || cfg.Burst <= 0 {
		return fmt.Errorf("invalid fixture count, concurrency or timeout")
	}
	l, err := openFixtureLink(cfg)
	if err != nil {
		return err
	}
	defer l.close()
	var trace fixtureTrace
	switch cfg.Role {
	case "plugin":
		trace.Marks, err = runPlugin(l, cfg.Count, cfg.Name)
	case "ui":
		trace, err = runUI(l, cfg)
	default:
		return fmt.Errorf("unknown fixture role %q", cfg.Role)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(trace)
}

func runPlugin(l link, count int, name string) ([]stamp, error) {
	if err := send(l, wire.PluginHello{Type: wire.MsgHello, Name: name}); err != nil {
		return nil, err
	}
	trace := make([]stamp, 0, count)
	for i := 0; i < count; i++ {
		var req wire.PluginRequest
		if err := receive(l, &req); err != nil {
			return nil, err
		}
		if req.Type != wire.MsgRequest || req.QueryID != sampleID(i) {
			return nil, fmt.Errorf("unexpected plugin request: %+v", req)
		}
		// Typed data so t0 includes encoding the suggestion itself.
		response := struct {
			Type    string          `json:"type"`
			QueryID string          `json:"query_id"`
			Data    wire.ResultItem `json:"data"`
		}{wire.MsgResponse, req.QueryID, wire.ResultItem{Plugin: name, ID: req.QueryID, Label: "Benchmark suggestion"}}
		t0 := now()
		if err := send(l, response); err != nil {
			return nil, err
		}
		trace = append(trace, stamp{req.QueryID, t0})
	}
	// Wait until the UI finished the final sample before exporting a trace.
	var done string
	if err := receive(l, &done); err != nil {
		return nil, err
	}
	if done != "done" {
		return nil, fmt.Errorf("unexpected final plugin message %q", done)
	}
	return trace, nil
}

func runUI(l link, cfg fixtureConfig) (fixtureTrace, error) {
	trace := fixtureTrace{Marks: make([]stamp, cfg.Count*cfg.Plugins), QueryStarts: make([]int64, cfg.Count)}
	plugins := make(map[string]int, cfg.Plugins)
	for i := range cfg.Plugins {
		plugins[pluginID(i, cfg.Plugins)] = i
	}
	for start := 0; start < cfg.Count; {
		end := start + min(cfg.Burst, cfg.Count-start)
		if start < cfg.Warmup {
			end = min(end, cfg.Warmup) // drain warmup completely before measuring
		}
		queries := make(map[string]int, end-start)
		for i := start; i < end; i++ {
			queries[sampleID(i)] = i
			trace.QueryStarts[i] = now()
			if err := send(l, wire.UIRequest{Type: "query", ClientID: "bench", Text: "bench"}); err != nil {
				return trace, err
			}
		}
		acked := make(map[string]bool, len(queries))
		remaining := len(queries) * cfg.Plugins
		for remaining > 0 {
			// Ack and update share the type/query_id envelope. Updates from
			// different queries can interleave and repeat earlier suggestions.
			var update wire.UpdateMessage
			if err := receive(l, &update); err != nil {
				return trace, err
			}
			idx, ok := queries[update.QueryID]
			if !ok {
				return trace, fmt.Errorf("unexpected query ID %s in burst", update.QueryID)
			}
			if update.Type == "ack" && !acked[update.QueryID] {
				acked[update.QueryID] = true
				continue
			}
			if update.Type != "update" || !acked[update.QueryID] {
				return trace, fmt.Errorf("unexpected %s for %s", update.Type, update.QueryID)
			}
			if idx >= cfg.Warmup {
				trace.Snapshots++
				trace.PayloadBytes += len(update.Payload)
			}
			var snapshot struct {
				QueryID string            `json:"query_id"`
				List    []wire.ResultItem `json:"list"`
			}
			if err := json.Unmarshal(update.Payload, &snapshot); err != nil {
				return trace, err
			}
			if snapshot.QueryID != update.QueryID {
				return trace, fmt.Errorf("snapshot ID mismatch for %s", update.QueryID)
			}
			for _, item := range snapshot.List {
				plugin, ok := plugins[item.Plugin]
				if !ok || item.ID != update.QueryID {
					return trace, fmt.Errorf("unexpected suggestion %s/%s", item.Plugin, item.ID)
				}
				pos := idx*cfg.Plugins + plugin
				if trace.Marks[pos].NS == 0 {
					trace.Marks[pos] = stamp{item.ID, now()}
					remaining--
				}
			}
		}
		start = end
	}
	return trace, nil
}
