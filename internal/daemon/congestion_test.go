package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

// Use the daemon's actual connection handlers. In-process fixture clients keep
// a common monotonic clock; bench ipc separately measures external processes.
func BenchmarkDaemonFanoutBurst(b *testing.B) {
	benchmarkDaemonDelivery(b, "", 0)
}

func BenchmarkDaemonLatestQuery(b *testing.B) {
	for _, interval := range []time.Duration{0, 2 * time.Millisecond} {
		b.Run(interval.String(), func(b *testing.B) {
			benchmarkDaemonDelivery(b, "search", interval)
		})
	}
}

func benchmarkDaemonDelivery(b *testing.B, stream string, interval time.Duration) {
	const fanout, burst = 50, 8
	dir := b.TempDir()
	b.Setenv("TARRAGON_PLUGINS_SOCKET", filepath.Join(dir, "p"))
	b.Setenv("TARRAGON_UI_SOCKET", filepath.Join(dir, "u"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewTransientQueryStore()
	ui := newUIRegistry()
	requests, registry := testPluginListener(b, ctx, store, ui)
	manager := plugins.NewManager(dir)
	var sent sync.Map
	var streamDispatches atomic.Int64
	for i := range fanout {
		name := fmt.Sprintf("p-%02d", i)
		manager.Plugins[name] = &plugins.Plugin{Config: plugins.PluginConfig{
			Name: name, Enabled: true, Lifecycle: plugins.LifecycleDaemon, ProvidesGeneral: true,
		}}
		c, err := net.Dial("unix", wire.ResolvePluginsSocketPath())
		if err != nil {
			b.Fatal(err)
		}
		defer c.Close()
		if err := wire.WriteMsg(c, wire.PluginHello{Type: wire.MsgHello, Name: name}); err != nil {
			b.Fatal(err)
		}
		go func() {
			scanner := wire.NewScanner(c)
			for {
				var req wire.PluginRequest
				if wire.ReadMsg(scanner, &req) != nil {
					return
				}
				if stream != "" {
					streamDispatches.Add(1)
				}
				sent.Store(name+req.QueryID, time.Now())
				if wire.WriteMsg(c, wire.PluginResponse{Type: wire.MsgResponse, QueryID: req.QueryID, Data: json.RawMessage(`{"id":"result","label":"Benchmark suggestion"}`)}) != nil {
					return
				}
			}
		}()
	}
	testUIListener(b, ctx, manager, requests, registry, store, ui, nil)
	c, err := dialUnixWithRetry(wire.ResolveUISocketPath(), 3*time.Second)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	deadline := time.Now().Add(3 * time.Second)
	for len(registry.Names()) != fanout {
		if time.Now().After(deadline) {
			b.Fatal("plugins did not register")
		}
		time.Sleep(time.Millisecond)
	}
	scanner := wire.NewScanner(c)
	var latencies, first, complete []int64
	var snapshots, payloadBytes int
	for b.Loop() {
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		var starts [burst]time.Time
		senderDone := make(chan error, 1)
		go func() {
			for i := range burst {
				if i != 0 && interval != 0 {
					time.Sleep(interval)
				}
				starts[i] = time.Now()
				if err := wire.WriteMsg(c, wire.UIRequest{Type: "query", Text: fmt.Sprintf("bench-%d", i), QueryStream: stream}); err != nil {
					senderDone <- err
					return
				}
			}
			senderDone <- nil
		}()
		active := burst
		if stream != "" {
			active = 1
		}
		acked := 0
		ids := make(map[string]int)
		var latestFirst, latestLast time.Time
		for remaining := active * fanout; remaining > 0; {
			var msg wire.UpdateMessage
			if err := wire.ReadMsg(scanner, &msg); err != nil {
				b.Fatal(err)
			}
			if msg.Type == "ack" {
				ids[msg.QueryID] = acked
				acked++
				continue
			}
			var snap struct {
				List []wire.ResultItem `json:"list"`
			}
			if err := json.Unmarshal(msg.Payload, &snap); err != nil {
				b.Fatal(err)
			}
			received := time.Now()
			snapshots++
			payloadBytes += len(msg.Payload)
			if stream != "" && ids[msg.QueryID] != burst-1 {
				continue
			}
			for _, item := range snap.List {
				if t0, ok := sent.LoadAndDelete(item.Plugin + msg.QueryID); ok {
					latencies = append(latencies, received.Sub(t0.(time.Time)).Nanoseconds())
					remaining--
					if ids[msg.QueryID] == burst-1 {
						if latestFirst.IsZero() {
							latestFirst = received
						}
						latestLast = received
					}
				}
			}
		}
		if err := <-senderDone; err != nil {
			b.Fatal(err)
		}
		if acked != burst {
			b.Fatalf("received %d/%d acknowledgements", acked, burst)
		}
		first = append(first, latestFirst.Sub(starts[burst-1]).Nanoseconds())
		complete = append(complete, latestLast.Sub(starts[burst-1]).Nanoseconds())
		// Late replies from superseded queries are never needed for this workload.
		sent.Clear()
	}
	for label, samples := range map[string][]int64{"p99-ms": latencies, "last-query-first-p99-ms": first, "last-query-all-p99-ms": complete} {
		slices.Sort(samples)
		b.ReportMetric(float64(samples[int(math.Ceil(.99*float64(len(samples))))-1])/1e6, label)
	}
	if stream != "" {
		b.ReportMetric(burst-1, "superseded/burst")
		olderDispatches := float64(streamDispatches.Load())/float64(b.N) - fanout
		b.ReportMetric(olderDispatches, "older-dispatched/burst")
		b.ReportMetric((burst-1)*fanout-olderDispatches, "older-skipped/burst")
	}
	b.ReportMetric(float64(snapshots)/float64(b.N*burst), "snapshots/query")
	b.ReportMetric(float64(payloadBytes)/float64(b.N*burst), "bytes/query")
}
