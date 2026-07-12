// Package ipc benchmarks single-suggestion delivery across local transports.
package ipc

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/mithrel-dots/tarragon/internal/daemon"
	"github.com/mithrel-dots/tarragon/internal/texttable"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/nats-io/nats-server/v2/server"
)

type Options struct {
	Samples, Warmup, Rounds int
	Plugins, Burst          int
	Timeout                 time.Duration
}

type route struct{ plugin, ui string }

type coreStamp struct {
	id     string
	t1, t2 int64
}

type durations [4]int64 // ingress, aggregation, egress, end-to-end, in ns

type roundResult struct {
	suggestions []durations
	first       []int64 // query send to first visible suggestion
	complete    []int64 // query send to last distinct suggestion at the UI
	elapsedNS   int64   // first measured send to last measured delivery
}

func join(plugin, ui []stamp, core []coreStamp, warmup int) ([]durations, error) {
	if len(plugin) != len(core) || len(ui) != len(core) {
		return nil, fmt.Errorf("incomplete traces: plugin=%d core=%d UI=%d", len(plugin), len(core), len(ui))
	}
	result := make([]durations, 0, len(core)-warmup)
	for i, c := range core {
		p, u := plugin[i], ui[i]
		if p.ID != c.id || u.ID != c.id || p.NS > c.t1 || c.t1 > c.t2 || c.t2 > u.NS {
			return nil, fmt.Errorf("uncorrelated or out-of-order timestamps at suggestion %s", c.id)
		}
		if i >= warmup {
			result = append(result, durations{c.t1 - p.NS, c.t2 - c.t1, u.NS - c.t2, u.NS - p.NS})
		}
	}
	return result, nil
}

func measure(ctx context.Context, r route, opts Options) (*roundResult, error) {
	dir, err := os.MkdirTemp("", "tg-ipc-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	cfg := fixtureConfig{Count: opts.Warmup + opts.Samples, Warmup: opts.Warmup, Timeout: opts.Timeout,
		Plugins: opts.Plugins, Burst: opts.Burst}
	var broker *server.Server
	if r.plugin != direct || r.ui != direct {
		cfg.Token = rand.Text()
		broker, err = server.NewServer(&server.Options{
			Host: "127.0.0.1", Port: -1, Authorization: cfg.Token,
			DontListen: r.plugin != tcp && r.ui != tcp, NoSigs: true, NoLog: true,
		})
		if err != nil {
			return nil, err
		}
		broker.Start()
		defer func() {
			broker.Shutdown()
			broker.WaitForShutdown()
		}()
		if !broker.ReadyForConnections(opts.Timeout) {
			return nil, fmt.Errorf("embedded NATS did not become ready")
		}
	}

	cfg.Role, cfg.Path = "plugin", r.plugin
	plugins := make([]*peer, cfg.Plugins)
	names := make([]string, len(plugins))
	for i := range plugins {
		cfg.Name = pluginID(i, len(plugins))
		plugin, err := startPeer(ctx, dir, broker, cfg)
		if err != nil {
			return nil, fmt.Errorf("start %s: %w", cfg.Name, err)
		}
		defer plugin.stop()
		var hello wire.PluginHello
		if err := receive(plugin, &hello); err != nil {
			return nil, fmt.Errorf("receive %s hello: %w", cfg.Name, err)
		}
		if hello.Name != cfg.Name || hello.Type != wire.MsgHello {
			return nil, fmt.Errorf("unexpected plugin hello: %+v", hello)
		}
		plugins[i], names[i] = plugin, hello.Name
	}
	cfg.Role, cfg.Name, cfg.Path = "ui", "ui", r.ui
	ui, err := startPeer(ctx, dir, broker, cfg)
	if err != nil {
		return nil, fmt.Errorf("start UI: %w", err)
	}
	defer ui.stop()

	core, err := serveQueries(plugins, names, ui, cfg.Count)
	if err != nil {
		return nil, err
	}
	uiTrace, err := ui.trace()
	if err != nil {
		return nil, err
	}
	if len(uiTrace.Marks) != cfg.Count*cfg.Plugins || len(uiTrace.QueryStarts) != cfg.Count {
		return nil, fmt.Errorf("incomplete UI trace")
	}
	for _, plugin := range plugins {
		if err := send(plugin, "done"); err != nil {
			return nil, err
		}
	}
	result := &roundResult{first: make([]int64, opts.Samples), complete: make([]int64, opts.Samples)}
	var lastDelivery int64
	for i, p := range plugins {
		plugin, err := p.trace()
		if err != nil {
			return nil, err
		}
		delivered := make([]stamp, cfg.Count)
		for q := range delivered {
			delivered[q] = uiTrace.Marks[q*cfg.Plugins+i]
			if q >= opts.Warmup {
				j := q - opts.Warmup
				fromQuery := delivered[q].NS - uiTrace.QueryStarts[q]
				if i == 0 || fromQuery < result.first[j] {
					result.first[j] = fromQuery
				}
				result.complete[j] = max(result.complete[j], fromQuery)
				lastDelivery = max(lastDelivery, delivered[q].NS)
			}
		}
		joined, err := join(plugin.Marks, delivered, core[i], opts.Warmup)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", names[i], err)
		}
		result.suggestions = append(result.suggestions, joined...)
	}
	result.elapsedNS = lastDelivery - uiTrace.QueryStarts[opts.Warmup]
	return result, nil
}

// Read each plugin concurrently, like the daemon's per-connection handlers.
// Cumulative snapshots must reach the UI in aggregation order: take the output
// lock before releasing the aggregation lock, so the next snapshot can be built
// while this one is written. Time waiting to publish is part of core->UI.
func serveQueries(plugins []*peer, names []string, ui *peer, count int) ([][]coreStamp, error) {
	store := daemon.NewTransientQueryStore()
	var aggregate, output sync.Mutex
	core := make([][]coreStamp, len(plugins))
	finished := make(chan error, len(plugins)+1)
	handlePlugin := func(i int) error {
		core[i] = make([]coreStamp, 0, count)
		for q := range count {
			var response wire.PluginResponse
			if err := receive(plugins[i], &response); err != nil {
				return fmt.Errorf("%s response %d: %w", names[i], q, err)
			}
			t1 := now()
			id := response.QueryID
			if id != sampleID(q) || response.Type != wire.MsgResponse {
				return fmt.Errorf("unexpected response from %s for %s", names[i], id)
			}
			aggregate.Lock()
			update, ok := store.ApplyPluginResponse(names[i], response, 0)
			t2 := now()
			output.Lock()
			aggregate.Unlock()
			if !ok {
				output.Unlock()
				return fmt.Errorf("missing aggregate for %s", id)
			}
			err := send(ui, update)
			output.Unlock()
			if err != nil {
				return fmt.Errorf("publish %s/%s: %w", names[i], id, err)
			}
			core[i] = append(core[i], coreStamp{id, t1, t2})
		}
		return nil
	}
	for i := range plugins {
		go func() { finished <- handlePlugin(i) }()
	}
	dispatch := func() error {
		for i := range count {
			var query wire.UIRequest
			if err := receive(ui, &query); err != nil {
				return fmt.Errorf("UI query %d: %w", i, err)
			}
			if query.Type != "query" {
				return fmt.Errorf("unexpected UI query: %+v", query)
			}
			id := sampleID(i)
			pending := store.PrepareQuery(id, "bench", query.Text, names)
			if pending == nil {
				return fmt.Errorf("could not create query %s", id)
			}
			output.Lock()
			err := send(ui, wire.AckMessage{Type: "ack", QueryID: id})
			if err == nil {
				err = send(ui, pending)
			}
			output.Unlock()
			if err != nil {
				return fmt.Errorf("start query %s: %w", id, err)
			}
			for j, plugin := range plugins {
				if err := send(plugin, wire.PluginRequest{Type: wire.MsgRequest, QueryID: id, Text: query.Text}); err != nil {
					return fmt.Errorf("dispatch %s to %s: %w", id, names[j], err)
				}
			}
		}
		return nil
	}
	go func() { finished <- dispatch() }()
	var firstErr error
	for range len(plugins) + 1 {
		if err := <-finished; err != nil && firstErr == nil {
			firstErr = err
			ui.close()
			for _, p := range plugins {
				p.close()
			}
		}
	}
	return core, firstErr
}

func Run(ctx context.Context, opts Options, out io.Writer) error {
	if opts.Samples <= 0 || opts.Rounds <= 0 || opts.Timeout <= 0 || opts.Warmup < 0 ||
		opts.Warmup > math.MaxInt-opts.Samples {
		return fmt.Errorf("samples, rounds and timeout must be positive; warmup must be nonnegative and fit in the sample count")
	}
	if opts.Plugins < 1 || opts.Plugins > math.MaxInt/(opts.Samples+opts.Warmup) {
		return fmt.Errorf("plugins must be positive and the total number of suggestions must fit in an int")
	}
	if opts.Burst < 1 || opts.Burst > 64 {
		return fmt.Errorf("burst must be between 1 and the transient core's 64-query capacity")
	}
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	paths := []route{{direct, direct}, {tcp, tcp}, {bridged, bridged}}
	results := make(map[route]*roundResult)
	for _, r := range paths {
		results[r] = &roundResult{}
	}
	fmt.Fprintf(out, "Suggestion delivery: %d queries + %d warmup per path per round; %d plugins, bursts of %d; %d rounds; timeout %s\n",
		opts.Samples, opts.Warmup, opts.Plugins, opts.Burst, opts.Rounds, opts.Timeout)
	for round := 0; round < opts.Rounds; round++ {
		for j := range paths {
			r := paths[(j+round)%len(paths)]
			values, err := measure(ctx, r, opts)
			if err != nil {
				return fmt.Errorf("round %d, %s -> %s (timeouts=%d; incomplete run): %w",
					round+1, r.plugin, r.ui, timeoutCount(err), err)
			}
			result := results[r]
			result.suggestions = append(result.suggestions, values.suggestions...)
			result.first = append(result.first, values.first...)
			result.complete = append(result.complete, values.complete...)
			result.elapsedNS += values.elapsedNS
		}
	}
	columns := []texttable.Column{
		{Header: "Plugin path"}, {Header: "UI path"}, {Header: "Sent/received"}, {Header: "Timeouts"},
		{Header: "Plugin->core"}, {Header: "Core"}, {Header: "Core->UI"}, {Header: "Plugin->UI"},
		{Header: "Query->first"}, {Header: "Query->all"}, {Header: "Queries/s"},
	}
	var rows [][]string
	for _, r := range paths {
		result := results[r]
		values := result.suggestions
		row := []string{r.plugin, r.ui, fmt.Sprintf("%d/%d", len(values), len(values)), "0"}
		for stage := 0; stage < len(durations{}); stage++ {
			times := make([]int64, len(values))
			for i := range values {
				times[i] = values[i][stage]
			}
			row = append(row, latencySummary(times))
		}
		qps := float64(len(result.complete)) * 1e9 / float64(result.elapsedNS)
		row = append(row, latencySummary(result.first), latencySummary(result.complete), fmt.Sprintf("%.1f", qps))
		rows = append(rows, row)
	}
	fmt.Fprintln(out, "Latency in microseconds (mean/p50/p95/p99), pooled across rounds:")
	texttable.Render(out, columns, rows)
	return nil
}

func latencySummary(times []int64) string {
	var totalNS float64
	for _, ns := range times {
		totalNS += float64(ns)
	}
	slices.Sort(times)
	us := func(p int) float64 {
		index := int(math.Ceil(float64(len(times))*float64(p)/100)) - 1
		return float64(times[index]) / 1000
	}
	meanUS := totalNS / float64(len(times)) / 1000
	return fmt.Sprintf("%.1f/%.1f/%.1f/%.1f", meanUS, us(50), us(95), us(99))
}

func timeoutCount(err error) int {
	if isTimeout(err) {
		return 1
	}
	return 0
}
