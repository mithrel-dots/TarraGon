package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mithrel-dots/tarragon/internal/db"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

// Inspect each update synchronously in the store's snapshot contract tests.
func (s *aggregateStore) update(id, plugin string, elapsed float64, data json.RawMessage) ([]byte, bool) {
	var result []byte
	ok := s.SubmitResponse(plugin, wire.PluginResponse{QueryID: id, Data: data}, elapsed, func(msg *wire.UpdateMessage, _ []string) { result = msg.Payload })
	s.WaitForPublications()
	return result, ok
}

func (s *aggregateStore) markPluginError(id, plugin string, elapsed float64, reason string) ([]byte, bool) {
	var result []byte
	ok := s.submitError(id, plugin, elapsed, reason, func(msg *wire.UpdateMessage, _ []string) { result = msg.Payload })
	s.WaitForPublications()
	return result, ok
}

// Time a complete query with the same coalescing policy as the daemon.
func BenchmarkAggregateFanout(b *testing.B) {
	for _, count := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("%d-plugins", count), func(b *testing.B) {
			s := NewTransientQueryStore()
			plugins := make([]string, count)
			for i := range plugins {
				plugins[i] = fmt.Sprintf("plugin-%02d", i)
			}
			data := json.RawMessage(`{"id":"result","label":"Benchmark suggestion","score":0.8}`)
			b.ReportAllocs()
			for b.Loop() {
				s.PrepareQuery("q", "ui", "bench", plugins)
				for _, plugin := range plugins {
					if !s.SubmitResponse(plugin, wire.PluginResponse{QueryID: "q", Data: data}, 1, func(*wire.UpdateMessage, []string) {}) {
						b.Fatal("missing update")
					}
				}
				s.WaitForPublications()
				s.removeByClient("ui")
			}
		})
	}
}

func TestAggregateStoreCreateUpdateSnapshot(t *testing.T) {
	s := newAggregateStore(10, "global", nil, 0.3)
	s.create("q1", "cli1", "hello")
	if _, ok := s.setExpectedPlugins("q1", []string{"pluginA", "pluginB"}); !ok {
		t.Fatalf("expected setExpectedPlugins ok")
	}
	snap, ok := s.update("q1", "pluginA", 12.5, json.RawMessage(`{"v":1}`))
	if !ok {
		t.Fatalf("expected update ok")
	}
	var ag aggregate
	if err := json.Unmarshal(snap, &ag); err != nil {
		t.Fatalf("unmarshal snap: %v", err)
	}
	if ag.QueryID != "q1" || ag.Input != "hello" {
		t.Fatalf("unexpected snapshot header: %+v", ag)
	}
	r, exists := ag.Results["pluginA"]
	if !exists || r.ElapsedMs <= 0 || string(r.Data) != `{"v":1}` {
		t.Fatalf("unexpected result: %+v", ag.Results)
	}
	if ag.Plugins["pluginA"].State != "empty" {
		t.Fatalf("expected pluginA empty state, got %+v", ag.Plugins["pluginA"])
	}
	if ag.Plugins["pluginB"].State != "pending" {
		t.Fatalf("expected pluginB pending state, got %+v", ag.Plugins["pluginB"])
	}
}

func TestAggregatePreservesQueryReplacementAction(t *testing.T) {
	s := newAggregateStore(10, "global", nil, 0.3)
	s.create("q1", "cli1", "@anime frieren")
	if _, ok := s.setExpectedPlugins("q1", []string{"anime"}); !ok {
		t.Fatalf("expected setExpectedPlugins ok")
	}

	snap, ok := s.update("q1", "anime", 1, json.RawMessage(`{"results":[{"id":"episodes","label":"Episodes","actions":[{"name":"Episodes","type":"query_replace","query":"@anime episodes 154587"}]}]}`))
	if !ok {
		t.Fatalf("expected update ok")
	}

	var ag aggregate
	if err := json.Unmarshal(snap, &ag); err != nil {
		t.Fatalf("unmarshal snap: %v", err)
	}
	if got := ag.List[0].Actions[0]; got.Type != wire.ActionTypeQueryReplace || got.Query != "@anime episodes 154587" {
		t.Fatalf("unexpected query replacement action: %+v", got)
	}
}

func TestAggregateStorePluginStates(t *testing.T) {
	s := newAggregateStore(10, "global", nil, 0.3)
	s.create("q1", "cli1", "hello")
	snap, ok := s.setExpectedPlugins("q1", []string{"pluginA", "pluginB", "pluginC"})
	if !ok {
		t.Fatalf("expected setExpectedPlugins ok")
	}
	var ag aggregate
	if err := json.Unmarshal(snap, &ag); err != nil {
		t.Fatalf("unmarshal initial snapshot: %v", err)
	}
	if ag.Plugins["pluginA"].State != "pending" || ag.Plugins["pluginB"].State != "pending" || ag.Plugins["pluginC"].State != "pending" {
		t.Fatalf("expected all plugins pending, got %+v", ag.Plugins)
	}

	snap, ok = s.update("q1", "pluginA", 2.5, json.RawMessage(`{"results":[{"id":"a1","label":"A1"}]}`))
	if !ok {
		t.Fatalf("expected pluginA update ok")
	}
	if err := json.Unmarshal(snap, &ag); err != nil {
		t.Fatalf("unmarshal pluginA snapshot: %v", err)
	}
	if ag.Plugins["pluginA"].State != "done" || ag.Plugins["pluginA"].Count != 1 {
		t.Fatalf("expected pluginA done with count=1, got %+v", ag.Plugins["pluginA"])
	}

	snap, ok = s.update("q1", "pluginB", 3.5, json.RawMessage(`{"results":[]}`))
	if !ok {
		t.Fatalf("expected pluginB update ok")
	}
	if err := json.Unmarshal(snap, &ag); err != nil {
		t.Fatalf("unmarshal pluginB snapshot: %v", err)
	}
	if ag.Plugins["pluginB"].State != "empty" {
		t.Fatalf("expected pluginB empty, got %+v", ag.Plugins["pluginB"])
	}

	snap, ok = s.markPluginError("q1", "pluginC", 4.5, "boom")
	if !ok {
		t.Fatalf("expected pluginC error ok")
	}
	if err := json.Unmarshal(snap, &ag); err != nil {
		t.Fatalf("unmarshal pluginC snapshot: %v", err)
	}
	if ag.Plugins["pluginC"].State != "error" || ag.Plugins["pluginC"].Error != "boom" {
		t.Fatalf("expected pluginC error state, got %+v", ag.Plugins["pluginC"])
	}
}

func TestAggregateReplacesCachedItemsAndRescoresEarlierResponses(t *testing.T) {
	s := newAggregateStore(1, "global", nil, 0.5)
	s.PrepareQuery("q", "ui", "query", []string{"a", "b"})
	update := func(plugin, data string) aggregate {
		t.Helper()
		snap, ok := s.update("q", plugin, 1, json.RawMessage(data))
		if !ok {
			t.Fatalf("missing update for %s", plugin)
		}
		var ag aggregate
		if err := json.Unmarshal(snap, &ag); err != nil {
			t.Fatal(err)
		}
		return ag
	}

	update("a", `{"items":[{"id":"a1","score":4},{"id":"a2","score":2}]}`)
	// Simulate a frecency refresh between replies. Reusing previously ranked
	// items would apply the weight twice or leave the old ranking in place.
	s.frecencyCache = map[string]float64{"a:a2": 10}
	ag := update("b", `["b1"]`)
	if len(ag.List) != 3 || ag.List[0].ID != "a2" || ag.List[0].Score != 6 || ag.List[1].Score != 2 {
		t.Fatalf("expected scores from the original plugin items and refreshed frecency: %+v", ag.List)
	}

	ag = update("a", `{"error":"unavailable"}`)
	if len(ag.List) != 1 || ag.List[0].ID != "b1" || ag.Plugins["a"].Error != "unavailable" {
		t.Fatalf("stale suggestions after replacement: %+v", ag)
	}
	ag = update("b", `[]`)
	if ag.List == nil || len(ag.List) != 0 || ag.Plugins["b"].State != "empty" {
		t.Fatalf("expected an empty result list: %+v", ag)
	}
}

func TestAggregateStoreEvictsOldest(t *testing.T) {
	s := newAggregateStore(2, "global", nil, 0.3)
	s.create("q1", "cli", "a")
	s.create("q2", "cli", "b")
	s.create("q3", "cli", "c") // should evict q1
	if _, ok := s.update("q1", "p", 1, json.RawMessage(`{}`)); ok {
		t.Fatalf("expected q1 evicted")
	}
	if _, ok := s.update("q2", "p", 1, json.RawMessage(`{}`)); !ok {
		t.Fatalf("expected q2 present")
	}
	if _, ok := s.update("q3", "p", 1, json.RawMessage(`{}`)); !ok {
		t.Fatalf("expected q3 present")
	}
}

func TestAggregateStoreRemoveByClient(t *testing.T) {
	s := newAggregateStore(10, "global", nil, 0.3)
	s.create("q1", "cliA", "x")
	s.create("q2", "cliA", "y")
	s.create("q3", "cliB", "z")
	n := s.removeByClient("cliA")
	if n != 2 {
		t.Fatalf("expected removed 2, got %d", n)
	}
	if _, ok := s.update("q1", "p", 1, json.RawMessage(`{}`)); ok {
		t.Fatalf("q1 should be gone")
	}
	if _, ok := s.update("q3", "p", 1, json.RawMessage(`{}`)); !ok {
		t.Fatalf("q3 should remain")
	}
}

func TestOrderResultsGlobalSortsByScoreDescending(t *testing.T) {
	items := []wire.ResultItem{
		{ID: "a", Plugin: "p1", Score: 0.2},
		{ID: "b", Plugin: "p2", Score: 0.9},
		{ID: "c", Plugin: "p3", Score: 0.5},
		{ID: "d", Plugin: "p4", Score: 0.5},
	}

	got := orderResults(items, "global", nil, 0.3)
	gotIDs := []string{got[0].ID, got[1].ID, got[2].ID, got[3].ID}
	wantIDs := []string{"b", "c", "d", "a"}

	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("unexpected order: got=%v want=%v", gotIDs, wantIDs)
	}
}

func TestOrderResultsGroupedSortsGroupsByBestScore(t *testing.T) {
	items := []wire.ResultItem{
		{ID: "a1", Plugin: "A", Score: 0.2},
		{ID: "a2", Plugin: "A", Score: 0.8},
		{ID: "b1", Plugin: "B", Score: 0.9},
		{ID: "b2", Plugin: "B", Score: 0.1},
		{ID: "c1", Plugin: "C", Score: 0.3},
	}

	got := orderResults(items, "grouped", nil, 0.3)
	gotIDs := []string{got[0].ID, got[1].ID, got[2].ID, got[3].ID, got[4].ID}
	wantIDs := []string{"b1", "b2", "a1", "a2", "c1"}

	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("unexpected grouped order: got=%v want=%v", gotIDs, wantIDs)
	}
}

func TestAggregateStoreUpdateUsesConfiguredOrderingMode(t *testing.T) {
	s := newAggregateStore(10, "grouped", nil, 0.3)
	s.create("q1", "cli", "hello")

	_, ok := s.update("q1", "alpha", 1, json.RawMessage(`{
		"items": [
			{"id":"a1","label":"A1","score":0.7},
			{"id":"a2","label":"A2","score":0.6}
		]
	}`))
	if !ok {
		t.Fatalf("first update failed")
	}

	snap, ok := s.update("q1", "beta", 1, json.RawMessage(`{
		"items": [
			{"id":"b1","label":"B1","score":0.95},
			{"id":"b2","label":"B2","score":0.1}
		]
	}`))
	if !ok {
		t.Fatalf("second update failed")
	}

	var ag aggregate
	if err := json.Unmarshal(snap, &ag); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}

	gotIDs := []string{ag.List[0].ID, ag.List[1].ID, ag.List[2].ID, ag.List[3].ID}
	wantIDs := []string{"b1", "b2", "a1", "a2"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("unexpected list order: got=%v want=%v", gotIDs, wantIDs)
	}
}

func TestOrderResultsGlobalBlendsFrecencyAndSetsField(t *testing.T) {
	items := []wire.ResultItem{{ID: "id1", Plugin: "p", Score: 0.5}}
	scores := map[string]float64{"p:id1": 1.5}

	got := orderResults(items, "global", scores, 0.3)
	if len(got) != 1 {
		t.Fatalf("expected 1 item, got %d", len(got))
	}
	if got[0].FrecencyScore != 1.5 {
		t.Fatalf("unexpected frecency score: %v", got[0].FrecencyScore)
	}
	want := (1-0.3)*0.5 + 0.3*1.5
	if math.Abs(got[0].Score-want) > 1e-9 {
		t.Fatalf("unexpected blended score: got=%v want=%v", got[0].Score, want)
	}
}

func TestAggregateStoreRefreshFrecencyCache(t *testing.T) {
	dir := t.TempDir()
	database, err := db.Open(filepath.Join(dir, "frecency.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if err := database.RecordSelection(context.Background(), "plug", "id-1"); err != nil {
		t.Fatalf("record selection: %v", err)
	}

	s := newAggregateStore(10, "global", database, 0.3)
	if err := s.RefreshFrecencyCache(context.Background()); err != nil {
		t.Fatalf("refresh frecency cache: %v", err)
	}

	scores := s.frecencyScoresSnapshot()
	if scores["plug:id-1"] <= 0 {
		t.Fatalf("expected cached frecency score, got %v", scores["plug:id-1"])
	}
}
