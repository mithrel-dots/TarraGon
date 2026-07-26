package daemon

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/mithrel-dots/tarragon/internal/db"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

// aggregate store keeps full results per query for streaming snapshots
type aggResult struct {
	ElapsedMs float64           `json:"elapsed_ms"`
	Data      json.RawMessage   `json:"data"`
	items     []wire.ResultItem // normalized, with the original plugin scores
}

type aggPluginState struct {
	State     string  `json:"state"`
	Count     int     `json:"count,omitempty"`
	ElapsedMs float64 `json:"elapsed_ms,omitempty"`
	Error     string  `json:"error,omitempty"`
}

type aggregate struct {
	QueryID         string                    `json:"query_id"`
	Input           string                    `json:"input"`
	StartedAtUnixMs int64                     `json:"started_at_unix_ms"`
	Results         map[string]aggResult      `json:"results"`
	Plugins         map[string]aggPluginState `json:"plugins,omitempty"`
	List            []wire.ResultItem         `json:"list"`
	Client          string                    `json:"-"`
	Created         time.Time                 `json:"-"`
}

// Each live query has a single publisher. Plugin readers only replace their
// own state; ranking, JSON encoding and delivery happen outside both locks.
type queryState struct {
	aggregate
	mu              sync.Mutex
	publishing      bool
	changed         []string
	session, stream string
	ctx             context.Context
	cancel          context.CancelFunc
	started         map[string]time.Time
	encoders        int
	mapsShared      bool
}

type aggregateStore struct {
	mu           sync.RWMutex
	publishers   sync.WaitGroup
	limit        int
	orderingMode string
	frecencyDB   *db.DB
	frecencyW    float64

	frecencyCacheMu sync.RWMutex
	frecencyCache   map[string]float64

	order   []string
	byID    map[string]*queryState
	byOwner map[string]map[string]struct{}
}

// NewTransientQueryStore provides the daemon's normal aggregation pipeline
// without opening a user's frecency database.
func NewTransientQueryStore() *aggregateStore {
	return newAggregateStore(64, "global", nil, 0.3)
}

func (s *aggregateStore) PrepareQuery(qid, client, input string, plugins []string) *wire.UpdateMessage {
	s.create(qid, client, input)
	snapshot, ok := s.setExpectedPlugins(qid, plugins)
	if !ok {
		return nil
	}
	return &wire.UpdateMessage{Type: "update", QueryID: qid, Payload: snapshot}
}

func newAggregateStore(limit int, orderingMode string, frecencyDB *db.DB, frecencyWeight float64) *aggregateStore {
	if orderingMode == "" {
		orderingMode = "global"
	}
	if frecencyWeight < 0 {
		frecencyWeight = 0
	}
	if frecencyWeight > 1 {
		frecencyWeight = 1
	}
	s := &aggregateStore{
		limit:         limit,
		orderingMode:  orderingMode,
		frecencyDB:    frecencyDB,
		frecencyW:     frecencyWeight,
		frecencyCache: make(map[string]float64),
		byID:          make(map[string]*queryState),
		byOwner:       make(map[string]map[string]struct{}),
	}
	if err := s.RefreshFrecencyCache(context.Background()); err != nil {
		log.Printf("[AGG] initial frecency cache refresh failed: %v", err)
	}
	return s
}

// RefreshFrecencyCache reloads frecency scores from DB into memory.
func (s *aggregateStore) RefreshFrecencyCache(ctx context.Context) error {
	if s == nil || s.frecencyDB == nil {
		return nil
	}

	scores, err := s.frecencyDB.GetFrecencyScores(ctx)
	if err != nil {
		return err
	}

	s.frecencyCacheMu.Lock()
	s.frecencyCache = scores
	s.frecencyCacheMu.Unlock()
	return nil
}

func (s *aggregateStore) frecencyScoresSnapshot() map[string]float64 {
	s.frecencyCacheMu.RLock()
	defer s.frecencyCacheMu.RUnlock()
	// Refresh publishes a new map; callers can read the old one until done.
	return s.frecencyCache
}

func (s *aggregateStore) create(qid, client, input string) {
	s.startQuery(context.Background(), qid, client, input, client, "")
}

func (s *aggregateStore) startQuery(ctx context.Context, qid, client, input, session, stream string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[qid]; ok {
		return
	}
	if stream != "" {
		for id := range s.byOwner[session] {
			if s.byID[id].stream == stream {
				s.removeLocked(id)
				break
			}
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	if s.limit > 0 && len(s.byID) >= s.limit {
		s.removeLocked(s.order[0])
	}
	s.byID[qid] = &queryState{ctx: ctx, cancel: cancel, session: session, stream: stream, aggregate: aggregate{
		QueryID:         qid,
		Input:           input,
		StartedAtUnixMs: time.Now().UnixMilli(),
		Results:         make(map[string]aggResult),
		Plugins:         make(map[string]aggPluginState),
		Client:          client,
		Created:         time.Now(),
	}}
	s.order = append(s.order, qid)
	if s.byOwner[session] == nil {
		s.byOwner[session] = make(map[string]struct{})
	}
	s.byOwner[session][qid] = struct{}{}
}

func (s *aggregateStore) removeLocked(id string) {
	q := s.byID[id]
	if q.cancel != nil {
		q.cancel()
	}
	delete(s.byID, id)
	delete(s.byOwner[q.session], id)
	if len(s.byOwner[q.session]) == 0 {
		delete(s.byOwner, q.session)
	}
	i := slices.Index(s.order, id)
	s.order = slices.Delete(s.order, i, i+1)
}

func (s *aggregateStore) query(qid string) *queryState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byID[qid]
}

func (s *aggregateStore) setExpectedPlugins(qid string, plugins []string) ([]byte, bool) {
	ag := s.query(qid)
	if ag == nil {
		return nil, false
	}
	ag.mu.Lock()
	ag.detachSnapshot()
	ag.Plugins = make(map[string]aggPluginState, len(plugins))
	for _, name := range plugins {
		ag.Plugins[name] = aggPluginState{State: "pending"}
	}
	return s.encodeUnlocked(ag)
}

// SnapshotPublisher receives a cumulative snapshot and the names of the
// plugins whose updates it incorporates since the previous publication.
type SnapshotPublisher func(*wire.UpdateMessage, []string)

// SubmitResponse accepts a plugin's replacement result for a live query.
// Pass the same publisher for every response to that query; callbacks are
// serialized per query and may run asynchronously.
func (s *aggregateStore) SubmitResponse(plugin string, resp wire.PluginResponse, elapsed float64, publish SnapshotPublisher) bool {
	q := s.query(resp.QueryID)
	if q == nil {
		return false
	}
	items, errMsg := normalizeResults(plugin, resp.Data)
	count := len(items)
	state := "done"
	if errMsg != "" {
		state = "error"
	} else if count == 0 {
		state = "empty"
	}

	q.mu.Lock()
	q.detachSnapshot()
	q.Results[plugin] = aggResult{ElapsedMs: elapsed, Data: resp.Data, items: items}
	q.Plugins[plugin] = aggPluginState{State: state, Count: count, ElapsedMs: elapsed, Error: errMsg}
	s.scheduleLocked(q, plugin, publish)
	return true
}

func (s *aggregateStore) submitError(qid, plugin string, elapsed float64, errMsg string, publish SnapshotPublisher) bool {
	q := s.query(qid)
	if q == nil {
		return false
	}
	q.mu.Lock()
	q.detachSnapshot()
	q.Plugins[plugin] = aggPluginState{State: "error", ElapsedMs: elapsed, Error: errMsg}
	s.scheduleLocked(q, plugin, publish)
	return true
}

// scheduleLocked releases q.mu. No timer delays the first or final update.
func (s *aggregateStore) scheduleLocked(q *queryState, plugin string, publish SnapshotPublisher) {
	if !slices.Contains(q.changed, plugin) {
		q.changed = append(q.changed, plugin)
	}
	if q.publishing {
		q.mu.Unlock()
		return
	}
	q.publishing = true
	single := len(q.Plugins) == 1
	s.publishers.Add(1)
	q.mu.Unlock()
	if single {
		s.drain(q, publish)
	} else {
		go s.drain(q, publish)
	}
}

func (s *aggregateStore) drain(q *queryState, publish SnapshotPublisher) {
	defer s.publishers.Done()
	for {
		q.mu.Lock()
		changed := q.changed
		if len(changed) == 0 {
			q.publishing = false
			q.mu.Unlock()
			return
		}
		q.changed = nil
		if data, ok := s.encodeUnlocked(q); ok && s.query(q.QueryID) == q {
			publish(&wire.UpdateMessage{Type: "update", QueryID: q.QueryID, Payload: data}, changed)
		}
	}
}

// WaitForPublications drains accepted updates once all submitters have stopped.
func (s *aggregateStore) WaitForPublications() { s.publishers.Wait() }

// Called before mutating either map under q.mu. When encoding and ingestion
// overlap, copy the maps once for the entire next batch. Payloads and items
// are immutable. Uncontended queries need no copies.
func (q *queryState) detachSnapshot() {
	if q.mapsShared {
		q.Results = maps.Clone(q.Results)
		q.Plugins = maps.Clone(q.Plugins)
		q.mapsShared = false
	}
}

// Called with q.mu held; returns with it unlocked.
func (s *aggregateStore) encodeUnlocked(q *queryState) ([]byte, bool) {
	snapshot := q.aggregate
	q.encoders++
	q.mapsShared = true
	q.mu.Unlock()
	data, ok := s.encodeSnapshot(&snapshot)
	q.mu.Lock()
	q.encoders--
	if q.encoders == 0 {
		q.mapsShared = false
	}
	q.mu.Unlock()
	return data, ok
}

func (s *aggregateStore) encodeSnapshot(ag *aggregate) ([]byte, bool) {
	if len(ag.Results) != 0 {
		total := 0
		for _, res := range ag.Results {
			total += len(res.items)
		}
		list := make([]wire.ResultItem, 0, total)
		for _, res := range ag.Results {
			list = append(list, res.items...)
		}
		ag.List = orderResults(list, s.orderingMode, s.frecencyScoresSnapshot(), s.frecencyW)
	}
	snap, err := json.Marshal(ag)
	if err != nil {
		return nil, false
	}
	return snap, true
}

func normalizeResults(plugin string, raw json.RawMessage) ([]wire.ResultItem, string) {
	// Object with known array fields.
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) == nil {
		errMsg := readStringField(obj, "error")
		for _, k := range []string{"suggestions", "variants", "items", "choices", "results"} {
			if arr, ok := obj[k]; ok {
				if items := parseArrayItems(plugin, arr); len(items) > 0 {
					return items, errMsg
				}
			}
		}
		// Single object with id/label/title/text.
		if item, ok := parseObjectItem(plugin, obj); ok {
			return []wire.ResultItem{item}, errMsg
		}
		return nil, errMsg
	}
	// Top-level array.
	return parseArrayItems(plugin, raw), ""
}

func parseArrayItems(plugin string, raw json.RawMessage) []wire.ResultItem {
	// Array of objects.
	var objs []map[string]json.RawMessage
	if json.Unmarshal(raw, &objs) == nil && len(objs) > 0 {
		out := make([]wire.ResultItem, 0, len(objs))
		for _, o := range objs {
			if item, ok := parseObjectItem(plugin, o); ok {
				out = append(out, item)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	// Array of strings.
	var strs []string
	if json.Unmarshal(raw, &strs) == nil && len(strs) > 0 {
		out := make([]wire.ResultItem, 0, len(strs))
		for _, s := range strs {
			out = append(out, wire.ResultItem{ID: s, Label: s, Plugin: plugin})
		}
		return out
	}
	return nil
}

func parseObjectItem(plugin string, obj map[string]json.RawMessage) (wire.ResultItem, bool) {
	id := readStringField(obj, "id")
	label := readStringField(obj, "label")
	if label == "" {
		label = readStringField(obj, "title")
	}
	if label == "" {
		label = readStringField(obj, "text")
	}
	if id == "" && label == "" {
		return wire.ResultItem{}, false
	}
	if id == "" {
		id = label
	}
	score := readFloatField(obj, "score")
	actions := readActionsField(obj, "actions")
	description := readStringField(obj, "description")
	icon := readStringField(obj, "icon")
	category := readStringField(obj, "category")
	previewPath := readStringField(obj, "preview_path")
	return wire.ResultItem{
		ID:          id,
		Label:       label,
		Description: description,
		Icon:        icon,
		Category:    category,
		PreviewPath: previewPath,
		Plugin:      plugin,
		Score:       score,
		Actions:     actions,
	}, true
}

func readStringField(obj map[string]json.RawMessage, key string) string {
	raw, ok := obj[key]
	if !ok {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return ""
}

func readFloatField(obj map[string]json.RawMessage, key string) float64 {
	raw, ok := obj[key]
	if !ok {
		return 0
	}
	var f float64
	if json.Unmarshal(raw, &f) == nil {
		return f
	}
	return 0
}

func readActionsField(obj map[string]json.RawMessage, key string) []wire.Action {
	raw, ok := obj[key]
	if !ok {
		return nil
	}
	var actions []wire.Action
	if json.Unmarshal(raw, &actions) == nil {
		return actions
	}
	return nil
}

func orderResults(items []wire.ResultItem, mode string, frecencyScores map[string]float64, weight float64) []wire.ResultItem {
	if len(items) <= 1 {
		if len(items) == 1 {
			items[0] = applyFrecency(items[0], frecencyScores, weight)
		}
		return items
	}

	for i := range items {
		items[i] = applyFrecency(items[i], frecencyScores, weight)
	}

	if mode != "grouped" {
		sort.SliceStable(items, func(i, j int) bool {
			if items[i].Score == items[j].Score {
				return items[i].Plugin < items[j].Plugin
			}
			return items[i].Score > items[j].Score
		})
		return items
	}

	type pluginGroup struct {
		name  string
		best  float64
		items []wire.ResultItem
	}

	groupIndex := make(map[string]int)
	groups := make([]pluginGroup, 0)
	for _, item := range items {
		idx, ok := groupIndex[item.Plugin]
		if !ok {
			idx = len(groups)
			groupIndex[item.Plugin] = idx
			groups = append(groups, pluginGroup{
				name:  item.Plugin,
				best:  item.Score,
				items: []wire.ResultItem{item},
			})
			continue
		}
		if item.Score > groups[idx].best {
			groups[idx].best = item.Score
		}
		groups[idx].items = append(groups[idx].items, item)
	}

	sort.Slice(groups, func(i, j int) bool {
		if groups[i].best == groups[j].best {
			return groups[i].name < groups[j].name
		}
		return groups[i].best > groups[j].best
	})

	ordered := make([]wire.ResultItem, 0, len(items))
	for _, g := range groups {
		ordered = append(ordered, g.items...)
	}
	return ordered
}

func applyFrecency(item wire.ResultItem, frecencyScores map[string]float64, weight float64) wire.ResultItem {
	if weight < 0 {
		weight = 0
	}
	if weight > 1 {
		weight = 1
	}
	key := item.Plugin + ":" + item.ID
	f := frecencyScores[key]
	item.FrecencyScore = f
	item.Score = (1-weight)*item.Score + weight*f
	return item
}

func (s *aggregateStore) removeByOwner(uid string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.byOwner[uid]
	removed := len(set)
	for qid := range set {
		s.removeLocked(qid)
	}
	return removed
}
