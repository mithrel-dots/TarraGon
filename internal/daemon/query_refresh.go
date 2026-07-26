package daemon

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

type resolvedQuery struct {
	q         *queryState
	text      string
	daemonCtx context.Context
	mgr       *plugins.Manager
	registry  *pluginRegistry
	requests  chan<- pluginRequest
	store     *aggregateStore
	ui        *uiRegistry
}

type dispatchReason uint8

const (
	initialDispatch dispatchReason = iota
	consumerRefresh                // Requires the producer's current replay promise.
	producerRefresh                // The producer explicitly requested its own replay.
)

type producerDispatch struct {
	*resolvedQuery
	name string

	// Guarded by q.mu. While outstanding, coalesce refresh requests and
	// remember whether at least one was authorized by the producer itself.
	outstanding bool
	again       dispatchReason
	conn        peerConn
	updatesFrom peerConn       // connection that may replace its last result without a new request
	reason      dispatchReason // authorization to recheck when a queued write begins
	started     time.Time
}

// Caller holds the manager read lock. Only on-call dispatch needs to
// materialize a Plugin value for process execution.
func (d *producerDispatch) eligiblePluginLocked(reason dispatchReason) *plugins.Plugin {
	p := d.mgr.Plugins[d.name]
	if p == nil || !p.Config.Enabled || reason == consumerRefresh && !slices.Contains(p.Config.Capabilities, "replay") {
		return nil
	}
	return p
}

func (d *producerDispatch) eligible(reason dispatchReason) bool {
	d.mgr.RLock()
	defer d.mgr.RUnlock()
	return d.eligiblePluginLocked(reason) != nil
}

func (q *queryState) hasRowLocked(producer, resultID string) bool {
	return slices.ContainsFunc(q.Results[producer].items, func(item wire.ResultItem) bool {
		return item.ID == resultID
	})
}

func (s *aggregateStore) offeredRow(owner, queryID, producer, resultID string) bool {
	q := s.query(queryID)
	if q == nil || q.session != owner {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.ctx.Err() == nil && q.hasRowLocked(producer, resultID)
}

// Called only by the terminal winner, after the owner has queued its answer.
func (s *aggregateStore) refreshAction(a *selectAction) {
	q := s.query(a.queryID)
	if !a.offered || q == nil || q.session != a.owner || q.ctx.Err() != nil {
		return
	}
	reason := consumerRefresh
	if a.handler == a.producer {
		reason = producerRefresh
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	d := q.producers[a.producer]
	if d == nil || !q.hasRowLocked(a.producer, a.resultID) || !d.eligible(reason) {
		return
	}
	if d.outstanding {
		d.again = max(d.again, reason)
		return
	}
	d.outstanding = true
	go d.run(reason)
}

// Only a definite end to this request releases the dispatch slot. In
// particular, a quiet but connected legacy plugin may still answer later.
func (d *producerDispatch) complete() {
	d.q.mu.Lock()
	defer d.q.mu.Unlock()
	reason := d.again
	d.again = initialDispatch
	d.outstanding = reason != initialDispatch && d.q.ctx.Err() == nil
	if d.outstanding {
		go d.run(reason)
	}
}

func elapsedMillis(start time.Time) float64 {
	if start.IsZero() {
		return 0
	}
	return float64(time.Since(start).Microseconds()) / 1000
}

func (d *producerDispatch) fail(start time.Time, message string) {
	d.store.submitError(d.q.QueryID, d.name, elapsedMillis(start), message, d.ui.publishSnapshot)
	d.complete()
}

func (d *producerDispatch) result(start time.Time, data json.RawMessage, err error) {
	if err != nil {
		d.fail(start, err.Error())
		return
	}
	d.store.SubmitResponse(d.name, wire.PluginResponse{QueryID: d.q.QueryID, Data: data}, elapsedMillis(start), d.ui.publishSnapshot)
	d.complete()
}

func (s *aggregateStore) beginQueryWrite(req pluginRequest, conn peerConn) bool {
	q := s.query(req.queryID)
	if q == nil || req.conn != conn {
		return false
	}
	q.mu.Lock()
	d := q.producers[req.name]
	if d == nil || d.conn != conn || !d.started.IsZero() || q.ctx.Err() != nil {
		q.mu.Unlock()
		return false
	}
	if d.reason != initialDispatch && !d.eligible(d.reason) {
		// Config may have changed while waiting for hello or the writer.
		d.conn = nil
		q.mu.Unlock()
		d.complete()
		return false
	}
	d.started = time.Now()
	d.updatesFrom = conn
	q.mu.Unlock()
	return true
}

func (s *aggregateStore) receiveQuery(name string, conn peerConn, resp wire.PluginResponse) {
	q := s.query(resp.QueryID)
	if q == nil {
		return
	}
	q.mu.Lock()
	d := q.producers[name]
	if d == nil || d.updatesFrom != conn {
		q.mu.Unlock()
		return
	}
	start := d.started // zero for an incremental update, even if a refresh is queued
	q.mu.Unlock()
	result, errMsg := parseResult(name, resp.Data, elapsedMillis(start))

	q.mu.Lock()
	if current, _ := d.registry.getConn(name); current != conn || d.updatesFrom != conn || q.ctx.Err() != nil {
		q.mu.Unlock()
		return // the producer or query was retired while decoding
	}
	if !start.IsZero() {
		d.conn, d.started = nil, time.Time{}
	}
	s.replaceResultLocked(q, name, result, errMsg, d.ui.publishSnapshot)
	if !start.IsZero() {
		d.complete()
	}
}

func (s *aggregateStore) retireQueries(name string, conn peerConn) {
	type retired struct {
		dispatch *producerDispatch
		started  time.Time
	}
	var retiredQueries []retired
	s.mu.RLock()
	for _, q := range s.byID {
		q.mu.Lock()
		if d := q.producers[name]; d != nil {
			if d.updatesFrom == conn {
				d.updatesFrom = nil
			}
			if d.conn == conn {
				retiredQueries = append(retiredQueries, retired{d, d.started})
				d.conn, d.started = nil, time.Time{}
			}
		}
		q.mu.Unlock()
	}
	s.mu.RUnlock()
	for _, r := range retiredQueries {
		r.dispatch.fail(r.started, "plugin disconnected")
	}
}
