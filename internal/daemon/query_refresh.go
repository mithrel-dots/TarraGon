package daemon

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

type resolvedQuery struct {
	q             *queryState
	text          string
	pluginContext string
	daemonCtx     context.Context
	mgr           *plugins.Manager
	registry      *pluginRegistry
	requests      chan<- pluginRequest
	store         *aggregateStore
	ui            *uiRegistry
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
	live        bool           // updatesFrom opted in for this query
	renewal     *time.Timer    // watch hold after a final push during a refresh
	reason      dispatchReason // authorization to recheck when a queued write begins
	started     time.Time
	endCapable  bool // manifest promise at the start of this request's write
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

func (q *queryState) sourceRowLocked(producer, resultID string) *wire.ResultItem {
	items := q.Results[producer].items
	for i := range items {
		if items[i].ID == resultID {
			return &items[i]
		}
	}
	return nil
}

func (s *aggregateStore) offeredRow(owner, queryID, producer, resultID string) bool {
	q := s.query(queryID)
	if q == nil || q.session != owner {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.ctx.Err() == nil && q.sourceRowLocked(producer, resultID) != nil
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
	if d == nil || q.sourceRowLocked(a.producer, a.resultID) == nil || !d.eligible(reason) {
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
	q := d.q
	q.mu.Lock()
	held := d.renewal != nil
	reason := d.completeLocked()
	demote := held && !q.publishing && q.watchFinishedLocked()
	q.mu.Unlock()
	if demote {
		// The writer may be the caller; eviction can enqueue an end on it.
		go d.store.demoteWatch(q)
	}
	d.runFollowup(reason)
}

// Settle the slot and watch hold before publishing its answer. A queued
// follow-up can still renew the watch within the same deadline.
func (d *producerDispatch) completeLocked() dispatchReason {
	reason := d.again
	d.again = initialDispatch
	d.outstanding = reason != initialDispatch && d.q.ctx.Err() == nil
	if d.live || !d.outstanding {
		d.stopWatchRenewalLocked()
	}
	if !d.outstanding {
		return initialDispatch
	}
	return reason
}

func (d *producerDispatch) runFollowup(reason dispatchReason) {
	if reason != initialDispatch {
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
	d.q.mu.Lock()
	reason := d.completeLocked()
	d.store.submitErrorLocked(d.q, d.name, elapsedMillis(start), message, d.ui.publishSnapshot)
	d.runFollowup(reason)
}

func (d *producerDispatch) result(start time.Time, data json.RawMessage, err error) {
	if err != nil {
		d.fail(start, err.Error())
		return
	}
	result, errMsg := parseResult(d.name, data, elapsedMillis(start))
	d.q.mu.Lock()
	reason := d.completeLocked()
	d.store.replaceResultLocked(d.q, d.name, result, errMsg, d.ui.publishSnapshot)
	d.runFollowup(reason)
}

// Only a successfully encoded request can take ownership of replies and end.
func (s *aggregateStore) beginQueryWrite(req pluginRequest, encodingErr error) bool {
	q := s.query(req.queryID)
	if q == nil {
		return false
	}
	q.mu.Lock()
	d := q.producers[req.name]
	if d == nil || d.conn != req.conn || !d.started.IsZero() || q.ctx.Err() != nil {
		q.mu.Unlock()
		return false
	}
	conn, _ := d.registry.getConn(req.name)
	if conn != req.conn {
		q.mu.Unlock()
		return false
	}
	d.mgr.RLock()
	p := d.eligiblePluginLocked(d.reason)
	if p == nil || encodingErr != nil {
		// Config may have changed while waiting for hello or the writer.
		d.mgr.RUnlock()
		d.conn = nil
		if p == nil && d.reason != initialDispatch {
			q.mu.Unlock()
			d.complete() // Revoked refresh: release its hold without replacing prior rows.
			return false
		}
		message := "plugin disabled"
		if p != nil {
			message = encodingErr.Error()
		}
		reason := d.completeLocked()
		s.submitErrorLocked(q, req.name, 0, message, d.ui.publishSnapshot)
		d.runFollowup(reason)
		return false
	}
	d.started = time.Now()
	if d.updatesFrom != conn {
		d.live = false
	}
	d.updatesFrom = conn
	d.endCapable = slices.Contains(p.Config.Capabilities, wire.MsgEnd)
	d.mgr.RUnlock()
	q.mu.Unlock()
	return true
}

// Report removal so the original connection's reader can check for an unanswered request.
func (s *aggregateStore) receiveQuery(name string, conn peerConn, resp wire.PluginResponse) bool {
	q := s.query(resp.QueryID)
	if q == nil {
		return true
	}
	q.mu.Lock()
	if q.ctx.Err() != nil {
		q.mu.Unlock()
		return s.receiveCanceledQuery(q, name, conn, resp)
	}
	d := q.producers[name]
	push := resp.Type == wire.MsgLiveUpdate
	if d == nil || d.updatesFrom != conn || push && !d.live {
		q.mu.Unlock()
		return false
	}
	start := d.started
	if push {
		start = time.Time{} // a push never completes a request, even if its write already started
	} else if start.IsZero() && (d.live || resp.Live) {
		q.mu.Unlock()
		return false // only a solicited response can start or renew a live subscription
	}
	q.mu.Unlock()
	result, errMsg := parseResult(name, resp.Data, elapsedMillis(start))

	q.mu.Lock()
	if q.ctx.Err() != nil {
		q.mu.Unlock()
		return s.receiveCanceledQuery(q, name, conn, resp)
	}
	if current, _ := d.registry.getConn(name); current != conn || d.updatesFrom != conn || push && !d.live {
		q.mu.Unlock()
		return false // the producer was retired while decoding
	}
	reason := initialDispatch
	if !start.IsZero() {
		d.conn, d.started = nil, time.Time{}
		d.live = resp.Live
		reason = d.completeLocked()
	} else if push && resp.Final {
		d.live = false
		if q.watch != nil && d.outstanding {
			d.holdWatchRenewalLocked()
		}
	}
	s.replaceResultLocked(q, name, result, errMsg, d.ui.publishSnapshot)
	d.runFollowup(reason)
	return false
}

// A terminal watch cancels before its final snapshot is published and removed.
// Resolve the reply or defer to the removal record, under the same lock order as removal.
func (s *aggregateStore) receiveCanceledQuery(q *queryState, name string, conn peerConn, resp wire.PluginResponse) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.byID[q.QueryID] != q {
		return true
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	d := q.producers[name]
	if resp.Type == wire.MsgResponse && d != nil && d.updatesFrom == conn && !d.started.IsZero() {
		d.live = d.live || resp.Live
		if !d.endCapable {
			d.conn, d.started = nil, time.Time{}
		}
	}
	return false
}

func (s *aggregateStore) retireQueries(name string, conn peerConn) {
	s.mu.RLock()
	queries := slices.Collect(maps.Values(s.byID))
	s.mu.RUnlock()
	for _, q := range queries {
		q.mu.Lock()
		d := q.producers[name]
		if d == nil {
			q.mu.Unlock()
			continue
		}
		lostLive := d.updatesFrom == conn && d.live
		if d.updatesFrom == conn {
			d.updatesFrom, d.live = nil, false
		}
		lostRequest := d.conn == conn
		start := d.started
		if lostRequest {
			d.conn, d.started = nil, time.Time{}
		}
		if !lostLive && !lostRequest {
			q.mu.Unlock()
			continue
		}
		reason := initialDispatch
		if lostRequest {
			reason = d.completeLocked()
		}
		s.submitErrorLocked(q, name, elapsedMillis(start), "plugin disconnected", d.ui.publishSnapshot)
		d.runFollowup(reason)
	}
}
