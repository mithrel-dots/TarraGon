package daemon

import (
	"encoding/json"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/mithrel-dots/tarragon/internal/wire"
)

type watchUsage struct {
	count int
	bytes int64
}

type watchBudget struct {
	mu               sync.Mutex // different queries can replace rows concurrently
	perOwner, global watchUsage
	timeout          time.Duration
	owners           map[string]watchUsage
	total            watchUsage
}

func defaultWatchBudget() watchBudget {
	return watchBudget{
		perOwner: watchUsage{4, 4 << 20}, global: watchUsage{32, 32 << 20},
		timeout: 5 * time.Second, owners: make(map[string]watchUsage),
	}
}

func (b *watchBudget) change(owner string, count int, bytes int64) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	u := b.owners[owner]
	u.count += count
	u.bytes += bytes
	total := watchUsage{b.total.count + count, b.total.bytes + bytes}
	switch {
	case u.count > b.perOwner.count:
		return "owner watch count limit reached"
	case total.count > b.global.count:
		return "global watch count limit reached"
	case u.bytes > b.perOwner.bytes:
		return "owner watch byte limit reached"
	case total.bytes > b.global.bytes:
		return "global watch byte limit reached"
	}
	if u.count == 0 {
		delete(b.owners, owner)
	} else {
		b.owners[owner] = u
	}
	b.total = total
	return ""
}

// Guarded by q.mu. Only ordinary queries belong to s.order.
type watchReservation struct {
	bytes   int64
	timer   *time.Timer
	publish SnapshotPublisher
}

func (s *aggregateStore) retainQueryLocked(q *queryState, watch bool) (string, queryEnds) {
	if !watch {
		return "", s.retainOrdinaryLocked(q)
	}
	size := stringBytes(q.QueryID, q.session, q.Input, q.Client, q.stream, q.target, q.pluginContext) + stringSliceBytes(q.uiFeatures)
	if reason := s.watches.change(q.session, 1, size); reason != "" {
		return reason, nil
	}
	q.watch = &watchReservation{bytes: size}
	return "", nil
}

func (s *aggregateStore) retainOrdinaryLocked(q *queryState) queryEnds {
	var ends queryEnds
	if s.limit > 0 && len(s.order) >= s.limit {
		ends = s.removeLocked(s.order[0])
	}
	s.order = append(s.order, q.QueryID)
	return ends
}

// Both locks are held. Stop the timer and return all charges in one operation.
func (s *aggregateStore) forgetRetentionLocked(q *queryState) {
	if q.watch == nil {
		i := slices.Index(s.order, q.QueryID)
		s.order = slices.Delete(s.order, i, i+1)
		return
	}
	q.stopWatchTimerLocked()
	for _, d := range q.producers {
		d.stopWatchRenewalLocked()
	}
	s.watches.change(q.session, -1, -q.watch.bytes)
	q.watch = nil
}

func watchRejectedUpdate(qid, reason string) *wire.UpdateMessage {
	// Repeating a refused input could itself exceed the base64 envelope limit.
	snap, _ := json.Marshal(aggregate{QueryID: qid, StartedAtUnixMs: time.Now().UnixMilli(), Results: map[string]aggResult{}, WatchError: reason})
	return &wire.UpdateMessage{Type: "update", QueryID: qid, Payload: snap}
}

func (s *aggregateStore) armWatch(qid string, publish SnapshotPublisher) {
	q := s.query(qid)
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.watch == nil {
		return
	}
	q.watch.publish = publish
	q.watch.timer = time.AfterFunc(s.watches.timeout, func() { s.expireInitialWatch(q) })
}

func (s *aggregateStore) expireInitialWatch(q *queryState) {
	q.mu.Lock()
	if q.watch == nil || q.watch.timer == nil || q.ctx.Err() != nil {
		q.mu.Unlock()
		return
	}
	if q.hasLiveProducerLocked() {
		// A silent sibling cannot expire a confirmed live feed, or keep
		// the reservation after that feed eventually stops.
		q.stopWatchTimerLocked()
		q.mu.Unlock()
		return
	}
	s.stopWatchLocked(q, "watch timed out waiting for initial responses", q.watch.publish)
}

func (d *producerDispatch) holdWatchRenewalLocked() {
	if d.renewal != nil {
		return
	}
	q := d.q
	// Once a live feed has ended, only its refresh can extend this hold.
	q.stopWatchTimerLocked()
	var timer *time.Timer
	timer = time.AfterFunc(d.store.watches.timeout, func() {
		q.mu.Lock()
		if d.renewal == timer && q.ctx.Err() == nil {
			d.renewal = nil
			if q.watchFinishedLocked() {
				d.store.stopWatchLocked(q, "watch timed out waiting for a refresh response", q.watch.publish)
				return
			}
		}
		q.mu.Unlock()
	})
	d.renewal = timer
}

func (d *producerDispatch) stopWatchRenewalLocked() {
	if d.renewal != nil {
		d.renewal.Stop()
		d.renewal = nil
	}
}

func (q *queryState) stopWatchTimerLocked() {
	if q.watch.timer != nil {
		q.watch.timer.Stop()
		q.watch.timer = nil
	}
}

func (q *queryState) checkInitialWatchLocked() {
	for _, p := range q.Plugins {
		if p.State == "pending" {
			return
		}
	}
	q.stopWatchTimerLocked()
}

// Freeze the last valid rows; the publisher sends the reason before removing
// the query, through the same writer and in order with already queued updates.
func (s *aggregateStore) stopWatchLocked(q *queryState, reason string, publish SnapshotPublisher) {
	q.WatchError = reason
	q.cancel()
	s.scheduleLocked(q, "", publish)
}

func (q *queryState) watchFinishedLocked() bool {
	if q.watch == nil || q.WatchError != "" || q.watch.timer != nil {
		return false
	}
	for _, d := range q.producers {
		if d.live || d.renewal != nil {
			return false
		}
	}
	return true
}

func (q *queryState) hasLiveProducerLocked() bool {
	for _, d := range q.producers {
		if d.live {
			return true
		}
	}
	return false
}

func (s *aggregateStore) demoteWatch(q *queryState) {
	var ends queryEnds
	s.mu.Lock()
	if s.byID[q.QueryID] == q {
		q.mu.Lock()
		if q.watchFinishedLocked() {
			s.forgetRetentionLocked(q)
			ends = s.retainOrdinaryLocked(q)
		}
		q.mu.Unlock()
	}
	s.mu.Unlock()
	ends.send()
}

func stringBytes(ss ...string) int64 {
	var size int64
	for _, v := range ss {
		size += int64(len(v))
	}
	return size
}

func stringSliceBytes(ss []string) int64 {
	return int64(cap(ss))*int64(unsafe.Sizeof("")) + stringBytes(ss...)
}

// Charge backing array capacity, including unused slots, plus the strings
// and nested values held by the projected rows. Shared strings are charged
// at each reference, so aliasing cannot make the quota underestimate them.
func resultWatchBytes(r aggResult) int64 {
	size := int64(cap(r.Data)) + int64(cap(r.items))*int64(unsafe.Sizeof(wire.ResultItem{}))
	for id, names := range r.blockedSelects {
		size += int64(len(id)) + int64(unsafe.Sizeof(id)+unsafe.Sizeof(names)) + stringSliceBytes(names)
	}
	for _, row := range r.items {
		size += stringBytes(row.ID, row.Plugin, row.Label, row.Description, row.Icon, row.Category, row.PreviewPath)
		if ref := row.Ref; ref != nil {
			size += int64(unsafe.Sizeof(*ref)) + stringBytes(ref.Type, ref.Value, ref.MIME)
		}
		size += int64(cap(row.Actions)) * int64(unsafe.Sizeof(wire.Action{}))
		for _, a := range row.Actions {
			size += stringBytes(a.Name, a.Type, a.Query, a.Description, a.Title, a.Placeholder, a.OfferID) + stringSliceBytes(a.Requires)
			if d := a.Destination; d != nil {
				size += int64(unsafe.Sizeof(*d)) + stringBytes(d.Plugin, d.Context, d.Text)
			}
		}
	}
	return size
}

func (s *aggregateStore) chargeWatchLocked(q *queryState, delta int64, publish SnapshotPublisher) bool {
	if reason := s.watches.change(q.session, 0, delta); reason != "" {
		s.stopWatchLocked(q, reason, publish)
		return false
	}
	q.watch.bytes += delta
	return true
}
