package daemon

import "github.com/mithrel-dots/tarragon/internal/wire"

const maxUnansweredRemovedQueries = 1024

// Snapshot destinations at removal: a refresh or a plugin restart must never
// redirect an end. The bounded router serializes it after the original write.
type queryEnd struct {
	requests chan<- pluginRequest
	shutdown <-chan struct{}
	req      pluginRequest
}

type queryEnds []queryEnd

func (q *queryState) endsLocked() queryEnds {
	var ends queryEnds
	for _, d := range q.producers {
		pending := !d.started.IsZero()
		if d.updatesFrom == nil || !(pending || d.live) {
			continue
		}
		if !d.live && !d.endCapable {
			session := d.registry.session(d.name)
			if session != nil && session.conn == d.updatesFrom {
				session.rememberRemovedQuery(q.QueryID)
			}
			continue
		}
		ends = append(ends, queryEnd{
			requests: d.requests, shutdown: d.daemonCtx.Done(),
			req: pluginRequest{name: d.name, conn: d.updatesFrom, queryID: q.QueryID, end: true},
		})
	}
	return ends
}

func (s *pluginSession) removedQueryLimitReached() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.removedQueries) >= maxUnansweredRemovedQueries
}

// Only already-written queries can add entries above the admission limit.
func (s *pluginSession) rememberRemovedQuery(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if s.removedQueries == nil {
		s.removedQueries = make(map[string]struct{})
	}
	s.removedQueries[id] = struct{}{}
}

func (s *pluginSession) lateLiveResponse(resp wire.PluginResponse) bool {
	if resp.Type != wire.MsgResponse {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, pending := s.removedQueries[resp.QueryID]
	delete(s.removedQueries, resp.QueryID)
	return pending && resp.Live
}

// Never wait for room on the plugin writer under a store or query lock.
func (ends queryEnds) send() {
	for _, end := range ends {
		end.send()
	}
}

func (end queryEnd) send() {
	select {
	case end.requests <- end.req:
	case <-end.shutdown:
	}
}

func (s *aggregateStore) endQuery(owner, id string) {
	var ends queryEnds
	s.mu.Lock()
	if q := s.byID[id]; q != nil && q.session == owner {
		ends = s.removeLocked(id)
	}
	s.mu.Unlock()
	ends.send()
}
