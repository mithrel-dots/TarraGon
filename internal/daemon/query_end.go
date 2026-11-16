package daemon

import "github.com/mithrel-dots/tarragon/internal/wire"

const maxUnansweredRemovedQueries = 1024

// Snapshot destinations at removal: a refresh or a plugin restart must never
// redirect an end. The bounded router serializes it after the original write.
type queryEnd struct {
	requests chan<- pluginRequest
	shutdown <-chan struct{}
	req      pluginRequest
	retire   *pluginRegistry // too many unanswered removed queries on this connection
}

type queryEnds []queryEnd

func (q *queryState) endsLocked() queryEnds {
	var ends queryEnds
	for _, d := range q.producers {
		pending := !d.started.IsZero()
		if d.updatesFrom == nil || !(pending || d.live) {
			continue
		}
		end := queryEnd{req: pluginRequest{name: d.name, conn: d.updatesFrom, queryID: q.QueryID, end: true}}
		if !d.live && !d.endCapable {
			session := d.registry.session(d.name)
			if session == nil || session.conn != d.updatesFrom || !session.rememberRemovedQuery(q.QueryID) {
				continue
			}
			end.retire = d.registry
		} else {
			end.requests, end.shutdown = d.requests, d.daemonCtx.Done()
		}
		ends = append(ends, end)
	}
	return ends
}

// Return true on overflow: forgetting an unanswered ID could strand a live feed.
func (s *pluginSession) rememberRemovedQuery(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.removedQueriesFull {
		return false
	}
	if len(s.removedQueries) == maxUnansweredRemovedQueries {
		s.removedQueriesFull = true
		return true
	}
	if s.removedQueries == nil {
		s.removedQueries = make(map[string]struct{})
	}
	s.removedQueries[id] = struct{}{}
	return false
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
	if end.retire != nil {
		end.retire.remove(end.req.name, end.req.conn)
		return
	}
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
