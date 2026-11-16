package daemon

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
		pending := !d.started.IsZero() && d.endCapable
		if d.updatesFrom == nil || !(pending || d.live) {
			continue
		}
		ends = append(ends, queryEnd{
			requests: d.requests, shutdown: d.daemonCtx.Done(),
			req: pluginRequest{name: d.name, conn: d.updatesFrom, queryID: q.QueryID, end: true},
		})
	}
	return ends
}

// Never wait for room on the plugin writer under a store or query lock.
func (ends queryEnds) send() {
	for _, end := range ends {
		select {
		case end.requests <- end.req:
		case <-end.shutdown:
		}
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
