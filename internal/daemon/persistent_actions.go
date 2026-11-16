package daemon

import (
	"errors"
	"io"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/mithrel-dots/tarragon/internal/wire"
)

const (
	maxPersistentActions = 32
	unansweredAction     = "Plugin still has an unanswered action. Wait for it or use @plugins → restart."
)

// Each hello gets a fresh scheduler. Only the router writes frames; the
// connection's reader and individual action timers can retire work concurrently.
type pluginSession struct {
	name   string
	conn   peerConn
	tagged bool

	mu       sync.Mutex
	closed   bool
	waiting  []*selectAction
	inFlight map[string]*selectAction // keyed by the ID actually sent ("" for legacy)

	unanswered bool      // the timed-out legacy invocation still owns the next final
	replying   bool      // legacy final is being settled by the reader
	reopened   time.Time // reject pre-recovery clicks delayed in the shared router queue
}

func (s *pluginSession) actionID(a *selectAction) string {
	if s.tagged {
		return a.id
	}
	return ""
}

func (s *pluginSession) enqueue(a *selectAction) {
	s.mu.Lock()
	var reject string
	switch {
	case s.closed:
		reject = "Plugin connection closed."
	case s.unanswered || a.received.Before(s.reopened):
		reject = unansweredAction
	case len(s.waiting) >= maxPersistentActions || (s.tagged && len(s.inFlight) >= maxPersistentActions):
		reject = "Plugin action queue is full."
	default:
		// onTerminal never runs the callback under the action lock, and
		// returns false if the timer already won while waiting for the router.
		if a.onTerminal(func() { s.ended(a) }) {
			s.waiting = append(s.waiting, a)
		}
	}
	s.mu.Unlock()
	if reject != "" {
		a.fail(reject)
	}
}

// The router leaves an entry in waiting during encoding. That way a timeout
// or disconnect can still remove it as definitely undispatched.
func (s *pluginSession) next() *selectAction {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.waiting) == 0 || (!s.tagged && (s.unanswered || s.inFlight[""] != nil)) {
		return nil
	}
	return s.waiting[0]
}

// beginDispatch only checks/marks under the action lock; it does not call a
// terminal callback. Register the match before the very first possible write.
func (s *pluginSession) begin(a *selectAction) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.waiting) == 0 || s.waiting[0] != a {
		return false
	}
	s.waiting = slices.Delete(s.waiting, 0, 1)
	if !a.beginDispatch() {
		return false
	}
	s.inFlight[s.actionID(a)] = a
	return true
}

func (s *pluginSession) ended(a *selectAction) {
	s.mu.Lock()
	id := s.actionID(a)
	var rejected []*selectAction
	if s.inFlight[id] == a {
		if !s.tagged && s.replying {
			// The reader saw a final. It will use finish's return value to
			// distinguish an accepted reply from a late one.
			s.mu.Unlock()
			return
		}
		delete(s.inFlight, id)
		if !s.tagged {
			s.unanswered = true
			rejected, s.waiting = s.waiting, nil
		}
	} else if i := slices.Index(s.waiting, a); i >= 0 {
		s.waiting = slices.Delete(s.waiting, i, i+1)
	}
	s.mu.Unlock()
	for _, queued := range rejected {
		queued.fail(unansweredAction)
	}
}

// final returns true when the router needs to write a waiting legacy select.
func (s *pluginSession) final(resp wire.SelectResponse) bool {
	s.mu.Lock()
	a := s.inFlight[resp.ActionID]
	if a == nil {
		if s.unanswered && resp.ActionID == "" {
			s.unanswered = false // drain exactly one late legacy final
			s.reopened = time.Now()
		}
		s.mu.Unlock()
		log.Printf("[PLUGINS] dropping unmatched select_response from %s (action_id=%q)", s.name, resp.ActionID)
		return false
	}
	if !s.tagged {
		s.replying = true
	}
	s.mu.Unlock()

	won := a.finish(resp.Success, resp.Message, resp.Next)
	if !won {
		log.Printf("[PLUGINS] dropping late select_response from %s (action_id=%q)", s.name, resp.ActionID)
	}
	if s.tagged {
		return false
	}
	s.mu.Lock()
	delete(s.inFlight, "")
	s.replying = false
	var rejected []*selectAction
	if !won {
		// Timeout won, but its cleanup raced this final. The final drains
		// the slot; anything previously waiting must still be rejected.
		rejected, s.waiting = s.waiting, nil
		s.reopened = time.Now()
	}
	ready := len(s.waiting) != 0
	s.mu.Unlock()
	for _, queued := range rejected {
		queued.fail(unansweredAction)
	}
	return ready
}

func (s *pluginSession) progress(resp wire.ActionProgress) {
	s.mu.Lock()
	a := s.inFlight[resp.ActionID]
	s.mu.Unlock()
	if !s.tagged || a == nil || !a.progress(resp.Message) {
		log.Printf("[PLUGINS] dropping unmatched action_progress from %s (action_id=%q)", s.name, resp.ActionID)
	}
}

// Detach every match before the registry makes a replacement visible. Leave
// publishing outcomes and closing the transport until after releasing its lock.
func (s *pluginSession) retire() []*selectAction {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	actions := s.waiting
	s.waiting = nil
	for _, a := range s.inFlight {
		actions = append(actions, a)
	}
	clear(s.inFlight)
	return actions
}

func (s *pluginSession) close(actions []*selectAction) {
	for _, a := range actions {
		a.fail("Plugin connection closed.")
	}
	_ = s.conn.Close()
}

// wire.WriteMsg validates the complete envelope before calling Write. A local
// encoding/size error or an entry expired in the queue is never a dispatch.
type selectFrameWriter struct {
	session *pluginSession
	action  *selectAction
}

var errSelectExpired = errors.New("select no longer pending")

func (w selectFrameWriter) Write(frame []byte) (int, error) {
	if !w.session.begin(w.action) {
		return 0, errSelectExpired
	}
	n, err := w.session.conn.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if n == 0 {
		// The transport has now established non-delivery. A deadline that
		// expired while Write was blocked must keep its conservative outcome.
		a := w.action
		a.mu.Lock()
		if a.pendingLocked() {
			a.dispatched = false
		}
		a.mu.Unlock()
	}
	return n, err
}

func (s *pluginSession) writeReady() error {
	for a := s.next(); a != nil; a = s.next() {
		if err := s.conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			return err
		}
		qid, row := a.handlerRow()
		err := wire.WriteMsg(selectFrameWriter{s, a}, wire.SelectRequest{
			Type: wire.MsgSelect, ActionID: s.actionID(a), QueryID: qid,
			ResultID: row, Action: a.action, Context: a.pluginContext,
		})
		_ = s.conn.SetWriteDeadline(time.Time{})
		switch {
		case errors.Is(err, wire.ErrMessageTooLarge):
			a.fail(err.Error())
		case errors.Is(err, errSelectExpired):
			continue
		case err != nil:
			return err
		}
	}
	return nil
}
