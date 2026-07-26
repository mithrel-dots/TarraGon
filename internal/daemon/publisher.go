package daemon

import "github.com/mithrel-dots/tarragon/internal/wire"

// Keep query removal ordered with the enqueue: once detach has been
// acknowledged, a publisher finishing an old encode cannot queue its result.
func (r *uiRegistry) publishSnapshot(msg *wire.UpdateMessage, _ []string) {
	r.store.mu.RLock()
	defer r.store.mu.RUnlock()
	q := r.store.byID[msg.QueryID]
	if q == nil {
		return
	}
	r.mu.RLock()
	client := r.clients[q.session]
	r.mu.RUnlock()
	if client != nil {
		_ = client.send(msg)
	}
}
