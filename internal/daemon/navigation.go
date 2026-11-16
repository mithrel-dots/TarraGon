package daemon

import (
	"slices"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

const navigationFeature = "navigation"

func validDestination(d *wire.Destination) bool {
	return d != nil && d.Plugin != "" && validPluginContext(d.Context)
}

// Prefix queries trim their text and can collide. Only offer the replacement
// if the actual resolver would dispatch the same literal text to the same ID.
func replacementQuery(mgr *plugins.Manager, d *wire.Destination) (string, bool) {
	if mgr == nil || d.Context != "" {
		return "", false
	}
	mgr.RLock()
	defer mgr.RUnlock()
	p := mgr.Plugins[d.Plugin]
	if p == nil || !p.Config.Enabled || p.Config.ResolvedPrefix == "" {
		return "", false
	}
	query := p.Config.ResolvedPrefix + " " + d.Text
	id, text, ok := resolvePrefixTarget(query, mgr)
	return query, ok && id == d.Plugin && text == d.Text
}

// Project newly decoded rows for this owner before caching or publishing them.
// The original plugin JSON remains in aggResult.Data.
func (r *aggResult) projectRow(row *wire.ResultItem, features []string, mgr *plugins.Manager) {
	if len(row.Actions) == 0 {
		return
	}
	actions := row.Actions[:0]
	lostDefault := false
	for _, action := range row.Actions {
		allowed := projectAction(&action, features, mgr)
		if action.Name != "" && (!allowed || isNavigation(action)) {
			r.blockSelect(row.ID, action.Name)
		}
		if allowed {
			actions = append(actions, action)
		} else if action.Default {
			lostDefault = true
		}
	}
	first := slices.IndexFunc(actions, func(a wire.Action) bool { return a.Default })
	if first < 0 && len(actions) > 0 {
		first = 0 // a baseline UI implicitly activates the first action
		actions[0].Default = lostDefault
	}
	if first < 0 || isNavigation(actions[first]) {
		r.blockSelect(row.ID, "")
	}
	clear(row.Actions[len(actions):])
	row.Actions = actions
}

func (r *aggResult) blockSelect(row, name string) {
	if r.blockedSelects == nil {
		r.blockedSelects = make(map[string][]string)
	}
	r.blockedSelects[row] = append(r.blockedSelects[row], name)
}

func isNavigation(a wire.Action) bool {
	return a.Type == wire.ActionTypeOpen || a.Type == wire.ActionTypeQueryReplace
}

func projectAction(action *wire.Action, features []string, mgr *plugins.Manager) bool {
	for _, feature := range action.Requires {
		if !slices.Contains(features, feature) {
			return false
		}
	}
	if action.Type != wire.ActionTypeOpen {
		return true
	}
	if !validDestination(action.Destination) {
		return false
	}
	if slices.Contains(features, navigationFeature) {
		return true
	}
	query, ok := replacementQuery(mgr, action.Destination)
	if ok {
		action.Type, action.Query = wire.ActionTypeQueryReplace, query
		action.Destination, action.Title, action.Placeholder = nil, "", ""
	}
	return ok
}
