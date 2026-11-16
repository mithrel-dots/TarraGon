package plugins

import (
	"mime"
	"slices"
	"strings"
	"unicode"
)

// Handler declares a select that can be invoked with a reference value alone.
type Handler struct {
	Plugin, Action, Label, Accepts, MIME string
}

func ReferenceKind(kind string) bool {
	return kind == "file" || kind == "url" || kind == "text"
}

// MediaType accepts a bare media type, optionally with a wildcard subtype in a manifest.
func MediaType(value string, wildcard bool) (string, bool) {
	if value == "" {
		return "", true
	}
	mt, params, err := mime.ParseMediaType(value)
	major, minor, ok := strings.Cut(mt, "/")
	if err != nil || len(params) != 0 || !strings.EqualFold(mt, value) || !ok || major == "" || minor == "" || strings.Contains(major, "*") {
		return "", false
	}
	return mt, !strings.Contains(minor, "*") || wildcard && minor == "*"
}

// Decode entries independently: one ill-typed optional rule must not hide the
// plugin or its other handlers.
func parseHandlers(id string, declarations any) []Handler {
	var handlers []Handler
	entries, _ := declarations.([]any)
	for _, entry := range entries {
		table, _ := entry.(map[string]any)
		action, _ := table["action"].(string)
		label, _ := table["label"].(string)
		kind, _ := table["accepts"].(string)
		media, isString := table["mime"].(string)
		media, valid := MediaType(media, true)
		if strings.TrimSpace(action) == "" || strings.TrimSpace(label) == "" ||
			strings.ContainsFunc(action+label, unicode.IsControl) || !ReferenceKind(kind) ||
			!valid || (table["mime"] != nil && !isString) {
			continue
		}
		handlers = append(handlers, Handler{id, action, label, kind, media})
	}
	return handlers
}

func (h Handler) Matches(kind, media string) bool {
	if h.Accepts != kind {
		return false
	}
	if h.MIME == "" {
		return true
	}
	if prefix, wildcard := strings.CutSuffix(h.MIME, "*"); wildcard {
		return strings.HasPrefix(strings.ToLower(media), prefix)
	}
	return strings.EqualFold(h.MIME, media)
}

func (m *Manager) indexHandlers() {
	var handlers []Handler
	for _, p := range m.Plugins {
		if p.Config.Enabled {
			handlers = append(handlers, p.Config.Handlers...)
		}
	}
	// Keep manifest order within a plugin.
	slices.SortStableFunc(handlers, func(a, b Handler) int { return strings.Compare(a.Plugin, b.Plugin) })
	m.handlers = handlers
}

// Handlers returns the immutable index of currently enabled manifest handlers.
func (m *Manager) Handlers() []Handler {
	m.RLock()
	defer m.RUnlock()
	return m.handlers
}
