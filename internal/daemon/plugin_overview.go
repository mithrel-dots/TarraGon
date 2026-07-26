package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mithrel-dots/tarragon/internal/config"
	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

func overviewDescription(p wire.PluginInfo) string {
	state := "disabled"
	if p.Enabled {
		switch {
		case p.Connected:
			state = "enabled, connected"
		case p.Lifecycle == string(plugins.LifecycleOnCall):
			state = "enabled, ready"
		case p.Lifecycle == string(plugins.LifecycleOnDemandPersistent):
			state = "enabled, on demand"
		default:
			state = "enabled, disconnected"
		}
	}
	parts := []string{p.ID, state, p.Lifecycle}
	for _, detail := range []string{p.Prefix, p.Source, p.Description} {
		if detail != "" {
			parts = append(parts, detail)
		}
	}
	return strings.Join(parts, " · ")
}

func canRestart(p wire.PluginInfo) bool {
	return p.Enabled && (p.Lifecycle == string(plugins.LifecycleDaemon) || p.Lifecycle == string(plugins.LifecycleOnDemandPersistent))
}

func overviewItems(infos []wire.PluginInfo, filter string) []wire.ResultItem {
	terms := strings.Fields(strings.ToLower(filter))
	items := make([]wire.ResultItem, 0, len(infos))
	claimed := make(map[string]bool)
nextPlugin:
	for _, p := range infos {
		// Status is sorted by ID, the same tie-breaker as query routing.
		canOpen := p.Enabled && p.Prefix != "" && !claimed[p.Prefix]
		if canOpen {
			claimed[p.Prefix] = true
		}
		desc := overviewDescription(p)
		text := strings.ToLower(p.Name + " " + desc)
		for _, term := range terms {
			if !strings.Contains(text, term) {
				continue nextPlugin
			}
		}
		actions := []wire.Action{{Name: "details", Type: wire.ActionTypeKeepOpen, Default: true}}
		if p.ID != plugins.OverviewID {
			if canOpen {
				actions[0].Default = false
				actions = append([]wire.Action{{Name: "open", Type: wire.ActionTypeQueryReplace, Query: p.Prefix + " ", Default: true}}, actions...)
			}
			if canRestart(p) {
				actions = append(actions, wire.Action{Name: "restart", Type: wire.ActionTypeKeepOpen})
			}
			action := "enable"
			if p.Enabled {
				action = "disable"
			}
			actions = append(actions, wire.Action{Name: action, Type: wire.ActionTypeKeepOpen})
		}
		items = append(items, wire.ResultItem{
			Plugin: plugins.OverviewID, ID: p.ID, Label: p.Name, Description: desc,
			Icon: p.Icon, Actions: actions,
		})
	}
	return items
}

func queryOverview(mgr *plugins.Manager, reg *pluginRegistry, filter string) (json.RawMessage, error) {
	items := overviewItems(pluginStatus(mgr, reg).Plugins, filter)
	return json.Marshal(items)
}

func (ui *uiRegistry) selectOverview(ctx context.Context, mgr *plugins.Manager, reg *pluginRegistry, a *selectAction) (string, *wire.ActionNext, error) {
	ui.management.Lock()
	defer ui.management.Unlock()
	id, action := a.resultID, a.action
	var target wire.PluginInfo
	var available bool
	for _, p := range pluginStatus(mgr, reg).Plugins {
		if p.ID == plugins.OverviewID {
			available = p.Enabled
		}
		if p.ID == id {
			target = p
		}
	}
	if !available {
		msg := "plugins overview is disabled"
		a.notRun(msg)
		return "", nil, errors.New(msg)
	}
	if target.ID == "" {
		msg := fmt.Sprintf("unknown plugin %q", id)
		a.notRun(msg)
		return "", nil, errors.New(msg)
	}
	if !a.beginDispatch() {
		return "", nil, errors.New("action expired")
	}
	if action == "" || action == "details" {
		return overviewDescription(target), nil, nil
	}
	if id == plugins.OverviewID {
		return "", nil, errors.New("the built-in overview cannot manage itself")
	}
	refresh := &wire.ActionNext{Type: "refresh"}
	switch action {
	case "enable", "disable":
		enabled := action == "enable"
		err := reloadPlugins(ctx, mgr, func() error {
			return config.WritePluginOverride(id, map[string]any{"enabled": enabled})
		})
		if err != nil {
			return "", refresh, err
		}
		mgr.RLock()
		effective := mgr.Plugins[id].Config.Enabled
		mgr.RUnlock()
		if effective != enabled {
			return "", refresh, fmt.Errorf("saved setting for %s, but a higher-precedence override takes priority", id)
		}
		if enabled {
			return id + " enabled", refresh, nil
		}
		return id + " disabled", refresh, nil
	case "restart":
		if !canRestart(target) {
			return "", refresh, fmt.Errorf("%s has no enabled persistent process to restart", id)
		}
		resp := restartPlugins(ctx, mgr, id)
		if len(resp.Results) == 0 {
			return "", refresh, errors.New(resp.Message)
		}
		r := resp.Results[0]
		if !resp.Success || r.Status == plugins.RestartStatusSkipped {
			return "", refresh, fmt.Errorf("%s: %s", id, r.Message)
		}
		return strings.TrimSpace(fmt.Sprintf("%s: %s %s", id, r.Status, r.Message)), refresh, nil
	default:
		return "", nil, fmt.Errorf("unknown plugins action %q", action)
	}
}
