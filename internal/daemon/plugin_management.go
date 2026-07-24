package daemon

import (
	"context"
	"fmt"
	"log"
	"slices"
	"sort"

	"github.com/mithrel-dots/tarragon/internal/config"
	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

// The overview and the status endpoint use the same effective configuration.
func pluginStatus(mgr *plugins.Manager, registry *pluginRegistry) wire.StatusResponse {
	status := wire.StatusResponse{Type: wire.MsgStatus, Connected: registry.Names()}
	mgr.RLock()
	for id, p := range mgr.Plugins {
		cfg := p.Config
		if cfg.Enabled {
			status.Total++
		}
		_, connected := slices.BinarySearch(status.Connected, id)
		status.Plugins = append(status.Plugins, wire.PluginInfo{
			ID: id, Name: cfg.Name, Description: cfg.Description, Icon: cfg.Icon,
			Source: cfg.Source, Enabled: cfg.Enabled, Connected: connected,
			Lifecycle: string(cfg.Lifecycle), Prefix: cfg.ResolvedPrefix,
			RequirePrefix: cfg.RequirePrefix, ProvidesGeneral: cfg.ProvidesGeneral,
			Capabilities: cfg.Capabilities,
		})
	}
	mgr.RUnlock()
	sort.Slice(status.Plugins, func(i, j int) bool { return status.Plugins[i].ID < status.Plugins[j].ID })
	return status
}

// load either reads the current config or writes and reloads a CLI-compatible
// override. Only config access and the state transition need the manager lock.
func reloadPlugins(ctx context.Context, mgr *plugins.Manager, load func() error) error {
	mgr.Lock()
	if err := load(); err != nil {
		mgr.Unlock()
		return err
	}
	before := make(map[string]plugins.PluginConfig, len(mgr.Plugins))
	for id, p := range mgr.Plugins {
		before[id] = p.Config
	}
	if err := mgr.DiscoverNew(); err != nil {
		mgr.Unlock()
		return fmt.Errorf("failed to discover new plugins: %w", err)
	}
	if err := mgr.ApplyOverrides(); err != nil {
		mgr.Unlock()
		return fmt.Errorf("failed to apply config overrides: %w", err)
	}
	var stop []string
	startDaemon := false
	for id, p := range mgr.Plugins {
		old := before[id]
		cfg := p.Config
		if p.Running() && (!cfg.Enabled || cfg.Lifecycle != old.Lifecycle) {
			stop = append(stop, id)
		}
		if cfg.Enabled && cfg.Lifecycle == plugins.LifecycleDaemon && (!old.Enabled || old.Lifecycle != cfg.Lifecycle) {
			startDaemon = true
		}
	}
	mgr.Unlock()
	mgr.StopPlugins(stop)
	if startDaemon {
		if err := mgr.StartPersistent(ctx, wire.ResolvePluginsSocketPath()); err != nil {
			return fmt.Errorf("reload applied, but failed to start daemon plugins: %w", err)
		}
	}
	return nil
}

func restartPlugins(ctx context.Context, mgr *plugins.Manager, target string) wire.RestartResponse {
	resp := wire.RestartResponse{Type: "restart_response"}
	mgr.Lock()
	if err := config.ReloadConfig(); err != nil {
		log.Printf("[UI] restart: could not re-read config: %v", err)
	}
	if err := mgr.RefreshConfigs(); err != nil {
		log.Printf("[UI] restart: plugin discovery failed: %v", err)
	}
	err := mgr.ApplyOverrides()
	mgr.Unlock()
	if err != nil {
		resp.Message = fmt.Sprintf("failed to apply config overrides: %v", err)
		return resp
	}
	var targets []string
	if target != "" {
		targets = []string{target}
	}
	failed, restarted := 0, 0
	for _, r := range mgr.Restart(ctx, wire.ResolvePluginsSocketPath(), targets) {
		resp.Results = append(resp.Results, wire.RestartResult(r))
		switch r.Status {
		case plugins.RestartStatusError:
			failed++
		case plugins.RestartStatusRestarted:
			restarted++
		}
	}
	resp.Success = failed == 0
	resp.Message = fmt.Sprintf("restarted %d plugin(s)", restarted)
	if failed > 0 {
		resp.Message += fmt.Sprintf(", %d failed", failed)
	}
	return resp
}
