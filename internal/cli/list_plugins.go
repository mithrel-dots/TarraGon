package cli

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/texttable"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/spf13/cobra"
)

var listPluginsCmd = &cobra.Command{
	Use:   "list",
	Short: "List installed plugins",
	RunE: func(cmd *cobra.Command, args []string) error {
		pluginRoot := plugins.DefaultDir()
		if pluginRoot == "" {
			return errors.New("could not determine plugin installation directory")
		}

		mgr := plugins.NewManager(pluginRoot)
		if err := mgr.Discover(); err != nil {
			return err
		}
		if err := mgr.ApplyOverrides(); err != nil {
			return err
		}
		loaded, daemonAvailable := loadedPluginsFromDaemon(750 * time.Millisecond)
		installed := make([]plugins.PluginConfig, 0, len(mgr.Plugins))
		for _, p := range mgr.Plugins {
			installed = append(installed, p.Config)
		}

		sort.Slice(installed, func(i, j int) bool { return installed[i].ID < installed[j].ID })
		rows := make([][]string, 0, len(installed))
		for _, cfg := range installed {
			loadedValue := "unknown"
			if daemonAvailable {
				_, ok := loaded[cfg.ID]
				loadedValue = fmt.Sprintf("%t", ok)
			}
			rows = append(rows, []string{
				cfg.ID,
				cfg.Name,
				cfg.Description,
				string(cfg.Lifecycle),
				fmt.Sprintf("%t", cfg.Enabled),
				loadedValue,
			})
		}

		texttable.Render(cmd.OutOrStdout(), []texttable.Column{
			{Header: "ID"},
			{Header: "NAME"},
			{Header: "DESCRIPTION"},
			{Header: "LIFECYCLE_MODE"},
			{Header: "ENABLED", AlignRight: true},
			{Header: "LOADED", AlignRight: true},
		}, rows)
		return nil
	},
}

func loadedPluginsFromDaemon(timeout time.Duration) (map[string]wire.PluginInfo, bool) {
	conn, err := net.DialTimeout("unix", wire.ResolveUISocketPath(), timeout)
	if err != nil {
		return nil, false
	}
	defer func() { _ = conn.Close() }()

	if err := wire.WriteMsg(conn, &wire.UIRequest{Type: wire.MsgStatus, ClientID: "plugin-list"}); err != nil {
		return nil, false
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, false
	}

	var status wire.StatusResponse
	if err := wire.ReadMsg(wire.NewScanner(conn), &status); err != nil || status.Type != wire.MsgStatus {
		return nil, false
	}

	loaded := make(map[string]wire.PluginInfo, len(status.Plugins))
	for _, info := range status.Plugins {
		loaded[info.ID] = info
	}
	return loaded, true
}
