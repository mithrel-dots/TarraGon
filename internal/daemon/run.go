package daemon

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mithrel-dots/tarragon/internal/db"
	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/spf13/viper"
)

// RunDaemon composes the daemon services and blocks until shutdown.
func RunDaemon() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pluginDir := plugins.DefaultDir()
	mgr := plugins.NewManager(pluginDir)
	mgr.SetStopTimeout(pluginStopTimeout())
	// TODO: periodically rescan pluginDir for new plugins/config changes.
	if err := mgr.Discover(); err != nil {
		log.Printf("Plugin discovery error: %v", err)
	}
	if err := mgr.ApplyOverrides(); err != nil {
		log.Printf("Plugin override error: %v", err)
	}
	defer mgr.StopAll()

	// Aggregates store with configurable limit
	maxAgg := viper.GetInt("max_aggregates")
	if maxAgg <= 0 {
		maxAgg = 64
	}
	orderingMode := viper.GetString("result_ordering")
	if orderingMode == "" {
		orderingMode = "global"
	}
	frecencyWeight := viper.GetFloat64("frecency_weight")
	if frecencyWeight == 0 {
		frecencyWeight = 0.3
	}
	database, err := db.Open(viper.GetString("db_path"))
	if err != nil {
		log.Printf("DB open error (frecency disabled): %v", err)
	}
	if database != nil {
		defer func() {
			if err := database.Close(); err != nil {
				log.Printf("DB close error: %v", err)
			}
		}()
	}
	store := newAggregateStore(maxAgg, orderingMode, database, frecencyWeight)
	ui := newUIRegistry()

	// Start plugin listener BEFORE spawning plugin processes, so the
	// socket is ready to accept connections when plugins start.
	reqOut, registry := startPluginRouter(ctx, store)
	port := 0
	if viper.GetBool("nats_tcp") {
		port = viper.GetInt("nats_port")
		if port < 1 || port > 65535 {
			return fmt.Errorf("nats_port must be between 1 and 65535")
		}
	}
	bus, err := startNATS(ctx, port, os.Getenv("TARRAGON_NATS_TOKEN"))
	if err != nil {
		return fmt.Errorf("start NATS: %w", err)
	}
	defer bus.close()
	registry.bus = bus
	handleUI := func(conn peerConn) { handleUIClient(bus.ctx, conn, mgr, reqOut, registry, store, ui, database) }
	handlePlugin := func(conn peerConn) { handlePluginConn(bus.ctx, conn, registry, store, ui) }
	if port != 0 {
		if err := bus.listenNative(handleUI, handlePlugin); err != nil {
			return err
		}
		log.Printf("[NATS] listening on 127.0.0.1:%d", port)
	}
	ln, err := bus.listenUnix(wire.ResolvePluginsSocketPath(), handlePlugin)
	if err != nil {
		return err
	}
	defer ln.Close()
	if viper.GetBool("run_ipc") {
		uiListener, err := bus.listenUnix(wire.ResolveUISocketPath(), handleUI)
		if err != nil {
			return err
		}
		defer uiListener.Close()
	}

	// Now start persistent plugin processes; the listener is already up.
	if err := mgr.StartPersistent(ctx, wire.ResolvePluginsSocketPath()); err != nil {
		log.Printf("Plugin start error: %v", err)
	}

	<-ctx.Done()
	log.Println("Daemon shutting down.")
	return nil
}

// pluginStopTimeout resolves the configured plugin shutdown grace period,
// falling back to the default when it is unset or unparseable.
func pluginStopTimeout() time.Duration {
	raw := strings.TrimSpace(viper.GetString("plugin_stop_timeout"))
	if raw == "" {
		return plugins.DefaultStopTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		log.Printf("Invalid plugin_stop_timeout %q; using %s", raw, plugins.DefaultStopTimeout)
		return plugins.DefaultStopTimeout
	}
	return d
}
