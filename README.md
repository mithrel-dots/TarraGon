# TarraGon

A highly extensible automation and interaction framework with a language-agnostic plugin system. Built for speed and responsiveness, Tarragon can power anything from command launchers to custom user interfaces. Similar in spirit to macOS Spotlight, Alfred, or dmenu, but designed around a lightweight daemon–plugin architecture that makes it far more extensible and performant.

---

## Core Purpose

- **Primary**: Provide a fast, lightweight core that aggregates and routes requests/responses between plugins and external frontends (CLI, GUI, or custom).
- **Secondary**: Enable plugin development in any language using JSON messages over NATS or a Unix socket adapter, supporting use cases like:
  - application launching (one of the original use cases)
  - calculations and unit conversions
  - web/API integrations
  - clipboard or system utilities
  - custom UI components

Plugins can be invoked directly or contextually, and can expose commands, values, or data streams. Prefixes (e.g., `@search`) are optional hints for targeting specific plugins.

---

## Core Architecture

Every daemon starts an embedded NATS broker. [Frontends](docs/ui.md) and [persistent plugins](docs/plugins.md) can keep using `ui.sock` and `plugins.sock`: the adapters translate each JSON line into a NATS message and translate replies back. On-call plugins keep their command-line interface; an execution adapter carries their requests and results over NATS.

![Native clients and compatibility adapters exchange messages with the daemon core through embedded NATS. The core ranks plugin results using SQLite selection history.](docs/diagrams/architecture.svg)

No separate NATS service is required. TCP is off by default. Set `nats_tcp = true` to connect native clients at `127.0.0.1:4222` (`nats_port`). Without a token, any process that can reach that loopback port has access to the whole bus. To require authentication, set a nonempty `TARRAGON_NATS_TOKEN` on the daemon and configure native clients with the same token.

Restart the daemon after changing listener settings. `run_ipc` controls only `ui.sock`. Native clients need no Unix socket support, but the daemon still requires Unix sockets for its plugin listener.

### How a suggestion reaches the UI

1. **Route.** The daemon returns a `query_id` to the frontend that asked. A matching prefix targets one plugin; an unprefixed query goes to all eligible plugins.
2. **Merge.** The daemon normalizes each reply and replaces only that plugin’s results for the query.
3. **Display.** The daemon ranks the results so far and sends a [full snapshot](docs/ui.md#message-shapes) only to that Unix socket connection or native NATS session. The UI replaces its list without waiting for every plugin. Several plugin replies can appear in one snapshot.

Two frontends can search and select concurrently, even with the same `client_id`. Each receives its own acknowledgements, updates and action outcomes. Two native sessions on one NATS connection are separate owners; broker access still allows subscribing to other subjects.

**Ranking.** SQLite records how often and how recently you select each result. The daemon caches these scores at startup and refreshes them after each selection. By default, ranking blends 70% plugin score with 30% selection history (`frecency_weight`). In `global` order, equal blended scores break by ascending plugin ID, then by the plugin's original row order. In `grouped` order, groups rank by their best blended score, then by plugin ID; rows within each group keep their original order.

**Selection.** A [select](docs/ui.md#selecting-a-result) gets an `ok` on receipt and one terminal `select_response` on the same open session. A plugin can [request a refresh](docs/plugins.md#refreshing-a-producers-results) of its own results on that query without re-running the action or the other producers. A plugin that just prints `["one","two"]` needs no new handshake, manifest capabilities or command-line arguments.

To rebuild the diagram, install [D2](https://d2lang.com/tour/install/), edit the [source](docs/diagrams/architecture.d2), and run `just diagram`.

---

## Features

### Application Launcher
- Provided by the bundled `desktop_files` plugin, which parses `.desktop` files to find and launch installed applications.
- Fuzzy search with user-configurable scoring.
- Frecency-based sorting (frequency x recency).
- Optional icon display (depending on UI backend).
- Launched applications are detached from the plugin process and remain running across Tarragon daemon restarts.

### Hyprland Windows
- The bundled [`hyprland_windows`](plugins/hyprland_windows/README.md) plugin lists open windows with `@windows`, searchable by title, app, and workspace.
- Focus a window, bring it to the currently focused workspace, close it, or toggle floating mode.
- Install with `just plugin-install hyprland_windows`, then run `tarragon plugin restart hyprland_windows`.

### Plugin Overview
Type `@plugins` to browse plugins, including disabled ones. Filter by name or status with `@plugins calc` or `@plugins disabled`. Each row shows the plugin ID, effective prefix, lifecycle, and connection state. The overview is built into the daemon and uses your configured prefix symbol.

Use **open** to search a plugin by its prefix and **details** to inspect it. **Enable**, **disable** and **restart** refresh the overview using its original filter, even if an override was saved but startup failed. Changes use the same primary TOML overrides as `tarragon plugin config`; higher-priority drop-ins still apply. **Restart** applies to persistent plugins; an on-demand plugin starts again when next needed.

### Plugin System
- **Integrated Suggestions**: Seamlessly blends suggestions from eligible plugins, including bundled plugins for installed applications and other local tasks.
- **Language Agnostic**: Plugins are external executables or scripts.
- **Persistent plugins** stay running between queries and stream suggestions over the broker.
- **Plugin Lifecycle Modes**: Plugins declare their required lifecycle:
    - `daemon`: Runs persistently alongside the launcher daemon (e.g., clipboard manager).
    - `on_demand_persistent`: Started when first needed for a query or selection and kept running afterward.
    - `on_call`: Executed per request (ephemeral process lifecycle).
- **Fan-Out/Gather for Suggestions**: Input is broadcast to eligible plugins concurrently. Results are gathered asynchronously and displayed.
- **Dispatch Controls**: `require_prefix` gates prefix-only plugins, while `provides_general_suggestions` is the general-suggestion eligibility contract for global/unprefixed fan-out.
- **Optional Prefixes**: Prefixes (e.g., `@search`) can still force explicit dispatch to a specific plugin. Plugins declare a bare token and the leading symbol comes from the `prefix_symbol` config option, so it can be changed globally. Any query beginning with that symbol is treated as an explicit routing attempt, so partial or unknown prefixes do not fan out to general plugins.
- **Graceful Shutdown**: Plugins are stopped with `SIGTERM` and only killed if they overrun `plugin_stop_timeout` (default `5s`), so they can flush state and close sockets.
- **Live Restart**: `tarragon plugin restart [plugin-id]` re-reads manifests and bounces plugin processes without restarting the daemon.

### Plugin Benchmarking
Benchmarks run from a source checkout; they are not included in the installed `tarragon` binary.

- `just bench` connects to a running daemon like an external frontend, sends queries through the UI socket, and prints plugin latency results as a table.
- `just bench ipc` runs a separate suggestion-delivery experiment. It starts its own core, persistent fixture plugin processes, and headless UI. No running daemon or installed plugins are needed.
- `just bench ipc --matrix --samples 2000 --warmup 200 --rounds 3` compares the transport paths below.
- For congestion, try `just bench ipc --plugins 50 --burst 8 --samples 80 --warmup 16 --rounds 3`. Each plugin returns one small suggestion per query. The UI sends each burst back-to-back, then waits for every plugin's suggestion for every query before starting the next burst. Use `--burst 1` at the same plugin count as a control. Both flags default to 1.
- The table reports mean/p50/p95/p99 in microseconds for plugin encode to core decode, aggregation (including contention), outbound update to UI decode, and end to end. The UI times the first appearance of each `(query, plugin)` in cumulative snapshots. `Query->first` and `Query->all` start at UI query send; `Queries/s` measures completed queries over the elapsed UI measurement window. Startup, warmup, and rendering are excluded. A missing or mismatched suggestion fails the run.

| Benchmark path | How messages travel |
| --- | --- |
| Direct UDS (`uds`) | NDJSON over Unix sockets to the fixture core, bypassing NATS for comparison. |
| Native NATS (`nats-tcp`) | NATS messages over loopback TCP. |
| Unix bridge (`nats-uds-bridge`) | Native NATS bytes forwarded unchanged over a Unix socket. |
| Unix adapter (`uds-adapter`) | NDJSON on a Unix socket translated to and from NATS messages. |

### Plugin Installation & Security
- **Bundled plugin sources**: First-party bundled plugins live in `plugins/`.
- **Template plugin sources**: Authoring templates live separately in `plugin_templates/` so examples are not installed by bundled-plugin recipes.
- **Location**: Plugins reside in `~/.local/lib/tarragon/plugins/`.
- **Install Sources**:
    - `tarragon plugin install <git-url>` for plugin Git repositories with `plugin.toml` and a Makefile.
    - `tarragon plugin enable <name>` for system binaries exposing `tarragon manifest`.
- **Build Standard**: Plugins requiring compilation must include a `Makefile` providing standardized targets:
    - `make check-deps`: Verifies necessary build tools are present. The launcher can use this to inform the user about requirements.
    - `make install`: Builds the plugin and places artifacts correctly.
- **Entrypoint Resolution**:
    - Relative `entrypoint` values are resolved from the plugin directory.
    - Absolute `entrypoint` values are executed directly (used by system-enabled plugins).
- **User Responsibility**: Users should inspect the `Makefile` of third-party plugins before installation to understand the build process. The launcher may facilitate checking dependencies but relies on the user to vet plugin sources.

---

## Configuration

Tarragon reads its primary config from
`$XDG_CONFIG_HOME/tarragon/tarragon.toml` (normally
`~/.config/tarragon/tarragon.toml`). Use `--config-dir` to select a different
directory.

Optional local or packaged overrides belong in `tarragon.d/*.toml` beside the
primary file:

```text
~/.config/tarragon/
|-- tarragon.toml
`-- tarragon.d/
    |-- 10-host.toml
    `-- 90-local.toml
```

Drop-ins are merged in filename order. Later files override earlier files,
including individual keys in nested plugin sections. Files in `tarragon.d`
that do not end in `.toml` are ignored.

The complete precedence order, from lowest to highest, is:

1. `tarragon.toml`
2. `tarragon.d/*.toml` in lexical filename order
3. Environment variables such as `RESULT_ORDERING` and
   `PLUGINS_CALCULATOR_ENABLED`
4. Explicitly supplied command-line flags

Daemon reload and restart requests re-read the complete stack.

## Plugin Configuration

See [docs/plugins.md](docs/plugins.md).

## Contributing

Contributions are VERY welcome! See [https://github.com/mithrel-dots/TarraGon/blob/master/CONTRIBUTING.md](CONTRIBUTING.md) for guidelines. Running pre-commit locally helps keep builds green and diffs clean <3.

### Development TODO

Moved to GitHub issue: Roadmap — https://github.com/mithrel-dots/TarraGon/issues/4

## Shell Completions

Generate a completion script for one shell to stdout and redirect it to the location used by your shell.

```bash
tarragon completion generate zsh > ~/.zsh/completions/_tarragon
```

Other examples:

```bash
tarragon completion generate bash > ~/.local/share/bash-completion/completions/tarragon
tarragon completion generate fish > ~/.config/fish/completions/tarragon.fish
tarragon completion generate powershell > tarragon.ps1
```
