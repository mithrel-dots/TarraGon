# Building a Plugin for Tarragon

This guide explains how plugins integrate with the Tarragon daemon today.

Plugins declare a lifecycle in `plugin.toml`, and Tarragon runs them according to that lifecycle:

1. `daemon`: a persistent process connected through NATS or the Unix socket adapter.
2. `on_demand_persistent`: a persistent process that Tarragon starts on first use, then keeps running until the daemon shuts down or the plugin is stopped.
3. `on_call`: an ephemeral CLI plugin invoked as `entrypoint tarragon <subcommand>`.

**Native NATS:** open a [session](ui.md#native-nats-session) on `tarragon.plugin.open`. Subscribe to the session's `.out` for requests and flush the subscription, then send your usual `hello` and responses on `.in`. Send one JSON envelope per NATS message. The same envelopes travel as NDJSON on `plugins.sock`. Neither transport adds a capability handshake: persistent plugins still register with `hello`, and on-call plugins need only argv and stdout.

## Directory Layout and Install

Plugins are managed under `~/.local/lib/tarragon/plugins/<plugin-id>/` and loaded by the daemon at startup.

Repository layout note:

- Bundled first-party plugins live in `plugins/`.
- Authoring templates live in `plugin_templates/` and are intentionally separate from bundled plugins.
- Template directories use the same `plugin.toml` + `Makefile` contract as installable plugin repositories.

There are two supported plugin sources:

1. **Plugin Git repository** (installed with `tarragon plugin install <git-url>`): the repository root must contain `plugin.toml` and a `Makefile`; `entrypoint` is typically a path relative to the installed plugin directory.
2. **System plugin** (enabled with `tarragon plugin enable <name>`): Tarragon looks up the binary in `PATH`, runs `<binary> tarragon manifest`, rewrites a relative `entrypoint` to the resolved absolute binary path, sets `source = "system"`, and stores the resulting manifest in Tarragon's plugin directory.

### System Plugin Enable + Reload Behavior

`tarragon plugin enable <name>` writes/updates plugin metadata on disk. A running daemon must reload or restart before it can use new manifests.

Current behavior note: `enable` itself does not trigger daemon reload. In practice, run `tarragon plugin restart`, or issue a reload-triggering config update (for example via `tarragon plugin config ...`).

Integration contract:

- enable writes the plugin manifest into Tarragon's plugin directory with system entrypoint normalization.
- reload re-reads plugin metadata/config and refreshes in-memory plugin state used for status + dispatch.
- after reload, newly enabled system plugins are eligible for status/dispatch according to their manifest fields (`enabled`, `lifecycle_mode`, `prefix`, `require_prefix`, `provides_general_suggestions`).

### Restarting Plugins

`tarragon plugin restart [plugin-id]` bounces plugin processes in the running daemon. With no argument every plugin is targeted.

Restart re-reads manifests and config overrides first, so it also picks up plugins installed or reconfigured since the daemon started. Reported status per plugin:

| Lifecycle | Status | Behavior |
| --- | --- | --- |
| `daemon` | `restarted` | process stopped and started again |
| `on_demand_persistent` | `stopped` | stopped only; starts again when next needed for a query or select |
| `on_call` | `skipped` | no long-lived process to bounce |
| any, disabled | `stopped` | stopped, not started |
| unknown name | `error` | reported as not found |

## Plugin Shutdown Contract

Tarragon stops plugin processes gracefully. A plugin is sent `SIGTERM` and is only sent `SIGKILL` if it is still alive after the grace period, which is set by the `plugin_stop_timeout` config option (default `5s`).

This applies to daemon shutdown, `tarragon plugin restart`, and plugins stopped by a reload because they were disabled or had their lifecycle changed.

Plugin authors should handle `SIGTERM`: flush state, close sockets and exit. Persistent plugins that ignore it will still be killed, just later. Plugins are stopped concurrently, so total shutdown time is bounded by the slowest plugin rather than the sum of all of them.

### Plugin Source Metadata

System-enabled manifests include:

```toml
source = "system"
```

Status responses currently include source metadata when it is present in the manifest. Treat it as origin metadata (`system` vs local/default), and tolerate missing source metadata for local plugins or older manifests.

- Required files for plugin repositories installed with `tarragon plugin install`:
  - `plugin.toml` (configuration)
  - `entrypoint` executable (e.g., `my_plugin.py` or `my_plugin`)
  - `Makefile` with targets `check-deps`, `install`, `uninstall`, `run` (required)
    - The plugin manager will call `make check-deps` and `make install` to help users install your plugin.

Example `plugin.toml`:

```toml
id = "template_python"
name = "Template Python"
description = "Template Python plugin"
enabled = true
entrypoint = "template_plugin.py"
lifecycle_mode = "daemon"  # "daemon" | "on_demand_persistent" | "on_call"

provides_general_suggestions = true
prefix = "tpl"  # typed as @tpl with the default prefix_symbol
build_dependencies = ["python3"]
capabilities = ["suggest"]
```

```toml
id = "calculator"
name = "Calculator"
description = "Evaluate basic math expressions"
enabled = true
entrypoint = "calc_plugin_executable"  # Relative for local plugins; may be absolute for system-enabled plugins
lifecycle_mode = "on_demand_persistent"  # Options: "daemon", "on_demand_persistent", "on_call"
provides_general_suggestions = true  # Responds to input without a prefix?
prefix = "calc"  # Optional: bare token; the prefix_symbol is prepended
build_dependencies = ["make", "go"]  # Optional: List of tools checked by 'make check-deps'
capabilities = ["suggest", "icon"]  # Optional: Extra features
icon = "calc.png"  # Optional: Icon path
```

## Plugin Identity

Every plugin has two distinct names:

- `id` is the stable identifier. It is used for the install directory, the `[plugins.<id>]` config section, `tarragon plugin config <id>`, the `plugin` field on results, and the plugin's IPC routing name.
- `name` is a human-readable display name with no routing meaning. It may contain spaces and capitals.

`id` must consist of lowercase letters, digits, `_` and `-`. When `id` is omitted it defaults to the plugin's install directory name; invalid characters are normalized (`"System Control"` becomes `system_control`). Declaring `id` explicitly is recommended.

The daemon passes the id to plugins as `TARRAGON_PLUGIN_NAME` and `TARRAGON_PLUGIN_ID`, and the display name as `TARRAGON_PLUGIN_DISPLAY_NAME`. A persistent plugin must send its id in the `hello` message, or the daemon cannot route requests to it.

## Configuration Overrides

Tarragon-level plugin settings are overridden in the configuration stack,
keyed by plugin id:

```toml
[plugins.system_control]
enabled = true
prefix = "@sys"
lifecycle_mode = "on_call"
```

Only `enabled`, `prefix` and `lifecycle_mode` are supported. Any other key, or a section that matches no installed plugin id, is ignored and reported as a warning in the daemon log. Sections keyed by display name still apply for backwards compatibility, but are deprecated and warn.

Overrides can also be written with `tarragon plugin config <id> --prefix sys`,
which updates the primary `tarragon.toml` and asks a running daemon to reload.
Settings in a higher-precedence `tarragon.d/*.toml` file still win. See the
main README for the complete configuration precedence order. Plugin-specific
settings that Tarragon does not understand belong in the plugin's own config
file instead.

Lifecycle modes:
- `daemon`: started by the daemon at startup and kept running.
- `on_demand_persistent`: started when a matching query or select needs the plugin and kept running afterward. For a select the daemon waits for `hello` within the action deadline; the process lifetime is independent of that wait.
- `on_call`: executed per query or select using the [CLI contract](#on-call-cli-contract).

## Dispatch Eligibility and Prefix Targeting

When the UI does not address a plugin by ID, Tarragon routes by input text:

- **Global/unprefixed query**:
  - `require_prefix = true` excludes a plugin from unprefixed dispatch.
  - `provides_general_suggestions` is the contract field for general-suggestion eligibility. Current dispatch requires it to be true for unprefixed queries. For compatibility, discovered plugin manifests that omit the field default to true, but plugin authors should set it explicitly.
- **Prefix-targeted query**: when input starts with a plugin prefix, Tarragon dispatches only to the matched plugin and forwards query text with the prefix removed.
  - Prefix-targeted dispatch does not depend on `provides_general_suggestions`.
  - If prefixes overlap, the longest matching prefix wins. Equal-length matches are a configuration error; dispatch falls back to the lowest plugin id and the daemon logs a collision warning.
- **Incomplete or unknown prefix**: any input beginning with the configured global prefix symbol is treated as an explicit routing attempt. If it does not match an enabled plugin prefix, no plugin is dispatched, including general-suggestion plugins.

Use `require_prefix` for strict prefix-only plugins. Use `provides_general_suggestions` as the explicit global-eligibility signal.

### The Prefix Symbol

Manifests declare a bare prefix token, not the leading symbol:

```toml
prefix = "calc"
```

The symbol comes from the top-level `prefix_symbol` config option, which defaults to `@`. The example above is typed as `@calc`, and changing `prefix_symbol` to `:` changes it to `:calc` for every plugin at once, without editing any manifest.

A prefix that starts with a non-alphanumeric character is treated as literal and is used exactly as written, so `prefix = "@calc"` and `prefix = "="` keep working regardless of the configured symbol. Prefix overrides in `[plugins.<id>]` follow the same rule.

The effective prefix is what `tarragon plugin list`, `tarragon plugin config` and the status response report. The configured symbol is passed to plugins as `TARRAGON_PREFIX_SYMBOL` for plugins that expose several prefixes of their own.

## Scoped queries and plugin context

A UI can query an enabled plugin by its stable `plugin` ID, including a plugin with no prefix. Only that plugin receives the query. Its `text` is literal: `""`, surrounding spaces, and text starting with another plugin's prefix all pass through unchanged. An unknown or disabled ID gets an `ack` and a snapshot with `plugins[id].state:"error"` and no rows.

The UI may include an opaque `context` string describing the destination. Use it for non-secret values such as a catalog ID. It must be valid Unicode encoded in at most **4096 UTF-8 bytes**, without NUL. Omitting it or sending `""` or `null` means no context. A query with invalid context (including other JSON types) or nonempty context without an explicit `plugin` gets an `error` before dispatch; a prefix in `text` does not qualify. Invalid context on a query or select leaves the UI session and its watches open.

For a persistent plugin, context is an optional field on `request` and `select` on either transport:

```text
UI → daemon       {"type":"query","plugin":"catalog","context":"show:42","text":""}
daemon → catalog  {"type":"request","query_id":"q2","context":"show:42","text":""}
UI → daemon       {"type":"select","plugin":"catalog","query_id":"q2","result_id":"episode:2","action":"play"}
daemon → catalog  {"type":"select","query_id":"q2","context":"show:42","result_id":"episode:2","action":"play"}
```

A select can supply `context` directly. When the same UI selects from its retained query for that handler, the recorded context takes precedence, including an empty context. A select naming another UI's retained query or supplying invalid context yields the normal `ok` followed by `select_response` with `code:"not_run"`. A producer refresh reuses the recorded text and context.

An `on_call` plugin reads nonempty context from **`TARRAGON_CONTEXT`** for that invocation. Without context the variable is absent, even if it was set in the daemon's environment. The example above runs `entrypoint tarragon query ""` and `entrypoint tarragon select "episode:2" "play"`; context adds no argv slot.

## Lifecycle-Aware Status Expectations

- `daemon` and `on_demand_persistent` plugins may be persistently connected (`connected=true`) while running.
- `on_call` plugins are ephemeral and usually not connected between requests.

Treat `connected` as transport state, not total availability. An enabled `on_call` plugin can be dispatchable even while not persistently connected.

Entrypoint path rules:
- Relative `entrypoint`: resolved from the plugin directory (`~/.local/lib/tarragon/plugins/<plugin-id>/...`).
- Absolute `entrypoint`: executed directly as-is (used by system-enabled plugins).
- If an absolute entrypoint for a `source = "system"` on-call plugin no longer
  exists, Tarragon resolves its basename through the daemon's `PATH`. This lets
  system plugins continue working after their executable moves between standard
  user/system bin directories. Local plugin manifests do not use this fallback.

## Unix Domain Socket + NDJSON Protocol

Endpoint: resolved by the daemon at startup (see "Socket Locations" below) and
passed to persistent plugins via `TARRAGON_PLUGINS_ENDPOINT`.

Protocol:
1) Connect to the Unix socket path from `TARRAGON_PLUGINS_ENDPOINT`.
2) Send hello (one NDJSON line):
   - `{ "type": "hello", "name": "<plugin-id>" }\n`
3) Loop:
   - Receive request (one NDJSON line):
     - `{ "type": "request", "query_id": "<id>", "text": "<input>" }\n`
   - Process and send response (one NDJSON line):
     - `{ "type": "response", "query_id": "<id>", "data": <your JSON> }\n`

NDJSON framing means each message is exactly one JSON object on one line, terminated by `\n`.

The daemon measures latency and merges your response into the snapshot for that `query_id`. Only the UI connection or native session that issued the query receives its updates. Its snapshot includes per-plugin states (`pending`, `done`, `empty`, `error`), and may combine several plugins' responses in one update.

A persistent plugin may send more `response` frames for a retained query on the same connection. Each replaces that plugin's rows; an empty list clears them. If you opt into refresh, finish sending the old round's updates before requesting a replay. The first response after a refresh is dispatched counts as its answer: the wire carries no round number.

### Live results

A persistent producer can opt into a live query on its **solicited** `response`. Keep a bounded window (for example, the last 100 log lines) and send it as a complete replacement each time:

```jsonl
{"type":"response","query_id":"q3","live":true,"data":{"results":["line 1"]}}
{"type":"live_update","query_id":"q3","data":{"results":["line 1","line 2"]}}
{"type":"live_update","query_id":"q3","final":true,"data":{"results":["finished"]}}
```

Each `live_update` replaces only your producer's rows. It is accepted only for the opted-in query on the original connection. `final:true` publishes the last replacement and ends that producer's subscription. A refresh `request` still needs its own `response`; a push, including a final push, cannot acknowledge it. Include `live:true` on that response to keep pushing. Legacy producers can continue using their original `response` behavior.

Opting in also promises to handle `{"type":"end","query_id":"q3"}`: stop the feed for that query only. This applies even without an `end` manifest capability. UIs need not request a reservation for live results; ordinary history eviction can end the feed. See [watch limits and errors](ui.md#watching-a-live-view) for the retention and envelope budgets. On-call plugins return one result per invocation.

Environment variables passed to plugins:
- `TARRAGON_PLUGINS_ENDPOINT`: Unix socket path to connect to (for example, `/run/user/1000/tarragon/plugins.sock`).
- `TARRAGON_PLUGIN_NAME`: The stable plugin id used for IPC routing. This is set for persistent and on-call plugins.
- `TARRAGON_PLUGIN_ID`: The stable plugin id. This is set for persistent and on-call plugins.
- `TARRAGON_PLUGIN_DISPLAY_NAME`: The human-readable display name. This is set for persistent and on-call plugins.
- `TARRAGON_PLUGIN_PREFIX`: The plugin's resolved user-facing prefix, or empty
  when it has no single prefix. This is set for persistent and on-call plugins.
- `TARRAGON_PREFIX_SYMBOL`: The configured global prefix symbol.

`TARRAGON_PLUGINS_ENDPOINT` is set only for persistent plugins. On-call plugins
receive the identity and prefix variables above when invoked for a query or
selection.

### Ending pending work

A persistent plugin can add `end` to its manifest `capabilities` if it can cancel a pending query by ID. When the view ends, the daemon sends one frame on the connection that received the request:

```json
{"type":"end","query_id":"q2"}
```

Stop work for `q2` only; overlapping requests and selects are independent. No reply is required. A request may finish while `end` is in transit, so an already finished ID is a no-op. Once the daemon accepts a `live:true` response, that query also gets `end` without a manifest capability. Declare `end` to receive cancellation while the first response is still pending.

An ordinary completed query gets no `end`. Neither does an undispatched request or a replacement connection. The daemon drops later data for the ended ID. If no `end` was sent while a request was pending and its first `response` arrives with `live:true`, the daemon sends one `end` on the original connection. An ordinary `response` or `live_update` cannot trigger this stop.

At 1,024 unanswered removed requests on a connection, the daemon reports a query error instead of sending additional requests that lack a declared `end` capability. A `response` to a removed request clears it, whether or not it opts into live results. Once the count drops below the limit, new requests can be sent again. Already-written requests, active live feeds and selects continue on the same connection. On-call queries use process cancellation instead; their select processes are independent.

### Socket Locations

The daemon resolves the plugins socket path at startup, in order:

1. `TARRAGON_PLUGINS_SOCKET`, if set and non-empty, is used verbatim.
2. Otherwise, if `XDG_RUNTIME_DIR` is set and non-empty:
   `$XDG_RUNTIME_DIR/tarragon/plugins.sock`.
3. Otherwise: `/tmp/tarragon-<euid>/plugins.sock` (`<euid>` is the daemon's
   effective UID).

The daemon creates a missing parent directory with mode `0700`. An existing
parent must already be private and owned by the daemon user. The daemon removes
stale sockets and refuses to replace live ones. Persistent plugins
should not hardcode a socket path; always read `TARRAGON_PLUGINS_ENDPOINT`.

### Python skeleton

```python
import json, os, socket

endpoint = os.environ["TARRAGON_PLUGINS_ENDPOINT"]  # always set by the daemon for persistent plugins
plugin_id = os.environ.get("TARRAGON_PLUGIN_ID", "my_plugin")

s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(endpoint)
s.sendall(json.dumps({"type": "hello", "name": plugin_id}).encode() + b"\n")

f = s.makefile('r')
while True:
    line = f.readline()
    if not line:
        break
    msg = json.loads(line)
    if msg.get("type") == "request":
        reply = {"type": "response", "query_id": msg["query_id"], "data": [msg["text"]]}
    elif msg.get("type") == "select":
        reply = {"type": "select_response", "success": True, "message": msg["result_id"]}
    else:
        continue
    s.sendall(json.dumps(reply).encode() + b"\n")
```

### Rust skeleton

```rust
use std::os::unix::net::UnixStream;
use std::io::{BufRead, BufReader, Write};

// Always set by the daemon for persistent plugins.
let endpoint = std::env::var("TARRAGON_PLUGINS_ENDPOINT")?;
let stream = UnixStream::connect(&endpoint)?;
let mut writer = stream.try_clone()?;
let reader = BufReader::new(stream);

writeln!(writer, r#"{{"type":"hello","name":"my_plugin"}}"#)?;

for line in reader.lines() {
    let msg: serde_json::Value = serde_json::from_str(&line?)?;
    let reply = match msg["type"].as_str() {
        Some("request") => serde_json::json!({
            "type": "response", "query_id": msg["query_id"], "data": [msg["text"]]
        }),
        Some("select") => serde_json::json!({
            "type": "select_response", "success": true, "message": msg["result_id"]
        }),
        _ => continue,
    };
    writeln!(writer, "{}", reply)?;
}
```

## Persistent selects

On either transport, the daemon sends `select` to the plugin that produced the row. Send **one final `select_response` per dispatched select**, on the same connection. A late reply still belongs to its original select. If you keep a query cache, the incoming `query_id` is the original ID when supplied. It can also be empty or refer to an evicted row; if you cannot act, reply with `success:false` and a useful `message`.

With no `action_id` manifest capability, the exchange is unchanged:

```text
daemon → plugin  {"type":"select","query_id":"q1","result_id":"one","action":"mark"}
plugin → daemon  {"type":"select_response","success":true,"message":"Marked"}
```

The daemon serializes selects across **all UIs** on that plugin connection; queries still flow. While one select is in flight, later selects wait in a bounded queue. Each deadline starts at receipt; an excess or expired entry is `not_run` and never reaches the plugin. If the in-flight select times out, its owner gets `completion_unknown`. The daemon quarantines that connection for selects: queued and new clicks get `not_run` until the daemon drains and discards the one late untagged final, or the connection is replaced (for example, via `@plugins` → **restart**). A successful query does not clear quarantine. Sending two finals risks acknowledging somebody else's click.

To handle concurrent selects, declare `capabilities = ["action_id"]` in `plugin.toml` (retain any other capabilities). This promises that the plugin echoes the ID on every action reply. The daemon pins that capability at registration for the lifetime of the connection. Changing the manifest does not switch an open connection between modes.

```text
daemon → plugin  {"type":"select","query_id":"q1","result_id":"one","action":"mark","action_id":"a7"}
plugin → daemon  {"type":"action_progress","action_id":"a7","message":"Applying"}
plugin → daemon  {"type":"select_response","action_id":"a7","success":true,"message":"Marked"}
```

Tagged selects are also bounded; excess submissions get `not_run`. Tagged replies may finish out of order. The daemon matches **both** `action_id` and the dispatch connection; it drops untagged, unknown, wrong-connection, duplicate or late finals without falling back to an untagged match. `action_progress` is optional and non-final. Only matching progress for a pending action renews that action's inactivity deadline. Only a UI that requested `ui_features:["action_progress"]` on its select receives progress.

On the UI side, the daemon sends `ok` for receipt (including rejected selects), then one terminal `select_response` to the originating peer while it remains connected. The daemon supplies `action_id` and echoes the UI's optional `request_id`; plugins do not choose the recipient. A definitive reply preserves the handler's `success` and `message`. When dispatch definitely did not occur, the daemon returns `success:false, code:"not_run"`; when delivery or completion is uncertain, it returns `success:false, code:"completion_unknown"`. The effect may already have happened; the action is never retried automatically.

## On-Call CLI Contract

If your plugin uses `lifecycle_mode = "on_call"`, Tarragon invokes CLI subcommands on the plugin executable under the `tarragon` namespace:

```
$ my_plugin tarragon query "hello world"
{"input":"hello world","variants":["HELLO WORLD","Hello World","world hello"]}
```

- Query command: `entrypoint tarragon query "<text>"`
  - the entire text is one argument, including spaces
  - stdout: one JSON payload (same shape rules as persistent plugin `data`)
  - non-zero exit: treated as plugin error
- Select command: `entrypoint tarragon select "<result-id>" [action]`
  - used when user executes a result action for this plugin
  - plugin should act based on `result-id` (on-call plugins are ephemeral and should not rely on in-memory query state)
  - omit the action argument entirely when empty; no query or action correlation IDs are appended to argv
  - exit zero with empty stdout means success; on a zero exit, optional stdout JSON can set `success` and `message`. A nonzero exit reports failure and is not proof that the action had no effect.

For example, a script that prints `["one","two"]` on `query` and exits zero silently on `select` needs no manifest capabilities. `entrypoint tarragon query "two words"` gets a single text argument; selecting `one` without `query_id` or an action runs `entrypoint tarragon select "one"`.

The UI can issue further requests while a select process runs. Failure to start the process gives `not_run`. Once started, the child can continue past the UI reply deadline under the daemon's longer process limit. The timeout gives the UI one `completion_unknown`; a late exit cannot send a second outcome or trigger a refresh. Query processes remain tied to their query request.

On-call stdout is limited to 8 MiB plus a final newline. The daemon rejects oversized replies and keeps at most 4 KiB of stderr for error messages. If a valid select reply grows past 8 MiB when encoded for the UI, the daemon shortens its message and preserves the reported outcome and any `refresh` instruction.

## Refreshing a producer's results

To show the effect of an action on the selected list, include a single `next` object on the final response. For an on-call select, print this to stdout and exit zero:

```json
{"success":true,"message":"Marked","next":{"type":"refresh"}}
```

A persistent handler puts the same `next` on its `select_response`, with `action_id` if it declared that capability. An action can explicitly request refresh even with `success:false`, for example if it changed some state before failing. To keep the list visible after success, give the offered row action `type:"keep_open"`.

The daemon consumes `next:{"type":"refresh"}`: it queues the action outcome, then sends only the **original producer** a query with the **original resolved text**, even if the prefix has since changed. The UI and persistent producer see the same `query_id` as before; an on-call producer receives the same query argv. Neither the select nor the other producers run again. A baseline UI just gets an ordinary full snapshot for the original query.

Refresh requires a retained source query and row owned by the selecting UI, an enabled producer, and consent to replay. Asking to refresh your own result gives consent for that invocation. Declare `capabilities = ["replay"]` (alongside any existing capabilities) only if your queries are safe to repeat. Another handler cannot give consent for your plugin. A missing or ineligible source simply skips refresh; the original action outcome stands.

For each `(query_id, producer)` only one query is outstanding. Multiple refresh requests during it schedule at most one follow-up after a definitive response or failure, if the source is still eligible. An ambiguous timeout does not permit a competing request with the same ID; the daemon waits for the old reply or connection retirement, or drops the state when the query goes away. Replies from a replaced connection are discarded. Old rows stay visible until replaced; return an explicit empty list to clear your rows. A transport error can mark your state `error` while retaining the last rows.

## Result Shape

Plugin payloads are normalized into Tarragon result items. The daemon currently understands these common shapes:

- `{ "results": [...] }`
- `{ "suggestions": [...] }`
- `{ "items": [...] }`
- `{ "choices": [...] }`
- `{ "variants": [...] }`
- `[ ... ]`
- a single object with `id` plus one of `label`, `title`, or `text`

Array items can be objects or strings.

To preserve a meaningful sequence such as episode order, return
`{"ordered":true,"results":[{"id":"ep2","score":0.1},{"id":"ep10","score":0.9}]}`.
When the query explicitly targeted your plugin by ID or resolved prefix, the
daemon keeps that order and skips frecency blending. Global search still uses
the configured ranking, even when yours is the only result. Each response
replaces the previous one; omit `ordered` to resume ordinary ranking.

Recognized object fields:

- `id`
- `label`, `title`, or `text`
- `description`
- `icon`
- `category`
- `score`
- `preview_path`
- `ref` (an independently usable value for [cross-plugin actions](#cross-plugin-actions))
- `actions`

Example:

```json
{
  "results": [
    {
      "id": "firefox.desktop",
      "label": "Firefox",
      "description": "Web browser",
      "score": 0.97,
      "icon": "firefox",
      "category": "apps",
      "actions": [
        { "name": "open", "default": true }
      ]
    }
  ]
}
```

### Cross-plugin actions

A producer can expose a value separately from its private row ID. For example, `file_finder` emits a `file` reference for each path and derives its MIME type from the extension. A producer with private IDs could emit:

```json
{"id":"f17","label":"Sky","ref":{"type":"file","value":"/pictures/sky.png","mime":"image/png"},"actions":[{"name":"open","default":true}]}
```

Declare independently invokable actions in the **consumer's** `plugin.toml`. The bundled `wallpaper` plugin declares:

```toml
[[handlers]]
action = "set"
label = "Set as wallpaper"
accepts = "file"
mime = "image/*"
```

`accepts` is `file`, `url` or `text`. `ref.value` must be a nonempty string without NUL. A `file` reference must be an absolute filesystem path. The daemon checks the path form without accessing the filesystem. It omits invalid references from normalized rows, so they cannot create cross-plugin offers. The raw producer result and its native actions are preserved.

`mime` is optional: without it, any reference of that kind matches. Otherwise use a bare media type or `type/*`; a producer must supply a concrete matching MIME type. A missing MIME never matches a restricted rule. Handler `action` and `label` are nonblank strings. Invalid rules do not prevent the producer's other results from appearing.

The daemon reads enabled effective manifests for all lifecycles. Creating an offer sends **nothing** to the consumer. If the user selects it, a persistent handler receives `{"type":"select","query_id":"","result_id":"/pictures/sky.png","action":"set"}` (with an `action_id` on a tagged connection). An on-call handler receives `entrypoint tarragon select "/pictures/sky.png" set`. Neither gets the producer's private context or cached row ID. The consumer validates the supplied value for its operation. A `next:{"type":"refresh"}` asks to refresh the **producer** and requires its `replay` capability.

Only the daemon assigns `secondary` and `offer_id`. The generated legacy action-name suffix ` [via <plugin>/<action> #<token>]` is reserved.

### Action behavior

An action can request query replacement instead of plugin selection:

```json
{
  "name": "Episodes",
  "type": "query_replace",
  "query": "@anime episodes 154587"
}
```

The UI replaces its input with `query`, remains open, and submits a normal
query request. It must not send a select request for this action.

An action can also keep the UI open after a successful normal selection:

```json
{
  "name": "Copy",
  "type": "keep_open"
}
```

Action types have these semantics:

- no `type`: send a normal select request; the UI dismisses after success.
- `type: "keep_open"`: send a normal select request; the UI remains open after success.
- `type: "query_replace"`: replace the query and submit it normally; the UI remains open.
- `type: "open"`: push the current view and send a new scoped query to `destination.plugin`.

The UI should remain open when an action fails so the user can see the outcome. The
`keep_open` type is a UI hint, and the UI may apply its own user-level policy.

### Opening a view or returning to its parent

When the destination is known in the result, return an Open action. The UI queries the target by ID; the plugin receives no select. `title` and `placeholder` are labels chosen by the opener, not fields on the target's query:

```json
{"name":"Episodes","type":"open","destination":{"plugin":"catalog","context":"show:42","text":""},"title":"Episodes","placeholder":"Filter episodes"}
```

If a destination depends on work performed by a select, return one `next` object on its successful final `select_response`, or on the successful on-call select's stdout. For example:

```jsonl
{"success":true,"message":"Saved","next":{"type":"open","destination":{"plugin":"catalog","context":"episode:7","text":""},"title":"Episode 7"}}
{"success":true,"message":"Saved","next":{"type":"back"}}
```

Open/Back applies to the still-current invoking view. For a dynamic Open the daemon identifies the handler as `next.issuer` in the UI reply. Issuing a scoped destination for **your own** plugin promises the UI can re-query that destination on Back, even after the earlier query was evicted. Opening another plugin makes no promise for it. A scoped destination's `context` is non-secret and subject to the same constraints as an ordinary scoped query.

A baseline UI gets a `query_replace` only if the effective prefix routes to the same plugin, with exactly the same forwarded text and no context. Otherwise the menu omits that Open. A post-select Open/Back cannot be translated to a legacy menu action. If the select itself only makes sense with the continuation, mark the **offered** action, e.g. `{"name":"Continue","requires":["navigation"]}`. Otherwise omit `requires` so the legacy UI still gets the ordinary action, success and message. The raw plugin payload is preserved; see [the UI navigation contract](ui.md#open-and-back).

### Non-secret prompts

A rename action can open `{"plugin":"files","context":"rename:file-17","text":""}`
with `placeholder:"New name"`. Treat each scoped query as validation or preview:
typing `new.txt` can return `[{"id":"new.txt","actions":[{"name":"save","default":true,"no_frecency":true}]}]`.
Only a select of the candidate commits the rename. Back needs no rollback.
`no_frecency` suppresses recording the candidate in the daemon's selection
database. Query text, context, and result IDs are not secret input channels.

## Makefile Specification (Required)

Your Makefile must define the following targets:

```
check-deps:   # verify required toolchain (e.g., python3 or cargo/rustc)
install:      # build and copy files into ~/.local/lib/tarragon/plugins/<plugin-id>/
uninstall:    # remove installed files from the plugin directory
run:          # local quick test (e.g., ./my_plugin tarragon query "Hello")
```

Tarragon's install flow currently:

1. clones the plugin repository,
2. validates `plugin.toml`,
3. runs `make check-deps`,
4. runs `make install`, passing `INSTALL_ROOT` and `PLUGIN_DIR`,
5. verifies the installed plugin directory exists.

Example (Python):

```
PLUGIN_NAME := my_plugin
INSTALL_ROOT := $(HOME)/.local/lib/tarragon/plugins/$(PLUGIN_NAME)

.PHONY: check-deps install uninstall run

check-deps:
	@command -v python3 >/dev/null || { echo "python3 missing" >&2; exit 1; }

install: check-deps
	@mkdir -p $(INSTALL_ROOT)
	@install -m 0644 plugin.toml $(INSTALL_ROOT)/plugin.toml
	@install -m 0755 my_plugin.py $(INSTALL_ROOT)/my_plugin.py

uninstall:
	@rm -rf $(INSTALL_ROOT)

run:
	@python3 my_plugin.py tarragon query "Hello Tarragon"
```

## Logging

- Log initialization and readiness clearly (for persistent plugins, after sending the hello message over the socket).
- Log each request/response pair with the `query_id` to aid tracing.
- For long-running plugins, add periodic heartbeat logs.

## Security & Resource Notes

- Treat input as untrusted; validate/escape as needed in shell calls.
- Avoid expensive initialization in per-request paths; prefer a persistent lifecycle when the plugin is queried often.
- For `on_call` plugins, keep stdout strictly for JSON responses from Tarragon subcommands.
