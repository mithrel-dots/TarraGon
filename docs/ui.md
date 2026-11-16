# Building a UI for Tarragon

Disclaimer: This documentation was created with the help of an AI assistant. It may contain inaccuracies or omissions; please validate details and test in your environment.

The daemon routes UI traffic through embedded NATS. Use the Unix socket adapter for NDJSON, or connect to the broker directly. The application messages are the same on both transports; there is no UI capability handshake.

Each Unix connection or native NATS session **owns** its queries and selects. Only that peer receives their acknowledgements, initial and later snapshots, action outcomes, and replies to its control requests. Two native sessions on the same NATS client connection are distinct owners. `client_id` is an optional, mutable label: reusing it neither takes over another session nor resumes a disconnected one. This is application routing; a trusted broker subscriber can still observe traffic on subjects it subscribes to.

## Native NATS session

1. Enable the [loopback listener](../README.md#core-architecture) and connect without credentials by default. If the daemon has a nonempty `TARRAGON_NATS_TOKEN`, supply that token when connecting.
2. Send an empty NATS request to `tarragon.ui.open` (plugins use `tarragon.plugin.open`). The plain-text reply is a session subject, `tarragon.session.<id>`.
3. Check that the reply starts with `tarragon.session.`, then subscribe to `<session>.out`. Flush the subscription before sending your first query. Publish `{"type":"query","text":"hello"}` to `<session>.in` (a publish, not a NATS request). Each JSON envelope is one NATS message.
4. Read the `ack` on `.out`, then follow `update` messages for its `query_id`. Base64-decode `payload`, parse the JSON, and replace the displayed list. When no entry in `plugins` is `pending`, the current round has finished; a later action can refresh the list.
5. Send an empty message on `.in` every 30 seconds when idle. Send the literal `close` to end the session. On a `close` reply, disconnect, or 90 seconds without input, open a new session before sending more requests.

## Responsibilities of a UI

- Keep one session open, either over NATS or the socket adapter.
- Send queries and read acknowledgements/updates on that same connection.
- Render the latest aggregate snapshot per active query.
- Use per-plugin query state metadata to show which plugins are still pending, returned no entries, or failed.
- `detach` or disconnect releases all queries owned by that peer, including queries without a stream. Other peers' queries are unaffected.

## Unix socket adapter

The daemon resolves the UI socket path at startup, in order:

1. `TARRAGON_UI_SOCKET`, if set and non-empty, is used verbatim.
2. Otherwise, if `XDG_RUNTIME_DIR` is set and non-empty:
   `$XDG_RUNTIME_DIR/tarragon/ui.sock`.
3. Otherwise: `/tmp/tarragon-<euid>/ui.sock` (`<euid>` is the daemon's
   effective UID).

The daemon creates a missing parent directory with mode `0700`. An existing
parent must already be private and owned by the daemon user. The daemon removes
stale sockets and refuses to replace live ones. UI clients should
implement the same resolution order (or read `TARRAGON_UI_SOCKET` directly)
rather than hardcoding a path.

### Socket query flow

1) Connect to the resolved UI Unix socket path (see above).
2) For each input:
   - Write NDJSON line: `{ "type": "query", "client_id": "<id>", "text": "<input>" }\n`
   - Read NDJSON line: `{ "type": "ack", "query_id": "..." }\n`
   - Read NDJSON update lines: `{ "type": "update", "query_id": "...", "payload": "<base64>" }\n`
   - Base64-decode `payload` and parse the result as JSON to get the aggregate snapshot.
   - Replace your displayed snapshot with the latest one for that `query_id`.
3) On exit, write NDJSON line `{ "type": "detach", "client_id": "<id>" }\n` (best-effort) to let the daemon purge memory.

For a search box that only displays the newest input, send a stable, nonempty
`query_stream` with each query, e.g. `{"type":"query","query_stream":"search","text":"tar"}`.
Each query still gets an ack. The new query supersedes the previous query in
that stream on the same connection. Ignore already queued updates for old query
IDs; superseded queries may never receive a final snapshot. Omit `query_stream`
for searches that must all complete, or use distinct streams for distinct inputs.

### Scoped queries

`{"type":"query","plugin":"catalog","context":"show:42","text":""}` addresses
one enabled plugin by ID, even if it has no prefix. Text is literal: empty
strings, surrounding spaces, and another plugin's prefix are passed through.
An unknown or disabled target still gets an `ack` and a snapshot with that
plugin in `error`; the daemon doesn't search other plugins.

`context` is optional, non-secret UTF-8 (at most 4096 bytes, no NUL). A query
with invalid context or context without a target gets `error` before dispatch.
A select on an owned source query uses its recorded context for that producer.
For a direct select without that source, supply the plugin's context explicitly.

### Ending one query

When a view closes, release its query on the session that created it:

```text
UI → daemon  {"type":"end","query_id":"q2"}
daemon → UI  {"type":"ok","query_id":"q2"}
```

The session and its other queries stay open. Repeating `end`, or naming an unknown or foreign ID, gets the same `ok`. Discard already queued updates for a closed view. Ending a query cancels its pending queries and refreshes; accepted selects still run and can return their usual outcomes.

Stream supersession and history eviction also end a query. `detach`, Unix disconnect, native `close`, or native idle expiry releases **all** queries on that peer. The 90-second native timeout measures time without session input; it is not a deadline for a query or select.

### Watching a live view

Send `{"type":"query","plugin":"logs","text":"","watch":true}` to reserve a view. The daemon sends the usual `ack` and full `update` snapshots. Continue reading after all plugins have answered: a live producer can replace its rows on the same `query_id`. An older UI that omits `watch` can still receive these snapshots until ordinary history evicts its query.

A reserved view survives other clients' searches, including while the first replies are pending. The limits are **4 watches and 4 MiB per owner**, **32 watches and 32 MiB daemon-wide**, separate from ordinary history. The byte charge includes retained query strings and features, raw producer data, and normalized rows and actions. Completing the initial round with no live producers returns the query to ordinary history. Leaving a view should send `{"type":"end","query_id":"q3"}`. Supersession, detach, and session loss also release reservations.

Check `watch_error` in the **decoded snapshot**. If capacity is unavailable, the daemon acknowledges the query, then sends an empty snapshot explaining the refusal; no plugin is invoked. If a replacement would exceed the byte budget, the final snapshot keeps the previous valid rows and explains the limit. A watch still waiting for initial replies ends after five seconds unless a producer has confirmed a live feed. After that deadline, silent producers cannot keep the reservation when the last live feed stops. Other watches are unaffected. The separate 8 MiB **encoded envelope** limit still yields a query-scoped `error` instead of the oversized update; it does not disconnect the session.

## Reading snapshots and control replies

Multiple queries can run concurrently on one session. Read continuously and dispatch messages by `type`. A snapshot is the whole list so far. The aggregate publisher can combine multiple plugin responses, and the UI writer can replace a queued snapshot with a newer one for the same `query_id`. Replacement only happens between control messages or flush barriers: a terminal reply is never coalesced, and a later snapshot cannot overtake it. Use the `plugins` map to detect pending/empty/error states instead of counting updates; `started_at_unix_ms` gives the query's start time.

For a control request, wait for the matching type: `status` for `status`, `restart_response` for `restart`, `reload_response` for `reload`. Keep reading with the same reader and the original deadline until that reply, an error, or EOF; an intervening `update` is not the reply. A one-shot Unix client can close its write half and keep reading: queued control replies and terminal outcomes for accepted selects arrive before EOF.

Keep reading while rendering. If the bounded writer queue fills or a write times out, the session closes. After a disconnect or NATS slow-consumer error, open a new session; previous snapshots and action outcomes are not replayed. Don't automatically rerun a select or assume a plugin query is safe to repeat.

## Selecting a result

Send the row's `plugin` (its producer ID), `result_id`, the chosen `action` name if any, and normally the displayed `query_id`. Older callers may use `id` for `result_id`, or `text` if neither ID is present. An ordinary select can omit `query_id` or refer to an evicted list: the handler may still act or report that the row is stale. Refresh requires an owned source row.

For example, this UI chooses its own `request_id` and opts into progress on this select. The daemon assigns a fresh `action_id` for each invocation, even for two clicks on the same row:

```text
UI → daemon  {"type":"select","request_id":"r7","query_id":"q1","plugin":"demo","result_id":"one","action":"mark","ui_features":["action_progress"]}
daemon → UI  {"type":"ok","request_id":"r7","action_id":"a7"}
daemon → UI  {"type":"action_progress","request_id":"r7","action_id":"a7","message":"Applying"}
daemon → UI  {"type":"select_response","request_id":"r7","action_id":"a7","success":true,"message":"Marked"}
```

`ok` means received, even if validation will fail. `action_progress` is optional and non-final; it is forwarded only to the owner that requested it, if the handler sends it. Wait for exactly one `select_response` while the connection remains writable. `request_id` and `ui_features` can both be omitted. A baseline UI can keep one select outstanding, ignore additional fields, and use the next `select_response` as its answer. To overlap selects, use `request_id` to match each `ok`, then track its `action_id`. Other queries and controls can proceed while an on-call action runs.

| Terminal outcome | What the UI can tell the user |
| --- | --- |
| Handler reports `success:true` or `success:false` | Show its `message` when present. A reported failure does not rule out partial effects. |
| `success:false, code:"not_run"` | The action was never dispatched (for example, an invalid target or an expired queue entry). A new user-initiated attempt cannot duplicate this invocation. |
| `success:false, code:"completion_unknown"` | The handler may already have acted. Show the explanation; never retry automatically. An on-call process can finish after the reply deadline without sending a second outcome. |

Superseding a query or closing its view doesn't cancel an accepted action. Disconnecting loses the reply; a new connection with the same `client_id` cannot collect it.

For a non-secret prompt, submit edits as scoped queries and send a select only
on explicit confirmation. Back leaves the view without committing. Echo
`no_frecency:true` on the select when the offered action requested it: this
suppresses daemon selection history even if the source query has been evicted.
The UI manages its own input history separately.

Keep `request_id` short. If a reply exceeds the 8 MiB envelope limit, the daemon keeps the terminal outcome and `action_id`, shortens the message, and drops the navigation continuation and then `request_id` only if it still cannot fit.

### Refresh after a select

A plugin can ask the daemon to refresh the selected row's **producer**. The daemon queues the terminal answer first, then, if eligible, reissues only the original producer's query with the same resolved text and `query_id`. The UI needn't send another query or interpret a `next` field. Render the ordinary full `update` for the original list. Other producers' rows remain; the refreshed rows stay visible until replaced. An empty result clears that producer's rows, while a transport error can leave its last rows visible with state `error`.

No refresh is run for a missing, superseded, foreign or otherwise unavailable source, a disabled producer, or a replay without the producer's consent. A handler requesting its own results consents for that refresh; a producer can also declare `replay` in its manifest. Skipping refresh does not change the action outcome, and even a failed action can request a refresh to show partial changes. For example, `@plugins` keeps its filter when **enable**, **disable** or **restart** refreshes the overview.

### Secondary actions from other plugins

Include `ui_features:["action_offers"]` on the **query** if your UI can render secondary choices. Read actions from the normalized `list`. For example:

```json
{"id":"f17","label":"Sky","plugin":"file_finder","ref":{"type":"file","value":"/pictures/sky.png","mime":"image/png"},"actions":[{"name":"open","default":true},{"name":"Set as wallpaper","secondary":true,"offer_id":"P0v3u5Id"}]}
```

Keep the producer's default. A secondary choice can be the only visible action: expose it explicitly, never as the default. On selection, echo the `offer_id`, name, displayed `query_id`, producer and original row ID:

```json
{"type":"select","query_id":"q4","plugin":"file_finder","result_id":"f17","action":"Set as wallpaper","offer_id":"P0v3u5Id"}
```

`offer_id` identifies a choice; the `ok` receipt still assigns a new `action_id` per invocation. Never substitute `ref.value` or send a select directly to the handler. Offers apply only to the owner and row that received them. A changed reference, disabled handler, ended query or unknown token can yield `not_run`.

A baseline query instead receives an appended name such as `Set as wallpaper [via wallpaper/set #<token>]`; echo the entire name unchanged, including its opaque suffix. The daemon offers these only after an existing native choice. Duplicate row IDs and colliding action names cannot authorize an injected action. Stale aliases never fall back to a producer select.

### Query Replacement Actions

An action may use `type: "query_replace"` and provide a `query`:

```json
{
  "name": "Episodes",
  "type": "query_replace",
  "query": "@anime episodes 154587"
}
```

When the user invokes this action, replace the input text with `query` and
send it as a normal query request. Keep the socket and UI open; process the
acknowledgement and updates exactly as for any other query. Do not send a
select request for this action.

An action with `type: "keep_open"` is a normal select action that should not
dismiss the UI after successful execution:

```json
{
  "name": "Copy",
  "type": "keep_open"
}
```

Actions without a type retain the default behavior of dismissing the UI after
successful selection. Keep the launcher open on failure to show the outcome.
`query_replace` always keeps the UI open.

### Open and Back

Send `ui_features:["navigation"]` on each query whose view supports Open and Back. Render the actions in the snapshot's **`list`**; `results[plugin].data` is the producer's original payload, including choices that might be unavailable to this UI.

For a row action with `type:"open"`, save the current view on a local stack, show the opener's optional `title` and `placeholder`, then submit the destination as a **new** scoped query. No select is involved:

```text
row action   {"name":"Episodes","type":"open","destination":{"plugin":"catalog","context":"show:42","text":""},"title":"Episodes","placeholder":"Filter episodes"}
UI → daemon  {"type":"query","plugin":"catalog","context":"show:42","text":"","ui_features":["navigation"]}
```

A normal select can also return a destination chosen by the handler, or ask to go Back:

```jsonl
{"type":"select_response","action_id":"a7","success":true,"next":{"type":"open","issuer":"catalog","destination":{"plugin":"catalog","context":"episode:7","text":""},"title":"Episode 7"}}
{"type":"select_response","action_id":"a8","success":true,"next":{"type":"back"}}
```

Associate each select's `action_id` with the view that invoked it. On success, apply its Open/Back **before** any normal dismiss-on-success behavior and only while that invoking view is still current. A late outcome remains the outcome of the original action, even if the user has moved on. Navigation support is remembered from the owned query. A capable UI can also send `ui_features:["navigation"]` on a direct select or when selecting a cached row whose query may have been evicted.

Save the destination, issuer, current input, selected `(plugin, result_id)`, scroll position and cached rows. For a row Open the issuer is `row.plugin`; for a select's Open use `next.issuer`, which the daemon sets to the actual handler. Back at the root does nothing. Otherwise restore the previous view from cache, including its selection and scroll. If a new query is needed, only automatically submit a **scoped** destination whose target issued it or whose enabled target declares `replay`. A plugin that opens another plugin (including Open in `@plugins`) does not grant that target's consent. With no cache or replay promise, restore the input and wait for user input; never automatically reissue an arbitrary user-typed legacy query. A replay gets a **new** `query_id`; [producer refresh](#refresh-after-a-select) keeps the original ID.

Without `navigation`, the daemon offers `query_replace` for a known Open only when the target's effective prefix routes to that same enabled plugin and forwards exactly the destination text, with no context to lose. For example, `@plugins` can offer `query:"@wp "` to a baseline client, but a prefixless plugin or the loser of a prefix collision has **details** instead. A navigation-capable client can open either directly by ID. A select marked `requires:["navigation"]` is hidden from a baseline client; an ordinary select still receives its normal outcome even when the handler recommends navigation. Do not send a substitute select for a hidden action or for Open.

### Lifecycle-aware availability

When consuming daemon status metadata:

- treat `connected` as current IPC attachment state (primarily relevant for persistent lifecycles),
- do not treat `connected=false` as unavailable for enabled `on_call` plugins.

`on_call` plugins are expected to be dispatchable without a persistent connection.
For a select, an enabled `on_demand_persistent` plugin can be started and awaited within the action deadline. A disconnected `daemon` handler has no such startup route.

## Message Shapes

- Query (UI → Daemon):
  - `{ "type": "query", "client_id": "<string>", "text": "<input>" }`
- Detach (UI → Daemon):
  - `{ "type": "detach", "client_id": "<string>" }`
- Ack (Daemon → UI):
  - `{ "type": "ack", "query_id": "<id>" }`
- Update (Daemon → UI):
  - `{ "type": "update", "query_id": "<id>", "payload": "<base64-encoded AggregateSnapshot JSON>" }`
  - `payload` is Tarragon's Go `[]byte` field, which `encoding/json` marshals as a
    standard base64 string, **not** an inline JSON object. Base64-decode it
    first, then parse the decoded bytes as JSON to get the AggregateSnapshot.
- Select and action replies: see the [select exchange and terminal outcomes](#selecting-a-result).
- Error (Daemon → UI): `{"type":"error","query_id":"<id>","error":"..."}`. An encoded envelope, including base64, can be at most 8 MiB. If a snapshot exceeds that limit, the UI receives this error instead of that update; the session stays open.
- AggregateSnapshot (after base64-decoding `payload`):
  - `{"query_id":"<id>","input":"<input>","started_at_unix_ms":<epoch-ms>,"results":{"<plugin>":{"elapsed_ms":<float>,"data":<plugin JSON>}, ...},"plugins":{"<plugin>":{"state":"pending|done|empty|error","count":<int>,"elapsed_ms":<float>,"error":"<message>"}},"list":[...]}`

`results[plugin].data` contains the raw plugin payload. Render `list` for the merged, ranked rows. In `global` mode, sort order is descending blended score, then ascending plugin ID, with producer order retained for ties within a plugin. In `grouped` mode, groups rank by their best blended score and then plugin ID; each group's rows keep producer order regardless of score.

For an explicitly addressed plugin or resolved prefix, a producer can return
`ordered:true` alongside its results. Render the resulting `list` in sequence;
its scores come directly from the plugin.

Plugin state meanings:
- `pending`: the plugin was selected for this query and has not responded yet.
- `done`: the plugin responded with one or more normalized result entries.
- `empty`: the plugin responded successfully but produced no normalized result entries.
- `error`: the plugin failed, timed out, or returned an error payload.

See [Query Replacement Actions](#query-replacement-actions) and [Open and Back](#open-and-back) for action fields and UI behavior.

## Example: Python

```python
import base64, json, os, socket, uuid


def resolve_ui_socket_path() -> str:
    if override := os.environ.get("TARRAGON_UI_SOCKET"):
        return override
    if runtime_dir := os.environ.get("XDG_RUNTIME_DIR"):
        return os.path.join(runtime_dir, "tarragon", "ui.sock")
    return f"/tmp/tarragon-{os.geteuid()}/ui.sock"


s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(resolve_ui_socket_path())
f = s.makefile('r')

client_id = f"cli-{uuid.uuid4()}"

query = {"type": "query", "client_id": client_id, "text": "hello"}
s.sendall(json.dumps(query).encode() + b"\n")

while True:
    line = f.readline()
    if not line:
        break
    msg = json.loads(line)
    if msg["type"] == "ack":
        print("query_id:", msg["query_id"])
    elif msg["type"] == "update":
        # payload is base64-encoded JSON; decode before parsing.
        snapshot = json.loads(base64.b64decode(msg["payload"]))
        print("snapshot:", json.dumps(snapshot, indent=2))
```

## Example: Go

```
func resolveUISocketPath() string {
	if v := os.Getenv("TARRAGON_UI_SOCKET"); v != "" {
		return v
	}
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, "tarragon", "ui.sock")
	}
	return fmt.Sprintf("/tmp/tarragon-%d/ui.sock", os.Geteuid())
}

conn, _ := net.Dial("unix", resolveUISocketPath())
scanner := bufio.NewScanner(conn)
scanner.Buffer(make([]byte, 64*1024), (8<<20)+1)

query, _ := json.Marshal(map[string]any{"type":"query","client_id":"cli-1","text":"hello"})
conn.Write(append(query, '\n'))

scanner.Scan()
var a struct{ QueryID string `json:"query_id"` }
json.Unmarshal(scanner.Bytes(), &a)

scanner.Scan()
// Payload is []byte: encoding/json base64-decodes it automatically because
// the wire value is a base64 string, not an inline JSON object.
var upd struct{ Payload []byte `json:"payload"` }
json.Unmarshal(scanner.Bytes(), &upd)

var snapshot map[string]any
json.Unmarshal(upd.Payload, &snapshot)
```
