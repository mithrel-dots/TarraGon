# Ports Plugin for Tarragon

Lists listening TCP/UDP sockets and the process behind each one. Answers
"what is using port 3000?" and lets you act on the answer without leaving the
launcher.

Type the prefix `@port` to list every listening port, or `@port 3000`,
`@port node`, `@port udp` to filter. Matching is token-based across the port,
process name, PID, protocol, bound addresses and command line.

## Actions

| Action | Type | Behavior |
| --- | --- | --- |
| `copy_pid` | default | Copy the owning PID to the clipboard |
| `kill` | `keep_open` | Send `SIGTERM` to the process |
| `force_kill` | `keep_open` | Send `SIGKILL` to the process |
| `open_browser` | — | `xdg-open http://localhost:<port>` (TCP only) |
| `copy_port` | — | Copy the port number |
| `copy_command` | — | Copy the process command line |

The kill actions are `keep_open` so the list stays up and you can free several
ports in a row.

Before signalling, the plugin re-scans and confirms the PID still owns that
port. This prevents killing an unrelated process that reused the PID between
the query and the selection. Signalling PID 1 or the plugin itself is refused.

For a socket whose owner is not visible (another user's process, without
root), the entry is still listed with the PID-based actions omitted.

## Requirements

- `python3`
- `ss` (iproute2), with automatic fallback to `lsof`
- A clipboard tool for the copy actions: `wl-copy`, `xclip` or `xsel`

Only sockets whose owning process is visible to the current user report a PID.
Run the daemon as root to see every owner, which is rarely worth it.

## Commands

- `make run`: list all ports as JSON (smoke test).
- `make check-deps`: verify python3 and a socket enumeration tool.
- `make install`: install to `~/.local/lib/tarragon/plugins/ports/`.
- `make uninstall`: remove the installed plugin.

Manual testing:

```sh
./ports_plugin.py tarragon query 3000
./ports_plugin.py tarragon select 'tcp|3000|12345' copy_pid
```

The plugin implements both the persistent NDJSON protocol and the on-call CLI
contract, so `lifecycle_mode` can be switched to `on_call` in
`[plugins.ports]` without code changes.
