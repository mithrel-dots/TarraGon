# Hyprland Windows

List and search open Hyprland windows with `@windows`. Add search terms to
match titles, app classes, and workspace names, for example `@windows firefox`
or `@windows terminal 2`. Matching is case-insensitive and accepts multiple
terms in any order. Results include windows across all monitors and special
workspaces, with recently focused windows first.

## Actions

- **focus** (default): focus the selected window on its workspace.
- **move_here** (first alternative): bring the window to the currently focused
  monitor's workspace without switching workspaces. The destination is read
  when the action runs; an open special workspace takes precedence.
- **close**: ask the selected window to close normally. Other windows belonging
  to the same app are not targeted.
- **toggle_floating**: switch the selected window between floating and tiled.

## Install

Requires Python 3.9+ and `hyprctl` on PATH. Tarragon must run in the Hyprland
session with access to its environment (`HYPRLAND_INSTANCE_SIGNATURE` and
`XDG_RUNTIME_DIR`). No Python packages are required.

```sh
just plugin-install hyprland_windows
tarragon plugin restart hyprland_windows
```

The default prefix is `@windows` (using Tarragon's configured prefix symbol).
To change it:

```sh
tarragon plugin config hyprland_windows --prefix win
```

## Development

```sh
make -C plugins/hyprland_windows run
make -C plugins/hyprland_windows test
```

The plugin uses the on-call CLI protocol and reads fresh compositor state for
each query and action. It supports Lua-based Hyprland dispatchers and falls back
to legacy dispatchers on older versions. Tests mock `hyprctl` and do not
manipulate real windows.
