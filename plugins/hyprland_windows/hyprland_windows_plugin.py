#!/usr/bin/env python3
"""Search and manage open Hyprland windows through Tarragon's on-call protocol."""

import argparse
import json
import re
import subprocess
import sys


ADDRESS = re.compile(r"0x[0-9a-fA-F]+")
ACTIONS = [
    {"name": "focus", "default": True, "description": "Focus window"},
    {"name": "move_here", "description": "Move to current workspace"},
    {"name": "close", "description": "Close window"},
    {"name": "toggle_floating", "description": "Toggle floating"},
]


class HyprlandError(Exception):
    """An unavailable compositor, invalid response, or failed action."""


def hyprctl(*args: str) -> str:
    try:
        proc = subprocess.run(
            ["hyprctl", *args], capture_output=True, text=True, timeout=3
        )
    except FileNotFoundError as exc:
        raise HyprlandError("hyprctl: not found on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise HyprlandError("hyprctl: command timed out") from exc
    except OSError as exc:
        raise HyprlandError(f"hyprctl: {exc}") from exc
    if proc.returncode != 0:
        raise HyprlandError(proc.stderr.strip() or proc.stdout.strip() or "hyprctl failed")
    return proc.stdout.strip()


def query_hyprland(command: str) -> list:
    try:
        data = json.loads(hyprctl("-j", command))
    except json.JSONDecodeError as exc:
        raise HyprlandError(f"hyprctl {command}: invalid JSON response") from exc
    if not isinstance(data, list) or any(not isinstance(item, dict) for item in data):
        raise HyprlandError(f"hyprctl {command}: expected a list of objects")
    return data


def windows() -> list[dict]:
    # Hidden group members and windows on other/special workspaces are still open.
    return [
        client for client in query_hyprland("clients")
        if client.get("mapped", True) and ADDRESS.fullmatch(client.get("address", ""))
    ]


def process(text: str) -> list[dict]:
    terms = text.casefold().split()
    clients = windows()
    clients.sort(key=lambda client: (
        client.get("focusHistoryID", -1) < 0,
        client.get("focusHistoryID", -1),
        client["address"],
    ))
    results = []
    for client in clients:
        app = client.get("class") or client.get("initialClass") or "Unknown app"
        title = client.get("title") or client.get("initialTitle") or app
        workspace = client.get("workspace") or {}
        workspace_name = workspace.get("name") or workspace.get("address") or str(workspace.get("id", "?"))
        description = f"{app} · Workspace {workspace_name}"
        if client.get("floating"):
            description += " · Floating"
        if client.get("pinned"):
            description += " · Pinned"
        searchable = f"{title} {description} {client.get('initialClass', '')}".casefold()
        if not all(term in searchable for term in terms):
            continue
        results.append({
            "id": client["address"],
            "label": title,
            "description": description,
            "icon": client.get("class") or client.get("initialClass") or "preferences-system-windows",
            "category": "Windows",
            "score": 1 / (len(results) + 1),
            "actions": [dict(action) for action in ACTIONS],
        })
    return results


def workspace_selector(workspace: dict) -> str:
    # New Hyprland versions expose a stable address, independent of display name.
    address = workspace.get("address")
    name = workspace.get("name", "")
    workspace_id = workspace.get("id", 0)
    if address:
        selector = address
    elif name.startswith("special:") or name == "special":
        selector = name
    elif isinstance(workspace_id, int) and workspace_id > 0:
        selector = str(workspace_id)
    elif name:
        selector = f"name:{name}"
    else:
        raise HyprlandError("Cannot determine the current workspace")
    return selector


def current_workspace() -> str:
    for monitor in query_hyprland("monitors"):
        if monitor.get("focused"):
            special = monitor.get("specialWorkspace") or {}
            # An open special workspace takes precedence over the underlying one.
            workspace = special if special.get("name") or special.get("address") else monitor.get("activeWorkspace", {})
            return workspace_selector(workspace)
    raise HyprlandError("Cannot find the focused Hyprland monitor")


def lua_string(value: str) -> str:
    # Lua's decimal escapes also handle controls that JSON would encode as \uXXXX.
    escaped = re.sub(r'[\\"\x00-\x1f\x7f]', lambda match: f"\\{ord(match[0]):03d}", value)
    return f'"{escaped}"'


def dispatch_action(action: str, target: str, workspace: str = "") -> None:
    window = f"window={lua_string(target)}"
    expression = {
        "focus": f"hl.dsp.focus({{{window}}})",
        "close": f"hl.dsp.window.close({{{window}}})",
        "toggle_floating": f'hl.dsp.window.float({{{window},action="toggle"}})',
        "move_here": f"hl.dsp.window.move({{{window},workspace={lua_string(workspace)},follow=false}})",
    }[action]
    try:
        response = hyprctl("dispatch", expression)
    except HyprlandError as exc:
        # Older compositors reject Lua before executing anything. Only retry that
        # specific failure: timeouts or action errors must never execute twice.
        if str(exc).casefold() != "invalid dispatcher":
            raise
        response = str(exc)
    if response.casefold() == "invalid dispatcher":
        dispatcher = {
            "focus": "focuswindow",
            "close": "closewindow",
            "toggle_floating": "togglefloating",
            "move_here": "movetoworkspacesilent",
        }[action]
        argument = target
        if action == "move_here":
            if any(char in workspace for char in (",", "\n", "\r")):
                raise HyprlandError("Workspace name cannot be represented in a legacy dispatcher")
            argument = f"{workspace},{target}"
        response = hyprctl("dispatch", dispatcher, argument)
    # Some versions report dispatcher failures with exit status zero.
    if response != "ok":
        raise HyprlandError(response or "Hyprland did not acknowledge the action")


def run_action(result_id: str, action: str = "") -> tuple[bool, str]:
    action = action or "focus"
    if action not in {item["name"] for item in ACTIONS}:
        return False, f"Unsupported action: {action}"
    if not ADDRESS.fullmatch(result_id):
        return False, "Invalid window address"
    try:
        if not any(client["address"] == result_id for client in windows()):
            return False, "Window is no longer open"
        target = f"address:{result_id}"
        # Resolve at selection time, before doing anything that could change focus.
        workspace = current_workspace() if action == "move_here" else ""
        dispatch_action(action, target, workspace)
    except HyprlandError as exc:
        return False, str(exc)
    return True, "Window action requested"


def print_results(text: str) -> int:
    try:
        results = process(text)
    except HyprlandError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps({"results": results}))
    return 0


def main(argv=None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv[:2] == ["tarragon", "query"]:
        return print_results(" ".join(argv[2:]))
    if argv[:2] == ["tarragon", "select"]:
        if len(argv) not in (3, 4):
            print("usage: tarragon select <window-address> [action]", file=sys.stderr)
            return 2
        success, message = run_action(argv[2], argv[3] if len(argv) == 4 else "")
        print(json.dumps({"success": success, "message": message}))
        if not success:
            print(message, file=sys.stderr)
        return 0 if success else 1

    parser = argparse.ArgumentParser(description="Tarragon Hyprland Windows Plugin")
    parser.add_argument("--once", metavar="TEXT", help="Search once and print JSON")
    args = parser.parse_args(argv)
    if args.once is not None:
        return print_results(args.once)
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
