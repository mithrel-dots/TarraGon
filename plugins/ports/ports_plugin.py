#!/usr/bin/env python3
"""
Ports plugin for Tarragon.

Lists listening TCP/UDP sockets together with the process behind each one.
The default action copies the owning PID; alternate actions terminate the
process, open the port in a browser, or copy the port and command line.

Socket enumeration shells out to `ss` (iproute2) and falls back to `lsof`.
Neither is asked to resolve names or ports, so scans stay fast and offline.
"""

import argparse
import json
import logging
import os
import re
import shutil
import signal
import socket as sock_mod
import subprocess
import sys
import time

PLUGIN_NAME = os.environ.get("TARRAGON_PLUGIN_NAME", "ports")
logging.basicConfig(
    level=logging.INFO,
    format=f"[PLUGIN: {PLUGIN_NAME}] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)

MAX_RESULTS = 250
SCAN_TIMEOUT = 5
CMDLINE_MAX = 90

# `ss` commonly lives in an sbin directory that is absent from a desktop
# session PATH, so probe the usual locations before giving up on it.
SS_CANDIDATES = ("ss", "/usr/bin/ss", "/usr/sbin/ss", "/sbin/ss", "/bin/ss")
LSOF_CANDIDATES = ("lsof", "/usr/bin/lsof", "/usr/sbin/lsof", "/sbin/lsof")

# users:(("sshd",pid=1234,fd=3),("sshd",pid=1235,fd=4))
SS_PROC_RE = re.compile(r'\(\("([^"]*)",pid=(\d+),fd=\d+\)')

# Ports that are worth offering an "open in browser" action for. Any port is
# allowed too; this list only drives how confident the description is.
WILDCARD_ADDRS = {"0.0.0.0", "[::]", "*", "::"}
LOOPBACK_PREFIXES = ("127.", "::1", "[::1]")


class Entry:
    """One listening socket, grouped by (proto, port, pid)."""

    __slots__ = ("proto", "port", "pid", "name", "addrs", "cmdline")

    def __init__(self, proto, port, pid, name):
        self.proto = proto
        self.port = port
        self.pid = pid
        self.name = name
        self.addrs = []
        self.cmdline = ""

    @property
    def key(self):
        return (self.proto, self.port, self.pid)

    @property
    def result_id(self):
        return f"{self.proto}|{self.port}|{self.pid if self.pid else '-'}"


def _tool(candidates):
    for candidate in candidates:
        if os.sep in candidate:
            if os.access(candidate, os.X_OK):
                return candidate
            continue
        found = shutil.which(candidate)
        if found:
            return found
    return None


def _run(args):
    return subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=SCAN_TIMEOUT,
    )


def _split_host_port(value):
    """Split an `addr:port` token, tolerating IPv6 and %scope suffixes."""
    if ":" not in value:
        return value, None
    addr, _, port = value.rpartition(":")
    if not port.isdigit():
        return value, None
    return addr or "*", int(port)


def _read_cmdline(pid):
    if not pid:
        return ""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as handle:
            raw = handle.read()
    except OSError:
        return ""
    parts = [p for p in raw.split(b"\x00") if p]
    text = " ".join(p.decode("utf-8", "replace") for p in parts)
    if not text.strip():
        # Kernel threads and some processes expose an empty cmdline.
        try:
            with open(f"/proc/{pid}/comm", "r") as handle:
                text = handle.read()
        except OSError:
            return ""
    return _sanitize(text)


def _sanitize(text):
    """Flatten a command line into one renderable line.

    Arguments routinely contain newlines (`python3 -c '...'`) and occasionally
    other control characters, which would otherwise break UI rendering.
    """
    cleaned = "".join(ch if ch.isprintable() else " " for ch in text)
    return " ".join(cleaned.split())


def _add(entries, proto, addr, port, pid, name):
    if port is None:
        return
    entry = Entry(proto, port, pid, name or "")
    existing = entries.get(entry.key)
    if existing is None:
        entries[entry.key] = entry
        existing = entry
    if not existing.name and name:
        existing.name = name
    if addr and addr not in existing.addrs:
        existing.addrs.append(addr)


def _scan_ss(binary):
    """Parse `ss -tulpn`. Rows without a users:(...) column are unowned."""
    proc = _run([binary, "-tulpn"])
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "ss failed")

    entries = {}
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("Netid"):
            continue
        fields = line.split(None, 6)
        if len(fields) < 6:
            continue
        proto = fields[0].lower()
        if proto not in ("tcp", "udp"):
            continue
        addr, port = _split_host_port(fields[4])
        if port is None:
            continue
        process_col = fields[6] if len(fields) > 6 else ""
        matches = SS_PROC_RE.findall(process_col)
        if matches:
            for name, pid in matches:
                _add(entries, proto, addr, port, int(pid), name)
        else:
            _add(entries, proto, addr, port, None, "")
    return entries


def _scan_lsof_stream(binary, extra, entries):
    """Parse lsof field output (-F) into entries."""
    proc = _run([binary, "-nP", "-F", "pcPnt", *extra])
    # lsof exits non-zero when nothing matches; only treat it as fatal when it
    # produced no usable output at all.
    if proc.returncode != 0 and not proc.stdout.strip():
        return

    pid = None
    name = ""
    proto = ""
    addr_port = ""

    def flush():
        if not proto or not addr_port or "->" in addr_port:
            return
        addr, port = _split_host_port(addr_port)
        _add(entries, proto.lower(), addr, port, pid, name)

    for line in proc.stdout.splitlines():
        if not line:
            continue
        tag, value = line[0], line[1:]
        if tag == "p":
            flush()
            proto, addr_port = "", ""
            pid = int(value) if value.isdigit() else None
        elif tag == "c":
            name = value
        elif tag == "f":
            flush()
            proto, addr_port = "", ""
        elif tag == "P":
            proto = value
        elif tag == "n":
            addr_port = value
    flush()


def _scan_lsof(binary):
    entries = {}
    _scan_lsof_stream(binary, ["-iTCP", "-sTCP:LISTEN"], entries)
    _scan_lsof_stream(binary, ["-iUDP"], entries)
    return entries


def scan():
    """Return listening sockets, newest snapshot each call.

    Raises RuntimeError when no supported enumeration tool is available.
    """
    ss_bin = _tool(SS_CANDIDATES)
    if ss_bin:
        try:
            entries = _scan_ss(ss_bin)
        except Exception as err:
            logger.warning("ss scan failed (%s); trying lsof", err)
            entries = None
        if entries:
            return _finish(entries)

    lsof_bin = _tool(LSOF_CANDIDATES)
    if lsof_bin:
        return _finish(_scan_lsof(lsof_bin))

    raise RuntimeError("neither 'ss' nor 'lsof' is available on PATH")


def _finish(entries):
    out = list(entries.values())
    for entry in out:
        entry.addrs.sort()
        entry.cmdline = _read_cmdline(entry.pid)
        if not entry.name and entry.cmdline:
            entry.name = os.path.basename(entry.cmdline.split(" ", 1)[0])
    out.sort(key=lambda e: (e.port, e.proto, e.pid or 0))
    return out


def _exposure(entry):
    if any(a in WILDCARD_ADDRS for a in entry.addrs):
        return "all interfaces"
    if entry.addrs and all(a.startswith(LOOPBACK_PREFIXES) for a in entry.addrs):
        return "localhost"
    return ", ".join(entry.addrs) or "unknown"


def _describe(entry):
    parts = [entry.proto, _exposure(entry)]
    if entry.pid:
        parts.append(f"pid {entry.pid}")
    else:
        parts.append("pid unavailable (run as root)")
    if entry.cmdline:
        cmd = entry.cmdline
        if len(cmd) > CMDLINE_MAX:
            cmd = cmd[: CMDLINE_MAX - 1] + "\u2026"
        parts.append(cmd)
    return " \u00b7 ".join(parts)


def _actions_for(entry):
    """Default action first; the UI treats the rest as alternates."""
    actions = []
    if entry.pid:
        actions.append(
            {
                "name": "copy_pid",
                "default": True,
                "description": f"Copy PID {entry.pid}",
            }
        )
        actions.append(
            {
                "name": "kill",
                "type": "keep_open",
                "description": "Kill process (SIGTERM)",
            }
        )
        actions.append(
            {
                "name": "force_kill",
                "type": "keep_open",
                "description": "Force kill process (SIGKILL)",
            }
        )
    if entry.proto == "tcp":
        actions.append(
            {
                "name": "open_browser",
                "default": not entry.pid,
                "description": f"Open http://localhost:{entry.port}",
            }
        )
    actions.append(
        {
            "name": "copy_port",
            "default": not actions,
            "description": f"Copy port {entry.port}",
        }
    )
    if entry.cmdline:
        actions.append(
            {
                "name": "copy_command",
                "description": "Copy command line",
            }
        )
    return actions


def _result_for(entry, score):
    label = f"{entry.port}  {entry.name}" if entry.name else str(entry.port)
    return {
        "id": entry.result_id,
        "label": label,
        "description": _describe(entry),
        "icon": "network-server",
        "category": "Ports",
        "score": score,
        "actions": _actions_for(entry),
    }


def _haystack(entry):
    return " ".join(
        (
            str(entry.port),
            entry.name,
            entry.proto,
            str(entry.pid or ""),
            " ".join(entry.addrs),
            entry.cmdline,
        )
    ).lower()


def _score(entry, query):
    port = str(entry.port)
    name = entry.name.lower()
    if query == port:
        return 1.0
    if port.startswith(query):
        return 0.9
    if name and name.startswith(query):
        return 0.8
    if query in port:
        return 0.7
    if query in name:
        return 0.6
    return 0.4


def process(text: str):
    entries = scan()
    query = text.strip().lower()

    if not query:
        # Bare prefix (e.g. "@port"): show everything immediately, ordered by
        # port, with a gently decreasing score so the order survives ranking
        # while still leaving room for frecency to promote favourites.
        return [
            _result_for(entry, max(0.1, 1.0 - index * 0.001))
            for index, entry in enumerate(entries[:MAX_RESULTS])
        ]

    tokens = query.split()
    matched = []
    for entry in entries:
        haystack = _haystack(entry)
        if all(token in haystack for token in tokens):
            matched.append((entry, _score(entry, query)))

    matched.sort(key=lambda pair: (-pair[1], pair[0].port, pair[0].pid or 0))
    return [_result_for(entry, score) for entry, score in matched[:MAX_RESULTS]]


def _parse_result_id(result_id):
    parts = result_id.split("|")
    if len(parts) != 3:
        raise ValueError(f"malformed result id: {result_id}")
    proto, port, pid = parts
    if not port.isdigit():
        raise ValueError(f"malformed port in result id: {result_id}")
    return proto, int(port), (int(pid) if pid.isdigit() else None)


def _copy_to_clipboard(value: str):
    # stdout/stderr go to /dev/null on purpose: wl-copy and xclip fork a
    # background clipboard server that would otherwise inherit and hold this
    # plugin's descriptors open, and in on-call mode stdout must stay strictly
    # JSON.
    for cmd in (["wl-copy"], ["xclip", "-selection", "clipboard"], ["xsel", "-ib"]):
        try:
            proc = subprocess.run(
                cmd,
                input=value,
                text=True,
                timeout=5,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError:
            continue
        except subprocess.TimeoutExpired:
            return False, f"{cmd[0]}: timed out"
        if proc.returncode == 0:
            return True, f"Copied {value}"
    return False, "no clipboard tool found (install wl-clipboard, xclip or xsel)"


def _find_live(proto, port, pid):
    """Re-scan and confirm the socket is still owned by pid.

    Guards against killing an unrelated process that reused the PID between
    the query and the selection.
    """
    for entry in scan():
        if entry.proto == proto and entry.port == port and entry.pid == pid:
            return entry
    return None


def _kill(proto, port, pid, sig):
    if pid is None:
        return False, "no PID available for this port"
    if pid == 1:
        return False, "refusing to signal pid 1"
    if pid == os.getpid():
        return False, "refusing to signal the ports plugin itself"

    entry = _find_live(proto, port, pid)
    if entry is None:
        return False, f"pid {pid} no longer listens on {proto}/{port}"

    label = entry.name or str(pid)
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        return False, f"pid {pid} no longer exists"
    except PermissionError:
        return False, f"not permitted to signal pid {pid} ({label})"
    except OSError as err:
        return False, f"failed to signal pid {pid}: {err}"

    name = "SIGKILL" if sig == signal.SIGKILL else "SIGTERM"
    return True, f"Sent {name} to {label} (pid {pid}) on {proto}/{port}"


def _open_browser(port):
    url = f"http://localhost:{port}"
    try:
        subprocess.Popen(
            ["xdg-open", url],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        return False, "xdg-open: not found on PATH"
    except OSError as err:
        return False, f"failed to open {url}: {err}"
    return True, f"Opened {url}"


def run_action(result_id: str, action: str):
    try:
        proto, port, pid = _parse_result_id(result_id)
    except ValueError as err:
        return False, str(err)

    action = action or "copy_pid"

    if action == "copy_pid":
        if pid is None:
            return False, "no PID available for this port"
        return _copy_to_clipboard(str(pid))
    if action == "copy_port":
        return _copy_to_clipboard(str(port))
    if action == "kill":
        return _kill(proto, port, pid, signal.SIGTERM)
    if action == "force_kill":
        return _kill(proto, port, pid, signal.SIGKILL)
    if action == "open_browser":
        return _open_browser(port)
    if action == "copy_command":
        cmdline = _read_cmdline(pid)
        if not cmdline:
            return False, "no command line available for this process"
        return _copy_to_clipboard(cmdline)
    return False, f"unsupported action: {action}"


def _send(sock, payload):
    sock.sendall(json.dumps(payload).encode() + b"\n")


def run_daemon():
    logger.info("initializing")
    endpoint = os.environ.get("TARRAGON_PLUGINS_ENDPOINT")

    if not endpoint:
        logger.info("started successfully; idle mode")
        signal.pause()
        return 0

    s = sock_mod.socket(sock_mod.AF_UNIX, sock_mod.SOCK_STREAM)
    for attempt in range(20):
        try:
            s.connect(endpoint)
            break
        except (ConnectionRefusedError, FileNotFoundError):
            if attempt == 19:
                logger.error("could not connect to %s after retries", endpoint)
                return 1
            time.sleep(0.1)

    state = {"stop": False}

    def _handle_stop(_signum, _frame):
        # Shut the socket down so the blocking readline() below returns at
        # once. Setting a flag alone is not enough: Python restarts the
        # interrupted read (PEP 475) and the plugin would hang until SIGKILL.
        state["stop"] = True
        try:
            s.shutdown(sock_mod.SHUT_RDWR)
        except OSError:
            pass

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    _send(s, {"type": "hello", "name": PLUGIN_NAME})
    logger.info("connected to %s", endpoint)

    f = s.makefile("r")
    while not state["stop"]:
        try:
            line = f.readline()
            if not line:
                break
            msg = json.loads(line)
        except (OSError, ValueError) as err:
            if state["stop"]:
                break
            logger.error("recv error: %s", err)
            break

        qid = msg.get("query_id", "")
        typ = msg.get("type")

        if typ == "request":
            text = msg.get("text", "")
            logger.info("request qid=%s: %s", qid, text)
            try:
                data = {"results": process(text)}
            except Exception as err:
                logger.error("scan failed: %s", err)
                data = {"error": str(err)}
            _send(s, {"type": "response", "query_id": qid, "data": data})
            logger.info("response sent qid=%s", qid)
        elif typ == "select":
            result_id = msg.get("result_id", "")
            action = msg.get("action", "")
            logger.info("select qid=%s result_id=%s action=%s", qid, result_id, action)
            try:
                success, message = run_action(result_id, action)
            except Exception as err:
                success, message = False, str(err)
            _send(
                s,
                {"type": "select_response", "success": success, "message": message},
            )

    logger.info("exiting")
    return 0


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    # On-call CLI contract, also used by `make run` for smoke tests.
    if argv[:2] == ["tarragon", "query"]:
        text = " ".join(argv[2:])
        try:
            print(json.dumps({"results": process(text)}))
        except Exception as err:
            print(json.dumps({"error": str(err)}))
            return 1
        return 0

    if argv[:2] == ["tarragon", "select"]:
        if len(argv) < 3:
            print("missing result id", file=sys.stderr)
            return 2
        action = argv[3] if len(argv) > 3 else ""
        success, message = run_action(argv[2], action)
        print(json.dumps({"success": success, "message": message}))
        if not success:
            print(message, file=sys.stderr)
        return 0 if success else 1

    parser = argparse.ArgumentParser(description="Tarragon Ports Plugin")
    parser.add_argument("--once", metavar="TEXT", help="Process once and print JSON")
    args = parser.parse_args(argv)

    if args.once is not None:
        print(json.dumps({"results": process(args.once)}))
        return 0

    return run_daemon()


if __name__ == "__main__":
    raise SystemExit(main())
