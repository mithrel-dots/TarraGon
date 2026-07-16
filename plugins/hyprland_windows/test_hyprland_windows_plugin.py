import contextlib
import io
import json
import subprocess
import unittest
from unittest import mock

import hyprland_windows_plugin as plugin


def client(address="0xabc", **overrides):
    return {
        "address": address,
        "mapped": True,
        "class": "firefox",
        "title": "Project notes",
        "workspace": {"id": 2, "name": "2"},
        "focusHistoryID": 0,
        **overrides,
    }


class HyprlandWindowsTest(unittest.TestCase):
    @mock.patch.object(plugin, "query_hyprland")
    def test_lists_all_workspaces_and_hidden_group_members_in_focus_order(self, query):
        query.return_value = [
            client("0x1", focusHistoryID=-1),
            client("0x2", focusHistoryID=2, hidden=True),
            client("0x3", mapped=False),
            client("0x4", workspace={"name": "special:music"}),
        ]
        results = plugin.process("")
        self.assertEqual([item["id"] for item in results], ["0x4", "0x2", "0x1"])
        self.assertIn("special:music", results[0]["description"])
        self.assertEqual(results[0]["actions"][0]["name"], "focus")
        self.assertTrue(results[0]["actions"][0]["default"])
        self.assertEqual(results[0]["actions"][1]["name"], "move_here")

    @mock.patch.object(plugin, "query_hyprland")
    def test_search_combines_title_app_and_workspace_terms(self, query):
        query.return_value = [client(), client("0x2", title="Other page")]
        results = plugin.process("  2 FIREFOX notes  ")
        self.assertEqual([item["id"] for item in results], ["0xabc"])
        self.assertEqual(plugin.process("missing"), [])

    @mock.patch.object(plugin, "query_hyprland")
    def test_untitled_window_uses_initial_class(self, query):
        query.return_value = [client(title="", initialClass="kitty", **{"class": ""})]
        result = plugin.process("kitty")[0]
        self.assertEqual(result["label"], "kitty")
        self.assertEqual(result["icon"], "kitty")

    @mock.patch.object(plugin, "hyprctl")
    def test_move_resolves_focused_monitor_at_selection_time(self, hyprctl):
        hyprctl.side_effect = [
            json.dumps([client()]),
            json.dumps([
                {"focused": False, "activeWorkspace": {"id": 1}},
                {"focused": True, "activeWorkspace": {"id": 7, "name": "code"}},
            ]),
            "ok",
        ]
        success, _ = plugin.run_action("0xabc", "move_here")
        self.assertTrue(success)
        self.assertEqual(hyprctl.call_args_list, [
            mock.call("-j", "clients"),
            mock.call("-j", "monitors"),
            mock.call("dispatch", 'hl.dsp.window.move({window="address:0xabc",workspace="7",follow=false})'),
        ])

    @mock.patch.object(plugin, "query_hyprland")
    def test_open_special_workspace_takes_precedence(self, query):
        for special in (
            {"id": -99, "name": "special:music"},
            {"address": "special:music", "type": "special", "name": "Music"},
        ):
            with self.subTest(special=special):
                query.return_value = [{
                    "focused": True,
                    "activeWorkspace": {"id": 7},
                    "specialWorkspace": special,
                }]
                self.assertEqual(plugin.current_workspace(), "special:music")

    def test_workspace_selectors_support_old_and_new_hyprland(self):
        for workspace, expected in (
            ({"id": 7, "name": "renamed"}, "7"),
            ({"id": -1337, "name": "work"}, "name:work"),
            ({"address": "work", "name": "renamed"}, "work"),
            ({"address": "7", "id": 7, "name": "renamed"}, "7"),
            ({"id": -99, "name": "special:music"}, "special:music"),
        ):
            with self.subTest(workspace=workspace):
                self.assertEqual(plugin.workspace_selector(workspace), expected)

    @mock.patch.object(plugin, "query_hyprland", return_value=[client()])
    @mock.patch.object(plugin, "hyprctl", return_value="ok")
    def test_actions_target_window_address_not_current_focus(self, hyprctl, query):
        for action, expression in (
            ("", 'hl.dsp.focus({window="address:0xabc"})'),
            ("focus", 'hl.dsp.focus({window="address:0xabc"})'),
            ("close", 'hl.dsp.window.close({window="address:0xabc"})'),
            ("toggle_floating", 'hl.dsp.window.float({window="address:0xabc",action="toggle"})'),
        ):
            with self.subTest(action=action):
                self.assertTrue(plugin.run_action("0xabc", action)[0])
                hyprctl.assert_called_with("dispatch", expression)

    @mock.patch.object(plugin, "hyprctl")
    def test_legacy_fallback_only_for_unsupported_dispatcher(self, hyprctl):
        for rejection in ("Invalid dispatcher", plugin.HyprlandError("Invalid dispatcher")):
            with self.subTest(rejection=rejection):
                hyprctl.side_effect = [rejection, "ok"]
                plugin.dispatch_action("move_here", "address:0xabc", "name:work")
                hyprctl.assert_called_with("dispatch", "movetoworkspacesilent", "name:work,address:0xabc")
        hyprctl.reset_mock()
        hyprctl.side_effect = plugin.HyprlandError("hyprctl: command timed out")
        with self.assertRaises(plugin.HyprlandError):
            plugin.dispatch_action("toggle_floating", "address:0xabc")
        self.assertEqual(hyprctl.call_count, 1)

    @mock.patch.object(plugin, "hyprctl", return_value="ok")
    def test_lua_workspace_names_are_escaped_as_data(self, hyprctl):
        plugin.dispatch_action("move_here", "address:0xabc", 'work",follow=true}); evil("\\\n')
        self.assertEqual(hyprctl.call_args.args, (
            "dispatch",
            'hl.dsp.window.move({window="address:0xabc",workspace="work\\034,follow=true}); evil(\\034\\092\\010",follow=false})',
        ))

    @mock.patch.object(plugin, "hyprctl")
    def test_invalid_targets_and_actions_do_not_run_commands(self, hyprctl):
        for address, action in (
            ("active", "close"),
            ("0xabc; dispatch killactive", "close"),
            ("0xabc", "exec"),
        ):
            self.assertFalse(plugin.run_action(address, action)[0])
        hyprctl.assert_not_called()

    @mock.patch.object(plugin, "hyprctl", return_value="[]")
    def test_stale_window_does_not_dispatch(self, hyprctl):
        success, message = plugin.run_action("0xabc", "close")
        self.assertFalse(success)
        self.assertIn("no longer open", message)
        hyprctl.assert_called_once_with("-j", "clients")

    @mock.patch.object(plugin, "hyprctl")
    def test_missing_focused_monitor_does_not_move_window(self, hyprctl):
        hyprctl.side_effect = [json.dumps([client()]), "[]"]
        success, message = plugin.run_action("0xabc", "move_here")
        self.assertFalse(success)
        self.assertIn("focused", message)
        self.assertEqual(hyprctl.call_count, 2)

    @mock.patch.object(plugin.subprocess, "run")
    def test_dispatcher_error_with_zero_exit_status_is_failure(self, run):
        run.side_effect = [
            subprocess.CompletedProcess([], 0, json.dumps([client()]), ""),
            subprocess.CompletedProcess([], 0, "Window not found", ""),
        ]
        self.assertEqual(plugin.run_action("0xabc", "close"), (False, "Window not found"))
        self.assertEqual(run.call_args.args[0], [
            "hyprctl", "dispatch", 'hl.dsp.window.close({window="address:0xabc"})',
        ])
        self.assertNotIn("shell", run.call_args.kwargs)

    @mock.patch.object(plugin.subprocess, "run")
    def test_query_failures_are_reported_on_stderr(self, run):
        for outcome in (
            FileNotFoundError(),
            subprocess.TimeoutExpired("hyprctl", 3),
            subprocess.CompletedProcess([], 1, "", "Cannot connect to Hyprland"),
            subprocess.CompletedProcess([], 0, "invalid JSON", ""),
            subprocess.CompletedProcess([], 0, "{}", ""),
        ):
            with self.subTest(outcome=outcome):
                run.side_effect = [outcome]
                stdout, stderr = io.StringIO(), io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    status = plugin.main(["tarragon", "query", ""])
                self.assertEqual(status, 1)
                self.assertEqual(stdout.getvalue(), "")
                self.assertTrue(stderr.getvalue())

    @mock.patch.object(plugin, "hyprctl")
    def test_cli_query_and_selection_emit_protocol_json(self, hyprctl):
        hyprctl.side_effect = [json.dumps([client()]), json.dumps([client()]), "ok"]
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.assertEqual(plugin.main(["tarragon", "query", "notes"]), 0)
        result_id = json.loads(stdout.getvalue())["results"][0]["id"]
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.assertEqual(plugin.main(["tarragon", "select", result_id, "close"]), 0)
        self.assertTrue(json.loads(stdout.getvalue())["success"])


if __name__ == "__main__":
    unittest.main()
