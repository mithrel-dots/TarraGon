package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

// Drain both pipes through cmd.Wait even after the retained prefix is full.
type limitedOutput struct {
	limit     int
	data      []byte
	truncated bool
}

func (b *limitedOutput) Write(p []byte) (int, error) {
	n := min(len(p), b.limit-len(b.data))
	b.data = append(b.data, p[:n]...)
	if n < len(p) {
		b.truncated = true
	}
	return len(p), nil
}

func (b *limitedOutput) reply() ([]byte, error) {
	out := bytes.TrimSpace(b.data)
	if b.truncated || len(out) > wire.MaxMessageSize {
		return nil, wire.ErrMessageTooLarge
	}
	return out, nil
}

func (b *limitedOutput) diagnostic() string {
	if b.truncated {
		return string(b.data) + " [truncated]"
	}
	return string(b.data)
}

func captureOnCallOutput(cmd *exec.Cmd) (stdout, stderr *limitedOutput) {
	stdout = &limitedOutput{limit: wire.MaxMessageSize + 1} // optional final newline
	stderr = &limitedOutput{limit: 4096}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	return stdout, stderr
}

// onCallEnv gives ephemeral plugins the same identity and prefix context that
// persistent plugins receive when the daemon starts them.
func onCallEnv(p *plugins.Plugin) []string {
	return append(os.Environ(),
		fmt.Sprintf("TARRAGON_PLUGIN_NAME=%s", p.Config.ID),
		fmt.Sprintf("TARRAGON_PLUGIN_ID=%s", p.Config.ID),
		fmt.Sprintf("TARRAGON_PLUGIN_DISPLAY_NAME=%s", p.Config.Name),
		fmt.Sprintf("TARRAGON_PREFIX_SYMBOL=%s", p.Config.PrefixSymbol),
		fmt.Sprintf("TARRAGON_PLUGIN_PREFIX=%s", p.Config.ResolvedPrefix),
	)
}

// invokeOnCallQuery runs an on-call plugin query command and returns its JSON output.
func invokeOnCallQuery(ctx context.Context, p *plugins.Plugin, query string) (json.RawMessage, error) {
	entry, err := resolveOnCallEntrypoint(p)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, entry, "tarragon", "query", query)
	cmd.WaitDelay = time.Second // Descendants may keep stdout/stderr open after cancellation.
	cmd.Env = onCallEnv(p)
	stdout, stderr := captureOnCallOutput(cmd)
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s: %w; stderr=%s", p.Config.ID, err, stderr.diagnostic())
	}
	out, err := stdout.reply()
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		out = []byte("{}")
	}
	return json.RawMessage(out), nil
}

// invokeOnCallSelect runs an on-call plugin select command.
//
// The command contract is:
//
//	<entrypoint> tarragon select <result-id> [action]
//
// If stdout is empty and exit status is zero, the action is treated as success.
// JSON stdout can override success, supply a message, or request a continuation.
func invokeOnCallSelect(ctx context.Context, p *plugins.Plugin, resultID, action string) (wire.SelectResponse, error) {
	return runOnCallSelect(ctx, p, resultID, action, nil)
}

func runOnCallSelect(ctx context.Context, p *plugins.Plugin, resultID, action string, a *selectAction) (wire.SelectResponse, error) {
	entry, err := resolveOnCallEntrypoint(p)
	if err != nil {
		return wire.SelectResponse{}, err
	}
	args := []string{"tarragon", "select", resultID}
	if strings.TrimSpace(action) != "" {
		args = append(args, action)
	}

	cmd := exec.CommandContext(ctx, entry, args...)
	cmd.WaitDelay = time.Second
	cmd.Env = onCallEnv(p)
	stdout, stderr := captureOnCallOutput(cmd)
	if a == nil {
		err = cmd.Start()
	} else {
		var started bool
		started, err = a.startProcess(cmd.Start)
		if !started && err == nil {
			// Its terminal outcome is already decided. There is no child to reap.
			return wire.SelectResponse{}, context.DeadlineExceeded
		}
	}
	if err != nil {
		return wire.SelectResponse{}, fmt.Errorf("%s: %w", p.Config.ID, err)
	}
	if err := cmd.Wait(); err != nil {
		return wire.SelectResponse{}, fmt.Errorf("%s: %w; stderr=%s", p.Config.ID, err, stderr.diagnostic())
	}

	resp := wire.SelectResponse{Type: wire.MsgSelectResponse, Success: true}
	out, err := stdout.reply()
	if err != nil {
		return wire.SelectResponse{}, err
	}
	if len(out) == 0 {
		return resp, nil
	}

	var parsed struct {
		Success *bool            `json:"success"`
		Message string           `json:"message"`
		Next    *wire.ActionNext `json:"next"`
	}
	if err := json.Unmarshal(out, &parsed); err != nil {
		return wire.SelectResponse{}, fmt.Errorf("%s: invalid select response JSON: %w", p.Config.ID, err)
	}
	if parsed.Success != nil {
		resp.Success = *parsed.Success
	}
	resp.Message = parsed.Message
	resp.Next = parsed.Next
	return resp, nil
}

// resolveOnCallEntrypoint recovers system plugins whose executable moved after
// installation. Local plugin entrypoints remain pinned to their manifest path.
func resolveOnCallEntrypoint(p *plugins.Plugin) (string, error) {
	entry := plugins.ResolveEntrypoint(p.Dir, p.Config.Entrypoint)
	if info, err := os.Stat(entry); err == nil && !info.IsDir() {
		return entry, nil
	}
	if p.Config.Source == "system" {
		name := filepath.Base(p.Config.Entrypoint)
		if resolved, err := exec.LookPath(name); err == nil {
			return resolved, nil
		}
	}
	return "", fmt.Errorf("%s: entrypoint %q is unavailable", p.Config.ID, entry)
}
