package wire

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mithrel-dots/tarragon/internal/filelock"
)

// Environment variables that override the resolved socket paths.
const (
	envUISocket      = "TARRAGON_UI_SOCKET"
	envPluginsSocket = "TARRAGON_PLUGINS_SOCKET"
	envXDGRuntimeDir = "XDG_RUNTIME_DIR"
)

// ResolveUISocketPath returns the Unix socket path the daemon listens on for
// UI connections, and that UI clients should dial.
//
// Resolution order:
//  1. TARRAGON_UI_SOCKET, if set and non-empty, is returned verbatim.
//  2. XDG_RUNTIME_DIR, if set and non-empty, yields "{XDG_RUNTIME_DIR}/tarragon/ui.sock".
//  3. Otherwise, "/tmp/tarragon-{euid}/ui.sock".
func ResolveUISocketPath() string {
	return resolveSocketPath(envUISocket, "ui.sock")
}

// ResolvePluginsSocketPath returns the Unix socket path the daemon listens on
// for plugin connections, and that plugin processes should dial.
//
// Resolution order:
//  1. TARRAGON_PLUGINS_SOCKET, if set and non-empty, is returned verbatim.
//  2. XDG_RUNTIME_DIR, if set and non-empty, yields "{XDG_RUNTIME_DIR}/tarragon/plugins.sock".
//  3. Otherwise, "/tmp/tarragon-{euid}/plugins.sock".
func ResolvePluginsSocketPath() string {
	return resolveSocketPath(envPluginsSocket, "plugins.sock")
}

func resolveSocketPath(overrideEnv, filename string) string {
	if v := os.Getenv(overrideEnv); v != "" {
		return v
	}
	if dir := os.Getenv(envXDGRuntimeDir); dir != "" {
		return filepath.Join(dir, "tarragon", filename)
	}
	return filepath.Join(fmt.Sprintf("/tmp/tarragon-%d", os.Geteuid()), filename)
}

// Message type constants (for plugins)
const (
	MsgHello          = "hello"
	MsgEnd            = "end"
	MsgRequest        = "request"
	MsgResponse       = "response"
	MsgLiveUpdate     = "live_update"
	MsgSelect         = "select"
	MsgSelectResponse = "select_response"
	MsgActionProgress = "action_progress"
	MsgStatus         = "status"
)

// ActionTypeQueryReplace tells a UI to submit the action's query as a new input.
const ActionTypeQueryReplace = "query_replace"

// ActionTypeKeepOpen tells a UI to execute a normal select action without
// dismissing the launcher after it succeeds.
const ActionTypeKeepOpen = "keep_open"

// ActionTypeOpen pushes a view and queries its destination without a select.
const ActionTypeOpen = "open"

type Destination struct {
	Plugin  string `json:"plugin"`
	Context string `json:"context,omitempty"`
	Text    string `json:"text"`
}

func (d *Destination) UnmarshalJSON(raw []byte) error {
	type fields Destination
	decoded := struct {
		fields
		Context ContextField `json:"context"`
	}{
		fields:  fields(*d),
		Context: ContextField{Value: d.Context},
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	if decoded.Context.Invalid {
		return ErrInvalidContext
	}
	*d = Destination(decoded.fields)
	d.Context = decoded.Context.Value
	return nil
}

// Action describes a named action a plugin can perform on a result.
// Type is omitted for the default select behavior, or can request a UI mode
// such as ActionTypeKeepOpen or ActionTypeQueryReplace.
type Action struct {
	Name        string       `json:"name"`
	Secondary   bool         `json:"secondary,omitempty"`
	OfferID     string       `json:"offer_id,omitempty"`
	Type        string       `json:"type,omitempty"`
	Query       string       `json:"query,omitempty"`
	Default     bool         `json:"default,omitempty"`
	Description string       `json:"description,omitempty"`
	Destination *Destination `json:"destination,omitempty"`
	Title       string       `json:"title,omitempty"`
	Placeholder string       `json:"placeholder,omitempty"`
	Requires    []string     `json:"requires,omitempty"`
	NoFrecency  bool         `json:"no_frecency,omitempty"`
}

// ActionNext is a continuation requested by the handler. The daemon consumes
// "refresh" after sending the ordinary terminal reply to the owner.
type ActionNext struct {
	Type        string       `json:"type"`
	Destination *Destination `json:"destination,omitempty"`
	Title       string       `json:"title,omitempty"`
	Placeholder string       `json:"placeholder,omitempty"`
	Issuer      string       `json:"issuer,omitempty"` // daemon-assigned handler ID on an open sent to a UI
}

// SelectResponse is the terminal outcome from a handler or from daemon to UI.
type SelectResponse struct {
	Type      string      `json:"type"`    // "select_response"
	Success   bool        `json:"success"` // whether the action succeeded
	Message   string      `json:"message,omitempty"`
	ActionID  string      `json:"action_id,omitempty"`
	RequestID string      `json:"request_id,omitempty"`
	Code      string      `json:"code,omitempty"`
	Next      *ActionNext `json:"next,omitempty"`
}

// SelectAck is sent to the owner before validation or dispatch.
type SelectAck struct {
	Type      string `json:"type"`
	ActionID  string `json:"action_id"`
	RequestID string `json:"request_id,omitempty"`
}

// ActionProgress is nonterminal and is delivered only to opted-in UIs.
type ActionProgress struct {
	Type      string `json:"type"`
	ActionID  string `json:"action_id"`
	RequestID string `json:"request_id,omitempty"`
	Message   string `json:"message,omitempty"`
}

// UIRequest is a query, select or control request on a UI session.
type UIRequest struct {
	Type     string `json:"type"`
	ClientID string `json:"client_id,omitempty"`
	Text     string `json:"text,omitempty"`
	Context  string `json:"context,omitempty"` // opaque, scoped to Plugin
	// New queries supersede older queries in this stream on the same connection.
	QueryStream string `json:"query_stream,omitempty"`
	Watch       bool   `json:"watch,omitempty"`
	// Optional fields for actions such as selection
	QueryID    string   `json:"query_id,omitempty"`
	Plugin     string   `json:"plugin,omitempty"`
	ResultID   string   `json:"result_id,omitempty"`
	Action     string   `json:"action,omitempty"`
	OfferID    string   `json:"offer_id,omitempty"`
	ID         string   `json:"id,omitempty"` // backward compatibility alias for ResultID
	RequestID  string   `json:"request_id,omitempty"`
	UIFeatures []string `json:"ui_features,omitempty"`
	NoFrecency bool     `json:"no_frecency,omitempty"`
}

// AckMessage acknowledges a query ("ack") or an explicit end ("ok").
type AckMessage struct {
	Type    string `json:"type"`
	QueryID string `json:"query_id"`
}

// PluginInfo describes a plugin's metadata and connection status for UI display.
//
// ID is the stable routing identifier and matches the value used in
// ResultItem.Plugin, UIRequest.Plugin and [plugins.<id>] config sections. Name
// is for display only.
type PluginInfo struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Description     string   `json:"description,omitempty"`
	Source          string   `json:"source,omitempty"`
	Enabled         bool     `json:"enabled"`
	Connected       bool     `json:"connected"`
	Lifecycle       string   `json:"lifecycle"`
	Prefix          string   `json:"prefix,omitempty"`
	RequirePrefix   bool     `json:"require_prefix,omitempty"`
	ProvidesGeneral bool     `json:"provides_general_suggestions,omitempty"`
	Capabilities    []string `json:"capabilities,omitempty"`
	Icon            string   `json:"icon,omitempty"`
}

// StatusResponse carries plugin connection status to the UI.
type StatusResponse struct {
	Type      string       `json:"type"`              // "status"
	Connected []string     `json:"connected"`         // sorted connected plugin names
	Total     int          `json:"total"`             // total enabled plugins
	Plugins   []PluginInfo `json:"plugins,omitempty"` // full metadata per plugin
}

// ReloadResponse is sent from daemon to UI after reload request handling.
type ReloadResponse struct {
	Type    string `json:"type"`
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// RestartResult reports the outcome of restarting a single plugin.
//
// Status is one of "restarted", "stopped", "skipped" or "error".
type RestartResult struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// RestartResponse is sent from daemon to UI after restart request handling.
type RestartResponse struct {
	Type    string          `json:"type"` // "restart_response"
	Success bool            `json:"success"`
	Message string          `json:"message,omitempty"`
	Results []RestartResult `json:"results,omitempty"`
}

// UpdateMessage is streamed from daemon to UI with aggregate snapshots.
type UpdateMessage struct {
	Type    string `json:"type"` // "update"
	QueryID string `json:"query_id"`
	// Payload is a JSON-encoded aggregate snapshot
	Payload []byte `json:"payload"`
}

// ResultRef is the producer's independently usable value for a manifest handler.
type ResultRef struct {
	Type  string `json:"type"`
	Value string `json:"value"`
	MIME  string `json:"mime,omitempty"`
}

// ResultItem represents a normalized suggestion for UI rendering.
type ResultItem struct {
	ID            string     `json:"id"`
	Label         string     `json:"label,omitempty"`
	Description   string     `json:"description,omitempty"`
	Icon          string     `json:"icon,omitempty"`
	Category      string     `json:"category,omitempty"`
	PreviewPath   string     `json:"preview_path,omitempty"`
	Ref           *ResultRef `json:"ref,omitempty"`
	Plugin        string     `json:"plugin"`
	Score         float64    `json:"score,omitempty"`
	FrecencyScore float64    `json:"frecency_score,omitempty"`
	Actions       []Action   `json:"actions,omitempty"`
}

// PluginHello identifies the plugin to the daemon router.
type PluginHello struct {
	Type string `json:"type"` // "hello"
	Name string `json:"name"`
}

// PluginRequest is sent from daemon to a plugin.
type PluginRequest struct {
	Type    string `json:"type"` // "request"
	QueryID string `json:"query_id"`
	Text    string `json:"text"`
	Context string `json:"context,omitempty"`
}

// SelectRequest is sent from daemon to a plugin for action execution.
type SelectRequest struct {
	Type     string `json:"type"`
	Context  string `json:"context,omitempty"`
	Plugin   string `json:"plugin,omitempty"`
	ResultID string `json:"result_id"`
	Action   string `json:"action"`
	QueryID  string `json:"query_id"`
	ActionID string `json:"action_id,omitempty"`
}

// PluginResponse is sent from plugin to daemon.
type PluginResponse struct {
	Type    string          `json:"type"` // "response"
	QueryID string          `json:"query_id"`
	Data    json.RawMessage `json:"data"`
	Live    bool            `json:"live,omitempty"`  // response: this producer will push replacements
	Final   bool            `json:"final,omitempty"` // live_update: last push from this producer
}

// MaxMessageSize limits one encoded JSON envelope, excluding the Unix newline.
const MaxMessageSize = 8 << 20

var ErrMessageTooLarge = errors.New("message exceeds the 8 MiB payload limit")

// WriteMsg marshals v as JSON and writes it as a single NDJSON line.
//
// If multiple goroutines share the same connection/writer, callers must
// serialize access (for example with a connection-scoped sync.Mutex).
func WriteMsg(w io.Writer, v any) error {
	b, err := EncodeMsg(v)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// EncodeMsg validates and returns one complete NDJSON frame.
func EncodeMsg(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxMessageSize {
		return nil, ErrMessageTooLarge
	}
	return append(b, '\n'), nil
}

// ReadMsg reads one NDJSON line from scanner and unmarshals it into v.
func ReadMsg(scanner *bufio.Scanner, v any) error {
	if scanner.Scan() {
		return json.Unmarshal(scanner.Bytes(), v)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return io.EOF
}

// NewScanner reserves one additional byte for the NDJSON delimiter.
func NewScanner(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), MaxMessageSize+1)
	return scanner
}

// Caller holds the parent directory lock through the subsequent bind.
func cleanupStaleSocket(path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	if fi.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("%s exists and is not a socket", path)
	}

	conn, err := net.DialTimeout("unix", path, time.Second)
	if err == nil {
		conn.Close()
		return fmt.Errorf("socket %s already in use", path)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("check existing socket %s: %w", path, err)
	}
	return os.Remove(path)
}

// ListenUnix creates a unix listener after ensuring the parent directory
// exists with mode 0700 and cleaning up a stale socket file, if present.
func ListenUnix(path string) (net.Listener, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create socket directory: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0o077 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("socket directory %s must belong to the current user and be private (0700)", dir)
	}
	lock, err := filelock.Dir(dir)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err := cleanupStaleSocket(path); err != nil {
		return nil, err
	}
	return net.Listen("unix", path)
}
