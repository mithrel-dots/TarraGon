package cli

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/pelletier/go-toml/v2"
	"github.com/pelletier/go-toml/v2/unstable"
	"github.com/spf13/cobra"
)

var enablePluginCmd = &cobra.Command{
	Use:   "enable <name>",
	Short: "Enable a system plugin by executable name",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := strings.TrimSpace(args[0])
		if name == "" {
			return errors.New("plugin name is required")
		}

		binaryPath, err := resolveSystemBinary(name)
		if err != nil {
			return err
		}

		manifestBytes, err := fetchSystemPluginManifest(binaryPath)
		if err != nil {
			return err
		}

		rewrittenManifest, pluginName, err := rewriteManifestForSystem(manifestBytes, binaryPath, name)
		if err != nil {
			return err
		}

		pluginRoot := plugins.DefaultDir()
		if pluginRoot == "" {
			return errors.New("could not determine plugin installation directory")
		}
		if err := os.MkdirAll(pluginRoot, 0o755); err != nil {
			return fmt.Errorf("create plugin root directory: %w", err)
		}

		pluginDir := filepath.Join(pluginRoot, pluginName)
		if err := os.MkdirAll(pluginDir, 0o755); err != nil {
			return fmt.Errorf("create plugin directory: %w", err)
		}

		manifestPath := filepath.Join(pluginDir, "plugin.toml")
		if err := os.WriteFile(manifestPath, rewrittenManifest, 0o644); err != nil {
			return fmt.Errorf("write plugin manifest: %w", err)
		}

		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Enabled system plugin %q using %s\n", pluginName, binaryPath)
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Wrote manifest to %s\n", manifestPath)
		return nil
	},
}

func resolveSystemBinary(name string) (string, error) {
	path, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("resolve plugin executable: %w", err)
	}
	return filepath.Abs(path)
}

func fetchSystemPluginManifest(binaryPath string) ([]byte, error) {
	cmd := exec.Command(binaryPath, "tarragon", "manifest")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		stderrMsg := strings.TrimSpace(stderr.String())
		if stderrMsg != "" {
			return nil, fmt.Errorf("run %q tarragon manifest: %w (stderr: %s)", binaryPath, err, stderrMsg)
		}
		return nil, fmt.Errorf("run %q tarragon manifest: %w", binaryPath, err)
	}

	manifest := bytes.TrimSpace(stdout.Bytes())
	if len(manifest) == 0 {
		return nil, errors.New("plugin manifest command returned empty output")
	}
	return manifest, nil
}

func rewriteManifestForSystem(manifest []byte, binaryPath string, fallbackName string) ([]byte, string, error) {
	var cfg plugins.PluginConfig
	if err := toml.Unmarshal(manifest, &cfg); err != nil {
		return nil, "", fmt.Errorf("parse plugin manifest: %w", err)
	}

	// Resolve the stable id used for the install directory and config section.
	pluginName := plugins.NormalizePluginID(cfg.ID)
	if pluginName == "" {
		pluginName = plugins.NormalizePluginID(cfg.Name)
	}
	if pluginName == "" {
		pluginName = plugins.NormalizePluginID(fallbackName)
	}
	if pluginName == "" {
		return nil, "", errors.New("plugin manifest has no usable id or name")
	}
	if pluginName == plugins.OverviewID {
		return nil, "", fmt.Errorf("plugin id %q is built in", pluginName)
	}
	if strings.TrimSpace(cfg.Entrypoint) == "" {
		return nil, "", errors.New("plugin manifest is missing required field: entrypoint")
	}

	rewritten := manifest
	if !filepath.IsAbs(cfg.Entrypoint) {
		rewritten = setRootManifestString(rewritten, "entrypoint", binaryPath)
	}
	rewritten = setRootManifestString(rewritten, "source", "system")

	return rewritten, pluginName, nil
}

// The manifest was validated above. Edit only root values, preserving comments
// and nested plugin-owned tables (including [[handlers]]).
func setRootManifestString(content []byte, key, value string) []byte {
	replacement := strconv.Quote(value)
	var parser unstable.Parser
	parser.Reset(content)
	for parser.NextExpression() {
		node := parser.Expression()
		if node.Kind != unstable.KeyValue {
			break
		}
		keys := node.Key()
		if keys.Next() && keys.IsLast() && strings.EqualFold(string(keys.Node().Data), key) {
			raw := node.Value().Raw
			return slices.Replace(bytes.Clone(content), int(raw.Offset), int(raw.Offset+raw.Length), []byte(replacement)...)
		}
	}
	return append([]byte(key+" = "+replacement+"\n"), content...)
}
