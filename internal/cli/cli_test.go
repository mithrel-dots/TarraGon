package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mithrel-dots/tarragon/internal/plugins"
)

func TestUninstallRequiresSingleInstalledDirectory(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	root := plugins.DefaultDir()
	installed := filepath.Join(root, "old-name")
	if err := os.MkdirAll(installed, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(installed, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", ".", "..", "../plugins", "old-name/.", installed, "linked"} {
		if err := uninstallPluginCmd.RunE(uninstallPluginCmd, []string{name}); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	if err := uninstallPluginCmd.RunE(uninstallPluginCmd, []string{"old-name"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(installed); !os.IsNotExist(err) {
		t.Fatalf("plugin directory still present: %v", err)
	}
}

func TestResolveSystemBinaryWithoutWhich(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir)
	path := filepath.Join(dir, "demo")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if got, err := resolveSystemBinary("demo"); err != nil || got != path {
		t.Fatalf("resolved %q: %v", got, err)
	}
}

func TestConfigGenerateCreatesFile(t *testing.T) {
	// Prepare a temp config dir via flag
	dir := t.TempDir()
	rootCmd.SetArgs([]string{"--config-dir", dir, "config", "generate", "--config-format", "toml"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	cfg := filepath.Join(dir, "tarragon.toml")
	if _, err := os.Stat(cfg); err != nil {
		t.Fatalf("expected config at %s: %v", cfg, err)
	}
}
