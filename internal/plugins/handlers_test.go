package plugins

import (
	"slices"
	"testing"

	"github.com/spf13/viper"
)

func TestManifestHandlersFollowEffectiveConfig(t *testing.T) {
	root := t.TempDir()
	const rules = `enabled=true
[[handlers]]
action="set"
label="Set as wallpaper"
accepts="file"
mime="IMAGE/*"
[[handlers]]
action="play"
label="Play"
accepts="url"
[[handlers]]
action="copy"
label="Copy text"
accepts="text"
mime="text/plain"
[[handlers]]
action=2
[[handlers]]
action="bad"
label="Invalid"
accepts="file"
mime="im*age/png"
[[handlers]]
action="bad"
label="Invalid"
accepts="not-a-reference"
[[handlers]]
action="bad"
label="Invalid"
accepts="file"
mime=3
`
	writeManifest(t, root, "images", "lifecycle_mode='on_demand_persistent'\n"+rules)
	mgr := NewManager(root)
	if err := mgr.Discover(); err != nil {
		t.Fatal(err)
	}
	want := []Handler{
		{"images", "set", "Set as wallpaper", "file", "image/*"},
		{"images", "play", "Play", "url", ""},
		{"images", "copy", "Copy text", "text", "text/plain"},
	}
	got := mgr.Handlers()
	if !slices.Equal(got, want) {
		t.Fatalf("discovered %+v, want %+v", got, want)
	}
	if !got[0].Matches("file", "image/PNG") || got[0].Matches("file", "") || got[0].Matches("file", "text/plain") ||
		got[0].Matches("url", "image/png") || !got[1].Matches("url", "") ||
		!got[2].Matches("text", "TEXT/PLAIN") || got[2].Matches("text", "text/html") {
		t.Fatal("handler kind/MIME matching violated the manifest rules")
	}
	t.Cleanup(viper.Reset)
	viper.Set("plugins.images.enabled", false)
	if err := mgr.ApplyOverrides(); err != nil || len(mgr.Handlers()) != 0 {
		t.Fatalf("disabled handlers still available: %+v (%v)", mgr.Handlers(), err)
	}
	writeManifest(t, root, "images", `lifecycle_mode='on_call'
enabled=true
[[handlers]]
action='apply'
label='Apply'
accepts='text'
`)
	viper.Set("plugins.images.enabled", true)
	if err := mgr.ReloadConfigs(true); err != nil {
		t.Fatal(err)
	}
	if got := mgr.Handlers(); len(got) != 1 || got[0].Action != "apply" {
		t.Fatalf("stale manifest after restart: %+v", got)
	}
}
