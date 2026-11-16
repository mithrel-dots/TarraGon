package daemon

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

func TestResultRefsForActionOffers(t *testing.T) {
	dir := t.TempDir()
	image := filepath.Join(dir, "sky.png")
	if err := os.WriteFile(image, nil, 0600); err != nil {
		t.Fatal(err)
	}
	mgr := plugins.NewManager(dir)
	mgr.Plugins["consumer"] = &plugins.Plugin{BaseConfig: plugins.PluginConfig{
		Enabled: true,
		Handlers: []plugins.Handler{
			{Plugin: "consumer", Action: "view", Label: "View image", Accepts: "file", MIME: "image/*"},
			{Plugin: "consumer", Action: "visit", Label: "Visit link", Accepts: "url"},
			{Plugin: "consumer", Action: "copy", Label: "Copy text", Accepts: "text"},
		},
	}}
	if err := mgr.ApplyOverrides(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, producer, id, action string
		ref                        wire.ResultRef
		wantOffer                  bool
	}{
		{"relative file", "file_finder", "f17", "open", wire.ResultRef{Type: "file", Value: "pictures/sky.png", MIME: "image/png"}, false},
		{"random wallpaper", "wallpaper", "wallpaper:random", "set", wire.ResultRef{Type: "file", Value: "wallpaper:random", MIME: "image/png"}, false},
		{"previous wallpaper", "wallpaper", "wallpaper:previous", "open", wire.ResultRef{Type: "file", Value: "wallpaper:previous", MIME: "image/png"}, false},
		{"absolute file", "file_finder", "f17", "open", wire.ResultRef{Type: "file", Value: image, MIME: "image/png"}, true},
		{"missing absolute file", "file_finder", "f18", "open", wire.ResultRef{Type: "file", Value: filepath.Join(dir, "gone.png"), MIME: "image/png"}, true},
		{"url", "browser", "link", "open", wire.ResultRef{Type: "url", Value: "https://example.com"}, true},
		{"text", "notes", "note", "open", wire.ResultRef{Type: "text", Value: "wallpaper:random"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			native := wire.Action{Name: tc.action, Default: true}
			raw, err := json.Marshal(wire.ResultItem{ID: tc.id, Ref: &tc.ref, Actions: []wire.Action{native}})
			if err != nil {
				t.Fatal(err)
			}
			original := string(raw)
			res, _ := parseResult(tc.producer, raw, 0)
			q := &queryState{uiFeatures: []string{actionOffersFeature}}
			q.addOffersLocked(res.items, mgr)
			if string(res.Data) != original || len(res.items) != 1 {
				t.Fatalf("producer result changed: %+v", res)
			}
			row := res.items[0]
			if row.Plugin != tc.producer || row.ID != tc.id || len(row.Actions) == 0 ||
				row.Actions[0].Name != native.Name || !row.Actions[0].Default {
				t.Fatalf("native row changed: %+v", row)
			}
			if tc.wantOffer {
				if row.Ref == nil || *row.Ref != tc.ref || len(row.Actions) != 2 || !row.Actions[1].Secondary || row.Actions[1].OfferID == "" {
					t.Fatalf("expected a secondary offer with the original ref: %+v", row)
				}
			} else if row.Ref != nil || len(row.Actions) != 1 {
				t.Fatalf("invalid file ref remained eligible: %+v", row)
			}
		})
	}
}
