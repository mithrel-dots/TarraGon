package ipc

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// startPeer re-execs the running binary. Route its private fixture invocation
// the same way as Cobra while exercising the actual three-process topology.
func TestMain(m *testing.M) {
	if slices.Equal(os.Args[1:], []string{"bench", "ipc", "fixture"}) {
		if err := RunFixture(os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	os.Exit(m.Run())
}

func TestDeliveryMatrix(t *testing.T) {
	var out bytes.Buffer
	err := Run(context.Background(), Options{Samples: 3, Warmup: 1, Plugins: 1, Burst: 1, Rounds: 1, Timeout: 5 * time.Second, Matrix: true}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if rows := strings.Count(out.String(), "  3/3  "); rows != 10 {
		t.Fatalf("expected direct UDS plus nine broker paths, got %d:\n%s", rows, out.String())
	}
}

func TestPipelinedFanout(t *testing.T) {
	// Partial warmup and measurement bursts; each cumulative snapshot must
	// contribute only the newly visible (query, plugin) suggestion.
	var out bytes.Buffer
	err := Run(context.Background(), Options{Samples: 5, Warmup: 3, Plugins: 4, Burst: 2, Rounds: 1, Timeout: 5 * time.Second, Matrix: true}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if rows := strings.Count(out.String(), "  20/20  "); rows != 10 {
		t.Fatalf("expected 20 measured suggestions for each route:\n%s", out.String())
	}
}

func TestJoinDeliveryTraces(t *testing.T) {
	plugin := []stamp{{"warm", 10}, {"measured", 100}}
	core := []coreStamp{{"warm", 20, 30}, {"measured", 110, 111}}
	ui := []stamp{{"warm", 40}, {"measured", 200}}
	got, err := join(plugin, ui, core, 1)
	if err != nil || !slices.Equal(got, []durations{{10, 1, 89, 100}}) {
		t.Fatalf("joined traces: %v, %v", got, err)
	}
	ui[1].ID = "stale"
	if _, err := join(plugin, ui, core, 1); err == nil {
		t.Fatal("accepted mismatched result ID")
	}
}
