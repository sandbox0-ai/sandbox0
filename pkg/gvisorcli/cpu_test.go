package gvisorcli

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestCPUFeaturesUsesStockReadOnlyCommand(t *testing.T) {
	runner, log := checkpointRunner(t, "printf 'xsave,aes,sse2\\n'\n")
	features, err := runner.CPUFeatures(t.Context())
	if err != nil || !slices.Equal(features, []string{"aes", "sse2", "xsave"}) {
		t.Fatalf("features: %v %v", features, err)
	}
	want := strings.Join(append(runner.GlobalArgs(), "cpu-features"), "\n") + "\n"
	if calls := readCheckpointCalls(t, log); calls != want {
		t.Fatalf("runtime calls = %q, want %q", calls, want)
	}
}

func TestCPUFeaturesRejectsFailedOrMalformedMeasurement(t *testing.T) {
	for _, behavior := range []string{
		"printf 'aes,sse2\\n'; exit 1\n",
		"printf 'warning: incomplete\\naes,sse2\\n'\n",
		"printf 'aes,aes\\n'\n",
		"exit 0\n",
		"head -c 65537 /dev/zero\n",
	} {
		runner, _ := checkpointRunner(t, behavior)
		if _, err := runner.CPUFeatures(t.Context()); err == nil {
			t.Fatalf("accepted measurement from %q", behavior)
		}
	}
}

func TestCPUObservationRejectsCanceledContextBeforeCommand(t *testing.T) {
	runner, log := checkpointRunner(t, "exit 0\n")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := runner.CPUFeatures(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("features: %v", err)
	}
	if _, err := runner.CPUProfile(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("profile: %v", err)
	}
	if calls := readCheckpointCalls(t, log); calls != "" {
		t.Fatalf("canceled observation invoked runsc: %q", calls)
	}
}

func TestCPUProfileRejectsUninspectableExecutableBeforeCommand(t *testing.T) {
	runner, log := checkpointRunner(t, "exit 0\n")
	if _, err := runner.CPUProfile(t.Context()); err == nil {
		t.Fatal("accepted script as native runsc")
	}
	if calls := readCheckpointCalls(t, log); calls != "" {
		t.Fatalf("uninspectable binary invoked: %q", calls)
	}
}
