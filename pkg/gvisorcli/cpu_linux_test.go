//go:build linux

package gvisorcli

import (
	"os"
	"runtime"
	"testing"

	"github.com/opencontainers/go-digest"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func TestNativeCPUStateLayout(t *testing.T) {
	cacheLine, layout, err := nativeCPUStateLayout()
	switch runtime.GOARCH {
	case "arm64":
		if err != nil || cacheLine != 0 || layout != "" {
			t.Fatalf("unexpected ARM64 x86 state: %d %s %v", cacheLine, layout, err)
		}
	case "amd64":
		if err != nil {
			t.Fatal(err)
		}
		if cacheLine == 0 || cacheLine&(cacheLine-1) != 0 {
			t.Fatalf("invalid measured cache line: %d", cacheLine)
		}
		if err := digest.Digest(layout).Validate(); err != nil {
			t.Fatal(err)
		}
	default:
		if err == nil {
			t.Fatal("unsupported architecture accepted")
		}
	}
}

// This probe only observes the host. It creates no sandbox and does not prove
// migration eligibility or cover every CPU in the node's allowed CPU set.
func TestStockRunscCPUProfile(t *testing.T) {
	path := os.Getenv("SANDBOX0_CPU_PROFILE_RUNSC")
	if path == "" {
		t.Skip("set SANDBOX0_CPU_PROFILE_RUNSC to a native stock runsc executable")
	}
	runner := New(Config{Path: path, Root: t.TempDir(), Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true}).(*Command)
	profile, err := runner.CPUProfile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if profile.Architecture != runtime.GOARCH {
		t.Fatalf("wrong architecture: %s", profile.Architecture)
	}
	again, err := runner.CPUProfile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.CheckMigrationCPUProfiles(profile, again); err != nil {
		t.Fatal(err)
	}
	first, err := profile.Digest()
	if err != nil {
		t.Fatal(err)
	}
	second, err := again.Digest()
	if err != nil || first != second {
		t.Fatalf("CPU observation changed: %s %s %v", first, second, err)
	}
	t.Logf("observed %s %s: %d features, profile %s", profile.Architecture, profile.RunscVersion, len(profile.Features), first)
}
