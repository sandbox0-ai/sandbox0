package rootfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/core/mount"
)

func TestOverlayUpperdir(t *testing.T) {
	t.Parallel()
	upperdir, ok := overlayUpperdir([]mount.Mount{{
		Type: "overlay", Options: []string{"lowerdir=/lower", "upperdir=/upper", "workdir=/work"},
	}})
	if !ok || upperdir != "/upper" {
		t.Fatalf("overlayUpperdir() = %q, %v", upperdir, ok)
	}
}

func TestMountedContainerdDataPathPrefersCtldMount(t *testing.T) {
	t.Parallel()
	mountedRoot := t.TempDir()
	upperdir := filepath.Join(mountedRoot, "snapshots", "1", "fs")
	if err := os.MkdirAll(upperdir, 0o755); err != nil {
		t.Fatal(err)
	}
	runtime := &ContainerdRuntime{
		containerdHostDataRoot: "/var/lib/containerd",
		containerdDataRoot:     mountedRoot,
	}
	got, err := runtime.mountedContainerdDataPath("/var/lib/containerd/snapshots/1/fs")
	if err != nil {
		t.Fatalf("mountedContainerdDataPath() error = %v", err)
	}
	if got != upperdir {
		t.Fatalf("mountedContainerdDataPath() = %q, want %q", got, upperdir)
	}
}
