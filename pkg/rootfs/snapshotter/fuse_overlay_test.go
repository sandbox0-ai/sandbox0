package snapshotter

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfs"
)

func TestMergedDirForRootFSUsesRootFSMountBase(t *testing.T) {
	got, err := mergedDirForRootFS(&ctldapi.PrepareRootFSResponse{
		MountPoint: "/var/lib/sandbox0/ctld/rootfs/team/sandbox/volume/s0fs",
		UpperDir:   "/ignored/upper",
	})
	if err != nil {
		t.Fatalf("mergedDirForRootFS() error = %v", err)
	}
	want := "/var/lib/sandbox0/ctld/rootfs/team/sandbox/volume/merged"
	if got != want {
		t.Fatalf("merged dir = %q, want %q", got, want)
	}
}

func TestMergedDirForRootFSFallsBackToUpperDirBase(t *testing.T) {
	got, err := mergedDirForRootFS(&ctldapi.PrepareRootFSResponse{
		UpperDir: "/var/lib/sandbox0/ctld/rootfs/team/sandbox/volume/s0fs/upper",
	})
	if err != nil {
		t.Fatalf("mergedDirForRootFS() error = %v", err)
	}
	want := "/var/lib/sandbox0/ctld/rootfs/team/sandbox/volume/merged"
	if got != want {
		t.Fatalf("merged dir = %q, want %q", got, want)
	}
}

func TestBindRootFSMountReturnsRecursiveWritableBind(t *testing.T) {
	got := bindRootFSMount(rootfs.Mount{Type: "overlay"}, "/merged")
	if got.Type != "bind" || got.Source != "/merged" {
		t.Fatalf("bind mount = %+v, want bind /merged", got)
	}
	if len(got.Options) != 2 || got.Options[0] != "rbind" || got.Options[1] != "rw" {
		t.Fatalf("bind options = %#v, want rbind,rw", got.Options)
	}
}

func TestFuseOverlayMounterUnmountRemovesActiveMount(t *testing.T) {
	mounter := NewFuseOverlayMounter("/bin/false")
	stopped := false
	mounter.active = map[string]*fuseOverlayMount{
		"container-a": {
			key:     "container-a",
			merged:  "/merged",
			mounted: func(string) bool { return true },
			unmount: func(string) error {
				stopped = true
				return nil
			},
		},
	}

	if err := mounter.Unmount(context.Background(), "container-a"); err != nil {
		t.Fatalf("Unmount() error = %v", err)
	}
	if !stopped {
		t.Fatal("expected active mount to be stopped")
	}
	if len(mounter.active) != 0 {
		t.Fatalf("active mounts = %#v, want empty", mounter.active)
	}
}
