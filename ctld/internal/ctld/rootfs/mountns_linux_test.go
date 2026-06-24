//go:build linux

package rootfs

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestFuseOptionsForMountedFDClearsDirectMount(t *testing.T) {
	opts := &fuse.MountOptions{
		FsName:            "sandbox0-rootfs",
		Name:              "sandbox0-rootfs",
		DirectMount:       true,
		DirectMountStrict: true,
		DirectMountFlags:  123,
		AllowOther:        true,
		MaxWrite:          256 * 1024,
	}

	got := fuseOptionsForMountedFD(opts)
	if got == opts {
		t.Fatal("fuseOptionsForMountedFD returned the input options")
	}
	if got.DirectMount || got.DirectMountStrict || got.DirectMountFlags != 0 {
		t.Fatalf("direct mount options were not cleared: %+v", got)
	}
	if !opts.DirectMount || !opts.DirectMountStrict || opts.DirectMountFlags != 123 {
		t.Fatalf("input options were mutated: %+v", opts)
	}
	if got.FsName != opts.FsName || got.Name != opts.Name || got.MaxWrite != opts.MaxWrite || got.AllowOther != opts.AllowOther {
		t.Fatalf("non-direct mount options were not preserved: got=%+v want=%+v", got, opts)
	}
}
