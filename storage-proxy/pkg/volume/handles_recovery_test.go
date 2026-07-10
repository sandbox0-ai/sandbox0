package volume

import "testing"

func TestS0FSHandlesAreSelfDescribing(t *testing.T) {
	volCtx := &VolumeContext{VolumeID: "vol-1"}
	first := volCtx.OpenFileHandle(42)
	second := volCtx.OpenFileHandle(42)
	if first != 42 || second != 42 {
		t.Fatalf("OpenFileHandle() = %d, %d; want stable inode handle 42", first, second)
	}
	if inode, ok := volCtx.ResolveFileHandle(first, 42); !ok || inode != 42 {
		t.Fatalf("ResolveFileHandle() = %d, %v; want 42, true", inode, ok)
	}
	if _, ok := volCtx.ResolveFileHandle(first, 43); ok {
		t.Fatal("ResolveFileHandle(mismatched inode) ok = true, want false")
	}
	if got := volCtx.FileOpenCount(42); got != 2 {
		t.Fatalf("FileOpenCount(42) = %d, want 2", got)
	}
}

func TestRecoveredSelfDescribingHandleNeedsNoInMemoryRegistration(t *testing.T) {
	recovered := &VolumeContext{VolumeID: "vol-1"}
	if inode, ok := recovered.HandleInode(73); !ok || inode != 73 {
		t.Fatalf("HandleInode(recovered) = %d, %v; want 73, true", inode, ok)
	}
	if inode, remaining, unlinked, ok := recovered.ReleaseFileHandle(73); ok || unlinked || remaining != 0 || inode != 73 {
		t.Fatalf("ReleaseFileHandle(recovered) = inode %d remaining %d unlinked %v ok %v", inode, remaining, unlinked, ok)
	}
}
