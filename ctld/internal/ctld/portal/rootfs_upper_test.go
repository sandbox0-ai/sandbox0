package portal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"golang.org/x/sys/unix"
)

func TestRootFSUpperDirSyncRoundTrip(t *testing.T) {
	ctx := context.Background()
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID: "fs-roundtrip",
		WALPath:  filepath.Join(t.TempDir(), "rootfs.wal"),
	})
	if err != nil {
		t.Fatalf("open s0fs: %v", err)
	}
	defer engine.Close()

	upper := t.TempDir()
	if err := os.MkdirAll(filepath.Join(upper, "tmp", "rootfs"), 0o755); err != nil {
		t.Fatalf("mkdir upper: %v", err)
	}
	if err := os.WriteFile(filepath.Join(upper, "tmp", "rootfs", "hello.txt"), []byte("hello rootfs"), 0o644); err != nil {
		t.Fatalf("write upper file: %v", err)
	}
	if err := os.Symlink("hello.txt", filepath.Join(upper, "tmp", "rootfs", "hello.link")); err != nil {
		t.Fatalf("symlink upper file: %v", err)
	}

	if err := syncRootFSUpperToS0FS(ctx, engine, upper); err != nil {
		t.Fatalf("sync upper: %v", err)
	}

	restored := filepath.Join(t.TempDir(), "restored")
	if err := restoreRootFSUpperDir(ctx, engine, restored); err != nil {
		t.Fatalf("restore upper: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(restored, "tmp", "rootfs", "hello.txt"))
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(data) != "hello rootfs" {
		t.Fatalf("restored file = %q", string(data))
	}
	link, err := os.Readlink(filepath.Join(restored, "tmp", "rootfs", "hello.link"))
	if err != nil {
		t.Fatalf("read restored symlink: %v", err)
	}
	if link != "hello.txt" {
		t.Fatalf("restored symlink = %q", link)
	}
}

func TestRootFSUpperDirSyncPreservesXAttrs(t *testing.T) {
	ctx := context.Background()
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID: "fs-xattrs",
		WALPath:  filepath.Join(t.TempDir(), "rootfs.wal"),
	})
	if err != nil {
		t.Fatalf("open s0fs: %v", err)
	}
	defer engine.Close()

	upper := t.TempDir()
	dir := filepath.Join(upper, "deleted-base-dir")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir upper: %v", err)
	}
	const attrName = "user.sandbox0.rootfs"
	attrValue := []byte("opaque")
	if err := unix.Lsetxattr(dir, attrName, attrValue, 0); err != nil {
		if isIgnorableXAttrError(err) || errors.Is(err, unix.EPERM) {
			t.Skipf("xattrs are not available in this test environment: %v", err)
		}
		t.Fatalf("set xattr: %v", err)
	}

	if err := syncRootFSUpperToS0FS(ctx, engine, upper); err != nil {
		t.Fatalf("sync upper: %v", err)
	}

	restored := filepath.Join(t.TempDir(), "restored")
	if err := restoreRootFSUpperDir(ctx, engine, restored); err != nil {
		t.Fatalf("restore upper: %v", err)
	}
	got := make([]byte, 64)
	n, err := unix.Lgetxattr(filepath.Join(restored, "deleted-base-dir"), attrName, got)
	if err != nil {
		t.Fatalf("read restored xattr: %v", err)
	}
	if string(got[:n]) != string(attrValue) {
		t.Fatalf("restored xattr = %q, want %q", string(got[:n]), string(attrValue))
	}
}
