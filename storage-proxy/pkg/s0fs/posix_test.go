package s0fs

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestPOSIXSpecialNodesXattrsAllocationCopyAndRename(t *testing.T) {
	engine, err := Open(context.Background(), Config{VolumeID: "vol-posix", WALPath: filepath.Join(t.TempDir(), "engine.wal")})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	fifo, err := engine.Mknod(RootInode, "events.fifo", uint32(syscall.S_IFIFO|0o640), 0)
	if err != nil || fifo.Type != TypeFIFO {
		t.Fatalf("Mknod(FIFO) = %+v, %v", fifo, err)
	}
	device, err := engine.Mknod(RootInode, "ttyS0", uint32(syscall.S_IFCHR|0o600), 0x1234)
	if err != nil || device.Type != TypeChar || device.Rdev != 0x1234 {
		t.Fatalf("Mknod(char) = %+v, %v", device, err)
	}

	source, err := engine.CreateFile(RootInode, "source", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Write(source.Inode, 0, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if err := engine.SetXattr(source.Inode, "user.test", []byte("one"), XattrCreate); err != nil {
		t.Fatal(err)
	}
	if err := engine.SetXattr(source.Inode, "user.test", []byte("duplicate"), XattrCreate); !errors.Is(err, ErrExists) {
		t.Fatalf("SetXattr(CREATE existing) error = %v, want ErrExists", err)
	}
	if err := engine.SetXattr(source.Inode, "user.missing", []byte("x"), XattrReplace); !errors.Is(err, ErrXattrNotFound) {
		t.Fatalf("SetXattr(REPLACE missing) error = %v, want ErrXattrNotFound", err)
	}
	if names, err := engine.ListXattrs(source.Inode); err != nil || len(names) != 1 || names[0] != "user.test" {
		t.Fatalf("ListXattrs() = %v, %v", names, err)
	}

	if err := engine.Fallocate(source.Inode, uint32(unix.FALLOC_FL_ZERO_RANGE|unix.FALLOC_FL_KEEP_SIZE), 2, 2); err != nil {
		t.Fatal(err)
	}
	if got, _ := engine.Read(source.Inode, 0, 32); string(got) != "ab\x00\x00ef" {
		t.Fatalf("zero-range bytes = %q", got)
	}
	if err := engine.Fallocate(source.Inode, uint32(unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE), 1, 3); err != nil {
		t.Fatal(err)
	}
	if got, _ := engine.Read(source.Inode, 0, 32); string(got) != "a\x00\x00\x00ef" {
		t.Fatalf("punch-hole bytes = %q", got)
	}
	if err := engine.Fallocate(source.Inode, 0, 10, 2); err != nil {
		t.Fatal(err)
	}
	if attr, _ := engine.GetAttr(source.Inode); attr.Size != 12 {
		t.Fatalf("allocated size = %d, want 12", attr.Size)
	}

	destination, err := engine.CreateFile(RootInode, "destination", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if copied, err := engine.CopyFileRange(source.Inode, 0, destination.Inode, 2, 6); err != nil || copied != 6 {
		t.Fatalf("CopyFileRange() = %d, %v", copied, err)
	}
	if got, _ := engine.Read(destination.Inode, 0, 16); string(got) != "\x00\x00a\x00\x00\x00ef" {
		t.Fatalf("copied bytes = %q", got)
	}

	whenA := time.Unix(1700000000, 123).UTC()
	whenM := time.Unix(1700000100, 456).UTC()
	if err := engine.SetTimes(source.Inode, whenA, whenM); err != nil {
		t.Fatal(err)
	}
	if attr, _ := engine.GetAttr(source.Inode); !attr.Atime.Equal(whenA) || !attr.Mtime.Equal(whenM) {
		t.Fatalf("times = %s %s, want %s %s", attr.Atime, attr.Mtime, whenA, whenM)
	}

	if err := engine.RenameWithFlags(RootInode, "source", RootInode, "destination", uint32(unix.RENAME_NOREPLACE)); !errors.Is(err, ErrExists) {
		t.Fatalf("Rename(NOREPLACE) error = %v, want ErrExists", err)
	}
	if err := engine.RenameWithFlags(RootInode, "source", RootInode, "destination", uint32(unix.RENAME_EXCHANGE)); err != nil {
		t.Fatalf("Rename(EXCHANGE) error = %v", err)
	}
	if node, _ := engine.Lookup(RootInode, "destination"); node.Inode != source.Inode {
		t.Fatalf("exchange destination inode = %d, want %d", node.Inode, source.Inode)
	}
}
