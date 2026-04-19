package s0fs

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestEngineSmallFileReadWriteReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")

	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "hello.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Fsync(node.Inode); err != nil {
		t.Fatalf("Fsync() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()

	replayedNode, err := replayed.Lookup(RootInode, "hello.txt")
	if err != nil {
		t.Fatalf("Lookup() after replay error = %v", err)
	}
	data, err := replayed.Read(replayedNode.Inode, 0, 1024)
	if err != nil {
		t.Fatalf("Read() after replay error = %v", err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("replayed data = %q, want hello", data)
	}
}

func TestEngineWriteExtendsWithZeros(t *testing.T) {
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: filepath.Join(t.TempDir(), "volume.wal")})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "sparse.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 4, []byte("x")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	data, err := engine.Read(node.Inode, 0, 8)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(data, []byte{0, 0, 0, 0, 'x'}) {
		t.Fatalf("sparse data = %#v", data)
	}
}

func TestEngineRenameAndUnlinkReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "before.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Rename(RootInode, "before.txt", RootInode, "after.txt"); err != nil {
		t.Fatalf("Rename() error = %v", err)
	}
	if err := engine.Fsync(node.Inode); err != nil {
		t.Fatalf("Fsync() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	replayedNode, err := replayed.Lookup(RootInode, "after.txt")
	if err != nil {
		t.Fatalf("Lookup(after) error = %v", err)
	}
	if data, err := replayed.Read(replayedNode.Inode, 0, 1024); err != nil || !bytes.Equal(data, []byte("payload")) {
		t.Fatalf("Read(after) = %q, %v", data, err)
	}
	if err := replayed.Unlink(RootInode, "after.txt"); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}
	if err := replayed.Fsync(replayedNode.Inode); err != nil {
		t.Fatalf("Fsync() after unlink error = %v", err)
	}
	if err := replayed.Close(); err != nil {
		t.Fatalf("Close(replayed) error = %v", err)
	}

	replayedAgain, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay again) error = %v", err)
	}
	defer replayedAgain.Close()
	if _, err := replayedAgain.Lookup(RootInode, "after.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(after) err = %v, want ErrNotFound", err)
	}
}

func TestEngineRejectsDuplicateDentry(t *testing.T) {
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: filepath.Join(t.TempDir(), "volume.wal")})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	if _, err := engine.CreateFile(RootInode, "dup.txt", 0o644); err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.CreateFile(RootInode, "dup.txt", 0o644); !errors.Is(err, ErrExists) {
		t.Fatalf("CreateFile(duplicate) err = %v, want ErrExists", err)
	}
}
