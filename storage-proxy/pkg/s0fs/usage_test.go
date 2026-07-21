package s0fs

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestEngineFilesystemUsageTracksLogicalDataAndInodes(t *testing.T) {
	engine, err := Open(context.Background(), Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	assertFilesystemUsage(t, engine, FilesystemUsage{Inodes: 1})

	dir, err := engine.Mkdir(RootInode, "dir", 0o755)
	if err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	file, err := engine.CreateFile(dir.Inode, "data.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(file.Inode, 4096, []byte("data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := engine.Symlink(RootInode, "data-link", "dir/data.bin", 0o777); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if _, err := engine.Link(file.Inode, RootInode, "data-hardlink"); err != nil {
		t.Fatalf("Link() error = %v", err)
	}

	assertFilesystemUsage(t, engine, FilesystemUsage{DataBytes: 4100, Inodes: 4})

	if err := engine.Unlink(dir.Inode, "data.bin"); err != nil {
		t.Fatalf("Unlink(data.bin) error = %v", err)
	}
	if err := engine.Unlink(RootInode, "data-hardlink"); err != nil {
		t.Fatalf("Unlink(data-hardlink) error = %v", err)
	}
	assertFilesystemUsage(t, engine, FilesystemUsage{DataBytes: 4100, Inodes: 4})

	if err := engine.Forget(file.Inode); err != nil {
		t.Fatalf("Forget() error = %v", err)
	}
	assertFilesystemUsage(t, engine, FilesystemUsage{Inodes: 3})
}

func TestEngineFilesystemUsageRejectsClosedEngine(t *testing.T) {
	engine, err := Open(context.Background(), Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := engine.FilesystemUsage(); !errors.Is(err, ErrClosed) {
		t.Fatalf("FilesystemUsage() error = %v, want ErrClosed", err)
	}
}

func assertFilesystemUsage(t *testing.T, engine *Engine, want FilesystemUsage) {
	t.Helper()
	got, err := engine.FilesystemUsage()
	if err != nil {
		t.Fatalf("FilesystemUsage() error = %v", err)
	}
	if got != want {
		t.Fatalf("FilesystemUsage() = %+v, want %+v", got, want)
	}
}
