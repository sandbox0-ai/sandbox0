package s0fs

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestMutationNameAndSymlinkTargetBounds(t *testing.T) {
	engine, err := Open(context.Background(), Config{
		VolumeID: "volume-validation",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	maxName := strings.Repeat("n", MaxNameBytes)
	node, err := engine.CreateFile(RootInode, maxName, 0o644)
	if err != nil {
		t.Fatalf("CreateFile(max name) error = %v", err)
	}
	if _, err := engine.CreateFile(RootInode, strings.Repeat("n", MaxNameBytes+1), 0o644); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("CreateFile(overlong name) error = %v, want ErrInvalidInput", err)
	}
	if _, err := engine.Mkdir(RootInode, "bad/name", 0o755); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Mkdir(slash name) error = %v, want ErrInvalidInput", err)
	}
	if _, err := engine.Link(node.Inode, RootInode, "bad\x00name"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Link(NUL name) error = %v, want ErrInvalidInput", err)
	}
	if err := engine.Rename(RootInode, maxName, RootInode, strings.Repeat("r", MaxNameBytes+1)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Rename(overlong name) error = %v, want ErrInvalidInput", err)
	}

	if _, err := engine.Symlink(RootInode, "max-target", strings.Repeat("t", MaxSymlinkTargetBytes), 0o777); err != nil {
		t.Fatalf("Symlink(max target) error = %v", err)
	}
	if _, err := engine.Symlink(RootInode, "large-target", strings.Repeat("t", MaxSymlinkTargetBytes+1), 0o777); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Symlink(overlong target) error = %v, want ErrInvalidInput", err)
	}
	if _, err := engine.Symlink(RootInode, "nul-target", "bad\x00target", 0o777); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Symlink(NUL target) error = %v, want ErrInvalidInput", err)
	}
}
