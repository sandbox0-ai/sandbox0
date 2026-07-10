package fsserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

const (
	handleRecoveryHelperEnv = "S0FS_HANDLE_RECOVERY_HELPER"
	handleRecoveryCacheEnv  = "S0FS_HANDLE_RECOVERY_CACHE"
)

func TestS0FSLogicalHandleRecoveryAcrossProcessRestart(t *testing.T) {
	if os.Getenv(handleRecoveryHelperEnv) == "1" {
		runS0FSHandleRecoveryCrashHelper(t)
		os.Exit(0)
	}

	cacheDir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestS0FSLogicalHandleRecoveryAcrossProcessRestart$")
	cmd.Env = append(os.Environ(),
		handleRecoveryHelperEnv+"=1",
		handleRecoveryCacheEnv+"="+cacheDir,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper error = %v\n%s", err, output)
	}

	handlePayload, err := os.ReadFile(filepath.Join(cacheDir, "test-handle"))
	if err != nil {
		t.Fatalf("ReadFile(test handle) error = %v", err)
	}
	handleID, err := strconv.ParseUint(string(handlePayload), 10, 64)
	if err != nil {
		t.Fatalf("ParseUint(test handle) error = %v", err)
	}
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID:          "vol-recovery",
		WALPath:           filepath.Join(cacheDir, "engine.wal"),
		RetainAllUnlinked: true,
	})
	if err != nil {
		t.Fatalf("Open(recovered engine) error = %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	volCtx := &volume.VolumeContext{
		VolumeID:  "vol-recovery",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		MountedAt: time.Now(),
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
	}
	server := newTestFileSystemServer(&fakeVolumeManager{volumes: map[string]*volume.VolumeContext{
		"vol-recovery": volCtx,
	}}, nil, nil)
	ctx := authContext("team-a", "")

	read, err := server.Read(ctx, &pb.ReadRequest{
		VolumeId: "vol-recovery",
		Inode:    handleID,
		HandleId: handleID,
		Size:     64,
	})
	if err != nil {
		t.Fatalf("Read(recovered handle) error = %v", err)
	}
	if !bytes.Equal(read.Data, []byte("before restart")) {
		t.Fatalf("Read(recovered handle) = %q, want %q", read.Data, "before restart")
	}
	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-recovery",
		Inode:    handleID,
		HandleId: handleID,
		Offset:   0,
		Data:     []byte("after restart"),
	}); err != nil {
		t.Fatalf("Write(recovered handle) error = %v", err)
	}
	if _, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: "vol-recovery",
		HandleId: handleID,
	}); err != nil {
		t.Fatalf("Fsync(recovered handle) error = %v", err)
	}
	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-recovery",
		Inode:    handleID,
		HandleId: handleID,
	}); err != nil {
		t.Fatalf("Release(recovered handle) error = %v", err)
	}
	if _, err := engine.GetAttr(handleID); err != nil {
		t.Fatalf("GetAttr(retained unlinked inode) error = %v", err)
	}
	if err := volCtx.FinalizeRecoverableHandles(); err != nil {
		t.Fatalf("FinalizeRecoverableHandles() error = %v", err)
	}
	if _, err := engine.GetAttr(handleID); !errors.Is(err, s0fs.ErrNotFound) {
		t.Fatalf("GetAttr(finalized inode) error = %v, want %v", err, s0fs.ErrNotFound)
	}
}

func runS0FSHandleRecoveryCrashHelper(t *testing.T) {
	t.Helper()
	cacheDir := os.Getenv(handleRecoveryCacheEnv)
	if cacheDir == "" {
		t.Fatal("recovery helper cache directory is empty")
	}
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID:          "vol-recovery",
		WALPath:           filepath.Join(cacheDir, "engine.wal"),
		RetainAllUnlinked: true,
	})
	if err != nil {
		t.Fatalf("Open(helper engine) error = %v", err)
	}
	volCtx := &volume.VolumeContext{
		VolumeID:  "vol-recovery",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		MountedAt: time.Now(),
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
	}
	server := newTestFileSystemServer(&fakeVolumeManager{volumes: map[string]*volume.VolumeContext{
		"vol-recovery": volCtx,
	}}, nil, nil)
	ctx := authContext("team-a", "")
	created, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-recovery",
		Parent:   1,
		Name:     "open-unlinked.txt",
		Mode:     0o600,
	})
	if err != nil {
		t.Fatalf("Create(helper) error = %v", err)
	}
	if created.HandleId != created.Inode {
		t.Fatalf("Create(helper) handle = %d, inode = %d; want self-describing handle", created.HandleId, created.Inode)
	}
	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-recovery",
		Inode:    created.Inode,
		HandleId: created.HandleId,
		Data:     []byte("before restart"),
	}); err != nil {
		t.Fatalf("Write(helper) error = %v", err)
	}
	if _, err := server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: "vol-recovery",
		Parent:   1,
		Name:     "open-unlinked.txt",
	}); err != nil {
		t.Fatalf("Unlink(helper) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(cacheDir, "test-handle"), []byte(fmt.Sprint(created.HandleId)), 0o600); err != nil {
		t.Fatalf("WriteFile(test handle) error = %v", err)
	}
}
