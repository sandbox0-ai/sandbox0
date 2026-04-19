package snapshot

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

func TestS0FSSnapshotCreateRestoreAndDelete(t *testing.T) {
	t.Parallel()

	mgr, repo, volMgr, engine := newS0FSSnapshotTestManager(t, "vol-1")
	writeS0FSFile(t, engine, "state.txt", "alpha")

	snap, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID:    "vol-1",
		Name:        "snap-a",
		Description: "snapshot a",
		TeamID:      "team-1",
		UserID:      "user-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	if snap == nil || snap.ID == "" {
		t.Fatalf("snapshot = %+v", snap)
	}

	writeS0FSFile(t, engine, "state.txt", "beta")
	if got := readS0FSFile(t, engine, "state.txt"); got != "beta" {
		t.Fatalf("Read() before restore = %q, want beta", got)
	}

	volMgr.beginPending = 1
	if err := mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
		VolumeID:   "vol-1",
		SnapshotID: snap.ID,
		TeamID:     "team-1",
		UserID:     "user-1",
	}); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	if got := readS0FSFile(t, engine, "state.txt"); got != "alpha" {
		t.Fatalf("Read() after restore = %q, want alpha", got)
	}
	if !volMgr.beginCalled || !volMgr.waitCalled {
		t.Fatalf("invalidate coordination = begin:%v wait:%v", volMgr.beginCalled, volMgr.waitCalled)
	}

	if err := mgr.DeleteSnapshot(context.Background(), "vol-1", snap.ID, "team-1"); err != nil {
		t.Fatalf("DeleteSnapshot() error = %v", err)
	}
	if _, err := s0fs.LoadSnapshot(context.Background(), mgr.s0fsConfig("vol-1"), snap.ID); err == nil {
		t.Fatal("LoadSnapshot() after delete returned nil error")
	}
	if len(repo.deleted) != 1 || repo.deleted[0] != snap.ID {
		t.Fatalf("deleted snapshots = %v", repo.deleted)
	}
}

func TestS0FSForkVolumeCopiesState(t *testing.T) {
	t.Parallel()

	mgr, repo, _, engine := newS0FSSnapshotTestManager(t, "vol-1")
	writeS0FSFile(t, engine, "fork.txt", "seed")

	forked, err := mgr.ForkVolume(context.Background(), &ForkVolumeRequest{
		SourceVolumeID: "vol-1",
		TeamID:         "team-1",
		UserID:         "user-2",
	})
	if err != nil {
		t.Fatalf("ForkVolume() error = %v", err)
	}
	if forked == nil || forked.ID == "" {
		t.Fatalf("forked volume = %+v", forked)
	}
	if _, ok := repo.volumes[forked.ID]; !ok {
		t.Fatalf("forked volume not persisted: %+v", repo.volumes)
	}

	forkedEngine, err := s0fs.Open(context.Background(), mgr.s0fsConfig(forked.ID))
	if err != nil {
		t.Fatalf("Open(forked) error = %v", err)
	}
	defer forkedEngine.Close()

	if got := readS0FSFile(t, forkedEngine, "fork.txt"); got != "seed" {
		t.Fatalf("forked file = %q, want seed", got)
	}
	writeS0FSFile(t, forkedEngine, "fork.txt", "forked")
	if got := readS0FSFile(t, engine, "fork.txt"); got != "seed" {
		t.Fatalf("source file after fork mutation = %q, want seed", got)
	}
}

func TestExportSnapshotArchiveS0FS(t *testing.T) {
	t.Parallel()

	mgr, _, _, engine := newS0FSSnapshotTestManager(t, "vol-1")
	dir, err := engine.Mkdir(s0fs.RootInode, "dir", 0o755)
	if err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	file, err := engine.CreateFile(dir.Inode, "hello.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(file.Inode, 0, []byte("archive-body")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	snap, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "vol-1",
		Name:     "snap-a",
		TeamID:   "team-1",
		UserID:   "user-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}

	var archive bytes.Buffer
	if err := mgr.ExportSnapshotArchive(context.Background(), &ExportSnapshotRequest{
		VolumeID:   "vol-1",
		SnapshotID: snap.ID,
		TeamID:     "team-1",
	}, &archive); err != nil {
		t.Fatalf("ExportSnapshotArchive() error = %v", err)
	}

	files := untarSnapshotArchive(t, archive.Bytes())
	if got := string(files["dir/hello.txt"]); got != "archive-body" {
		t.Fatalf("archive file = %q, want archive-body", got)
	}
}

func newS0FSSnapshotTestManager(t *testing.T, volumeID string) (*Manager, *fakeRepo, *fakeVolumeProvider, *s0fs.Engine) {
	t.Helper()

	cacheDir := t.TempDir()
	repo := newFakeRepo()
	repo.volumes[volumeID] = &db.SandboxVolume{
		ID:         volumeID,
		TeamID:     "team-1",
		UserID:     "user-1",
		AccessMode: string(volume.AccessModeRWO),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: volumeID,
		WALPath:  filepath.Join(cacheDir, "s0fs", volumeID, "engine.wal"),
	})
	if err != nil {
		t.Fatalf("Open(s0fs) error = %v", err)
	}
	t.Cleanup(func() {
		_ = engine.Close()
	})

	volMgr := &fakeVolumeProvider{
		ctx: &volume.VolumeContext{
			VolumeID:  volumeID,
			TeamID:    "team-1",
			Backend:   volume.BackendS0FS,
			S0FS:      engine,
			RootInode: 1,
		},
	}
	mgr := &Manager{
		repo:      repo,
		volMgr:    volMgr,
		config:    &config.StorageProxyConfig{CacheDir: cacheDir, DefaultClusterId: "test-cluster", RestoreRemountTimeout: "100ms"},
		logger:    logrus.New(),
		clusterID: "test-cluster",
		podID:     "test-pod",
		locks:     make(map[string]time.Time),
	}
	return mgr, repo, volMgr, engine
}

func writeS0FSFile(t *testing.T, engine *s0fs.Engine, name, value string) {
	t.Helper()

	node, err := engine.Lookup(s0fs.RootInode, name)
	if err != nil {
		node, err = engine.CreateFile(s0fs.RootInode, name, 0o644)
		if err != nil {
			t.Fatalf("CreateFile(%q) error = %v", name, err)
		}
	}
	if err := engine.Truncate(node.Inode, 0); err != nil {
		t.Fatalf("Truncate(%q) error = %v", name, err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte(value)); err != nil {
		t.Fatalf("Write(%q) error = %v", name, err)
	}
}

func readS0FSFile(t *testing.T, engine *s0fs.Engine, name string) string {
	t.Helper()

	node, err := engine.Lookup(s0fs.RootInode, name)
	if err != nil {
		t.Fatalf("Lookup(%q) error = %v", name, err)
	}
	payload, err := engine.Read(node.Inode, 0, node.Size)
	if err != nil {
		t.Fatalf("Read(%q) error = %v", name, err)
	}
	return string(payload)
}

func untarSnapshotArchive(t *testing.T, payload []byte) map[string][]byte {
	t.Helper()

	gzipReader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("gzip.NewReader() error = %v", err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	files := make(map[string][]byte)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next() error = %v", err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		body, err := io.ReadAll(tarReader)
		if err != nil {
			t.Fatalf("ReadAll(%q) error = %v", header.Name, err)
		}
		files[header.Name] = body
	}
	return files
}
