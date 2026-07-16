package snapshot

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
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
	if _, err := engine.SyncMaterialize(context.Background()); err != nil {
		t.Fatalf("SyncMaterialize() before restore error = %v", err)
	}
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
	fresh := openFreshS0FSEngine(t, mgr, "team-1", "vol-1")
	defer fresh.Close()
	if got := readS0FSFile(t, fresh, "state.txt"); got != "alpha" {
		t.Fatalf("fresh engine read after restore = %q, want alpha", got)
	}

	if err := mgr.DeleteSnapshot(context.Background(), "vol-1", snap.ID, "team-1"); err != nil {
		t.Fatalf("DeleteSnapshot() error = %v", err)
	}
	cfg, err := mgr.s0fsConfig("team-1", "vol-1")
	if err != nil {
		t.Fatalf("s0fsConfig() error = %v", err)
	}
	if _, err := s0fs.LoadSnapshot(context.Background(), cfg, snap.ID); err == nil {
		t.Fatal("LoadSnapshot() after delete returned nil error")
	}
	if len(repo.deleted) != 1 || repo.deleted[0] != snap.ID {
		t.Fatalf("deleted snapshots = %v", repo.deleted)
	}
}

func TestS0FSSnapshotDeleteCleansCanonicalObjectAfterRequestCancellation(t *testing.T) {
	t.Parallel()

	mgr, repo, _, engine := newS0FSSnapshotTestManager(t, "vol-delete-canceled")
	writeS0FSFile(t, engine, "state.txt", "payload")
	snap, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "vol-delete-canceled",
		Name:     "snap-delete-canceled",
		TeamID:   "team-1",
		UserID:   "user-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	cfg, err := mgr.s0fsConfig("team-1", "vol-delete-canceled")
	if err != nil {
		t.Fatalf("s0fsConfig() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mgr.repo = &cancelAfterTxRepo{fakeRepo: repo, cancel: cancel}
	if err := mgr.DeleteSnapshot(ctx, "vol-delete-canceled", snap.ID, "team-1"); err != nil {
		t.Fatalf("DeleteSnapshot() error = %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("delete request context was not canceled after transaction")
	}
	if _, err := s0fs.LoadSnapshot(context.Background(), cfg, snap.ID); !errors.Is(err, s0fs.ErrSnapshotNotFound) {
		t.Fatalf("LoadSnapshot() after canceled delete error = %v, want ErrSnapshotNotFound", err)
	}
}

func TestS0FSSnapshotRestoreSurvivesCacheReplacement(t *testing.T) {
	t.Parallel()

	mgr, _, _, engine := newS0FSSnapshotTestManager(t, "vol-restart")
	writeS0FSFile(t, engine, "state.txt", "alpha")

	snap, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "vol-restart",
		Name:     "snap-restart",
		TeamID:   "team-1",
		UserID:   "user-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	writeS0FSFile(t, engine, "state.txt", "beta")
	if _, err := engine.SyncMaterialize(context.Background()); err != nil {
		t.Fatalf("SyncMaterialize() before cache replacement error = %v", err)
	}

	// The manager cache is an EmptyDir in Kubernetes. Replacing it models a
	// manager Pod restart with an empty local cache.
	mgr.config.CacheDir = t.TempDir()
	mgr.volMgr = nil
	if err := mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
		VolumeID:   "vol-restart",
		SnapshotID: snap.ID,
		TeamID:     "team-1",
		UserID:     "user-1",
	}); err != nil {
		t.Fatalf("RestoreSnapshot() after cache replacement error = %v", err)
	}

	fresh := openFreshS0FSEngine(t, mgr, "team-1", "vol-restart")
	defer fresh.Close()
	if got := readS0FSFile(t, fresh, "state.txt"); got != "alpha" {
		t.Fatalf("fresh engine read after restore = %q, want alpha", got)
	}
}

func TestS0FSLegacySnapshotRecoversFromMatchingManifest(t *testing.T) {
	t.Parallel()

	mgr, _, _, engine := newS0FSSnapshotTestManager(t, "vol-legacy-recovery")
	writeS0FSFile(t, engine, "state.txt", "alpha")
	snap, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "vol-legacy-recovery",
		Name:     "snap-legacy",
		TeamID:   "team-1",
		UserID:   "user-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}

	writeS0FSFile(t, engine, "state.txt", "beta")
	if _, err := engine.SyncMaterialize(context.Background()); err != nil {
		t.Fatalf("SyncMaterialize() after snapshot error = %v", err)
	}
	legacyCfg, err := mgr.s0fsConfig("team-1", "vol-legacy-recovery")
	if err != nil {
		t.Fatalf("s0fsConfig() error = %v", err)
	}
	if err := s0fs.DeleteSnapshot(context.Background(), legacyCfg, snap.ID); err != nil {
		t.Fatalf("DeleteSnapshotState() error = %v", err)
	}

	mgr.config.CacheDir = t.TempDir()
	mgr.volMgr = nil
	if err := mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
		VolumeID:   "vol-legacy-recovery",
		SnapshotID: snap.ID,
		TeamID:     "team-1",
		UserID:     "user-1",
	}); err != nil {
		t.Fatalf("RestoreSnapshot() with legacy manifest recovery error = %v", err)
	}

	fresh := openFreshS0FSEngine(t, mgr, "team-1", "vol-legacy-recovery")
	defer fresh.Close()
	if got := readS0FSFile(t, fresh, "state.txt"); got != "alpha" {
		t.Fatalf("fresh engine read after legacy recovery = %q, want alpha", got)
	}

	// A second cache replacement must use the backfilled durable snapshot
	// object, without depending on the historical manifest fallback again.
	mgr.config.CacheDir = t.TempDir()
	backfilledCfg, err := mgr.s0fsConfig("team-1", "vol-legacy-recovery")
	if err != nil {
		t.Fatalf("s0fsConfig(backfilled) error = %v", err)
	}
	state, err := s0fs.LoadSnapshot(context.Background(), backfilledCfg, snap.ID)
	if err != nil {
		t.Fatalf("LoadSnapshot() after recovery backfill error = %v", err)
	}
	if got := snapshotSizeBytes(state); got != snap.SizeBytes {
		t.Fatalf("backfilled snapshot size = %d, want %d", got, snap.SizeBytes)
	}
}

func TestS0FSLegacySnapshotRejectsManifestSizeMismatch(t *testing.T) {
	t.Parallel()

	mgr, repo, _, engine := newS0FSSnapshotTestManager(t, "vol-legacy-mismatch")
	writeS0FSFile(t, engine, "state.txt", "alpha")
	snap, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "vol-legacy-mismatch",
		Name:     "snap-legacy-mismatch",
		TeamID:   "team-1",
		UserID:   "user-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	cfg, err := mgr.s0fsConfig("team-1", "vol-legacy-mismatch")
	if err != nil {
		t.Fatalf("s0fsConfig() error = %v", err)
	}
	if err := s0fs.DeleteSnapshot(context.Background(), cfg, snap.ID); err != nil {
		t.Fatalf("DeleteSnapshotState() error = %v", err)
	}
	repo.snapshots[snap.ID].SizeBytes++

	mgr.config.CacheDir = t.TempDir()
	mgr.volMgr = nil
	err = mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
		VolumeID:   "vol-legacy-mismatch",
		SnapshotID: snap.ID,
		TeamID:     "team-1",
		UserID:     "user-1",
	})
	if !errors.Is(err, s0fs.ErrSnapshotNotFound) {
		t.Fatalf("RestoreSnapshot() error = %v, want ErrSnapshotNotFound", err)
	}
	newCfg, cfgErr := mgr.s0fsConfig("team-1", "vol-legacy-mismatch")
	if cfgErr != nil {
		t.Fatalf("s0fsConfig(new cache) error = %v", cfgErr)
	}
	if _, loadErr := s0fs.LoadSnapshot(context.Background(), newCfg, snap.ID); !errors.Is(loadErr, s0fs.ErrSnapshotNotFound) {
		t.Fatalf("LoadSnapshot() after rejected recovery error = %v, want ErrSnapshotNotFound", loadErr)
	}
}

func TestS0FSForkVolumeUsesCopyOnWriteState(t *testing.T) {
	t.Parallel()

	mgr, repo, _, engine := newS0FSSnapshotTestManager(t, "vol-1")
	coordinator := &failingFlushCoordinator{}
	mgr.SetFlushCoordinator(coordinator)
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
	if coordinator.called {
		t.Fatal("ForkVolume called distributed flush coordinator")
	}

	forkCfg, err := mgr.s0fsConfig("team-1", forked.ID)
	if err != nil {
		t.Fatalf("s0fsConfig(forked) error = %v", err)
	}
	forkMaterializer := s0fs.NewMaterializer(forked.ID, forkCfg.ObjectStore, forkCfg.HeadStore, forkCfg.ObjectStoreForVolume)
	forkMaterializer.SetEncryption(forkCfg.Encryption)
	forkState, _, err := forkMaterializer.LoadLatestState(context.Background())
	if err != nil {
		t.Fatalf("LoadLatestState(forked) error = %v", err)
	}
	if len(forkState.Data) != 0 {
		t.Fatalf("forked materialized data = %+v, want empty", forkState.Data)
	}
	if len(forkState.Segments) == 0 {
		t.Fatal("forked materialized state has no inherited segments")
	}
	for _, segment := range forkState.Segments {
		if segment.VolumeID != "vol-1" {
			t.Fatalf("forked segment volume = %q, want vol-1", segment.VolumeID)
		}
	}
	childSegments, _, _, err := forkCfg.ObjectStore.List("segments/", "", "", "", 100)
	if err != nil {
		t.Fatalf("List(child segments) error = %v", err)
	}
	if len(childSegments) != 0 {
		t.Fatalf("child segment objects after fork = %+v, want none", childSegments)
	}

	forkCfg, err = mgr.s0fsConfig("team-1", forked.ID)
	if err != nil {
		t.Fatalf("s0fsConfig(forked) error = %v", err)
	}
	forkedEngine, err := s0fs.Open(context.Background(), forkCfg)
	if err != nil {
		t.Fatalf("Open(forked) error = %v", err)
	}
	defer forkedEngine.Close()

	if got := readS0FSFile(t, forkedEngine, "fork.txt"); got != "seed" {
		t.Fatalf("forked file = %q, want seed", got)
	}
	writeS0FSFile(t, forkedEngine, "fork.txt", "forked")
	if _, err := forkedEngine.SyncMaterialize(context.Background()); err != nil {
		t.Fatalf("SyncMaterialize(forked) error = %v", err)
	}
	if got := readS0FSFile(t, engine, "fork.txt"); got != "seed" {
		t.Fatalf("source file after fork mutation = %q, want seed", got)
	}

	freshForked := openFreshS0FSEngine(t, mgr, "team-1", forked.ID)
	defer freshForked.Close()
	if got := readS0FSFile(t, freshForked, "fork.txt"); got != "forked" {
		t.Fatalf("fresh forked file = %q, want forked", got)
	}
}

func TestS0FSForkFreshEmptyVolumeCreatesEmptyChild(t *testing.T) {
	t.Parallel()

	mgr, repo, _, _ := newS0FSSnapshotTestManager(t, "vol-empty")

	forked, err := mgr.ForkVolume(context.Background(), &ForkVolumeRequest{
		SourceVolumeID: "vol-empty",
		TeamID:         "team-1",
		UserID:         "user-2",
	})
	if err != nil {
		t.Fatalf("ForkVolume() error = %v", err)
	}
	if forked == nil || forked.ID == "" {
		t.Fatalf("forked volume = %+v", forked)
	}
	if forked.SourceVolumeID == nil || *forked.SourceVolumeID != "vol-empty" {
		t.Fatalf("forked source volume = %v, want vol-empty", forked.SourceVolumeID)
	}
	if _, ok := repo.volumes[forked.ID]; !ok {
		t.Fatalf("forked volume not persisted: %+v", repo.volumes)
	}

	forkCfg, err := mgr.s0fsConfig("team-1", forked.ID)
	if err != nil {
		t.Fatalf("s0fsConfig(forked) error = %v", err)
	}
	forkMaterializer := s0fs.NewMaterializer(forked.ID, forkCfg.ObjectStore, forkCfg.HeadStore, forkCfg.ObjectStoreForVolume)
	forkMaterializer.SetEncryption(forkCfg.Encryption)
	forkState, _, err := forkMaterializer.LoadLatestState(context.Background())
	if err != nil {
		t.Fatalf("LoadLatestState(forked) error = %v", err)
	}
	if len(forkState.Data) != 0 || len(forkState.ColdFiles) != 0 || len(forkState.Segments) != 0 {
		t.Fatalf("forked state has file data: data=%v cold=%v segments=%v", forkState.Data, forkState.ColdFiles, forkState.Segments)
	}
	if got := len(forkState.Children[s0fs.RootInode]); got != 0 {
		t.Fatalf("forked root entries = %d, want 0", got)
	}

	freshForked := openFreshS0FSEngine(t, mgr, "team-1", forked.ID)
	defer freshForked.Close()
	entries, err := freshForked.ReadDir(s0fs.RootInode)
	if err != nil {
		t.Fatalf("ReadDir(forked root) error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("forked root entries = %+v, want empty", entries)
	}
}

func TestS0FSCreateVolumeFromSnapshotUsesCopyOnWriteState(t *testing.T) {
	t.Parallel()

	mgr, _, _, engine := newS0FSSnapshotTestManager(t, "vol-snapshot-source")
	writeS0FSFile(t, engine, "snapshot.txt", "seed")

	snap, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "vol-snapshot-source",
		Name:     "snap-cow",
		TeamID:   "team-1",
		UserID:   "user-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	mgr.config.CacheDir = t.TempDir()

	child, err := mgr.CreateVolumeFromSnapshot(context.Background(), &CreateVolumeFromSnapshotRequest{
		SnapshotID: snap.ID,
		TeamID:     "team-1",
		UserID:     "user-2",
	})
	if err != nil {
		t.Fatalf("CreateVolumeFromSnapshot() error = %v", err)
	}
	if child == nil || child.ID == "" {
		t.Fatalf("child volume = %+v", child)
	}

	childCfg, err := mgr.s0fsConfig("team-1", child.ID)
	if err != nil {
		t.Fatalf("s0fsConfig(child) error = %v", err)
	}
	childMaterializer := s0fs.NewMaterializer(child.ID, childCfg.ObjectStore, childCfg.HeadStore, childCfg.ObjectStoreForVolume)
	childMaterializer.SetEncryption(childCfg.Encryption)
	childState, _, err := childMaterializer.LoadLatestState(context.Background())
	if err != nil {
		t.Fatalf("LoadLatestState(child) error = %v", err)
	}
	if len(childState.Data) != 0 {
		t.Fatalf("child materialized data = %+v, want empty", childState.Data)
	}
	if len(childState.Segments) == 0 {
		t.Fatal("child materialized state has no inherited segments")
	}
	for _, segment := range childState.Segments {
		if segment.VolumeID != "vol-snapshot-source" {
			t.Fatalf("child segment volume = %q, want vol-snapshot-source", segment.VolumeID)
		}
	}
	childSegments, _, _, err := childCfg.ObjectStore.List("segments/", "", "", "", 100)
	if err != nil {
		t.Fatalf("List(child segments) error = %v", err)
	}
	if len(childSegments) != 0 {
		t.Fatalf("child segment objects after create from snapshot = %+v, want none", childSegments)
	}

	childEngine := openFreshS0FSEngine(t, mgr, "team-1", child.ID)
	defer childEngine.Close()
	if got := readS0FSFile(t, childEngine, "snapshot.txt"); got != "seed" {
		t.Fatalf("child file = %q, want seed", got)
	}
	writeS0FSFile(t, childEngine, "snapshot.txt", "child")
	if _, err := childEngine.SyncMaterialize(context.Background()); err != nil {
		t.Fatalf("SyncMaterialize(child) error = %v", err)
	}
	if got := readS0FSFile(t, engine, "snapshot.txt"); got != "seed" {
		t.Fatalf("source file after child mutation = %q, want seed", got)
	}
}

func TestS0FSGarbageCollectsObjectsAfterSnapshotDelete(t *testing.T) {
	t.Parallel()

	mgr, _, _, engine := newS0FSSnapshotTestManager(t, "vol-gc-delete")
	writeS0FSFile(t, engine, "state.txt", "old")
	if _, err := engine.SyncMaterialize(context.Background()); err != nil {
		t.Fatalf("SyncMaterialize(old) error = %v", err)
	}
	snap, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "vol-gc-delete",
		Name:     "snap-old",
		TeamID:   "team-1",
		UserID:   "user-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	writeS0FSFile(t, engine, "state.txt", "new")
	if _, err := engine.SyncMaterialize(context.Background()); err != nil {
		t.Fatalf("SyncMaterialize(new) error = %v", err)
	}

	if err := mgr.DeleteSnapshot(context.Background(), "vol-gc-delete", snap.ID, "team-1"); err != nil {
		t.Fatalf("DeleteSnapshot() error = %v", err)
	}
	cfg, err := mgr.s0fsConfig("team-1", "vol-gc-delete")
	if err != nil {
		t.Fatalf("s0fsConfig() error = %v", err)
	}
	if got, want := listS0FSKeys(t, cfg.ObjectStore, "segments/"), []string{"segments/00000000000000000005-0.bin"}; !sameStrings(got, want) {
		t.Fatalf("segments after GC = %v, want %v", got, want)
	}
	if got, want := listS0FSKeys(t, cfg.ObjectStore, "manifests/"), []string{"manifests/00000000000000000005.json"}; !sameStrings(got, want) {
		t.Fatalf("manifests after GC = %v, want %v", got, want)
	}
}

func TestS0FSGarbageCollectionSkipsWhenForkExists(t *testing.T) {
	t.Parallel()

	mgr, repo, _, engine := newS0FSSnapshotTestManager(t, "vol-gc-fork-source")
	writeS0FSFile(t, engine, "state.txt", "old")
	if _, err := engine.SyncMaterialize(context.Background()); err != nil {
		t.Fatalf("SyncMaterialize(old) error = %v", err)
	}
	snap, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "vol-gc-fork-source",
		Name:     "snap-old",
		TeamID:   "team-1",
		UserID:   "user-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	sourceID := "vol-gc-fork-source"
	repo.volumes["vol-gc-child"] = &db.SandboxVolume{
		ID:             "vol-gc-child",
		TeamID:         "team-1",
		UserID:         "user-1",
		SourceVolumeID: &sourceID,
		AccessMode:     string(volume.AccessModeRWO),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	writeS0FSFile(t, engine, "state.txt", "new")
	if _, err := engine.SyncMaterialize(context.Background()); err != nil {
		t.Fatalf("SyncMaterialize(new) error = %v", err)
	}

	if err := mgr.DeleteSnapshot(context.Background(), "vol-gc-fork-source", snap.ID, "team-1"); err != nil {
		t.Fatalf("DeleteSnapshot() error = %v", err)
	}
	cfg, err := mgr.s0fsConfig("team-1", "vol-gc-fork-source")
	if err != nil {
		t.Fatalf("s0fsConfig() error = %v", err)
	}
	if got, want := listS0FSKeys(t, cfg.ObjectStore, "segments/"), []string{
		"segments/00000000000000000003-0.bin",
		"segments/00000000000000000005-0.bin",
	}; !sameStrings(got, want) {
		t.Fatalf("segments with fork child = %v, want %v", got, want)
	}
}

func TestS0FSDeleteVolumeObjectsIfUnreferencedDeletesPrefix(t *testing.T) {
	t.Parallel()

	mgr, repo, _, engine := newS0FSSnapshotTestManager(t, "vol-gc-volume-delete")
	writeS0FSFile(t, engine, "state.txt", "payload")
	if _, err := engine.SyncMaterialize(context.Background()); err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	vol := repo.volumes["vol-gc-volume-delete"]
	if err := mgr.DeleteVolumeObjectsIfUnreferenced(context.Background(), vol); err != nil {
		t.Fatalf("DeleteVolumeObjectsIfUnreferenced() error = %v", err)
	}
	cfg, err := mgr.s0fsConfig("team-1", "vol-gc-volume-delete")
	if err != nil {
		t.Fatalf("s0fsConfig() error = %v", err)
	}
	if keys := listS0FSKeys(t, cfg.ObjectStore, ""); len(keys) != 0 {
		t.Fatalf("objects after volume cleanup = %v, want none", keys)
	}
}

type failingFlushCoordinator struct {
	called bool
}

func (f *failingFlushCoordinator) CoordinateFlush(context.Context, string) error {
	f.called = true
	return errors.New("distributed flush should not be called for fork")
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
	mgr.config.CacheDir = t.TempDir()

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

func TestExportSnapshotArchiveS0FSMissingLegacyStateRemainsNotFound(t *testing.T) {
	t.Parallel()

	mgr, _, _, engine := newS0FSSnapshotTestManager(t, "vol-missing-legacy")
	writeS0FSFile(t, engine, "state.txt", "payload")
	snap, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "vol-missing-legacy",
		Name:     "snap-missing-legacy",
		TeamID:   "team-1",
		UserID:   "user-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	cfg, err := mgr.s0fsConfig("team-1", "vol-missing-legacy")
	if err != nil {
		t.Fatalf("s0fsConfig() error = %v", err)
	}
	if err := s0fs.DeleteSnapshot(context.Background(), cfg, snap.ID); err != nil {
		t.Fatalf("DeleteSnapshotState() error = %v", err)
	}
	for _, key := range listS0FSKeys(t, cfg.ObjectStore, "manifests/") {
		if err := cfg.ObjectStore.Delete(key); err != nil {
			t.Fatalf("Delete(%q) error = %v", key, err)
		}
	}
	mgr.config.CacheDir = t.TempDir()

	var archive bytes.Buffer
	err = mgr.ExportSnapshotArchive(context.Background(), &ExportSnapshotRequest{
		VolumeID:   "vol-missing-legacy",
		SnapshotID: snap.ID,
		TeamID:     "team-1",
	}, &archive)
	if !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("ExportSnapshotArchive() error = %v, want ErrSnapshotNotFound", err)
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

	volMgr := &fakeVolumeProvider{
		ctx: &volume.VolumeContext{
			VolumeID:  volumeID,
			TeamID:    "team-1",
			Backend:   volume.BackendS0FS,
			RootInode: 1,
		},
	}
	mgr := &Manager{
		repo:   repo,
		volMgr: volMgr,
		config: &config.StorageProxyConfig{
			CacheDir:              cacheDir,
			DefaultClusterId:      "test-cluster",
			RestoreRemountTimeout: "100ms",
			ObjectStorageType:     "mem",
			S3Bucket:              "snapshot-tests-" + sanitizeTestObjectStoreName(t.Name()),
		},
		logger:    logrus.New(),
		clusterID: "test-cluster",
		podID:     "test-pod",
		locks:     make(map[string]time.Time),
	}
	cfg, err := mgr.s0fsConfig("team-1", volumeID)
	if err != nil {
		t.Fatalf("s0fsConfig() error = %v", err)
	}
	engine, err := s0fs.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open(s0fs) error = %v", err)
	}
	t.Cleanup(func() {
		_ = engine.Close()
	})
	volMgr.ctx.S0FS = engine
	return mgr, repo, volMgr, engine
}

func sanitizeTestObjectStoreName(name string) string {
	replacer := strings.NewReplacer("/", "-", " ", "-", "\t", "-")
	return replacer.Replace(name)
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

func openFreshS0FSEngine(t *testing.T, mgr *Manager, teamID, volumeID string) *s0fs.Engine {
	t.Helper()
	cfg, err := mgr.s0fsConfig(teamID, volumeID)
	if err != nil {
		t.Fatalf("s0fsConfig(fresh %s) error = %v", volumeID, err)
	}
	cfg.WALPath = filepath.Join(t.TempDir(), "s0fs", volumeID, "engine.wal")
	engine, err := s0fs.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open(fresh %s) error = %v", volumeID, err)
	}
	return engine
}

func listS0FSKeys(t *testing.T, store objectstore.Store, prefix string) []string {
	t.Helper()
	var keys []string
	var startAfter, token string
	for {
		objects, hasMore, nextToken, err := store.List(prefix, startAfter, token, "", 1000)
		if err != nil {
			t.Fatalf("List(%q) error = %v", prefix, err)
		}
		for _, object := range objects {
			keys = append(keys, object.Key)
		}
		if !hasMore {
			return keys
		}
		if len(objects) > 0 {
			startAfter = objects[len(objects)-1].Key
		}
		token = nextToken
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type cancelAfterTxRepo struct {
	*fakeRepo
	cancel context.CancelFunc
}

func (r *cancelAfterTxRepo) WithTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	err := fn(nil)
	r.cancel()
	return err
}
