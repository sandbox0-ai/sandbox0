package snapshot

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sirupsen/logrus"
)

func newTestSandboxFilesystemManager(t *testing.T, repo *fakeRepo) *Manager {
	t.Helper()
	return &Manager{
		repo: repo,
		config: &config.StorageProxyConfig{
			DefaultClusterId:  "test-cluster",
			CacheDir:          t.TempDir(),
			ObjectStorageType: "mem",
			S3Bucket:          t.Name(),
		},
		logger:    logrus.New(),
		clusterID: "test-cluster",
		podID:     "test-pod",
		locks:     make(map[string]time.Time),
	}
}

func TestCreateSandboxFilesystemSnapshotUsesCommittedHeadWithoutReMaterializing(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()
	mgr := newTestSandboxFilesystemManager(t, repo)
	filesystem := &db.SandboxFilesystem{
		ID:              "fs-committed-head",
		TeamID:          "team-1",
		UserID:          "user-1",
		BaseImageDigest: "sha256:base",
		State:           db.SandboxFilesystemStateAvailable,
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}
	repo.filesystems[filesystem.ID] = cloneFakeSandboxFilesystem(filesystem)

	engine, closeFn, err := mgr.openSandboxFilesystemS0FSEngine(ctx, filesystem.TeamID, filesystem.ID)
	if err != nil {
		t.Fatalf("openSandboxFilesystemS0FSEngine() error = %v", err)
	}
	node, err := engine.CreateFile(s0fs.RootInode, "hello.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	manifest, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	if err := closeFn(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	snapshot, err := mgr.CreateSandboxFilesystemSnapshot(ctx, &CreateSandboxFilesystemSnapshotRequest{
		FilesystemID: filesystem.ID,
		TeamID:       filesystem.TeamID,
		UserID:       filesystem.UserID,
		Name:         "checkpoint",
	})
	if err != nil {
		t.Fatalf("CreateSandboxFilesystemSnapshot() error = %v", err)
	}
	wantHead := s0fsManifestKey(manifest.ManifestSeq)
	if snapshot.S0FSHead != wantHead {
		t.Fatalf("snapshot head = %q, want %q", snapshot.S0FSHead, wantHead)
	}
	if repo.fsHeads[filesystem.ID].ManifestSeq != manifest.ManifestSeq {
		t.Fatalf("committed head seq = %d, want %d", repo.fsHeads[filesystem.ID].ManifestSeq, manifest.ManifestSeq)
	}
}

func TestForkSandboxFilesystemAllowsEmptySource(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()
	mgr := newTestSandboxFilesystemManager(t, repo)
	filesystem := &db.SandboxFilesystem{
		ID:              "fs-empty-source",
		TeamID:          "team-1",
		UserID:          "user-1",
		BaseImageDigest: "sha256:base",
		State:           db.SandboxFilesystemStateAvailable,
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}
	repo.filesystems[filesystem.ID] = cloneFakeSandboxFilesystem(filesystem)

	forked, err := mgr.ForkSandboxFilesystem(ctx, &ForkSandboxFilesystemRequest{
		SourceFilesystemID: filesystem.ID,
		TeamID:             filesystem.TeamID,
		UserID:             filesystem.UserID,
	})
	if err != nil {
		t.Fatalf("ForkSandboxFilesystem() error = %v", err)
	}
	if forked.SourceFilesystemID == nil || *forked.SourceFilesystemID != filesystem.ID {
		t.Fatalf("fork source = %v, want %s", forked.SourceFilesystemID, filesystem.ID)
	}
	if forked.S0FSHead != "" {
		t.Fatalf("forked S0FSHead = %q, want empty", forked.S0FSHead)
	}
	if _, ok := repo.fsHeads[filesystem.ID]; ok {
		t.Fatal("source empty filesystem unexpectedly created a committed head")
	}
	if _, ok := repo.fsHeads[forked.ID]; ok {
		t.Fatal("forked empty filesystem unexpectedly created a committed head")
	}
}

func TestCreateSandboxFilesystemSnapshotAllowsEmptyFilesystem(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()
	mgr := newTestSandboxFilesystemManager(t, repo)
	filesystem := &db.SandboxFilesystem{
		ID:              "fs-empty-snapshot",
		TeamID:          "team-1",
		UserID:          "user-1",
		BaseImageDigest: "sha256:base",
		State:           db.SandboxFilesystemStateAvailable,
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}
	repo.filesystems[filesystem.ID] = cloneFakeSandboxFilesystem(filesystem)

	snapshot, err := mgr.CreateSandboxFilesystemSnapshot(ctx, &CreateSandboxFilesystemSnapshotRequest{
		FilesystemID: filesystem.ID,
		TeamID:       filesystem.TeamID,
		UserID:       filesystem.UserID,
		Name:         "empty",
	})
	if err != nil {
		t.Fatalf("CreateSandboxFilesystemSnapshot() error = %v", err)
	}
	if snapshot.S0FSHead != "" {
		t.Fatalf("snapshot S0FSHead = %q, want empty", snapshot.S0FSHead)
	}
	if snapshot.SizeBytes != 0 {
		t.Fatalf("snapshot SizeBytes = %d, want 0", snapshot.SizeBytes)
	}
	if _, ok := repo.fsHeads[filesystem.ID]; ok {
		t.Fatal("empty snapshot unexpectedly created a committed head")
	}
}

func TestCreateSandboxFilesystemFromEmptySnapshotKeepsEmptyHead(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()
	mgr := newTestSandboxFilesystemManager(t, repo)
	filesystem := &db.SandboxFilesystem{
		ID:              "fs-empty-snapshot-source",
		TeamID:          "team-1",
		UserID:          "user-1",
		BaseImageDigest: "sha256:base",
		State:           db.SandboxFilesystemStateAvailable,
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}
	snapshot := &db.SandboxFilesystemSnapshot{
		ID:              "snap-empty",
		FilesystemID:    filesystem.ID,
		TeamID:          filesystem.TeamID,
		UserID:          filesystem.UserID,
		BaseImageDigest: filesystem.BaseImageDigest,
		Name:            "empty",
		CreatedAt:       time.Now().UTC(),
	}
	repo.filesystems[filesystem.ID] = cloneFakeSandboxFilesystem(filesystem)
	repo.fsSnapshots[snapshot.ID] = cloneFakeSandboxFilesystemSnapshot(snapshot)

	created, err := mgr.CreateSandboxFilesystemFromSnapshot(ctx, &CreateSandboxFilesystemFromSnapshotRequest{
		SnapshotID: snapshot.ID,
		TeamID:     filesystem.TeamID,
		UserID:     filesystem.UserID,
	})
	if err != nil {
		t.Fatalf("CreateSandboxFilesystemFromSnapshot() error = %v", err)
	}
	if created.S0FSHead != "" {
		t.Fatalf("created S0FSHead = %q, want empty", created.S0FSHead)
	}
	if _, ok := repo.fsHeads[created.ID]; ok {
		t.Fatal("created empty filesystem unexpectedly created a committed head")
	}
}

func TestRestoreSandboxFilesystemSnapshotAllowsEmptyHead(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()
	mgr := newTestSandboxFilesystemManager(t, repo)
	filesystem := &db.SandboxFilesystem{
		ID:              "fs-restore-empty",
		TeamID:          "team-1",
		UserID:          "user-1",
		BaseImageDigest: "sha256:base",
		S0FSHead:        s0fsManifestKey(2),
		State:           db.SandboxFilesystemStateAvailable,
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}
	snapshot := &db.SandboxFilesystemSnapshot{
		ID:              "snap-empty-restore",
		FilesystemID:    filesystem.ID,
		TeamID:          filesystem.TeamID,
		UserID:          filesystem.UserID,
		BaseImageDigest: filesystem.BaseImageDigest,
		Name:            "empty",
		CreatedAt:       time.Now().UTC(),
	}
	repo.filesystems[filesystem.ID] = cloneFakeSandboxFilesystem(filesystem)
	repo.fsSnapshots[snapshot.ID] = cloneFakeSandboxFilesystemSnapshot(snapshot)
	repo.fsHeads[filesystem.ID] = &db.SandboxFilesystemS0FSCommittedHead{
		FilesystemID:  filesystem.ID,
		ManifestSeq:   2,
		CheckpointSeq: 2,
		ManifestKey:   s0fsManifestKey(2),
		UpdatedAt:     time.Now().UTC(),
	}

	restored, err := mgr.RestoreSandboxFilesystemSnapshot(ctx, &RestoreSandboxFilesystemSnapshotRequest{
		FilesystemID: filesystem.ID,
		SnapshotID:   snapshot.ID,
		TeamID:       filesystem.TeamID,
		UserID:       filesystem.UserID,
	})
	if err != nil {
		t.Fatalf("RestoreSandboxFilesystemSnapshot() error = %v", err)
	}
	if restored.S0FSHead != "" {
		t.Fatalf("restored S0FSHead = %q, want empty", restored.S0FSHead)
	}
	if _, ok := repo.fsHeads[filesystem.ID]; ok {
		t.Fatal("empty snapshot restore did not clear committed head")
	}
}
