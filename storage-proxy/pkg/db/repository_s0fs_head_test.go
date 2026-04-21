package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	storagemigrations "github.com/sandbox0-ai/sandbox0/storage-proxy/migrations"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

func TestS0FSCommittedHeadCompareAndSwapLifecycle(t *testing.T) {
	repo := newS0FSCommittedHeadTestRepository(t)
	if repo == nil {
		return
	}

	ctx := context.Background()
	volumeID := "vol-" + uuid.NewString()
	createTestSandboxVolume(t, repo, volumeID)

	headOne := &S0FSCommittedHead{
		VolumeID:      volumeID,
		ManifestSeq:   7,
		CheckpointSeq: 7,
		ManifestKey:   "manifests/00000000000000000007.json",
		UpdatedAt:     time.Now().UTC(),
	}
	if err := repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, 0, headOne); err != nil {
		t.Fatalf("CompareAndSwapS0FSCommittedHead(insert) error = %v", err)
	}

	loaded, err := repo.GetS0FSCommittedHead(ctx, volumeID)
	if err != nil {
		t.Fatalf("GetS0FSCommittedHead() error = %v", err)
	}
	if loaded.ManifestSeq != headOne.ManifestSeq || loaded.ManifestKey != headOne.ManifestKey {
		t.Fatalf("loaded head = %+v, want %+v", loaded, headOne)
	}

	headTwo := &S0FSCommittedHead{
		VolumeID:      volumeID,
		ManifestSeq:   9,
		CheckpointSeq: 9,
		ManifestKey:   "manifests/00000000000000000009.json",
		UpdatedAt:     time.Now().UTC(),
	}
	if err := repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, 0, headTwo); err != ErrConflict {
		t.Fatalf("CompareAndSwapS0FSCommittedHead(stale insert) err = %v, want %v", err, ErrConflict)
	}
	if err := repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, headOne.ManifestSeq, headTwo); err != nil {
		t.Fatalf("CompareAndSwapS0FSCommittedHead(update) error = %v", err)
	}
	loaded, err = repo.GetS0FSCommittedHead(ctx, volumeID)
	if err != nil {
		t.Fatalf("GetS0FSCommittedHead(after update) error = %v", err)
	}
	if loaded.ManifestSeq != headTwo.ManifestSeq || loaded.ManifestKey != headTwo.ManifestKey {
		t.Fatalf("loaded head after update = %+v, want %+v", loaded, headTwo)
	}
}

func TestS0FSHeadStoreAdapterMapsConflicts(t *testing.T) {
	repo := newS0FSCommittedHeadTestRepository(t)
	if repo == nil {
		return
	}

	ctx := context.Background()
	volumeID := "vol-" + uuid.NewString()
	createTestSandboxVolume(t, repo, volumeID)
	store := NewS0FSHeadStore(repo)

	first := &s0fs.CommittedHead{
		VolumeID:      volumeID,
		ManifestSeq:   3,
		CheckpointSeq: 3,
		ManifestKey:   "manifests/00000000000000000003.json",
		UpdatedAt:     time.Now().UTC(),
	}
	if err := store.CompareAndSwapCommittedHead(ctx, volumeID, 0, first); err != nil {
		t.Fatalf("CompareAndSwapCommittedHead(first) error = %v", err)
	}

	second := &s0fs.CommittedHead{
		VolumeID:      volumeID,
		ManifestSeq:   4,
		CheckpointSeq: 4,
		ManifestKey:   "manifests/00000000000000000004.json",
		UpdatedAt:     time.Now().UTC(),
	}
	if err := store.CompareAndSwapCommittedHead(ctx, volumeID, 0, second); err != s0fs.ErrCommittedHeadConflict {
		t.Fatalf("CompareAndSwapCommittedHead(conflict) err = %v, want %v", err, s0fs.ErrCommittedHeadConflict)
	}

	loaded, err := store.LoadCommittedHead(ctx, volumeID)
	if err != nil {
		t.Fatalf("LoadCommittedHead() error = %v", err)
	}
	if loaded.ManifestSeq != first.ManifestSeq || loaded.ManifestKey != first.ManifestKey {
		t.Fatalf("loaded committed head = %+v, want %+v", loaded, first)
	}
}

func TestAcquireMountRejectsConflictingRWOMount(t *testing.T) {
	repo := newS0FSCommittedHeadTestRepository(t)
	if repo == nil {
		return
	}

	ctx := context.Background()
	volumeID := "vol-" + uuid.NewString()
	createTestSandboxVolume(t, repo, volumeID)

	first := &VolumeMount{
		ID:            uuid.NewString(),
		VolumeID:      volumeID,
		ClusterID:     "cluster-a",
		PodID:         "pod-a",
		LastHeartbeat: time.Now().UTC(),
		MountedAt:     time.Now().UTC(),
		MountOptions:  mustMountOptionsRaw(t, "RWO"),
	}
	if err := repo.AcquireMount(ctx, first, 15); err != nil {
		t.Fatalf("AcquireMount(first) error = %v", err)
	}

	second := &VolumeMount{
		ID:            uuid.NewString(),
		VolumeID:      volumeID,
		ClusterID:     "cluster-b",
		PodID:         "pod-b",
		LastHeartbeat: time.Now().UTC(),
		MountedAt:     time.Now().UTC(),
		MountOptions:  mustMountOptionsRaw(t, "RWO"),
	}
	if err := repo.AcquireMount(ctx, second, 15); err != ErrConflict {
		t.Fatalf("AcquireMount(second) err = %v, want %v", err, ErrConflict)
	}
}

func TestAcquireMountAllowsROXSharing(t *testing.T) {
	repo := newS0FSCommittedHeadTestRepository(t)
	if repo == nil {
		return
	}

	ctx := context.Background()
	volumeID := "vol-" + uuid.NewString()
	now := time.Now().UTC()
	if err := repo.CreateSandboxVolume(ctx, &SandboxVolume{
		ID:         volumeID,
		TeamID:     "team-1",
		UserID:     "user-1",
		AccessMode: "ROX",
		CreatedAt:  now,
		UpdatedAt:  now,
	}); err != nil {
		t.Fatalf("CreateSandboxVolume(%s) error = %v", volumeID, err)
	}

	first := &VolumeMount{
		ID:            uuid.NewString(),
		VolumeID:      volumeID,
		ClusterID:     "cluster-a",
		PodID:         "pod-a",
		LastHeartbeat: time.Now().UTC(),
		MountedAt:     time.Now().UTC(),
		MountOptions:  mustMountOptionsRaw(t, "ROX"),
	}
	if err := repo.AcquireMount(ctx, first, 15); err != nil {
		t.Fatalf("AcquireMount(first) error = %v", err)
	}

	second := &VolumeMount{
		ID:            uuid.NewString(),
		VolumeID:      volumeID,
		ClusterID:     "cluster-b",
		PodID:         "pod-b",
		LastHeartbeat: time.Now().UTC(),
		MountedAt:     time.Now().UTC(),
		MountOptions:  mustMountOptionsRaw(t, "ROX"),
	}
	if err := repo.AcquireMount(ctx, second, 15); err != nil {
		t.Fatalf("AcquireMount(second) error = %v", err)
	}
}

func newS0FSCommittedHeadTestRepository(t *testing.T) *Repository {
	t.Helper()

	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
		return nil
	}

	ctx := context.Background()
	schema := fmt.Sprintf("storage_proxy_s0fs_head_test_%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: dbURL,
		Schema:      schema,
	})
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
		pool.Close()
	})

	if err := migrate.Up(ctx, pool, ".", migrate.WithBaseFS(storagemigrations.FS), migrate.WithSchema(schema)); err != nil {
		t.Fatalf("migrate storage-proxy schema: %v", err)
	}
	return NewRepository(pool)
}

func createTestSandboxVolume(t *testing.T, repo *Repository, volumeID string) {
	t.Helper()

	now := time.Now().UTC()
	if err := repo.CreateSandboxVolume(context.Background(), &SandboxVolume{
		ID:         volumeID,
		TeamID:     "team-1",
		UserID:     "user-1",
		AccessMode: "RWO",
		CreatedAt:  now,
		UpdatedAt:  now,
	}); err != nil {
		t.Fatalf("CreateSandboxVolume(%s) error = %v", volumeID, err)
	}
}

func mustMountOptionsRaw(t *testing.T, accessMode string) *json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(struct {
		AccessMode string `json:"access_mode"`
	}{
		AccessMode: accessMode,
	})
	if err != nil {
		t.Fatalf("marshal mount options: %v", err)
	}
	msg := json.RawMessage(raw)
	return &msg
}
