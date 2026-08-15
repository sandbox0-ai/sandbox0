package db

import (
	"context"
	"encoding/json"
	"errors"
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
		VolumeID:       volumeID,
		ManifestSeq:    7,
		CheckpointSeq:  7,
		ManifestKey:    "manifests/00000000000000000007.json",
		ManifestDigest: "digest-7",
		CommitID:       "commit-7",
		Generation:     1,
		UpdatedAt:      time.Now().UTC(),
	}
	if err := repo.BeginS0FSCommit(ctx, volumeID, headOne.CommitID, nil, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("BeginS0FSCommit(first) error = %v", err)
	}
	if err := repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, nil, headOne); err != nil {
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
		VolumeID:       volumeID,
		ManifestSeq:    9,
		CheckpointSeq:  9,
		ManifestKey:    "manifests/00000000000000000009.json",
		ManifestDigest: "digest-9",
		CommitID:       "commit-9",
		Generation:     2,
		UpdatedAt:      time.Now().UTC(),
	}
	if err := repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, nil, headTwo); err != ErrConflict {
		t.Fatalf("CompareAndSwapS0FSCommittedHead(stale insert) err = %v, want %v", err, ErrConflict)
	}
	if err := repo.BeginS0FSCommit(ctx, volumeID, headTwo.CommitID, headOne, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("BeginS0FSCommit(second) error = %v", err)
	}
	if err := repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, headOne, headTwo); err != nil {
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

func TestS0FSConcurrentInitialCommitsReturnExactCASConflict(t *testing.T) {
	repo := newS0FSCommittedHeadTestRepository(t)
	if repo == nil {
		return
	}

	ctx := context.Background()
	volumeID := "vol-" + uuid.NewString()
	createTestSandboxVolume(t, repo, volumeID)
	heads := []*S0FSCommittedHead{
		{
			VolumeID: volumeID, ManifestSeq: 1, CheckpointSeq: 1,
			ManifestKey: "manifests/00000000000000000001-a.json", ManifestDigest: "digest-a",
			CommitID: "commit-a", Generation: 1, UpdatedAt: time.Now().UTC(),
		},
		{
			VolumeID: volumeID, ManifestSeq: 1, CheckpointSeq: 1,
			ManifestKey: "manifests/00000000000000000001-b.json", ManifestDigest: "digest-b",
			CommitID: "commit-b", Generation: 1, UpdatedAt: time.Now().UTC(),
		},
	}
	for _, head := range heads {
		if err := repo.BeginS0FSCommit(ctx, volumeID, head.CommitID, nil, time.Now().Add(time.Minute)); err != nil {
			t.Fatalf("BeginS0FSCommit(%s) error = %v", head.CommitID, err)
		}
	}

	start := make(chan struct{})
	results := make(chan error, len(heads))
	for _, head := range heads {
		head := head
		go func() {
			<-start
			results <- repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, nil, head)
		}()
	}
	close(start)

	var succeeded, conflicted int
	for range heads {
		switch err := <-results; {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrConflict):
			conflicted++
		default:
			t.Fatalf("concurrent initial CAS returned non-conflict error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("concurrent initial CAS results = succeeded:%d conflicted:%d, want 1/1", succeeded, conflicted)
	}
}

func TestS0FSCommitAndGarbageCollectionFencingWithDurableGrace(t *testing.T) {
	repo := newS0FSCommittedHeadTestRepository(t)
	if repo == nil {
		return
	}

	ctx := context.Background()
	volumeID := "vol-" + uuid.NewString()
	createTestSandboxVolume(t, repo, volumeID)
	head := &S0FSCommittedHead{
		VolumeID: volumeID, ManifestSeq: 2, CheckpointSeq: 2,
		ManifestKey: "manifests/00000000000000000002-base.json", ManifestDigest: "digest-base",
		CommitID: "commit-base", Generation: 1, UpdatedAt: time.Now().UTC(),
	}
	if err := repo.BeginS0FSCommit(ctx, volumeID, head.CommitID, nil, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("BeginS0FSCommit(base) error = %v", err)
	}
	if err := repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, nil, head); err != nil {
		t.Fatalf("CompareAndSwapS0FSCommittedHead(base) error = %v", err)
	}

	gcToken := "gc-" + uuid.NewString()
	if err := repo.AcquireS0FSGarbageCollection(ctx, volumeID, gcToken, head, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("AcquireS0FSGarbageCollection() error = %v", err)
	}
	if err := repo.BeginS0FSCommit(ctx, volumeID, "commit-blocked", head, time.Now().Add(time.Minute)); err != ErrConflict {
		t.Fatalf("BeginS0FSCommit(during GC) error = %v, want ErrConflict", err)
	}
	if err := repo.ReleaseS0FSGarbageCollection(ctx, volumeID, gcToken); err != nil {
		t.Fatalf("ReleaseS0FSGarbageCollection() error = %v", err)
	}

	commitID := "commit-" + uuid.NewString()
	if err := repo.BeginS0FSCommit(ctx, volumeID, commitID, head, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("BeginS0FSCommit() error = %v", err)
	}
	if err := repo.AcquireS0FSGarbageCollection(ctx, volumeID, gcToken, head, time.Now().Add(time.Minute)); err != ErrConflict {
		t.Fatalf("AcquireS0FSGarbageCollection(during commit) error = %v, want ErrConflict", err)
	}
	if err := repo.AbortS0FSCommit(ctx, volumeID, commitID); err != nil {
		t.Fatalf("AbortS0FSCommit() error = %v", err)
	}
	if err := repo.AcquireS0FSGarbageCollection(ctx, volumeID, gcToken, head, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("AcquireS0FSGarbageCollection(after abort) error = %v", err)
	}

	candidates := []string{"segments/orphan.bin", "manifests/orphan.json"}
	due, err := repo.StageS0FSGarbageCollection(ctx, volumeID, gcToken, head, candidates, time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatalf("StageS0FSGarbageCollection(first) error = %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("new tombstones due = %v, want none before grace", due)
	}
	if _, err := repo.pool.Exec(ctx, `
		UPDATE sandbox_volume_s0fs_gc_tombstones SET delete_after = NOW() - INTERVAL '1 second'
		WHERE volume_id = $1
	`, volumeID); err != nil {
		t.Fatalf("expire tombstones: %v", err)
	}
	due, err = repo.StageS0FSGarbageCollection(ctx, volumeID, gcToken, head, candidates, time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatalf("StageS0FSGarbageCollection(second) error = %v", err)
	}
	if len(due) != len(candidates) {
		t.Fatalf("grace-expired tombstones due = %v, want %v", due, candidates)
	}
	staleHead := *head
	staleHead.ManifestDigest = "stale"
	if err := repo.ValidateS0FSGarbageCollection(ctx, volumeID, gcToken, &staleHead); err != ErrConflict {
		t.Fatalf("ValidateS0FSGarbageCollection(stale head) error = %v, want ErrConflict", err)
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
		VolumeID:       volumeID,
		ManifestSeq:    3,
		CheckpointSeq:  3,
		ManifestKey:    "manifests/00000000000000000003.json",
		ManifestDigest: "digest-3",
		CommitID:       "commit-3",
		Generation:     1,
		UpdatedAt:      time.Now().UTC(),
	}
	if err := store.BeginCommit(ctx, volumeID, first.CommitID, nil, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("BeginCommit(first) error = %v", err)
	}
	if err := store.CompareAndSwapCommittedHead(ctx, volumeID, nil, first); err != nil {
		t.Fatalf("CompareAndSwapCommittedHead(first) error = %v", err)
	}

	second := &s0fs.CommittedHead{
		VolumeID:       volumeID,
		ManifestSeq:    4,
		CheckpointSeq:  4,
		ManifestKey:    "manifests/00000000000000000004.json",
		ManifestDigest: "digest-4",
		CommitID:       "commit-4",
		Generation:     2,
		UpdatedAt:      time.Now().UTC(),
	}
	if err := store.CompareAndSwapCommittedHead(ctx, volumeID, nil, second); err != s0fs.ErrCommittedHeadConflict {
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

func TestRequireS0FSHeadIdentityPreservesCanceledContext(t *testing.T) {
	repo := newS0FSCommittedHeadTestRepository(t)
	if repo == nil {
		return
	}

	volumeID := "vol-" + uuid.NewString()
	createTestSandboxVolume(t, repo, volumeID)
	tx, err := repo.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = repo.requireS0FSHeadIdentity(canceledCtx, tx, volumeID, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("requireS0FSHeadIdentity() error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrConflict) {
		t.Fatalf("requireS0FSHeadIdentity() error = %v, must not be classified as ErrConflict", err)
	}
}

func TestListSandboxVolumesBySource(t *testing.T) {
	repo := newS0FSCommittedHeadTestRepository(t)
	if repo == nil {
		return
	}

	ctx := context.Background()
	sourceID := "vol-" + uuid.NewString()
	childID := "vol-" + uuid.NewString()
	unrelatedID := "vol-" + uuid.NewString()
	createTestSandboxVolume(t, repo, sourceID)
	now := time.Now().UTC()
	if err := repo.CreateSandboxVolume(ctx, &SandboxVolume{
		ID:             childID,
		TeamID:         "team-1",
		UserID:         "user-1",
		SourceVolumeID: &sourceID,
		AccessMode:     "RWO",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("CreateSandboxVolume(child) error = %v", err)
	}
	if err := repo.CreateSandboxVolume(ctx, &SandboxVolume{
		ID:         unrelatedID,
		TeamID:     "team-1",
		UserID:     "user-1",
		AccessMode: "RWO",
		CreatedAt:  now,
		UpdatedAt:  now,
	}); err != nil {
		t.Fatalf("CreateSandboxVolume(unrelated) error = %v", err)
	}

	children, err := repo.ListSandboxVolumesBySource(ctx, sourceID)
	if err != nil {
		t.Fatalf("ListSandboxVolumesBySource() error = %v", err)
	}
	if len(children) != 1 || children[0].ID != childID {
		t.Fatalf("children = %+v, want child %s", children, childID)
	}
}

func TestSandboxVolumeBackendConfigRoundTrip(t *testing.T) {
	repo := newS0FSCommittedHeadTestRepository(t)
	if repo == nil {
		return
	}

	ctx := context.Background()
	now := time.Now().UTC()
	defaultID := "vol-" + uuid.NewString()
	if err := repo.CreateSandboxVolume(ctx, &SandboxVolume{
		ID:         defaultID,
		TeamID:     "team-1",
		UserID:     "user-1",
		AccessMode: "RWO",
		CreatedAt:  now,
		UpdatedAt:  now,
	}); err != nil {
		t.Fatalf("CreateSandboxVolume(default) error = %v", err)
	}
	defaultVol, err := repo.GetSandboxVolume(ctx, defaultID)
	if err != nil {
		t.Fatalf("GetSandboxVolume(default) error = %v", err)
	}
	if defaultVol.Backend != "s0fs" {
		t.Fatalf("default backend = %q, want s0fs", defaultVol.Backend)
	}
	if string(defaultVol.BackendConfig) != "{}" {
		t.Fatalf("default backend_config = %s, want {}", string(defaultVol.BackendConfig))
	}

	s3ID := "vol-" + uuid.NewString()
	rawConfig := json.RawMessage(`{"provider":"aws","bucket":"sandbox-data","prefix":"team-a/vol-a","access_key":"ak","secret_key":"sk"}`)
	if err := repo.CreateSandboxVolume(ctx, &SandboxVolume{
		ID:            s3ID,
		TeamID:        "team-1",
		UserID:        "user-1",
		AccessMode:    "RWO",
		Backend:       "s3",
		BackendConfig: rawConfig,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatalf("CreateSandboxVolume(s3) error = %v", err)
	}
	s3Vol, err := repo.GetSandboxVolume(ctx, s3ID)
	if err != nil {
		t.Fatalf("GetSandboxVolume(s3) error = %v", err)
	}
	if s3Vol.Backend != "s3" {
		t.Fatalf("s3 backend = %q, want s3", s3Vol.Backend)
	}
	if !json.Valid(s3Vol.BackendConfig) {
		t.Fatalf("backend_config is not valid JSON: %s", string(s3Vol.BackendConfig))
	}
	var decoded map[string]string
	if err := json.Unmarshal(s3Vol.BackendConfig, &decoded); err != nil {
		t.Fatalf("unmarshal backend_config: %v", err)
	}
	if decoded["bucket"] != "sandbox-data" || decoded["prefix"] != "team-a/vol-a" || decoded["secret_key"] != "sk" {
		t.Fatalf("backend_config = %#v", decoded)
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
		MaxConns:    4,
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
