package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRootFSQuotaRejectsBeforePublishAndRollsBackInventory(t *testing.T) {
	ctx := context.Background()
	pool, store, quotaRepo := newRootFSQuotaIntegration(t, 100)
	var publishCalls atomic.Int64
	var abortCalls atomic.Int64
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{
				Handle: "handle-over-limit",
				Descriptor: ctldapi.RootFSDiffDescriptor{
					Digest:    "sha256:over-limit",
					MediaType: "application/vnd.oci.image.layer.v1.tar",
					Size:      101,
				},
			}))
		case "/api/v1/rootfs/snapshots/publish":
			publishCalls.Add(1)
			t.Fatal("publish must not run after quota rejection")
		case "/api/v1/rootfs/snapshots/abort":
			abortCalls.Add(1)
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.AbortRootFSSnapshotResponse{Aborted: true}))
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()

	svc := &SandboxService{
		sandboxStore:   store,
		teamQuotaStore: quotaRepo,
		ctldClient:     NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		logger:         zap.NewNop(),
		clock:          systemTime{},
	}
	_, err := svc.prepareAndPublishSandboxRootFSSnapshot(
		ctx,
		ctld.URL,
		ctldapi.PrepareRootFSSnapshotRequest{},
		"sandbox-1",
		"team-1",
		1,
		"layer-over-limit",
	)
	require.Error(t, err)
	assert.True(t, teamquota.IsExceeded(err), "unexpected error: %v", err)
	assert.Equal(t, int64(0), publishCalls.Load())
	assert.Equal(t, int64(1), abortCalls.Load())
	assertRootFSQuotaObjectCount(t, pool, "rootfs_objects", 0)
	assertRootFSQuotaObjectCount(t, pool, "rootfs_object_deletions", 0)
	assertRootFSQuotaStatus(t, quotaRepo, "team-1", 0, 0)
	require.NoError(t, quotaRepo.ValidateUsageInvariant(ctx, "team-1"))
}

func TestRootFSObjectDeletionFailsClosedWithoutTransactionalQuotaStore(t *testing.T) {
	ctx := context.Background()
	pool, store, quotaRepo := newRootFSQuotaIntegration(t, 1024)
	state := rootFSTestStoreState("sandbox-delete-guard", "team-1", "layer-delete-guard", "", 1, "delete-guard")
	require.NoError(t, prepareRootFSObjectPublishForTest(ctx, store, state, time.Now().Add(-time.Minute), quotaRepo))
	deleter := &recordingRootFSObjectDeleter{}

	deleted, err := store.DeletePendingRootFSObjectsWithOptions(
		ctx,
		deleter,
		DeletePendingRootFSObjectsOptions{Limit: 1},
	)

	require.ErrorIs(t, err, ErrTeamQuotaUnavailable)
	assert.Empty(t, deleted)
	assert.Empty(t, deleter.keys)
	assertRootFSQuotaObjectCount(t, pool, "rootfs_object_deletions", 1)
	assertRootFSQuotaStatus(t, quotaRepo, "team-1", state.DiffSize, 0)
}

func TestForkSandboxIdentityQuotaCommitsWithTargetRecordAndFilesystem(t *testing.T) {
	ctx := context.Background()
	_, store, quotaRepo := newRootFSQuotaIntegration(t, 1024)
	source := rootFSTestSandboxRecord("sandbox-source", "team-1")
	source.Status = SandboxStatusPaused
	require.NoError(t, store.UpsertSandbox(ctx, source))
	require.NoError(t, store.SaveRootFSState(
		ctx,
		rootFSTestStoreState("sandbox-source", "team-1", "layer-source", "", 1, "fork-source"),
	))
	require.NoError(t, quotaRepo.UnsafePutTeamPolicyForTest(ctx, "team-1", teamquota.Policy{
		Key:   teamquota.KeySandboxIdentityCount,
		Kind:  teamquota.KindCapacity,
		Limit: 1,
	}))
	sourceOwner := teamquota.Owner{
		TeamID:    "team-1",
		Kind:      "sandbox",
		ID:        source.ID,
		ClusterID: "default",
	}
	require.NoError(t, quotaRepo.ReconcileTarget(
		ctx,
		sourceOwner,
		pausedSandboxTeamQuotaTarget(),
		teamquota.RuntimeRef{},
	))
	target := rootFSTestSandboxRecord("sandbox-fork", "team-1")
	target.Status = SandboxStatusPaused
	svc := &SandboxService{
		sandboxStore:   store,
		teamQuotaStore: quotaRepo,
		clock:          systemTime{},
		logger:         zap.NewNop(),
	}

	err := svc.commitForkSandbox(ctx, store, source.ID, source.TeamID, target, nil)

	require.ErrorIs(t, err, ErrQuotaExceeded)
	storedTarget, getErr := store.GetSandbox(ctx, target.ID)
	require.NoError(t, getErr)
	assert.Nil(t, storedTarget)
	filesystem, fsErr := store.GetRootFSFilesystem(ctx, target.ID)
	require.NoError(t, fsErr)
	assert.Nil(t, filesystem)
	assertTeamQuotaKeyStatus(t, quotaRepo, "team-1", teamquota.KeySandboxIdentityCount, 1, 0)

	require.NoError(t, quotaRepo.UnsafePutTeamPolicyForTest(ctx, "team-1", teamquota.Policy{
		Key:   teamquota.KeySandboxIdentityCount,
		Kind:  teamquota.KindCapacity,
		Limit: 2,
	}))
	require.NoError(t, svc.commitForkSandbox(ctx, store, source.ID, source.TeamID, target, nil))
	storedTarget, getErr = store.GetSandbox(ctx, target.ID)
	require.NoError(t, getErr)
	assert.Equal(t, SandboxStatusPaused, storedTarget.Status)
	filesystem, fsErr = store.GetRootFSFilesystem(ctx, target.ID)
	require.NoError(t, fsErr)
	require.NotNil(t, filesystem)
	assert.Equal(t, "sandbox-source", filesystem.SourceFilesystemID)
	assertTeamQuotaKeyStatus(t, quotaRepo, "team-1", teamquota.KeySandboxIdentityCount, 2, 0)
	require.NoError(t, quotaRepo.ValidateUsageInvariant(ctx, "team-1"))
}

func TestRootFSPublishFailureReleasesQuotaOnlyAfterConfirmedDelete(t *testing.T) {
	tests := []struct {
		name          string
		deleteErr     error
		wantCommitted int64
		wantQueued    int64
		wantDeleted   bool
	}{
		{
			name:          "delete succeeds",
			wantCommitted: 0,
			wantQueued:    0,
			wantDeleted:   true,
		},
		{
			name:          "delete fails",
			deleteErr:     errors.New("object store unavailable"),
			wantCommitted: 64,
			wantQueued:    1,
			wantDeleted:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pool, store, quotaRepo := newRootFSQuotaIntegration(t, 1024)
			ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v1/rootfs/snapshots/prepare":
					require.NoError(t, json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{
						Handle: "handle-publish-failure",
						Descriptor: ctldapi.RootFSDiffDescriptor{
							Digest:    "sha256:publish-failure",
							MediaType: "application/vnd.oci.image.layer.v1.tar",
							Size:      64,
						},
					}))
				case "/api/v1/rootfs/snapshots/publish":
					http.Error(w, `{"error":"publish failed"}`, http.StatusInternalServerError)
				case "/api/v1/rootfs/snapshots/abort":
					require.NoError(t, json.NewEncoder(w).Encode(ctldapi.AbortRootFSSnapshotResponse{Aborted: true}))
				default:
					t.Fatalf("unexpected ctld path %s", r.URL.Path)
				}
			}))
			defer ctld.Close()
			deleter := &recordingRootFSObjectDeleter{err: tt.deleteErr}
			if tt.deleteErr != nil {
				deleter.failKey = "sandbox-rootfs/team-1/sandbox-1/1/sha256/publish-failure.tar"
			}
			svc := &SandboxService{
				sandboxStore:        store,
				teamQuotaStore:      quotaRepo,
				rootFSObjectDeleter: deleter,
				ctldClient:          NewCtldClient(CtldClientConfig{Timeout: time.Second}),
				logger:              zap.NewNop(),
				clock:               systemTime{},
			}
			_, err := svc.prepareAndPublishSandboxRootFSSnapshot(
				ctx,
				ctld.URL,
				ctldapi.PrepareRootFSSnapshotRequest{},
				"sandbox-1",
				"team-1",
				1,
				"layer-publish-failure",
			)
			require.Error(t, err, "publish failure must be returned")
			assert.Empty(t, deleter.keys, "request path must leave durable cleanup to the claimed GC worker")
			assertRootFSQuotaStatus(t, quotaRepo, "team-1", 64, 0)
			assertRootFSQuotaObjectCount(t, pool, "rootfs_object_deletions", 1)

			deletedKeys, deleteErr := store.DeletePendingRootFSObjectsWithOptions(
				ctx,
				deleter,
				DeletePendingRootFSObjectsOptions{
					Limit:            1,
					ClaimedBy:        "publish-failure-cleanup",
					ClaimTTL:         time.Minute,
					ObjectQuotaStore: quotaRepo,
				},
			)
			if tt.deleteErr != nil {
				require.Error(t, deleteErr)
				assert.Empty(t, deletedKeys)
			} else {
				require.NoError(t, deleteErr)
				require.Len(t, deletedKeys, 1)
			}
			require.Len(t, deleter.keys, 1)
			assertRootFSQuotaStatus(t, quotaRepo, "team-1", tt.wantCommitted, 0)
			assertRootFSQuotaObjectCount(t, pool, "rootfs_object_deletions", tt.wantQueued)
			var deleted bool
			require.NoError(t, pool.QueryRow(ctx, `
				SELECT deleted_at IS NOT NULL
				FROM manager.rootfs_objects
				WHERE object_key = $1
			`, deleter.keys[0]).Scan(&deleted))
			assert.Equal(t, tt.wantDeleted, deleted)
			require.NoError(t, quotaRepo.ValidateUsageInvariant(ctx, "team-1"))
		})
	}
}

func TestRootFSGCCompletionFailureRetainsQueueForRetry(t *testing.T) {
	ctx := context.Background()
	pool, store, quotaRepo := newRootFSQuotaIntegration(t, 1024)
	state := rootFSTestStoreState("sandbox-1", "team-1", "layer-gc", "", 1, "gc-quota")
	state.DiffSize = 80
	require.NoError(t, prepareRootFSObjectPublishForTest(ctx, store, state, time.Now().Add(-time.Minute), quotaRepo))
	deleter := &recordingRootFSObjectDeleter{}
	failingQuotaStore := &rootFSConfirmReleaseFailOnceStore{CapacityTxStore: quotaRepo}
	opts := DeletePendingRootFSObjectsOptions{
		Limit:            1,
		ClaimedBy:        "quota-callback-worker",
		ClaimTTL:         time.Minute,
		BackoffBase:      time.Millisecond,
		BackoffMax:       time.Millisecond,
		ObjectQuotaStore: failingQuotaStore,
	}
	deleted, err := store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, opts)
	require.Error(t, err)
	assert.Empty(t, deleted)
	assertRootFSQuotaObjectCount(t, pool, "rootfs_object_deletions", 1)
	assertRootFSQuotaStatus(t, quotaRepo, "team-1", 80, 0)
	var deletedAt *time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted_at
		FROM manager.rootfs_objects
		WHERE object_key = $1
	`, state.DiffObjectKey).Scan(&deletedAt))
	assert.Nil(t, deletedAt, "quota and inventory completion must roll back together")

	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_object_deletions
		SET next_attempt_at = NOW(),
			claimed_by = '',
			claimed_until = NULL
		WHERE object_key = $1
	`, state.DiffObjectKey)
	require.NoError(t, err)
	deleted, err = store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, opts)
	require.NoError(t, err)
	assert.Equal(t, []string{state.DiffObjectKey}, deleted)
	assert.Equal(t, 2, failingQuotaStore.calls)
	assert.Equal(t, []string{state.DiffObjectKey, state.DiffObjectKey}, deleter.keys)
	assertRootFSQuotaObjectCount(t, pool, "rootfs_object_deletions", 0)
	assertRootFSQuotaStatus(t, quotaRepo, "team-1", 0, 0)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted_at
		FROM manager.rootfs_objects
		WHERE object_key = $1
	`, state.DiffObjectKey).Scan(&deletedAt))
	assert.NotNil(t, deletedAt)
	require.NoError(t, quotaRepo.ValidateUsageInvariant(ctx, "team-1"))
}

type rootFSConfirmReleaseFailOnceStore struct {
	teamquota.CapacityTxStore
	calls int
}

func (s *rootFSConfirmReleaseFailOnceStore) ConfirmReleaseTx(
	ctx context.Context,
	tx pgx.Tx,
	operation teamquota.OperationRef,
	runtime teamquota.RuntimeRef,
) error {
	s.calls++
	if s.calls == 1 {
		return errors.New("quota database unavailable")
	}
	return s.CapacityTxStore.ConfirmReleaseTx(ctx, tx, operation, runtime)
}

func TestReconcileRootFSObjectQuotaAdoptsActiveAndDeletedInventory(t *testing.T) {
	ctx := context.Background()
	pool, _, quotaRepo := newRootFSQuotaIntegration(t, 1024)
	_, err := pool.Exec(ctx, `
		INSERT INTO manager.rootfs_objects (
			object_key, team_id, diff_digest, diff_size, first_layer_id, deleted_at
		) VALUES
			('rootfs/active.tar', 'team-1', 'sha256:active', 90, 'layer-active', NULL),
			('rootfs/deleted.tar', 'team-1', 'sha256:deleted', 70, 'layer-deleted', NOW())
	`)
	require.NoError(t, err)
	deletedOwner, err := rootFSObjectQuotaOwner("team-1", "rootfs/deleted.tar")
	require.NoError(t, err)
	require.NoError(t, quotaRepo.ReconcileTarget(
		ctx,
		deletedOwner,
		teamquota.Values{teamquota.KeyRootFSStorageBytes: 70},
		teamquota.RuntimeRef{},
	))

	store := NewPGSandboxStore(pool)
	require.NoError(t, ReconcileRootFSObjectQuota(ctx, store, quotaRepo))
	assertRootFSQuotaStatus(t, quotaRepo, "team-1", 90, 0)
	require.NoError(t, quotaRepo.ValidateUsageInvariant(ctx, "team-1"))

	rows, err := pool.Query(ctx, `
		SELECT a.owner_id, i.committed_value
		FROM quota.allocations a
		JOIN quota.allocation_items i ON i.allocation_id = a.allocation_id
		WHERE a.team_id = 'team-1'
			AND a.owner_kind = 'rootfs_object'
			AND i.quota_key = 'rootfs_storage_bytes'
		ORDER BY a.owner_id
	`)
	require.NoError(t, err)
	defer rows.Close()
	values := make(map[string]int64)
	for rows.Next() {
		var key string
		var value int64
		require.NoError(t, rows.Scan(&key, &value))
		values[key] = value
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string]int64{
		"rootfs/active.tar":  90,
		"rootfs/deleted.tar": 0,
	}, values)
}

func TestRootFSObjectQuotaSupportsReconcileAndObjectKeyReuse(t *testing.T) {
	ctx := context.Background()
	pool, store, quotaRepo := newRootFSQuotaIntegration(t, 1024)
	state := rootFSTestStoreState("sandbox-reuse", "team-1", "layer-reuse", "", 1, "reuse")
	state.DiffSize = 60
	require.NoError(t, prepareRootFSObjectPublishForTest(ctx, store, state, time.Now().Add(time.Hour), quotaRepo))
	require.NoError(t, ReconcileRootFSObjectQuota(ctx, store, quotaRepo))

	require.NoError(t, prepareRootFSObjectPublishForTest(ctx, store, state, time.Now().Add(-time.Minute), quotaRepo))
	assertRootFSQuotaStatus(t, quotaRepo, "team-1", 60, 0)
	deleter := &recordingRootFSObjectDeleter{}
	deleted, err := store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, DeletePendingRootFSObjectsOptions{
		Limit:            1,
		ClaimedBy:        "reuse-delete",
		ClaimTTL:         time.Minute,
		ObjectQuotaStore: quotaRepo,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{state.DiffObjectKey}, deleted)
	assertRootFSQuotaStatus(t, quotaRepo, "team-1", 0, 0)

	require.NoError(t, prepareRootFSObjectPublishForTest(ctx, store, state, time.Now().Add(time.Hour), quotaRepo))
	assertRootFSQuotaStatus(t, quotaRepo, "team-1", 60, 0)
	var deletedAt *time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted_at
		FROM manager.rootfs_objects
		WHERE object_key = $1
	`, state.DiffObjectKey).Scan(&deletedAt))
	assert.Nil(t, deletedAt)
	require.NoError(t, quotaRepo.ValidateUsageInvariant(ctx, "team-1"))
}

func TestRootFSPhysicalDeleteSerializesConcurrentRepublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, store, quotaRepo := newRootFSQuotaIntegration(t, 1024)
	state := rootFSTestStoreState("sandbox-race", "team-1", "layer-race", "", 1, "delete-publish-race")
	state.DiffSize = 72
	require.NoError(t, prepareRootFSObjectPublishForTest(ctx, store, state, time.Now().Add(-time.Minute), quotaRepo))

	deleteEntered := make(chan string, 1)
	releaseDelete := make(chan struct{})
	deleter := &blockingRootFSObjectDeleter{
		entered: deleteEntered,
		release: releaseDelete,
	}
	type deleteResult struct {
		keys []string
		err  error
	}
	deleteDone := make(chan deleteResult, 1)
	go func() {
		keys, err := store.DeletePendingRootFSObjectsWithOptions(
			ctx,
			deleter,
			DeletePendingRootFSObjectsOptions{
				Limit:            1,
				ClaimedBy:        "delete-publish-race",
				ClaimTTL:         time.Minute,
				ObjectQuotaStore: quotaRepo,
			},
		)
		deleteDone <- deleteResult{keys: keys, err: err}
	}()
	require.Equal(t, state.DiffObjectKey, <-deleteEntered)

	republishDone := make(chan error, 1)
	go func() {
		republishDone <- prepareRootFSObjectPublishForTest(
			ctx,
			store,
			state,
			time.Now().Add(time.Hour),
			quotaRepo,
		)
	}()
	select {
	case err := <-republishDone:
		t.Fatalf("republish admission returned while physical deletion held the object lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseDelete)
	result := <-deleteDone
	require.NoError(t, result.err)
	assert.Equal(t, []string{state.DiffObjectKey}, result.keys)
	require.NoError(t, <-republishDone)

	var deleted bool
	var queued int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted_at IS NOT NULL
		FROM manager.rootfs_objects
		WHERE object_key = $1
	`, state.DiffObjectKey).Scan(&deleted))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM manager.rootfs_object_deletions
		WHERE object_key = $1
	`, state.DiffObjectKey).Scan(&queued))
	assert.False(t, deleted)
	assert.Equal(t, int64(1), queued)
	assertRootFSQuotaStatus(t, quotaRepo, state.TeamID, state.DiffSize, 0)
	assertTeamQuotaKeyStatus(t, quotaRepo, state.TeamID, teamquota.KeyStorageObjectCount, 1, 0)
	require.NoError(t, quotaRepo.ValidateUsageInvariant(ctx, state.TeamID))
}

type blockingRootFSObjectDeleter struct {
	entered chan<- string
	release <-chan struct{}
}

func (d *blockingRootFSObjectDeleter) Delete(key string) error {
	d.entered <- key
	<-d.release
	return nil
}

func TestRootFSObjectTombstoneRetentionBoundsHighChurnPerTeam(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRootFSQuotaIntegration(t, 1024)
	_, err := pool.Exec(ctx, `
		INSERT INTO manager.rootfs_objects (
			object_key, team_id, diff_digest, diff_size, first_layer_id, deleted_at
		)
		SELECT
			'rootfs/churn-' || value::TEXT || '.tar',
			'team-churn',
			'sha256:churn-' || value::TEXT,
			1,
			'layer-churn-' || value::TEXT,
			NOW()
		FROM generate_series(1, $1::BIGINT) AS value
	`, maxRootFSObjectTombstonesPerTeam+5)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.rootfs_objects (
			object_key, team_id, diff_digest, diff_size, first_layer_id, deleted_at
		) VALUES
			('rootfs/expired.tar', 'team-expired', 'sha256:expired', 1, 'layer-expired', NOW() - INTERVAL '25 hours'),
			('rootfs/fresh.tar', 'team-fresh', 'sha256:fresh', 1, 'layer-fresh', NOW())
	`)
	require.NoError(t, err)

	pruned, err := store.PruneDeletedRootFSObjectTombstones(ctx, "", 256)
	require.NoError(t, err)
	assert.Equal(t, 6, pruned)

	var count int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM manager.rootfs_objects
		WHERE team_id = 'team-churn'
	`).Scan(&count))
	assert.Equal(t, int64(maxRootFSObjectTombstonesPerTeam), count)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM manager.rootfs_objects
		WHERE object_key IN ('rootfs/expired.tar', 'rootfs/fresh.tar')
	`).Scan(&count))
	assert.Equal(t, int64(1), count)
}

func TestRootFSObjectTombstoneRetentionSkipsLockedAndRecoveryEvidence(t *testing.T) {
	ctx := context.Background()
	pool, store, quotaRepo := newRootFSQuotaIntegration(t, 1024)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-layer-blocked", "team-1")))
	layerState := rootFSTestStoreState(
		"sandbox-layer-blocked",
		"team-1",
		"layer-blocked",
		"",
		1,
		"layer-blocked",
	)
	require.NoError(t, store.SaveRootFSState(ctx, layerState))
	_, err := pool.Exec(ctx, `
		UPDATE manager.rootfs_objects
		SET deleted_at = NOW() - INTERVAL '25 hours'
		WHERE object_key = $1
	`, layerState.DiffObjectKey)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO manager.rootfs_objects (
			object_key, team_id, diff_digest, diff_size, first_layer_id, deleted_at
		) VALUES
			('rootfs/queue-blocked.tar', 'team-1', 'sha256:queue-blocked', 1, 'layer-queue-blocked', NOW() - INTERVAL '25 hours'),
			('rootfs/quota-blocked.tar', 'team-1', 'sha256:quota-blocked', 1, 'layer-quota-blocked', NOW() - INTERVAL '25 hours'),
			('rootfs/released.tar', 'team-1', 'sha256:released', 1, 'layer-released', NOW() - INTERVAL '25 hours'),
			('rootfs/locked.tar', 'team-1', 'sha256:locked', 1, 'layer-locked', NOW() - INTERVAL '25 hours'),
			('rootfs/free.tar', 'team-1', 'sha256:free', 1, 'layer-free', NOW() - INTERVAL '25 hours')
	`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.rootfs_object_deletions (
			object_key, team_id, next_attempt_at
		) VALUES (
			'rootfs/queue-blocked.tar', 'team-1', NOW()
		)
	`)
	require.NoError(t, err)
	activeOwner, err := rootFSObjectQuotaOwner("team-1", "rootfs/quota-blocked.tar")
	require.NoError(t, err)
	require.NoError(t, quotaRepo.ReconcileTarget(
		ctx,
		activeOwner,
		teamquota.Values{
			teamquota.KeyRootFSStorageBytes: 1,
			teamquota.KeyStorageObjectCount: 1,
		},
		teamquota.RuntimeRef{},
	))
	releasedOwner, err := rootFSObjectQuotaOwner("team-1", "rootfs/released.tar")
	require.NoError(t, err)
	require.NoError(t, quotaRepo.ReconcileTarget(
		ctx,
		releasedOwner,
		teamquota.Values{
			teamquota.KeyRootFSStorageBytes: 0,
			teamquota.KeyStorageObjectCount: 0,
		},
		teamquota.RuntimeRef{},
	))

	lockTx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer func() { _ = lockTx.Rollback(ctx) }()
	_, err = lockTx.Exec(ctx, `
		SELECT 1
		FROM manager.rootfs_objects
		WHERE object_key = 'rootfs/locked.tar'
		FOR UPDATE
	`)
	require.NoError(t, err)

	pruneCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	pruned, err := store.PruneDeletedRootFSObjectTombstones(pruneCtx, "team-1", 10)
	require.NoError(t, err)
	assert.Equal(t, 2, pruned, "free and released rows should prune while the locked row is skipped")
	for _, objectKey := range []string{
		layerState.DiffObjectKey,
		"rootfs/queue-blocked.tar",
		"rootfs/quota-blocked.tar",
		"rootfs/locked.tar",
	} {
		var exists bool
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM manager.rootfs_objects
				WHERE object_key = $1
			)
		`, objectKey).Scan(&exists))
		assert.True(t, exists, "recovery evidence %s must be retained", objectKey)
	}

	require.NoError(t, lockTx.Rollback(ctx))
	pruned, err = store.PruneDeletedRootFSObjectTombstones(ctx, "team-1", 10)
	require.NoError(t, err)
	assert.Equal(t, 1, pruned)
}

func TestReconcileRootFSObjectQuotaLocksAndRereadsConcurrentDeletion(t *testing.T) {
	ctx := context.Background()
	pool, store, quotaRepo := newRootFSQuotaIntegration(t, 1024)
	state := rootFSTestStoreState("sandbox-reconcile", "team-1", "layer-reconcile", "", 1, "reconcile")
	state.DiffSize = 90
	require.NoError(t, prepareRootFSObjectPublishForTest(ctx, store, state, time.Now().Add(time.Hour), quotaRepo))

	deleteTx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer func() { _ = deleteTx.Rollback(ctx) }()
	var object RootFSObjectInventory
	require.NoError(t, deleteTx.QueryRow(ctx, `
		SELECT object_key, team_id, diff_size, deleted_at IS NOT NULL
		FROM manager.rootfs_objects
		WHERE object_key = $1
		FOR UPDATE
	`, state.DiffObjectKey).Scan(
		&object.ObjectKey,
		&object.TeamID,
		&object.SizeBytes,
		&object.Deleted,
	))

	reconciled := make(chan error, 1)
	go func() {
		reconciled <- ReconcileRootFSObjectQuota(ctx, store, quotaRepo)
	}()
	select {
	case reconcileErr := <-reconciled:
		t.Fatalf("reconcile returned before the inventory mutation committed: %v", reconcileErr)
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, releaseRootFSObjectQuotaTx(ctx, deleteTx, quotaRepo, object))
	_, err = deleteTx.Exec(ctx, `
		UPDATE manager.rootfs_objects
		SET deleted_at = NOW()
		WHERE object_key = $1
	`, state.DiffObjectKey)
	require.NoError(t, err)
	require.NoError(t, deleteTx.Commit(ctx))
	require.NoError(t, <-reconciled)

	assertRootFSQuotaStatus(t, quotaRepo, "team-1", 0, 0)
	require.NoError(t, quotaRepo.ValidateUsageInvariant(ctx, "team-1"))
}

func TestReconcileRootFSSnapshotQuotaLocksAndRereadsConcurrentDeletion(t *testing.T) {
	ctx := context.Background()
	pool, store, quotaRepo := newRootFSQuotaIntegration(t, 1024)
	record := rootFSTestSandboxRecord("sandbox-snapshot-reconcile", "team-1")
	record.Status = SandboxStatusPaused
	require.NoError(t, store.UpsertSandbox(ctx, record))
	require.NoError(t, store.SaveRootFSState(
		ctx,
		rootFSTestStoreState(record.ID, record.TeamID, "layer-snapshot-reconcile", "", 1, "snapshot-reconcile"),
	))
	snapshot, err := store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID:  record.ID,
		SnapshotID: "snapshot-reconcile",
	})
	require.NoError(t, err)
	require.NoError(t, ReconcileRootFSSnapshotQuota(ctx, store, quotaRepo))

	deleteTx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer func() { _ = deleteTx.Rollback(ctx) }()
	var snapshotTeamID string
	require.NoError(t, deleteTx.QueryRow(ctx, `
		SELECT team_id
		FROM manager.rootfs_snapshots
		WHERE snapshot_id = $1
		FOR UPDATE
	`, snapshot.ID).Scan(&snapshotTeamID))

	reconciled := make(chan error, 1)
	go func() {
		reconciled <- ReconcileRootFSSnapshotQuota(ctx, store, quotaRepo)
	}()
	select {
	case reconcileErr := <-reconciled:
		t.Fatalf("reconcile returned before the snapshot mutation committed: %v", reconcileErr)
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, deleteRootFSSnapshotWithQuotaTx(
		ctx,
		deleteTx,
		quotaRepo,
		snapshot.ID,
		snapshotTeamID,
	))
	require.NoError(t, deleteTx.Commit(ctx))
	require.NoError(t, <-reconciled)

	assertTeamQuotaKeyStatus(t, quotaRepo, record.TeamID, teamquota.KeyStorageObjectCount, 0, 0)
	require.NoError(t, quotaRepo.ValidateUsageInvariant(ctx, record.TeamID))
}

func TestRootFSSnapshotObjectQuotaIsAtomicWithCatalogLifecycle(t *testing.T) {
	ctx := context.Background()
	pool, store, quotaRepo := newRootFSQuotaIntegration(t, 1024)
	require.NoError(t, quotaRepo.UnsafePutTeamPolicyForTest(ctx, "team-1", teamquota.Policy{
		Key:   teamquota.KeyStorageObjectCount,
		Kind:  teamquota.KindCapacity,
		Limit: 1,
	}))
	record := rootFSTestSandboxRecord("sandbox-snapshot-quota", "team-1")
	record.Status = SandboxStatusPaused
	require.NoError(t, store.UpsertSandbox(ctx, record))
	require.NoError(t, store.SaveRootFSState(
		ctx,
		rootFSTestStoreState(record.ID, record.TeamID, "layer-snapshot-quota", "", 1, "snapshot-quota"),
	))
	svc := &SandboxService{
		sandboxStore:   store,
		teamQuotaStore: quotaRepo,
		clock:          systemTime{},
		logger:         zap.NewNop(),
	}

	first, err := svc.CreateSandboxRootFSSnapshot(ctx, record.ID, record.TeamID, nil)
	require.NoError(t, err)
	require.NotNil(t, first)
	assertTeamQuotaKeyStatus(t, quotaRepo, record.TeamID, teamquota.KeyStorageObjectCount, 1, 0)

	_, err = svc.CreateSandboxRootFSSnapshot(ctx, record.ID, record.TeamID, nil)
	require.ErrorIs(t, err, ErrQuotaExceeded)
	var snapshotCount int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM manager.rootfs_snapshots
		WHERE team_id = $1
	`, record.TeamID).Scan(&snapshotCount))
	assert.Equal(t, int64(1), snapshotCount)

	require.NoError(t, svc.DeleteSandboxRootFSSnapshot(ctx, first.ID, record.TeamID))
	assertTeamQuotaKeyStatus(t, quotaRepo, record.TeamID, teamquota.KeyStorageObjectCount, 0, 0)
	require.NoError(t, quotaRepo.ValidateUsageInvariant(ctx, record.TeamID))
}

func TestReconcileAndExpireRootFSSnapshotObjectQuota(t *testing.T) {
	ctx := context.Background()
	pool, store, quotaRepo := newRootFSQuotaIntegration(t, 1024)
	record := rootFSTestSandboxRecord("sandbox-expired-snapshot", "team-1")
	record.Status = SandboxStatusPaused
	require.NoError(t, store.UpsertSandbox(ctx, record))
	require.NoError(t, store.SaveRootFSState(
		ctx,
		rootFSTestStoreState(record.ID, record.TeamID, "layer-expired-snapshot", "", 1, "expired-snapshot"),
	))
	snapshot, err := store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID:  record.ID,
		SnapshotID: "snapshot-expired",
		ExpiresAt:  time.Now().Add(-time.Minute),
	})
	require.NoError(t, err)
	require.NotNil(t, snapshot)

	require.NoError(t, ReconcileRootFSSnapshotQuota(ctx, store, quotaRepo))
	assertTeamQuotaKeyStatus(t, quotaRepo, record.TeamID, teamquota.KeyStorageObjectCount, 1, 0)

	deleted, err := store.DeleteExpiredRootFSSnapshots(ctx, record.TeamID, 10, quotaRepo)
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)
	assertTeamQuotaKeyStatus(t, quotaRepo, record.TeamID, teamquota.KeyStorageObjectCount, 0, 0)
	require.NoError(t, quotaRepo.ValidateUsageInvariant(ctx, record.TeamID))

	var snapshotCount int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM manager.rootfs_snapshots
		WHERE snapshot_id = $1
	`, snapshot.ID).Scan(&snapshotCount))
	assert.Zero(t, snapshotCount)
}

func newRootFSQuotaIntegration(
	t *testing.T,
	rootFSLimit int64,
) (*pgxpool.Pool, *PGSandboxStore, *teamquota.Repository) {
	t.Helper()
	pool := newSandboxStoreIntegrationPool(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS quota CASCADE")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS quota CASCADE")
	})
	require.NoError(t, teamquota.RunMigrations(ctx, pool, nil))
	repo := teamquota.NewRepository(pool)
	require.NoError(t, repo.UnsafeReplaceDefaultPoliciesForTest(ctx, rootFSTestDefaultQuotaPolicies(rootFSLimit)))
	return pool, NewPGSandboxStore(pool), repo
}

func rootFSTestDefaultQuotaPolicies(rootFSLimit int64) []teamquota.Policy {
	policies := make([]teamquota.Policy, 0, len(teamquota.Keys()))
	for _, key := range teamquota.Keys() {
		kind, _ := teamquota.KindForKey(key)
		policy := teamquota.Policy{Key: key, Kind: kind}
		switch kind {
		case teamquota.KindCapacity:
			policy.Limit = 1 << 60
			if key == teamquota.KeyRootFSStorageBytes {
				policy.Limit = rootFSLimit
			}
		case teamquota.KindConcurrency:
			policy.Limit = 1 << 52
		case teamquota.KindRate:
			policy.Tokens = 1000
			policy.IntervalMillis = 1000
			policy.Burst = 2000
		}
		policies = append(policies, policy)
	}
	return policies
}

func prepareRootFSObjectPublishForTest(
	ctx context.Context,
	store *PGSandboxStore,
	state *SandboxRootFSState,
	notBefore time.Time,
	quotaRepo *teamquota.Repository,
) error {
	stageID := uuid.NewString()
	expiresAt := time.Now().Add(time.Hour)
	if err := store.PrepareRootFSPublishStage(ctx, RootFSPublishStage{
		StageID:           stageID,
		TeamID:            state.TeamID,
		SandboxID:         state.SandboxID,
		CtldAddress:       "http://ctld.test",
		RuntimeGeneration: state.RuntimeGeneration,
		ExpiresAt:         expiresAt,
		ReleaseAfter:      expiresAt.Add(time.Minute),
	}, quotaRepo); err != nil {
		return err
	}
	return store.PrepareRootFSObjectPublish(ctx, stageID, state, notBefore, quotaRepo)
}

func assertRootFSQuotaStatus(
	t *testing.T,
	repo *teamquota.Repository,
	teamID string,
	committed int64,
	reserved int64,
) {
	t.Helper()
	statuses, err := repo.ListStatus(context.Background(), teamID)
	require.NoError(t, err)
	for _, status := range statuses {
		if status.Key == teamquota.KeyRootFSStorageBytes {
			assert.Equal(t, committed, status.Committed)
			assert.Equal(t, reserved, status.Reserved)
			return
		}
	}
	t.Fatalf("rootfs quota status was not returned for team %q", teamID)
}

func assertTeamQuotaKeyStatus(
	t *testing.T,
	repo *teamquota.Repository,
	teamID string,
	key teamquota.Key,
	committed int64,
	reserved int64,
) {
	t.Helper()
	statuses, err := repo.ListStatus(context.Background(), teamID)
	require.NoError(t, err)
	for _, status := range statuses {
		if status.Key == key {
			assert.Equal(t, committed, status.Committed)
			assert.Equal(t, reserved, status.Reserved)
			return
		}
	}
	t.Fatalf("quota status %q was not returned for team %q", key, teamID)
}

func assertRootFSQuotaObjectCount(
	t *testing.T,
	pool *pgxpool.Pool,
	table string,
	want int64,
) {
	t.Helper()
	var got int64
	require.NoError(t, pool.QueryRow(
		context.Background(),
		"SELECT COUNT(*) FROM manager."+table,
	).Scan(&got))
	assert.Equal(t, want, got)
}
