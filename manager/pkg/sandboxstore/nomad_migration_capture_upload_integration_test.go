package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func retainedCaptureUploadFixture(t *testing.T, f *nomadPauseStoreFixture, id string) protocol.MigrationStagingRequest {
	t.Helper()
	var payload []byte
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT staging_request FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, id).Scan(&payload))
	var r protocol.MigrationStagingRequest
	require.NoError(t, json.Unmarshal(payload, &r))
	require.NoError(t, r.Validate())
	return r
}

func releaseCaptureUploadStagingFixture(t *testing.T, f *nomadPauseStoreFixture, id string) {
	t.Helper()
	r, err := f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, id)
	require.NoError(t, err)
	require.NotNil(t, r)
	d, err := r.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationStagingRelease(f.ctx, *r, protocol.MigrationStagingReleased{RequestDigest: d}))
}

type captureUploadGCCommitLoss struct {
	*PGSandboxStore
	fail bool
}

func (s *captureUploadGCCommitLoss) CompleteNomadMigrationCaptureUploadGC(ctx context.Context, scope runtimecheckpoint.CaptureScope) error {
	if s.fail {
		s.fail = false
		return errors.New("injected capture GC completion failure")
	}
	return s.PGSandboxStore.CompleteNomadMigrationCaptureUploadGC(ctx, scope)
}

func TestCaptureUploadGCRequiresFailedCaptureCleanupAndSurvivesSandboxDeletionIntegration(t *testing.T) {
	f, reservation, publication := migrationPublicationStoreFixture(t, "capture-upload-gc")
	id := publication.Assignment.OperationID
	r := retainedCaptureUploadFixture(t, f, id)
	require.Equal(t, 1, r.CaptureUpload.Version)
	scope, err := r.CaptureUpload.Scope(r.Source)
	require.NoError(t, err)
	objects := objectstore.NewMemoryStore("")
	store, err := runtimecheckpoint.New(objects, 1<<20)
	require.NoError(t, err)
	stage, err := store.OpenCaptureStaging(f.ctx, scope, r.CaptureUpload.MaxBytes)
	require.NoError(t, err)
	_, err = stage.StageChunk(f.ctx, []byte("unpublished capture"))
	require.NoError(t, err)
	blocked := func() {
		t.Helper()
		s, err := f.store.AuthorizeNomadMigrationCaptureUploadGC(f.ctx, id)
		require.NoError(t, err)
		require.Nil(t, s)
		_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET capture_upload_gc_scope_digest=staging_request->'capture_upload'->>'scope_digest' WHERE operation_id=$1`, id)
		require.Error(t, err, "SQL must also reject early reclamation")
	}
	blocked()
	_, err = f.store.RequestSandboxRuntimeClaimCleanup(f.ctx, f.sandboxID, "delete failed capture fixture")
	require.NoError(t, err)
	capture := publication.Capture
	capture.State = protocol.MigrationCaptureUncertain
	capture.RootFS = nil
	command, err := f.store.AuthorizeNomadSandboxMigrationCaptureFailure(f.ctx, capture)
	require.NoError(t, err)
	cd, err := command.Digest()
	require.NoError(t, err)
	proof := protocol.MigrationCaptureFailureProof{RequestDigest: cd, Cleanup: migrationNodeCleanupStoreProof(t, command.Cleanup)}
	require.NoError(t, f.store.CommitNomadSandboxMigrationCaptureFailureCleanup(f.ctx, *command, proof))
	final, err := f.store.AuthorizeNomadSandboxMigrationCaptureFailureFinalization(f.ctx, id)
	require.NoError(t, err)
	fd, err := final.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationCaptureFailureFinalization(f.ctx, *final, protocol.MigrationCaptureFailureFinalizeProof{RequestDigest: fd, RootFSArtifactsAbsent: true, ImageAbsent: true}))
	blocked()
	for _, slot := range []*RuntimeSlot{reservation.SourceSlot, reservation.TargetSlot} {
		_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: slot.ID, AllocationID: slot.AllocationID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0x76}, 32)})
		require.NoError(t, err)
	}
	gc := protocol.MigrationSourceGCRequest{Target: capture.Request.Target, FinalizationDigest: fd, CleanupProofDigest: proof.Cleanup.ProofDigest, AllocationAbsenceDigest: hex.EncodeToString(bytes.Repeat([]byte{0x76}, 32))}
	gd, err := gc.Digest()
	require.NoError(t, err)
	_, err = f.store.CompleteNomadSandboxMigrationCaptureFailure(f.ctx, id, gc, protocol.MigrationSourceGCAcknowledgement{RequestDigest: gd})
	require.NoError(t, err)
	blocked()
	releaseCaptureUploadStagingFixture(t, f, id)
	blocked()
	releaseCaptureUploadStagingFixture(t, f, id)
	require.NoError(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()))
	var wg sync.WaitGroup
	scopes := make([]*runtimecheckpoint.CaptureScope, 4)
	errs := make([]error, 4)
	for i := range scopes {
		wg.Go(func() {
			scopes[i], errs[i] = NewPGSandboxStore(f.pool).AuthorizeNomadMigrationCaptureUploadGC(f.ctx, id)
		})
	}
	wg.Wait()
	for i := range scopes {
		require.NoError(t, errs[i])
		require.NotNil(t, scopes[i])
		require.Equal(t, scope, *scopes[i])
	}
	wrongGrant := r.CaptureUpload
	wrongGrant, err = protocol.NewMigrationCaptureUpload(r.Source, "different-team", wrongGrant.CompatibilityDigest, wrongGrant.CPUFeaturesDigest, r.Bytes)
	require.NoError(t, err)
	wrong, err := wrongGrant.Scope(r.Source)
	require.NoError(t, err)
	require.ErrorIs(t, f.store.CompleteNomadMigrationCaptureUploadGC(f.ctx, wrong), ErrNomadSandboxMigrationConflict)
	collector, err := runtimecheckpoint.NewCollector(objects)
	require.NoError(t, err)
	worker, err := nomadmigration.NewCaptureUploadGC(&captureUploadGCCommitLoss{PGSandboxStore: f.store, fail: true}, collector)
	require.NoError(t, err)
	_, err = worker.RunOnce(f.ctx)
	require.NoError(t, err)
	_, err = worker.RunOnce(f.ctx)
	require.ErrorContains(t, err, "injected")
	worker, err = nomadmigration.NewCaptureUploadGC(NewPGSandboxStore(f.pool), collector)
	require.NoError(t, err)
	result, err := worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
	require.NoError(t, f.store.CompleteNomadMigrationCaptureUploadGC(f.ctx, scope))
	ids, err := f.store.ListNomadMigrationCaptureUploadGC(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET capture_upload_gc_completed_at=NULL WHERE operation_id=$1`, id)
	require.Error(t, err)
	entries, more, _, err := objects.List("runtime-checkpoints/", "", "", "", 100)
	require.NoError(t, err)
	require.False(t, more)
	require.Empty(t, entries)
}

func TestCaptureUploadRegionalBudgetSerializesAdmissionAndReclaimsUnusedGrantsIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	fixtures := make([]*nomadPauseStoreFixture, 4)
	assignments := make([]runtimecontrol.MigrationAssignment, 4)
	for i := range fixtures {
		name := fmt.Sprintf("capture-budget-%d", i)
		f := newNomadPauseStoreFixtureOnNode(t, name, pool, name, func(r *AcquireRuntimeSlotRequest) { r.Resources.MemoryBytes = 8 << 30 })
		fixtures[i] = f
		assignments[i] = migrationAssignmentFixture(t, f, name)
		migrationReadyTarget(t, f, name, name+"-target")
		_, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignments[i])
		require.NoError(t, err)
		retainMigrationCPUFixture(t, f, assignments[i])
	}
	var wg sync.WaitGroup
	requests := make([]*protocol.MigrationStagingRequest, 4)
	errs := make([]error, 4)
	for i := range fixtures {
		wg.Go(func() {
			requests[i], errs[i] = fixtures[i].store.AuthorizeNomadSandboxMigrationStaging(fixtures[i].ctx, assignments[i])
		})
	}
	wg.Wait()
	granted := 0
	var expected int64
	for i, r := range requests {
		require.NoError(t, errs[i])
		require.NotNil(t, r)
		if r.CaptureUpload.Version != 0 {
			granted++
			expected += r.CaptureUpload.ReservedBytes()
		}
		retry, err := NewPGSandboxStore(pool).AuthorizeNomadSandboxMigrationStaging(fixtures[i].ctx, assignments[i])
		require.NoError(t, err)
		require.Equal(t, r, retry)
	}
	require.Equal(t, 3, granted, "three large capture grants fit; the fourth uses legacy publication")
	var used int64
	require.NoError(t, pool.QueryRow(fixtures[0].ctx, `SELECT SUM(manager.runtime_migration_capture_upload_reserved_bytes(staging_request)) FROM manager.sandbox_runtime_migrations WHERE capture_upload_gc_completed_at IS NULL`).Scan(&used))
	require.Equal(t, expected, used)
	require.LessOrEqual(t, used, nomadMigrationCaptureUploadRegionBytes)
	_, err := pool.Exec(fixtures[0].ctx, `UPDATE manager.sandbox_lifecycle_txns SET created_at=clock_timestamp()-INTERVAL '1 hour' WHERE kind='migrate'`)
	require.NoError(t, err)
	_, err = fixtures[0].store.RecoverExpiredNomadMigrationReservations(fixtures[0].ctx, 8)
	require.NoError(t, err)
	for i, f := range fixtures {
		id := assignments[i].OperationID
		scope, err := f.store.AuthorizeNomadMigrationCaptureUploadGC(f.ctx, id)
		require.NoError(t, err)
		require.Nil(t, scope)
		releaseCaptureUploadStagingFixture(t, f, id)
		releaseCaptureUploadStagingFixture(t, f, id)
		if requests[i].CaptureUpload.Version != 0 {
			scope, err = f.store.AuthorizeNomadMigrationCaptureUploadGC(f.ctx, id)
			require.NoError(t, err)
			require.NotNil(t, scope)
			collector, err := runtimecheckpoint.NewCollector(objectstore.NewMemoryStore(""))
			require.NoError(t, err)
			done, err := collector.CollectCapture(f.ctx, *scope)
			require.NoError(t, err)
			require.True(t, done)
			require.NoError(t, f.store.CompleteNomadMigrationCaptureUploadGC(f.ctx, *scope))
		}
		source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
		require.NoError(t, err)
		require.Equal(t, RuntimeResourceLeaseActive, source.ResourceLeaseState)
	}
	require.NoError(t, pool.QueryRow(fixtures[0].ctx, `SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(staging_request)),0) FROM manager.sandbox_runtime_migrations WHERE capture_upload_gc_completed_at IS NULL`).Scan(&used))
	require.Zero(t, used)
}
