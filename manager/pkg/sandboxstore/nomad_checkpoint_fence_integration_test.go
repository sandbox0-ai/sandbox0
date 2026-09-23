package sandboxstore

import (
	"sync"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func checkpointFenceStoreFixture(t *testing.T, name string) (*nomadPauseStoreFixture, protocol.MigrationSourceFenceRequest, protocol.MigrationSourceFenceProof) {
	t.Helper()
	f, publication := checkpointPublicationFixture(t, name)
	_, err := f.store.AuthorizeNomadCheckpointPublication(f.ctx, publication.Capture)
	require.NoError(t, err)
	request := protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: migrationPublicationReceipt(t, publication)}
	return f, request, migrationSourceFenceStoreProof(t, f, request)
}

func TestNomadCheckpointFenceRequiresRetentionAndExactPhysicalProofIntegration(t *testing.T) {
	f, request, proof := checkpointFenceStoreFixture(t, "checkpoint-fence")
	_, err := f.store.AuthorizeNomadCheckpointSourceFence(f.ctx, request)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "unretained source must remain recoverable")
	_, err = f.store.CommitNomadCheckpointPublication(f.ctx, request.PublicationRequest, request.Publication)
	require.NoError(t, err)
	require.ErrorIs(t, f.store.CommitNomadCheckpointSourceFence(f.ctx, request, proof), ErrNomadCheckpointConflict)
	wrong := request
	wrong.Publication.Reference.ManifestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	_, err = f.store.AuthorizeNomadCheckpointSourceFence(f.ctx, wrong)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	authorized, err := f.store.AuthorizeNomadCheckpointSourceFence(f.ctx, request)
	require.NoError(t, err)
	require.Equal(t, request, *authorized)
	_, err = f.store.RenewRootFSWriterGrant(f.ctx, &RenewRootFSWriterGrantRequest{GrantID: f.issue.GrantID, WriterEpoch: f.writerEpoch,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: f.issue.BindingDigest, ConsumerNodeUID: f.issue.NodeUID},
		RootFSWriterLeaseRenewalPolicy{LeaseTTL: time.Minute, GracePeriod: RootFSWriterMaxRenewGrace})
	require.Error(t, err)
	filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, f.initialGenerationID, filesystem.HeadGenerationID)
	invalid := proof
	invalid.ContainerAbsent = false
	invalid.Digest, err = invalid.ProofDigest()
	require.NoError(t, err)
	require.Error(t, f.store.CommitNomadCheckpointSourceFence(f.ctx, request, invalid))
	require.NoError(t, f.store.CommitNomadCheckpointSourceFence(f.ctx, request, proof))
	filesystem, err = f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, request.PublicationRequest.Capture.RootFS.Generation.GenerationID, filesystem.HeadGenerationID)
	require.Equal(t, f.writerEpoch, filesystem.WriterEpoch)
	slot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, slot.State)
	require.Equal(t, RuntimeResourceLeaseActive, slot.ResourceLeaseState, "fencing is not complete physical cleanup")
	lifecycle, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxLifecyclePhaseCommitting, lifecycle.Phase)
	_, err = f.store.FinalizeRuntimeSlot(f.ctx, &FinalizeRuntimeSlotRequest{SlotID: slot.ID,
		OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID, ProofDigest: make([]byte, 32), Reason: "migration_source",
		ResourceLeaseID: slot.ResourceLease.LeaseID, ResourceLeaseDigest: slot.ResourceLeaseDigest, ResourceCgroupAbsent: true})
	require.ErrorIs(t, err, ErrRuntimeSlotConflict, "generic cleanup cannot bypass memory lifecycle completion")
	var slots int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_slots`).Scan(&slots))
	require.Equal(t, 1, slots, "memory pause must never allocate a destination")
}

func TestNomadCheckpointFenceConcurrentRecoveryAfterHardTTLIntegration(t *testing.T) {
	f, request, proof := checkpointFenceStoreFixture(t, "checkpoint-fence-retry")
	// Capture is already authorized. Expiration must not strand its captured
	// image or prevent the source writer from becoming physically absent.
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	_, err = f.store.CommitNomadCheckpointPublication(f.ctx, request.PublicationRequest, request.Publication)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadCheckpointSourceFence(f.ctx, request)
	require.NoError(t, err)
	const workers = 8
	results := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			results[i] = NewPGSandboxStore(f.pool).CommitNomadCheckpointSourceFence(f.ctx, request, proof)
		})
	}
	wg.Wait()
	for _, err := range results {
		require.NoError(t, err)
	}
	retry, err := NewPGSandboxStore(f.pool).AuthorizeNomadCheckpointSourceFence(f.ctx, request)
	require.NoError(t, err)
	require.Equal(t, request, *retry)
	changed := proof
	changed.MountNamespaceID = "mnt:[2]"
	changed.Digest, err = changed.ProofDigest()
	require.NoError(t, err)
	require.ErrorIs(t, f.store.CommitNomadCheckpointSourceFence(f.ctx, request, changed), ErrNomadCheckpointConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoints SET evidence=evidence-'fenced' WHERE operation_id=$1`,
		request.PublicationRequest.Capture.Request.OperationID)
	require.Error(t, err)
}
