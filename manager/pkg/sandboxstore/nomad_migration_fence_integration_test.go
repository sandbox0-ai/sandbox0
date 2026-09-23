package sandboxstore

import (
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationFenceStoreFixture(t *testing.T, suffix string) (*nomadPauseStoreFixture, *NomadSandboxMigrationReservation, protocol.MigrationSourceFenceRequest, protocol.MigrationSourceFenceProof) {
	t.Helper()
	f, reservation, publication := migrationPublicationStoreFixture(t, suffix)
	_, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
	require.NoError(t, err)
	receipt := migrationPublicationReceipt(t, publication)
	require.NoError(t, f.store.CommitNomadSandboxMigrationPublication(f.ctx, publication, receipt))
	preparation, err := f.store.AuthorizeNomadSandboxMigrationImagePreparation(f.ctx, publication.Assignment)
	require.NoError(t, err)
	preparationDigest, err := preparation.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationImagePreparation(f.ctx, *preparation, protocol.MigrationImagePrepared{
		RequestDigest: preparationDigest, ManifestDigest: receipt.Reference.ManifestDigest, TotalBytes: 1024}))
	request := protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: receipt}
	return f, reservation, request, migrationSourceFenceStoreProof(t, f, request)
}

func migrationSourceFenceStoreProof(t *testing.T, f *nomadPauseStoreFixture, request protocol.MigrationSourceFenceRequest) protocol.MigrationSourceFenceProof {
	t.Helper()
	detach, err := request.RootFSRequest()
	require.NoError(t, err)
	rootfs, err := rootfshandoff.NewMigrationRootFSDetachProof(detach, rootfshandoff.CrashFenceSessionObservation{
		Parent: f.issue.GateParent, RootFSID: f.filesystem.ID, WriterEpoch: f.writerEpoch, OperationID: request.PublicationRequest.Capture.Request.OperationID,
		BindingDigest: request.PublicationRequest.Capture.Request.BindingDigest, SessionState: rootfshandoff.StateTombstoned, BranchPath: "/private/captured.wal",
		DeviceBound: true, DevicePath: "/dev/nbd0", LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	require.NoError(t, err)
	d, err := request.Digest()
	require.NoError(t, err)
	proof := protocol.MigrationSourceFenceProof{RequestDigest: d, RootFS: rootfs, ContainerID: protocol.NomadRunscContainerID(f.slotID), MountNamespaceID: "mnt:[1]", ContainerAbsent: true, StableMountAbsent: true}
	proof.Digest, err = proof.ProofDigest()
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(request))
	return proof
}

func TestNomadMigrationSourceFenceRequiresCommittedImageAndPhysicalProofIntegration(t *testing.T) {
	f, reservation, request, proof := migrationFenceStoreFixture(t, "fence-authority")
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationSourceFence(f.ctx, request, proof), ErrNomadSandboxMigrationConflict)
	changed := request
	changed.Publication.Reference.ManifestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	_, err := f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, changed)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	authorized, err := f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, request)
	require.NoError(t, err)
	require.Equal(t, request, *authorized)
	var state, kind string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT state,retire_kind FROM manager.rootfs_writer_grants WHERE grant_id=$1`, f.issue.GrantID).Scan(&state, &kind))
	require.Equal(t, RootFSWriterGrantStateRetiring, state)
	require.Equal(t, RootFSWriterRetireKindMigration, kind)
	_, err = f.store.RenewRootFSWriterGrant(f.ctx, &RenewRootFSWriterGrantRequest{GrantID: f.issue.GrantID, WriterEpoch: f.writerEpoch,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: f.issue.BindingDigest, ConsumerNodeUID: f.issue.NodeUID}, RootFSWriterLeaseRenewalPolicy{LeaseTTL: time.Minute, GracePeriod: RootFSWriterMaxRenewGrace})
	require.Error(t, err, "committed handoff authority must revoke old writer renewal")
	filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, f.initialGenerationID, filesystem.HeadGenerationID, "authorization alone is not a physical fence")
	invalid := proof
	invalid.ContainerAbsent = false
	invalid.Digest, err = invalid.ProofDigest()
	require.NoError(t, err)
	require.Error(t, f.store.CommitNomadSandboxMigrationSourceFence(f.ctx, request, invalid))
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFence(f.ctx, request, proof))
	filesystem, err = f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, request.PublicationRequest.Capture.RootFS.Generation.GenerationID, filesystem.HeadGenerationID)
	require.Equal(t, f.writerEpoch, filesystem.WriterEpoch, "fencing does not issue another writer")
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT state FROM manager.rootfs_writer_grants WHERE grant_id=$1`, f.issue.GrantID).Scan(&state))
	require.Equal(t, RootFSWriterGrantStateRetired, state)
	lifecycle, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.NotNil(t, lifecycle)
	require.Equal(t, SandboxLifecyclePhaseCommitting, lifecycle.Phase)
	sandbox, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, int64(1), sandbox.RuntimeGeneration)
	target, err := f.store.GetRuntimeSlot(f.ctx, reservation.TargetSlot.ID)
	require.NoError(t, err)
	require.Empty(t, target.WriterGrantID)
	var active int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&active))
	require.Equal(t, 2, active, "physical source fence is not final resource cleanup")
}

func TestNomadMigrationSourceFenceConcurrentRecoveryAndImmutableProofIntegration(t *testing.T) {
	f, _, request, proof := migrationFenceStoreFixture(t, "fence-recovery")
	_, err := f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, request)
	require.NoError(t, err)
	const workers = 8
	errors := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			errors[i] = NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationSourceFence(f.ctx, request, proof)
		})
	}
	wg.Wait()
	for _, err := range errors {
		require.NoError(t, err)
	}
	recovered, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationSourceFence(f.ctx, request)
	require.NoError(t, err)
	require.Equal(t, request, *recovered)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET source_fence_proof=NULL WHERE operation_id=$1`, request.PublicationRequest.Assignment.OperationID)
	require.Error(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET source_fence_request=NULL,source_fence_digest=NULL WHERE operation_id=$1`, request.PublicationRequest.Assignment.OperationID)
	require.Error(t, err)
	changed := proof
	changed.MountNamespaceID = "mnt:[2]"
	changed.Digest, err = changed.ProofDigest()
	require.NoError(t, err)
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationSourceFence(f.ctx, request, changed), ErrNomadSandboxMigrationConflict)
}
