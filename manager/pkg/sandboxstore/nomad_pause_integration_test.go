package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

func TestRequestNomadSandboxPausePersistsDeterministicIntentIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "deterministic")

	first, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	expectedOperation := rootfshandoff.PlannedRetireOperationID(
		fixture.issue.GateParent, fixture.issue.GrantID, fixture.writerEpoch,
	)
	require.Equal(t, expectedOperation, first.OperationID)
	require.Equal(t, SandboxLifecyclePhasePreparing, first.LifecyclePhase)
	require.Equal(t, fixture.slotID, first.SlotID)
	require.Equal(t, fixture.allocationID, first.AllocationID)
	require.Equal(t, fixture.issue.GrantID, first.WriterGrantID)

	retry, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceAuto,
	)
	require.NoError(t, err)
	require.Equal(t, first, retry)

	active, err := fixture.store.GetActiveLifecycleTxn(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, expectedOperation, active.ID)
	require.Equal(t, SandboxLifecycleSourceManual, active.Source)
	require.False(t, active.Cancelable)
	require.Equal(t, int64(1), active.FromGeneration)
	require.Equal(t, fixture.allocationNamespace, active.FromRuntimeNamespace)
	require.Equal(t, fixture.allocationID, active.FromRuntimeID)
	require.Equal(t, fixture.initialGenerationID, active.ExpectedGenerationID)

	var lifecycleCount int
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT COUNT(*) FROM manager.sandbox_lifecycle_txns WHERE sandbox_id = $1
	`, fixture.sandboxID).Scan(&lifecycleCount))
	require.Equal(t, 1, lifecycleCount)
}

func TestRequestNomadSandboxPauseRejectsMismatchedRuntimeIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "mismatch")
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.sandboxes SET runtime_id = 'another-allocation'
		WHERE sandbox_id = $1
	`, fixture.sandboxID)
	require.NoError(t, err)

	_, err = fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.ErrorIs(t, err, ErrNomadSandboxPauseNotReady)
	active, getErr := fixture.store.GetActiveLifecycleTxn(fixture.ctx, fixture.sandboxID)
	require.NoError(t, getErr)
	require.Nil(t, active)
}

func TestRequestNomadSandboxPauseRecoversExactOrphanedSlotIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "orphaned-retry")
	first, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	orphanProof := bytes.Repeat([]byte{0xd1}, sha256.Size)
	orphaned, err := fixture.store.MarkRuntimeSlotAllocationMissing(
		fixture.ctx,
		&MarkRuntimeSlotAllocationMissingRequest{
			SlotID: first.SlotID, AllocationID: first.AllocationID,
			NodeUID: first.NodeUID, NodeBootID: first.NodeBootID,
			ObservationDigest: orphanProof,
		},
	)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateOrphaned, orphaned.State)

	retry, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceAuto,
	)
	require.NoError(t, err)
	require.Equal(t, first.OperationID, retry.OperationID)
	require.Equal(t, RuntimeSlotStateOrphaned, retry.SlotState)
	require.Equal(t, SandboxLifecycleSourceManual, retry.Source)
	require.Equal(t, first.WriterGrantID, retry.WriterGrantID)
}

func TestRequestNomadSandboxPauseRecoversExactQuiescingSlotIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "quiescing-retry")
	first, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	quiescing, err := fixture.store.BeginRuntimeSlotQuiesce(
		fixture.ctx,
		&BeginRuntimeSlotQuiesceRequest{
			SlotID: first.SlotID, OperationID: first.ClaimOperationID, ClaimID: first.ClaimID,
		},
	)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, quiescing.State)

	retry, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceAuto,
	)
	require.NoError(t, err)
	require.Equal(t, first.OperationID, retry.OperationID)
	require.Equal(t, RuntimeSlotStateQuiescing, retry.SlotState)
	require.Equal(t, SandboxLifecycleSourceManual, retry.Source)
	require.Equal(t, first.WriterGrantID, retry.WriterGrantID)
}

func TestRequestNomadSandboxTTLPauseRechecksDeadlinesUnderLockIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "ttl-recheck")
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.sandboxes
		SET expires_at = NOW() + INTERVAL '1 hour',
			hard_expires_at = NOW() + INTERVAL '2 hours'
		WHERE sandbox_id = $1
	`, fixture.sandboxID)
	require.NoError(t, err)

	_, err = fixture.store.RequestNomadSandboxTTLPause(fixture.ctx, fixture.sandboxID)
	require.ErrorIs(t, err, ErrNomadSandboxTTLNotExpired)
	active, getErr := fixture.store.GetActiveLifecycleTxn(fixture.ctx, fixture.sandboxID)
	require.NoError(t, getErr)
	require.Nil(t, active)

	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.sandboxes
		SET expires_at = NOW() - INTERVAL '1 second',
			hard_expires_at = NOW() - INTERVAL '1 millisecond'
		WHERE sandbox_id = $1
	`, fixture.sandboxID)
	require.NoError(t, err)
	_, err = fixture.store.RequestNomadSandboxTTLPause(fixture.ctx, fixture.sandboxID)
	require.ErrorIs(t, err, ErrNomadSandboxHardTTLExpired)
	active, getErr = fixture.store.GetActiveLifecycleTxn(fixture.ctx, fixture.sandboxID)
	require.NoError(t, getErr)
	require.Nil(t, active)

	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.sandboxes
		SET hard_expires_at = NOW() + INTERVAL '1 hour'
		WHERE sandbox_id = $1
	`, fixture.sandboxID)
	require.NoError(t, err)
	candidate, err := fixture.store.RequestNomadSandboxTTLPause(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxLifecycleSourceAuto, candidate.Source)
}

func TestRequestNomadSandboxPressurePauseFencesExactWriterIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "pressure-binding")
	request := &RootFSWriterPressurePauseRequest{
		SandboxID: fixture.sandboxID, GrantID: fixture.issue.GrantID,
		WriterEpoch: fixture.writerEpoch, BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: fixture.issue.BindingDigest, NodeUID: fixture.issue.NodeUID,
	}
	wrong := *request
	wrong.WriterEpoch++
	_, err := fixture.store.RequestNomadSandboxPressurePause(fixture.ctx, &wrong)
	require.ErrorIs(t, err, ErrNomadSandboxPauseConflict)
	active, getErr := fixture.store.GetActiveLifecycleTxn(fixture.ctx, fixture.sandboxID)
	require.NoError(t, getErr)
	require.Nil(t, active)

	candidate, err := fixture.store.RequestNomadSandboxPressurePause(fixture.ctx, request)
	require.NoError(t, err)
	require.Equal(t, fixture.issue.GrantID, candidate.WriterGrantID)
	require.Equal(t, SandboxLifecycleSourceAuto, candidate.Source)
	require.Equal(t, rootfshandoff.PlannedRetireOperationID(
		fixture.issue.GateParent, fixture.issue.GrantID, fixture.writerEpoch,
	), candidate.OperationID)
}

func TestRequestNomadSandboxPauseReturnsExactSlotAfterCommittedPublishIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "committed")
	candidate, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceAuto,
	)
	require.NoError(t, err)
	fixture.publishPlannedPause(t, candidate.OperationID)

	paused, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	require.True(t, paused.AlreadyPaused)
	require.Equal(t, candidate.OperationID, paused.OperationID)
	require.Equal(t, SandboxLifecycleSourceAuto, paused.Source)
	require.Equal(t, fixture.slotID, paused.SlotID)
	require.Equal(t, RootFSWriterGrantStateRetired, paused.WriterGrantState)

	_, err = fixture.store.BeginRuntimeSlotQuiesce(fixture.ctx, &BeginRuntimeSlotQuiesceRequest{
		SlotID: paused.SlotID, OperationID: paused.ClaimOperationID, ClaimID: paused.ClaimID,
	})
	require.NoError(t, err)
	retry, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	require.True(t, retry.AlreadyPaused)
	require.Equal(t, RuntimeSlotStateQuiescing, retry.SlotState)
}

func TestBeginRootFSWriterRetireAllowsExpiredLeaseForExactPlannedPauseIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "expired-exact-retire")
	candidate, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.rootfs_writer_grants
		SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE grant_id = $1
	`, fixture.issue.GrantID)
	require.NoError(t, err)

	fixture.publishPlannedPause(t, candidate.OperationID)
	retired, err := fixture.store.GetRootFSWriterGrant(fixture.ctx, fixture.issue.GrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, retired.State)
	require.Equal(t, candidate.OperationID, retired.RetireOperationID)
	require.Equal(t, RootFSWriterRetireKindPlannedPublish, retired.RetireKind)
	paused, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, paused.DesiredState)
	active, err := fixture.store.GetActiveLifecycleTxn(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Nil(t, active)
}

func TestBeginRootFSWriterRetireRejectsExpiredLeaseForAnotherRuntimeIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "expired-mismatched-retire")
	candidate, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.rootfs_writer_grants
		SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE grant_id = $1
	`, fixture.issue.GrantID)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = 'publishing', from_runtime_id = 'another-allocation'
		WHERE txn_id = $1
	`, candidate.OperationID)
	require.NoError(t, err)

	_, err = fixture.store.BeginRootFSWriterRetire(
		fixture.ctx,
		&BeginRootFSWriterRetireRequest{
			GrantID: fixture.issue.GrantID, WriterEpoch: fixture.writerEpoch,
			OperationID: candidate.OperationID, BindingVersion: RootFSWriterBindingVersion,
			BindingDigest:           fixture.issue.BindingDigest,
			ExpectedOldGenerationID: fixture.initial.ID,
		},
	)
	require.ErrorIs(t, err, ErrRootFSWriterLeaseExpired)
}

type nomadPauseStoreFixture struct {
	ctx                 context.Context
	pool                *pgxpool.Pool
	store               *PGSandboxStore
	sandboxID           string
	slotID              string
	allocationID        string
	allocationNamespace string
	initialGenerationID string
	writerEpoch         int64
	issue               *IssueRootFSWriterGrantRequest
	filesystem          *RootFSFilesystem
	initial             *RootFSGeneration
}

func (f *nomadPauseStoreFixture) publishPlannedPause(t *testing.T, operationID string) {
	t.Helper()
	blockHead := digest.FromString("nomad-pause-head-" + f.sandboxID).String()
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 1 << 30,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: blockHead,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/nomad-pause/map.page", Length: 4096,
				Checksum: digest.FromString("nomad-pause-map-page-" + f.sandboxID).String(),
			},
		},
	})
	require.NoError(t, err)
	next := &RootFSGeneration{
		ID: "generation-nomad-pause-" + f.sandboxID, FilesystemID: f.filesystem.ID,
		ParentGenerationID: f.initial.ID, SourceOCIDigest: f.initial.SourceOCIDigest,
		BaseArtifactDigest: f.initial.BaseArtifactDigest, BaseBlockRoot: f.initial.BaseBlockRoot,
		CurrentBlockHead: blockHead, WriterEpoch: f.writerEpoch,
		FormatGeneration: f.initial.FormatGeneration, DurabilityState: RootFSGenerationStateS3Materialized,
		LocatorVersion: f.initial.LocatorVersion + 1, Descriptor: descriptor,
	}
	proof := sha256.Sum256([]byte("nomad-pause-proof-" + f.sandboxID))
	require.NoError(t, f.store.WithSandboxLock(f.ctx, f.sandboxID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		if err := tx.UpdateLifecycleTxnPhase(lockCtx, operationID, SandboxLifecyclePhasePublishing); err != nil {
			return err
		}
		writerTx := tx.(RootFSWriterGrantTx)
		if _, err := writerTx.BeginRootFSWriterRetire(lockCtx, &BeginRootFSWriterRetireRequest{
			GrantID: f.issue.GrantID, WriterEpoch: f.writerEpoch, OperationID: operationID,
			BindingVersion: RootFSWriterBindingVersion, BindingDigest: f.issue.BindingDigest,
			ExpectedOldGenerationID: f.initial.ID,
		}); err != nil {
			return err
		}
		if _, err := writerTx.CompleteRootFSWriterRetireAndPublishGeneration(lockCtx,
			&CompleteRootFSWriterRetireAndPublishGenerationRequest{
				LifecycleTxnID: operationID, GrantID: f.issue.GrantID, WriterEpoch: f.writerEpoch,
				OperationID: operationID, BindingVersion: RootFSWriterBindingVersion,
				BindingDigest: f.issue.BindingDigest, ProofDigest: proof[:],
				ExpectedOldGenerationID: f.initial.ID, Generation: next,
			}); err != nil {
			return err
		}
		return tx.MarkRuntimePaused(lockCtx, f.sandboxID, 1, time.Now().UTC())
	}))
}

func newNomadPauseStoreFixture(t *testing.T, suffix string) *nomadPauseStoreFixture {
	t.Helper()
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	sandboxID := "sandbox-nomad-pause-" + suffix
	operationID := "claim-nomad-pause-" + suffix
	claimID := "claim-id-nomad-pause-" + suffix
	slotID := "slot-nomad-pause-" + suffix
	allocationID := "allocation-nomad-pause-" + suffix
	allocationNamespace := "nomad"
	record := rootFSTestSandboxRecord(sandboxID, "team-slot")
	record.ClusterID = "cluster-a"
	record.RuntimeGeneration = 1
	_, err := store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{
		Record: record, OperationID: operationID, LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	filesystem, initial, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: sandboxID, TeamID: record.TeamID, SourceOCIRef: artifact.SourceOCIRef,
		SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)
	registration := runtimeSlotTestRegistration(slotID, allocationID)
	registration.AllocationNamespace = allocationNamespace
	_, err = registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
	require.NoError(t, err)
	proof := bytes.Repeat([]byte{0x91}, 32)
	_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
		SlotID: slotID, AllocationID: allocationID, NodeUID: registration.NodeUID,
		NodeBootID: registration.NodeBootID, RuntimeReadyDigest: proof,
		NetworkReadyDigest: proof, StorageReadyDigest: proof, HeartbeatTTL: time.Minute,
	})
	require.NoError(t, err)
	acquire := &AcquireRuntimeSlotRequest{
		OperationID: operationID, ClaimID: claimID, SandboxID: sandboxID,
		FilesystemID: filesystem.ID, SourceGenerationID: initial.ID,
		CompatibilityDigest: registration.CompatibilityDigest, ClusterID: registration.ClusterID,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
		Resources: runtimeSlotTestResources(),
	}
	claimed, err := store.AcquireRuntimeSlot(ctx, acquire)
	require.NoError(t, err)
	binding := bytes.Repeat([]byte{0x92}, 32)
	issue := rootFSWriterGrantTestIssueRequest(sandboxID, "grant-nomad-pause-"+suffix, claimID, slotID, binding)
	issue.ExpectedFilesystemID = filesystem.ID
	issue.InitialGenerationID = initial.ID
	issue.RuntimeNamespace = allocationNamespace
	issue.RuntimeID = "slot"
	issue.RuntimeIncarnationID = allocationID
	issue.NodeName = registration.NodeID
	issue.NodeUID = registration.NodeUID
	issue.NodeBootID = registration.NodeBootID
	issue.RuntimeGeneration = "1"
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	_, err = store.BindRuntimeSlotWriterGrant(ctx, &BindRuntimeSlotWriterGrantRequest{
		SlotID: slotID, OperationID: operationID, ClaimID: claimID, GrantID: issue.GrantID,
	})
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding,
		ConsumerNodeUID: registration.NodeUID, ConsumerAgentUID: "ctld-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = store.StartRuntimeSlot(ctx, &StartRuntimeSlotRequest{
		SlotID: slotID, AllocationID: allocationID, NodeUID: registration.NodeUID,
		NodeBootID: registration.NodeBootID, OperationID: operationID, ClaimID: claimID,
		LaunchAttempt: "launch-" + suffix, RunscContainerID: "runsc-" + suffix,
		RootFSBindingDigest: binding, ClaimNetworkDigest: bytes.Repeat([]byte{0x93}, 32),
		ResourceLeaseID: claimed.ResourceLease.LeaseID, ResourceLeaseDigest: claimed.ResourceLeaseDigest,
	})
	require.NoError(t, err)
	_, err = store.MarkRuntimeSlotCommandReady(ctx, &MarkRuntimeSlotCommandReadyRequest{
		SlotID: slotID, AllocationID: allocationID, NodeUID: registration.NodeUID,
		NodeBootID: registration.NodeBootID, OperationID: operationID, ClaimID: claimID,
		ProcdInstanceID: "procd-" + suffix, ProcdAddress: "http://192.0.2.2:49983",
		CommandReadyDigest: bytes.Repeat([]byte{0x94}, 32),
	})
	require.NoError(t, err)
	_, err = store.CompleteSandboxClaim(ctx, &CompleteSandboxClaimRequest{
		SandboxID: sandboxID, OperationID: operationID, SlotID: claimed.ID,
		AllocationID: allocationID, AllocationNamespace: allocationNamespace,
		ResourceLeaseID: claimed.ResourceLease.LeaseID, ResourceLeaseDigest: claimed.ResourceLeaseDigest,
	})
	require.NoError(t, err)
	return &nomadPauseStoreFixture{
		ctx: ctx, pool: pool, store: store, sandboxID: sandboxID, slotID: slotID,
		allocationID: allocationID, allocationNamespace: allocationNamespace,
		initialGenerationID: initial.ID, writerEpoch: issued.Grant.WriterEpoch, issue: issue,
		filesystem: filesystem, initial: initial,
	}
}
