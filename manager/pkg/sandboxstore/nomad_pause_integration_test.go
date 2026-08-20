package sandboxstore

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
	require.Equal(t, fixture.allocationNamespace, active.FromPodNamespace)
	require.Equal(t, fixture.allocationID, active.FromPodName)
	require.Equal(t, fixture.initialGenerationID, active.ExpectedHeadLayerID)

	var lifecycleCount int
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT COUNT(*) FROM manager.sandbox_lifecycle_txns WHERE sandbox_id = $1
	`, fixture.sandboxID).Scan(&lifecycleCount))
	require.Equal(t, 1, lifecycleCount)
}

func TestRequestNomadSandboxPauseRejectsMismatchedRuntimeIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "mismatch")
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.sandboxes SET current_pod_name = 'another-allocation'
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
	record.RuntimeBackend = SandboxRuntimeBackendNomad
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
	_, err = store.RegisterRuntimeSlot(ctx, registration)
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
	}
	claimed, err := store.AcquireRuntimeSlot(ctx, acquire)
	require.NoError(t, err)
	binding := bytes.Repeat([]byte{0x92}, 32)
	issue := rootFSWriterGrantTestIssueRequest(sandboxID, "grant-nomad-pause-"+suffix, claimID, slotID, binding)
	issue.ExpectedFilesystemID = filesystem.ID
	issue.InitialGenerationID = initial.ID
	issue.PodNamespace = allocationNamespace
	issue.PodName = "slot"
	issue.PodUID = allocationID
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
		ConsumerNodeUID: registration.NodeUID, ConsumerCtldPodUID: "ctld-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = store.StartRuntimeSlot(ctx, &StartRuntimeSlotRequest{
		SlotID: slotID, AllocationID: allocationID, NodeUID: registration.NodeUID,
		NodeBootID: registration.NodeBootID, OperationID: operationID, ClaimID: claimID,
		LaunchAttempt: "launch-" + suffix, RunscContainerID: "runsc-" + suffix,
		RootFSBindingDigest: binding, ClaimNetworkDigest: bytes.Repeat([]byte{0x93}, 32),
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
	})
	require.NoError(t, err)
	return &nomadPauseStoreFixture{
		ctx: ctx, pool: pool, store: store, sandboxID: sandboxID, slotID: slotID,
		allocationID: allocationID, allocationNamespace: allocationNamespace,
		initialGenerationID: initial.ID, writerEpoch: issued.Grant.WriterEpoch, issue: issue,
	}
}
