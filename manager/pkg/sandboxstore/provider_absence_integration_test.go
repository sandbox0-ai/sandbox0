package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/providerinstance"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func newProviderAbandonFixture(t *testing.T) (*rootFSWriterCrashAbandonFixture, objectstore.ContextConditionalStore, *RuntimeSlot) {
	t.Helper()
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	objects := objectstore.NewMemoryStore(t.Name()).(objectstore.ContextConditionalStore)
	image, err := os.CreateTemp(t.TempDir(), "provider-base")
	require.NoError(t, err)
	t.Cleanup(func() { _ = image.Close() })
	require.NoError(t, image.Truncate(rootfsblock.LogicalBlockSize))
	name := image.Name()
	require.NoError(t, image.Close())
	image, err = os.Open(name)
	require.NoError(t, err)
	built, err := rootfsblock.BuildMaterializedSparseFileGeneration(ctx, image, 1<<30, rootfsblock.ObjectStorePublisher{Store: objects}, rootfsblock.BuildOptions{ObjectPrefix: "rootfs/provider-test"}, nil)
	require.NoError(t, err)
	base := readyRootFSBaseArtifactTestRequest()
	base.Descriptor, base.BaseBlockRoot = built.Payload, built.Descriptor.MappingRoot.RootDigest
	record := rootFSTestSandboxRecord("sandbox-provider", "team-a")
	record.RuntimeGeneration = 1
	record.ClusterID = "cluster-a"
	_, err = store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{Record: record, OperationID: "claim-operation-provider", LeaseTTL: time.Minute})
	require.NoError(t, err)
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, base)
	require.NoError(t, err)
	filesystem, initial, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{SandboxID: record.ID, TeamID: record.TeamID, SourceOCIRef: artifact.SourceOCIRef, SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest})
	require.NoError(t, err)
	for _, ref := range built.References {
		_, err = pool.Exec(ctx, `INSERT INTO manager.rootfs_materialization_objects(object_key,object_kind,object_size,checksum,uploaded_at) VALUES($1,$2,$3,$4,NOW())`, ref.Key, ref.Kind, ref.Size, ref.Checksum)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `INSERT INTO manager.rootfs_base_artifact_objects VALUES($1,$2)`, base.ArtifactDigest, ref.Key)
		require.NoError(t, err)
	}
	registration := runtimeSlotTestRegistration("slot-provider", "allocation-provider")
	registration.NodeUID = "ecs/us-east-1/i-provider"
	_, err = registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
	require.NoError(t, err)
	_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{SlotID: registration.SlotID, AllocationID: registration.AllocationID, NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID, RuntimeReadyDigest: bytes.Repeat([]byte{1}, 32), NetworkReadyDigest: bytes.Repeat([]byte{2}, 32), StorageReadyDigest: bytes.Repeat([]byte{3}, 32), HeartbeatTTL: time.Minute})
	require.NoError(t, err)
	acquire := &AcquireRuntimeSlotRequest{OperationID: "claim-operation-provider", ClaimID: "claim-provider", SandboxID: record.ID, FilesystemID: filesystem.ID, SourceGenerationID: initial.ID, CompatibilityDigest: registration.CompatibilityDigest, ClusterID: registration.ClusterID, RuntimeAssignmentRevision: strings.Repeat("ab", 32), NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute, Resources: runtimeSlotTestResources()}
	slot, err := store.AcquireRuntimeSlot(ctx, acquire)
	require.NoError(t, err)
	binding := sha256.Sum256([]byte("provider-bound-writer"))
	issue := rootFSWriterGrantTestIssueRequest(record.ID, "grant-provider", slot.ClaimID, slot.ID, binding[:])
	issue.NodeUID, issue.NodeBootID = slot.NodeUID, slot.NodeBootID
	issue.ExpectedFilesystemID, issue.InitialGenerationID = filesystem.ID, initial.ID
	issue.RuntimeNamespace, issue.RuntimeIncarnationID, issue.RuntimeGeneration = slot.AllocationNamespace, slot.AllocationID, "1"
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	_, err = store.BindRuntimeSlotWriterGrant(ctx, &BindRuntimeSlotWriterGrantRequest{SlotID: slot.ID, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID, GrantID: issued.Grant.ID})
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{GrantID: issued.Grant.ID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ConsumerNodeUID: slot.NodeUID, ConsumerAgentUID: "ctld-provider", LeaseTTL: time.Minute})
	require.NoError(t, err)
	_, err = store.StartRuntimeSlot(ctx, &StartRuntimeSlotRequest{SlotID: slot.ID, AllocationID: slot.AllocationID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID, LaunchAttempt: "launch-provider", RunscContainerID: "runsc-provider", RootFSBindingDigest: binding[:], ClaimNetworkDigest: bytes.Repeat([]byte{4}, 32), ResourceLeaseID: slot.ResourceLease.LeaseID, ResourceLeaseDigest: slot.ResourceLeaseDigest})
	require.NoError(t, err)
	slot, err = store.MarkRuntimeSlotCommandReady(ctx, &MarkRuntimeSlotCommandReadyRequest{SlotID: slot.ID, AllocationID: slot.AllocationID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID, ProcdInstanceID: "procd-provider", ProcdAddress: "http://192.0.2.2:49983", CommandReadyDigest: bytes.Repeat([]byte{5}, 32)})
	require.NoError(t, err)
	_, err = store.CompleteSandboxClaim(ctx, &CompleteSandboxClaimRequest{SandboxID: record.ID, OperationID: slot.ClaimOperationID, SlotID: slot.ID, AllocationID: slot.AllocationID, AllocationNamespace: slot.AllocationNamespace, ResourceLeaseID: slot.ResourceLease.LeaseID, ResourceLeaseDigest: slot.ResourceLeaseDigest})
	require.NoError(t, err)
	lifecycleID := "provider-crash-operation"
	require.NoError(t, store.WithSandboxLock(ctx, record.ID, func(ctx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		return tx.BeginLifecycleTxn(ctx, &SandboxLifecycleTxn{ID: lifecycleID, SandboxID: record.ID, Kind: SandboxLifecycleKindPause, Phase: SandboxLifecyclePhasePublishing, Source: SandboxLifecycleSourceCrash, FromGeneration: 1, FromRuntimeNamespace: slot.AllocationNamespace, FromRuntimeID: slot.AllocationID, ExpectedGenerationID: initial.ID})
	}))
	f := &rootFSWriterCrashAbandonFixture{pool: pool, store: store, sandboxID: record.ID, runtimeGeneration: 1, filesystem: filesystem, initial: initial, issued: issued,
		beginRequest: &BeginRootFSWriterCrashAbandonRequest{GrantID: issued.Grant.ID, WriterEpoch: issued.Grant.WriterEpoch, OperationID: lifecycleID, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, ExpectedOldGenerationID: initial.ID},
		request:      &CompleteRootFSWriterCrashAbandonRequest{LifecycleTxnID: lifecycleID, GrantID: issued.Grant.ID, WriterEpoch: issued.Grant.WriterEpoch, OperationID: lifecycleID, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ProofVersion: RootFSWriterCrashAbandonProofVersion, ProofDigest: bytes.Repeat([]byte{6}, 32), NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, ExpectedOldGenerationID: initial.ID}}
	return f, objects, slot
}

func fencedProviderFixture(t *testing.T) (*rootFSWriterCrashAbandonFixture, objectstore.ContextConditionalStore, *RuntimeSlot, *PublishRootFSLiveCommitRequest) {
	t.Helper()
	f, objects, slot := newProviderAbandonFixture(t)
	ctx := t.Context()
	cut := buildLiveCommit(t, f, objects, f.initial, f.initial, 9, 99)
	_, err := f.store.PublishRootFSLiveCommit(ctx, cut)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `UPDATE manager.rootfs_writer_grants SET lease_expires_at=NOW()-INTERVAL '1 minute' WHERE grant_id=$1`, cut.GrantID)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()-INTERVAL '1 minute' WHERE slot_id=$1`, slot.ID)
	require.NoError(t, err)
	slot, err = f.store.FenceRuntimeSlotForReconcile(ctx, &FenceRuntimeSlotForReconcileRequest{SlotID: slot.ID, ExpectedRevision: slot.Revision})
	require.NoError(t, err)
	_, err = f.store.BeginRootFSWriterCrashAbandon(ctx, f.beginRequest)
	require.NoError(t, err)
	return f, objects, slot, cut
}

func providerObservation(slot *RuntimeSlot, n string, present bool) (RuntimeProviderTarget, providerinstance.Observation) {
	target := RuntimeProviderTarget{slot.ClusterID, slot.NodeID, slot.NodeUID, slot.NodeBootID}
	scope := providerinstance.Scope{Provider: "aliyun", AccountID: "1234567890123456", RegionID: "us-east-1", InstanceID: "i-provider"}
	return target, providerinstance.Observation{Scope: scope, Present: present, RequestID: "ecs-" + n, IdentityRequestID: "sts-" + n}
}

func confirmedProviderProof(t *testing.T, f *rootFSWriterCrashAbandonFixture, slot *RuntimeSlot) *RuntimeProviderAbsenceProof {
	t.Helper()
	target, o := providerObservation(slot, "first", false)
	p, err := f.store.RecordRuntimeProviderInstanceObservation(t.Context(), target, o)
	require.NoError(t, err)
	require.Nil(t, p)
	// Only this isolated test database advances the pending regional clock.
	_, err = f.pool.Exec(t.Context(), `UPDATE manager.runtime_provider_absence SET first_observed_at=first_observed_at-INTERVAL '31 seconds'`)
	require.NoError(t, err)
	_, o = providerObservation(slot, "second", false)
	p, err = f.store.RecordRuntimeProviderInstanceObservation(t.Context(), target, o)
	require.NoError(t, err)
	require.NotNil(t, p)
	return p
}

func providerCompletionRequest(t *testing.T, f *rootFSWriterCrashAbandonFixture, slot *RuntimeSlot, p *RuntimeProviderAbsenceProof) *CompleteRuntimeSlotProviderAbandonRequest {
	t.Helper()
	digest, err := p.Digest()
	require.NoError(t, err)
	writer := *f.request
	writer.ProofVersion = RootFSWriterProviderAbandonProofVersion
	writer.ProofDigest = digest
	return &CompleteRuntimeSlotProviderAbandonRequest{SlotID: slot.ID, Proof: *p, Writer: writer, AllocationObservationDigest: bytes.Repeat([]byte{9}, 32)}
}

func TestRuntimeProviderAbsenceObservationsRequireGraceAndFreshReceiptsIntegration(t *testing.T) {
	f, _, slot, _ := fencedProviderFixture(t)
	ctx := t.Context()
	target, o := providerObservation(slot, "first", false)
	p, err := f.store.RecordRuntimeProviderInstanceObservation(ctx, target, o)
	require.NoError(t, err)
	require.Nil(t, p)
	_, second := providerObservation(slot, "second", false)
	p, err = f.store.RecordRuntimeProviderInstanceObservation(ctx, target, second)
	require.NoError(t, err)
	require.Nil(t, p, "two immediate responses do not establish grace")
	_, err = f.pool.Exec(ctx, `UPDATE manager.runtime_provider_absence SET first_observed_at=first_observed_at-INTERVAL '31 seconds'`)
	require.NoError(t, err)
	p, err = f.store.RecordRuntimeProviderInstanceObservation(ctx, target, second)
	require.NoError(t, err)
	require.Nil(t, p, "replay must not seal a proof after time advances")
	_, positive := providerObservation(slot, "present", true)
	p, err = f.store.RecordRuntimeProviderInstanceObservation(ctx, target, positive)
	require.NoError(t, err)
	require.Nil(t, p)
	var count int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.runtime_provider_absence`).Scan(&count))
	require.Zero(t, count)
	forged := target
	forged.NodeBootID = "replacement-boot"
	_, err = f.store.RecordRuntimeProviderInstanceObservation(ctx, forged, o)
	require.Error(t, err)
	p = confirmedProviderProof(t, f, slot)
	_, err = p.Digest()
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `UPDATE manager.runtime_provider_absence SET last_observed_at=last_observed_at+INTERVAL '1 second'`)
	require.ErrorContains(t, err, "confirmed provider absence proof is immutable")
	_, err = f.store.RecordRuntimeProviderInstanceObservation(ctx, target, positive)
	require.NoError(t, err)
	_, err = f.store.CompleteRuntimeSlotProviderAbandon(ctx, providerCompletionRequest(t, f, slot, p))
	require.ErrorContains(t, err, "revoked")
}

func TestRuntimeProviderAbandonAtomicallyRestoresRemoteCutAndResourcesIntegration(t *testing.T) {
	f, objects, slot, cut := fencedProviderFixture(t)
	ctx := t.Context()
	p := confirmedProviderProof(t, f, slot)
	request := providerCompletionRequest(t, f, slot, p)
	_, err := f.pool.Exec(ctx, `INSERT INTO manager.runtime_node_fences(cluster_id,node_id,node_uid,state,reason) VALUES($1,$2,$3,'revoked','foreign rollout fence')`, slot.ClusterID, slot.NodeID, slot.NodeUID)
	require.NoError(t, err)
	legacy := request.Writer
	require.Error(t, f.store.WithSandboxLock(ctx, f.sandboxID, func(ctx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		_, err := tx.(RootFSWriterCrashAbandonTx).CompleteRootFSWriterCrashAbandon(ctx, &legacy)
		return err
	}), "node API must reject provider proof version")
	terminal, err := f.store.CompleteRuntimeSlotProviderAbandon(ctx, request)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateTerminal, terminal.State)
	require.Equal(t, RuntimeResourceLeaseReleased, terminal.ResourceLeaseState)
	box, err := f.store.GetSandbox(ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, box.DesiredState)
	require.Empty(t, box.RuntimeID)
	fs, err := f.store.GetRootFSFilesystem(ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, cut.Generation.ID, fs.HeadGenerationID)
	grant, err := f.store.GetRootFSWriterGrant(ctx, cut.GrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, grant.State)
	var fenceReason string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT reason FROM manager.runtime_node_fences WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3`, slot.ClusterID, slot.NodeID, slot.NodeUID).Scan(&fenceReason))
	require.Equal(t, "foreign rollout fence", fenceReason)
	recovery, err := f.store.ClaimSandboxRuntimeRecovery(ctx, f.sandboxID, "provider-recovery-test", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, recovery, "existing automatic reconstruction must recognize provider crash completion")
	_, err = f.store.CompleteRuntimeSlotProviderAbandon(ctx, request)
	require.NoError(t, err, "response-lost exact retry")
	changed := *request
	changed.AllocationObservationDigest = bytes.Repeat([]byte{10}, 32)
	_, err = f.store.CompleteRuntimeSlotProviderAbandon(ctx, &changed)
	require.ErrorContains(t, err, "exact retry")
	for range 5 {
		_, err = f.store.InventoryRootFSGeneration(ctx, objects, 100)
		require.NoError(t, err)
	}
	_, err = f.store.DeleteUnreferencedRootFSGenerations(ctx, "", 100)
	require.NoError(t, err)
	assertRetentionContents(t, objects, cut.Generation, 99)
}

func TestRuntimeProviderAbandonRollsBackHeadAndWriterOnResourceConflictIntegration(t *testing.T) {
	f, _, slot, cut := fencedProviderFixture(t)
	ctx := t.Context()
	p := confirmedProviderProof(t, f, slot)
	_, err := f.pool.Exec(ctx, `UPDATE manager.runtime_resource_leases SET lease_state='released',released_at=NOW() WHERE lease_id=$1`, slot.ResourceLease.LeaseID)
	require.NoError(t, err)
	_, err = f.store.CompleteRuntimeSlotProviderAbandon(ctx, providerCompletionRequest(t, f, slot, p))
	require.Error(t, err)
	grant, err := f.store.GetRootFSWriterGrant(ctx, cut.GrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetiring, grant.State)
	fs, err := f.store.GetRootFSFilesystem(ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, f.initial.ID, fs.HeadGenerationID)
	var retained bool
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT committed_generation_ref=$2 AND installed_generation_ref=$3 FROM manager.rootfs_writer_live_progress WHERE grant_id=$1`, cut.GrantID, cut.Generation.ID, f.initial.ID).Scan(&retained))
	require.True(t, retained)
	var count int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.runtime_provider_absence_retirements`).Scan(&count))
	require.Zero(t, count)
}

func TestRuntimeProviderAbandonCannotConvertPlannedPauseToCrashIntegration(t *testing.T) {
	f, _, slot, _ := fencedProviderFixture(t)
	ctx := t.Context()
	p := confirmedProviderProof(t, f, slot)
	_, err := f.pool.Exec(ctx, `UPDATE manager.sandbox_lifecycle_txns SET source=$2 WHERE txn_id=$1`, f.request.LifecycleTxnID, SandboxLifecycleSourceManual)
	require.NoError(t, err)
	_, err = f.store.CompleteRuntimeSlotProviderAbandon(ctx, providerCompletionRequest(t, f, slot, p))
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	grant, err := f.store.GetRootFSWriterGrant(ctx, f.issued.Grant.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetiring, grant.State)
	stored, err := f.store.GetRuntimeSlot(ctx, slot.ID)
	require.NoError(t, err)
	require.Equal(t, RuntimeResourceLeaseActive, stored.ResourceLeaseState)
	fs, err := f.store.GetRootFSFilesystem(ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, f.initial.ID, fs.HeadGenerationID)
}
