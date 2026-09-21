package sandboxstore

import (
	"bytes"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationRestoreStoreFixture(t *testing.T, suffix string) (*nomadPauseStoreFixture, runtimecontrol.MigrationAssignment, rootfshandoff.StageRequest, *IssueRootFSWriterGrantRequest, *StartRuntimeSlotRequest) {
	t.Helper()
	f, _, fence, proof := migrationFenceStoreFixture(t, suffix)
	assignment := fence.PublicationRequest.Assignment
	_, err := f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, fence)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFence(f.ctx, fence, proof))
	target, err := f.store.AcquireNomadSandboxMigrationTarget(f.ctx, assignment)
	require.NoError(t, err)
	issue := rootFSWriterGrantTestIssueRequest(f.sandboxID, "restore-writer", target.ClaimID, target.ID, nil)
	issue.OperationID = assignment.OperationID
	issue.ExpectedFilesystemID, issue.InitialGenerationID = target.FilesystemID, target.SourceGenerationID
	issue.ExpectedWriterEpoch = f.writerEpoch
	issue.NodeUID, issue.NodeBootID, issue.NodeName = target.NodeUID, target.NodeBootID, target.NodeID
	issue.RuntimeNamespace, issue.RuntimeIncarnationID, issue.RuntimeID = target.AllocationNamespace, target.AllocationID, protocol.NomadTaskName
	issue.RuntimeGeneration, issue.ConsumeExpiresAt = strconv.FormatInt(assignment.Target.RuntimeGeneration, 10), target.ClaimLeaseExpiresAt
	issue.GateParent = digest.FromString("restore-parent").String()
	cut := fence.PublicationRequest.Capture.RootFS.Generation
	stage := rootfshandoff.StageRequest{BindingVersion: rootfshandoff.WriterBindingVersion,
		Parent: issue.GateParent, InitialGeneration: cut.GenerationID, Generation: &cut,
		Identity: rootfshandoff.Identity{NodeUID: target.NodeUID, BootID: target.NodeBootID, RuntimeGeneration: issue.RuntimeGeneration,
			AllocationID: target.AllocationID, NetworkIncarnationID: "restore-network", TaskName: protocol.NomadTaskName,
			SourceOCIDigest: cut.SourceOCIDigest, RootFSDriver: "nomad-driver", RuntimeClass: "sandbox0-gvisor", SlotNonce: target.ID,
			ClaimID: target.ClaimID, LaunchAttempt: "restore-launch", RootFSID: cut.FilesystemID, WriterEpoch: f.writerEpoch + 1,
			WriterGrantID: issue.GrantID, WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest(issue.RawToken)},
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{AllocationID: target.AllocationID, NetworkIncarnationID: "restore-network", ClaimID: target.ClaimID,
			NetworkEpoch: 1, PolicyDigest: target.ClaimNetworkPolicyDigest, SourceIP: "192.0.2.1", CtldGeneration: "ctld-1", NetNSIdentity: target.NetNSIdentity},
	}
	revision, err := assignment.Target.Revision()
	require.NoError(t, err)
	resourceDigest, err := target.ResourceLease.Digest()
	require.NoError(t, err)
	stage.Labels = map[string]string{protocol.RuntimeAssignmentRevisionLabel: revision, protocol.RuntimeResourceLeaseDigestLabel: resourceDigest}
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	issue.BindingDigest = binding[:]
	_, err = f.store.IssueNomadSandboxMigrationTargetWriter(f.ctx, assignment, issue)
	require.NoError(t, err)
	start := &StartRuntimeSlotRequest{SlotID: target.ID, AllocationID: target.AllocationID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID,
		OperationID: assignment.OperationID, ClaimID: target.ClaimID, LaunchAttempt: stage.Identity.LaunchAttempt,
		RunscContainerID: protocol.NomadRunscContainerID(target.ID), RootFSBindingDigest: binding[:],
		ClaimNetworkDigest: bytes.Repeat([]byte{0x71}, 32), ResourceLeaseID: target.ResourceLease.LeaseID, ResourceLeaseDigest: target.ResourceLeaseDigest}
	return f, assignment, stage, issue, start
}

func TestNomadMigrationRestoreRequiresDurableAuthorizationAndConsumedWriterIntegration(t *testing.T) {
	f, assignment, stage, issue, start := migrationRestoreStoreFixture(t, "restore-authorization")
	start.MigrationRestoreDigest = strings.Repeat("ab", 32)
	_, err := f.store.StartRuntimeSlot(f.ctx, start)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	request, err := f.store.AuthorizeNomadSandboxMigrationRestore(f.ctx, assignment, stage)
	require.NoError(t, err)
	start.MigrationRestoreDigest, err = request.Digest()
	require.NoError(t, err)
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "issued writer is not yet consumed")
	_, err = f.store.ConsumeRootFSWriterGrant(f.ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issue.GrantID, WriterEpoch: stage.Identity.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: issue.BindingVersion, BindingDigest: issue.BindingDigest,
		ConsumerNodeUID: issue.NodeUID, ConsumerAgentUID: "target-ctld", LeaseTTL: time.Minute})
	require.NoError(t, err)
	const workers = 8
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() { _, errs[i] = NewPGSandboxStore(f.pool).StartRuntimeSlot(f.ctx, start) })
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	actual, err := f.store.GetRuntimeSlot(f.ctx, start.SlotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateStarting, actual.State)
	require.Equal(t, stage.Identity.LaunchAttempt, actual.LaunchAttempt)
	visible, err := f.store.GetRuntimeSlotBySandboxID(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, f.slotID, visible.ID, "starting is not a routed generation commit")
	sandbox, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, assignment.SourceGeneration, sandbox.RuntimeGeneration)
	replay, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationRestore(f.ctx, assignment, stage)
	require.NoError(t, err)
	require.Equal(t, *request, *replay)
	changed := stage
	changed.Identity.LaunchAttempt += "changed"
	_, err = f.store.AuthorizeNomadSandboxMigrationRestore(f.ctx, assignment, changed)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET restore_request=NULL,restore_digest=NULL WHERE operation_id=$1`, assignment.OperationID)
	require.Error(t, err, "durable restore intent is immutable")
	fresh := *start
	fresh.MigrationRestoreDigest = ""
	_, err = f.store.StartRuntimeSlot(f.ctx, &fresh)
	require.ErrorIs(t, err, ErrRuntimeSlotInvalid, "ordinary start cannot replace a restore retry")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, start.SlotID)
	require.NoError(t, err)
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "expired node authority cannot acknowledge a starting retry")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()+INTERVAL '1 minute' WHERE slot_id=$1`, start.SlotID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_lease_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, start.SlotID)
	require.NoError(t, err)
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "an expired starting retry cannot revive execution authority")
}

func TestNomadMigrationRestoreRejectsUnboundWriterAndSecretIntegration(t *testing.T) {
	f, assignment, stage, _, _ := migrationRestoreStoreFixture(t, "restore-binding")
	changed := stage
	changed.Identity.WriterGrantToken = "secret"
	_, err := f.store.AuthorizeNomadSandboxMigrationRestore(f.ctx, assignment, changed)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	changed = stage
	changed.Identity.WriterGrantTokenDigest = strings.Repeat("ab", 32)
	_, err = f.store.AuthorizeNomadSandboxMigrationRestore(f.ctx, assignment, changed)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	changed = stage
	changed.Parent = digest.FromString("other-parent").String()
	_, err = f.store.AuthorizeNomadSandboxMigrationRestore(f.ctx, assignment, changed)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.store.AuthorizeNomadSandboxMigrationRestore(f.ctx, assignment, stage)
	require.NoError(t, err)
}

func TestNomadMigrationRestoreRejectsFencedStorageEpochIntegration(t *testing.T) {
	f, assignment, stage, issue, start := migrationRestoreStoreFixture(t, "restore-fenced-epoch")
	request, err := f.store.AuthorizeNomadSandboxMigrationRestore(f.ctx, assignment, stage)
	require.NoError(t, err)
	start.MigrationRestoreDigest, err = request.Digest()
	require.NoError(t, err)
	_, err = f.store.ConsumeRootFSWriterGrant(f.ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issue.GrantID, WriterEpoch: stage.Identity.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: issue.BindingVersion, BindingDigest: issue.BindingDigest,
		ConsumerNodeUID: issue.NodeUID, ConsumerAgentUID: "target-ctld", LeaseTTL: time.Minute})
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.rootfs_filesystems SET writer_epoch=writer_epoch+1 WHERE filesystem_id=$1`, f.filesystem.ID)
	require.NoError(t, err)
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	slot, err := f.store.GetRuntimeSlot(f.ctx, start.SlotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateClaiming, slot.State)
}
