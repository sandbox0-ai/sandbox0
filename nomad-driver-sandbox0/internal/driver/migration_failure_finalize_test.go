package driver

import (
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func failedMigrationDriverReceipt(t *testing.T, r protocol.MigrationRestoreRequest) *nomadruntime.MigrationFailureStopCustody {
	t.Helper()
	failure := protocol.MigrationFailureRequest{Restore: r, Reason: protocol.MigrationFailureDestinationUnavailable}
	digest, err := failure.Digest()
	require.NoError(t, err)
	target := r.Image.Target
	stop := protocol.MigrationFailureStopProof{RequestDigest: digest, ContainerID: protocol.NomadRunscContainerID(target.SlotID), ContainerAbsent: true}
	resourceDigest, err := r.Image.Resources.Digest()
	require.NoError(t, err)
	op := protocol.MigrationFailureCleanupOperationID(r.Image.Publication.Assignment.OperationID)
	c := protocol.NodeCleanupControlRequest{OperationID: op, WriterOperationID: op, WriterRetireKind: protocol.WriterRetireKindCrashAbandon,
		WriterGrantID: r.Stage.Identity.WriterGrantID, WriterAuthorityDigest: digest, SlotID: target.SlotID, ClusterID: target.ClusterID, AllocationID: target.AllocationID,
		NodeID: target.NodeID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID, RunscContainerID: stop.ContainerID,
		NetNSIdentity: r.Stage.ExpectedPolicyToken.NetNSIdentity, Resources: r.Image.Resources, ResourceLeaseDigest: strings.TrimPrefix(resourceDigest, "sha256:")}
	request := protocol.MigrationFailureCleanupRequest{Failure: protocol.MigrationFailureStopReceipt{Request: failure, Proof: stop}, Cleanup: c}
	cleanupDigest, err := request.Digest()
	require.NoError(t, err)
	p := protocol.NodeCleanupControlProof{Version: protocol.NodeCleanupProofVersion, OperationID: c.OperationID, WriterOperationID: c.WriterOperationID,
		WriterRetireKind: c.WriterRetireKind, WriterGrantID: c.WriterGrantID, WriterAuthorityDigest: c.WriterAuthorityDigest,
		SlotID: c.SlotID, ClusterID: c.ClusterID, AllocationID: c.AllocationID, NodeID: c.NodeID, NodeUID: c.NodeUID, NodeBootID: c.NodeBootID,
		NetNSIdentity: c.NetNSIdentity, RunscContainerID: c.RunscContainerID, Resources: c.Resources, ResourceLeaseID: c.Resources.LeaseID, ResourceLeaseDigest: c.ResourceLeaseDigest,
		RootFSOperationID: c.WriterOperationID, RootFSProofDigest: strings.Repeat("b", 64), RunscAbsent: true, StableMountAbsent: true,
		RootFSWriterAbsent: true, NetworkPolicyAbsent: true, ResourceCgroupAbsent: true}
	p.ProofDigest, err = p.Digest()
	require.NoError(t, err)
	final := protocol.MigrationFailureFinalizeRequest{Request: request, Proof: protocol.MigrationFailureCleanupProof{RequestDigest: cleanupDigest, Cleanup: p}}
	want, err := final.Digest()
	require.NoError(t, err)
	return &nomadruntime.MigrationFailureStopCustody{Request: failure, RequestDigest: digest, Proof: &stop,
		Cleanup: &nomadruntime.MigrationFailureCleanupCustody{Request: request, RequestDigest: cleanupDigest,
			Finalization: &nomadruntime.MigrationFailureFinalization{Request: final, RequestDigest: want,
				Proof: &protocol.MigrationFailureFinalizeProof{RequestDigest: want, RootFSArtifactsAbsent: true, ImageAbsent: true}}}}
}

func TestFailedMigrationFinalizationAllowsOnlyExitedCarrierRecovery(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "restored-handle", true: "stale-warm-handle"}[stale], func(t *testing.T) {
			h, claim, runner, custodian, _ := migrationRestoreHandleFixture(t)
			if !stale {
				require.NoError(t, h.Claim(claim))
				h.stopExitWatch()
				h.stopConsumerRenewal()
			}
			custodian.custody.Failure = failedMigrationDriverReceipt(t, *claim.MigrationRestore)
			runner.mu.Lock()
			runner.stateErr = errdefs.ErrNotFound
			runner.mu.Unlock()
			before := runner.callsSnapshot()
			complete, err := h.observeMigrationFinalization()
			require.NoError(t, err)
			require.True(t, complete)
			require.True(t, h.migrationFinalized)
			require.Equal(t, phaseExited, h.PersistedState().Phase)
			require.Equal(t, 1, h.exitResult.ExitCode)
			require.Error(t, h.Claim(claim))
			require.NoError(t, h.Stop(time.Second, "TERM"))
			require.NoError(t, h.Close(true))
			require.Equal(t, before, runner.callsSnapshot(), "physical cleanup receipt must not cause execution or a second teardown")
		})
	}
}

func TestFailedMigrationFinalizationRejectsIncompleteOrChangedCustody(t *testing.T) {
	for _, which := range []string{"image", "cleanup", "container", "socket", "in-flight"} {
		t.Run(which, func(t *testing.T) {
			h, claim, _, custodian, _ := migrationRestoreHandleFixture(t)
			custodian.custody.Failure = failedMigrationDriverReceipt(t, *claim.MigrationRestore)
			f := custodian.custody.Failure.Cleanup.Finalization
			switch which {
			case "image":
				f.Proof.ImageAbsent = false
			case "cleanup":
				f.Request.Proof.Cleanup.RootFSWriterAbsent = false
			case "container":
				h.containerID = "different-container"
			case "socket":
				h.socketPath += ".other"
			case "in-flight":
				h.migrationInFlight = true
			}
			complete, err := h.observeMigrationFinalization()
			require.Error(t, err)
			require.False(t, complete)
			require.False(t, h.migrationFinalized)
		})
	}
}
