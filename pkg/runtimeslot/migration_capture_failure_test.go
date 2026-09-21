package runtimeslot

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/require"
)

func captureFailureProtocolFixture(t *testing.T) (MigrationCaptureFailureRequest, MigrationCaptureFailureProof) {
	t.Helper()
	s := migrationProtocolRequest()
	target := s.Target
	lease, err := NewRuntimeResourceLease("source-claim-operation", "source-claim", target.SlotID, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID,
		RuntimeResourceRequest{Version: RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 128 << 20, PIDsLimit: 1024}, "0", "0")
	require.NoError(t, err)
	rd, err := lease.Digest()
	require.NoError(t, err)
	s.ResourceLeaseDigest = strings.TrimPrefix(rd, "sha256:")
	cd, err := s.Digest()
	require.NoError(t, err)
	op := MigrationCaptureFailureOperationID(s.OperationID)
	c := NodeCleanupControlRequest{OperationID: op, WriterOperationID: op, WriterRetireKind: WriterRetireKindCrashAbandon,
		WriterGrantID: "source-writer", WriterAuthorityDigest: cd, SlotID: target.SlotID, ClusterID: target.ClusterID, AllocationID: target.AllocationID,
		NodeID: target.NodeID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID, NetNSIdentity: "netns-v1:1:2", RunscContainerID: NomadRunscContainerID(target.SlotID),
		Resources: lease, ResourceLeaseDigest: s.ResourceLeaseDigest}
	request := MigrationCaptureFailureRequest{Capture: MigrationCapture{Request: s, RequestDigest: cd, State: MigrationCaptureUncertain}, Cleanup: c}
	want, err := request.Digest()
	require.NoError(t, err)
	p := NodeCleanupControlProof{Version: NodeCleanupProofVersion, OperationID: c.OperationID, WriterOperationID: c.WriterOperationID, WriterRetireKind: c.WriterRetireKind,
		SlotID: c.SlotID, ClusterID: c.ClusterID, AllocationID: c.AllocationID, NodeID: c.NodeID, NodeUID: c.NodeUID, NodeBootID: c.NodeBootID, NetNSIdentity: c.NetNSIdentity,
		RunscContainerID: c.RunscContainerID, WriterGrantID: c.WriterGrantID, WriterAuthorityDigest: c.WriterAuthorityDigest, RootFSOperationID: c.WriterOperationID, RootFSProofDigest: strings.Repeat("a", 64),
		Resources: c.Resources, ResourceLeaseID: c.Resources.LeaseID, ResourceLeaseDigest: c.ResourceLeaseDigest,
		RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true, ResourceCgroupAbsent: true}
	p.ProofDigest, err = p.Digest()
	require.NoError(t, err)
	proof := MigrationCaptureFailureProof{RequestDigest: want, Cleanup: p}
	require.NoError(t, proof.ValidateFor(request))
	return request, proof
}

type captureFailureExecutor struct {
	cleanup *MigrationCaptureFailureProof
	final   *MigrationCaptureFailureFinalizeProof
	err     error
}

func (e *captureFailureExecutor) CleanupFailedMigrationCapture(context.Context, MigrationCaptureFailureRequest) (*MigrationCaptureFailureProof, error) {
	return e.cleanup, e.err
}
func (e *captureFailureExecutor) FinalizeFailedMigrationCapture(context.Context, MigrationCaptureFailureFinalizeRequest) (*MigrationCaptureFailureFinalizeProof, error) {
	return e.final, e.err
}

func TestMigrationCaptureFailureChannelSeparatesCleanupFinalizationAndOtherCustody(t *testing.T) {
	request, proof := captureFailureProtocolFixture(t)
	cleanup, err := NewNodeChannelMigrationCaptureFailureCleanupCommand(request)
	require.NoError(t, err)
	finalRequest := MigrationCaptureFailureFinalizeRequest{Request: request, Proof: proof}
	final, err := NewNodeChannelMigrationCaptureFailureFinalizeCommand(finalRequest)
	require.NoError(t, err)
	fd, err := finalRequest.Digest()
	require.NoError(t, err)
	executor := &captureFailureExecutor{cleanup: &proof, final: &MigrationCaptureFailureFinalizeProof{RequestDigest: fd, RootFSArtifactsAbsent: true, ImageAbsent: true}}
	agent := &NodeChannelAgent{config: NodeChannelAgentConfig{MigrationCaptureFailureExecutor: executor, RunningForkTimeout: time.Second}}
	for _, cmd := range []NodeChannelCommand{cleanup, final} {
		result := agent.execute(t.Context(), cmd)
		require.NoError(t, result.ValidateFor(cmd))
		require.Empty(t, result.Error)
		mixed := result
		mixed.MigrationCapture = &request.Capture
		require.Error(t, mixed.ValidateFor(cmd))
		changed := cmd
		changed.Target.NodeBootID = "successor-boot"
		_, err := sealNodeChannelCommand(changed)
		require.Error(t, err)
		changed = cmd
		changed.Cleanup = &request.Cleanup
		_, err = sealNodeChannelCommand(changed)
		require.Error(t, err)
		executor.err = errdefs.ErrUnavailable
		failed := agent.execute(t.Context(), cmd)
		require.NotEmpty(t, failed.Error)
		require.Nil(t, failed.MigrationCaptureFailureCleanup)
		require.Nil(t, failed.MigrationCaptureFailureFinalize)
		require.NoError(t, failed.ValidateFor(cmd))
		executor.err = nil
	}
	result := agent.execute(t.Context(), cleanup)
	require.Error(t, result.ValidateFor(final))
	changed := request
	changed.Capture.State = MigrationCaptureComplete
	_, err = changed.Digest()
	require.Error(t, err, "a completed cut is not failed-capture authority")
	changed = request
	changed.Cleanup.WriterRetireKind = WriterRetireKindMigration
	_, err = changed.Digest()
	require.Error(t, err, "normal source handoff is a different retirement protocol")
}
