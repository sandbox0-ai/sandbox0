package runtimeslotnode

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationFailureCleanupChannelExecutor struct {
	calls         atomic.Int32
	finalizations atomic.Int32
}

func (e *migrationFailureCleanupChannelExecutor) FinalizeFailedMigrationDestination(_ context.Context, r protocol.MigrationFailureFinalizeRequest) (*protocol.MigrationFailureFinalizeProof, error) {
	e.finalizations.Add(1)
	want, err := r.Digest()
	return &protocol.MigrationFailureFinalizeProof{RequestDigest: want, RootFSArtifactsAbsent: true, ImageAbsent: true}, err
}

func (e *migrationFailureCleanupChannelExecutor) CleanupFailedMigrationDestination(_ context.Context, r protocol.MigrationFailureCleanupRequest) (*protocol.MigrationFailureCleanupProof, error) {
	e.calls.Add(1)
	p, err := migrationChannelPhysicalCleanupProof(r.Cleanup)
	if err != nil {
		return nil, err
	}
	want, err := r.Digest()
	return &protocol.MigrationFailureCleanupProof{RequestDigest: want, Cleanup: p}, err
}

func migrationFailureChannelRequest(t *testing.T) protocol.MigrationFailureCleanupRequest {
	t.Helper()
	capture := protocol.MigrationCaptureRequest{Target: protocol.NodeChannelTarget{SlotID: "source-slot", ClusterID: "cluster-1", AllocationID: "source-allocation",
		NodeID: "source-node", NodeUID: "source-uid", NodeBootID: "source-boot", ControlEndpoint: "unix:///private/source.sock"},
		OperationID: "migration-1", LifecycleEpoch: 2, SandboxID: "sandbox-1", SourceGeneration: 1, ProcdInstanceID: "procd-1",
		AssignmentRevision: strings.Repeat("a", 64), BindingDigest: strings.Repeat("b", 64), ResourceLeaseDigest: strings.Repeat("c", 64)}
	publication := migrationChannelPublication(t, capture)
	target := protocol.NodeChannelTarget{SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1", ControlEndpoint: "unix:///private/target.sock"}
	resources, err := protocol.NewRuntimeResourceLease(capture.OperationID, "target-claim", target.SlotID, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID,
		protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 128 << 20, PIDsLimit: 1024}, "0", "0")
	require.NoError(t, err)
	image := protocol.MigrationImagePrepareRequest{Target: target, Resources: resources, Publication: publication, Receipt: migrationChannelReceipt(t, publication)}
	executor := &migrationChannelExecutor{}
	prepared, err := executor.PrepareMigrationImage(t.Context(), image)
	require.NoError(t, err)
	fence := protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: image.Receipt}
	proof, err := executor.FenceMigrationSource(t.Context(), fence)
	require.NoError(t, err)
	stage := testChannelClaimRequest().Stage.WithoutWriterGrantToken()
	cut := publication.Capture.RootFS.Generation
	stage.Generation, stage.InitialGeneration = &cut, cut.GenerationID
	stage.Identity.RootFSID, stage.Identity.SourceOCIDigest, stage.Identity.WriterEpoch = cut.FilesystemID, cut.SourceOCIDigest, cut.WriterEpoch+1
	stage.Identity.NodeUID, stage.Identity.BootID = target.NodeUID, target.NodeBootID
	stage.Identity.AllocationID, stage.Identity.SlotNonce, stage.Identity.ClaimID = target.AllocationID, target.SlotID, resources.ClaimID
	stage.Identity.TaskName, stage.Identity.RuntimeClass, stage.Identity.RootFSDriver = protocol.NomadTaskName, "sandbox0-gvisor", "nomad-driver"
	stage.Identity.RuntimeGeneration = strconv.FormatInt(publication.Assignment.Target.RuntimeGeneration, 10)
	stage.ExpectedPolicyToken.AllocationID, stage.ExpectedPolicyToken.ClaimID = target.AllocationID, resources.ClaimID
	revision, err := publication.Assignment.Target.Revision()
	require.NoError(t, err)
	resourceDigest, err := resources.Digest()
	require.NoError(t, err)
	stage.Labels = map[string]string{protocol.RuntimeAssignmentRevisionLabel: revision, protocol.RuntimeResourceLeaseDigestLabel: resourceDigest}
	failure := protocol.MigrationFailureRequest{Restore: protocol.MigrationRestoreRequest{Image: image, Prepared: *prepared, Fence: fence, Proof: *proof, Stage: stage}, Reason: protocol.MigrationFailureDestinationUnavailable}
	want, err := failure.Digest()
	require.NoError(t, err)
	container := protocol.NomadRunscContainerID(target.SlotID)
	cleanupID := protocol.MigrationFailureCleanupOperationID(capture.OperationID)
	request := protocol.MigrationFailureCleanupRequest{Failure: protocol.MigrationFailureStopReceipt{Request: failure,
		Proof: protocol.MigrationFailureStopProof{RequestDigest: want, ContainerID: container, ContainerAbsent: true}},
		Cleanup: protocol.NodeCleanupControlRequest{OperationID: cleanupID, WriterOperationID: cleanupID, WriterRetireKind: protocol.WriterRetireKindCrashAbandon,
			WriterGrantID: stage.Identity.WriterGrantID, WriterAuthorityDigest: want, SlotID: target.SlotID, ClusterID: target.ClusterID, AllocationID: target.AllocationID,
			NodeID: target.NodeID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID, NetNSIdentity: stage.ExpectedPolicyToken.NetNSIdentity,
			RunscContainerID: container, Resources: resources, ResourceLeaseDigest: strings.TrimPrefix(resourceDigest, "sha256:")}}
	_, err = request.Digest()
	require.NoError(t, err)
	return request
}

func TestMigrationFailureCleanupRequiresAuthenticatedCapability(t *testing.T) {
	for _, supported := range []bool{false, true} {
		hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
		require.NoError(t, err)
		server, files := newNodeChannelTLSServer(t, hub)
		executor := &migrationFailureCleanupChannelExecutor{}
		config := protocol.NodeChannelAgentConfig{BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert, ClientKeyFile: files.clientKey,
			TokenFile: files.token, PeerURISAN: testNodeChannelServerURI, NodeUID: "node-uid-1", NodeBootIDFile: files.boot, ClusterID: "cluster-1", NodeID: "node-1",
			Executor: &channelTestExecutor{}, Capacity: channelTestCapacity(), AgentInstanceID: "agent-1", ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond}
		if supported {
			config.MigrationFailureCleanupExecutor = executor
			config.MigrationFailureFinalizeExecutor = executor
		}
		agent, err := protocol.NewNodeChannelAgent(config)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- agent.Run(ctx) }()
		t.Cleanup(func() { cancel(); <-done; server.Close(); hub.Close() })
		waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-1")
		request := migrationFailureChannelRequest(t)
		result, err := hub.CleanupFailedMigrationDestination(ctx, request)
		if supported {
			require.NoError(t, err)
			require.NoError(t, result.ValidateFor(request))
			require.EqualValues(t, 1, executor.calls.Load())
		} else {
			require.Error(t, err)
			require.Nil(t, result)
			require.Zero(t, executor.calls.Load())
		}
		calls := executor.calls.Load()
		command, err := protocol.NewNodeChannelMigrationFailureCleanupCommand(request)
		require.NoError(t, err)
		for _, stale := range []string{"node", "boot", "allocation", "proof"} {
			changed := command
			switch stale {
			case "node":
				changed.Target.NodeUID = "another-node"
			case "boot":
				changed.Target.NodeBootID = "stale-boot"
			case "allocation":
				changed.Target.AllocationID = "other-allocation"
			case "proof":
				copy := *command.MigrationFailureCleanup
				copy.Failure.Proof.ContainerAbsent = false
				changed.MigrationFailureCleanup = &copy
			}
			attempt, stop := context.WithTimeout(ctx, 100*time.Millisecond)
			_, err = hub.dispatch(attempt, changed)
			stop()
			require.Error(t, err)
			require.Equal(t, calls, executor.calls.Load())
		}
		fixture := &migrationFailureCleanupChannelExecutor{}
		cleanup, err := fixture.CleanupFailedMigrationDestination(ctx, request)
		require.NoError(t, err)
		finalization := protocol.MigrationFailureFinalizeRequest{Request: request, Proof: *cleanup}
		finalized, err := hub.FinalizeFailedMigrationDestination(ctx, finalization)
		if supported {
			require.NoError(t, err)
			require.NoError(t, finalized.ValidateFor(finalization))
			require.EqualValues(t, 1, executor.finalizations.Load())
		} else {
			require.Error(t, err)
			require.Nil(t, finalized)
			require.Zero(t, executor.finalizations.Load())
		}
		finalCommand, err := protocol.NewNodeChannelMigrationFailureFinalizeCommand(finalization)
		require.NoError(t, err)
		finalCommand.Target.NodeBootID = "stale-boot"
		_, err = hub.dispatch(ctx, finalCommand)
		require.Error(t, err)
		require.EqualValues(t, map[bool]int{false: 0, true: 1}[supported], executor.finalizations.Load())
	}
}

func migrationChannelPhysicalCleanupProof(c protocol.NodeCleanupControlRequest) (protocol.NodeCleanupControlProof, error) {
	p := protocol.NodeCleanupControlProof{Version: protocol.NodeCleanupProofVersion, OperationID: c.OperationID, WriterOperationID: c.WriterOperationID,
		WriterRetireKind: c.WriterRetireKind, WriterGrantID: c.WriterGrantID, WriterAuthorityDigest: c.WriterAuthorityDigest,
		SlotID: c.SlotID, ClusterID: c.ClusterID, AllocationID: c.AllocationID, NodeID: c.NodeID, NodeUID: c.NodeUID, NodeBootID: c.NodeBootID,
		NetNSIdentity: c.NetNSIdentity, RunscContainerID: c.RunscContainerID, Resources: c.Resources,
		ResourceLeaseID: c.Resources.LeaseID, ResourceLeaseDigest: c.ResourceLeaseDigest, RootFSOperationID: c.WriterOperationID, RootFSProofDigest: strings.Repeat("a", 64),
		RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true, ResourceCgroupAbsent: true}
	var err error
	p.ProofDigest, err = p.Digest()
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	return p, nil
}
