package runtimeslot

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

func TestNodeChannelCommandBindsExactTargetAndRequest(t *testing.T) {
	target := testNodeChannelTarget(true)
	claim := testNodeChannelClaim()
	command, err := NewNodeChannelClaimCommand(target, claim)
	if err != nil {
		t.Fatal(err)
	}
	if len(command.RequestID) != 64 {
		t.Fatalf("request ID = %q", command.RequestID)
	}
	exact, err := NewNodeChannelClaimCommand(target, claim)
	if err != nil || exact.RequestID != command.RequestID {
		t.Fatalf("exact retry request ID = %q, %v", exact.RequestID, err)
	}
	changed := command
	changed.Target.NodeBootID = "another-boot"
	if err := changed.Validate(); err == nil || !strings.Contains(err.Error(), "request_id") {
		t.Fatalf("changed command error = %v", err)
	}
}

func TestNodeChannelCommandRejectsCrossTargetPayloads(t *testing.T) {
	networkTarget := testNodeChannelTarget(false)
	network := testNodeChannelNetworkPrepare()
	network.NodeBootID = "another-boot"
	if _, err := NewNodeChannelNetworkPrepareCommand(networkTarget, network); err == nil {
		t.Fatal("cross-boot network preparation was accepted")
	}

	target := testNodeChannelTarget(true)
	claim := testNodeChannelClaim()
	claim.Stage.Identity.NodeUID = "another-node"
	if _, err := NewNodeChannelClaimCommand(target, claim); err == nil {
		t.Fatal("cross-node claim was accepted")
	}

	ready := CommandReadyControlRequest{Proof: testNodeChannelCommandProof()}
	ready.Proof.SlotID = "another-slot"
	if _, err := NewNodeChannelCommandReadyCommand(target, ready); err == nil {
		t.Fatal("cross-slot command readiness was accepted")
	}

	cleanupTarget := testNodeChannelTarget(false)
	cleanup := testNodeChannelCleanupRequest()
	cleanup.AllocationID = "another-allocation"
	if _, err := NewNodeChannelCleanupCommand(cleanupTarget, cleanup); err == nil {
		t.Fatal("cross-allocation cleanup was accepted")
	}
}

func TestNodeChannelNetworkResultRequiresValidPolicyToken(t *testing.T) {
	request := testNodeChannelNetworkPrepare()
	command, err := NewNodeChannelNetworkPrepareCommand(testNodeChannelTarget(false), request)
	if err != nil {
		t.Fatal(err)
	}
	token := rootfshandoff.NetworkPolicyToken{
		PodUID: request.AllocationID, PodSandboxID: RuntimeSlotNetworkIncarnationID(request),
		ClaimID: request.ClaimID, NetworkEpoch: 1, PolicyDigest: request.PolicyDigest,
		PodIP: "192.0.2.2", CtldGeneration: "ctld-1", NetNSIdentity: request.NetNSIdentity,
	}
	result := NodeChannelResult{
		Version: NodeChannelVersion, RequestID: command.RequestID,
		Kind: command.Kind, NetworkPolicyToken: &token,
	}
	if err := result.ValidateFor(command); err != nil {
		t.Fatal(err)
	}
	result.NetworkPolicyToken.ClaimID = "another-claim"
	if err := result.ValidateFor(command); err == nil {
		t.Fatal("network policy token for another request was accepted")
	}
	result.NetworkPolicyToken.ClaimID = request.ClaimID
	result.NetworkPolicyToken.PolicyDigest = strings.Repeat("0", 64)
	if err := result.ValidateFor(command); err == nil {
		t.Fatal("network policy token for another policy was accepted")
	}
}

func TestNodeChannelResultValidatesExactSuccessAndError(t *testing.T) {
	claim, err := NewNodeChannelClaimCommand(testNodeChannelTarget(true), testNodeChannelClaim())
	if err != nil {
		t.Fatal(err)
	}
	response := NodeControlResponse{Phase: string(StateActive)}
	result := NodeChannelResult{
		Version: NodeChannelVersion, RequestID: claim.RequestID,
		Kind: claim.Kind, ControlResponse: &response,
	}
	if err := result.ValidateFor(claim); err != nil {
		t.Fatal(err)
	}
	result.RequestID = strings.Repeat("0", 64)
	if err := result.ValidateFor(claim); err == nil {
		t.Fatal("result for another request was accepted")
	}

	result = NodeChannelResult{
		Version: NodeChannelVersion, RequestID: claim.RequestID, Kind: claim.Kind,
		Error: "node is draining", ErrorClass: NodeChannelErrorUnavailable,
	}
	if err := result.ValidateFor(claim); err != nil {
		t.Fatal(err)
	}
	result.ErrorClass = "retry_sometime"
	if err := result.ValidateFor(claim); err == nil {
		t.Fatal("unknown error class was accepted")
	}
}

func TestNodeChannelCleanupResultRequiresCanonicalProof(t *testing.T) {
	request := testNodeChannelCleanupRequest()
	command, err := NewNodeChannelCleanupCommand(testNodeChannelTarget(false), request)
	if err != nil {
		t.Fatal(err)
	}
	proof := NodeCleanupControlProof{
		Version:     NodeCleanupProofVersion,
		OperationID: request.OperationID, SlotID: request.SlotID, ClusterID: request.ClusterID,
		AllocationID: request.AllocationID, NodeID: request.NodeID, NodeUID: request.NodeUID,
		NodeBootID: request.NodeBootID, NetNSIdentity: request.NetNSIdentity,
		RunscContainerID: request.RunscContainerID,
		RunscAbsent:      true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true,
	}
	digest, err := proof.Digest()
	if err != nil {
		t.Fatal(err)
	}
	proof.ProofDigest = digest
	result := NodeChannelResult{
		Version: NodeChannelVersion, RequestID: command.RequestID,
		Kind: command.Kind, CleanupProof: &proof,
	}
	if err := result.ValidateFor(command); err != nil {
		t.Fatal(err)
	}
	result.CleanupProof.NodeBootID = "another-boot"
	if err := result.ValidateFor(command); err == nil {
		t.Fatal("changed cleanup proof was accepted")
	}
}

func TestNodeChannelRunningForkBindsExactWriterAndCheckpoint(t *testing.T) {
	stage := testNodeChannelClaim().Stage
	binding, err := stage.BindingDigest()
	if err != nil {
		t.Fatal(err)
	}
	fork := rootfshandoff.RunningForkCheckpointRequest{
		OperationID: "fork-operation-1", SourceSandboxID: "sandbox-source",
		TargetSandboxID: "sandbox-target", TargetGenerationID: "generation-target",
	}
	request := NodeRunningForkControlRequest{
		Fork: fork, SourceFilesystemID: stage.Identity.RootFSID,
		SourceWriterGrantID: stage.Identity.WriterGrantID, SourceWriterEpoch: stage.Identity.WriterEpoch,
		BindingVersion: stage.BindingVersion, BindingDigest: hex.EncodeToString(binding[:]),
		ExpectedSourceGenerationID: stage.InitialGeneration,
	}
	command, err := NewNodeChannelRunningForkCommand(testNodeChannelTarget(false), request)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := testNodeChannelRunningForkCheckpoint(t, *stage, fork)
	result := NodeChannelResult{
		Version: NodeChannelVersion, RequestID: command.RequestID,
		Kind: command.Kind, RunningFork: &checkpoint,
	}
	if err := result.ValidateFor(command); err != nil {
		t.Fatal(err)
	}
	result.RunningFork.Proof.SourceWriterGrantID = "another-grant"
	if err := result.ValidateFor(command); err == nil {
		t.Fatal("running-fork checkpoint for another writer was accepted")
	}
}

func TestNodeChannelHelloRequiresCanonicalCapabilities(t *testing.T) {
	hello := testNodeChannelHello()
	if err := hello.Validate(); err != nil {
		t.Fatal(err)
	}
	if hello.Supports(NodeChannelCommandNetworkPrepare) {
		t.Fatal("legacy-capability hello advertised network preparation")
	}
	hello.Capabilities[0], hello.Capabilities[1] = hello.Capabilities[1], hello.Capabilities[0]
	if err := hello.Validate(); err == nil {
		t.Fatal("reordered capabilities were accepted")
	}
	hello = testNodeChannelHello()
	hello.Capabilities = append([]NodeChannelCommandKind{NodeChannelCommandNetworkPrepare}, hello.Capabilities...)
	if err := hello.Validate(); err != nil {
		t.Fatalf("network-capable hello error = %v", err)
	}
	if !hello.Supports(NodeChannelCommandNetworkPrepare) {
		t.Fatal("network-capable hello omitted network preparation")
	}
	hello.Capabilities[0], hello.Capabilities[1] = hello.Capabilities[1], hello.Capabilities[0]
	if err := hello.Validate(); err == nil {
		t.Fatal("misordered network capability was accepted")
	}
	hello = testNodeChannelHello()
	hello.Capabilities = []NodeChannelCommandKind{
		NodeChannelCommandClaim, NodeChannelCommandCommandReady,
		NodeChannelCommandRunningFork, NodeChannelCommandCleanup,
	}
	if err := hello.Validate(); err != nil || !hello.Supports(NodeChannelCommandRunningFork) {
		t.Fatalf("running-fork-capable hello error = %v", err)
	}
}

func testNodeChannelRunningForkCheckpoint(
	t *testing.T,
	stage rootfshandoff.StageRequest,
	fork rootfshandoff.RunningForkCheckpointRequest,
) rootfshandoff.RunningForkCheckpointResult {
	t.Helper()
	root := digest.FromString("running-fork-map").String()
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: rootfsblock.LogicalBlockSize,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: root,
			Object: rootfsblock.ObjectRange{
				Key: "maps/running-fork", Length: 1, Checksum: digest.FromString("running-fork-page").String(),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := stage.BindingDigest()
	if err != nil {
		t.Fatal(err)
	}
	proof := rootfshandoff.RunningForkCheckpointProof{
		Version: rootfshandoff.RunningForkCheckpointVersion, OperationID: fork.OperationID,
		SourceSandboxID: fork.SourceSandboxID, SourceFilesystemID: stage.Identity.RootFSID,
		TargetSandboxID: fork.TargetSandboxID, SourceWriterGrantID: stage.Identity.WriterGrantID,
		SourceWriterEpoch: stage.Identity.WriterEpoch, BindingVersion: stage.BindingVersion,
		BindingDigest: hex.EncodeToString(binding[:]), ExpectedSourceGenerationID: stage.InitialGeneration,
		CheckpointGenerationID: fork.TargetGenerationID, CheckpointSequence: 1,
		CheckpointDescriptorDigest: digest.FromBytes(descriptor).String(),
	}
	proofDigest, err := proof.Digest()
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := rootfshandoff.RunningForkCheckpointResult{
		Generation: rootfshandoff.GenerationDescriptor{
			Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: fork.TargetGenerationID,
			FilesystemID: fork.TargetSandboxID, SourceOCIDigest: digest.FromString("source-image").String(),
			BaseArtifactDigest: digest.FromString("base-artifact").String(), BaseBlockRoot: root,
			CurrentBlockHead: root, WriterEpoch: stage.Identity.WriterEpoch, FormatGeneration: 1,
			DurabilityState: rootfsblock.DurabilityS3, LocatorVersion: 2, Descriptor: descriptor,
		},
		Proof: proof, ProofDigest: hex.EncodeToString(proofDigest[:]),
	}
	if err := checkpoint.Validate(); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func testNodeChannelNetworkPrepare() NodeNetworkPrepareControlRequest {
	policy := `{"mode":"block-all"}`
	return NodeNetworkPrepareControlRequest{
		OperationID: "operation-1", ClaimID: "claim-1", SlotID: "slot-1",
		ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
		NetworkPolicy: policy, PolicyDigest: NetworkPolicyDigest(policy),
	}
}

func testNodeChannelHello() NodeChannelHello {
	return NodeChannelHello{
		Version: NodeChannelVersion, AgentInstanceID: "agent-1",
		ClusterID: "cluster-1", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1",
		Capabilities: []NodeChannelCommandKind{
			NodeChannelCommandClaim, NodeChannelCommandCommandReady, NodeChannelCommandCleanup,
		},
	}
}

func testNodeChannelTarget(withControl bool) NodeChannelTarget {
	target := NodeChannelTarget{
		SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
		NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
	}
	if withControl {
		target.ControlEndpoint = "unix:///var/run/sandbox0/nomad-slots/task.sock"
	}
	return target
}

func testNodeChannelClaim() NodeClaimControlRequest {
	return testNodeClaimControlRequest()
}

func testNodeChannelCommandProof() CommandReadyProof {
	return CommandReadyProof{
		Version: CommandReadyProofVersion, SlotID: "slot-1", OperationID: "operation-1",
		ClaimID: "claim-1", LaunchAttempt: "launch-1", RunscContainerID: NomadRunscContainerID("slot-1"),
		ProcdInstanceID: "procd-1", ProcdAddress: "http://10.0.0.2:49983",
		RequestMethod: "PUT", RequestPath: ProcdCommandReadyProbePath, ResponseStatus: 200,
		ResponseBodyDigest: strings.Repeat("4", 64),
	}
}

func testNodeChannelCleanupRequest() NodeCleanupControlRequest {
	return NodeCleanupControlRequest{
		OperationID: "cleanup-1", SlotID: "slot-1", ClusterID: "cluster-1",
		AllocationID: "allocation-1", NodeID: "node-1", NodeUID: "node-uid-1",
		NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
		RunscContainerID: NomadRunscContainerID("slot-1"),
	}
}
