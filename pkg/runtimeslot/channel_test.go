package runtimeslot

import (
	"strings"
	"testing"
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

func TestNodeChannelHelloRequiresCanonicalCapabilities(t *testing.T) {
	hello := testNodeChannelHello()
	if err := hello.Validate(); err != nil {
		t.Fatal(err)
	}
	hello.Capabilities[0], hello.Capabilities[1] = hello.Capabilities[1], hello.Capabilities[0]
	if err := hello.Validate(); err == nil {
		t.Fatal("reordered capabilities were accepted")
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
