package runtimeslot

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

type testNodeChannelExecutor struct {
	claimErr   error
	commandErr error
	cleanupErr error
}

func (e *testNodeChannelExecutor) PrepareNetwork(
	_ context.Context,
	_ NodeChannelTarget,
	request NodeNetworkPrepareControlRequest,
) (rootfshandoff.NetworkPolicyToken, error) {
	return rootfshandoff.NetworkPolicyToken{
		PodUID: request.AllocationID, PodSandboxID: "allocation-network-1",
		ClaimID: request.ClaimID, NetworkEpoch: 1, PolicyDigest: request.PolicyDigest,
		PodIP: "192.0.2.2", CtldGeneration: "ctld-1", NetNSIdentity: request.NetNSIdentity,
	}, nil
}

func (e *testNodeChannelExecutor) Claim(
	context.Context,
	NodeChannelTarget,
	NodeClaimControlRequest,
) (NodeControlResponse, error) {
	return NodeControlResponse{Phase: string(StateActive)}, e.claimErr
}

func (e *testNodeChannelExecutor) CommandReady(
	context.Context,
	NodeChannelTarget,
	CommandReadyControlRequest,
) (NodeControlResponse, error) {
	return NodeControlResponse{Phase: string(StateActive)}, e.commandErr
}

func (e *testNodeChannelExecutor) Cleanup(
	_ context.Context,
	_ NodeChannelTarget,
	request NodeCleanupControlRequest,
) (NodeCleanupControlProof, error) {
	if e.cleanupErr != nil {
		return NodeCleanupControlProof{}, e.cleanupErr
	}
	proof := NodeCleanupControlProof{
		Version:     NodeCleanupProofVersion,
		OperationID: request.OperationID, WriterOperationID: request.WriterOperationID,
		SlotID: request.SlotID, ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, RunscContainerID: request.RunscContainerID,
		WriterGrantID: request.WriterGrantID, WriterFenceDigest: request.WriterFenceDigest,
		RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true,
	}
	if request.WriterGrantID != "" {
		proof.RootFSCrashOperationID = request.WriterOperationID
		proof.RootFSCrashProofDigest = strings.Repeat("5", 64)
	}
	digest, err := proof.Digest()
	if err != nil {
		return NodeCleanupControlProof{}, err
	}
	proof.ProofDigest = digest
	return proof, nil
}

func TestNodeChannelAgentExecutesExactCommandsAndClassifiesErrors(t *testing.T) {
	executor := &testNodeChannelExecutor{}
	agent := &NodeChannelAgent{config: NodeChannelAgentConfig{
		Executor: executor, NetworkExecutor: executor,
		OperationTimeout: defaultNodeChannelOperationTimeout,
	}}
	network, err := NewNodeChannelNetworkPrepareCommand(testNodeChannelTarget(false), testNodeChannelNetworkPrepare())
	if err != nil {
		t.Fatal(err)
	}
	result := agent.execute(t.Context(), network)
	if err := result.ValidateFor(network); err != nil || result.NetworkPolicyToken == nil {
		t.Fatalf("network result = %+v, %v", result, err)
	}
	claim, err := NewNodeChannelClaimCommand(testNodeChannelTarget(true), testNodeClaimControlRequest())
	if err != nil {
		t.Fatal(err)
	}
	result = agent.execute(t.Context(), claim)
	if err := result.ValidateFor(claim); err != nil || result.ControlResponse == nil {
		t.Fatalf("claim result = %+v, %v", result, err)
	}

	executor.claimErr = errdefs.ErrFailedPrecondition
	result = agent.execute(t.Context(), claim)
	if err := result.ValidateFor(claim); err != nil {
		t.Fatal(err)
	}
	if result.ErrorClass != NodeChannelErrorFailedPrecondition || result.Error == "" {
		t.Fatalf("classified result = %+v", result)
	}
}

func TestNodeChannelAgentRejectsInvalidExecutorResult(t *testing.T) {
	executor := &testNodeChannelExecutor{}
	agent := &NodeChannelAgent{config: NodeChannelAgentConfig{
		Executor: executor, OperationTimeout: defaultNodeChannelOperationTimeout,
	}}
	command, err := NewNodeChannelCommandReadyCommand(
		testNodeChannelTarget(true), CommandReadyControlRequest{Proof: testNodeChannelCommandProof()},
	)
	if err != nil {
		t.Fatal(err)
	}
	executor.commandErr = errors.New(strings.Repeat("x", NodeChannelMaxError+100))
	result := agent.execute(t.Context(), command)
	if err := result.ValidateFor(command); err != nil {
		t.Fatal(err)
	}
	if result.ErrorClass != NodeChannelErrorInternal || len(result.Error) > NodeChannelMaxError {
		t.Fatalf("bounded result = class %q, bytes %d", result.ErrorClass, len(result.Error))
	}
}

func TestNewNodeChannelAgentRejectsAmbientOrIncompleteIdentity(t *testing.T) {
	directory := t.TempDir()
	config := NodeChannelAgentConfig{
		BaseURL: "https://region.internal:8421", CAFile: filepath.Join(directory, "ca.pem"),
		ClientCertFile: filepath.Join(directory, "client.pem"),
		ClientKeyFile:  filepath.Join(directory, "client-key.pem"), TokenFile: filepath.Join(directory, "token"),
		PeerURISAN: "spiffe://sandbox0.test/region/runtime-slot-channel",
		ClusterID:  "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1",
		NodeBootIDFile: filepath.Join(directory, "boot-id"), Executor: &testNodeChannelExecutor{},
	}
	if _, err := NewNodeChannelAgent(config); err != nil {
		t.Fatal(err)
	}
	config.BaseURL = "https://region.internal:8421/"
	if _, err := NewNodeChannelAgent(config); !errdefs.IsInvalidArgument(err) {
		t.Fatalf("non-origin URL error = %v", err)
	}
	config.BaseURL = "https://region.internal:8421"
	config.PeerURISAN = "https://region.internal"
	if _, err := NewNodeChannelAgent(config); !errdefs.IsInvalidArgument(err) {
		t.Fatalf("non-SPIFFE peer error = %v", err)
	}
}
