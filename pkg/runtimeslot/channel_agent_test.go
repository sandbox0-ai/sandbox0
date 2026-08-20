package runtimeslot

import (
	"context"
	"errors"
	"net"
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

type staticNodeChannelResolver struct {
	addresses []net.IPAddr
	err       error
	host      string
}

func (r *staticNodeChannelResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	r.host = host
	return r.addresses, r.err
}

func (e *testNodeChannelExecutor) PrepareNetwork(
	_ context.Context,
	_ NodeChannelTarget,
	request NodeNetworkPrepareControlRequest,
) (rootfshandoff.NetworkPolicyToken, error) {
	return rootfshandoff.NetworkPolicyToken{
		PodUID: request.AllocationID, PodSandboxID: RuntimeSlotNetworkIncarnationID(request),
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

func TestNodeChannelAgentSetResolvesCanonicalExactEndpoints(t *testing.T) {
	directory := t.TempDir()
	resolver := &staticNodeChannelResolver{addresses: []net.IPAddr{
		{IP: net.ParseIP("2001:db8::2")},
		{IP: net.ParseIP("192.0.2.2")},
		{IP: net.ParseIP("192.0.2.2")},
	}}
	set, err := NewNodeChannelAgentSet(NodeChannelAgentSetConfig{
		Agent: NodeChannelAgentConfig{
			BaseURL: "https://manager-nodes.sandbox0-system.svc:8421",
			CAFile:  filepath.Join(directory, "ca.pem"), ClientCertFile: filepath.Join(directory, "client.pem"),
			ClientKeyFile: filepath.Join(directory, "client-key.pem"), TokenFile: filepath.Join(directory, "token"),
			PeerURISAN: "spiffe://sandbox0.test/region/runtime-slot-channel",
			ClusterID:  "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1",
			NodeBootIDFile: filepath.Join(directory, "boot-id"), Executor: &testNodeChannelExecutor{},
		},
		Resolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	addresses, err := set.resolve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"192.0.2.2:8421", "[2001:db8::2]:8421"}
	if strings.Join(addresses, ",") != strings.Join(want, ",") ||
		resolver.host != "manager-nodes.sandbox0-system.svc" {
		t.Fatalf("resolved endpoints = %#v via %q, want %#v", addresses, resolver.host, want)
	}
}

func TestNodeChannelAgentSetRejectsVirtualOrUnboundedDiscovery(t *testing.T) {
	directory := t.TempDir()
	base := NodeChannelAgentSetConfig{Agent: NodeChannelAgentConfig{
		BaseURL: "https://manager-nodes.sandbox0-system.svc:8421",
		CAFile:  filepath.Join(directory, "ca.pem"), ClientCertFile: filepath.Join(directory, "client.pem"),
		ClientKeyFile: filepath.Join(directory, "client-key.pem"), TokenFile: filepath.Join(directory, "token"),
		PeerURISAN: "spiffe://sandbox0.test/region/runtime-slot-channel",
		ClusterID:  "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1",
		NodeBootIDFile: filepath.Join(directory, "boot-id"), Executor: &testNodeChannelExecutor{},
	}, MaxEndpoints: 1}
	base.Agent.BaseURL = "https://192.0.2.10:8421"
	if _, err := NewNodeChannelAgentSet(base); !errdefs.IsInvalidArgument(err) {
		t.Fatalf("IP authority error = %v", err)
	}
	base.Agent.BaseURL = "https://manager-nodes.sandbox0-system.svc:8421"
	base.Resolver = &staticNodeChannelResolver{addresses: []net.IPAddr{
		{IP: net.ParseIP("192.0.2.2")}, {IP: net.ParseIP("192.0.2.3")},
	}}
	set, err := NewNodeChannelAgentSet(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := set.resolve(t.Context()); !errdefs.IsInvalidArgument(err) {
		t.Fatalf("endpoint limit error = %v", err)
	}
}
