package runtimeslotnode

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const testNodeChannelServerURI = "spiffe://sandbox0.test/region/runtime-slot-channel"

type channelTestVerifier struct{}

type channelTestCapacityStore struct{}

func (channelTestCapacityStore) RegisterRuntimeNodeCapacity(
	_ context.Context,
	request *sandboxstore.RegisterRuntimeNodeCapacityRequest,
) (*sandboxstore.RuntimeNodeCapacity, error) {
	return &sandboxstore.RuntimeNodeCapacity{
		ClusterID: request.ClusterID, NodeID: request.NodeID, NodeUID: request.NodeUID,
		NodeBootID: request.NodeBootID, CPUMillicores: request.CPUMillicores,
		MemoryBytes: request.MemoryBytes, CPUSetCPUs: request.CPUSetCPUs,
		CPUSetMems: request.CPUSetMems, HeartbeatExpiresAt: time.Now().Add(request.TTL), Revision: 1,
	}, nil
}

func channelTestCapacity() protocol.NodeChannelCapacity {
	return protocol.NodeChannelCapacity{
		CPUMillicores: 4_000, MemoryBytes: 8 << 30,
		CPUSetCPUs: "0-3", CPUSetMems: "0", TTLMilliseconds: protocol.DefaultNodeChannelCapacityTTLMilliseconds,
	}
}

func (channelTestVerifier) Verify(_ context.Context, bearer string) (nodeauth.Identity, error) {
	if bearer != "Bearer node-token" {
		return nodeauth.Identity{}, errdefs.ErrPermissionDenied
	}
	return nodeauth.Identity{
		ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1", AgentUID: "agent-1",
	}, nil
}

type channelTestExecutor struct {
	mu            sync.Mutex
	cleanupErr    error
	calls         []protocol.NodeCleanupControlRequest
	networks      []protocol.NodeNetworkPrepareControlRequest
	claims        []protocol.NodeClaimControlRequest
	commands      []protocol.CommandReadyControlRequest
	forks         []protocol.NodeRunningForkControlRequest
	runningFork   rootfshandoff.RunningForkCheckpointResult
	rebases       []protocol.NodePausedRebaseControlRequest
	rebaseRejects []protocol.NodePausedRebaseControlRequest
	rebaseAcks    []protocol.NodePausedRebaseControlRequest
	pausedRebase  rootfsrebase.WorkerResult
	entered       chan<- struct{}
	release       <-chan struct{}
}

func (e *channelTestExecutor) PrepareNetwork(
	_ context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.NodeNetworkPrepareControlRequest,
) (rootfshandoff.NetworkPolicyToken, error) {
	e.mu.Lock()
	e.networks = append(e.networks, request)
	e.mu.Unlock()
	return rootfshandoff.NetworkPolicyToken{
		AllocationID: request.AllocationID, NetworkIncarnationID: protocol.RuntimeSlotNetworkIncarnationID(request),
		ClaimID: request.ClaimID, NetworkEpoch: 1, PolicyDigest: request.PolicyDigest,
		SourceIP: "192.0.2.2", CtldGeneration: "ctld-1", NetNSIdentity: request.NetNSIdentity,
	}, nil
}

func TestNodeChannelIdentityMustMatchAuthenticatedRoute(t *testing.T) {
	identity := nodeauth.Identity{ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1"}
	hello := protocol.NodeChannelHello{ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1"}
	if !nodeChannelIdentityMatches(identity, hello) {
		t.Fatal("exact authenticated route did not match")
	}
	for name, mutate := range map[string]func(*protocol.NodeChannelHello){
		"cluster": func(candidate *protocol.NodeChannelHello) { candidate.ClusterID = "other-cluster" },
		"node":    func(candidate *protocol.NodeChannelHello) { candidate.NodeID = "other-node" },
		"uid":     func(candidate *protocol.NodeChannelHello) { candidate.NodeUID = "other-uid" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := hello
			mutate(&candidate)
			if nodeChannelIdentityMatches(identity, candidate) {
				t.Fatal("self-asserted route replaced authenticated identity")
			}
		})
	}
}

func TestNodeChannelHubWaitsForAuthenticatedReconnect(t *testing.T) {
	hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	server, files := newNodeChannelTLSServer(t, hub)
	defer server.Close()
	target := runtimeslotclaim.NodeTarget{
		SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
		NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		ControlEndpoint: "unix:///var/run/sandbox0/nomad-slots/task.sock",
	}
	type outcome struct {
		response protocol.NodeControlResponse
		err      error
	}
	result := make(chan outcome, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() {
		response, err := hub.Claim(ctx, target, testChannelClaimRequest())
		result <- outcome{response: response, err: err}
	}()
	select {
	case early := <-result:
		t.Fatalf("claim returned before reconnect: %+v", early)
	case <-time.After(20 * time.Millisecond):
	}
	executor := &channelTestExecutor{}
	agent, err := protocol.NewNodeChannelAgent(protocol.NodeChannelAgentConfig{
		BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
		ClientKeyFile: files.clientKey, TokenFile: files.token,
		PeerURISAN: testNodeChannelServerURI, NodeUID: "node-uid-1", NodeBootIDFile: files.boot,
		ClusterID: "cluster-1", NodeID: "node-1", Executor: executor,
		Capacity:     channelTestCapacity(),
		ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond,
		AgentInstanceID: "agent-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	agentCtx, cancelAgent := context.WithCancel(t.Context())
	agentDone := make(chan error, 1)
	go func() { agentDone <- agent.Run(agentCtx) }()
	waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-1")
	select {
	case completed := <-result:
		if completed.err != nil || completed.response.Phase != string(protocol.StateActive) {
			t.Fatalf("claim after reconnect = %+v, %v", completed.response, completed.err)
		}
	case <-time.After(time.Second):
		t.Fatal("claim did not resume after reconnect")
	}
	cancelAgent()
	if err := <-agentDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("agent stop = %v", err)
	}
}

func TestNodeChannelHubReconnectWaitHonorsContext(t *testing.T) {
	hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err = hub.Claim(ctx, runtimeslotclaim.NodeTarget{
		SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
		NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		ControlEndpoint: "unix:///var/run/sandbox0/nomad-slots/task.sock",
	}, testChannelClaimRequest())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("claim error = %v, want deadline exceeded", err)
	}
}

func TestNodeChannelHubRoutesOnlyCleanupThroughAuthenticatedSuccessorBoot(t *testing.T) {
	hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	server, files := newNodeChannelTLSServer(t, hub)
	defer server.Close()
	if err := os.WriteFile(files.boot, []byte("boot-2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	executor := &channelTestExecutor{}
	agent, err := protocol.NewNodeChannelAgent(protocol.NodeChannelAgentConfig{
		BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
		ClientKeyFile: files.clientKey, TokenFile: files.token,
		PeerURISAN: testNodeChannelServerURI, NodeUID: "node-uid-1", NodeBootIDFile: files.boot,
		ClusterID: "cluster-1", NodeID: "node-1", Executor: executor,
		Capacity:     channelTestCapacity(),
		ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond,
		AgentInstanceID: "agent-boot-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	agentDone := make(chan error, 1)
	go func() { agentDone <- agent.Run(ctx) }()
	waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-2")

	cleanup := testChannelCleanupRequest()
	resources, err := protocol.NewRuntimeResourceLease(
		"cleanup-reboot-operation", "cleanup-reboot-claim", cleanup.SlotID,
		cleanup.ClusterID, cleanup.NodeID, cleanup.NodeUID, cleanup.NodeBootID,
		protocol.RuntimeResourceRequest{
			Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1_500,
			MemoryBytes: 768 << 20, PIDsLimit: protocol.DefaultRuntimePIDsLimit,
		},
		"0-3", "0",
	)
	if err != nil {
		t.Fatal(err)
	}
	resourceDigest, err := resources.Digest()
	if err != nil {
		t.Fatal(err)
	}
	cleanup.Resources = resources
	cleanup.ResourceLeaseDigest = strings.TrimPrefix(resourceDigest, "sha256:")
	proof, err := hub.CleanupRuntimeSlot(t.Context(), Target{
		ClusterID: cleanup.ClusterID, NodeID: cleanup.NodeID,
		NodeUID: cleanup.NodeUID, NodeBootID: cleanup.NodeBootID,
	}, cleanup)
	if err != nil {
		t.Fatal(err)
	}
	if err := proof.Validate(); err != nil || proof.Request() != cleanup || !proof.ResourceCgroupAbsent {
		t.Fatalf("successor-boot cleanup proof = %+v, %v", proof, err)
	}
	executor.mu.Lock()
	if len(executor.calls) != 1 || executor.calls[0] != cleanup {
		t.Fatalf("successor-boot cleanup calls = %+v", executor.calls)
	}
	executor.mu.Unlock()

	claimCtx, cancelClaim := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancelClaim()
	_, err = hub.Claim(claimCtx, runtimeslotclaim.NodeTarget{
		SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
		NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		ControlEndpoint: "unix:///var/run/sandbox0/nomad-slots/task.sock",
	}, testChannelClaimRequest())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cross-boot claim error = %v, want deadline exceeded", err)
	}
	executor.mu.Lock()
	if len(executor.claims) != 0 {
		t.Fatalf("successor boot received a claim for the old boot: %+v", executor.claims)
	}
	executor.mu.Unlock()

	boot3File := filepath.Join(filepath.Dir(files.boot), "boot-id-3")
	if err := os.WriteFile(boot3File, []byte("boot-3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	agent3, err := protocol.NewNodeChannelAgent(protocol.NodeChannelAgentConfig{
		BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
		ClientKeyFile: files.clientKey, TokenFile: files.token,
		PeerURISAN: testNodeChannelServerURI, NodeUID: "node-uid-1", NodeBootIDFile: boot3File,
		ClusterID: "cluster-1", NodeID: "node-1", Executor: &channelTestExecutor{},
		Capacity:     channelTestCapacity(),
		ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond,
		AgentInstanceID: "agent-boot-3",
	})
	if err != nil {
		t.Fatal(err)
	}
	agent3Ctx, cancelAgent3 := context.WithCancel(t.Context())
	agent3Done := make(chan error, 1)
	go func() { agent3Done <- agent3.Run(agent3Ctx) }()
	waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-3")
	_, err = hub.CleanupRuntimeSlot(t.Context(), Target{
		ClusterID: cleanup.ClusterID, NodeID: cleanup.NodeID,
		NodeUID: cleanup.NodeUID, NodeBootID: cleanup.NodeBootID,
	}, cleanup)
	if !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("ambiguous successor-boot cleanup error = %v", err)
	}
	cancelAgent3()
	if err := <-agent3Done; !errors.Is(err, context.Canceled) {
		t.Fatalf("successor agent Run() error = %v", err)
	}

	cancel()
	if err := <-agentDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("agent Run() error = %v", err)
	}
}

func (e *channelTestExecutor) Claim(
	_ context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.NodeClaimControlRequest,
) (protocol.NodeControlResponse, error) {
	e.mu.Lock()
	e.claims = append(e.claims, request)
	e.mu.Unlock()
	return protocol.NodeControlResponse{Phase: string(protocol.StateActive)}, nil
}

func (e *channelTestExecutor) CommandReady(
	_ context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.CommandReadyControlRequest,
) (protocol.NodeControlResponse, error) {
	e.mu.Lock()
	e.commands = append(e.commands, request)
	e.mu.Unlock()
	return protocol.NodeControlResponse{Phase: string(protocol.StateActive)}, nil
}

func (e *channelTestExecutor) RunningFork(
	_ context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.NodeRunningForkControlRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	e.mu.Lock()
	e.forks = append(e.forks, request)
	checkpoint := e.runningFork
	e.mu.Unlock()
	return checkpoint, nil
}

func (e *channelTestExecutor) PausedRebase(
	_ context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerResult, error) {
	e.mu.Lock()
	e.rebases = append(e.rebases, request)
	result := e.pausedRebase
	e.mu.Unlock()
	return result, nil
}

func (e *channelTestExecutor) RejectPausedRebase(
	_ context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerRejection, error) {
	e.mu.Lock()
	e.rebaseRejects = append(e.rebaseRejects, request)
	e.mu.Unlock()
	return rootfsrebase.RejectWithoutResult(request.Worker)
}

func (e *channelTestExecutor) AcknowledgePausedRebase(
	_ context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) error {
	e.mu.Lock()
	e.rebaseAcks = append(e.rebaseAcks, request)
	e.mu.Unlock()
	return nil
}

func (e *channelTestExecutor) Cleanup(
	_ context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.NodeCleanupControlRequest,
) (protocol.NodeCleanupControlProof, error) {
	e.mu.Lock()
	e.calls = append(e.calls, request)
	cleanupErr := e.cleanupErr
	entered := e.entered
	release := e.release
	e.mu.Unlock()
	if entered != nil {
		entered <- struct{}{}
	}
	if release != nil {
		<-release
	}
	if cleanupErr != nil {
		return protocol.NodeCleanupControlProof{}, cleanupErr
	}
	proof := protocol.NodeCleanupControlProof{
		Version:     protocol.NodeCleanupProofVersion,
		OperationID: request.OperationID, SlotID: request.SlotID, ClusterID: request.ClusterID,
		AllocationID: request.AllocationID, NodeID: request.NodeID, NodeUID: request.NodeUID,
		NodeBootID: request.NodeBootID, NetNSIdentity: request.NetNSIdentity,
		RunscContainerID: request.RunscContainerID,
		Resources:        request.Resources, ResourceLeaseID: request.Resources.LeaseID,
		ResourceLeaseDigest: request.ResourceLeaseDigest,
		RunscAbsent:         true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true,
		ResourceCgroupAbsent: !request.Resources.IsZero(),
	}
	digest, err := proof.Digest()
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	proof.ProofDigest = digest
	return proof, nil
}

func TestNodeChannelHubRoutesCleanupOverAuthenticatedOutboundStream(t *testing.T) {
	hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	server, files := newNodeChannelTLSServer(t, hub)
	defer server.Close()
	forkRequest, forkCheckpoint := testChannelRunningFork(t)
	rebaseRequest, rebaseResult := testChannelPausedRebase(t)
	executor := &channelTestExecutor{runningFork: forkCheckpoint, pausedRebase: rebaseResult}
	agent, err := protocol.NewNodeChannelAgent(protocol.NodeChannelAgentConfig{
		BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
		ClientKeyFile: files.clientKey, TokenFile: files.token,
		PeerURISAN: testNodeChannelServerURI, NodeUID: "node-uid-1", NodeBootIDFile: files.boot,
		ClusterID: "cluster-1", NodeID: "node-1",
		Executor: executor, RunningForkExecutor: executor,
		PausedRebaseExecutor: executor, NetworkExecutor: executor,
		Capacity:     channelTestCapacity(),
		ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond,
		AgentInstanceID: "agent-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	agentDone := make(chan error, 1)
	go func() { agentDone <- agent.Run(ctx) }()
	waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-1")
	selected, err := hub.SelectPausedRebaseNode("cluster-1", "rebase-selection-operation")
	if err != nil || selected.NodeID != "node-1" || selected.NodeUID != "node-uid-1" || selected.NodeBootID != "boot-1" {
		t.Fatalf("selected paused-rebase worker = %+v, %v", selected, err)
	}
	resolved, err := hub.ResolvePausedRebaseNode("cluster-1", selected.NodeID, selected.NodeUID)
	if err != nil || resolved != selected {
		t.Fatalf("resolved paused-rebase worker = %+v, %v", resolved, err)
	}
	standbyExecutor := &channelTestExecutor{}
	standbyAgent, err := protocol.NewNodeChannelAgent(protocol.NodeChannelAgentConfig{
		BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
		ClientKeyFile: files.clientKey, TokenFile: files.token,
		PeerURISAN: testNodeChannelServerURI, NodeUID: "node-uid-1", NodeBootIDFile: files.boot,
		ClusterID: "cluster-1", NodeID: "node-1",
		Executor: standbyExecutor, NetworkExecutor: standbyExecutor,
		Capacity:     channelTestCapacity(),
		ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond,
		AgentInstanceID: "agent-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	standbyCtx, cancelStandby := context.WithCancel(t.Context())
	standbyDone := make(chan error, 1)
	go func() { standbyDone <- standbyAgent.Run(standbyCtx) }()
	time.Sleep(20 * time.Millisecond)
	nodeTarget := runtimeslotclaim.NodeTarget{
		SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
		NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		ControlEndpoint: "unix:///var/run/sandbox0/nomad-slots/task.sock",
	}
	networkRequest := runtimeslotclaim.NetworkPrepareRequest{
		OperationID: "operation-1", ClaimID: "claim-1", SlotID: "slot-1",
		ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1", NetNSIdentity: "1:2",
		NetworkPolicy: `{"mode":"block-all"}`,
	}
	networkRequest.PolicyDigest = protocol.NetworkPolicyDigest(networkRequest.NetworkPolicy)
	policyToken, err := hub.Prepare(t.Context(), networkRequest)
	if err != nil || policyToken.PolicyDigest != networkRequest.PolicyDigest || policyToken.SourceIP != "192.0.2.2" {
		t.Fatalf("network prepare token = %+v, %v", policyToken, err)
	}
	claimRequest := testChannelClaimRequest()
	claimResponse, err := hub.Claim(t.Context(), nodeTarget, claimRequest)
	if err != nil || claimResponse.Phase != string(protocol.StateActive) {
		t.Fatalf("node claim response = %+v, %v", claimResponse, err)
	}
	commandRequest := protocol.CommandReadyControlRequest{Proof: testChannelCommandReadyProof()}
	commandResponse, err := hub.CommandReady(t.Context(), nodeTarget, commandRequest)
	if err != nil || commandResponse.Phase != string(protocol.StateActive) {
		t.Fatalf("node command-ready response = %+v, %v", commandResponse, err)
	}
	forkResult, err := hub.RunningFork(t.Context(), protocol.NodeChannelTarget{
		SlotID: nodeTarget.SlotID, ClusterID: nodeTarget.ClusterID, AllocationID: nodeTarget.AllocationID,
		NodeID: nodeTarget.NodeID, NodeUID: nodeTarget.NodeUID, NodeBootID: nodeTarget.NodeBootID,
	}, forkRequest)
	if err != nil || forkResult.ProofDigest != forkCheckpoint.ProofDigest {
		t.Fatalf("node running-fork result = %+v, %v", forkResult, err)
	}
	pausedRebase, err := hub.PausedRebase(t.Context(), protocol.NodeChannelTarget{
		ClusterID: nodeTarget.ClusterID, NodeID: nodeTarget.NodeID,
		NodeUID: nodeTarget.NodeUID, NodeBootID: nodeTarget.NodeBootID,
	}, rebaseRequest)
	if err != nil || pausedRebase.ProofDigest != rebaseResult.ProofDigest {
		t.Fatalf("node paused-rebase result = %+v, %v", pausedRebase, err)
	}
	rejectRequest := rebaseRequest
	rejectRequest.Reject = true
	rejected, err := hub.RejectPausedRebase(t.Context(), protocol.NodeChannelTarget{
		ClusterID: nodeTarget.ClusterID, NodeID: nodeTarget.NodeID,
		NodeUID: nodeTarget.NodeUID, NodeBootID: nodeTarget.NodeBootID,
	}, rejectRequest)
	if err != nil || rejected.ValidateFor(rebaseRequest.Worker) != nil || rejected.Result != nil {
		t.Fatalf("node paused-rebase rejection = %+v, %v", rejected, err)
	}
	ackRequest := rebaseRequest
	ackRequest.AcknowledgeProofDigest = rebaseResult.ProofDigest
	acknowledged, err := hub.AcknowledgePausedRebase(t.Context(), protocol.NodeChannelTarget{
		ClusterID: nodeTarget.ClusterID, NodeID: nodeTarget.NodeID,
		NodeUID: nodeTarget.NodeUID, NodeBootID: nodeTarget.NodeBootID,
	}, ackRequest)
	if err != nil || acknowledged.ProofDigest != rebaseResult.ProofDigest {
		t.Fatalf("node paused-rebase acknowledgement = %+v, %v", acknowledged, err)
	}
	executor.mu.Lock()
	if len(executor.networks) != 1 || executor.networks[0] != (protocol.NodeNetworkPrepareControlRequest{
		OperationID: networkRequest.OperationID, ClaimID: networkRequest.ClaimID,
		SlotID: networkRequest.SlotID, ClusterID: networkRequest.ClusterID,
		AllocationID: networkRequest.AllocationID, NodeID: networkRequest.NodeID,
		NodeUID: networkRequest.NodeUID, NodeBootID: networkRequest.NodeBootID,
		NetNSIdentity: networkRequest.NetNSIdentity, NetworkPolicy: networkRequest.NetworkPolicy,
		PolicyDigest: networkRequest.PolicyDigest,
	}) || len(executor.claims) != 1 || executor.claims[0].PolicyToken != claimRequest.PolicyToken ||
		len(executor.commands) != 1 || executor.commands[0] != commandRequest ||
		len(executor.forks) != 1 || executor.forks[0] != forkRequest ||
		len(executor.rebases) != 1 || len(executor.rebaseRejects) != 1 || len(executor.rebaseAcks) != 1 {
		t.Fatalf("node control calls = claims %d, commands %d, forks %d, rebases %d, rejects %d",
			len(executor.claims), len(executor.commands), len(executor.forks),
			len(executor.rebases), len(executor.rebaseRejects))
	}
	executor.mu.Unlock()

	request := testChannelCleanupRequest()
	proof, err := hub.CleanupRuntimeSlot(t.Context(), Target{
		ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
	}, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := proof.Validate(); err != nil || proof.Request() != request {
		t.Fatalf("cleanup proof = %+v, %v", proof, err)
	}
	executor.mu.Lock()
	if len(executor.calls) != 1 || executor.calls[0] != request {
		t.Fatalf("cleanup calls = %+v", executor.calls)
	}
	executor.mu.Unlock()
	standbyExecutor.mu.Lock()
	if len(standbyExecutor.calls) != 0 {
		t.Fatalf("standby agent preempted the live owner: %+v", standbyExecutor.calls)
	}
	standbyExecutor.mu.Unlock()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	executor.mu.Lock()
	executor.entered = entered
	executor.release = release
	executor.mu.Unlock()
	concurrentRequests := []protocol.NodeCleanupControlRequest{request, request}
	concurrentRequests[0].OperationID = "cleanup-2"
	concurrentRequests[0].SlotID = "slot-2"
	concurrentRequests[0].AllocationID = "allocation-2"
	concurrentRequests[0].RunscContainerID = protocol.NomadRunscContainerID("slot-2")
	concurrentRequests[1].OperationID = "cleanup-3"
	concurrentRequests[1].SlotID = "slot-3"
	concurrentRequests[1].AllocationID = "allocation-3"
	concurrentRequests[1].RunscContainerID = protocol.NomadRunscContainerID("slot-3")
	concurrentResults := make(chan error, len(concurrentRequests))
	for _, concurrentRequest := range concurrentRequests {
		go func(request protocol.NodeCleanupControlRequest) {
			_, err := hub.CleanupRuntimeSlot(t.Context(), Target{
				ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
			}, request)
			concurrentResults <- err
		}(concurrentRequest)
	}
	for range concurrentRequests {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("independent node commands were serialized")
		}
	}
	close(release)
	for range concurrentRequests {
		if err := <-concurrentResults; err != nil {
			t.Fatalf("concurrent cleanup error = %v", err)
		}
	}
	executor.mu.Lock()
	executor.entered = nil
	executor.release = nil
	executor.mu.Unlock()
	timeoutEntered := make(chan struct{}, 1)
	timeoutRelease := make(chan struct{})
	executor.mu.Lock()
	executor.entered = timeoutEntered
	executor.release = timeoutRelease
	executor.mu.Unlock()
	timeoutRequest := request
	timeoutRequest.OperationID = "cleanup-timeout"
	timeoutRequest.SlotID = "slot-timeout"
	timeoutRequest.AllocationID = "allocation-timeout"
	timeoutRequest.RunscContainerID = protocol.NomadRunscContainerID(timeoutRequest.SlotID)
	timeoutCtx, cancelTimeout := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancelTimeout()
	timeoutResult := make(chan error, 1)
	go func() {
		_, timeoutErr := hub.CleanupRuntimeSlot(timeoutCtx, Target{
			ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		}, timeoutRequest)
		timeoutResult <- timeoutErr
	}()
	select {
	case <-timeoutEntered:
	case <-time.After(time.Second):
		t.Fatal("timed-out cleanup did not reach the node")
	}
	if err := <-timeoutResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out cleanup error = %v", err)
	}
	retryResult := make(chan error, 1)
	go func() {
		_, retryErr := hub.CleanupRuntimeSlot(t.Context(), Target{
			ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		}, timeoutRequest)
		retryResult <- retryErr
	}()
	time.Sleep(10 * time.Millisecond)
	close(timeoutRelease)
	if err := <-retryResult; err != nil {
		t.Fatalf("retry after caller timeout = %v", err)
	}
	executor.mu.Lock()
	timeoutCalls := 0
	for _, call := range executor.calls {
		if call.OperationID == timeoutRequest.OperationID {
			timeoutCalls++
		}
	}
	if timeoutCalls != 1 {
		t.Fatalf("timed-out exact command executed %d times", timeoutCalls)
	}
	executor.entered = nil
	executor.release = nil
	executor.cleanupErr = errdefs.ErrFailedPrecondition
	executor.mu.Unlock()
	_, err = hub.CleanupRuntimeSlot(t.Context(), Target{
		ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
	}, request)
	if !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("remote classified error = %v", err)
	}
	_, err = hub.CleanupRuntimeSlot(t.Context(), Target{
		ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "other-boot",
	}, request)
	if !errdefs.IsInvalidArgument(err) {
		t.Fatalf("cross-boot target error = %v", err)
	}

	cancelStandby()
	if err := <-standbyDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("standby agent Run() error = %v", err)
	}
	cancel()
	if err := <-agentDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("agent Run() error = %v", err)
	}
}

func waitNodeChannelConnected(t *testing.T, hub *ChannelHub, clusterID, nodeID, nodeUID, bootID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !hub.Connected(clusterID, nodeID, nodeUID, bootID) {
		if time.Now().After(deadline) {
			t.Fatal("node channel did not connect")
		}
		time.Sleep(time.Millisecond)
	}
}

func testChannelCleanupRequest() protocol.NodeCleanupControlRequest {
	return protocol.NodeCleanupControlRequest{
		OperationID: "cleanup-1", SlotID: "slot-1", ClusterID: "cluster-1",
		AllocationID: "allocation-1", NodeID: "node-1", NodeUID: "node-uid-1",
		NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
		RunscContainerID: protocol.NomadRunscContainerID("slot-1"),
	}
}

func testChannelClaimRequest() protocol.NodeClaimControlRequest {
	networkPolicy := `{"mode":"block-all"}`
	token := strings.Repeat("writer-token-", 4)
	assignment := &runtimecontrol.Assignment{
		SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 1, SecurityClass: "standard",
		EnvVars: map[string]string{runtimecontrol.EnvSandboxID: "sandbox-1"},
	}
	revision, _ := assignment.Revision()
	resources, err := protocol.NewRuntimeResourceLease(
		"operation-1", "claim-1", "slot-1", "cluster-1", "node-1", "node-uid-1", "boot-1",
		protocol.RuntimeResourceRequest{
			Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1_000,
			MemoryBytes: 1 << 30, PIDsLimit: protocol.DefaultRuntimePIDsLimit,
		},
		"0-3", "0",
	)
	if err != nil {
		panic(err)
	}
	resourceDigest, err := resources.Digest()
	if err != nil {
		panic(err)
	}
	stage := &rootfshandoff.StageRequest{
		BindingVersion: rootfshandoff.WriterBindingVersion,
		Parent:         "sha256:" + strings.Repeat("a", 64), InitialGeneration: "generation-1",
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{
			AllocationID: "allocation-1", NetworkIncarnationID: "allocation-network-1", ClaimID: "claim-1",
			NetworkEpoch: 4, PolicyDigest: protocol.NetworkPolicyDigest(networkPolicy), SourceIP: "192.0.2.2",
			CtldGeneration: "ctld-1", NetNSIdentity: "1:2",
		},
		Labels: map[string]string{
			protocol.RuntimeAssignmentRevisionLabel:  revision,
			protocol.RuntimeResourceLeaseDigestLabel: resourceDigest,
		},
		Identity: rootfshandoff.Identity{
			NodeUID: "node-uid-1", BootID: "boot-1", RuntimeGeneration: "1",
			AllocationID: "allocation-1", NetworkIncarnationID: "allocation-network-1", TaskName: protocol.NomadTaskName,
			SourceOCIDigest: "procd-image-1", RootFSDriver: "nomad-driver", RuntimeClass: "sandbox0-gvisor",
			SlotNonce: "slot-1", ClaimID: "claim-1", LaunchAttempt: "attempt-1",
			RootFSID: "filesystem-1", WriterEpoch: 4, WriterGrantID: "grant-1",
			WriterGrantToken: token, WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest(token),
		},
	}
	return protocol.NodeClaimControlRequest{
		OperationID: "operation-1", ClaimID: stage.Identity.ClaimID, PolicyToken: token,
		WriterEpoch: strconv.FormatInt(stage.Identity.WriterEpoch, 10), Stage: stage,
		NetworkPolicy: networkPolicy, Runtime: assignment, Resources: resources,
	}
}

func testChannelRunningFork(t *testing.T) (
	protocol.NodeRunningForkControlRequest,
	rootfshandoff.RunningForkCheckpointResult,
) {
	t.Helper()
	stage := testChannelClaimRequest().Stage
	binding, err := stage.BindingDigest()
	if err != nil {
		t.Fatal(err)
	}
	fork := rootfshandoff.RunningForkCheckpointRequest{
		OperationID: "fork-operation-1", SourceSandboxID: "sandbox-1",
		TargetSandboxID: "sandbox-fork-1", TargetGenerationID: "generation-fork-1",
	}
	request := protocol.NodeRunningForkControlRequest{
		Fork: fork, SourceFilesystemID: stage.Identity.RootFSID,
		SourceWriterGrantID: stage.Identity.WriterGrantID, SourceWriterEpoch: stage.Identity.WriterEpoch,
		BindingVersion: stage.BindingVersion, BindingDigest: hex.EncodeToString(binding[:]),
		ExpectedSourceGenerationID: stage.InitialGeneration,
	}
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
	proof := rootfshandoff.RunningForkCheckpointProof{
		Version: rootfshandoff.RunningForkCheckpointVersion, OperationID: fork.OperationID,
		SourceSandboxID: fork.SourceSandboxID, SourceFilesystemID: request.SourceFilesystemID,
		TargetSandboxID: fork.TargetSandboxID, SourceWriterGrantID: request.SourceWriterGrantID,
		SourceWriterEpoch: request.SourceWriterEpoch, BindingVersion: request.BindingVersion,
		BindingDigest: request.BindingDigest, ExpectedSourceGenerationID: request.ExpectedSourceGenerationID,
		CheckpointGenerationID: fork.TargetGenerationID, CheckpointSequence: 1,
		CheckpointDescriptorDigest: digest.FromBytes(descriptor).String(),
	}
	proofDigest, err := proof.Digest()
	if err != nil {
		t.Fatal(err)
	}
	result := rootfshandoff.RunningForkCheckpointResult{
		Generation: rootfshandoff.GenerationDescriptor{
			Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: fork.TargetGenerationID,
			FilesystemID: fork.TargetSandboxID, SourceOCIDigest: digest.FromString("source-image").String(),
			BaseArtifactDigest: digest.FromString("base-artifact").String(), BaseBlockRoot: root,
			CurrentBlockHead: root, WriterEpoch: request.SourceWriterEpoch, FormatGeneration: 1,
			DurabilityState: rootfsblock.DurabilityS3, LocatorVersion: 2, Descriptor: descriptor,
		},
		Proof: proof, ProofDigest: hex.EncodeToString(proofDigest[:]),
	}
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
	return request, result
}

func testChannelPausedRebase(t *testing.T) (
	protocol.NodePausedRebaseControlRequest,
	rootfsrebase.WorkerResult,
) {
	t.Helper()
	sourceBaseRoot := digest.FromString("hub-rebase-source-base").String()
	sourceHead := digest.FromString("hub-rebase-source-head").String()
	targetBaseRoot := digest.FromString("hub-rebase-target-base").String()
	descriptor := func(name, root string) []byte {
		payload, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
			Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 8 * rootfsblock.LogicalBlockSize,
			BlockSizeBytes: rootfsblock.LogicalBlockSize,
			MappingRoot: rootfsblock.MappingRootLocator{
				Version: rootfsblock.MappingPageVersion, RootDigest: root,
				Object: rootfsblock.ObjectRange{
					Key: "rootfs/hub-rebase/" + name + "/map", Length: 4096,
					Checksum: digest.FromString("hub-rebase-map-" + name).String(),
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	worker := rootfsrebase.WorkerRequest{
		Version: rootfsrebase.WorkerProtocolVersion, OperationID: "hub-rebase-operation",
		SandboxID: "sandbox-rebase", TeamID: "team-rebase", FilesystemID: "filesystem-rebase",
		SourceGenerationID: "generation-source", SourceOCIDigest: digest.FromString("source-oci").String(),
		SourceBaseArtifactDigest: digest.FromString("source-artifact").String(),
		SourceBaseBlockRoot:      sourceBaseRoot, SourceCurrentBlockHead: sourceHead,
		SourceFormatGeneration: 1, SourceLocatorVersion: 2,
		SourceBaseDescriptor:       descriptor("source-base", sourceBaseRoot),
		SourceGenerationDescriptor: descriptor("source", sourceHead),
		TargetGenerationID:         "generation-target", TargetSourceOCIDigest: digest.FromString("target-oci").String(),
		TargetBaseArtifactDigest: digest.FromString("target-artifact").String(),
		TargetBaseBlockRoot:      targetBaseRoot, TargetFormatGeneration: 1, TargetWriterEpoch: 3,
		TargetBaseDescriptor: descriptor("target-base", targetBaseRoot),
		RollbackExpiresAt:    time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
		MaxChangedBlocks:     1024,
	}
	requestDigest, err := worker.Digest()
	if err != nil {
		t.Fatal(err)
	}
	hexDigest := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:])
	}
	health := sha256.Sum256([]byte("hub-rebase-health"))
	apply := rootfsrebase.ApplyResult{
		Version: rootfsrebase.ApplyResultVersion, AppliedChanges: 1, TargetNodeCount: 1,
		OldManifestDigest: hexDigest("old"), SourceManifestDigest: hexDigest("source"),
		DiffDigest: hexDigest("diff"), TargetManifestDigest: hexDigest("target"),
		HealthProof: hex.EncodeToString(health[:]),
	}
	result := rootfsrebase.WorkerResult{
		Version: rootfsrebase.WorkerProtocolVersion, RequestDigest: requestDigest,
		GenerationID: worker.TargetGenerationID, FilesystemID: worker.FilesystemID,
		ParentGenerationID: worker.SourceGenerationID, SourceOCIDigest: worker.TargetSourceOCIDigest,
		BaseArtifactDigest: worker.TargetBaseArtifactDigest, BaseBlockRoot: worker.TargetBaseBlockRoot,
		CurrentBlockHead: worker.TargetBaseBlockRoot, WriterEpoch: worker.TargetWriterEpoch,
		FormatGeneration: worker.TargetFormatGeneration, DurabilityState: rootfsblock.DurabilityS3,
		LocatorVersion: worker.SourceLocatorVersion + 1, Descriptor: append([]byte(nil), worker.TargetBaseDescriptor...),
		HealthCheckDigest: health[:], DirtyBlocks: 1, Apply: apply,
	}
	if err := result.SealProof(); err != nil {
		t.Fatal(err)
	}
	return protocol.NodePausedRebaseControlRequest{Worker: worker}, result
}

func testChannelCommandReadyProof() protocol.CommandReadyProof {
	return protocol.CommandReadyProof{
		Version: protocol.CommandReadyProofVersion, SlotID: "slot-1", OperationID: "operation-1",
		ClaimID: "claim-1", LaunchAttempt: "attempt-1",
		RunscContainerID: protocol.NomadRunscContainerID("slot-1"), ProcdInstanceID: "procd-1",
		ProcdAddress: "http://192.0.2.2:49983", RequestMethod: "PUT",
		RequestPath: protocol.ProcdCommandReadyProbePath, ResponseStatus: 200,
		ResponseBodyDigest: strings.Repeat("4", 64),
	}
}

type nodeChannelTLSFiles struct {
	ca, clientCert, clientKey, token, boot string
}

func newNodeChannelTLSServer(t *testing.T, handler *ChannelHub) (*httptest.Server, nodeChannelTLSFiles) {
	t.Helper()
	directory := t.TempDir()
	ca, caKey, caPEM := newNodeChannelTestCA(t)
	serverCertificate := newNodeChannelTestCertificate(t, ca, caKey, nodeChannelCertificateRequest{
		commonName: "127.0.0.1", server: true, uris: []string{testNodeChannelServerURI},
	})
	clientPEM, clientKeyPEM := newNodeChannelTestCertificatePEM(t, ca, caKey, nodeChannelCertificateRequest{
		commonName: "node-agent", client: true, uris: []string{"spiffe://sandbox0.test/node/node-uid-1"},
	})
	files := nodeChannelTLSFiles{
		ca: filepath.Join(directory, "ca.pem"), clientCert: filepath.Join(directory, "client.pem"),
		clientKey: filepath.Join(directory, "client-key.pem"), token: filepath.Join(directory, "token"),
		boot: filepath.Join(directory, "boot-id"),
	}
	for path, payload := range map[string][]byte{
		files.ca: caPEM, files.clientCert: clientPEM, files.clientKey: clientKeyPEM,
		files.token: []byte("node-token\n"), files.boot: []byte("boot-1\n"),
	} {
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	clientRoots := x509.NewCertPool()
	if !clientRoots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append client CA")
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCertificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientRoots,
	}
	server.StartTLS()
	return server, files
}

type nodeChannelCertificateRequest struct {
	commonName string
	server     bool
	client     bool
	uris       []string
	dnsNames   []string
}

func newNodeChannelTestCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey, []byte) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "node-channel-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(raw)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, privateKey, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw})
}

func newNodeChannelTestCertificate(
	t *testing.T,
	ca *x509.Certificate,
	caKey ed25519.PrivateKey,
	request nodeChannelCertificateRequest,
) tls.Certificate {
	t.Helper()
	certificatePEM, keyPEM := newNodeChannelTestCertificatePEM(t, ca, caKey, request)
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func newNodeChannelTestCertificatePEM(
	t *testing.T,
	ca *x509.Certificate,
	caKey ed25519.PrivateKey,
	request nodeChannelCertificateRequest,
) ([]byte, []byte) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: request.commonName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	if request.server {
		template.ExtKeyUsage = append(template.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
		if address := net.ParseIP(request.commonName); address != nil {
			template.IPAddresses = []net.IP{address}
		}
		template.DNSNames = append(template.DNSNames, request.dnsNames...)
	}
	if request.client {
		template.ExtKeyUsage = append(template.ExtKeyUsage, x509.ExtKeyUsageClientAuth)
	}
	for _, value := range request.uris {
		identity, err := url.Parse(value)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = append(template.URIs, identity)
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, ca, publicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	privateRaw, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateRaw})
}
