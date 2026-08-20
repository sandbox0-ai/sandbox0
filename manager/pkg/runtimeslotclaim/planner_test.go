package runtimeslotclaim

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type fakeStore struct {
	mu sync.Mutex

	filesystem *sandboxstore.RootFSFilesystem
	generation *sandboxstore.RootFSGeneration
	slot       *sandboxstore.RuntimeSlot
	grant      *sandboxstore.RootFSWriterGrant

	acquires  []*sandboxstore.AcquireRuntimeSlotRequest
	issues    []*sandboxstore.IssueRootFSWriterGrantRequest
	binds     []*sandboxstore.BindRuntimeSlotWriterGrantRequest
	issueLost bool
}

func (f *fakeStore) GetRootFSFilesystem(context.Context, string) (*sandboxstore.RootFSFilesystem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	clone := *f.filesystem
	return &clone, nil
}

func (f *fakeStore) GetRootFSGeneration(context.Context, string) (*sandboxstore.RootFSGeneration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	clone := *f.generation
	clone.Descriptor = append([]byte(nil), f.generation.Descriptor...)
	return &clone, nil
}

func (f *fakeStore) GetRootFSWriterGrant(_ context.Context, grantID string) (*sandboxstore.RootFSWriterGrant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.grant == nil || f.grant.ID != grantID {
		return nil, sandboxstore.ErrRootFSWriterGrantNotFound
	}
	return cloneGrant(f.grant), nil
}

func (f *fakeStore) AcquireRuntimeSlot(_ context.Context, request *sandboxstore.AcquireRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cloneRequest := *request
	f.acquires = append(f.acquires, &cloneRequest)
	if f.slot.ClaimOperationID == "" {
		now := time.Now().UTC()
		f.slot.State = sandboxstore.RuntimeSlotStateClaiming
		f.slot.ClaimOperationID = request.OperationID
		f.slot.ClaimID = request.ClaimID
		f.slot.ClaimClusterFilter = request.ClusterID
		f.slot.ClaimTTL = request.ClaimTTL
		f.slot.ClaimRuntimeAssignmentRevision = request.RuntimeAssignmentRevision
		f.slot.ClaimNetworkPolicyDigest = request.NetworkPolicyDigest
		f.slot.SandboxID = request.SandboxID
		f.slot.FilesystemID = request.FilesystemID
		f.slot.SourceGenerationID = request.SourceGenerationID
		f.slot.ClaimedAt = now
		f.slot.ClaimLeaseExpiresAt = now.Add(request.ClaimTTL)
	}
	f.slot.AuthorityObservedAt = time.Now().UTC()
	clone := *f.slot
	return &clone, nil
}

func (f *fakeStore) IssueRootFSWriterGrant(_ context.Context, request *sandboxstore.IssueRootFSWriterGrantRequest) (*sandboxstore.IssuedRootFSWriterGrant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cloneRequest := *request
	cloneRequest.BindingDigest = append([]byte(nil), request.BindingDigest...)
	f.issues = append(f.issues, &cloneRequest)
	if f.grant == nil {
		f.grant = &sandboxstore.RootFSWriterGrant{
			ID: request.GrantID, FilesystemID: request.ExpectedFilesystemID,
			SandboxID: request.SandboxID, ClaimID: request.ClaimID, SlotID: request.SlotID,
			IssueOperationID: request.OperationID, WriterEpoch: request.ExpectedWriterEpoch + 1,
			State:              sandboxstore.RootFSWriterGrantStateIssued,
			InitialHeadLayerID: request.InitialGenerationID, InitialGenerationID: request.InitialGenerationID,
			BindingVersion: request.BindingVersion, BindingDigest: append([]byte(nil), request.BindingDigest...),
			NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
			PodNamespace: request.PodNamespace, PodName: request.PodName, PodUID: request.PodUID,
			NodeName: request.NodeName, GateParent: request.GateParent,
			RuntimeGeneration: request.RuntimeGeneration, ConsumeExpiresAt: request.ConsumeExpiresAt,
		}
		f.filesystem.WriterEpoch = f.grant.WriterEpoch
	}
	if f.issueLost {
		f.issueLost = false
		return nil, errors.New("writer issue response lost")
	}
	return &sandboxstore.IssuedRootFSWriterGrant{Grant: cloneGrant(f.grant), RawToken: request.RawToken}, nil
}

func (f *fakeStore) BindRuntimeSlotWriterGrant(_ context.Context, request *sandboxstore.BindRuntimeSlotWriterGrantRequest) (*sandboxstore.RuntimeSlot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cloneRequest := *request
	f.binds = append(f.binds, &cloneRequest)
	f.slot.WriterGrantID = request.GrantID
	f.slot.AuthorityObservedAt = time.Now().UTC()
	clone := *f.slot
	return &clone, nil
}

func cloneGrant(source *sandboxstore.RootFSWriterGrant) *sandboxstore.RootFSWriterGrant {
	if source == nil {
		return nil
	}
	clone := *source
	clone.BindingDigest = append([]byte(nil), source.BindingDigest...)
	return &clone
}

type fakeNetwork struct {
	mu       sync.Mutex
	requests []NetworkPrepareRequest
	mutate   func(*rootfshandoff.NetworkPolicyToken)
}

func (f *fakeNetwork) Prepare(_ context.Context, request NetworkPrepareRequest) (rootfshandoff.NetworkPolicyToken, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, request)
	token := rootfshandoff.NetworkPolicyToken{
		PodUID: request.AllocationID,
		PodSandboxID: protocol.RuntimeSlotNetworkIncarnationID(protocol.NodeNetworkPrepareControlRequest{
			SlotID: request.SlotID, ClusterID: request.ClusterID, AllocationID: request.AllocationID,
			NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
			NetNSIdentity: request.NetNSIdentity,
		}),
		ClaimID:      request.ClaimID,
		NetworkEpoch: 1, PolicyDigest: request.PolicyDigest, PodIP: "192.0.2.8",
		CtldGeneration: "ctld-1", NetNSIdentity: request.NetNSIdentity,
	}
	if f.mutate != nil {
		f.mutate(&token)
	}
	return token, nil
}

type fakeNode struct {
	mu            sync.Mutex
	claims        []protocol.NodeClaimControlRequest
	commands      []protocol.CommandReadyControlRequest
	claimErrors   []error
	commandErrors []error
}

func (f *fakeNode) Claim(_ context.Context, _ NodeTarget, request protocol.NodeClaimControlRequest) (protocol.NodeControlResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims = append(f.claims, cloneNodeClaim(request))
	if len(f.claimErrors) == 0 {
		return protocol.NodeControlResponse{Phase: string(protocol.StateActive)}, nil
	}
	err := f.claimErrors[0]
	f.claimErrors = f.claimErrors[1:]
	return protocol.NodeControlResponse{}, err
}

func (f *fakeNode) CommandReady(_ context.Context, _ NodeTarget, request protocol.CommandReadyControlRequest) (protocol.NodeControlResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commands = append(f.commands, request)
	if len(f.commandErrors) == 0 {
		return protocol.NodeControlResponse{Phase: string(protocol.StateActive)}, nil
	}
	err := f.commandErrors[0]
	f.commandErrors = f.commandErrors[1:]
	return protocol.NodeControlResponse{}, err
}

func cloneNodeClaim(source protocol.NodeClaimControlRequest) protocol.NodeClaimControlRequest {
	clone := source
	if source.Stage != nil {
		stage := *source.Stage
		if source.Stage.Generation != nil {
			generation := *source.Stage.Generation
			generation.Descriptor = append([]byte(nil), source.Stage.Generation.Descriptor...)
			stage.Generation = &generation
		}
		clone.Stage = &stage
	}
	if source.Runtime != nil {
		runtime := cloneAssignment(*source.Runtime)
		clone.Runtime = &runtime
	}
	return clone
}

type fakeProber struct {
	mu        sync.Mutex
	addresses []string
	tokens    []string
}

func (f *fakeProber) ProbeCommandReady(_ context.Context, address, token string) (*procdapi.CommandReadyProbeResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addresses = append(f.addresses, address)
	f.tokens = append(f.tokens, token)
	return &procdapi.CommandReadyProbeResult{
		CommandReadyProbeResponse: procdapi.CommandReadyProbeResponse{InstanceID: "procd-instance-1", Status: "ready"},
		ResponseBodyDigest:        strings.Repeat("ab", 32),
	}, nil
}

type fakeTokenGenerator struct {
	requests [][3]string
}

func (f *fakeTokenGenerator) GenerateToken(teamID, userID, sandboxID string) (string, error) {
	f.requests = append(f.requests, [3]string{teamID, userID, sandboxID})
	return "internal-token", nil
}

type fakeObserver struct {
	observations []Observation
}

func (f *fakeObserver) ObserveRuntimeSlotClaim(observation Observation) {
	f.observations = append(f.observations, observation)
}

type plannerFixture struct {
	planner  *Planner
	store    *fakeStore
	network  *fakeNetwork
	node     *fakeNode
	prober   *fakeProber
	tokens   *fakeTokenGenerator
	observer *fakeObserver
	request  Request
}

func newPlannerFixture(t *testing.T) *plannerFixture {
	t.Helper()
	root := digest.FromString("mapping-root").String()
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 1 << 30,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: root,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/maps/root.page", Offset: 0, Length: 4096,
				Checksum: digest.FromString("mapping-page").String(),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := digest.FromString("base-artifact").String()
	store := &fakeStore{
		filesystem: &sandboxstore.RootFSFilesystem{
			ID: "filesystem-1", TeamID: "team-1", HeadGenerationID: "generation-1",
			WriterEpoch: 7, StorageFormat: sandboxstore.RootFSStorageFormatBlockCOWV1,
			BaseArtifactDigest: artifact, FormatGeneration: 1,
		},
		generation: &sandboxstore.RootFSGeneration{
			ID: "generation-1", FilesystemID: "filesystem-1",
			SourceOCIDigest:    digest.FromString("procd-image").String(),
			BaseArtifactDigest: artifact, BaseBlockRoot: digest.FromString("base-root").String(),
			CurrentBlockHead: root, WriterEpoch: 7, FormatGeneration: 1,
			DurabilityState: sandboxstore.RootFSGenerationStateS3Materialized,
			LocatorVersion:  1, Descriptor: descriptor,
		},
		slot: &sandboxstore.RuntimeSlot{
			ID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
			AllocationNamespace: "default", NodeID: "nomad-node-1", NodeUID: "node-uid-1",
			NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
			ControlEndpoint:     "unix:///run/sandbox0/nomad-slots/slot.sock",
			CompatibilityDigest: digest.FromString("compatibility").String(),
			ClaimLeaseExpiresAt: time.Now().Add(time.Minute),
		},
	}
	network := &fakeNetwork{}
	node := &fakeNode{}
	prober := &fakeProber{}
	tokens := &fakeTokenGenerator{}
	observer := &fakeObserver{}
	planner, err := New(Config{
		Store: store, Network: network, Node: node, Prober: prober, TokenGenerator: tokens,
		Observer: observer, WriterTokenKey: bytes.Repeat([]byte{0x42}, 32), ClaimTTL: 20 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{
		OperationID: "operation-1", SandboxID: "sandbox-1", TeamID: "team-1", UserID: "user-1",
		CompatibilityDigest: store.slot.CompatibilityDigest, ClusterID: store.slot.ClusterID,
		NetworkPolicy: `{"version":"v1","sandboxId":"sandbox-1","teamId":"team-1","mode":"allow-all"}`,
		Runtime: runtimecontrol.Assignment{
			SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 19,
			EnvVars: map[string]string{runtimecontrol.EnvSandboxID: "sandbox-1", "MODE": "test"},
		},
	}
	return &plannerFixture{
		planner: planner, store: store, network: network, node: node,
		prober: prober, tokens: tokens, observer: observer, request: request,
	}
}

func TestPlannerExecutesCompleteRegionToProcdClaim(t *testing.T) {
	fixture := newPlannerFixture(t)
	result, err := fixture.planner.Claim(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if result.ProcdAddress != "http://192.0.2.8:49983" || result.ProcdInstanceID != "procd-instance-1" ||
		result.Stage.Identity.WriterEpoch != 8 || result.Stage.Identity.RuntimeGeneration != "19" ||
		result.Stage.Identity.ContainerName != protocol.NomadTaskName ||
		result.CommandProof.RunscContainerID != protocol.NomadRunscContainerID("slot-1") {
		t.Fatalf("claim result = %+v", result)
	}
	if err := result.Stage.Validate(); err != nil {
		t.Fatalf("result stage is invalid: %v", err)
	}
	fixture.store.mu.Lock()
	if len(fixture.store.acquires) != 1 || len(fixture.store.issues) != 1 || len(fixture.store.binds) != 1 {
		t.Fatalf("store calls = acquire %d issue %d bind %d", len(fixture.store.acquires), len(fixture.store.issues), len(fixture.store.binds))
	}
	issue := fixture.store.issues[0]
	fixture.store.mu.Unlock()
	binding, err := result.Stage.BindingDigest()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(issue.BindingDigest, binding[:]) || issue.ExpectedWriterEpoch != 7 ||
		issue.ConsumeExpiresAt != fixture.store.slot.ClaimLeaseExpiresAt {
		t.Fatalf("writer issue request = %+v", issue)
	}
	fixture.network.mu.Lock()
	networkRequests := append([]NetworkPrepareRequest(nil), fixture.network.requests...)
	fixture.network.mu.Unlock()
	if len(networkRequests) != 1 || networkRequests[0].ClusterID != "cluster-1" {
		t.Fatalf("network prepare requests = %+v", networkRequests)
	}
	fixture.node.mu.Lock()
	defer fixture.node.mu.Unlock()
	if len(fixture.node.claims) != 1 || len(fixture.node.commands) != 1 ||
		fixture.node.claims[0].PolicyToken != result.Stage.Identity.WriterGrantToken ||
		fixture.node.commands[0].Proof != result.CommandProof {
		t.Fatalf("node claims = %+v, commands = %+v", fixture.node.claims, fixture.node.commands)
	}
	if len(fixture.tokens.requests) != 1 || fixture.tokens.requests[0] != [3]string{"team-1", "user-1", "sandbox-1"} {
		t.Fatalf("token requests = %+v", fixture.tokens.requests)
	}
	if len(fixture.observer.observations) != 1 || !fixture.observer.observations[0].Succeeded ||
		!fixture.observer.observations[0].WithinSLO || fixture.observer.observations[0].SlotID != "slot-1" {
		t.Fatalf("observation = %+v", fixture.observer.observations)
	}
	wantPhases := []string{
		PhaseRequestValidation, PhaseIngressToPlanner, PhaseRootFSMetadata, PhaseSlotAcquire,
		PhaseNetworkPrepare, PhaseWriterIssueBind, PhaseNodeClaim, PhaseProcdProbe,
		PhaseCommandReadyCommit,
	}
	if len(result.Phases) != len(wantPhases) || len(fixture.observer.observations[0].Phases) != len(wantPhases) {
		t.Fatalf("result phases = %+v observation phases = %+v", result.Phases, fixture.observer.observations[0].Phases)
	}
	for index, phase := range result.Phases {
		if phase.Phase != wantPhases[index] || !phase.Succeeded || phase.Duration < 0 {
			t.Fatalf("phase %d = %+v, want successful %s", index, phase, wantPhases[index])
		}
	}
}

func TestPlannerRecoversWriterIssueResponseLossWithExactBinding(t *testing.T) {
	fixture := newPlannerFixture(t)
	fixture.store.issueLost = true
	if _, err := fixture.planner.Claim(context.Background(), fixture.request); err == nil ||
		!strings.Contains(err.Error(), "writer issue response lost") {
		t.Fatalf("first Claim() error = %v", err)
	}
	result, err := fixture.planner.Claim(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("retry Claim() error = %v", err)
	}
	fixture.store.mu.Lock()
	issues := append([]*sandboxstore.IssueRootFSWriterGrantRequest(nil), fixture.store.issues...)
	fixture.store.mu.Unlock()
	if len(issues) != 2 || !reflect.DeepEqual(issues[0], issues[1]) || issues[1].ExpectedWriterEpoch != 7 {
		t.Fatalf("writer issue retries = %+v", issues)
	}
	if result.Stage.Identity.WriterEpoch != 8 {
		t.Fatalf("retry writer epoch = %d, want 8", result.Stage.Identity.WriterEpoch)
	}
	if len(fixture.observer.observations) != 2 || fixture.observer.observations[0].Succeeded ||
		!fixture.observer.observations[1].Succeeded {
		t.Fatalf("observations = %+v", fixture.observer.observations)
	}
}

func TestPlannerRetriesExactNodeClaimAfterAmbiguousSuccess(t *testing.T) {
	fixture := newPlannerFixture(t)
	fixture.node.claimErrors = []error{errors.New("node claim response lost")}
	if _, err := fixture.planner.Claim(context.Background(), fixture.request); err == nil {
		t.Fatal("ambiguous node claim unexpectedly succeeded")
	}
	if _, err := fixture.planner.Claim(context.Background(), fixture.request); err != nil {
		t.Fatalf("retry Claim() error = %v", err)
	}
	fixture.node.mu.Lock()
	defer fixture.node.mu.Unlock()
	if len(fixture.node.claims) != 2 || !reflect.DeepEqual(fixture.node.claims[0], fixture.node.claims[1]) {
		t.Fatalf("node claim retries changed: %+v", fixture.node.claims)
	}
	if len(fixture.node.commands) != 1 {
		t.Fatalf("node command-ready calls = %d, want 1", len(fixture.node.commands))
	}
}

func TestPlannerRejectsUntrustedNetworkTokenBeforeWriterIssue(t *testing.T) {
	fixture := newPlannerFixture(t)
	fixture.network.mutate = func(token *rootfshandoff.NetworkPolicyToken) {
		token.PodUID = "another-allocation"
	}
	if _, err := fixture.planner.Claim(context.Background(), fixture.request); err == nil ||
		!strings.Contains(err.Error(), "does not match runtime slot claim") {
		t.Fatalf("Claim() error = %v", err)
	}
	fixture.store.mu.Lock()
	defer fixture.store.mu.Unlock()
	if len(fixture.store.issues) != 0 || len(fixture.store.binds) != 0 {
		t.Fatalf("untrusted network token reached writer authority: issue %d bind %d", len(fixture.store.issues), len(fixture.store.binds))
	}
	if len(fixture.node.claims) != 0 {
		t.Fatalf("untrusted network token reached node: %+v", fixture.node.claims)
	}
}

func TestPlannerRejectsChangedRuntimeRetryBeforeNetworkMutation(t *testing.T) {
	fixture := newPlannerFixture(t)
	fixture.network.mutate = func(token *rootfshandoff.NetworkPolicyToken) {
		token.PodUID = "another-allocation"
	}
	if _, err := fixture.planner.Claim(context.Background(), fixture.request); err == nil {
		t.Fatal("first claim unexpectedly succeeded")
	}
	fixture.network.mutate = nil
	changed := fixture.request
	changed.Runtime = cloneAssignment(fixture.request.Runtime)
	changed.Runtime.EnvVars["MODE"] = "changed"
	if _, err := fixture.planner.Claim(context.Background(), changed); err == nil ||
		!strings.Contains(err.Error(), "another claim binding") {
		t.Fatalf("changed retry error = %v", err)
	}
	fixture.network.mu.Lock()
	defer fixture.network.mu.Unlock()
	if len(fixture.network.requests) != 1 {
		t.Fatalf("network prepare calls = %d, changed retry mutated policy", len(fixture.network.requests))
	}
}

func TestPlannerValidatesLogicalInputsBeforeAuthorityMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Request)
	}{
		{
			name: "mismatched sandbox",
			mutate: func(request *Request) {
				request.Runtime.EnvVars[runtimecontrol.EnvSandboxID] = "another-sandbox"
			},
		},
		{
			name: "malformed compatibility digest",
			mutate: func(request *Request) {
				request.CompatibilityDigest = "sha256:not-a-digest"
			},
		},
		{
			name: "oversized runtime assignment",
			mutate: func(request *Request) {
				request.Runtime.EnvVars["OVERSIZED"] = strings.Repeat("x", protocol.MaxRuntimeAssignmentBytes)
			},
		},
		{
			name: "oversized network policy",
			mutate: func(request *Request) {
				request.NetworkPolicy = strings.Repeat("x", protocol.MaxNetworkPolicyBytes+1)
			},
		},
		{
			name: "mismatched network policy identity",
			mutate: func(request *Request) {
				request.NetworkPolicy = `{"version":"v1","sandboxId":"another-sandbox","teamId":"team-1","mode":"allow-all"}`
			},
		},
		{
			name: "unknown network policy field",
			mutate: func(request *Request) {
				request.NetworkPolicy = `{"version":"v1","sandboxId":"sandbox-1","teamId":"team-1","mode":"allow-all","unknown":true}`
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPlannerFixture(t)
			test.mutate(&fixture.request)
			if _, err := fixture.planner.Claim(context.Background(), fixture.request); err == nil {
				t.Fatal("invalid request was accepted")
			}
			fixture.store.mu.Lock()
			defer fixture.store.mu.Unlock()
			if len(fixture.store.acquires) != 0 || len(fixture.store.issues) != 0 {
				t.Fatalf("invalid request mutated authority: acquire %d issue %d", len(fixture.store.acquires), len(fixture.store.issues))
			}
		})
	}
}

func TestNewRequiresStableWriterTokenKey(t *testing.T) {
	fixture := newPlannerFixture(t)
	_, err := New(Config{
		Store: fixture.store, Network: fixture.network, Node: fixture.node,
		Prober: fixture.prober, TokenGenerator: fixture.tokens,
		WriterTokenKey: []byte("short"),
	})
	if err == nil {
		t.Fatal("short writer token key was accepted")
	}
	_, err = New(Config{
		Store: fixture.store, Network: fixture.network, Node: fixture.node,
		Prober: fixture.prober, TokenGenerator: fixture.tokens,
		WriterTokenKey: bytes.Repeat([]byte{0x42}, 32), ClaimTTL: 500 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("sub-second claim TTL was accepted")
	}
}

func TestPlannerMeasuresTrustedIngressToTerminalProbe(t *testing.T) {
	fixture := newPlannerFixture(t)
	startedAt := time.Now().UTC()
	finishedAt := startedAt.Add(1250 * time.Millisecond)
	planner, err := New(Config{
		Store: fixture.store, Network: fixture.network, Node: fixture.node,
		Prober: fixture.prober, TokenGenerator: fixture.tokens, Observer: fixture.observer,
		WriterTokenKey: bytes.Repeat([]byte{0x42}, 32), ClaimTTL: 20 * time.Second,
		SLO: time.Second, Now: func() time.Time { return finishedAt },
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.request.StartedAt = startedAt
	result, err := planner.Claim(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if result.Duration != 1250*time.Millisecond || result.WithinSLO {
		t.Fatalf("timing result = duration %s within_slo %t", result.Duration, result.WithinSLO)
	}
	if len(fixture.observer.observations) != 1 || fixture.observer.observations[0].Duration != result.Duration ||
		fixture.observer.observations[0].WithinSLO {
		t.Fatalf("observation = %+v", fixture.observer.observations)
	}
}
