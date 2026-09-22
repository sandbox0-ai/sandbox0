package runtimeslotnode

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationAdoptionChannelStore struct {
	channelTestCapacityStore
	mu                    sync.Mutex
	want                  protocol.MigrationAdoptionReceipt
	committed             *protocol.MigrationAdoptionReceipt
	calls                 int
	failBefore, failAfter int
}

var errAdoptionCommit = errors.New("injected adoption commit acknowledgement failure")

func (s *migrationAdoptionChannelStore) CommitNomadSandboxMigrationAdoption(ctx context.Context, request protocol.MigrationAdoptionRequest, proof protocol.MigrationAdoptionProof) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if err := ctx.Err(); err != nil {
		return err
	}
	receipt := protocol.MigrationAdoptionReceipt{Request: request, Proof: proof}
	if receipt != s.want {
		return sandboxstore.ErrNomadSandboxMigrationConflict
	}
	if s.failBefore > 0 {
		s.failBefore--
		return errAdoptionCommit
	}
	s.committed = &receipt
	if s.failAfter > 0 {
		s.failAfter--
		return errAdoptionCommit
	}
	return nil
}

type migrationAdoptionChannelExecutor struct {
	channelTestExecutor
	receipt protocol.MigrationAdoptionReceipt
}

func (e *migrationAdoptionChannelExecutor) CommandReady(_ context.Context, _ protocol.NodeChannelTarget, request protocol.CommandReadyControlRequest) (protocol.NodeControlResponse, error) {
	e.mu.Lock()
	e.commands = append(e.commands, request)
	e.mu.Unlock()
	copy := e.receipt
	return protocol.NodeControlResponse{Phase: string(protocol.StateActive), MigrationAdoption: &copy}, nil
}

func migrationAdoptionChannelFixture(t *testing.T) (runtimeslotclaim.NodeTarget, protocol.CommandReadyControlRequest, protocol.MigrationAdoptionReceipt) {
	t.Helper()
	target := runtimeslotclaim.NodeTarget{SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1", ControlEndpoint: "unix:///var/run/sandbox0/target.sock"}
	ready := protocol.CommandReadyControlRequest{Proof: testChannelCommandReadyProof()}
	digest, err := ready.Proof.Digest()
	require.NoError(t, err)
	request := protocol.MigrationAdoptionRequest{Target: nodeChannelTarget(target), OperationID: ready.Proof.OperationID, ClaimID: ready.Proof.ClaimID,
		SandboxID: "sandbox-1", RuntimeGeneration: 2, ProcdInstanceID: ready.Proof.ProcdInstanceID,
		RestoreDigest: strings.Repeat("a", 64), CommandReadyDigest: digest}
	digest, err = request.Digest()
	require.NoError(t, err)
	return target, ready, protocol.MigrationAdoptionReceipt{Request: request, Proof: protocol.MigrationAdoptionProof{RequestDigest: digest, ImageAbsent: true}}
}

func startMigrationAdoptionChannel(t *testing.T, store CapacityStore, receipt protocol.MigrationAdoptionReceipt) (*ChannelHub, *migrationAdoptionChannelExecutor) {
	t.Helper()
	hub, err := NewChannelHub(channelTestVerifier{}, store)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hub.Close() })
	server, files := newNodeChannelTLSServer(t, hub)
	t.Cleanup(server.Close)
	executor := &migrationAdoptionChannelExecutor{receipt: receipt}
	agent, err := protocol.NewNodeChannelAgent(protocol.NodeChannelAgentConfig{BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
		ClientKeyFile: files.clientKey, TokenFile: files.token, PeerURISAN: testNodeChannelServerURI,
		NodeUID: "node-uid-1", NodeBootIDFile: files.boot, ClusterID: "cluster-1", NodeID: "node-1",
		Executor: executor, PlannedRetireExecutor: executor, Capacity: channelTestCapacity(), AgentInstanceID: "agent-1",
		ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-1")
	return hub, executor
}

func TestMigrationAdoptionChannelCommitsBeforeAcknowledgementAndRetriesLostCommit(t *testing.T) {
	target, ready, receipt := migrationAdoptionChannelFixture(t)
	store := &migrationAdoptionChannelStore{want: receipt, failBefore: 1, failAfter: 1}
	hub, executor := startMigrationAdoptionChannel(t, store, receipt)
	response, err := hub.CommandReady(t.Context(), target, ready)
	require.ErrorIs(t, err, errAdoptionCommit)
	require.Empty(t, response.Phase)
	store.mu.Lock()
	require.Nil(t, store.committed)
	store.mu.Unlock()
	response, err = hub.CommandReady(t.Context(), target, ready)
	require.ErrorIs(t, err, errAdoptionCommit)
	require.Empty(t, response.Phase)
	store.mu.Lock()
	require.Equal(t, &receipt, store.committed)
	store.mu.Unlock()
	var wg sync.WaitGroup
	errs := make([]error, 8)
	responses := make([]protocol.NodeControlResponse, len(errs))
	for i := range errs {
		wg.Go(func() { responses[i], errs[i] = hub.CommandReady(t.Context(), target, ready) })
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err)
		require.Equal(t, &receipt, responses[i].MigrationAdoption)
	}
	store.mu.Lock()
	require.Equal(t, 10, store.calls)
	require.Equal(t, &receipt, store.committed)
	store.mu.Unlock()
	executor.mu.Lock()
	defer executor.mu.Unlock()
	require.NotEmpty(t, executor.commands)
	for _, command := range executor.commands {
		require.Equal(t, ready, command)
	}
	require.Empty(t, executor.claims)
	require.Empty(t, executor.calls)
}

func TestMigrationAdoptionChannelRejectsForeignOrIncompleteReceiptBeforeStore(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*protocol.MigrationAdoptionReceipt)
	}{
		{"node UID", func(r *protocol.MigrationAdoptionReceipt) { r.Request.Target.NodeUID = "other-node" }},
		{"boot", func(r *protocol.MigrationAdoptionReceipt) { r.Request.Target.NodeBootID = "other-boot" }},
		{"allocation", func(r *protocol.MigrationAdoptionReceipt) { r.Request.Target.AllocationID = "other-allocation" }},
		{"socket", func(r *protocol.MigrationAdoptionReceipt) { r.Request.Target.ControlEndpoint = "unix:///other.sock" }},
		{"operation", func(r *protocol.MigrationAdoptionReceipt) { r.Request.OperationID = "other-operation" }},
		{"procd", func(r *protocol.MigrationAdoptionReceipt) { r.Request.ProcdInstanceID = "other-procd" }},
		{"command proof", func(r *protocol.MigrationAdoptionReceipt) { r.Request.CommandReadyDigest = strings.Repeat("b", 64) }},
		{"image present", func(r *protocol.MigrationAdoptionReceipt) { r.Proof.ImageAbsent = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, ready, receipt := migrationAdoptionChannelFixture(t)
			store := &migrationAdoptionChannelStore{want: receipt}
			tc.change(&receipt)
			digest, err := receipt.Request.Digest()
			require.NoError(t, err)
			receipt.Proof.RequestDigest = digest
			hub, executor := startMigrationAdoptionChannel(t, store, receipt)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			response, err := hub.CommandReady(ctx, target, ready)
			require.Error(t, err)
			require.Empty(t, response.Phase)
			executor.mu.Lock()
			require.NotEmpty(t, executor.commands, "rejection must exercise the authenticated response path")
			executor.mu.Unlock()
			store.mu.Lock()
			defer store.mu.Unlock()
			require.Zero(t, store.calls)
		})
	}
}

func TestMigrationAdoptionChannelRequiresRegionalAuthority(t *testing.T) {
	for _, missing := range []bool{false, true} {
		target, ready, receipt := migrationAdoptionChannelFixture(t)
		var store CapacityStore = channelTestCapacityStore{}
		if !missing {
			store = &migrationAdoptionChannelStore{}
		}
		hub, _ := startMigrationAdoptionChannel(t, store, receipt)
		response, err := hub.CommandReady(t.Context(), target, ready)
		require.Error(t, err)
		require.Empty(t, response.Phase)
		if !missing {
			require.ErrorIs(t, err, sandboxstore.ErrNomadSandboxMigrationConflict)
		}
	}
}
