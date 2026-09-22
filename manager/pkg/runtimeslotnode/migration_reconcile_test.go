package runtimeslotnode

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

// The embedded legacy interfaces deliberately have no implementation: source
// migration must never fall back to ordinary writer or terminal cleanup.
type migrationReconcileStore struct {
	runtimeslotreconciler.Store
	slot                                 sandboxstore.RuntimeSlot
	grant                                sandboxstore.RootFSWriterGrant
	command                              protocol.MigrationSourceFinalizeRequest
	receipt                              *protocol.MigrationSourceFinalizationReceipt
	authorizeErr, commitErr, completeErr error
	completed                            int
	order                                *[]string
}

func (s *migrationReconcileStore) RecoverExpiredNomadMigrationReservations(context.Context, int) (int, error) {
	return 0, nil
}
func (s *migrationReconcileStore) ListRuntimeSlotsForReconcileAfter(context.Context, int, *sandboxstore.RuntimeSlot) ([]sandboxstore.RuntimeSlot, error) {
	if s.slot.State == sandboxstore.RuntimeSlotStateTerminal {
		return nil, nil
	}
	return []sandboxstore.RuntimeSlot{s.slot}, nil
}
func (s *migrationReconcileStore) GetRuntimeSlot(context.Context, string) (*sandboxstore.RuntimeSlot, error) {
	copy := s.slot
	return &copy, nil
}
func (s *migrationReconcileStore) GetRootFSWriterGrant(context.Context, string) (*sandboxstore.RootFSWriterGrant, error) {
	copy := s.grant
	return &copy, nil
}
func (s *migrationReconcileStore) MarkRuntimeSlotAllocationMissing(_ context.Context, r *sandboxstore.MarkRuntimeSlotAllocationMissingRequest) (*sandboxstore.RuntimeSlot, error) {
	*s.order = append(*s.order, "mark-missing")
	if r.SlotID != s.slot.ID || r.AllocationID != s.slot.AllocationID || r.NodeUID != s.slot.NodeUID || r.NodeBootID != s.slot.NodeBootID {
		return nil, errors.New("changed allocation")
	}
	s.slot.State = sandboxstore.RuntimeSlotStateOrphaned
	s.slot.OrphanObservationDigest = r.ObservationDigest
	copy := s.slot
	return &copy, nil
}
func (s *migrationReconcileStore) GetNomadSandboxMigrationSourceFinalization(context.Context, string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	*s.order = append(*s.order, "read-receipt")
	return s.receipt, nil
}
func (s *migrationReconcileStore) AuthorizeNomadSandboxMigrationSourceFinalization(context.Context, string) (*protocol.MigrationSourceFinalizeRequest, error) {
	*s.order = append(*s.order, "authorize")
	copy := s.command
	return &copy, s.authorizeErr
}
func (s *migrationReconcileStore) CommitNomadSandboxMigrationSourceFinalization(_ context.Context, r protocol.MigrationSourceFinalizeRequest, p protocol.MigrationSourceFinalizeProof) error {
	*s.order = append(*s.order, "commit-receipt")
	if s.commitErr != nil {
		return s.commitErr
	}
	if err := p.ValidateFor(r); err != nil {
		return err
	}
	s.receipt = &protocol.MigrationSourceFinalizationReceipt{Request: r, Proof: p}
	return nil
}
func (s *migrationReconcileStore) CompleteNomadSandboxMigrationSource(context.Context, string) (*sandboxstore.RuntimeSlot, error) {
	*s.order = append(*s.order, "complete")
	if s.completeErr != nil {
		return nil, s.completeErr
	}
	if s.receipt == nil || s.slot.State != sandboxstore.RuntimeSlotStateOrphaned || len(s.slot.OrphanObservationDigest) != 32 {
		return nil, errors.New("missing physical evidence")
	}
	s.completed++
	s.slot.State = sandboxstore.RuntimeSlotStateTerminal
	s.slot.TerminalReason = "migration_source"
	s.slot.ResourceLeaseState = sandboxstore.RuntimeResourceLeaseReleased
	copy := s.slot
	return &copy, nil
}

type migrationReconcileAllocation struct {
	target                          runtimeslotreconciler.AllocationTarget
	present, keepPresent, purgeLost bool
	purges                          int
	order                           *[]string
}

func (a *migrationReconcileAllocation) Observe(context.Context, runtimeslotreconciler.AllocationTarget) (runtimeslotreconciler.AllocationObservation, error) {
	*a.order = append(*a.order, "observe")
	return runtimeslotreconciler.AllocationObservation{Target: a.target, PhysicalPresent: a.present, ProofDigest: bytes.Repeat([]byte{0x71}, 32)}, nil
}
func (a *migrationReconcileAllocation) Purge(_ context.Context, r runtimeslotreconciler.AllocationPurgeRequest) error {
	*a.order = append(*a.order, "purge")
	a.purges++
	if r.Target != a.target || r.OperationID == "" {
		return errors.New("changed purge target")
	}
	a.present = a.keepPresent
	if a.purgeLost {
		a.purgeLost = false
		return errors.New("purge response lost")
	}
	return nil
}

type migrationReconcileTransport struct {
	*fakeTransport
	executor   migrationChannelExecutor
	err        error
	invalid    bool
	gcErr      error
	gcInvalid  bool
	gcCalls    int
	gcLost     bool
	gcRequests []protocol.MigrationSourceGCRequest
	order      *[]string
}

func (n *migrationReconcileTransport) AcknowledgeMigrationSourceGC(_ context.Context, r protocol.MigrationSourceGCRequest) (*protocol.MigrationSourceGCAcknowledgement, error) {
	*n.order = append(*n.order, "ack-gc")
	n.gcCalls++
	n.gcRequests = append(n.gcRequests, r)
	if n.gcErr != nil {
		return nil, n.gcErr
	}
	digest, err := r.Digest()
	if n.gcLost {
		n.gcLost = false
		return nil, errors.New("GC acknowledgement response lost")
	}
	if n.gcInvalid {
		digest = strings.Repeat("f", 64)
	}
	return &protocol.MigrationSourceGCAcknowledgement{RequestDigest: digest}, err
}

func (n *migrationReconcileTransport) FinalizeMigrationSource(ctx context.Context, r protocol.MigrationSourceFinalizeRequest) (*protocol.MigrationSourceFinalizeProof, error) {
	*n.order = append(*n.order, "finalize-node")
	if n.err != nil {
		return nil, n.err
	}
	p, err := n.executor.FinalizeMigrationSource(ctx, r)
	if p != nil && n.invalid {
		p.ImageAbsent = false
	}
	return p, err
}

func migrationReconcileFixture(t *testing.T) (*runtimeslotreconciler.Reconciler, *migrationReconcileStore, *migrationReconcileAllocation, *migrationReconcileTransport) {
	t.Helper()
	capture := protocol.MigrationCaptureRequest{Target: protocol.NodeChannelTarget{SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1", ControlEndpoint: "unix:///source/control.sock"},
		OperationID: "migration-1", LifecycleEpoch: 2, SandboxID: "sandbox-1", SourceGeneration: 1, ProcdInstanceID: "procd-1", AssignmentRevision: strings.Repeat("ab", 32), BindingDigest: strings.Repeat("cd", 32), ResourceLeaseDigest: strings.Repeat("ef", 32)}
	command := migrationChannelFinalize(t, capture)
	c := command.Cleanup
	resourceDigest, err := hex.DecodeString(c.ResourceLeaseDigest)
	require.NoError(t, err)
	binding, err := hex.DecodeString(capture.BindingDigest)
	require.NoError(t, err)
	fence, err := hex.DecodeString(command.SourceProof.Digest)
	require.NoError(t, err)
	order := []string{}
	store := &migrationReconcileStore{command: command, order: &order}
	store.slot = sandboxstore.RuntimeSlot{ID: c.SlotID, ClusterID: c.ClusterID, AllocationID: c.AllocationID, AllocationNamespace: "default", NodeID: c.NodeID, NodeUID: c.NodeUID, NodeBootID: c.NodeBootID, NetNSIdentity: c.NetNSIdentity,
		ClaimOperationID: c.Resources.OperationID, ClaimID: c.Resources.ClaimID, SandboxID: capture.SandboxID, FilesystemID: command.SourceProof.RootFS.Session.RootFSID, SourceGenerationID: "initial-generation", WriterGrantID: c.WriterGrantID, RunscContainerID: c.RunscContainerID,
		State: sandboxstore.RuntimeSlotStateQuiescing, RootFSBindingDigest: binding, ResourceLease: c.Resources, ResourceLeaseDigest: resourceDigest, ResourceLeaseState: sandboxstore.RuntimeResourceLeaseActive}
	store.grant = sandboxstore.RootFSWriterGrant{ID: c.WriterGrantID, FilesystemID: store.slot.FilesystemID, SandboxID: capture.SandboxID, SlotID: c.SlotID, ClaimID: c.Resources.ClaimID, IssueOperationID: c.Resources.OperationID, WriterEpoch: 1, InitialGenerationID: store.slot.SourceGenerationID,
		BindingVersion: sandboxstore.RootFSWriterBindingVersion, BindingDigest: binding, NodeUID: c.NodeUID, NodeBootID: c.NodeBootID, State: sandboxstore.RootFSWriterGrantStateRetired, RetireKind: sandboxstore.RootFSWriterRetireKindMigration, RetireOperationID: capture.OperationID, RetireProofDigest: fence}
	allocation := &migrationReconcileAllocation{target: runtimeslotreconciler.AllocationTarget{ClusterID: c.ClusterID, AllocationID: c.AllocationID, AllocationNamespace: "default", NodeID: c.NodeID}, present: true, order: &order}
	transport := &migrationReconcileTransport{fakeTransport: &fakeTransport{}, order: &order}
	node, err := New(transport)
	require.NoError(t, err)
	writer := struct {
		runtimeslotreconciler.WriterController
	}{}
	reconciler, err := runtimeslotreconciler.New(runtimeslotreconciler.Config{Store: store, Allocation: allocation, Node: node, Writer: writer, Limit: 1})
	require.NoError(t, err)
	return reconciler, store, allocation, transport
}

func TestMigrationCompletionWorkerOrdersNodeReceiptPurgeAndAtomicRelease(t *testing.T) {
	r, s, a, n := migrationReconcileFixture(t)
	result, err := r.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, []string{"observe", "read-receipt", "authorize", "finalize-node", "commit-receipt", "purge", "observe", "mark-missing", "ack-gc", "complete"}, *s.order)
	require.Equal(t, 1, n.executor.calls)
	require.Equal(t, 1, a.purges)
	require.Equal(t, 1, s.completed)
	result, err = r.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, result.Completed)
}

func TestMigrationCompletionWorkerRetainsCustodyBeforeEveryRequiredProof(t *testing.T) {
	for _, failure := range []string{"pending", "node", "malformed-proof", "changed-source", "receipt", "allocation", "gc", "malformed-gc", "completion"} {
		t.Run(failure, func(t *testing.T) {
			r, s, a, n := migrationReconcileFixture(t)
			switch failure {
			case "pending":
				s.authorizeErr = sandboxstore.ErrNomadSandboxMigrationNotReady
			case "node":
				n.err = errors.New("node unavailable")
			case "malformed-proof":
				n.invalid = true
			case "changed-source":
				s.command.Cleanup.NetNSIdentity = "another-namespace"
			case "receipt":
				s.commitErr = errors.New("receipt commit failed")
			case "allocation":
				a.keepPresent = true
			case "completion":
				s.completeErr = errors.New("completion commit failed")
			case "gc":
				n.gcErr = errors.New("GC acknowledgement unavailable")
			case "malformed-gc":
				n.gcInvalid = true
			}
			result, err := r.RunOnce(t.Context())
			if failure == "pending" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Zero(t, result.Completed)
			require.Zero(t, s.completed)
			require.Equal(t, sandboxstore.RuntimeResourceLeaseActive, s.slot.ResourceLeaseState)
			if failure != "allocation" && failure != "completion" && failure != "gc" && failure != "malformed-gc" {
				require.Zero(t, a.purges)
			}
			if failure != "completion" && failure != "gc" && failure != "malformed-gc" {
				require.Zero(t, n.gcCalls)
			}
		})
	}
}

func TestMigrationCompletionWorkerReusesCommittedReceiptAfterLostPurgeResponse(t *testing.T) {
	r, s, a, n := migrationReconcileFixture(t)
	a.purgeLost = true
	_, err := r.RunOnce(t.Context())
	require.ErrorContains(t, err, "purge response lost")
	require.NotNil(t, s.receipt)
	require.Zero(t, s.completed)
	// Restart with the durable store. Finalization need not run again, but the
	// lightweight GC acknowledgement still requires a reachable node channel.
	n.err = errors.New("ctld unavailable after cleanup")
	n.gcErr = errors.New("ctld unavailable for GC acknowledgement")
	node, err := New(n)
	require.NoError(t, err)
	r, err = runtimeslotreconciler.New(runtimeslotreconciler.Config{Store: s, Allocation: a, Node: node, Writer: struct {
		runtimeslotreconciler.WriterController
	}{}, Limit: 1})
	require.NoError(t, err)
	result, err := r.RunOnce(t.Context())
	require.ErrorContains(t, err, "ctld unavailable for GC acknowledgement")
	require.Zero(t, result.Completed)
	n.gcErr = nil
	result, err = r.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, 1, n.executor.calls)
	require.Equal(t, 3, a.purges)
	require.Equal(t, 1, s.completed)
}

func TestMigrationCompletionWorkerUsesAuthenticatedNodeChannel(t *testing.T) {
	_, store, allocation, _ := migrationReconcileFixture(t)
	hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
	require.NoError(t, err)
	defer hub.Close()
	server, files := newNodeChannelTLSServer(t, hub)
	defer server.Close()
	executor := &migrationChannelExecutor{}
	agent, err := protocol.NewNodeChannelAgent(protocol.NodeChannelAgentConfig{BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
		ClientKeyFile: files.clientKey, TokenFile: files.token, PeerURISAN: testNodeChannelServerURI,
		NodeUID: "node-uid-1", NodeBootIDFile: files.boot, ClusterID: "cluster-1", NodeID: "node-1", AgentInstanceID: "agent-1",
		Executor: &channelTestExecutor{}, Capacity: channelTestCapacity(), MigrationFinalizeExecutor: executor, MigrationSourceGCExecutor: executor,
		ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx) }()
	defer func() { cancel(); <-done }()
	waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-1")
	node, err := New(hub)
	require.NoError(t, err)
	r, err := runtimeslotreconciler.New(runtimeslotreconciler.Config{Store: store, Allocation: allocation, Node: node, Writer: struct {
		runtimeslotreconciler.WriterController
	}{}, Limit: 1})
	require.NoError(t, err)
	result, err := r.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.NoError(t, store.receipt.Validate())
	executor.mu.Lock()
	require.Equal(t, 1, executor.calls)
	executor.mu.Unlock()
}

func TestMigrationCompletionWorkerRetainsSourceOnUnsupportedTransport(t *testing.T) {
	_, store, allocation, _ := migrationReconcileFixture(t)
	node, err := New(&fakeTransport{})
	require.NoError(t, err)
	r, err := runtimeslotreconciler.New(runtimeslotreconciler.Config{Store: store, Allocation: allocation, Node: node, Writer: struct {
		runtimeslotreconciler.WriterController
	}{}, Limit: 1})
	require.NoError(t, err)
	result, err := r.RunOnce(t.Context())
	require.ErrorContains(t, err, "does not support migration finalization")
	require.Zero(t, result.Completed)
	require.Zero(t, allocation.purges)
	require.Nil(t, store.receipt)
}

func TestMigrationCompletionWorkerRetriesAtomicCommitWithoutNodeReplay(t *testing.T) {
	r, store, allocation, node := migrationReconcileFixture(t)
	store.completeErr = errors.New("completion transaction unavailable")
	_, err := r.RunOnce(t.Context())
	require.ErrorContains(t, err, "completion transaction unavailable")
	require.Equal(t, sandboxstore.RuntimeSlotStateOrphaned, store.slot.State)
	require.NotNil(t, store.receipt)
	store.completeErr = nil
	node.err = errors.New("ctld unavailable")
	result, err := r.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, 1, node.executor.calls)
	require.Equal(t, 2, allocation.purges)
}

func TestMigrationCompletionWorkerRetriesLostGCAcknowledgementBeforeRelease(t *testing.T) {
	r, store, allocation, node := migrationReconcileFixture(t)
	node.gcLost = true
	result, err := r.RunOnce(t.Context())
	require.ErrorContains(t, err, "GC acknowledgement response lost")
	require.Zero(t, result.Completed)
	require.Equal(t, sandboxstore.RuntimeSlotStateOrphaned, store.slot.State)
	require.Equal(t, sandboxstore.RuntimeResourceLeaseActive, store.slot.ResourceLeaseState)
	require.NotNil(t, store.receipt)
	node.err = errors.New("physical cleanup must not repeat")
	result, err = r.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, 1, node.executor.calls)
	require.Equal(t, 2, allocation.purges)
	require.Len(t, node.gcRequests, 2)
	require.Equal(t, node.gcRequests[0], node.gcRequests[1])
}
