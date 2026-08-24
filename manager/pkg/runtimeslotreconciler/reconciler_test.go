package runtimeslotreconciler

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type fakeStore struct {
	slot          *sandboxstore.RuntimeSlot
	grant         *sandboxstore.RootFSWriterGrant
	order         *[]string
	fenceCalls    int
	markCalls     int
	finalizeCalls int
	terminalProof []byte
}

func (f *fakeStore) ListRuntimeSlotsForReconcile(context.Context, int) ([]sandboxstore.RuntimeSlot, error) {
	f.record("list")
	if f.slot.State == sandboxstore.RuntimeSlotStateTerminal {
		return nil, nil
	}
	return []sandboxstore.RuntimeSlot{*cloneSlot(f.slot)}, nil
}

func (f *fakeStore) GetRuntimeSlot(context.Context, string) (*sandboxstore.RuntimeSlot, error) {
	f.record("get-slot")
	return cloneSlot(f.slot), nil
}

func (f *fakeStore) FenceRuntimeSlotForReconcile(_ context.Context, request *sandboxstore.FenceRuntimeSlotForReconcileRequest) (*sandboxstore.RuntimeSlot, error) {
	f.record("fence")
	f.fenceCalls++
	if request.SlotID != f.slot.ID {
		return nil, sandboxstore.ErrRuntimeSlotConflict
	}
	if !runtimeSlotDue(f.slot) {
		return nil, sandboxstore.ErrRuntimeSlotNotDue
	}
	f.slot.State = sandboxstore.RuntimeSlotStateQuiescing
	f.slot.Revision++
	f.slot.HeartbeatExpiresAt = f.slot.AuthorityObservedAt
	return cloneSlot(f.slot), nil
}

func (f *fakeStore) GetRootFSWriterGrant(context.Context, string) (*sandboxstore.RootFSWriterGrant, error) {
	f.record("get-grant")
	if f.grant == nil {
		return nil, sandboxstore.ErrRootFSWriterGrantNotFound
	}
	clone := *f.grant
	clone.BindingDigest = append([]byte(nil), f.grant.BindingDigest...)
	return &clone, nil
}

func (f *fakeStore) MarkRuntimeSlotAllocationMissing(_ context.Context, request *sandboxstore.MarkRuntimeSlotAllocationMissingRequest) (*sandboxstore.RuntimeSlot, error) {
	f.record("mark-missing")
	f.markCalls++
	if request.SlotID != f.slot.ID || request.AllocationID != f.slot.AllocationID ||
		request.NodeUID != f.slot.NodeUID || request.NodeBootID != f.slot.NodeBootID {
		return nil, sandboxstore.ErrRuntimeSlotConflict
	}
	if f.slot.ClaimID == "" {
		f.slot.State = sandboxstore.RuntimeSlotStateTerminal
		f.slot.TerminalReason = "allocation_missing"
		f.slot.TerminalProofDigest = append([]byte(nil), request.ObservationDigest...)
	} else {
		f.slot.State = sandboxstore.RuntimeSlotStateOrphaned
		f.slot.OrphanObservationDigest = append([]byte(nil), request.ObservationDigest...)
	}
	f.slot.Revision++
	return cloneSlot(f.slot), nil
}

func (f *fakeStore) FinalizeRuntimeSlot(_ context.Context, request *sandboxstore.FinalizeRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error) {
	f.record("finalize-slot")
	f.finalizeCalls++
	if request.SlotID != f.slot.ID || request.OperationID != f.slot.ClaimOperationID || request.ClaimID != f.slot.ClaimID {
		return nil, sandboxstore.ErrRuntimeSlotConflict
	}
	if f.slot.ResourceLease.IsZero() {
		if request.ResourceLeaseID != "" || len(request.ResourceLeaseDigest) != 0 || request.ResourceCgroupAbsent {
			return nil, sandboxstore.ErrRuntimeSlotConflict
		}
	} else if request.ResourceLeaseID != f.slot.ResourceLease.LeaseID ||
		!bytes.Equal(request.ResourceLeaseDigest, f.slot.ResourceLeaseDigest) || !request.ResourceCgroupAbsent {
		return nil, sandboxstore.ErrRuntimeSlotConflict
	}
	if f.grant != nil && f.grant.State != sandboxstore.RootFSWriterGrantStateRetired &&
		f.grant.State != sandboxstore.RootFSWriterGrantStateCanceled {
		return nil, sandboxstore.ErrRuntimeSlotInvalid
	}
	f.slot.State = sandboxstore.RuntimeSlotStateTerminal
	f.slot.TerminalReason = request.Reason
	f.slot.TerminalProofDigest = append([]byte(nil), request.ProofDigest...)
	if !f.slot.ResourceLease.IsZero() {
		f.slot.ResourceLeaseState = sandboxstore.RuntimeResourceLeaseReleased
		f.slot.ResourceLeaseReleasedAt = f.slot.AuthorityObservedAt
	}
	f.terminalProof = append([]byte(nil), request.ProofDigest...)
	f.slot.Revision++
	return cloneSlot(f.slot), nil
}

func (f *fakeStore) record(value string) {
	*f.order = append(*f.order, value)
}

func cloneSlot(source *sandboxstore.RuntimeSlot) *sandboxstore.RuntimeSlot {
	clone := *source
	clone.OrphanObservationDigest = append([]byte(nil), source.OrphanObservationDigest...)
	clone.TerminalProofDigest = append([]byte(nil), source.TerminalProofDigest...)
	clone.ResourceLeaseDigest = append([]byte(nil), source.ResourceLeaseDigest...)
	return &clone
}

type fakeAllocation struct {
	target       AllocationTarget
	present      bool
	order        *[]string
	observations []AllocationObservation
	purges       []AllocationPurgeRequest
	purgeLost    bool
}

func (f *fakeAllocation) Observe(context.Context, AllocationTarget) (AllocationObservation, error) {
	*f.order = append(*f.order, "observe-allocation")
	proofByte := byte(0x31)
	if !f.present {
		proofByte = 0x32
	}
	observation := AllocationObservation{
		Target: f.target, PhysicalPresent: f.present, ProofDigest: bytes.Repeat([]byte{proofByte}, 32),
	}
	f.observations = append(f.observations, observation)
	return observation, nil
}

func (f *fakeAllocation) Purge(_ context.Context, request AllocationPurgeRequest) error {
	*f.order = append(*f.order, "purge-allocation")
	f.purges = append(f.purges, request)
	f.present = false
	if f.purgeLost {
		f.purgeLost = false
		return errors.New("purge response lost")
	}
	return nil
}

type fakeNode struct {
	order       *[]string
	requests    []NodeCleanupRequest
	cleanupLost bool
	mutate      func(*NodeCleanupProof)
}

func (f *fakeNode) Cleanup(_ context.Context, request NodeCleanupRequest) (NodeCleanupProof, error) {
	*f.order = append(*f.order, "cleanup-node")
	f.requests = append(f.requests, request)
	proof := NodeCleanupProof{
		OperationID: request.OperationID, SlotID: request.SlotID, AllocationID: request.AllocationID,
		NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		ResourceLeaseID:      request.Resources.LeaseID,
		ResourceLeaseDigest:  append([]byte(nil), request.ResourceLeaseDigest...),
		ResourceCgroupAbsent: !request.Resources.IsZero(),
		ProofDigest:          bytes.Repeat([]byte{0x41}, 32),
	}
	if f.mutate != nil {
		f.mutate(&proof)
	}
	if f.cleanupLost {
		f.cleanupLost = false
		return NodeCleanupProof{}, errors.New("node cleanup response lost")
	}
	return proof, nil
}

type fakeWriter struct {
	store        *fakeStore
	order        *[]string
	fences       []WriterFenceRequest
	completes    []WriterCompleteRequest
	fenceLost    bool
	completeLost bool
	mutate       func(*WriterFenceProof)
}

func (f *fakeWriter) Fence(_ context.Context, request WriterFenceRequest) (WriterFenceProof, error) {
	*f.order = append(*f.order, "fence-writer")
	clone := request
	clone.BindingDigest = append([]byte(nil), request.BindingDigest...)
	f.fences = append(f.fences, clone)
	if f.store.grant.State == sandboxstore.RootFSWriterGrantStateIssued ||
		f.store.grant.State == sandboxstore.RootFSWriterGrantStateConsumed {
		f.store.grant.State = sandboxstore.RootFSWriterGrantStateRetiring
	}
	if f.fenceLost {
		f.fenceLost = false
		return WriterFenceProof{}, errors.New("writer fence response lost")
	}
	proof := WriterFenceProof{
		OperationID: request.OperationID, GrantID: request.GrantID,
		ProofDigest: bytes.Repeat([]byte{0x50}, 32),
	}
	if f.mutate != nil {
		f.mutate(&proof)
	}
	return proof, nil
}

func (f *fakeWriter) Complete(_ context.Context, request WriterCompleteRequest) (WriterFinalizeProof, error) {
	*f.order = append(*f.order, "complete-writer")
	clone := request
	clone.WriterFenceDigest = append([]byte(nil), request.WriterFenceDigest...)
	clone.NodeCleanupDigest = append([]byte(nil), request.NodeCleanupDigest...)
	f.completes = append(f.completes, clone)
	f.store.grant.State = sandboxstore.RootFSWriterGrantStateRetired
	f.store.grant.RetireOperationID = request.OperationID
	f.store.grant.RetireKind = sandboxstore.RootFSWriterRetireKindCrashAbandon
	f.store.grant.RetireProofDigest = append([]byte(nil), request.NodeCleanupDigest...)
	if f.completeLost {
		f.completeLost = false
		return WriterFinalizeProof{}, errors.New("writer completion response lost")
	}
	return WriterFinalizeProof{
		OperationID: request.OperationID, GrantID: request.GrantID,
		State: sandboxstore.RootFSWriterGrantStateRetired, ProofDigest: bytes.Repeat([]byte{0x51}, 32),
	}, nil
}

type reconcileFixture struct {
	reconciler *Reconciler
	store      *fakeStore
	allocation *fakeAllocation
	node       *fakeNode
	writer     *fakeWriter
	order      *[]string
}

func newReconcileFixture(t *testing.T, claimed bool) *reconcileFixture {
	t.Helper()
	now := time.Now().UTC()
	order := &[]string{}
	slot := &sandboxstore.RuntimeSlot{
		ID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
		AllocationNamespace: "default", NodeID: "nomad-node-1", NodeUID: "node-uid-1",
		NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2", State: sandboxstore.RuntimeSlotStateFastpathReady,
		Revision: 7, HeartbeatExpiresAt: now.Add(-time.Second), AuthorityObservedAt: now,
	}
	var grant *sandboxstore.RootFSWriterGrant
	if claimed {
		slot.State = sandboxstore.RuntimeSlotStateActive
		slot.ClaimOperationID = "claim-operation-1"
		slot.ClaimID = "claim-1"
		slot.SandboxID = "sandbox-1"
		slot.FilesystemID = "filesystem-1"
		slot.SourceGenerationID = "generation-1"
		slot.WriterGrantID = "grant-1"
		slot.RunscContainerID = "runsc-1"
		grant = &sandboxstore.RootFSWriterGrant{
			ID: "grant-1", SlotID: slot.ID, SandboxID: slot.SandboxID, ClaimID: slot.ClaimID,
			FilesystemID: slot.FilesystemID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
			IssueOperationID: "issue-operation-1", WriterEpoch: 8,
			State:          sandboxstore.RootFSWriterGrantStateConsumed,
			BindingVersion: sandboxstore.RootFSWriterBindingVersion,
			BindingDigest:  bytes.Repeat([]byte{0x61}, 32), InitialGenerationID: slot.SourceGenerationID,
		}
	}
	store := &fakeStore{slot: slot, grant: grant, order: order}
	target := allocationTarget(slot)
	allocation := &fakeAllocation{target: target, present: true, order: order}
	node := &fakeNode{order: order}
	writer := &fakeWriter{store: store, order: order}
	reconciler, err := New(Config{Store: store, Allocation: allocation, Node: node, Writer: writer, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	return &reconcileFixture{
		reconciler: reconciler, store: store, allocation: allocation,
		node: node, writer: writer, order: order,
	}
}

func attachResourceLease(t *testing.T, fixture *reconcileFixture) protocol.RuntimeResourceLease {
	t.Helper()
	slot := fixture.store.slot
	lease, err := protocol.NewRuntimeResourceLease(
		slot.ClaimOperationID, slot.ClaimID, slot.ID, slot.ClusterID,
		slot.NodeID, slot.NodeUID, slot.NodeBootID,
		protocol.RuntimeResourceRequest{
			Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1_500,
			MemoryBytes: 768 << 20, PIDsLimit: protocol.DefaultRuntimePIDsLimit,
		},
		"0-1", "0",
	)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := lease.Digest()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := protocol.DecodeProof("resource_lease_digest", strings.TrimPrefix(digest, "sha256:"))
	if err != nil {
		t.Fatal(err)
	}
	slot.ResourceLease = lease
	slot.ResourceLeaseDigest = decoded
	slot.ResourceLeaseState = sandboxstore.RuntimeResourceLeaseActive
	return lease
}

func TestReconcilerFencesCleansRetiresPurgesAndFinalizesClaim(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	result, err := fixture.reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result != (Result{Candidates: 1, Completed: 1}) {
		t.Fatalf("result = %+v", result)
	}
	if fixture.store.slot.State != sandboxstore.RuntimeSlotStateTerminal ||
		fixture.store.slot.TerminalReason != "reconciled_orphan" || len(fixture.store.terminalProof) != 32 {
		t.Fatalf("terminal slot = %+v", fixture.store.slot)
	}
	if fixture.store.grant.State != sandboxstore.RootFSWriterGrantStateRetired {
		t.Fatalf("writer state = %s", fixture.store.grant.State)
	}
	wantOrder := []string{
		"list", "get-slot", "fence", "observe-allocation", "get-grant", "fence-writer", "get-grant",
		"cleanup-node", "complete-writer", "get-grant", "purge-allocation",
		"observe-allocation", "mark-missing", "finalize-slot",
	}
	if !reflect.DeepEqual(*fixture.order, wantOrder) {
		t.Fatalf("operation order = %v, want %v", *fixture.order, wantOrder)
	}
	if len(fixture.node.requests) != 1 || len(fixture.writer.fences) != 1 ||
		len(fixture.writer.completes) != 1 || len(fixture.allocation.purges) != 1 {
		t.Fatalf("calls = cleanup %d fence %d complete %d purge %d", len(fixture.node.requests),
			len(fixture.writer.fences), len(fixture.writer.completes), len(fixture.allocation.purges))
	}
	if !bytes.Equal(fixture.node.requests[0].WriterAuthorityDigest, bytes.Repeat([]byte{0x50}, 32)) ||
		fixture.node.requests[0].WriterOperationID != fixture.writer.fences[0].OperationID ||
		fixture.node.requests[0].WriterRetireKind != protocol.WriterRetireKindCrashAbandon ||
		!bytes.Equal(fixture.writer.completes[0].NodeCleanupDigest, bytes.Repeat([]byte{0x41}, 32)) {
		t.Fatalf("fence/cleanup binding = node %x writer %x", fixture.node.requests[0].WriterAuthorityDigest,
			fixture.writer.completes[0].NodeCleanupDigest)
	}
}

func TestReconcilerReleasesExactResourceLeaseOnlyAfterCgroupCleanupAndPurge(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	lease := attachResourceLease(t, fixture)

	result, err := fixture.reconciler.RunOnce(t.Context())
	if err != nil || result.Completed != 1 {
		t.Fatalf("RunOnce() = %+v, %v", result, err)
	}
	if len(fixture.node.requests) != 1 || fixture.node.requests[0].Resources != lease ||
		!bytes.Equal(fixture.node.requests[0].ResourceLeaseDigest, fixture.store.slot.ResourceLeaseDigest) {
		t.Fatalf("resource cleanup request = %+v", fixture.node.requests)
	}
	if fixture.store.slot.ResourceLeaseState != sandboxstore.RuntimeResourceLeaseReleased ||
		fixture.store.slot.ResourceLeaseReleasedAt.IsZero() {
		t.Fatalf("resource lease was not released after terminal cleanup: %+v", fixture.store.slot)
	}
	cleanupIndex := indexOf(*fixture.order, "cleanup-node")
	purgeIndex := indexOf(*fixture.order, "purge-allocation")
	finalizeIndex := indexOf(*fixture.order, "finalize-slot")
	if cleanupIndex < 0 || purgeIndex <= cleanupIndex || finalizeIndex <= purgeIndex {
		t.Fatalf("unsafe resource release order = %v", *fixture.order)
	}
}

func TestReconcilerRejectsResourceCleanupProofWithoutExactCgroupAbsence(t *testing.T) {
	tests := map[string]func(*NodeCleanupProof){
		"cgroup remains": func(proof *NodeCleanupProof) { proof.ResourceCgroupAbsent = false },
		"lease digest changed": func(proof *NodeCleanupProof) {
			proof.ResourceLeaseDigest = bytes.Repeat([]byte{0xff}, 32)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newReconcileFixture(t, true)
			attachResourceLease(t, fixture)
			fixture.node.mutate = mutate

			result, err := fixture.reconciler.RunOnce(t.Context())
			if err == nil || result.Failed != 1 {
				t.Fatalf("RunOnce() = %+v, %v", result, err)
			}
			if len(fixture.writer.completes) != 0 || len(fixture.allocation.purges) != 0 ||
				fixture.store.markCalls != 0 || fixture.store.finalizeCalls != 0 ||
				fixture.store.slot.ResourceLeaseState != sandboxstore.RuntimeResourceLeaseActive {
				t.Fatalf("invalid resource cleanup proof advanced teardown")
			}
		})
	}
}

func TestReconcilerCleansPlannedRetiredWriterWithoutReplacingItsAuthority(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	fixture.store.slot.State = sandboxstore.RuntimeSlotStateQuiescing
	fixture.store.slot.HeartbeatExpiresAt = fixture.store.slot.AuthorityObservedAt.Add(time.Minute)
	fixture.store.grant.State = sandboxstore.RootFSWriterGrantStateRetired
	fixture.store.grant.RetireOperationID = "planned-retire-1"
	fixture.store.grant.RetireKind = sandboxstore.RootFSWriterRetireKindPlannedPublish
	fixture.store.grant.RetireProofDigest = bytes.Repeat([]byte{0x71}, 32)

	result, err := fixture.reconciler.RunOnce(t.Context())
	if err != nil || result.Completed != 1 {
		t.Fatalf("RunOnce() = %+v, %v", result, err)
	}
	if len(fixture.writer.fences) != 0 || len(fixture.writer.completes) != 0 {
		t.Fatalf("planned retirement was replaced: fences=%+v completes=%+v", fixture.writer.fences, fixture.writer.completes)
	}
	if len(fixture.node.requests) != 1 {
		t.Fatalf("node cleanup calls = %+v", fixture.node.requests)
	}
	request := fixture.node.requests[0]
	if request.WriterOperationID != fixture.store.grant.RetireOperationID ||
		request.WriterRetireKind != protocol.WriterRetireKindPlannedPublish ||
		!bytes.Equal(request.WriterAuthorityDigest, fixture.store.grant.RetireProofDigest) {
		t.Fatalf("planned node cleanup = %+v", request)
	}
	wantOrder := []string{
		"list", "get-slot", "observe-allocation", "get-grant", "cleanup-node", "get-grant",
		"purge-allocation", "observe-allocation", "mark-missing", "finalize-slot",
	}
	if !reflect.DeepEqual(*fixture.order, wantOrder) {
		t.Fatalf("operation order = %v, want %v", *fixture.order, wantOrder)
	}
}

func TestReconcilerRecoversPurgeResponseLossWithExactOperations(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	fixture.allocation.purgeLost = true
	first, err := fixture.reconciler.RunOnce(context.Background())
	if err == nil || first.Failed != 1 {
		t.Fatalf("first RunOnce() = %+v, %v", first, err)
	}
	second, err := fixture.reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("retry RunOnce() error = %v", err)
	}
	if second.Completed != 1 || fixture.store.slot.State != sandboxstore.RuntimeSlotStateTerminal {
		t.Fatalf("retry result = %+v slot = %+v", second, fixture.store.slot)
	}
	if len(fixture.node.requests) != 2 || !reflect.DeepEqual(fixture.node.requests[0], fixture.node.requests[1]) {
		t.Fatalf("cleanup retries changed: %+v", fixture.node.requests)
	}
	if len(fixture.writer.fences) != 2 || !reflect.DeepEqual(fixture.writer.fences[0], fixture.writer.fences[1]) {
		t.Fatalf("writer fence retries changed: %+v", fixture.writer.fences)
	}
	if len(fixture.writer.completes) != 2 || !reflect.DeepEqual(fixture.writer.completes[0], fixture.writer.completes[1]) {
		t.Fatalf("writer completion retries changed: %+v", fixture.writer.completes)
	}
	if len(fixture.allocation.purges) != 1 {
		t.Fatalf("purge calls = %d, want one accepted call", len(fixture.allocation.purges))
	}
}

func TestReconcilerRecoversWriterFenceResponseLossBeforeNodeCleanup(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	fixture.writer.fenceLost = true
	first, err := fixture.reconciler.RunOnce(context.Background())
	if err == nil || first.Failed != 1 {
		t.Fatalf("first RunOnce() = %+v, %v", first, err)
	}
	if len(fixture.node.requests) != 0 || fixture.store.grant.State != sandboxstore.RootFSWriterGrantStateRetiring {
		t.Fatalf("cleanup ran before durable writer fence: cleanup %d state %s", len(fixture.node.requests), fixture.store.grant.State)
	}
	second, err := fixture.reconciler.RunOnce(context.Background())
	if err != nil || second.Completed != 1 {
		t.Fatalf("retry RunOnce() = %+v, %v", second, err)
	}
	if len(fixture.writer.fences) != 2 || !reflect.DeepEqual(fixture.writer.fences[0], fixture.writer.fences[1]) {
		t.Fatalf("writer fence retries changed: %+v", fixture.writer.fences)
	}
	if len(fixture.node.requests) != 1 || fixture.store.slot.State != sandboxstore.RuntimeSlotStateTerminal {
		t.Fatalf("retry cleanup/terminal = %d %+v", len(fixture.node.requests), fixture.store.slot)
	}
}

func TestReconcilerRecoversNodeCleanupResponseLossWithExactRequest(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	fixture.node.cleanupLost = true
	first, err := fixture.reconciler.RunOnce(context.Background())
	if err == nil || first.Failed != 1 {
		t.Fatalf("first RunOnce() = %+v, %v", first, err)
	}
	if len(fixture.writer.completes) != 0 || len(fixture.allocation.purges) != 0 {
		t.Fatalf("teardown advanced after lost cleanup response")
	}
	second, err := fixture.reconciler.RunOnce(context.Background())
	if err != nil || second.Completed != 1 {
		t.Fatalf("retry RunOnce() = %+v, %v", second, err)
	}
	if len(fixture.node.requests) != 2 || !reflect.DeepEqual(fixture.node.requests[0], fixture.node.requests[1]) {
		t.Fatalf("cleanup retries changed: %+v", fixture.node.requests)
	}
}

func TestReconcilerRecoversWriterCompletionResponseLossBeforePurge(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	fixture.writer.completeLost = true
	first, err := fixture.reconciler.RunOnce(context.Background())
	if err == nil || first.Failed != 1 {
		t.Fatalf("first RunOnce() = %+v, %v", first, err)
	}
	if len(fixture.allocation.purges) != 0 || fixture.store.grant.State != sandboxstore.RootFSWriterGrantStateRetired {
		t.Fatalf("purge advanced before durable writer completion response")
	}
	second, err := fixture.reconciler.RunOnce(context.Background())
	if err != nil || second.Completed != 1 {
		t.Fatalf("retry RunOnce() = %+v, %v", second, err)
	}
	if len(fixture.writer.completes) != 2 || !reflect.DeepEqual(fixture.writer.completes[0], fixture.writer.completes[1]) {
		t.Fatalf("writer completion retries changed: %+v", fixture.writer.completes)
	}
}

func TestReconcilerCleansUnclaimedSlotBeforePersistingTerminal(t *testing.T) {
	fixture := newReconcileFixture(t, false)
	result, err := fixture.reconciler.RunOnce(context.Background())
	if err != nil || result.Completed != 1 {
		t.Fatalf("RunOnce() = %+v, %v", result, err)
	}
	if fixture.store.fenceCalls != 0 || len(fixture.writer.fences) != 0 ||
		len(fixture.writer.completes) != 0 || fixture.store.finalizeCalls != 0 {
		t.Fatalf("unexpected claimed cleanup calls: fence %d writer %d/%d finalize %d", fixture.store.fenceCalls,
			len(fixture.writer.fences), len(fixture.writer.completes), fixture.store.finalizeCalls)
	}
	if fixture.store.slot.State != sandboxstore.RuntimeSlotStateTerminal || fixture.store.slot.TerminalReason != "allocation_missing" {
		t.Fatalf("terminal slot = %+v", fixture.store.slot)
	}
	cleanupIndex := indexOf(*fixture.order, "cleanup-node")
	purgeIndex := indexOf(*fixture.order, "purge-allocation")
	terminalIndex := indexOf(*fixture.order, "mark-missing")
	if cleanupIndex < 0 || purgeIndex <= cleanupIndex || terminalIndex <= purgeIndex {
		t.Fatalf("unsafe operation order = %v", *fixture.order)
	}
}

func TestReconcilerFinalizesGrantlessClaimOnlyAfterPurge(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	fixture.store.slot.State = sandboxstore.RuntimeSlotStateClaiming
	fixture.store.slot.WriterGrantID = ""
	fixture.store.slot.RunscContainerID = ""
	fixture.store.slot.ClaimLeaseExpiresAt = fixture.store.slot.AuthorityObservedAt.Add(-time.Second)
	fixture.store.grant = nil

	result, err := fixture.reconciler.RunOnce(context.Background())
	if err != nil || result.Completed != 1 {
		t.Fatalf("RunOnce() = %+v, %v", result, err)
	}
	if fixture.store.slot.State != sandboxstore.RuntimeSlotStateTerminal ||
		fixture.store.slot.TerminalReason != "prelaunch_abort" {
		t.Fatalf("terminal slot = %+v", fixture.store.slot)
	}
	if len(fixture.writer.fences) != 0 || len(fixture.writer.completes) != 0 {
		t.Fatalf("grantless claim called writer controller: %+v %+v", fixture.writer.fences, fixture.writer.completes)
	}
	if len(fixture.node.requests) != 1 || fixture.node.requests[0].WriterOperationID != "" ||
		fixture.node.requests[0].RunscContainerID != protocol.NomadRunscContainerID(fixture.store.slot.ID) {
		t.Fatalf("grantless cleanup writer operation = %+v", fixture.node.requests)
	}
	purgeIndex := indexOf(*fixture.order, "purge-allocation")
	missingIndex := indexOf(*fixture.order, "mark-missing")
	finalizeIndex := indexOf(*fixture.order, "finalize-slot")
	if purgeIndex < 0 || missingIndex <= purgeIndex || finalizeIndex <= missingIndex {
		t.Fatalf("unsafe grantless terminal order = %v", *fixture.order)
	}
}

func TestReconcilerRejectsChangedCleanupProofBeforeWriterCompletionOrPurge(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	fixture.node.mutate = func(proof *NodeCleanupProof) { proof.NodeUID = "another-node" }
	result, err := fixture.reconciler.RunOnce(context.Background())
	if err == nil || result.Failed != 1 {
		t.Fatalf("RunOnce() = %+v, %v", result, err)
	}
	if len(fixture.writer.fences) != 1 || len(fixture.writer.completes) != 0 ||
		len(fixture.allocation.purges) != 0 || fixture.store.markCalls != 0 {
		t.Fatalf("invalid cleanup proof advanced teardown")
	}
}

func TestReconcilerRejectsChangedWriterFenceProofBeforeNodeCleanup(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	fixture.writer.mutate = func(proof *WriterFenceProof) { proof.GrantID = "another-grant" }
	result, err := fixture.reconciler.RunOnce(context.Background())
	if err == nil || result.Failed != 1 {
		t.Fatalf("RunOnce() = %+v, %v", result, err)
	}
	if len(fixture.node.requests) != 0 || len(fixture.writer.completes) != 0 ||
		len(fixture.allocation.purges) != 0 || fixture.store.markCalls != 0 {
		t.Fatalf("invalid writer fence proof advanced teardown")
	}
}

func TestReconcilerSkipsCandidateWhoseLivenessRecovered(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	fixture.store.slot.HeartbeatExpiresAt = fixture.store.slot.AuthorityObservedAt.Add(time.Minute)
	result, err := fixture.reconciler.RunOnce(context.Background())
	if err != nil || result.Skipped != 1 {
		t.Fatalf("RunOnce() = %+v, %v", result, err)
	}
	if fixture.store.fenceCalls != 0 || len(fixture.node.requests) != 0 {
		t.Fatalf("recovered slot was fenced or cleaned")
	}
}

func indexOf(values []string, target string) int {
	for index, value := range values {
		if value == target {
			return index
		}
	}
	return -1
}
