package runtimeslotwriter

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

type fakeStore struct {
	grant         *sandboxstore.RootFSWriterGrant
	record        *sandboxstore.SandboxRecord
	lifecycle     *sandboxstore.SandboxLifecycleTxn
	cancelCalls   int
	beginCalls    int
	completeCalls int
	cancelLost    bool
	beginLost     bool
	completeLost  bool
}

func (f *fakeStore) GetRootFSWriterGrant(context.Context, string) (*sandboxstore.RootFSWriterGrant, error) {
	return cloneGrant(f.grant), nil
}

func (f *fakeStore) CancelRootFSWriterGrant(
	_ context.Context,
	request *sandboxstore.CancelRootFSWriterGrantRequest,
) (*sandboxstore.RootFSWriterGrant, error) {
	f.cancelCalls++
	if request.GrantID != f.grant.ID || request.OperationID != f.grant.IssueOperationID ||
		request.WriterEpoch != f.grant.WriterEpoch || !bytes.Equal(request.BindingDigest, f.grant.BindingDigest) {
		return nil, sandboxstore.ErrRootFSWriterGrantConflict
	}
	if f.grant.State == sandboxstore.RootFSWriterGrantStateIssued {
		f.grant.State = sandboxstore.RootFSWriterGrantStateCanceled
	}
	if f.grant.State != sandboxstore.RootFSWriterGrantStateCanceled {
		return nil, sandboxstore.ErrRootFSWriterGrantInvalidState
	}
	if f.cancelLost {
		f.cancelLost = false
		return nil, errors.New("writer cancellation response lost")
	}
	return cloneGrant(f.grant), nil
}

func (f *fakeStore) BeginRootFSWriterCrashAbandon(
	_ context.Context,
	request *sandboxstore.BeginRootFSWriterCrashAbandonRequest,
) (*sandboxstore.RootFSWriterGrant, error) {
	f.beginCalls++
	if request.GrantID != f.grant.ID || request.OperationID == "" || request.WriterEpoch != f.grant.WriterEpoch ||
		!bytes.Equal(request.BindingDigest, f.grant.BindingDigest) || request.NodeUID != f.grant.NodeUID ||
		request.NodeBootID != f.grant.NodeBootID || request.ExpectedOldGenerationID != f.grant.InitialGenerationID {
		return nil, sandboxstore.ErrRootFSWriterGrantConflict
	}
	if f.grant.State == sandboxstore.RootFSWriterGrantStateConsumed {
		f.grant.State = sandboxstore.RootFSWriterGrantStateRetiring
		f.grant.RetireOperationID = request.OperationID
		f.grant.RetireKind = sandboxstore.RootFSWriterRetireKindCrashAbandon
	}
	if f.grant.RetireOperationID != request.OperationID ||
		f.grant.RetireKind != sandboxstore.RootFSWriterRetireKindCrashAbandon ||
		(f.grant.State != sandboxstore.RootFSWriterGrantStateRetiring && f.grant.State != sandboxstore.RootFSWriterGrantStateRetired) {
		return nil, sandboxstore.ErrRootFSWriterGrantConflict
	}
	if f.beginLost {
		f.beginLost = false
		return nil, errors.New("writer fence response lost")
	}
	return cloneGrant(f.grant), nil
}

func (f *fakeStore) WithSandboxLock(
	ctx context.Context,
	sandboxID string,
	fn func(context.Context, sandboxstore.SandboxStoreTx, *sandboxstore.SandboxRecord) error,
) error {
	if sandboxID != f.record.ID {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	return fn(ctx, fakeTx{store: f}, cloneRecord(f.record))
}

type fakeTx struct {
	store *fakeStore
}

func (f fakeTx) GetActiveLifecycleTxn(context.Context, string) (*sandboxstore.SandboxLifecycleTxn, error) {
	if f.store.lifecycle == nil || f.store.lifecycle.Phase == sandboxstore.SandboxLifecyclePhaseAborted ||
		f.store.lifecycle.Phase == sandboxstore.SandboxLifecyclePhaseCommitted {
		return nil, nil
	}
	return cloneLifecycle(f.store.lifecycle), nil
}

func (f fakeTx) BeginLifecycleTxn(_ context.Context, txn *sandboxstore.SandboxLifecycleTxn) error {
	if f.store.lifecycle != nil && f.store.lifecycle.Phase != sandboxstore.SandboxLifecyclePhaseAborted &&
		f.store.lifecycle.Phase != sandboxstore.SandboxLifecyclePhaseCommitted {
		return errors.New("active lifecycle exists")
	}
	f.store.lifecycle = cloneLifecycle(txn)
	return nil
}

func (f fakeTx) CompleteRootFSWriterCrashAbandon(
	_ context.Context,
	request *sandboxstore.CompleteRootFSWriterCrashAbandonRequest,
) (*sandboxstore.RootFSWriterGrant, error) {
	f.store.completeCalls++
	grant := f.store.grant
	if request.GrantID != grant.ID || request.OperationID != grant.RetireOperationID ||
		request.LifecycleTxnID != request.OperationID || request.WriterEpoch != grant.WriterEpoch ||
		!bytes.Equal(request.BindingDigest, grant.BindingDigest) || len(request.ProofDigest) != 32 {
		return nil, sandboxstore.ErrRootFSWriterGrantConflict
	}
	if grant.State == sandboxstore.RootFSWriterGrantStateRetiring {
		grant.State = sandboxstore.RootFSWriterGrantStateRetired
		grant.RetireProofDigest = append([]byte(nil), request.ProofDigest...)
		f.store.lifecycle.Phase = sandboxstore.SandboxLifecyclePhaseAborted
		f.store.record.DesiredState = sandboxstore.SandboxDesiredStatePaused
		f.store.record.CurrentPodNamespace = ""
		f.store.record.CurrentPodName = ""
	}
	if grant.State != sandboxstore.RootFSWriterGrantStateRetired ||
		!bytes.Equal(grant.RetireProofDigest, request.ProofDigest) {
		return nil, sandboxstore.ErrRootFSWriterGrantConflict
	}
	if f.store.completeLost {
		f.store.completeLost = false
		return nil, errors.New("writer completion response lost")
	}
	return cloneGrant(grant), nil
}

func (fakeTx) SaveSandbox(context.Context, *sandboxstore.SandboxRecord) error { return nil }
func (fakeTx) SaveRuntime(context.Context, string, string, string, int64, time.Time, time.Time, sandboxstore.SandboxRuntimeMetadata) error {
	return nil
}
func (fakeTx) MarkHotClaimCompleted(context.Context, string, time.Time) error          { return nil }
func (fakeTx) MarkRuntimePaused(context.Context, string, int64, time.Time) error       { return nil }
func (fakeTx) MarkRuntimeTerminating(context.Context, string) error                    { return nil }
func (fakeTx) SaveRootFSState(context.Context, *sandboxstore.SandboxRootFSState) error { return nil }
func (fakeTx) SetLifecycleTxnRuntime(context.Context, string, string, string) error    { return nil }
func (fakeTx) UpdateLifecycleTxnPhase(context.Context, string, string) error           { return nil }
func (fakeTx) SetLifecycleTxnPreparedHead(context.Context, string, string) error       { return nil }
func (fakeTx) RequestLifecycleTxnCancel(context.Context, string, string) (bool, error) {
	return false, nil
}
func (fakeTx) CommitLifecycleTxn(context.Context, string, string) error { return nil }
func (fakeTx) AbortLifecycleTxn(context.Context, string, string) error  { return nil }

func TestControllerFencesAndCompletesConsumedWriterExactly(t *testing.T) {
	store, controller, fenceRequest := newFixture(t, sandboxstore.RootFSWriterGrantStateConsumed)
	store.beginLost = true
	_, err := controller.Fence(context.Background(), fenceRequest)
	if err == nil || store.grant.State != sandboxstore.RootFSWriterGrantStateRetiring {
		t.Fatalf("first Fence() error = %v, grant = %+v", err, store.grant)
	}
	fence, err := controller.Fence(context.Background(), fenceRequest)
	if err != nil {
		t.Fatalf("retry Fence() error = %v", err)
	}
	if store.beginCalls != 2 || len(fence.ProofDigest) != 32 || store.lifecycle == nil {
		t.Fatalf("fence result = %+v calls = %d lifecycle = %+v", fence, store.beginCalls, store.lifecycle)
	}

	completeRequest := runtimeslotreconciler.WriterCompleteRequest{
		OperationID: fence.OperationID, GrantID: fence.GrantID, SlotID: fenceRequest.SlotID,
		WriterEpoch: fenceRequest.WriterEpoch, WriterFenceDigest: append([]byte(nil), fence.ProofDigest...),
		NodeCleanupDigest: bytes.Repeat([]byte{0x71}, 32),
	}
	store.completeLost = true
	_, err = controller.Complete(context.Background(), completeRequest)
	if err == nil || store.grant.State != sandboxstore.RootFSWriterGrantStateRetired {
		t.Fatalf("first Complete() error = %v, grant = %+v", err, store.grant)
	}
	final, err := controller.Complete(context.Background(), completeRequest)
	if err != nil {
		t.Fatalf("retry Complete() error = %v", err)
	}
	if final.State != sandboxstore.RootFSWriterGrantStateRetired || len(final.ProofDigest) != 32 ||
		store.completeCalls != 2 || store.record.DesiredState != sandboxstore.SandboxDesiredStatePaused {
		t.Fatalf("final = %+v calls = %d record = %+v", final, store.completeCalls, store.record)
	}
	retried, err := controller.Complete(context.Background(), completeRequest)
	if err != nil || !reflect.DeepEqual(final, retried) {
		t.Fatalf("terminal retry = %+v, %v; want %+v", retried, err, final)
	}
}

func TestControllerCancelsIssuedWriterBeforeNodeCleanup(t *testing.T) {
	store, controller, request := newFixture(t, sandboxstore.RootFSWriterGrantStateIssued)
	store.cancelLost = true
	_, err := controller.Fence(context.Background(), request)
	if err == nil || store.grant.State != sandboxstore.RootFSWriterGrantStateCanceled {
		t.Fatalf("first Fence() error = %v, grant = %+v", err, store.grant)
	}
	fence, err := controller.Fence(context.Background(), request)
	if err != nil {
		t.Fatalf("Fence() error = %v", err)
	}
	if store.grant.State != sandboxstore.RootFSWriterGrantStateCanceled || store.cancelCalls != 2 || store.lifecycle != nil {
		t.Fatalf("issued fence = grant %+v calls %d lifecycle %+v", store.grant, store.cancelCalls, store.lifecycle)
	}
	final, err := controller.Complete(context.Background(), runtimeslotreconciler.WriterCompleteRequest{
		OperationID: request.OperationID, GrantID: request.GrantID, SlotID: request.SlotID,
		WriterEpoch: request.WriterEpoch, WriterFenceDigest: fence.ProofDigest,
		NodeCleanupDigest: bytes.Repeat([]byte{0x72}, 32),
	})
	if err != nil || final.State != sandboxstore.RootFSWriterGrantStateCanceled || store.completeCalls != 0 {
		t.Fatalf("Complete() = %+v, %v; complete calls %d", final, err, store.completeCalls)
	}
}

func TestControllerRejectsAnotherRetirementBeforeCreatingLifecycle(t *testing.T) {
	store, controller, request := newFixture(t, sandboxstore.RootFSWriterGrantStateRetiring)
	store.grant.RetireOperationID = "another-retirement"
	store.grant.RetireKind = sandboxstore.RootFSWriterRetireKindPlannedPublish
	_, err := controller.Fence(context.Background(), request)
	if err == nil || store.lifecycle != nil || store.beginCalls != 0 {
		t.Fatalf("Fence() = %v, lifecycle %+v, begin calls %d", err, store.lifecycle, store.beginCalls)
	}
}

func TestControllerAcceptsWriterAlreadyRetiredByIndependentTerminalFlow(t *testing.T) {
	for _, retireKind := range []string{
		sandboxstore.RootFSWriterRetireKindCrashAbandon,
		sandboxstore.RootFSWriterRetireKindPlannedPublish,
		sandboxstore.RootFSWriterRetireKindPrelaunchAbort,
	} {
		t.Run(retireKind, func(t *testing.T) {
			store, controller, request := newFixture(t, sandboxstore.RootFSWriterGrantStateRetired)
			store.grant.RetireOperationID = "independent-terminal-operation"
			store.grant.RetireKind = retireKind
			store.grant.RetireProofDigest = bytes.Repeat([]byte{0x74}, 32)
			fence, err := controller.Fence(context.Background(), request)
			if err != nil {
				t.Fatalf("Fence() error = %v", err)
			}
			final, err := controller.Complete(context.Background(), runtimeslotreconciler.WriterCompleteRequest{
				OperationID: request.OperationID, GrantID: request.GrantID, SlotID: request.SlotID,
				WriterEpoch: request.WriterEpoch, WriterFenceDigest: fence.ProofDigest,
				NodeCleanupDigest: bytes.Repeat([]byte{0x75}, 32),
			})
			if err != nil || final.State != sandboxstore.RootFSWriterGrantStateRetired || store.completeCalls != 0 {
				t.Fatalf("Complete() = %+v, %v; complete calls %d", final, err, store.completeCalls)
			}
		})
	}
}

func TestControllerRejectsAnotherLifecycleAndChangedProof(t *testing.T) {
	store, controller, request := newFixture(t, sandboxstore.RootFSWriterGrantStateConsumed)
	store.lifecycle = &sandboxstore.SandboxLifecycleTxn{
		ID: "another-operation", SandboxID: store.record.ID, Kind: sandboxstore.SandboxLifecycleKindFork,
		Phase: sandboxstore.SandboxLifecyclePhasePublishing, Source: sandboxstore.SandboxLifecycleSourceManual,
	}
	_, err := controller.Fence(context.Background(), request)
	if err == nil || store.beginCalls != 0 || store.grant.State != sandboxstore.RootFSWriterGrantStateConsumed {
		t.Fatalf("Fence() = %v, begin calls %d, state %s", err, store.beginCalls, store.grant.State)
	}

	store.lifecycle = nil
	fence, err := controller.Fence(context.Background(), request)
	if err != nil {
		t.Fatalf("Fence() error = %v", err)
	}
	changed := append([]byte(nil), fence.ProofDigest...)
	changed[0] ^= 0xff
	_, err = controller.Complete(context.Background(), runtimeslotreconciler.WriterCompleteRequest{
		OperationID: request.OperationID, GrantID: request.GrantID, SlotID: request.SlotID,
		WriterEpoch: request.WriterEpoch, WriterFenceDigest: changed,
		NodeCleanupDigest: bytes.Repeat([]byte{0x73}, 32),
	})
	if err == nil || store.completeCalls != 0 {
		t.Fatalf("Complete() accepted changed fence proof: %v", err)
	}
}

func TestControllerRequiresSandboxRuntimeToMatchAllocation(t *testing.T) {
	store, controller, request := newFixture(t, sandboxstore.RootFSWriterGrantStateConsumed)
	store.record.CurrentPodName = "another-allocation"
	_, err := controller.Fence(context.Background(), request)
	if err == nil || store.beginCalls != 0 || store.lifecycle != nil {
		t.Fatalf("Fence() = %v, begin calls %d, lifecycle %+v", err, store.beginCalls, store.lifecycle)
	}
}

func newFixture(
	t *testing.T,
	state string,
) (*fakeStore, *Controller, runtimeslotreconciler.WriterFenceRequest) {
	t.Helper()
	grant := &sandboxstore.RootFSWriterGrant{
		ID: "grant-1", FilesystemID: "filesystem-1", SandboxID: "sandbox-1", ClaimID: "claim-1",
		SlotID: "slot-1", IssueOperationID: "issue-1", WriterEpoch: 9, State: state,
		InitialGenerationID: "generation-1", BindingVersion: sandboxstore.RootFSWriterBindingVersion,
		BindingDigest: bytes.Repeat([]byte{0x61}, 32), NodeUID: "node-1", NodeBootID: "boot-1",
		PodNamespace: "default", PodName: "slot", PodUID: "allocation-1", NodeName: "nomad-node-1",
		RuntimeGeneration: "7",
	}
	store := &fakeStore{
		grant: grant,
		record: &sandboxstore.SandboxRecord{
			ID: grant.SandboxID, DesiredState: sandboxstore.SandboxDesiredStateActive,
			RuntimeGeneration: 7, CurrentPodNamespace: grant.PodNamespace, CurrentPodName: grant.PodUID,
		},
	}
	controller, err := New(store)
	if err != nil {
		t.Fatal(err)
	}
	return store, controller, fenceRequestFromGrant("reconcile-writer-1", grant)
}

func cloneGrant(source *sandboxstore.RootFSWriterGrant) *sandboxstore.RootFSWriterGrant {
	if source == nil {
		return nil
	}
	clone := *source
	clone.BindingDigest = append([]byte(nil), source.BindingDigest...)
	clone.RetireProofDigest = append([]byte(nil), source.RetireProofDigest...)
	return &clone
}

func cloneRecord(source *sandboxstore.SandboxRecord) *sandboxstore.SandboxRecord {
	if source == nil {
		return nil
	}
	clone := *source
	return &clone
}

func cloneLifecycle(source *sandboxstore.SandboxLifecycleTxn) *sandboxstore.SandboxLifecycleTxn {
	if source == nil {
		return nil
	}
	clone := *source
	return &clone
}
