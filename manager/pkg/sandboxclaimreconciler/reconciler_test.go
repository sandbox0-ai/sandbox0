package sandboxclaimreconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

type fakeStore struct {
	claims     []sandboxstore.SandboxRuntimeClaim
	candidates map[string]*sandboxstore.SandboxClaimCleanupCandidate
	fenceErr   map[string]error
	deleted    []string
	cleaned    []string
}

func (f *fakeStore) ListSandboxRuntimeClaimsForCleanup(context.Context, int) ([]sandboxstore.SandboxRuntimeClaim, error) {
	return append([]sandboxstore.SandboxRuntimeClaim(nil), f.claims...), nil
}

func (f *fakeStore) FenceSandboxRuntimeClaimForCleanup(_ context.Context, sandboxID, _, _ string) (*sandboxstore.SandboxClaimCleanupCandidate, error) {
	if err := f.fenceErr[sandboxID]; err != nil {
		return nil, err
	}
	return f.candidates[sandboxID], nil
}

func (f *fakeStore) MarkSandboxDeleted(_ context.Context, sandboxID string, _ time.Time) error {
	f.deleted = append(f.deleted, sandboxID)
	return nil
}

func (f *fakeStore) MarkSandboxRuntimeClaimCleaned(_ context.Context, sandboxID, _ string) error {
	f.cleaned = append(f.cleaned, sandboxID)
	return nil
}

func TestWorkerCleansOnlyClaimsWithoutLivePhysicalState(t *testing.T) {
	store := &fakeStore{
		claims: []sandboxstore.SandboxRuntimeClaim{
			{SandboxID: "no-slot", OperationID: "operation-a"},
			{SandboxID: "terminal", OperationID: "operation-b"},
			{SandboxID: "quiescing", OperationID: "operation-c"},
			{SandboxID: "renewed", OperationID: "operation-d"},
			{SandboxID: "missing-required", OperationID: "operation-e"},
		},
		candidates: map[string]*sandboxstore.SandboxClaimCleanupCandidate{
			"no-slot":   {SandboxID: "no-slot", OperationID: "operation-a"},
			"terminal":  {SandboxID: "terminal", OperationID: "operation-b", SlotID: "slot-b", SlotState: sandboxstore.RuntimeSlotStateTerminal},
			"quiescing": {SandboxID: "quiescing", OperationID: "operation-c", SlotID: "slot-c", SlotState: sandboxstore.RuntimeSlotStateQuiescing},
			"renewed":   nil,
			"missing-required": {
				SandboxID: "missing-required", OperationID: "operation-e", PhysicalStateRequired: true,
			},
		},
		fenceErr: make(map[string]error),
	}
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	worker, err := New(Config{Store: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := Result{Scanned: 5, Fenced: 4, Pending: 2, Cleaned: 2, Skipped: 1}
	if result != want {
		t.Fatalf("result = %+v, want %+v", result, want)
	}
	if len(store.deleted) != 2 || store.deleted[0] != "no-slot" || store.deleted[1] != "terminal" ||
		len(store.cleaned) != 2 {
		t.Fatalf("deleted=%v cleaned=%v", store.deleted, store.cleaned)
	}
}

func TestWorkerContinuesAfterIndependentFenceFailure(t *testing.T) {
	store := &fakeStore{
		claims: []sandboxstore.SandboxRuntimeClaim{
			{SandboxID: "failed", OperationID: "operation-a"},
			{SandboxID: "clean", OperationID: "operation-b"},
		},
		candidates: map[string]*sandboxstore.SandboxClaimCleanupCandidate{
			"clean": {SandboxID: "clean", OperationID: "operation-b"},
		},
		fenceErr: map[string]error{"failed": errors.New("database conflict")},
	}
	worker, err := New(Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.RunOnce(context.Background())
	if err == nil || result.Failed != 1 || result.Cleaned != 1 {
		t.Fatalf("RunOnce() = %+v, %v", result, err)
	}
}
