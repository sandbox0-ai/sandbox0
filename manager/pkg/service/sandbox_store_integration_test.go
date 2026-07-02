package service

import (
	"context"
	"testing"
	"time"
)

func TestPGSandboxStoreRuntimeDeletionMatchesCommittedPause(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	ctx := context.Background()

	record := rootFSTestSandboxRecord("sandbox-pause-match", "team-1")
	record.CurrentPodNamespace = "ns-a"
	record.CurrentPodName = "pod-old"
	record.RuntimeGeneration = 7
	if err := store.UpsertSandbox(ctx, record); err != nil {
		t.Fatalf("UpsertSandbox() error = %v", err)
	}

	err := store.WithSandboxLock(ctx, record.ID, func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		txn := &SandboxLifecycleTxn{
			ID:               "txn-pause-match",
			SandboxID:        record.ID,
			Kind:             SandboxLifecycleKindPause,
			Phase:            SandboxLifecyclePhasePreparing,
			FromGeneration:   7,
			FromPodNamespace: "ns-a",
			FromPodName:      "pod-old",
		}
		if err := tx.BeginLifecycleTxn(lockCtx, txn); err != nil {
			return err
		}
		if err := tx.MarkRuntimePaused(lockCtx, record.ID, 7, time.Now().UTC()); err != nil {
			return err
		}
		return tx.CommitLifecycleTxn(lockCtx, txn.ID, "")
	})
	if err != nil {
		t.Fatalf("commit pause txn error = %v", err)
	}

	matched, err := store.RuntimeDeletionMatchesCommittedPause(ctx, record.ID, "ns-a", "pod-old", 7)
	if err != nil {
		t.Fatalf("RuntimeDeletionMatchesCommittedPause() error = %v", err)
	}
	if !matched {
		t.Fatalf("RuntimeDeletionMatchesCommittedPause() = false, want true")
	}

	if err := store.MarkSandboxDeleted(ctx, record.ID, time.Now().UTC()); err != nil {
		t.Fatalf("MarkSandboxDeleted() error = %v", err)
	}
	matched, err = store.RuntimeDeletionMatchesCommittedPause(ctx, record.ID, "ns-a", "pod-old", 7)
	if err != nil {
		t.Fatalf("RuntimeDeletionMatchesCommittedPause() after delete error = %v", err)
	}
	if !matched {
		t.Fatalf("RuntimeDeletionMatchesCommittedPause() after delete = false, want true")
	}

	matched, err = store.RuntimeDeletionMatchesCommittedPause(ctx, record.ID, "ns-a", "pod-new", 8)
	if err != nil {
		t.Fatalf("RuntimeDeletionMatchesCommittedPause() mismatch error = %v", err)
	}
	if matched {
		t.Fatalf("RuntimeDeletionMatchesCommittedPause() mismatch = true, want false")
	}
}
