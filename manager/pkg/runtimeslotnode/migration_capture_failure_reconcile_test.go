package runtimeslotnode

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationCaptureFailureReconcileStore struct {
	*migrationReconcileStore
	finalization *protocol.MigrationCaptureFailureReceipt
}

func (s *migrationCaptureFailureReconcileStore) GetNomadMigrationCaptureFailureForSlot(context.Context, string) (bool, *protocol.MigrationCaptureFailureReceipt, error) {
	return true, s.finalization, nil
}

func (s *migrationCaptureFailureReconcileStore) CompleteNomadSandboxMigrationCaptureFailure(_ context.Context, operation string, gc protocol.MigrationSourceGCRequest, ack protocol.MigrationSourceGCAcknowledgement) (*sandboxstore.RuntimeSlot, error) {
	*s.order = append(*s.order, "complete-failure")
	if s.completeErr != nil {
		return nil, s.completeErr
	}
	if s.finalization == nil || operation != s.finalization.Request.Request.Capture.Request.OperationID ||
		s.slot.State != sandboxstore.RuntimeSlotStateOrphaned || ack.ValidateFor(gc) != nil ||
		gc.AllocationAbsenceDigest != hex.EncodeToString(s.slot.OrphanObservationDigest) || gc.FinalizationDigest != s.finalization.Proof.RequestDigest {
		return nil, errors.New("missing failed capture evidence")
	}
	s.completed++
	s.slot.State = sandboxstore.RuntimeSlotStateTerminal
	s.slot.TerminalReason = "migration_capture_failed"
	s.slot.ResourceLeaseState = sandboxstore.RuntimeResourceLeaseReleased
	copy := s.slot
	return &copy, nil
}

func migrationCaptureFailureReconcileFixture(t *testing.T) (*runtimeslotreconciler.Reconciler, *migrationCaptureFailureReconcileStore, *migrationReconcileAllocation, *migrationReconcileTransport) {
	t.Helper()
	_, base, allocation, transport := migrationReconcileFixture(t)
	cleanup := migrationCaptureFailureChannelRequest(t)
	executor := &migrationCaptureFailureChannelExecutor{}
	proof, err := executor.CleanupFailedMigrationCapture(t.Context(), cleanup)
	require.NoError(t, err)
	request := protocol.MigrationCaptureFailureFinalizeRequest{Request: cleanup, Proof: *proof}
	finalized, err := executor.FinalizeFailedMigrationCapture(t.Context(), request)
	require.NoError(t, err)
	receipt := &protocol.MigrationCaptureFailureReceipt{Request: request, Proof: *finalized}
	require.NoError(t, receipt.Validate())
	c := cleanup.Cleanup
	base.slot.NetNSIdentity = c.NetNSIdentity
	base.slot.ClaimID, base.slot.ClaimOperationID = c.Resources.ClaimID, c.Resources.OperationID
	base.slot.ResourceLease, base.slot.WriterGrantID = c.Resources, c.WriterGrantID
	base.slot.ResourceLeaseDigest, err = hex.DecodeString(c.ResourceLeaseDigest)
	require.NoError(t, err)
	store := &migrationCaptureFailureReconcileStore{migrationReconcileStore: base, finalization: receipt}
	node, err := New(transport)
	require.NoError(t, err)
	writer := struct {
		runtimeslotreconciler.WriterController
	}{}
	r, err := runtimeslotreconciler.New(runtimeslotreconciler.Config{Store: store, Allocation: allocation, Node: node, Writer: writer, Limit: 1})
	require.NoError(t, err)
	return r, store, allocation, transport
}

func TestMigrationCaptureFailureCompletionWorkerRequiresEveryPhysicalReceipt(t *testing.T) {
	for _, failure := range []string{"pending", "changed-slot", "changed-proof", "allocation", "gc", "malformed-gc", "completion"} {
		t.Run(failure, func(t *testing.T) {
			r, s, a, n := migrationCaptureFailureReconcileFixture(t)
			receipt := s.finalization
			switch failure {
			case "pending":
				s.finalization = nil
			case "changed-slot":
				s.slot.NetNSIdentity = "different-netns"
			case "changed-proof":
				s.finalization.Proof.ImageAbsent = false
			case "allocation":
				a.keepPresent = true
			case "gc":
				n.gcErr = errors.New("node unavailable")
			case "malformed-gc":
				n.gcInvalid = true
			case "completion":
				s.completeErr = errors.New("commit unavailable")
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
			require.Zero(t, n.executor.calls, "ordinary or source cleanup must never run on a failed capture")
			if failure == "pending" || failure == "changed-slot" || failure == "changed-proof" {
				require.Zero(t, a.purges)
			}
			s.finalization = receipt
			s.slot.NetNSIdentity = receipt.Request.Request.Cleanup.NetNSIdentity
			s.finalization.Proof.ImageAbsent = true
			a.keepPresent, n.gcInvalid = false, false
			n.gcErr, s.completeErr = nil, nil
			result, err = r.RunOnce(t.Context())
			require.NoError(t, err)
			require.Equal(t, 1, result.Completed)
			require.Equal(t, "migration_capture_failed", s.slot.TerminalReason)
			require.Equal(t, sandboxstore.RuntimeResourceLeaseReleased, s.slot.ResourceLeaseState)
		})
	}
}

func TestMigrationCaptureFailureCompletionWorkerRetriesLostPurgeAndNodeAcknowledgement(t *testing.T) {
	r, s, a, n := migrationCaptureFailureReconcileFixture(t)
	a.purgeLost, n.gcLost = true, true
	for attempt := 0; attempt < 2; attempt++ {
		result, err := r.RunOnce(t.Context())
		require.Error(t, err)
		require.Zero(t, result.Completed)
		require.Equal(t, sandboxstore.RuntimeResourceLeaseActive, s.slot.ResourceLeaseState)
	}
	result, err := r.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, n.gcRequests[0], n.gcRequests[1], "lost acknowledgement retains the exact allocation absence identity")
	require.Equal(t, []string{"purge", "purge", "observe", "mark-missing", "ack-gc", "purge", "observe", "ack-gc", "complete-failure"}, *s.order)
	result, err = r.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, result.Completed)
}
