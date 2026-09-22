package sandboxstore

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type captureFailureTestNode struct {
	t                        *testing.T
	lost                     string
	cleanupCalls, finalCalls int
}

func (n *captureFailureTestNode) CleanupFailedMigrationCapture(_ context.Context, request protocol.MigrationCaptureFailureRequest) (*protocol.MigrationCaptureFailureProof, error) {
	n.cleanupCalls++
	if n.lost == "cleanup-node" {
		n.lost = ""
		return nil, errors.New("lost cleanup reply")
	}
	d, err := request.Digest()
	require.NoError(n.t, err)
	return &protocol.MigrationCaptureFailureProof{RequestDigest: d, Cleanup: migrationNodeCleanupStoreProof(n.t, request.Cleanup)}, nil
}

func (n *captureFailureTestNode) FinalizeFailedMigrationCapture(_ context.Context, request protocol.MigrationCaptureFailureFinalizeRequest) (*protocol.MigrationCaptureFailureFinalizeProof, error) {
	n.finalCalls++
	if n.lost == "final-node" {
		n.lost = ""
		return nil, errors.New("lost finalization reply")
	}
	d, err := request.Digest()
	require.NoError(n.t, err)
	return &protocol.MigrationCaptureFailureFinalizeProof{RequestDigest: d, RootFSArtifactsAbsent: true, ImageAbsent: true}, nil
}

type captureFailureTestStore struct {
	*PGSandboxStore
	lost string
}

func (s *captureFailureTestStore) CommitNomadSandboxMigrationCaptureFailureCleanup(ctx context.Context, r protocol.MigrationCaptureFailureRequest, p protocol.MigrationCaptureFailureProof) error {
	if err := s.PGSandboxStore.CommitNomadSandboxMigrationCaptureFailureCleanup(ctx, r, p); err != nil {
		return err
	}
	if s.lost == "cleanup-store" {
		s.lost = ""
		return errors.New("lost cleanup commit reply")
	}
	return nil
}

func (s *captureFailureTestStore) CommitNomadSandboxMigrationCaptureFailureFinalization(ctx context.Context, r protocol.MigrationCaptureFailureFinalizeRequest, p protocol.MigrationCaptureFailureFinalizeProof) error {
	if err := s.PGSandboxStore.CommitNomadSandboxMigrationCaptureFailureFinalization(ctx, r, p); err != nil {
		return err
	}
	if s.lost == "final-store" {
		s.lost = ""
		return errors.New("lost finalization commit reply")
	}
	return nil
}

func TestNomadMigrationCaptureFailureWorkersRecoverLostRepliesIntegration(t *testing.T) {
	for _, boundary := range []string{"cleanup-node", "cleanup-store", "final-node", "final-store"} {
		t.Run(boundary, func(t *testing.T) {
			f, _, publication := migrationPublicationStoreFixture(t, "capture-worker-"+boundary)
			ageMigrationCPUPreflight(t, f, publication.Assignment.OperationID)
			source := &sourceRecoveryTestNode{t: t, request: publication, outcome: "uncertain"}
			worker, err := nomadmigration.NewSourceRecovery(f.store, source)
			require.NoError(t, err)
			result, err := worker.RunOnce(f.ctx)
			require.NoError(t, err)
			require.Equal(t, 1, result.Advanced)
			store := &captureFailureTestStore{PGSandboxStore: f.store, lost: boundary}
			node := &captureFailureTestNode{t: t, lost: boundary}
			failures := 0
			for range 4 {
				worker, err := nomadmigration.NewCaptureFailure(store, node)
				require.NoError(t, err)
				_, err = worker.RunOnce(f.ctx)
				if err != nil {
					failures++
				}
				store.PGSandboxStore = NewPGSandboxStore(f.pool)
			}
			require.Equal(t, 1, failures)
			require.Equal(t, 1, source.calls, "failure recovery cannot capture again")
			failed, receipt, err := f.store.GetNomadMigrationCaptureFailureForSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.True(t, failed)
			require.NotNil(t, receipt)
			require.NoError(t, receipt.Validate())
			pending, err := f.store.ListNomadMigrationCaptureFailures(f.ctx, "", 8)
			require.NoError(t, err)
			require.Empty(t, pending)
			slot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.Equal(t, RuntimeResourceLeaseActive, slot.ResourceLeaseState, "artifact receipts alone cannot release source capacity")
		})
	}
}
