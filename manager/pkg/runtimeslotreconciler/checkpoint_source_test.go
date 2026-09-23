package runtimeslotreconciler

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

// Embedded migration methods deliberately panic if checkpoint authority falls
// through into a different lifecycle's cleanup. The store may identify a memory
// operation before its first finalization receipt has been committed.
type checkpointSourceSelectionStore struct {
	*fakeStore
	migrationCompletionStore
	checkpointCompletionStore
	known                           bool
	lookupErr                       error
	authorizeErr                    error
	checkpointCalls, migrationCalls int
}

func (s *checkpointSourceSelectionStore) GetNomadCheckpointSourceFinalization(context.Context, string) (*protocol.MigrationSourceFinalizationReceipt, bool, error) {
	return nil, s.known, s.lookupErr
}
func (s *checkpointSourceSelectionStore) AuthorizeNomadCheckpointSourceFinalization(context.Context, string) (*protocol.MigrationSourceFinalizeRequest, error) {
	s.checkpointCalls++
	return nil, s.authorizeErr
}
func (s *checkpointSourceSelectionStore) GetNomadSandboxMigrationSourceFinalization(context.Context, string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	s.migrationCalls++
	return nil, s.authorizeErr
}

func TestCheckpointSourceReconciliationCannotFallBackToMigrationCleanup(t *testing.T) {
	for _, mode := range []string{"checkpoint-pending", "checkpoint-unavailable", "lookup-unavailable", "migration"} {
		t.Run(mode, func(t *testing.T) {
			f := newReconcileFixture(t, true)
			unavailable := errors.New("regional authority unavailable")
			s := &checkpointSourceSelectionStore{fakeStore: f.store, known: true, authorizeErr: unavailable}
			switch mode {
			case "checkpoint-pending":
				s.authorizeErr = sandboxstore.ErrNomadSandboxMigrationNotReady
			case "lookup-unavailable":
				s.lookupErr = unavailable
			case "migration":
				s.known = false
			}
			f.reconciler.store = s
			f.store.grant.RetireOperationID = "captured-source"
			f.store.grant.RetireProofDigest = make([]byte, 32)
			done, err := f.reconciler.reconcileMigrationSource(t.Context(), f.store.slot, f.store.grant)
			require.False(t, done)
			if mode == "checkpoint-pending" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, unavailable)
			}
			switch mode {
			case "lookup-unavailable":
				require.Zero(t, s.checkpointCalls)
				require.Zero(t, s.migrationCalls)
			case "migration":
				require.Zero(t, s.checkpointCalls)
				require.Equal(t, 1, s.migrationCalls)
			default:
				require.Equal(t, 1, s.checkpointCalls)
				require.Zero(t, s.migrationCalls)
			}
			require.Zero(t, f.store.markCalls)
			require.Zero(t, f.store.finalizeCalls)
		})
	}
}
