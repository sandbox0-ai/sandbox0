package nomadclaim

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type checkpointCancelStore struct {
	work      []sandboxstore.NomadCheckpointRestoreCancellationWork
	request   protocol.CheckpointImageCancelRequest
	proof     *protocol.CheckpointImageCancelProof
	steps     []string
	completed bool
}

func (s *checkpointCancelStore) ListNomadCheckpointRestoreCancellations(_ context.Context, after string, _ int) ([]sandboxstore.NomadCheckpointRestoreCancellationWork, error) {
	if after != "" {
		return nil, nil
	}
	return s.work, nil
}

func (s *checkpointCancelStore) AbortNomadSandboxResume(context.Context, string, string, string) (bool, error) {
	if s.proof == nil {
		s.steps = append(s.steps, "persist")
	} else {
		s.steps = append(s.steps, "finish")
		s.completed = true
	}
	return true, nil
}

func (s *checkpointCancelStore) GetNomadCheckpointRestoreCancellation(context.Context, string) (*protocol.CheckpointImageCancelRequest, *protocol.CheckpointImageCancelProof, error) {
	s.steps = append(s.steps, "read")
	return &s.request, s.proof, nil
}

func (s *checkpointCancelStore) CommitNomadCheckpointRestoreCancellation(_ context.Context, _ string, request protocol.CheckpointImageCancelRequest, proof protocol.CheckpointImageCancelProof) error {
	s.steps = append(s.steps, "commit")
	if proof.ValidateFor(request) != nil {
		return errors.New("invalid proof")
	}
	s.proof = &proof
	return nil
}

type checkpointCancelNode struct {
	store *checkpointCancelStore
	proof protocol.CheckpointImageCancelProof
}

func (n checkpointCancelNode) CancelCheckpointImage(_ context.Context, _ protocol.CheckpointImageCancelRequest) (*protocol.CheckpointImageCancelProof, error) {
	n.store.steps = append(n.store.steps, "node")
	return &n.proof, nil
}

func TestCheckpointRestoreCancellationWorkerPreservesProofOrder(t *testing.T) {
	store := &checkpointCancelStore{work: []sandboxstore.NomadCheckpointRestoreCancellationWork{{OperationID: "op-1", SandboxID: "sb-1"}}}
	// The worker must reject a malformed node response before committing it.
	worker, err := NewCheckpointRestoreCancellationWorker(store, checkpointCancelNode{store: store}, nil)
	require.NoError(t, err)
	err = worker.RunOnce(context.Background())
	require.ErrorContains(t, err, "node did not prove")
	require.Equal(t, []string{"persist", "read", "node"}, store.steps)
	require.False(t, store.completed)
}

func TestCheckpointRestoreCancellationWorkerFinishesStoredProofAfterRestart(t *testing.T) {
	store := &checkpointCancelStore{work: []sandboxstore.NomadCheckpointRestoreCancellationWork{{OperationID: "op-2", SandboxID: "sb-2"}},
		proof: &protocol.CheckpointImageCancelProof{ImageAbsent: true}}
	worker, err := NewCheckpointRestoreCancellationWorker(store, checkpointCancelNode{store: store}, nil)
	require.NoError(t, err)
	require.NoError(t, worker.RunOnce(context.Background()))
	require.Equal(t, []string{"finish", "read", "finish"}, store.steps)
	require.True(t, store.completed)
}
