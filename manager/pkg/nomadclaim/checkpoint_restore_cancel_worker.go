package nomadclaim

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"go.uber.org/zap"
)

type CheckpointRestoreCancellationStore interface {
	ListNomadCheckpointRestoreCancellations(context.Context, string, int) ([]sandboxstore.NomadCheckpointRestoreCancellationWork, error)
	AbortNomadSandboxResume(context.Context, string, string, string) (bool, error)
	GetNomadCheckpointRestoreCancellation(context.Context, string) (*protocol.CheckpointImageCancelRequest, *protocol.CheckpointImageCancelProof, error)
	CommitNomadCheckpointRestoreCancellation(context.Context, string, protocol.CheckpointImageCancelRequest, protocol.CheckpointImageCancelProof) error
}

type checkpointImageCanceler interface {
	CancelCheckpointImage(context.Context, protocol.CheckpointImageCancelRequest) (*protocol.CheckpointImageCancelProof, error)
}

// The worklist keeps a fixed bound even while a node transfer is hung. Its
// persisted request survives manager restarts and the node command is idempotent.
type CheckpointRestoreCancellationWorker struct {
	store  CheckpointRestoreCancellationStore
	node   checkpointImageCanceler
	logger *zap.Logger
	mu     sync.Mutex
	after  string
}

func NewCheckpointRestoreCancellationWorker(store CheckpointRestoreCancellationStore, node checkpointImageCanceler, logger *zap.Logger) (*CheckpointRestoreCancellationWorker, error) {
	if store == nil || node == nil {
		return nil, errors.New("checkpoint cancellation store and node are required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &CheckpointRestoreCancellationWorker{store: store, node: node, logger: logger}, nil
}

func (w *CheckpointRestoreCancellationWorker) RunOnce(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	const batch = 32
	items, err := w.store.ListNomadCheckpointRestoreCancellations(ctx, w.after, batch)
	if err != nil {
		return err
	}
	if len(items) > batch {
		return errors.New("checkpoint cancellation scan exceeded bound")
	}
	previous := w.after
	for _, item := range items {
		if item.OperationID <= previous || item.SandboxID == "" {
			return errors.New("checkpoint cancellation scan did not advance")
		}
		previous = item.OperationID
	}
	jobs := make(chan sandboxstore.NomadCheckpointRestoreCancellationWork)
	var wg sync.WaitGroup
	var failureMu sync.Mutex
	var failures []error
	for range min(4, len(items)) {
		wg.Go(func() {
			for item := range jobs {
				attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
				err := w.process(attempt, item)
				cancel()
				if err != nil {
					failureMu.Lock()
					failures = append(failures, fmt.Errorf("cancel checkpoint restore %s: %w", item.OperationID, err))
					failureMu.Unlock()
				}
			}
		})
	}
	for _, item := range items {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return errors.Join(append(failures, ctx.Err())...)
		case jobs <- item:
			w.after = item.OperationID
		}
	}
	close(jobs)
	wg.Wait()
	if len(items) < batch {
		w.after = ""
	}
	return errors.Join(failures...)
}

func (w *CheckpointRestoreCancellationWorker) process(ctx context.Context, item sandboxstore.NomadCheckpointRestoreCancellationWork) error {
	const reason = "memory restore canceled before execution; target physically reclaimed"
	if _, err := w.store.AbortNomadSandboxResume(ctx, item.SandboxID, item.OperationID, reason); err != nil {
		return err
	}
	request, proof, err := w.store.GetNomadCheckpointRestoreCancellation(ctx, item.OperationID)
	if err != nil {
		return err
	}
	if request == nil {
		return nil
	}
	if proof == nil {
		proof, err = w.node.CancelCheckpointImage(ctx, *request)
		if err != nil {
			return err
		}
		if proof == nil || proof.ValidateFor(*request) != nil {
			return errors.New("node did not prove exact checkpoint image absence")
		}
		if err := w.store.CommitNomadCheckpointRestoreCancellation(ctx, item.OperationID, *request, *proof); err != nil {
			return err
		}
	}
	_, err = w.store.AbortNomadSandboxResume(ctx, item.SandboxID, item.OperationID, reason)
	return err
}

func (w *CheckpointRestoreCancellationWorker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
			w.logger.Warn("Memory restore cancellation pass failed", zap.Error(err))
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return ctx.Err()
}
