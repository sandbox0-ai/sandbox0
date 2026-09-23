package nomadclaim

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"go.uber.org/zap"
)

const checkpointResumeBatch = 32

// CheckpointResumeStore discovers accepted operations, never just paused owners.
type CheckpointResumeStore interface {
	ListNomadCheckpointResumes(context.Context, string, int) ([]sandboxstore.NomadCheckpointResumeWork, error)
}

type CheckpointResumer interface {
	ResumeMemorySandboxOperation(context.Context, string, string) error
}

// ResumeMemorySandboxOperation is recovery of a previously accepted operation.
// Missing or superseded work cannot reserve new quota or cold-start an owner.
func (s *Service) ResumeMemorySandboxOperation(ctx context.Context, sandboxID, operation string) error {
	if operation == "" || len(operation) > 512 {
		return errors.New("exact memory resume operation is required")
	}
	_, _, err := s.resumeNomadSandboxOperation(ctx, sandboxID, true, operation)
	return err
}

// CheckpointResumeWorker schedules bounded parallel retries. PostgreSQL and
// node journals remain execution authority across replicas and process loss.
// Short attempts let asynchronous image preparation continue on the node while
// other restores progress; no retry extends the fixed resource lease deadline.
type CheckpointResumeWorker struct {
	store          CheckpointResumeStore
	resume         CheckpointResumer
	logger         *zap.Logger
	mu             sync.Mutex
	after          string
	concurrency    int
	attemptTimeout time.Duration
}

func NewCheckpointResumeWorker(store CheckpointResumeStore, resume CheckpointResumer, logger *zap.Logger) (*CheckpointResumeWorker, error) {
	if store == nil || resume == nil {
		return nil, errors.New("memory resume store and backend are required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &CheckpointResumeWorker{store: store, resume: resume, logger: logger, concurrency: 4, attemptTimeout: 10 * time.Second}, nil
}

func (w *CheckpointResumeWorker) RunOnce(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	items, err := w.store.ListNomadCheckpointResumes(ctx, w.after, checkpointResumeBatch)
	if err != nil {
		return err
	}
	if len(items) > checkpointResumeBatch {
		return errors.New("memory resume scan exceeded limit")
	}
	previous := w.after
	for _, item := range items {
		if item.OperationID <= previous || item.SandboxID == "" {
			return errors.New("memory resume scan did not advance exact identities")
		}
		previous = item.OperationID
	}
	jobs := make(chan sandboxstore.NomadCheckpointResumeWork)
	var wg sync.WaitGroup
	var failureMu sync.Mutex
	var failures []error
	for range min(w.concurrency, len(items)) {
		wg.Go(func() {
			for item := range jobs {
				if ctx.Err() != nil {
					return
				}
				attempt, cancel := context.WithTimeout(ctx, w.attemptTimeout)
				err := w.resume.ResumeMemorySandboxOperation(attempt, item.SandboxID, item.OperationID)
				cancel()
				if err != nil {
					failureMu.Lock()
					failures = append(failures, fmt.Errorf("resume memory operation %s: %w", item.OperationID, err))
					failureMu.Unlock()
				}
			}
		})
	}
	canceled := false
send:
	for _, item := range items {
		if ctx.Err() != nil {
			canceled = true
			break
		}
		select {
		case jobs <- item:
			w.after = item.OperationID
		case <-ctx.Done():
			canceled = true
			break send
		}
	}
	close(jobs)
	wg.Wait()
	if canceled {
		return errors.Join(append(failures, ctx.Err())...)
	}
	if len(items) < checkpointResumeBatch {
		w.after = ""
	}
	return errors.Join(failures...)
}

func (w *CheckpointResumeWorker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
			w.logger.Warn("Memory resume recovery pass failed", zap.Error(err))
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
