package portal

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const recoveryWriteCoalesceDelay = 2 * time.Millisecond
const recoveryWriteRetryDelay = 100 * time.Millisecond

type portalRecoveryPersistence interface {
	put(RecoveryManifest) error
	delete(string) error
	syncDirectory() error
}

type recoveryWriteOperation struct {
	generation uint64
	manifest   *RecoveryManifest
	deleteKey  string
}

// portalRecoveryWriter coalesces standby-acknowledged manifest updates and
// commits them to the node-local recovery cache off the bind success path.
// The standby remains the synchronous process-failure boundary.
type portalRecoveryWriter struct {
	store    portalRecoveryPersistence
	logger   *zap.Logger
	observer *Observer

	mu        sync.Mutex
	pending   map[string]recoveryWriteOperation
	enqueued  uint64
	completed uint64
	progress  chan struct{}
	commitErr error
	closed    bool
	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func newPortalRecoveryWriter(store portalRecoveryPersistence, logger *zap.Logger, observer *Observer) *portalRecoveryWriter {
	w := &portalRecoveryWriter{
		store:    store,
		logger:   logger,
		observer: observer,
		pending:  make(map[string]recoveryWriteOperation),
		progress: make(chan struct{}),
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go w.run()
	return w
}

func (w *portalRecoveryWriter) EnqueuePut(manifest RecoveryManifest) error {
	manifest.Version = portalRecoveryVersion
	if err := validateRecoveryManifest(manifest); err != nil {
		return err
	}
	_, err := w.enqueue(manifest.Key, recoveryWriteOperation{manifest: &manifest})
	return err
}

func (w *portalRecoveryWriter) DeleteAndWait(ctx context.Context, key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	generation, err := w.enqueue(key, recoveryWriteOperation{deleteKey: key})
	if err != nil {
		return err
	}
	return w.wait(ctx, generation)
}

func (w *portalRecoveryWriter) Flush(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	target := w.enqueued
	w.mu.Unlock()
	return w.wait(ctx, target)
}

func (w *portalRecoveryWriter) Error() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.commitErr
}

func (w *portalRecoveryWriter) Close(ctx context.Context) error {
	if w == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		select {
		case <-w.done:
			return w.Error()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.closed = true
	target := w.enqueued
	w.mu.Unlock()
	flushErr := w.wait(ctx, target)
	w.closeOnce.Do(func() { close(w.stop) })
	select {
	case <-w.done:
	case <-ctx.Done():
		if flushErr == nil {
			flushErr = ctx.Err()
		}
	}
	if flushErr == nil {
		flushErr = w.Error()
	}
	return flushErr
}

func (w *portalRecoveryWriter) enqueue(key string, operation recoveryWriteOperation) (uint64, error) {
	if w == nil || w.store == nil {
		return 0, fmt.Errorf("portal recovery writer is unavailable")
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return 0, fmt.Errorf("portal recovery writer is closed")
	}
	w.enqueued++
	operation.generation = w.enqueued
	w.pending[key] = operation
	generation := operation.generation
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
	return generation, nil
}

func (w *portalRecoveryWriter) wait(ctx context.Context, target uint64) error {
	if w == nil || target == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		w.mu.Lock()
		if w.completed >= target {
			err := w.commitErr
			w.mu.Unlock()
			return err
		}
		progress := w.progress
		w.mu.Unlock()
		select {
		case <-progress:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *portalRecoveryWriter) run() {
	defer close(w.done)
	for {
		select {
		case <-w.wake:
			if !w.waitDelay(recoveryWriteCoalesceDelay) {
				w.commitPending()
				return
			}
			for !w.commitPending() {
				if !w.waitDelay(recoveryWriteRetryDelay) {
					w.commitPending()
					return
				}
			}
		case <-w.stop:
			w.commitPending()
			return
		}
	}
}

func (w *portalRecoveryWriter) waitDelay(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-w.stop:
		return false
	}
}

// commitPending returns true when every operation through the selected batch
// is durably represented in the local recovery directory.
func (w *portalRecoveryWriter) commitPending() bool {
	operations, maxGeneration := w.takePending()
	if len(operations) == 0 {
		return true
	}
	started := time.Now()
	volumeID := ""
	for _, operation := range operations {
		if operation.manifest != nil && operation.manifest.VolumeID != "" {
			volumeID = operation.manifest.VolumeID
			break
		}
	}
	var commitErr error
	for _, operation := range operations {
		switch {
		case operation.manifest != nil:
			if err := w.store.put(*operation.manifest); err != nil {
				commitErr = err
			}
		case operation.deleteKey != "":
			if err := w.store.delete(operation.deleteKey); err != nil {
				commitErr = err
			}
		}
		if commitErr != nil {
			break
		}
	}
	if commitErr == nil {
		commitErr = w.store.syncDirectory()
	}
	w.mu.Lock()
	if commitErr == nil {
		if maxGeneration > w.completed {
			w.completed = maxGeneration
		}
		w.commitErr = nil
	} else {
		w.commitErr = commitErr
		w.requeueLocked(operations)
	}
	close(w.progress)
	w.progress = make(chan struct{})
	w.mu.Unlock()
	if commitErr != nil && w.logger != nil {
		w.logger.Error("ctld asynchronous portal recovery commit failed", zap.Error(commitErr))
	}
	if w.observer != nil {
		w.observer.ObservePhase("recovery", "local_group_commit", "local", 0, volumeID, started, commitErr)
	}
	return commitErr == nil
}

func (w *portalRecoveryWriter) requeueLocked(operations []recoveryWriteOperation) {
	for _, operation := range operations {
		key := operation.deleteKey
		if operation.manifest != nil {
			key = operation.manifest.Key
		}
		if key == "" {
			continue
		}
		if current, ok := w.pending[key]; ok && current.generation > operation.generation {
			continue
		}
		w.pending[key] = operation
	}
}

func (w *portalRecoveryWriter) takePending() ([]recoveryWriteOperation, uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) == 0 {
		return nil, 0
	}
	keys := make([]string, 0, len(w.pending))
	for key := range w.pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	operations := make([]recoveryWriteOperation, 0, len(keys))
	var maxGeneration uint64
	for _, key := range keys {
		operation := w.pending[key]
		operations = append(operations, operation)
		if operation.generation > maxGeneration {
			maxGeneration = operation.generation
		}
		delete(w.pending, key)
	}
	return operations, maxGeneration
}
