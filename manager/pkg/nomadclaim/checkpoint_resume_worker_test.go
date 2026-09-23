package nomadclaim

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
)

type checkpointWorkStore struct {
	items   []sandboxstore.NomadCheckpointResumeWork
	cursors []string
}

func (s *checkpointWorkStore) ListNomadCheckpointResumes(_ context.Context, after string, limit int) ([]sandboxstore.NomadCheckpointResumeWork, error) {
	s.cursors = append(s.cursors, after)
	var result []sandboxstore.NomadCheckpointResumeWork
	for _, item := range s.items {
		if item.OperationID > after && len(result) < limit {
			result = append(result, item)
		}
	}
	return result, nil
}

type checkpointResumeFunc func(context.Context, string, string) error

func (f checkpointResumeFunc) ResumeMemorySandboxOperation(ctx context.Context, sandbox, operation string) error {
	return f(ctx, sandbox, operation)
}

func TestCheckpointRecoveryOnlyExecutesExactAcceptedOperation(t *testing.T) {
	for _, state := range []string{"accepted", "absent", "superseded", "complete"} {
		t.Run(state, func(t *testing.T) {
			f, id, p := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
			operation := f.store.resumeCandidate.OperationID
			f.store.resumeRequested = state != "absent"
			if state == "superseded" {
				operation = "old-operation"
			}
			if state == "complete" {
				require.NoError(t, f.service.ResumeMemorySandboxOperation(t.Context(), id, operation))
				p.authorities = nil
			}
			require.NoError(t, f.service.ResumeMemorySandboxOperation(t.Context(), id, operation))
			require.Empty(t, f.store.resumeRequests, "recovery never admits a replacement lifecycle")
			require.Zero(t, p.coldCalls)
			if state == "accepted" {
				require.Len(t, p.authorities, 1)
			} else {
				require.Empty(t, p.authorities)
			}
		})
	}
}

func TestCheckpointRecoveryBoundsConcurrencyAndAdvancesPastFailures(t *testing.T) {
	store := &checkpointWorkStore{}
	for i := range 35 {
		store.items = append(store.items, sandboxstore.NomadCheckpointResumeWork{OperationID: fmt.Sprintf("op-%03d", i), SandboxID: fmt.Sprintf("sandbox-%03d", i)})
	}
	var active, peak, calls atomic.Int32
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	worker, err := NewCheckpointResumeWorker(store, checkpointResumeFunc(func(ctx context.Context, sandbox, operation string) error {
		current := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
		}
		if calls.Add(1) <= 4 {
			started <- struct{}{}
		}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return errors.New("lost reply")
	}), nil)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- worker.RunOnce(t.Context()) }()
	for range 4 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("slow restore serialized other work")
		}
	}
	require.Equal(t, int32(4), active.Load())
	close(release)
	require.ErrorContains(t, <-done, "lost reply")
	require.Equal(t, int32(4), peak.Load())
	require.Equal(t, int32(32), calls.Load())
	require.Error(t, worker.RunOnce(t.Context()))
	require.Equal(t, []string{"", "op-031"}, store.cursors)
	require.Equal(t, int32(35), calls.Load())
	require.Empty(t, worker.after, "next pass must revisit failed work after completing the bounded scan")
}

func TestCheckpointRecoveryCancellationKeepsUnvisitedWorkAndCanRestart(t *testing.T) {
	store := &checkpointWorkStore{}
	for i := range 8 {
		store.items = append(store.items, sandboxstore.NomadCheckpointResumeWork{OperationID: fmt.Sprintf("op-%d", i), SandboxID: "sandbox"})
	}
	ctx, cancel := context.WithCancel(t.Context())
	var calls atomic.Int32
	worker, err := NewCheckpointResumeWorker(store, checkpointResumeFunc(func(ctx context.Context, _, _ string) error {
		if calls.Add(1) == 4 {
			cancel()
		}
		<-ctx.Done()
		return ctx.Err()
	}), nil)
	require.NoError(t, err)
	require.ErrorIs(t, worker.RunOnce(ctx), context.Canceled)
	require.LessOrEqual(t, calls.Load(), int32(4))
	require.NotEqual(t, "op-7", worker.after)
	restarted, err := NewCheckpointResumeWorker(store, checkpointResumeFunc(func(context.Context, string, string) error { return nil }), nil)
	require.NoError(t, err)
	require.NoError(t, restarted.RunOnce(t.Context()))
	require.Equal(t, "", store.cursors[len(store.cursors)-1], "process-local cursor is not durable completion")
}
