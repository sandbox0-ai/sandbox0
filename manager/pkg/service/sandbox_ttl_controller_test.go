package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type fakeSandboxExpirationLister struct {
	mu      sync.Mutex
	pages   [][]sandboxstore.SandboxExpirationCandidate
	err     error
	calls   int
	now     time.Time
	backend string
	limit   int
}

func (f *fakeSandboxExpirationLister) ListSandboxExpirationCandidates(
	_ context.Context,
	now time.Time,
	backend string,
	limit int,
) ([]sandboxstore.SandboxExpirationCandidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.now, f.backend, f.limit = now, backend, limit
	if f.err != nil {
		return nil, f.err
	}
	if len(f.pages) == 0 {
		return nil, nil
	}
	page := append([]sandboxstore.SandboxExpirationCandidate(nil), f.pages[0]...)
	f.pages = f.pages[1:]
	return page, nil
}

type fakeSandboxExpirationBackend struct {
	pauses         []string
	terminations   []string
	pauseErrors    map[string]error
	terminateError map[string]error
}

func (f *fakeSandboxExpirationBackend) PauseSandboxByID(_ context.Context, sandboxID string) error {
	f.pauses = append(f.pauses, sandboxID)
	return f.pauseErrors[sandboxID]
}

func (f *fakeSandboxExpirationBackend) TerminateHardExpiredSandbox(_ context.Context, sandboxID string) error {
	f.terminations = append(f.terminations, sandboxID)
	return f.terminateError[sandboxID]
}

func TestSandboxTTLControllerProcessesHardExpiryBeforeSoftTTL(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	lister := &fakeSandboxExpirationLister{pages: [][]sandboxstore.SandboxExpirationCandidate{{
		{SandboxID: "hard-active", DesiredState: sandboxstore.SandboxDesiredStateActive,
			ExpiresAt: now.Add(-time.Minute), HardExpiresAt: now.Add(-time.Second)},
		{SandboxID: "hard-paused", DesiredState: sandboxstore.SandboxDesiredStatePaused,
			HardExpiresAt: now.Add(-time.Second)},
		{SandboxID: "soft-active", DesiredState: sandboxstore.SandboxDesiredStateActive,
			ExpiresAt: now.Add(-time.Second), HardExpiresAt: now.Add(time.Hour)},
		{SandboxID: "future", DesiredState: sandboxstore.SandboxDesiredStateActive,
			ExpiresAt: now.Add(time.Second)},
	}}}
	backend := &fakeSandboxExpirationBackend{
		pauseErrors: make(map[string]error), terminateError: make(map[string]error),
	}
	controller, err := NewSandboxTTLController(
		lister, backend, backend,
		SandboxTTLControllerConfig{RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad},
		func() time.Time { return now }, zap.NewNop(),
	)
	require.NoError(t, err)

	require.NoError(t, controller.runOnce(t.Context()))

	assert.Equal(t, []string{"hard-active", "hard-paused"}, backend.terminations)
	assert.Equal(t, []string{"soft-active"}, backend.pauses)
	assert.Equal(t, now, lister.now)
	assert.Equal(t, sandboxstore.SandboxRuntimeBackendNomad, lister.backend)
}

func TestSandboxTTLControllerContinuesAfterCandidateFailure(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 1, 0, 0, time.UTC)
	lister := &fakeSandboxExpirationLister{pages: [][]sandboxstore.SandboxExpirationCandidate{{
		{SandboxID: "pause-fails", DesiredState: sandboxstore.SandboxDesiredStateActive, ExpiresAt: now.Add(-time.Second)},
		{SandboxID: "pause-succeeds", DesiredState: sandboxstore.SandboxDesiredStateActive, ExpiresAt: now.Add(-time.Second)},
		{SandboxID: "terminate-fails", DesiredState: sandboxstore.SandboxDesiredStatePaused, HardExpiresAt: now.Add(-time.Second)},
		{SandboxID: "terminate-succeeds", DesiredState: sandboxstore.SandboxDesiredStatePaused, HardExpiresAt: now.Add(-time.Second)},
	}}}
	backend := &fakeSandboxExpirationBackend{
		pauseErrors:    map[string]error{"pause-fails": errors.New("pause failed")},
		terminateError: map[string]error{"terminate-fails": errors.New("terminate failed")},
	}
	controller, err := NewSandboxTTLController(
		lister, backend, backend,
		SandboxTTLControllerConfig{RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad},
		func() time.Time { return now }, zap.NewNop(),
	)
	require.NoError(t, err)

	require.NoError(t, controller.runOnce(t.Context()))

	assert.Equal(t, []string{"pause-fails", "pause-succeeds"}, backend.pauses)
	assert.Equal(t, []string{"terminate-fails", "terminate-succeeds"}, backend.terminations)
}

func TestSandboxTTLControllerEscalatesSoftPauseWhenHardTTLWinsRace(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 2, 0, 0, time.UTC)
	lister := &fakeSandboxExpirationLister{pages: [][]sandboxstore.SandboxExpirationCandidate{{{
		SandboxID: "sandbox-1", DesiredState: sandboxstore.SandboxDesiredStateActive,
		ExpiresAt: now.Add(-time.Second), HardExpiresAt: now.Add(time.Second),
	}}}}
	backend := &fakeSandboxExpirationBackend{
		pauseErrors:    map[string]error{"sandbox-1": sandboxstore.ErrNomadSandboxHardTTLExpired},
		terminateError: make(map[string]error),
	}
	controller, err := NewSandboxTTLController(
		lister, backend, backend,
		SandboxTTLControllerConfig{RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad},
		func() time.Time { return now }, zap.NewNop(),
	)
	require.NoError(t, err)

	require.NoError(t, controller.runOnce(t.Context()))

	assert.Equal(t, []string{"sandbox-1"}, backend.pauses)
	assert.Equal(t, []string{"sandbox-1"}, backend.terminations)
}

func TestSandboxTTLControllerBoundsBatchesAndSkipsDuplicateFailures(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 3, 0, 0, time.UTC)
	candidate := func(id string) sandboxstore.SandboxExpirationCandidate {
		return sandboxstore.SandboxExpirationCandidate{
			SandboxID: id, DesiredState: sandboxstore.SandboxDesiredStateActive, ExpiresAt: now.Add(-time.Second),
		}
	}
	lister := &fakeSandboxExpirationLister{pages: [][]sandboxstore.SandboxExpirationCandidate{
		{candidate("a"), candidate("b")},
		{candidate("a"), candidate("c")},
		{candidate("d"), candidate("e")},
	}}
	backend := &fakeSandboxExpirationBackend{
		pauseErrors:    map[string]error{"a": errors.New("persistent failure")},
		terminateError: make(map[string]error),
	}
	controller, err := NewSandboxTTLController(
		lister, backend, backend,
		SandboxTTLControllerConfig{
			RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad,
			BatchSize:      2, MaxBatchesPerRun: 2,
		}, func() time.Time { return now }, zap.NewNop(),
	)
	require.NoError(t, err)

	require.NoError(t, controller.runOnce(t.Context()))

	assert.Equal(t, []string{"a", "b", "c"}, backend.pauses)
	assert.Equal(t, 2, lister.calls)
	assert.Equal(t, 2, lister.limit)
}

func TestSandboxTTLControllerRunScansImmediatelyAndStopsWithContext(t *testing.T) {
	lister := &fakeSandboxExpirationLister{}
	backend := &fakeSandboxExpirationBackend{
		pauseErrors: make(map[string]error), terminateError: make(map[string]error),
	}
	controller, err := NewSandboxTTLController(
		lister, backend, backend,
		SandboxTTLControllerConfig{
			RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad,
			Interval:       time.Hour,
		}, time.Now, zap.NewNop(),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- controller.Run(ctx) }()
	require.Eventually(t, func() bool {
		lister.mu.Lock()
		defer lister.mu.Unlock()
		return lister.calls == 1
	}, time.Second, time.Millisecond)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}
