package nomadruntime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type recoveryTestRuntime struct {
	*fakeRootFSRuntime
	attempt func(context.Context, rootfshandoff.StageRequest) error
}

func (r *recoveryTestRuntime) CrashFence(ctx context.Context, stage rootfshandoff.StageRequest, _ string, _ CrashTaskObservation) (rootfshandoff.CrashFenceProof, error) {
	return rootfshandoff.CrashFenceProof{}, r.attempt(ctx, stage)
}

func (r *recoveryTestRuntime) Retire(ctx context.Context, stage rootfshandoff.StageRequest, _ string) (rootfssession.RetireResult, error) {
	return rootfssession.RetireResult{}, r.attempt(ctx, stage)
}

func (r *recoveryTestRuntime) ReclaimExternallyRetired(ctx context.Context, stage rootfshandoff.StageRequest) (bool, error) {
	return false, r.attempt(ctx, stage)
}

func (r *recoveryTestRuntime) setSessions(sessions ...rootfssession.RecoverySession) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recoverySessions = append([]rootfssession.RecoverySession(nil), sessions...)
}

func recoveryTestSession(name string, proof bool) rootfssession.RecoverySession {
	now := time.Now()
	return rootfssession.RecoverySession{
		Stage: rootfshandoff.StageRequest{
			BindingVersion: rootfshandoff.WriterBindingVersion, Parent: name, InitialGeneration: "generation",
			Identity: rootfshandoff.Identity{
				NodeUID: "node", BootID: "boot", AllocationID: "allocation-" + name,
				SlotNonce: "slot-" + name, ClaimID: "claim-" + name, RootFSID: "rootfs-" + name,
				WriterEpoch: 1, WriterGrantID: "grant-" + name, WriterGrantTokenDigest: "issuance-1",
			},
		},
		Kind: rootfssession.RecoveryCrashAbandon, State: "tombstoned",
		CreatedAt: now.Add(-time.Hour), CrashRequestedAt: now.Add(-72 * time.Hour),
		ExternalCrash: proof, BranchRemoved: proof, ExternalProofExpiresAt: now.Add(-time.Hour),
	}
}

type blockedRecoveryAttempt struct {
	stage   rootfshandoff.StageRequest
	ctx     context.Context
	release chan struct{}
}

type recoveryTestHarness struct {
	d       *nodeRuntime
	runtime *recoveryTestRuntime
	ctx     context.Context
	cancel  context.CancelFunc
	started chan blockedRecoveryAttempt
}

// Backend calls deliberately ignore cancellation until released. This checks
// the lifetime contract, not just the usual cooperative cancellation path.
func newRecoveryTestHarness(t *testing.T, sessions ...rootfssession.RecoverySession) *recoveryTestHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	allReleased := make(chan struct{})
	h := &recoveryTestHarness{ctx: ctx, cancel: cancel, started: make(chan blockedRecoveryAttempt, 128)}
	h.runtime = &recoveryTestRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{}}
	h.runtime.setSessions(sessions...)
	h.runtime.attempt = func(ctx context.Context, stage rootfshandoff.StageRequest) error {
		attempt := blockedRecoveryAttempt{stage: stage, ctx: ctx, release: make(chan struct{})}
		h.started <- attempt
		select {
		case <-attempt.release:
		case <-allReleased:
		}
		return errors.New("unchanged recovery")
	}
	h.d = &nodeRuntime{runtime: h.runtime, logger: newLogger(zap.NewNop()), trigger: make(chan string, 16)}
	t.Cleanup(func() {
		cancel()
		close(allReleased)
		waitRecoveryWorkers(t, h.d)
	})
	return h
}

func waitRecoveryWorkers(t *testing.T, d *nodeRuntime) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciliation workers did not join")
	}
}

func (h *recoveryTestHarness) take(t *testing.T, count int) []blockedRecoveryAttempt {
	t.Helper()
	var attempts []blockedRecoveryAttempt
	for range count {
		select {
		case attempt := <-h.started:
			attempts = append(attempts, attempt)
		case <-time.After(time.Second):
			t.Fatalf("only %d of %d backend operations started", len(attempts), count)
		}
	}
	return attempts
}

func requireRecoveryCounts(t *testing.T, d *nodeRuntime, total, proofs int) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	require.Equal(t, total, d.periodicRecovery)
	require.Equal(t, proofs, d.periodicProofs)
}

func recoverySequence(d *nodeRuntime) uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.recoverySequence
}

func TestNodeRuntimeRecoveryBoundsReservePhysicalCapacity(t *testing.T) {
	var sessions []rootfssession.RecoverySession
	for i := range 30 {
		sessions = append(sessions, recoveryTestSession(fmt.Sprintf("old-%02d", i), true))
	}
	h := newRecoveryTestHarness(t, sessions...)
	h.d.scan(h.ctx, "")
	proofs := h.take(t, rootFSProofRecoveryConcurrency)
	requireRecoveryCounts(t, h.d, rootFSProofRecoveryConcurrency, rootFSProofRecoveryConcurrency)

	// A full historical-proof budget must still leave room for both an owned
	// live writer and externally authorized cleanup with a physical branch.
	live := recoveryTestSession("live", false)
	live.Live = true
	externalPhysical := recoveryTestSession("external-physical", true)
	externalPhysical.BranchRemoved = false
	sessions = append(sessions, live, externalPhysical)
	h.runtime.setSessions(sessions...)
	h.d.scan(h.ctx, "")
	physical := h.take(t, 2)
	require.ElementsMatch(t, []string{live.Stage.Parent, externalPhysical.Stage.Parent},
		[]string{physical[0].stage.Parent, physical[1].stage.Parent})
	requireRecoveryCounts(t, h.d, rootFSRecoveryConcurrency, rootFSProofRecoveryConcurrency)

	var scanners sync.WaitGroup
	for range 12 {
		scanners.Add(1)
		go func() { defer scanners.Done(); h.d.scan(h.ctx, "") }()
	}
	scanners.Wait()
	require.EqualValues(t, rootFSRecoveryConcurrency, recoverySequence(h.d), "rescans cannot overbook or duplicate a slot")
	for _, attempt := range append(proofs, physical...) {
		close(attempt.release)
	}
	waitRecoveryWorkers(t, h.d)
	requireRecoveryCounts(t, h.d, 0, 0)
}

func TestNodeRuntimeRecoveryFairnessAmongFailingIdentities(t *testing.T) {
	var sessions []rootfssession.RecoverySession
	for i := range 2 * rootFSRecoveryConcurrency {
		sessions = append(sessions, recoveryTestSession(fmt.Sprintf("writer-%02d", i), false))
	}
	h := newRecoveryTestHarness(t, sessions...)
	seen := make(map[string]bool)
	for batch := range 2 {
		// Both batches are eligible again: least-recently-attempted ordering,
		// not their cooldown, must give the second half its first turn.
		h.d.scanAt(h.ctx, "", time.Now().Add(time.Duration(batch)*time.Hour))
		attempts := h.take(t, rootFSRecoveryConcurrency)
		for _, attempt := range attempts {
			require.False(t, seen[attempt.stage.Parent], "front-of-journal failures starved an unattempted writer")
			seen[attempt.stage.Parent] = true
			close(attempt.release)
		}
		waitRecoveryWorkers(t, h.d)
	}
	require.Len(t, seen, len(sessions))
}

func TestNodeRuntimeRecoveryPhysicalAdmissionPrecedesHistoricalBacklog(t *testing.T) {
	var sessions []rootfssession.RecoverySession
	for i := range 20 {
		sessions = append(sessions, recoveryTestSession(fmt.Sprintf("a-proof-%02d", i), true))
	}
	for i := range rootFSRecoveryConcurrency + 1 {
		session := recoveryTestSession(fmt.Sprintf("z-physical-%d", i), false)
		session.Live = i > 0
		sessions = append(sessions, session)
	}
	h := newRecoveryTestHarness(t, sessions...)
	h.d.scan(h.ctx, "")
	for _, attempt := range h.take(t, rootFSRecoveryConcurrency) {
		require.Contains(t, attempt.stage.Parent, "z-physical-")
		require.NotEqual(t, "z-physical-0", attempt.stage.Parent, "owned live writers must precede non-live recovery")
	}
	requireRecoveryCounts(t, h.d, rootFSRecoveryConcurrency, 0)
}

func TestNodeRuntimeRecoveryBackoffIncludesNilNoProgress(t *testing.T) {
	for _, attemptErr := range []error{nil, errors.New("terminal authority denied"), context.DeadlineExceeded} {
		t.Run(fmt.Sprint(attemptErr), func(t *testing.T) {
			session := recoveryTestSession("no-progress", true)
			var calls atomic.Int64
			r := &recoveryTestRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{}, attempt: func(context.Context, rootfshandoff.StageRequest) error {
				calls.Add(1)
				return attemptErr
			}}
			r.setSessions(session)
			d := &nodeRuntime{runtime: r, logger: newLogger(zap.NewNop())}
			now := time.Now()
			for index, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute} {
				d.scanAt(t.Context(), "", now)
				waitRecoveryWorkers(t, d)
				require.EqualValues(t, index+1, calls.Load())
				d.mu.Lock()
				retry := *d.recoveryRetries[session.Stage.Parent]
				d.mu.Unlock()
				require.Equal(t, delay, retry.delay)
				d.scanAt(t.Context(), "", retry.next.Add(-time.Nanosecond))
				waitRecoveryWorkers(t, d)
				require.EqualValues(t, index+1, calls.Load(), "retry before its deadline")
				now = retry.next
			}
		})
	}
}

func TestNodeRuntimeRecoveryIdentityAndProgressResetBackoff(t *testing.T) {
	base := recoveryTestSession("identity", true)
	base.Consumer = &rootfssession.ConsumerRegistration{LeaseID: "lease", LeaseExpiresAt: "first heartbeat"}
	tests := []struct {
		name   string
		change func(*rootfssession.RecoverySession)
		reset  bool
	}{
		{"slot", func(s *rootfssession.RecoverySession) { s.Stage.Identity.SlotNonce = "replacement" }, true},
		{"grant", func(s *rootfssession.RecoverySession) { s.Stage.Identity.WriterGrantID = "replacement" }, true},
		{"issuance", func(s *rootfssession.RecoverySession) { s.Stage.Identity.WriterGrantTokenDigest = "replacement" }, true},
		{"epoch", func(s *rootfssession.RecoverySession) { s.Stage.Identity.WriterEpoch++ }, true},
		{"boot", func(s *rootfssession.RecoverySession) { s.Stage.Identity.BootID = "replacement" }, true},
		{"node", func(s *rootfssession.RecoverySession) { s.Stage.Identity.NodeUID = "replacement" }, true},
		{"allocation", func(s *rootfssession.RecoverySession) { s.Stage.Identity.AllocationID = "replacement" }, true},
		{"rootfs", func(s *rootfssession.RecoverySession) { s.Stage.Identity.RootFSID = "replacement" }, true},
		{"initial generation", func(s *rootfssession.RecoverySession) { s.Stage.InitialGeneration = "replacement" }, true},
		{"state", func(s *rootfssession.RecoverySession) { s.State = "removing" }, true},
		{"retirement", func(s *rootfssession.RecoverySession) { s.RetireOperationID = "replacement" }, true},
		{"crash operation", func(s *rootfssession.RecoverySession) { s.CrashOperationID = "replacement" }, true},
		{"pressure", func(s *rootfssession.RecoverySession) { s.PressureOperationID = "replacement" }, true},
		{"physical branch", func(s *rootfssession.RecoverySession) { s.BranchRemoved = false }, true},
		{"proof expiry", func(s *rootfssession.RecoverySession) {
			s.ExternalProofExpiresAt = s.ExternalProofExpiresAt.Add(time.Hour)
		}, true},
		{"consumer lease", func(s *rootfssession.RecoverySession) { s.Consumer.LeaseID = "replacement" }, true},
		{"heartbeat", func(s *rootfssession.RecoverySession) { s.Consumer.LeaseExpiresAt = "next heartbeat" }, false},
		{"secret token", func(s *rootfssession.RecoverySession) { s.Stage.Identity.WriterGrantToken = "rotated-secret" }, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := &nodeRuntime{logger: newLogger(zap.NewNop())}
			old := d.observeRecoverySessions([]rootfssession.RecoverySession{base})[0]
			old.delay, old.next, old.lastAttempt = time.Minute, time.Now().Add(time.Minute), 10
			changed := base
			consumer := *base.Consumer
			changed.Consumer = &consumer
			test.change(&changed)
			next := d.observeRecoverySessions([]rootfssession.RecoverySession{changed})[0]
			if test.reset {
				require.NotSame(t, old, next)
				require.Zero(t, next.delay)
				require.True(t, next.next.IsZero())
				require.Zero(t, next.lastAttempt)
			} else {
				require.Same(t, old, next)
				require.Equal(t, time.Minute, next.delay)
			}
			require.Equal(t, "first heartbeat", base.Consumer.LeaseExpiresAt, "fingerprinting must not mutate the snapshot")
		})
	}
}

func TestNodeRuntimeRecoveryPrunesAbsentAndIgnoresLateCompletion(t *testing.T) {
	session := recoveryTestSession("replaced", false)
	h := newRecoveryTestHarness(t, session)
	h.d.scan(h.ctx, "")
	oldAttempt := h.take(t, 1)[0]
	h.d.mu.Lock()
	oldRetry := h.d.recoveryRetries[session.Stage.Parent]
	h.d.mu.Unlock()
	h.runtime.setSessions()
	h.d.scan(h.ctx, "")
	require.Empty(t, h.d.recoveryRetries)
	requireRecoveryCounts(t, h.d, 1, 0)

	// Reuse the parent and slot, but never overlap different exact bindings
	// while the previous backend call is still unwinding.
	session.Stage.Identity.WriterEpoch++
	h.runtime.setSessions(session)
	h.d.scan(h.ctx, "")
	require.EqualValues(t, 1, recoverySequence(h.d))
	h.d.mu.Lock()
	newRetry := h.d.recoveryRetries[session.Stage.Parent]
	h.d.mu.Unlock()
	require.NotSame(t, oldRetry, newRetry)
	close(oldAttempt.release)
	waitRecoveryWorkers(t, h.d)
	require.True(t, newRetry.next.IsZero(), "old binding completion must not penalize a replacement")
	h.d.scan(h.ctx, "")
	newAttempt := h.take(t, 1)[0]
	require.Equal(t, session.Stage.Identity, newAttempt.stage.Identity)

	h.runtime.setSessions()
	h.d.scan(h.ctx, "")
	close(newAttempt.release)
	waitRecoveryWorkers(t, h.d)
	require.Empty(t, h.d.recoveryRetries, "late completion resurrected forgotten scheduling state")
	require.Empty(t, h.d.inflight)
}

func TestNodeRuntimeRecoveryTransitionImmediatelyEscapesCooldown(t *testing.T) {
	session := recoveryTestSession("transition", false)
	h := newRecoveryTestHarness(t, session)
	h.d.scan(h.ctx, "")
	first := h.take(t, 1)[0]
	close(first.release)
	waitRecoveryWorkers(t, h.d)
	h.d.mu.Lock()
	h.d.recoveryRetries[session.Stage.Parent].next = time.Now().Add(time.Hour)
	h.d.mu.Unlock()
	h.d.scan(h.ctx, "")
	require.EqualValues(t, 1, recoverySequence(h.d))
	session.Kind = rootfssession.RecoveryPlannedRetire
	session.RetireOperationID = "new-planned-retirement"
	h.runtime.setSessions(session)
	h.d.scan(h.ctx, "")
	h.take(t, 1)
	require.EqualValues(t, 2, recoverySequence(h.d), "new durable retirement work inherited an obsolete cooldown")
}

func TestNodeRuntimeRecoveryUrgentAndPressureBypassPeriodicBudget(t *testing.T) {
	var sessions []rootfssession.RecoverySession
	for i := range rootFSRecoveryConcurrency {
		sessions = append(sessions, recoveryTestSession(fmt.Sprintf("blocked-%d", i), false))
	}
	h := newRecoveryTestHarness(t, sessions...)
	h.d.scan(h.ctx, "")
	h.take(t, rootFSRecoveryConcurrency)

	urgent := recoveryTestSession("urgent", false)
	sessions = append(sessions, urgent)
	h.runtime.setSessions(sessions...)
	h.d.scan(h.ctx, "")
	require.EqualValues(t, rootFSRecoveryConcurrency, recoverySequence(h.d))
	h.d.mu.Lock()
	retry := h.d.recoveryRetries[urgent.Stage.Parent]
	retry.delay, retry.next = time.Minute, time.Now().Add(time.Hour)
	h.d.mu.Unlock()
	allocations := &failingNomadAllocationSource{}
	h.d.allocations = allocations
	h.d.writerLeaseLost(urgent.Stage, errors.New("lost lease"))
	h.d.scan(h.ctx, <-h.d.trigger)
	attempt := h.take(t, 1)[0]
	require.Equal(t, urgent.Stage.Parent, attempt.stage.Parent)
	require.Zero(t, allocations.calls, "writer fencing must not wait on the optional catalog")
	requireRecoveryCounts(t, h.d, rootFSRecoveryConcurrency, 0)
	h.d.scan(h.ctx, urgent.Stage.Parent)
	require.EqualValues(t, rootFSRecoveryConcurrency+1, recoverySequence(h.d), "urgent triggers must still deduplicate an exact slot")

	pressure := rootfssession.DirtyTailPressureSession{Stage: recoveryTestSession("pressure", false).Stage}
	h.runtime.mu.Lock()
	h.runtime.pressures = []rootfssession.DirtyTailPressureSession{pressure}
	h.runtime.mu.Unlock()
	h.d.scanDirtyTailPressures(h.ctx)
	select {
	case parent := <-h.d.trigger:
		require.Equal(t, pressure.Stage.Parent, parent)
	case <-time.After(time.Second):
		t.Fatal("periodic saturation blocked pressure planning")
	}
	requireRecoveryCounts(t, h.d, rootFSRecoveryConcurrency, 0)
}

func TestNodeRuntimeRecoveryBoundsUrgentBurstAndRetainsDeferredWork(t *testing.T) {
	var sessions []rootfssession.RecoverySession
	for i := range 40 {
		sessions = append(sessions, recoveryTestSession(fmt.Sprintf("burst-%02d", i), false))
	}
	h := newRecoveryTestHarness(t, sessions...)
	for _, session := range sessions {
		h.d.scan(h.ctx, session.Stage.Parent)
	}
	attempts := h.take(t, rootFSUrgentRecoveryConcurrency)
	require.EqualValues(t, rootFSUrgentRecoveryConcurrency, recoverySequence(h.d))
	h.d.mu.Lock()
	require.Equal(t, rootFSUrgentRecoveryConcurrency, h.d.urgentRecovery)
	state := h.d.inflight[recoveryInflightKey(sessions[0])]
	h.d.mu.Unlock()
	state.cancel()
	for _, attempt := range attempts {
		if attempt.stage.Parent == sessions[0].Stage.Parent {
			<-attempt.ctx.Done()
		}
	}
	h.d.scan(h.ctx, sessions[2].Stage.Parent)
	require.EqualValues(t, rootFSUrgentRecoveryConcurrency, recoverySequence(h.d), "cancellation must not free a lane before backend completion")
	for _, attempt := range attempts {
		close(attempt.release)
	}
	waitRecoveryWorkers(t, h.d)
	h.d.mu.Lock()
	require.Zero(t, h.d.urgentRecovery)
	h.d.mu.Unlock()
	// Ignore the completed identities; the original durable snapshot still
	// contains every deferred writer without keeping an unbounded hint queue.
	h.runtime.setSessions(sessions[2:]...)
	h.d.scan(h.ctx, "")
	deferred := h.take(t, rootFSRecoveryConcurrency)
	for _, attempt := range deferred {
		require.NotContains(t, []string{sessions[0].Stage.Parent, sessions[1].Stage.Parent}, attempt.stage.Parent)
	}
	requireRecoveryCounts(t, h.d, rootFSRecoveryConcurrency, 0)
}

func TestNodeRuntimeRecoveryCancellationDoesNotReleaseActiveBackend(t *testing.T) {
	session := recoveryTestSession("shutdown", false)
	h := newRecoveryTestHarness(t, session)
	h.d.scan(h.ctx, "")
	attempt := h.take(t, 1)[0]
	h.cancel()
	<-attempt.ctx.Done()
	requireRecoveryCounts(t, h.d, 1, 0)
	require.True(t, h.d.reconciliationInFlight(recoveryInflightKey(session)))
	h.d.scan(h.ctx, session.Stage.Parent)
	require.EqualValues(t, 1, recoverySequence(h.d))
	close(attempt.release)
	waitRecoveryWorkers(t, h.d)
	requireRecoveryCounts(t, h.d, 0, 0)
	require.True(t, h.d.recoveryRetries[session.Stage.Parent].next.IsZero(), "shutdown must not count as failed progress")
}

func TestNodeRuntimeRecoveryExternalPreemptionJoinsBeforeAdmission(t *testing.T) {
	session := recoveryTestSession("preempt", false)
	h := newRecoveryTestHarness(t, session)
	h.d.scan(h.ctx, "")
	attempt := h.take(t, 1)[0]
	key := recoveryInflightKey(session)
	result := make(chan error, 1)
	go func() { result <- h.d.beginExternalReconciliation(h.ctx, key) }()
	<-attempt.ctx.Done()
	requireRecoveryCounts(t, h.d, 1, 0)
	select {
	case err := <-result:
		t.Fatalf("cleanup entered before the backend joined: %v", err)
	default:
	}
	close(attempt.release)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("external cleanup did not enter after backend exit")
	}
	waitRecoveryWorkers(t, h.d)
	requireRecoveryCounts(t, h.d, 0, 0)
	require.False(t, h.d.beginReconciliation(key, func() {}))
	h.d.endReconciliation(key)
	require.Empty(t, h.d.preempting)
	require.True(t, h.d.recoveryRetries[session.Stage.Parent].next.IsZero(), "preemption must not impose a retry penalty")
}

func TestNodeRuntimeRecoveryCanceledPreemptionWaitersDoNotLeak(t *testing.T) {
	d := &nodeRuntime{}
	key := "shared-slot"
	localCtx, localCancel := context.WithCancel(t.Context())
	defer localCancel()
	canceled := make(chan struct{}, 2)
	require.True(t, d.beginReconciliation(key, func() { localCancel(); canceled <- struct{}{} }))
	contexts := make([]context.Context, 2)
	cancels := make([]context.CancelFunc, 2)
	results := make([]chan error, 2)
	for i := range 2 {
		contexts[i], cancels[i] = context.WithCancel(t.Context())
		defer cancels[i]()
		results[i] = make(chan error, 1)
		go func() { results[i] <- d.beginExternalReconciliation(contexts[i], key) }()
		select {
		case <-canceled:
		case <-time.After(time.Second):
			t.Fatal("preempting waiter did not cancel local work")
		}
	}
	<-localCtx.Done()
	for i := range 2 {
		cancels[i]()
		select {
		case err := <-results[i]:
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorIs(t, err, errdefs.ErrUnavailable)
		case <-time.After(time.Second):
			t.Fatal("canceled preemption waiter did not return")
		}
		d.mu.Lock()
		remaining := d.preempting[key]
		d.mu.Unlock()
		require.Equal(t, 1-i, remaining, "one canceled waiter must not clear another waiter's priority")
	}
	require.True(t, d.reconciliationInFlight(key), "canceling waiters must not release local backend ownership")
	d.endReconciliation(key)
	require.True(t, d.beginReconciliation(key, func() {}), "abandoned preemption marker permanently blocked recovery")
	d.endReconciliation(key)
	require.ErrorIs(t, d.beginExternalReconciliation(contexts[0], key), context.Canceled)
	require.False(t, d.reconciliationInFlight(key), "already canceled cleanup must not acquire an empty slot")
}
