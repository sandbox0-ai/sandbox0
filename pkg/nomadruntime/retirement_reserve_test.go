package nomadruntime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRetirementReserveHintPreservesCleanupAndRecoveryExclusion(t *testing.T) {
	for _, test := range []string{"live", "unfenced", "already_reclaimed", "pressure", "pending_cleanup", "recovery_inflight", "pressure_inflight", "another_parent"} {
		t.Run(test, func(t *testing.T) {
			session := recoveryTestSession("blocking-parent", true)
			session.BranchRemoved = false
			runtime := &recoveryTestRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{}}
			runtime.attempt = func(context.Context, rootfshandoff.StageRequest) error {
				t.Fatal("an excluded writer must not be reclaimed")
				return nil
			}
			daemon := &nodeRuntime{runtime: runtime, logger: newLogger(zap.NewNop())}
			parent := session.Stage.Parent
			switch test {
			case "live":
				session.Live = true
			case "unfenced":
				session.ExternalCrash = false
			case "already_reclaimed":
				session.BranchRemoved = true
			case "pressure":
				session.PressureOperationID = "pending-pressure"
			case "another_parent":
				parent = "unrelated-parent"
			case "recovery_inflight", "pressure_inflight":
				key := recoveryInflightKey(session)
				if test == "pressure_inflight" {
					key = "pressure:" + parent
				}
				require.True(t, daemon.beginReconciliation(key, func() {}))
				defer daemon.endReconciliation(key)
			case "pending_cleanup":
				journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
				require.NoError(t, err)
				defer journal.Close()
				daemon.journal = journal
				registration := testRuntimeSlotJournalRegistration(t, session.Stage.Identity.SlotNonce)
				require.NoError(t, journal.Register(registration))
				_, err = journal.BeginCleanup(testRuntimeSlotJournalCleanup(registration))
				require.NoError(t, err)
			}
			runtime.setSessions(session)
			reclaimed, err := daemon.reclaimRetirementReserve(t.Context(), parent)
			require.NoError(t, err)
			require.False(t, reclaimed)
		})
	}
}

func TestRetirementReserveHintBoundsAndCancelsOneExactAuthorityAttempt(t *testing.T) {
	session := recoveryTestSession("blocking-parent", true)
	session.BranchRemoved = false
	runtime := &recoveryTestRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{}}
	runtime.setSessions(session, recoveryTestSession("unrelated", false))
	started := make(chan context.Context, 1)
	runtime.attempt = func(ctx context.Context, stage rootfshandoff.StageRequest) error {
		require.Equal(t, session.Stage, stage)
		started <- ctx
		<-ctx.Done()
		return ctx.Err()
	}
	daemon := &nodeRuntime{runtime: runtime, logger: newLogger(zap.NewNop())}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := daemon.reclaimRetirementReserve(ctx, session.Stage.Parent)
		done <- err
	}()
	var attempt context.Context
	select {
	case attempt = <-started:
	case <-time.After(time.Second):
		t.Fatal("authority attempt did not start")
	}
	deadline, ok := attempt.Deadline()
	require.True(t, ok)
	require.LessOrEqual(t, time.Until(deadline), 2*time.Second)
	reclaimed, err := daemon.reclaimRetirementReserve(ctx, session.Stage.Parent)
	require.NoError(t, err)
	require.False(t, reclaimed, "an in-flight owner cannot receive a duplicate authority request")
	cancel()
	select {
	case err := <-done:
		require.True(t, errors.Is(err, context.Canceled))
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not release the targeted attempt")
	}
	require.False(t, daemon.reconciliationInFlight(recoveryInflightKey(session)))
	require.Empty(t, started)
}
