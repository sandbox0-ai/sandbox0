package nomadruntime

import (
	"path/filepath"
	"testing"
	"time"

	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
)

func TestNodeRuntimeExternalReclamationWaitsForSlotCleanupProof(t *testing.T) {
	external := recoveryTestSession("external-slot-cleanup", true)
	external.BranchRemoved = false
	h := newRecoveryTestHarness(t, external)
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	h.d.journal = journal
	registration := testRuntimeSlotJournalRegistration(t, external.Stage.Identity.SlotNonce)
	require.NoError(t, journal.Register(registration))
	cleanup := testRuntimeSlotJournalCleanup(registration)
	_, err = journal.BeginCleanup(cleanup)
	require.NoError(t, err)
	h.d.scan(h.ctx, "")
	require.Zero(t, recoverySequence(h.d), "unfinished regional slot cleanup owns physical reconciliation")

	require.NoError(t, journal.CompleteCleanup(cleanup, testRuntimeSlotJournalProof(t, cleanup)))
	h.d.scan(h.ctx, "")
	attempt := h.take(t, 1)[0]
	require.Equal(t, external.Stage, attempt.stage)
	close(attempt.release)
	waitRecoveryWorkers(t, h.d)
}

func TestExternalCrashReclamationDoesNotWaitForConsumerExpiry(t *testing.T) {
	now := time.Now()
	session := recoveryTestSession("external-retirement", true)
	session.CreatedAt = now
	session.BranchRemoved = false
	session.Consumer = &rootfssession.ConsumerRegistration{
		LeaseID: "lease", ActiveKey: "task", ContainerID: "container", StableMount: "/tmp/task/rootfs",
		HostMountNamespace: "mnt:[1]", LeaseExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano),
	}
	_, err := session.Consumer.Validate()
	require.NoError(t, err)
	require.True(t, rootFSSessionNeedsReconciliation(session, now, false),
		"an externally fenced branch may retain the node retirement reserve; check regional terminal authority without waiting for its old consumer lease")

	// A/B takeover must still protect an unfenced consumer, even when it is
	// not live in this ctld process. Only the external crash proof changes that.
	live := session
	live.ExternalCrash = false
	require.False(t, rootFSSessionNeedsReconciliation(live, now, false))
	live.Consumer = nil
	require.False(t, rootFSSessionNeedsReconciliation(live, now, false), "unbound sessions retain their attach grace")
	session.Consumer = nil
	require.True(t, rootFSSessionNeedsReconciliation(session, now, false), "external fencing also supersedes attach grace")
}

func TestNodeRuntimeReclaimsExternalCrashBeforeConsumerExpiry(t *testing.T) {
	now := time.Now()
	external := recoveryTestSession("external-reserve-owner", true)
	external.CreatedAt = now
	external.BranchRemoved = false
	external.Consumer = &rootfssession.ConsumerRegistration{
		LeaseID: "lease", ActiveKey: "task", ContainerID: "container", StableMount: "/tmp/task/rootfs",
		HostMountNamespace: "mnt:[1]", LeaseExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano),
	}
	live := recoveryTestSession("unfenced-consumer", false)
	live.Consumer = external.Consumer
	h := newRecoveryTestHarness(t, external, live)
	h.d.scanAt(h.ctx, "", now)
	attempt := h.take(t, 1)[0]
	require.Equal(t, external.Stage, attempt.stage)
	requireRecoveryCounts(t, h.d, 1, 0)
	close(attempt.release)
	waitRecoveryWorkers(t, h.d)
	require.EqualValues(t, 1, recoverySequence(h.d), "a valid unfenced consumer must not be reclaimed")

	// Once authority-gated reclamation removes the branch, retain the compact
	// proof quietly. Neither this consumer TTL nor forced scans shorten replay.
	external.BranchRemoved = true
	external.ExternalProofExpiresAt = now.Add(rootfssession.ExternalTerminalProofRetention)
	h.runtime.setSessions(external, live)
	h.d.scanAt(h.ctx, "", now.Add(time.Second))
	waitRecoveryWorkers(t, h.d)
	require.EqualValues(t, 1, recoverySequence(h.d))
	require.False(t, rootFSSessionNeedsReconciliation(external, now, true))
}
