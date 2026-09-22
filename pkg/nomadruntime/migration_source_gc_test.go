package nomadruntime

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func migrationSourceGCFixture(t *testing.T) (*nodeRuntime, protocol.MigrationSourceGCRequest) {
	t.Helper()
	d, request, _, _ := migrationSourceFinalizeNodeFixture(t)
	p, err := d.FinalizeMigrationSource(t.Context(), request)
	require.NoError(t, err)
	return d, protocol.MigrationSourceGCRequest{Target: request.Fence.PublicationRequest.Capture.Request.Target,
		FinalizationDigest: p.RequestDigest, CleanupProofDigest: p.Cleanup.ProofDigest, AllocationAbsenceDigest: strings.Repeat("a", 64)}
}

func TestMigrationSourceGCRequiresRegionalAcknowledgementBeforePruning(t *testing.T) {
	d, request := migrationSourceGCFixture(t)
	// Merely reading the complete receipt, even repeatedly, cannot acknowledge
	// allocation GC. A driver may crash before Nomad records its response.
	for range 2 {
		receipt, err := d.GetMigrationSourceFinalization(t.Context(), request.Target.SlotID)
		require.NoError(t, err)
		require.NotNil(t, receipt)
	}
	deleted, err := d.journal.Prune(time.Now().Add(48 * time.Hour))
	require.NoError(t, err)
	require.Zero(t, deleted)
	ack, err := d.AcknowledgeMigrationSourceGC(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, ack.ValidateFor(request))
	path := d.journal.db.Path()
	require.NoError(t, d.journal.Close())
	d.journal, err = newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.journal.Close()) })
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, request, *record.Migration.Finalization.AllocationGC)
	stale := *record.Migration.Finalization
	stale.AllocationGC = nil
	require.ErrorIs(t, d.journal.recordMigrationFinalization(request.Target.SlotID, stale), errdefs.ErrAlreadyExists)
	// A new direct observation may have a new digest. Keep the first durable
	// acknowledgement while accepting another exact-source retention request.
	request.AllocationAbsenceDigest = strings.Repeat("b", 64)
	ack, err = d.AcknowledgeMigrationSourceGC(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, ack.ValidateFor(request))
	deleted, err = d.journal.Prune(time.Now().Add(48 * time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	// Simulate loss of the successful response followed by pruning. Retrying
	// must not recreate a journal, cleanup proof, or execution permission.
	ack, err = d.AcknowledgeMigrationSourceGC(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, ack.ValidateFor(request))
	_, err = d.journal.Get(request.Target.SlotID)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
}

func TestMigrationSourceGCRejectsIncompleteOrChangedEvidence(t *testing.T) {
	for _, failure := range []string{"node", "boot", "allocation", "socket", "finalization", "cleanup", "absence", "invalidated", "pending-session"} {
		t.Run(failure, func(t *testing.T) {
			d, request := migrationSourceGCFixture(t)
			switch failure {
			case "node":
				request.Target.NodeUID = "other-node"
			case "boot":
				request.Target.NodeBootID = "other-boot"
			case "allocation":
				request.Target.AllocationID = "other-allocation"
			case "socket":
				request.Target.ControlEndpoint = "unix:///other/control.sock"
			case "finalization":
				request.FinalizationDigest = strings.Repeat("c", 64)
			case "cleanup":
				request.CleanupProofDigest = strings.Repeat("c", 64)
			case "absence":
				request.AllocationAbsenceDigest = ""
			case "invalidated":
				require.NoError(t, d.journal.invalidateMigrationExecution(request.Target.SlotID))
			case "pending-session":
				record, err := d.journal.Get(request.Target.SlotID)
				require.NoError(t, err)
				record.Migration.Finalization.SessionForgotten = false
				require.NoError(t, d.journal.db.Update(func(tx *bolt.Tx) error {
					bucket, err := runtimeSlotJournalBucketFrom(tx)
					if err != nil {
						return err
					}
					return putRuntimeSlotJournalRecord(bucket, record)
				}))
			}
			_, err := d.AcknowledgeMigrationSourceGC(t.Context(), request)
			require.Error(t, err)
			deleted, err := d.journal.Prune(time.Now().Add(48 * time.Hour))
			require.NoError(t, err)
			require.Zero(t, deleted)
		})
	}
}

func TestMigrationSourceGCJournalRejectsDowngradeAndUnboundAcknowledgement(t *testing.T) {
	d, request := migrationSourceGCFixture(t)
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	// Version 5 records without GC acknowledgement remain readable but retained.
	record.Version = runtimeSlotLegacyFinalizationJournalVersion
	payload, err := json.Marshal(record)
	require.NoError(t, err)
	_, err = decodeRuntimeSlotJournalRecord(payload)
	require.NoError(t, err)
	record.Migration.Finalization.AllocationGC = &request
	payload, err = json.Marshal(record)
	require.NoError(t, err)
	_, err = decodeRuntimeSlotJournalRecord(payload)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	record.Version = runtimeSlotFinalizationJournalVersion
	record.Migration.Finalization.AllocationGC.CleanupProofDigest = strings.Repeat("e", 64)
	payload, err = json.Marshal(record)
	require.NoError(t, err)
	_, err = decodeRuntimeSlotJournalRecord(payload)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}
