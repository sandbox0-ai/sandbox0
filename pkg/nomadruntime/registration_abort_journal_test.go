package nomadruntime

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestRegistrationAbortJournalBoundsScanningAndCandidateWork(t *testing.T) {
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
	require.NoError(t, err)
	defer journal.Close()
	base := testRuntimeSlotJournalRegistration(t, "slot-0000")
	now := time.Now().UTC()
	created := now.Add(-registrationAbortGrace - time.Second).Format(time.RFC3339Nano)
	require.NoError(t, journal.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		for i := 0; i < registrationAbortScanLimit+20; i++ {
			r := base
			r.SlotID = fmt.Sprintf("slot-%04d", i)
			r.AllocationID = "allocation-" + r.SlotID
			r.RunscContainerID = protocol.NomadRunscContainerID(r.SlotID)
			r.NetworkChain = networkChainName(r.RunscContainerID)
			record := runtimeSlotJournalRecord{Version: RuntimeSlotJournalVersion, Registration: r, CreatedAt: created, UpdatedAt: created, RegionalRegistrationObserved: i < registrationAbortScanLimit}
			if err := putRuntimeSlotJournalRecord(bucket, record); err != nil {
				return err
			}
		}
		return nil
	}))
	rows, next, err := journal.registrationAbortCandidates("", now)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Equal(t, "slot-0255", next)
	seen := map[string]bool{}
	for next != "" {
		rows, next, err = journal.registrationAbortCandidates(next, now)
		require.NoError(t, err)
		require.LessOrEqual(t, len(rows), registrationAbortBatchLimit)
		for _, row := range rows {
			require.False(t, seen[row.Registration.SlotID])
			seen[row.Registration.SlotID] = true
		}
	}
	require.Len(t, seen, 20)
	rows, _, err = journal.registrationAbortCandidates("slot-0255", now.Add(-time.Minute))
	require.NoError(t, err)
	require.Empty(t, rows, "in-flight registration must receive its grace period")
}
