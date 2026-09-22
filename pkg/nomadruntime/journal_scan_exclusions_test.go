package nomadruntime

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestRegistrationScanExclusionsPreserveGraceAndChangedRecordValidation(t *testing.T) {
	j, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, j.Close()) })
	registration := testRuntimeSlotJournalRegistration(t, "scan-slot")
	require.NoError(t, j.Register(registration))
	rows, _, err := j.registrationAbortCandidates("", time.Now())
	require.NoError(t, err)
	require.Empty(t, rows)
	rows, _, err = j.registrationAbortCandidates("", time.Now().Add(2*registrationAbortGrace))
	require.NoError(t, err)
	require.Len(t, rows, 1, "time passing must make the unchanged registration eligible")
	require.NoError(t, j.acknowledgeRegionalRegistration(registration))
	for range 2 {
		rows, _, err = j.registrationAbortCandidates("", time.Now().Add(2*registrationAbortGrace))
		require.NoError(t, err)
		require.Empty(t, rows)
		rows, _, err = j.migrationAdoptionCandidates("")
		require.NoError(t, err)
		require.Empty(t, rows)
	}
	require.Len(t, j.registrationScanExclusions.keys, 1)
	require.Len(t, j.adoptionScanExclusions.keys, 1)
	// A changed durable payload must be decoded again, including corruption
	// whose outer receipt flags still say that no background work is needed.
	require.NoError(t, j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		var record runtimeSlotJournalRecord
		if err := json.Unmarshal(bucket.Get([]byte(registration.SlotID)), &record); err != nil {
			return err
		}
		record.Version = 999
		payload, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(registration.SlotID), payload)
	}))
	_, _, err = j.registrationAbortCandidates("", time.Now().Add(2*registrationAbortGrace))
	require.Error(t, err)
	_, _, err = j.migrationAdoptionCandidates("")
	require.Error(t, err)
}

func TestAdoptionScanExclusionsDoNotHideNewReceiptWork(t *testing.T) {
	d, request, _, _ := migrationAdoptionNodeFixture(t)
	_, err := d.AdoptMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	installAdoptionReporter(t, d, request)
	completed, err := d.reconcileMigrationAdoptions(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, completed)
	for range 2 {
		rows, _, err := d.journal.migrationAdoptionCandidates("")
		require.NoError(t, err)
		require.Empty(t, rows)
	}
	require.NotEmpty(t, d.journal.adoptionScanExclusions.keys)
	// Model a retained pre-acknowledgement journal record being restored.
	// Eligibility depends on its current bytes, not the previous scan result.
	require.NoError(t, d.journal.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.Target.SlotID)))
		if err != nil {
			return err
		}
		record.MigrationDestination.Adoption.RegionalAcknowledgement = nil
		return putRuntimeSlotJournalRecord(bucket, record)
	}))
	rows, _, err := d.journal.migrationAdoptionCandidates("")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, request.Target.SlotID, rows[0].Registration.SlotID)
}

func TestJournalScanExclusionsRemainBoundedAndIndependent(t *testing.T) {
	var cache, other journalScanExclusions
	first, found := cache.contains([]byte("first"))
	require.False(t, found)
	cache.remember(first)
	_, found = other.contains([]byte("first"))
	require.False(t, found, "another scanner or reopened journal starts without exclusions")
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Go(func() {
			for i := range journalScanExclusionLimit {
				key, _ := cache.contains([]byte(fmt.Sprintf("%d-%d", worker, i)))
				cache.remember(key)
			}
		})
	}
	workers.Wait()
	require.Len(t, cache.keys, journalScanExclusionLimit)
	_, found = cache.contains([]byte("first"))
	require.False(t, found, "eviction requires full validation on the next scan")
}

func TestMigrationPoolScanExclusionsObserveNewReservationAndRelease(t *testing.T) {
	d, request := migrationStagingReservationFixture(t)
	check := func() error {
		return d.journal.db.View(func(tx *bolt.Tx) error {
			bucket, err := runtimeSlotJournalBucketFrom(tx)
			if err != nil {
				return err
			}
			return d.journal.checkMigrationStagingPool(bucket, "other-slot")
		})
	}
	require.NoError(t, check())
	require.NoError(t, check())
	require.Len(t, d.journal.stagingScanExclusions.keys, 1)
	_, err := d.ReserveMigrationStaging(t.Context(), request)
	require.NoError(t, err)
	require.ErrorIs(t, check(), errdefs.ErrResourceExhausted, "a previously excluded slot now owns staging")
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), request))
	require.NoError(t, check())
	require.NoError(t, check())
}

func TestMigrationPoolScanExclusionsReusePruneValidation(t *testing.T) {
	d, request := migrationStagingReservationFixture(t)
	deleted, err := d.journal.Prune(time.Now())
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Len(t, d.journal.stagingScanExclusions.keys, 1)
	_, err = d.ReserveMigrationStaging(t.Context(), request)
	require.NoError(t, err)
	// Startup's unchanged-byte exclusion cannot hide new staging custody.
	require.ErrorIs(t, d.journal.db.View(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		return d.journal.checkMigrationStagingPool(bucket, "other-slot")
	}), errdefs.ErrResourceExhausted)
	// Nor can a fully validated old record authorize changed/corrupted bytes.
	require.NoError(t, d.journal.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(request.Target.SlotID), []byte(`{"version":999}`))
	}))
	require.Error(t, d.journal.db.View(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		return d.journal.checkMigrationStagingPool(bucket, "other-slot")
	}))
}
