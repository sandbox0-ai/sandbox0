package nomadruntime

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

const (
	registrationAbortScanLimit  = 256
	registrationAbortBatchLimit = 8
	registrationAbortGrace      = 2 * time.Minute
)

func isRegistrationAbort(record runtimeSlotJournalRecord) bool {
	return record.Cleanup != nil && strings.HasPrefix(record.Cleanup.OperationID, protocol.RegistrationAbortOperationPrefix)
}

// registrationAbortCandidates bounds both Bolt scanning and regional work.
// A registration acknowledgement is a receipt, never a cleanup permission.
func (j *runtimeSlotJournal) registrationAbortCandidates(after string, now time.Time) ([]runtimeSlotJournalRecord, string, error) {
	if len(after) > runtimeSlotJournalMaxIDSize {
		return nil, after, errdefs.ErrInvalidArgument
	}
	var rows []runtimeSlotJournalRecord
	next := ""
	err := j.db.View(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		cursor := bucket.Cursor()
		key, value := cursor.First()
		if after != "" {
			key, value = cursor.Seek([]byte(after))
			if bytes.Equal(key, []byte(after)) {
				key, value = cursor.Next()
			}
		}
		for scanned := 0; key != nil && scanned < registrationAbortScanLimit; scanned++ {
			next = string(key)
			record, err := decodeRuntimeSlotJournalRecord(value)
			if err != nil {
				return err
			}
			created, err := time.Parse(time.RFC3339Nano, record.CreatedAt)
			if err != nil {
				return err
			}
			if !record.RegionalRegistrationObserved && !record.RegistrationAbortAcknowledged &&
				(record.Cleanup == nil || isRegistrationAbort(record)) && !created.Add(registrationAbortGrace).After(now) {
				rows = append(rows, record)
				if len(rows) == registrationAbortBatchLimit {
					break
				}
			}
			key, value = cursor.Next()
		}
		if key == nil {
			next = ""
		}
		return nil
	})
	return rows, next, err
}

func (j *runtimeSlotJournal) acknowledgeRegionalRegistration(registration RuntimeSlotRegistration) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(registration.SlotID)))
		if err != nil {
			return err
		}
		if record.Registration != registration || isRegistrationAbort(record) {
			return fmt.Errorf("regional registration receipt changed: %w", errdefs.ErrFailedPrecondition)
		}
		if record.RegionalRegistrationObserved {
			return nil
		}
		record.RegionalRegistrationObserved = true
		return putRuntimeSlotJournalRecord(bucket, record)
	})
}

func (j *runtimeSlotJournal) acknowledgeRegistrationAbort(proof protocol.NodeCleanupControlProof) error {
	if err := proof.Validate(); err != nil {
		return err
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(proof.SlotID)))
		if err != nil {
			return err
		}
		if !isRegistrationAbort(record) || record.Proof == nil || *record.Proof != proof || record.RegionalRegistrationObserved {
			return fmt.Errorf("regional abort receipt changed: %w", errdefs.ErrFailedPrecondition)
		}
		if record.RegistrationAbortAcknowledged {
			return nil
		}
		record.RegistrationAbortAcknowledged = true
		return putRuntimeSlotJournalRecord(bucket, record)
	})
}
