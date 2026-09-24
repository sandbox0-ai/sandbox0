package nomadruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

type migrationAdoptionReporter interface {
	ReportMigrationAdoption(context.Context, protocol.MigrationAdoptionReceipt) (protocol.MigrationAdoptionAcknowledgement, error)
}

type migrationAdoptionCommandReader interface {
	GetMigrationAdoptionCommand(context.Context, string) (*protocol.MigrationAdoptionRequest, error)
}

// reconcileMigrationAdoptionsLoop has a bounded lane independent of writer
// recovery and driver responses. Missing local intent requires the exact
// committed regional command; this loop never creates execution authority.
func (d *nodeRuntime) reconcileMigrationAdoptionsLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		pass, cancel := context.WithTimeout(ctx, 20*time.Second)
		completed, err := d.reconcileMigrationAdoptions(pass)
		cancel()
		if err != nil && ctx.Err() == nil {
			d.logger.Error("deliver migration adoption receipts", "completed", completed, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *nodeRuntime) reconcileMigrationAdoptions(ctx context.Context) (int, error) {
	if d.journal == nil {
		return 0, errdefs.ErrUnavailable
	}
	rows, next, err := d.journal.migrationAdoptionCandidates(d.migrationAdoptionAfter)
	if err != nil {
		return 0, err
	}
	d.migrationAdoptionAfter = next
	if len(rows) == 0 {
		return 0, nil
	}
	reporter, ok := d.registrationAuthority.(migrationAdoptionReporter)
	if !ok {
		return 0, errdefs.ErrUnavailable
	}
	completed := 0
	var failures []error
	for _, record := range rows {
		if err := ctx.Err(); err != nil {
			return completed, errors.Join(append(failures, err)...)
		}
		custody := record.MigrationDestination
		adoption := custody.Adoption
		target := custody.Request.Target
		committedCheckpoint := false
		if target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID {
			failures = append(failures, fmt.Errorf("migration adoption belongs to another node: %w", errdefs.ErrFailedPrecondition))
			continue
		}
		if adoption == nil {
			reader, ok := d.registrationAuthority.(migrationAdoptionCommandReader)
			if !ok {
				failures = append(failures, errdefs.ErrUnavailable)
				continue
			}
			command, readErr := reader.GetMigrationAdoptionCommand(ctx, target.SlotID)
			if readErr != nil {
				failures = append(failures, readErr)
				continue
			}
			if command == nil {
				continue
			}
			if custody.Restore == nil {
				failures = append(failures, errdefs.ErrFailedPrecondition)
				continue
			}
			restored := *custody.Restore
			if command.CheckpointRestoreDigest != "" && custody.Request.Checkpoint != nil {
				restored.State = protocol.MigrationRestoreComplete
			}
			if command.ValidateFor(restored) != nil {
				failures = append(failures, errdefs.ErrFailedPrecondition)
				continue
			}
			adoption = &MigrationAdoptionCustody{Request: *command}
			committedCheckpoint = command.CheckpointRestoreDigest != "" && custody.Request.Checkpoint != nil
		}
		proof := adoption.Proof
		if proof == nil {
			// The existing adopter rechecks current custody under its per-slot
			// gate. First adoption still requires a live writer and process;
			// a persisted intent may finish after a reboot or driver loss.
			if committedCheckpoint {
				// Only the authenticated regional command can complete a
				// checkpoint adoption after the resumed process has stopped.
				if !d.beginReconciliation(target.SlotID, nil) {
					failures = append(failures, errdefs.ErrUnavailable)
					continue
				}
				proof, err = d.adoptMigrationDestination(ctx, adoption.Request, true)
				d.endReconciliation(target.SlotID)
			} else {
				proof, err = d.AdoptMigrationDestination(ctx, adoption.Request)
			}
			if err != nil {
				failures = append(failures, err)
				continue
			}
		}
		if proof == nil {
			failures = append(failures, errdefs.ErrUnavailable)
			continue
		}
		receipt := protocol.MigrationAdoptionReceipt{Request: adoption.Request, Proof: *proof}
		ack, err := reporter.ReportMigrationAdoption(ctx, receipt)
		if err == nil {
			err = d.journal.acknowledgeMigrationAdoption(receipt, ack)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("deliver adoption for slot %s: %w", target.SlotID, err))
			continue
		}
		completed++
	}
	return completed, errors.Join(failures...)
}

func (j *runtimeSlotJournal) migrationAdoptionCandidates(after string) ([]runtimeSlotJournalRecord, string, error) {
	var rows []runtimeSlotJournalRecord
	var next string
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
		for scanned := 0; key != nil && scanned < 128; scanned++ {
			next = string(key)
			fingerprint, excluded := j.adoptionScanExclusions.contains(value)
			if excluded {
				key, value = cursor.Next()
				continue
			}
			record, err := decodeRuntimeSlotJournalRecord(value)
			if err != nil {
				return err
			}
			if c := record.MigrationDestination; c != nil && ((c.Adoption != nil && c.Adoption.RegionalAcknowledgement == nil) ||
				(c.Adoption == nil && c.Restore != nil && (c.Restore.State == protocol.MigrationRestoreComplete ||
					c.Request.Checkpoint != nil && c.Restore.State == protocol.MigrationRestoreUncertain))) {
				rows = append(rows, record)
				if len(rows) == 16 {
					break
				}
			} else {
				j.adoptionScanExclusions.remember(fingerprint)
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

func (j *runtimeSlotJournal) acknowledgeMigrationAdoption(receipt protocol.MigrationAdoptionReceipt, ack protocol.MigrationAdoptionAcknowledgement) error {
	if err := ack.ValidateFor(receipt); err != nil {
		return err
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(receipt.Request.Target.SlotID)))
		if err != nil {
			return err
		}
		c := record.MigrationDestination
		if c == nil || !c.Adopted() || c.Adoption.Request != receipt.Request || *c.Adoption.Proof != receipt.Proof {
			return errdefs.ErrFailedPrecondition
		}
		if c.Adoption.RegionalAcknowledgement != nil {
			return nil
		}
		c.Adoption.RegionalAcknowledgement = &ack
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, record)
	})
}
