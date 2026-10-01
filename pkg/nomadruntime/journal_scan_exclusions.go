package nomadruntime

import (
	"crypto/sha256"
	"sync"
)

// At most 4 MiB of fingerprint pairs (plus map overhead) per scanner. A
// full cache keeps its admitted records instead of evicting them on every
// sequential miss. Journal pruning releases entries; changes replace the same
// slot's fingerprint rather than accumulating historical versions.
const journalScanExclusionLimit = 65536

// journalScanExclusions stores one fully validated exclusion per journal key.
// Payload fingerprints are checked inside the original Bolt transaction. It
// never caches execution authority; changed bytes and restart require decoding.
type journalScanExclusions struct {
	mu   sync.Mutex
	keys map[[sha256.Size]byte][sha256.Size]byte
}

func (c *journalScanExclusions) contains(slot, payload []byte) ([sha256.Size]byte, bool) {
	fingerprint := sha256.Sum256(payload)
	key := sha256.Sum256(slot)
	c.mu.Lock()
	prior, exists := c.keys[key]
	c.mu.Unlock()
	return fingerprint, exists && prior == fingerprint
}

func (c *journalScanExclusions) remember(slot []byte, fingerprint [sha256.Size]byte) {
	key := sha256.Sum256(slot)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.keys == nil {
		c.keys = make(map[[sha256.Size]byte][sha256.Size]byte)
	}
	if _, exists := c.keys[key]; !exists && len(c.keys) >= journalScanExclusionLimit {
		return
	}
	c.keys[key] = fingerprint
}

func (c *journalScanExclusions) forget(slot []byte) {
	c.mu.Lock()
	delete(c.keys, sha256.Sum256(slot))
	c.mu.Unlock()
}

// migrationPoolScanRecord only excludes records that contribute neither image
// custody, receipt-count admission nor an exclusive staging reservation. These
// predicates depend solely on the fully validated bytes. Every active or newly
// changed reservation still participates in the original Bolt transaction.
func (j *runtimeSlotJournal) migrationPoolScanRecord(slot, payload []byte) (runtimeSlotJournalRecord, bool, error) {
	fingerprint, excluded := j.stagingScanExclusions.contains(slot, payload)
	if excluded {
		return runtimeSlotJournalRecord{}, true, nil
	}
	record, err := decodeRuntimeSlotJournalRecord(payload)
	if err != nil {
		return runtimeSlotJournalRecord{}, false, err
	}
	j.rememberMigrationPoolExclusion(slot, fingerprint, record)
	return record, false, nil
}

// Reuse validation performed by startup pruning without caching authority.
// A later scan still hashes the exact current bytes before excluding a record.
func (j *runtimeSlotJournal) rememberMigrationPoolExclusion(slot []byte, fingerprint [sha256.Size]byte, record runtimeSlotJournalRecord) {
	if !record.retainsMigrationCountAdmission() && !record.hasMigrationImageCustody() &&
		(record.MigrationStaging == nil || record.MigrationStaging.Released || record.Proof != nil) {
		j.stagingScanExclusions.remember(slot, fingerprint)
	}
}
