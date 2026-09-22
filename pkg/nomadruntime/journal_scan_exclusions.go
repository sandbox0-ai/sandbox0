package nomadruntime

import (
	"crypto/sha256"
	"sync"
)

const journalScanExclusionLimit = 256

// journalScanExclusions remembers only fully validated, byte-identical records
// that cannot need a particular scan without a journal write.
// It never authorizes work or caches a record used for execution. Changed bytes,
// eviction and process restart all require full validation again. Store only
// bounded fingerprints, not historical custody payloads or a second work queue.
type journalScanExclusions struct {
	mu    sync.Mutex
	keys  map[[sha256.Size]byte]struct{}
	order [journalScanExclusionLimit][sha256.Size]byte
	next  int
}

func (c *journalScanExclusions) contains(payload []byte) ([sha256.Size]byte, bool) {
	key := sha256.Sum256(payload)
	c.mu.Lock()
	_, exists := c.keys[key]
	c.mu.Unlock()
	return key, exists
}

func (c *journalScanExclusions) remember(key [sha256.Size]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.keys[key]; exists {
		return
	}
	if c.keys == nil {
		c.keys = make(map[[sha256.Size]byte]struct{}, journalScanExclusionLimit)
	}
	if len(c.keys) == journalScanExclusionLimit {
		delete(c.keys, c.order[c.next])
	}
	c.keys[key] = struct{}{}
	c.order[c.next] = key
	c.next = (c.next + 1) % journalScanExclusionLimit
}

// migrationPoolScanRecord only excludes records that contribute neither image
// custody, receipt-count admission nor an exclusive staging reservation. These
// predicates depend solely on the fully validated bytes. Every active or newly
// changed reservation still participates in the original Bolt transaction.
func (j *runtimeSlotJournal) migrationPoolScanRecord(payload []byte) (runtimeSlotJournalRecord, bool, error) {
	fingerprint, excluded := j.stagingScanExclusions.contains(payload)
	if excluded {
		return runtimeSlotJournalRecord{}, true, nil
	}
	record, err := decodeRuntimeSlotJournalRecord(payload)
	if err != nil {
		return runtimeSlotJournalRecord{}, false, err
	}
	j.rememberMigrationPoolExclusion(fingerprint, record)
	return record, false, nil
}

// Reuse validation performed by startup pruning without caching authority.
// A later scan still hashes the exact current bytes before excluding a record.
func (j *runtimeSlotJournal) rememberMigrationPoolExclusion(fingerprint [sha256.Size]byte, record runtimeSlotJournalRecord) {
	if !record.retainsMigrationCountAdmission() && !record.hasMigrationImageCustody() &&
		(record.MigrationStaging == nil || record.MigrationStaging.Released || record.Proof != nil) {
		j.stagingScanExclusions.remember(fingerprint)
	}
}
