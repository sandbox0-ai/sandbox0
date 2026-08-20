package rootfsblock

import (
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
)

// NodeDirtyTailUsage is the aggregate unpublished payload reserved by all
// branch journals on one node.
type NodeDirtyTailUsage struct {
	UsedBytes int64
	MaxBytes  int64
	Owners    int
}

type dirtyTailOwner struct {
	usedBytes int64
	attached  bool
}

// DirtyTailBudget atomically limits unpublished branch payload across every
// session sharing one durable node volume. Recovered usage may exceed a newly
// lowered limit so existing writers can still be opened and retired, but no
// positive reservation is admitted until usage falls below the limit.
type DirtyTailBudget struct {
	mu       sync.Mutex
	maxBytes int64
	used     int64
	owners   map[string]dirtyTailOwner
}

// NewDirtyTailBudget creates an aggregate block-aligned node budget.
func NewDirtyTailBudget(maxBytes int64) (*DirtyTailBudget, error) {
	if maxBytes <= 0 || maxBytes%LogicalBlockSize != 0 {
		return nil, fmt.Errorf("node dirty tail limit must be a positive multiple of %d bytes", LogicalBlockSize)
	}
	return &DirtyTailBudget{maxBytes: maxBytes, owners: make(map[string]dirtyTailOwner)}, nil
}

// Preload records one journal found during startup before any branch handle is
// opened. It is idempotent for an exact owner and recovered usage.
func (b *DirtyTailBudget) Preload(owner string, usedBytes int64) error {
	owner = strings.TrimSpace(owner)
	if owner == "" || usedBytes < 0 || usedBytes%LogicalBlockSize != 0 {
		return fmt.Errorf("dirty tail owner and block-aligned non-negative usage are required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current, ok := b.owners[owner]
	if ok {
		if current.attached {
			return fmt.Errorf("dirty tail owner %q is already attached", owner)
		}
		return b.replaceUsageLocked(owner, current, usedBytes)
	}
	if usedBytes > math.MaxInt64-b.used {
		return fmt.Errorf("aggregate dirty tail usage overflows int64")
	}
	b.owners[owner] = dirtyTailOwner{usedBytes: usedBytes}
	b.used += usedBytes
	return nil
}

// ReleaseOwner removes one closed journal from aggregate accounting after its
// backing file has been deleted.
func (b *DirtyTailBudget) ReleaseOwner(owner string) error {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return fmt.Errorf("dirty tail owner is required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current, ok := b.owners[owner]
	if !ok {
		return nil
	}
	if current.attached {
		return fmt.Errorf("dirty tail owner %q is still attached", owner)
	}
	b.used -= current.usedBytes
	delete(b.owners, owner)
	return nil
}

// ValidateOwnerDetached verifies that a journal can be deleted without
// racing an open branch handle.
func (b *DirtyTailBudget) ValidateOwnerDetached(owner string) error {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return fmt.Errorf("dirty tail owner is required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if current, ok := b.owners[owner]; ok && current.attached {
		return fmt.Errorf("dirty tail owner %q is still attached", owner)
	}
	return nil
}

// Usage returns one race-free aggregate occupancy snapshot.
func (b *DirtyTailBudget) Usage() NodeDirtyTailUsage {
	b.mu.Lock()
	defer b.mu.Unlock()
	return NodeDirtyTailUsage{UsedBytes: b.used, MaxBytes: b.maxBytes, Owners: len(b.owners)}
}

func (b *DirtyTailBudget) attach(owner string, recoveredBytes int64) error {
	if owner == "" || recoveredBytes < 0 || recoveredBytes%LogicalBlockSize != 0 {
		return fmt.Errorf("dirty tail owner and block-aligned recovered usage are required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current := b.owners[owner]
	if current.attached {
		return fmt.Errorf("dirty tail owner %q already has an open branch", owner)
	}
	if err := b.replaceUsageLocked(owner, current, recoveredBytes); err != nil {
		return err
	}
	current = b.owners[owner]
	current.attached = true
	b.owners[owner] = current
	return nil
}

func (b *DirtyTailBudget) detach(owner string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	current, ok := b.owners[owner]
	if !ok || !current.attached {
		return fmt.Errorf("dirty tail owner %q is not attached", owner)
	}
	current.attached = false
	b.owners[owner] = current
	return nil
}

func (b *DirtyTailBudget) reserve(owner string, requestedBytes int64) error {
	if requestedBytes <= 0 || requestedBytes%LogicalBlockSize != 0 {
		return fmt.Errorf("dirty tail reservation must be a positive block-aligned size")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current, ok := b.owners[owner]
	if !ok || !current.attached {
		return fmt.Errorf("dirty tail owner %q is not attached", owner)
	}
	if b.used > b.maxBytes || requestedBytes > b.maxBytes-b.used {
		return &DirtyTailCapacityError{
			Scope: "node", UsedBytes: b.used, RequestedBytes: requestedBytes, LimitBytes: b.maxBytes,
		}
	}
	b.used += requestedBytes
	current.usedBytes += requestedBytes
	b.owners[owner] = current
	return nil
}

func (b *DirtyTailBudget) releaseReservation(owner string, releasedBytes int64) error {
	if releasedBytes <= 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current, ok := b.owners[owner]
	if !ok || !current.attached || releasedBytes > current.usedBytes {
		return fmt.Errorf("dirty tail reservation release does not match owner %q", owner)
	}
	current.usedBytes -= releasedBytes
	b.used -= releasedBytes
	b.owners[owner] = current
	return nil
}

func (b *DirtyTailBudget) replaceUsageLocked(owner string, current dirtyTailOwner, usedBytes int64) error {
	withoutCurrent := b.used - current.usedBytes
	if withoutCurrent < 0 || usedBytes > math.MaxInt64-withoutCurrent {
		return fmt.Errorf("aggregate dirty tail usage overflows int64")
	}
	current.usedBytes = usedBytes
	b.owners[owner] = current
	b.used = withoutCurrent + usedBytes
	return nil
}

// BranchJournalDirtyBytes returns conservative logical payload occupancy for
// a closed branch journal. An incomplete final record counts as one block; a
// later OpenBranch truncates it and reconciles the exact value.
func BranchJournalDirtyBytes(path string) (int64, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscallNoFollow, 0)
	if err != nil {
		return 0, fmt.Errorf("open branch journal for accounting: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat branch journal for accounting: %w", err)
	}
	if !info.Mode().IsRegular() {
		return 0, fmt.Errorf("branch journal is not a regular file")
	}
	if info.Size() == 0 {
		return 0, nil
	}
	_, headerEnd, err := readBranchHeader(file)
	if err != nil {
		return 0, err
	}
	if info.Size() < headerEnd {
		return 0, fmt.Errorf("branch journal is shorter than its header")
	}
	tailBytes := info.Size() - headerEnd
	records := tailBytes / branchRecordBytes
	if tailBytes%branchRecordBytes != 0 {
		records++
	}
	if records > math.MaxInt64/LogicalBlockSize {
		return 0, fmt.Errorf("branch journal dirty usage overflows int64")
	}
	return records * LogicalBlockSize, nil
}
