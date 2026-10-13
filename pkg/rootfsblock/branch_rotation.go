package rootfsblock

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// BranchCut identifies the exact flushed WAL prefix published to the region.
// Closing the checkpoint does not invalidate this value. It is not itself a
// regional acknowledgement; the session owner must obtain that separately.
type BranchCut struct {
	Identity BranchIdentity
	Sequence uint64
	End      int64
}

func (c *BranchCheckpoint) Cut() BranchCut {
	return BranchCut{Identity: c.identity, Sequence: c.sequence, End: c.end}
}

// RotateCommitted installs an acknowledged immutable base and retains every
// post-cut record, including overwrites. The NBD backend pointer stays the
// same. persist must durably record the new path, identity and base descriptor
// before returning nil. An error from persist may be an ambiguous local
// commit: both WALs are retained and subsequent writes fail closed until the
// session owner recovers its durable installation record.
//
// The caller serializes checkpoint/lifecycle operations, closes their captured
// WAL handles, and keeps a bounded suffix so the local copy does not exhaust
// the node disk or delay I/O for an unbounded interval.
func (b *Branch) RotateCommitted(cut BranchCut, base io.ReaderAt, rootDigest, destination string, persist func(BranchIdentity) error) error {
	if base == nil || persist == nil {
		return fmt.Errorf("committed base reader and durable installation callback are required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.retiring || b.checkpoints != 0 {
		return fmt.Errorf("branch is closed, retiring, or has an outstanding checkpoint")
	}
	if cut.Identity != b.identity || cut.Sequence < b.identity.SequenceBase || cut.Sequence > b.sequence ||
		cut.End != b.header+int64(cut.Sequence-b.identity.SequenceBase)*branchRecordBytes || cut.End > b.end {
		return fmt.Errorf("committed cut does not belong to the current branch")
	}
	destination = filepath.Clean(destination)
	if !filepath.IsAbs(destination) || destination == b.file.Name() || filepath.Dir(destination) != filepath.Dir(b.file.Name()) {
		return fmt.Errorf("rotation requires a distinct journal in the same directory")
	}
	identity := b.identity
	identity.BaseRootDigest = rootDigest
	identity.SequenceBase = cut.Sequence
	if err := identity.Validate(); err != nil {
		return err
	}
	// O_EXCL prevents a retry from truncating an already authoritative suffix.
	file, err := os.OpenFile(destination, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscallNoFollow, 0o600)
	if err != nil {
		return err
	}
	installed := false
	budgetAttached := false
	defer func() {
		if !installed {
			_ = file.Close()
			_ = os.Remove(destination)
			if budgetAttached {
				_ = b.nodeDirty.detach(destination)
				_ = b.nodeDirty.ReleaseOwner(destination)
			}
		}
	}()
	// Account for the duplicated suffix before allocating any record payload
	// on disk. Protected retirement headroom remains unavailable to rotation.
	if b.nodeDirty != nil {
		if err := b.nodeDirty.attach(destination, int64(b.sequence-cut.Sequence)*LogicalBlockSize, b.retirementGroup); err != nil {
			return err
		}
		budgetAttached = true
	}
	next := &Branch{file: file, base: base, identity: identity, blocks: make(map[uint64]branchRecord)}
	if err := next.createHeader(); err != nil {
		return err
	}
	if _, err := file.Seek(next.header, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.CopyN(file, io.NewSectionReader(b.file, cut.End, b.end-cut.End), b.end-cut.End); err != nil {
		return fmt.Errorf("copy post-cut branch records: %w", err)
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := next.scanRecords(next.header + b.end - cut.End); err != nil {
		return err
	}
	if err := syncBranchDirectory(destination); err != nil {
		return err
	}
	if err := persist(identity); err != nil {
		installed = true // Retain the possibly committed file on every error.
		_ = file.Close()
		if budgetAttached {
			_ = b.nodeDirty.detach(destination)
		}
		b.rotationFailure = err
		b.pressureChanged.Broadcast()
		return err
	}
	// No operation after the durable callback may restore the old pointer.
	old, oldOwner := b.file, b.dirtyOwner
	b.file, b.base = next.file, next.base
	b.identity.BaseRootDigest, b.identity.SequenceBase = identity.BaseRootDigest, identity.SequenceBase
	b.header, b.end = next.header, next.end
	b.sequence, b.durable = next.sequence, next.durable
	b.blocks, b.records = next.blocks, next.records
	b.dirtyOwner = destination
	installed = true
	closeErr := old.Close()
	removeErr := os.Remove(old.Name())
	if b.nodeDirty != nil {
		closeErr = errors.Join(closeErr, b.nodeDirty.detach(oldOwner))
		if removeErr == nil {
			closeErr = errors.Join(closeErr, b.nodeDirty.ReleaseOwner(oldOwner))
		}
	}
	b.pressure = nil
	b.pressureChanged.Broadcast()
	return errors.Join(closeErr, removeErr, syncBranchDirectory(destination))
}

func syncBranchDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
