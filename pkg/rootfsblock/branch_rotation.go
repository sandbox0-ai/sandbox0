package rootfsblock

import (
	"bytes"
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

// Rotation copies at most this suffix while holding the NBD branch lock.
// Larger suffixes must be included in a newer acknowledged cut instead.
const MaxRotationSuffixBytes = 8 << 20

func (c *BranchCheckpoint) Cut() BranchCut {
	return BranchCut{Identity: c.identity, Sequence: c.sequence, End: c.end}
}

// RotateCommitted installs an acknowledged immutable base and retains every
// post-cut record, including overwrites. The NBD backend pointer stays the
// same. persist must durably record the new path, identity and base descriptor
// before returning nil. An error from persist may be an ambiguous local
// commit: both WALs are retained and subsequent writes wait until the
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
	if b.rotationFailure != nil {
		return fmt.Errorf("recover the durable journal selection before another rotation: %w", b.rotationFailure)
	}
	if err := b.validateCutLocked(cut); err != nil {
		return err
	}
	if suffix := int64(b.sequence-cut.Sequence) * LogicalBlockSize; suffix > MaxRotationSuffixBytes {
		return &DirtyTailCapacityError{Scope: "rotation", UsedBytes: suffix, LimitBytes: MaxRotationSuffixBytes}
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
	installed = true
	return b.installRotationLocked(next, destination)
}

func (b *Branch) installRotationLocked(next *Branch, destination string) error {
	old, oldOwner := b.file, b.dirtyOwner
	b.file, b.base = next.file, next.base
	b.identity.BaseRootDigest, b.identity.SequenceBase = next.identity.BaseRootDigest, next.identity.SequenceBase
	b.header, b.end = next.header, next.end
	b.sequence, b.durable = next.sequence, next.durable
	b.blocks, b.records = next.blocks, next.records
	b.dirtyOwner = destination
	b.rotationFailure = nil
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

// RecoverRotation applies the owner's durably selected journal after an
// ambiguous installation callback. It preserves the same NBD backend and
// verifies the complete candidate suffix against the still-complete old WAL.
// The session owner must read its durable pointer before choosing this path.
func (b *Branch) RecoverRotation(selectedPath string, identity BranchIdentity, base io.ReaderAt) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.retiring || b.checkpoints != 0 || b.rotationFailure == nil || base == nil {
		return fmt.Errorf("branch is not recoverable at a rotation boundary")
	}
	if selectedPath == b.file.Name() && identity == b.identity {
		b.rotationFailure = nil
		b.pressureChanged.Broadcast()
		return nil
	}
	if !filepath.IsAbs(selectedPath) || filepath.Clean(selectedPath) != selectedPath || filepath.Dir(selectedPath) != filepath.Dir(b.file.Name()) ||
		identity.RootFSID != b.identity.RootFSID || identity.GenerationID != b.identity.GenerationID || identity.WriterEpoch != b.identity.WriterEpoch ||
		identity.LogicalSizeBytes != b.identity.LogicalSizeBytes || identity.Version != b.identity.Version ||
		identity.SequenceBase <= b.identity.SequenceBase || identity.SequenceBase > b.sequence {
		return fmt.Errorf("selected rotation does not match the immutable branch owner")
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	file, err := os.OpenFile(selectedPath, os.O_RDWR|syscallNoFollow, 0)
	if err != nil {
		return err
	}
	installed := false
	defer func() {
		if !installed {
			_ = file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	_, header, err := readBranchHeader(file)
	if err != nil {
		return err
	}
	tailBytes := int64(b.sequence-identity.SequenceBase) * branchRecordBytes
	if !info.Mode().IsRegular() || info.Size() != header+tailBytes {
		return fmt.Errorf("selected rotation suffix is incomplete")
	}
	next := &Branch{file: file, base: base, identity: identity, blocks: make(map[uint64]branchRecord)}
	if err = next.open(); err != nil {
		return err
	}
	if next.sequence != b.sequence {
		return fmt.Errorf("selected rotation sequence does not match the complete old WAL")
	}
	oldOffset := b.header + int64(identity.SequenceBase-b.identity.SequenceBase)*branchRecordBytes
	oldBuffer, newBuffer := make([]byte, 64<<10), make([]byte, 64<<10)
	for offset := int64(0); offset < tailBytes; {
		length := int64(len(oldBuffer))
		if length > tailBytes-offset {
			length = tailBytes - offset
		}
		if _, err = b.file.ReadAt(oldBuffer[:length], oldOffset+offset); err != nil {
			return err
		}
		if _, err = file.ReadAt(newBuffer[:length], header+offset); err != nil {
			return err
		}
		if !bytes.Equal(oldBuffer[:length], newBuffer[:length]) {
			return fmt.Errorf("selected rotation changed a post-cut record")
		}
		offset += length
	}
	if b.nodeDirty != nil {
		if err = b.nodeDirty.attach(selectedPath, int64(next.records.count)*LogicalBlockSize, b.retirementGroup); err != nil {
			return err
		}
	}
	installed = true
	return b.installRotationLocked(next, selectedPath)
}

func (b *Branch) RotationFailure() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.rotationFailure
}

func syncBranchDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
