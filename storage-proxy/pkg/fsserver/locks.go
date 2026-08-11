package fsserver

import (
	"context"
	"math"
	"sync"
	"syscall"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

// fileLockManager implements the kernel-visible lifetime of advisory locks.
// Locks deliberately remain node-local: a mounted S0FS volume has one primary
// storage-proxy, and locks must disappear rather than be persisted on restart.
type fileLockManager struct {
	mu      sync.Mutex
	locks   map[fileLockKey][]heldFileLock
	changed chan struct{}
}

type fileLockKey struct {
	volumeID string
	inode    uint64
	flock    bool
}

type heldFileLock struct {
	handleID uint64
	owner    uint64
	start    uint64
	end      uint64
	typ      uint32
	pid      uint32
}

func newFileLockManager() *fileLockManager {
	return &fileLockManager{
		locks:   make(map[fileLockKey][]heldFileLock),
		changed: make(chan struct{}),
	}
}

func (m *fileLockManager) get(volumeID string, inode, handleID, owner uint64, requested *pb.FileLock) *pb.FileLock {
	if m == nil || requested == nil {
		return &pb.FileLock{Typ: uint32(syscall.F_UNLCK)}
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	wanted := heldFileLock{
		handleID: handleID,
		owner:    lockOwner(owner, handleID),
		start:    requested.Start,
		end:      requested.End,
		typ:      requested.Typ,
		pid:      requested.Pid,
	}
	if conflict, ok := firstLockConflict(m.locks[fileLockKey{volumeID: volumeID, inode: inode}], wanted); ok {
		return &pb.FileLock{Start: conflict.start, End: conflict.end, Typ: conflict.typ, Pid: conflict.pid}
	}
	return &pb.FileLock{Start: requested.Start, End: requested.End, Typ: uint32(syscall.F_UNLCK), Pid: requested.Pid}
}

func (m *fileLockManager) set(ctx context.Context, volumeID string, inode, handleID, owner uint64, requested *pb.FileLock, block bool) error {
	if m == nil || requested == nil {
		return fserror.NewErrno(syscall.EINVAL, "lock is required")
	}
	if requested.End < requested.Start {
		return fserror.NewErrno(syscall.EINVAL, "lock end is before start")
	}
	switch requested.Typ {
	case uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK):
	default:
		return fserror.NewErrno(syscall.EINVAL, "invalid lock type")
	}

	wanted := heldFileLock{
		handleID: handleID,
		owner:    lockOwner(owner, handleID),
		start:    requested.Start,
		end:      requested.End,
		typ:      requested.Typ,
		pid:      requested.Pid,
	}
	key := fileLockKey{volumeID: volumeID, inode: inode}
	for {
		m.mu.Lock()
		if wanted.typ == uint32(syscall.F_UNLCK) {
			m.locks[key] = subtractOwnedLockRange(m.locks[key], wanted.owner, wanted.start, wanted.end)
			m.removeEmptyKeyLocked(key)
			m.signalChangedLocked()
			m.mu.Unlock()
			return nil
		}
		if _, conflict := firstLockConflict(m.locks[key], wanted); !conflict {
			remaining := subtractOwnedLockRange(m.locks[key], wanted.owner, wanted.start, wanted.end)
			m.locks[key] = append(remaining, wanted)
			m.signalChangedLocked()
			m.mu.Unlock()
			return nil
		}
		if !block {
			m.mu.Unlock()
			return fserror.NewErrno(syscall.EAGAIN, "file lock would block")
		}
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return fserror.New(fserror.Canceled, ctx.Err().Error())
		case <-changed:
		}
	}
}

func (m *fileLockManager) flock(ctx context.Context, volumeID string, inode, handleID, owner uint64, typ uint32, block bool) error {
	return m.setFlock(ctx, fileLockKey{volumeID: volumeID, inode: inode, flock: true}, heldFileLock{
		handleID: handleID,
		owner:    lockOwner(owner, handleID),
		start:    0,
		end:      math.MaxUint64,
		typ:      typ,
	}, block)
}

func (m *fileLockManager) setFlock(ctx context.Context, key fileLockKey, wanted heldFileLock, block bool) error {
	switch wanted.typ {
	case uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK):
	default:
		return fserror.NewErrno(syscall.EINVAL, "invalid flock type")
	}
	for {
		m.mu.Lock()
		if wanted.typ == uint32(syscall.F_UNLCK) {
			m.locks[key] = removeLockOwner(m.locks[key], wanted.owner)
			m.removeEmptyKeyLocked(key)
			m.signalChangedLocked()
			m.mu.Unlock()
			return nil
		}
		if _, conflict := firstLockConflict(m.locks[key], wanted); !conflict {
			m.locks[key] = append(removeLockOwner(m.locks[key], wanted.owner), wanted)
			m.signalChangedLocked()
			m.mu.Unlock()
			return nil
		}
		if !block {
			m.mu.Unlock()
			return fserror.NewErrno(syscall.EWOULDBLOCK, "flock would block")
		}
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return fserror.New(fserror.Canceled, ctx.Err().Error())
		case <-changed:
		}
	}
}

func (m *fileLockManager) releaseHandle(volumeID string, handleID uint64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	for key, locks := range m.locks {
		if key.volumeID != volumeID {
			continue
		}
		kept := locks[:0]
		for _, lock := range locks {
			if lock.handleID == handleID {
				changed = true
				continue
			}
			kept = append(kept, lock)
		}
		if len(kept) == 0 {
			delete(m.locks, key)
		} else {
			m.locks[key] = kept
		}
	}
	if changed {
		m.signalChangedLocked()
	}
}

func lockOwner(owner, handleID uint64) uint64 {
	if owner != 0 {
		return owner
	}
	return handleID
}

func firstLockConflict(locks []heldFileLock, wanted heldFileLock) (heldFileLock, bool) {
	for _, existing := range locks {
		if existing.owner == wanted.owner || !lockRangesOverlap(existing, wanted) {
			continue
		}
		if existing.typ == uint32(syscall.F_RDLCK) && wanted.typ == uint32(syscall.F_RDLCK) {
			continue
		}
		return existing, true
	}
	return heldFileLock{}, false
}

func lockRangesOverlap(left, right heldFileLock) bool {
	return left.start <= right.end && right.start <= left.end
}

func subtractOwnedLockRange(locks []heldFileLock, owner, start, end uint64) []heldFileLock {
	result := make([]heldFileLock, 0, len(locks)+1)
	for _, existing := range locks {
		if existing.owner != owner || existing.end < start || end < existing.start {
			result = append(result, existing)
			continue
		}
		if existing.start < start {
			left := existing
			left.end = start - 1
			result = append(result, left)
		}
		if end < existing.end && end != math.MaxUint64 {
			right := existing
			right.start = end + 1
			result = append(result, right)
		}
	}
	return result
}

func removeLockOwner(locks []heldFileLock, owner uint64) []heldFileLock {
	result := locks[:0]
	for _, existing := range locks {
		if existing.owner != owner {
			result = append(result, existing)
		}
	}
	return result
}

func (m *fileLockManager) removeEmptyKeyLocked(key fileLockKey) {
	if len(m.locks[key]) == 0 {
		delete(m.locks, key)
	}
}

func (m *fileLockManager) signalChangedLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}
