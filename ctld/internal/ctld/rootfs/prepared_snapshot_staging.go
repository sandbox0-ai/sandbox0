package rootfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	defaultPreparedSnapshotMaxBytes       int64 = 8 << 30
	defaultPreparedSnapshotMaxTotalBytes  int64 = 32 << 30
	defaultPreparedSnapshotMaxEntries           = 64
	defaultPreparedSnapshotMaxTeamEntries       = 8
	defaultPreparedSnapshotMaxConcurrent        = 2
	defaultPreparedSnapshotMaxPerTeam           = 1
	defaultPreparedSnapshotTTL                  = 15 * time.Minute
	defaultPreparedSnapshotSweepInterval        = time.Minute
	preparedSnapshotMetadataMaxBytes            = 64 << 10
	preparedSnapshotLockName                    = ".staging.lock"
	preparedSnapshotReservationSuffix           = ".reservation"
	preparedSnapshotClockSkew                   = 5 * time.Second
)

var (
	ErrPreparedSnapshotTooLarge = errors.New("rootfs snapshot staging artifact is too large")
	ErrPreparedSnapshotCapacity = errors.New("rootfs snapshot staging capacity is exhausted")
	ErrPreparedSnapshotBusy     = errors.New("rootfs snapshot staging concurrency is exhausted")
)

type preparedSnapshotOwner struct {
	TeamID                    string `json:"team_id"`
	SandboxID                 string `json:"sandbox_id"`
	ExpectedRuntimeGeneration int64  `json:"expected_runtime_generation"`
}

type preparedSnapshotReservationRecord struct {
	Handle    string                `json:"handle"`
	Owner     preparedSnapshotOwner `json:"owner"`
	MaxBytes  int64                 `json:"max_bytes"`
	CreatedAt time.Time             `json:"created_at"`
	ExpiresAt time.Time             `json:"expires_at"`
}

type preparedSnapshotReservation struct {
	handle string
	path   string
	file   *os.File
	record preparedSnapshotReservationRecord
}

type preparedSnapshotGroup struct {
	handle       string
	paths        []string
	reservation  string
	owner        preparedSnapshotOwner
	reservedSize int64
	actualSize   int64
	ownedSize    int64
	modTime      time.Time
	expiresAt    time.Time
}

var preparedSnapshotProcessLocks sync.Map

func validatePreparedSnapshotOwner(teamID, sandboxID string, generation int64, expiresAt time.Time, maxTTL time.Duration) (preparedSnapshotOwner, error) {
	owner, err := validateRootFSCacheOwner(teamID, sandboxID)
	owner.ExpectedRuntimeGeneration = generation
	if err != nil {
		return preparedSnapshotOwner{}, err
	}
	if generation < 0 {
		return preparedSnapshotOwner{}, fmt.Errorf("%w: expected_runtime_generation must be non-negative", ErrBadRequest)
	}
	now := time.Now()
	if expiresAt.IsZero() || !expiresAt.After(now.Add(-preparedSnapshotClockSkew)) {
		return preparedSnapshotOwner{}, fmt.Errorf("%w: expires_at must be in the future", ErrBadRequest)
	}
	if maxTTL <= 0 || expiresAt.After(now.Add(maxTTL+preparedSnapshotClockSkew)) {
		return preparedSnapshotOwner{}, fmt.Errorf("%w: expires_at exceeds the node staging TTL", ErrBadRequest)
	}
	return owner, nil
}

func validateRootFSCacheOwner(teamID, sandboxID string) (preparedSnapshotOwner, error) {
	owner := preparedSnapshotOwner{
		TeamID:    strings.TrimSpace(teamID),
		SandboxID: strings.TrimSpace(sandboxID),
	}
	if owner.TeamID == "" {
		return preparedSnapshotOwner{}, fmt.Errorf("%w: team_id is required", ErrBadRequest)
	}
	if owner.SandboxID == "" {
		return preparedSnapshotOwner{}, fmt.Errorf("%w: sandbox_id is required", ErrBadRequest)
	}
	if strings.ContainsAny(owner.TeamID, `/\`) || strings.ContainsAny(owner.SandboxID, `/\`) {
		return preparedSnapshotOwner{}, fmt.Errorf("%w: team_id and sandbox_id cannot contain path separators", ErrBadRequest)
	}
	return owner, nil
}

func cleanPreparedSnapshotHandle(handle string) (string, error) {
	handle = strings.TrimSpace(handle)
	if handle == "" {
		return "", fmt.Errorf("%w: snapshot handle is required", ErrBadRequest)
	}
	if handle != filepath.Base(handle) || strings.ContainsAny(handle, `/\`) {
		return "", fmt.Errorf("%w: invalid snapshot handle", ErrBadRequest)
	}
	return handle, nil
}

// Start performs startup cleanup before scheduling periodic cleanup. Prepared
// snapshots are an untrusted, node-local handoff cache; process crashes must
// not turn them into permanent disk usage.
func (c *Controller) Start(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if err := c.SweepPreparedSnapshots(); err != nil {
		return err
	}
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := c.SweepPreparedSnapshots(); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "rootfs prepared snapshot sweep failed: %v\n", err)
		}
	}, c.preparedSnapshotSweepInterval)
	return nil
}

func (c *Controller) reservePreparedSnapshot(handle string, owner preparedSnapshotOwner, expiresAt time.Time) (*preparedSnapshotReservation, error) {
	handle, err := cleanPreparedSnapshotHandle(handle)
	if err != nil {
		return nil, err
	}
	var reservation *preparedSnapshotReservation
	err = c.withPreparedSnapshotLock(func() error {
		if err := c.sweepPreparedSnapshotsLocked(time.Now()); err != nil {
			return err
		}
		groups, err := c.preparedSnapshotGroupsLocked()
		if err != nil {
			return err
		}
		var totalBytes, teamBytes int64
		var concurrent, teamConcurrent, retainedEntries, teamRetainedEntries int
		for _, group := range groups {
			retainedEntries++
			groupSize := group.actualSize + group.reservedSize
			totalBytes, err = checkedAddPreparedSnapshotBytes(totalBytes, groupSize)
			if err != nil {
				return err
			}
			if group.reservation != "" {
				concurrent++
			}
			if group.owner.TeamID == owner.TeamID {
				teamRetainedEntries++
				teamBytes, err = checkedAddPreparedSnapshotBytes(teamBytes, group.ownedSize+group.reservedSize)
				if err != nil {
					return err
				}
				if group.reservation != "" {
					teamConcurrent++
				}
			}
		}
		if concurrent >= c.preparedSnapshotMaxConcurrent {
			return fmt.Errorf("%w: node limit is %d", ErrPreparedSnapshotBusy, c.preparedSnapshotMaxConcurrent)
		}
		if teamConcurrent >= c.preparedSnapshotMaxPerTeam {
			return fmt.Errorf("%w: team %s limit is %d", ErrPreparedSnapshotBusy, owner.TeamID, c.preparedSnapshotMaxPerTeam)
		}
		if retainedEntries >= c.preparedSnapshotMaxEntries {
			return fmt.Errorf("%w: node retained entry limit is %d", ErrPreparedSnapshotCapacity, c.preparedSnapshotMaxEntries)
		}
		if teamRetainedEntries >= c.preparedSnapshotMaxTeamEntries {
			return fmt.Errorf("%w: team %s retained entry limit is %d", ErrPreparedSnapshotCapacity, owner.TeamID, c.preparedSnapshotMaxTeamEntries)
		}
		if exceedsPreparedSnapshotLimit(totalBytes, c.preparedSnapshotMaxBytes, c.preparedSnapshotMaxTotalBytes) {
			return fmt.Errorf("%w: node byte limit is %d", ErrPreparedSnapshotCapacity, c.preparedSnapshotMaxTotalBytes)
		}
		if exceedsPreparedSnapshotLimit(teamBytes, c.preparedSnapshotMaxBytes, c.preparedSnapshotMaxTeamBytes) {
			return fmt.Errorf("%w: team %s byte limit is %d", ErrPreparedSnapshotCapacity, owner.TeamID, c.preparedSnapshotMaxTeamBytes)
		}
		if ok, err := hasFilesystemHeadroom(c.preparedSnapshotDir(), c.preparedSnapshotMinFreeBytes, c.preparedSnapshotMaxBytes); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("%w: node filesystem minimum free-space guard would be violated", ErrPreparedSnapshotCapacity)
		}

		record := preparedSnapshotReservationRecord{
			Handle:    handle,
			Owner:     owner,
			MaxBytes:  c.preparedSnapshotMaxBytes,
			CreatedAt: time.Now().UTC(),
			ExpiresAt: expiresAt.UTC(),
		}
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		raw = append(raw, '\n')
		path := c.preparedSnapshotReservationPath(handle)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
		closeFile := true
		defer func() {
			if closeFile {
				_ = file.Close()
				_ = os.Remove(path)
			}
		}()
		if _, err := file.Write(raw); err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}
		if err := unix.Flock(int(file.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
			return fmt.Errorf("lock rootfs snapshot staging reservation: %w", err)
		}
		closeFile = false
		reservation = &preparedSnapshotReservation{
			handle: handle,
			path:   path,
			file:   file,
			record: record,
		}
		return nil
	})
	return reservation, err
}

func (c *Controller) completePreparedSnapshotReservation(reservation *preparedSnapshotReservation) error {
	if reservation == nil {
		return fmt.Errorf("rootfs snapshot staging reservation is required")
	}
	return c.withPreparedSnapshotLock(func() error {
		if err := closePreparedSnapshotReservation(reservation); err != nil {
			return err
		}
		if err := os.Remove(reservation.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	})
}

func (c *Controller) discardPreparedSnapshot(handle string, reservation *preparedSnapshotReservation) error {
	return c.withPreparedSnapshotLock(func() error {
		if reservation != nil {
			if err := closePreparedSnapshotReservation(reservation); err != nil {
				return err
			}
		}
		return c.removePreparedSnapshotFiles(handle)
	})
}

func closePreparedSnapshotReservation(reservation *preparedSnapshotReservation) error {
	if reservation == nil || reservation.file == nil {
		return nil
	}
	file := reservation.file
	reservation.file = nil
	unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	closeErr := file.Close()
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}

func (c *Controller) openPreparedSnapshot(handle string) (preparedRootFSSnapshot, *os.File, error) {
	handle, err := cleanPreparedSnapshotHandle(handle)
	if err != nil {
		return preparedRootFSSnapshot{}, nil, err
	}
	var prepared preparedRootFSSnapshot
	var content *os.File
	err = c.withPreparedSnapshotLock(func() error {
		metaFile, err := os.Open(c.preparedSnapshotMetaPath(handle))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%w: rootfs snapshot handle %s", ErrNotFound, handle)
			}
			return err
		}
		defer metaFile.Close()
		raw, err := io.ReadAll(io.LimitReader(metaFile, preparedSnapshotMetadataMaxBytes+1))
		if err != nil {
			return err
		}
		if len(raw) > preparedSnapshotMetadataMaxBytes {
			return fmt.Errorf("%w: rootfs snapshot metadata exceeds %d bytes", ErrPreparedSnapshotTooLarge, preparedSnapshotMetadataMaxBytes)
		}
		if err := json.Unmarshal(raw, &prepared); err != nil {
			return fmt.Errorf("%w: decode prepared rootfs snapshot metadata: %v", ErrConflict, err)
		}
		if prepared.Handle != handle {
			return fmt.Errorf("%w: prepared rootfs snapshot handle mismatch", ErrConflict)
		}
		if prepared.CreatedAt.IsZero() || prepared.ExpiresAt.IsZero() || !time.Now().Before(prepared.ExpiresAt) {
			_ = c.removePreparedSnapshotFiles(handle)
			return fmt.Errorf("%w: rootfs snapshot handle %s expired", ErrNotFound, handle)
		}
		content, err = os.Open(c.preparedSnapshotContentPath(handle))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%w: rootfs snapshot handle %s", ErrNotFound, handle)
			}
			return err
		}
		info, err := content.Stat()
		if err != nil {
			_ = content.Close()
			content = nil
			return err
		}
		if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > c.preparedSnapshotMaxBytes {
			_ = content.Close()
			content = nil
			return fmt.Errorf("%w: invalid prepared rootfs snapshot content", ErrConflict)
		}
		if info.Size() != prepared.Descriptor.Size {
			_ = content.Close()
			content = nil
			return fmt.Errorf("%w: prepared rootfs snapshot size %d does not match descriptor size %d", ErrConflict, info.Size(), prepared.Descriptor.Size)
		}
		return nil
	})
	if err != nil {
		return preparedRootFSSnapshot{}, nil, err
	}
	return prepared, content, nil
}

func (c *Controller) removePreparedSnapshotHandle(handle string) error {
	return c.withPreparedSnapshotLock(func() error {
		group, err := c.preparedSnapshotGroupLocked(handle)
		if err != nil {
			return err
		}
		if group == nil {
			return os.ErrNotExist
		}
		if group.reservation != "" {
			locked, release, err := tryLockPreparedSnapshotReservation(group.reservation)
			if err != nil {
				return err
			}
			if !locked {
				return fmt.Errorf("%w: rootfs snapshot handle %s is still being prepared", ErrConflict, handle)
			}
			defer release()
		}
		return removePreparedSnapshotGroup(group)
	})
}

func (c *Controller) removePreparedSnapshotFiles(handle string) error {
	handle, err := cleanPreparedSnapshotHandle(handle)
	if err != nil {
		return err
	}
	paths := []string{
		c.preparedSnapshotContentPath(handle),
		c.preparedSnapshotContentPath(handle) + ".tmp",
		c.preparedSnapshotMetaPath(handle),
		c.preparedSnapshotMetaPath(handle) + ".tmp",
		c.preparedSnapshotReservationPath(handle),
	}
	found := false
	for _, path := range paths {
		if err := os.Remove(path); err == nil {
			found = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if !found {
		return os.ErrNotExist
	}
	return nil
}

func (c *Controller) preparedSnapshotReservationPath(handle string) string {
	return filepath.Join(c.preparedSnapshotDir(), filepath.Base(handle)+preparedSnapshotReservationSuffix)
}

// SweepPreparedSnapshots removes expired crash leftovers and, after a limit
// decrease or upgrade from an unbounded version, evicts the oldest unlocked
// groups until the configured node total and minimum-free guard are restored.
func (c *Controller) SweepPreparedSnapshots() error {
	if c == nil {
		return nil
	}
	return c.withPreparedSnapshotLock(func() error {
		return c.sweepPreparedSnapshotsLocked(time.Now())
	})
}

func (c *Controller) sweepPreparedSnapshotsLocked(now time.Time) error {
	groups, err := c.preparedSnapshotGroupsLocked()
	if err != nil {
		return err
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].modTime.Equal(groups[j].modTime) {
			return groups[i].handle < groups[j].handle
		}
		return groups[i].modTime.Before(groups[j].modTime)
	})
	cutoff := now.Add(-c.preparedSnapshotTTL)
	for _, group := range groups {
		if (group.expiresAt.IsZero() || now.Before(group.expiresAt)) && group.modTime.After(cutoff) {
			continue
		}
		removed, err := removeUnlockedPreparedSnapshotGroup(group)
		if err != nil {
			return err
		}
		if removed {
			group.paths = nil
			group.actualSize = 0
			group.ownedSize = 0
			group.reservedSize = 0
		}
	}

	var total int64
	var retainedEntries int
	for _, group := range groups {
		if len(group.paths) == 0 {
			continue
		}
		retainedEntries++
		total, err = checkedAddPreparedSnapshotBytes(total, group.actualSize+group.reservedSize)
		if err != nil {
			return err
		}
	}
	for _, group := range groups {
		headroom, headroomErr := hasFilesystemHeadroom(c.preparedSnapshotDir(), c.preparedSnapshotMinFreeBytes, 0)
		if headroomErr != nil {
			return headroomErr
		}
		if total <= c.preparedSnapshotMaxTotalBytes &&
			retainedEntries <= c.preparedSnapshotMaxEntries &&
			headroom {
			break
		}
		if len(group.paths) == 0 {
			continue
		}
		removed, err := removeUnlockedPreparedSnapshotGroup(group)
		if err != nil {
			return err
		}
		if removed {
			total -= group.actualSize + group.reservedSize
			retainedEntries--
			group.paths = nil
			group.actualSize = 0
			group.ownedSize = 0
			group.reservedSize = 0
		}
	}

	// A team can otherwise accumulate arbitrarily many tiny completed
	// artifacts while staying below its byte limit. Evict that team's oldest
	// unlocked entries deterministically until both retained guards hold.
	teamBytes := make(map[string]int64)
	teamEntries := make(map[string]int)
	for _, group := range groups {
		if len(group.paths) == 0 || group.owner.TeamID == "" {
			continue
		}
		teamBytes[group.owner.TeamID], err = checkedAddPreparedSnapshotBytes(
			teamBytes[group.owner.TeamID],
			group.ownedSize+group.reservedSize,
		)
		if err != nil {
			return err
		}
		teamEntries[group.owner.TeamID]++
	}
	for _, group := range groups {
		teamID := group.owner.TeamID
		if len(group.paths) == 0 || teamID == "" {
			continue
		}
		if teamEntries[teamID] <= c.preparedSnapshotMaxTeamEntries &&
			teamBytes[teamID] <= c.preparedSnapshotMaxTeamBytes {
			continue
		}
		removed, err := removeUnlockedPreparedSnapshotGroup(group)
		if err != nil {
			return err
		}
		if removed {
			teamEntries[teamID]--
			teamBytes[teamID] -= group.ownedSize + group.reservedSize
		}
	}
	return nil
}

func (c *Controller) preparedSnapshotGroupsLocked() ([]*preparedSnapshotGroup, error) {
	if err := os.MkdirAll(c.preparedSnapshotDir(), 0o700); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(c.preparedSnapshotDir())
	if err != nil {
		return nil, err
	}
	byHandle := make(map[string]*preparedSnapshotGroup)
	for _, entry := range entries {
		if entry.Name() == preparedSnapshotLockName || entry.IsDir() {
			continue
		}
		handle := preparedSnapshotHandleFromFile(entry.Name())
		if handle == "" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		group := byHandle[handle]
		if group == nil {
			group = &preparedSnapshotGroup{handle: handle}
			byHandle[handle] = group
		}
		path := filepath.Join(c.preparedSnapshotDir(), entry.Name())
		group.paths = append(group.paths, path)
		group.actualSize, err = checkedAddPreparedSnapshotBytes(group.actualSize, info.Size())
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(entry.Name(), ".tar") || strings.HasSuffix(entry.Name(), ".tar.tmp") {
			group.ownedSize, err = checkedAddPreparedSnapshotBytes(group.ownedSize, info.Size())
			if err != nil {
				return nil, err
			}
		}
		if info.ModTime().After(group.modTime) {
			group.modTime = info.ModTime()
		}
		if strings.HasSuffix(entry.Name(), preparedSnapshotReservationSuffix) {
			group.reservation = path
			record, recordErr := readPreparedSnapshotReservation(path)
			if recordErr == nil {
				group.owner = record.Owner
				group.reservedSize = max(record.MaxBytes, c.preparedSnapshotMaxBytes)
				group.expiresAt = record.ExpiresAt
			} else {
				// A corrupt marker is still charged conservatively and requires
				// TTL/pressure cleanup before it can stop blocking admission.
				group.reservedSize = c.preparedSnapshotMaxBytes
			}
		} else if strings.HasSuffix(entry.Name(), ".json") && group.owner.TeamID == "" {
			if prepared, preparedErr := readPreparedSnapshotMetadata(path); preparedErr == nil {
				group.owner = prepared.Owner
				group.expiresAt = prepared.ExpiresAt
			}
		}
	}
	groups := make([]*preparedSnapshotGroup, 0, len(byHandle))
	for _, group := range byHandle {
		groups = append(groups, group)
	}
	return groups, nil
}

func (c *Controller) preparedSnapshotGroupLocked(handle string) (*preparedSnapshotGroup, error) {
	groups, err := c.preparedSnapshotGroupsLocked()
	if err != nil {
		return nil, err
	}
	for _, group := range groups {
		if group.handle == handle {
			return group, nil
		}
	}
	return nil, nil
}

func preparedSnapshotHandleFromFile(name string) string {
	for _, suffix := range []string{
		".tar.tmp",
		".json.tmp",
		preparedSnapshotReservationSuffix,
		".tar",
		".json",
	} {
		if strings.HasSuffix(name, suffix) {
			return strings.TrimSuffix(name, suffix)
		}
	}
	return ""
}

func readPreparedSnapshotReservation(path string) (preparedSnapshotReservationRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return preparedSnapshotReservationRecord{}, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, preparedSnapshotMetadataMaxBytes+1))
	if err != nil {
		return preparedSnapshotReservationRecord{}, err
	}
	if len(raw) > preparedSnapshotMetadataMaxBytes {
		return preparedSnapshotReservationRecord{}, fmt.Errorf("rootfs staging reservation metadata is too large")
	}
	var record preparedSnapshotReservationRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return preparedSnapshotReservationRecord{}, err
	}
	return record, nil
}

func readPreparedSnapshotMetadata(path string) (preparedRootFSSnapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return preparedRootFSSnapshot{}, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, preparedSnapshotMetadataMaxBytes+1))
	if err != nil {
		return preparedRootFSSnapshot{}, err
	}
	if len(raw) > preparedSnapshotMetadataMaxBytes {
		return preparedRootFSSnapshot{}, fmt.Errorf("rootfs staging metadata is too large")
	}
	var prepared preparedRootFSSnapshot
	if err := json.Unmarshal(raw, &prepared); err != nil {
		return preparedRootFSSnapshot{}, err
	}
	return prepared, nil
}

func removeUnlockedPreparedSnapshotGroup(group *preparedSnapshotGroup) (bool, error) {
	if group == nil || len(group.paths) == 0 {
		return false, nil
	}
	if group.reservation != "" {
		locked, release, err := tryLockPreparedSnapshotReservation(group.reservation)
		if err != nil {
			return false, err
		}
		if !locked {
			return false, nil
		}
		defer release()
	}
	return true, removePreparedSnapshotGroup(group)
}

func tryLockPreparedSnapshotReservation(path string) (bool, func(), error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, func() {}, nil
		}
		return false, nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return false, func() {}, nil
		}
		return false, nil, err
	}
	return true, func() {
		_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
		_ = file.Close()
	}, nil
}

func removePreparedSnapshotGroup(group *preparedSnapshotGroup) error {
	var firstErr error
	for _, path := range group.paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *Controller) withPreparedSnapshotLock(fn func() error) error {
	if c == nil {
		return fmt.Errorf("rootfs controller is required")
	}
	dir := c.preparedSnapshotDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	lockValue, _ := preparedSnapshotProcessLocks.LoadOrStore(filepath.Clean(dir), &sync.Mutex{})
	processLock := lockValue.(*sync.Mutex)
	processLock.Lock()
	defer processLock.Unlock()

	lockFile, err := os.OpenFile(filepath.Join(dir, preparedSnapshotLockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lockFile.Close()
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		return fmt.Errorf("lock rootfs snapshot staging: %w", err)
	}
	defer func() { _ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN) }()
	return fn()
}

func checkedAddPreparedSnapshotBytes(left, right int64) (int64, error) {
	if left < 0 || right < 0 || left > int64(^uint64(0)>>1)-right {
		return 0, fmt.Errorf("%w: rootfs snapshot staging byte accounting overflow", ErrPreparedSnapshotCapacity)
	}
	return left + right, nil
}

func exceedsPreparedSnapshotLimit(current, additional, limit int64) bool {
	if current < 0 || additional < 0 || limit < 0 {
		return true
	}
	return current > limit || additional > limit-current
}

func hasFilesystemHeadroom(dir string, minFreeBytes, additionalBytes int64) (bool, error) {
	if minFreeBytes <= 0 && additionalBytes <= 0 {
		return true, nil
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(dir, &stat); err != nil {
		return false, fmt.Errorf("inspect rootfs cache filesystem capacity: %w", err)
	}
	blockSize := int64(stat.Bsize)
	if blockSize <= 0 || uint64(stat.Bavail) > uint64((^uint64(0)>>1)/uint64(blockSize)) {
		return false, fmt.Errorf("rootfs cache filesystem free-space accounting overflow")
	}
	free := int64(stat.Bavail) * blockSize
	required, err := checkedAddPreparedSnapshotBytes(minFreeBytes, additionalBytes)
	if err != nil {
		return false, err
	}
	return free >= required, nil
}

type rootFSDiffMaxBytesContextKey struct{}

func withRootFSDiffMaxBytes(ctx context.Context, maxBytes int64) context.Context {
	return context.WithValue(ctx, rootFSDiffMaxBytesContextKey{}, maxBytes)
}

func rootFSDiffMaxBytes(ctx context.Context) int64 {
	if ctx == nil {
		return 0
	}
	value, _ := ctx.Value(rootFSDiffMaxBytesContextKey{}).(int64)
	return max(value, 0)
}

type rootFSDiffLimitWriter struct {
	writer   io.Writer
	maxBytes int64
	written  int64
	tooLarge bool
}

func (w *rootFSDiffLimitWriter) Write(payload []byte) (int, error) {
	if w.maxBytes <= 0 {
		n, err := w.writer.Write(payload)
		w.written += int64(n)
		return n, err
	}
	remaining := w.maxBytes - w.written
	if remaining <= 0 {
		w.tooLarge = len(payload) > 0
		return 0, ErrPreparedSnapshotTooLarge
	}
	if int64(len(payload)) > remaining {
		n, err := w.writer.Write(payload[:remaining])
		w.written += int64(n)
		w.tooLarge = true
		if err != nil {
			return n, err
		}
		return n, ErrPreparedSnapshotTooLarge
	}
	n, err := w.writer.Write(payload)
	w.written += int64(n)
	return n, err
}

func limitedRootFSDiffWriter(ctx context.Context, writer io.Writer) io.Writer {
	maxBytes := rootFSDiffMaxBytes(ctx)
	if maxBytes <= 0 {
		return writer
	}
	return &rootFSDiffLimitWriter{writer: writer, maxBytes: maxBytes}
}
