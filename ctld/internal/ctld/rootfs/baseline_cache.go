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
	"syscall"
	"time"

	"github.com/containerd/continuity/fs"
	"github.com/containerd/continuity/sysx"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	defaultRootFSBaselineMaxBytes       int64 = 8 << 30
	defaultRootFSBaselineMaxTotalBytes  int64 = 16 << 30
	defaultRootFSBaselineMaxEntries           = 64
	defaultRootFSBaselineMaxTeamEntries       = 8
	defaultRootFSBaselineTTL                  = time.Hour
	defaultRootFSBaselineSweepInterval        = time.Minute
	rootFSBaselineEntryOverhead         int64 = 4 << 10
	rootFSBaselineMetadataMaxBytes            = 64 << 10
	rootFSBaselineLockName                    = ".baseline.lock"
	rootFSBaselineMetadataName                = "metadata.json"
)

var ErrRootFSBaselineCacheCapacity = errors.New("rootfs baseline cache capacity is exhausted")

type rootFSBaselineMetadata struct {
	TeamID    string    `json:"team_id"`
	SandboxID string    `json:"sandbox_id"`
	LayerID   string    `json:"layer_id"`
	SizeBytes int64     `json:"size_bytes"`
	CreatedAt time.Time `json:"created_at"`
}

type rootFSBaselineEntry struct {
	path    string
	teamID  string
	size    int64
	modTime time.Time
}

type rootFSBaselineInode struct {
	device uint64
	inode  uint64
}

type rootFSBaselineBudget struct {
	max  int64
	used int64
}

var rootFSBaselineProcessLocks sync.Map

// StartBaselineCache removes crash leftovers synchronously, then keeps the
// disposable cache within its TTL, byte, entry-count, and free-space limits.
func (r *ContainerdRuntime) StartBaselineCache(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if err := r.SweepRootFSBaselines(); err != nil {
		return err
	}
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := r.SweepRootFSBaselines(); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "rootfs baseline cache sweep failed: %v\n", err)
		}
	}, r.baselineSweepInterval)
	return nil
}

func (r *ContainerdRuntime) captureRootFSBaseline(
	ctx context.Context,
	info ctldapi.RootFSInfo,
	teamID string,
	sandboxID string,
	layerID string,
	upperdir string,
	filter rootFSPathFilter,
) error {
	if _, err := validateRootFSCacheOwner(teamID, sandboxID); err != nil {
		return err
	}
	return r.withRootFSBaselineLock(func() error {
		if err := r.sweepRootFSBaselinesLocked(time.Now()); err != nil {
			return err
		}
		targetData := r.rootFSBaselinePath(info, layerID)
		targetEntry := filepath.Dir(targetData)
		// A baseline is only an acceleration cache. Removing an older copy before
		// replacement gives the new bounded capture its full configured budget.
		if err := os.RemoveAll(targetEntry); err != nil {
			return fmt.Errorf("replace rootfs baseline cache entry: %w", err)
		}
		if err := r.makeRootFSBaselineRoomLocked(targetEntry, teamID); err != nil {
			return err
		}

		root := r.rootFSBaselineRoot()
		tmpEntry, err := os.MkdirTemp(root, ".baseline-*")
		if err != nil {
			return fmt.Errorf("create rootfs baseline temp entry: %w", err)
		}
		removeTmp := true
		defer func() {
			if removeTmp {
				_ = os.RemoveAll(tmpEntry)
			}
		}()
		dataDir := filepath.Join(tmpEntry, "data")
		sizeBytes, err := copyRootFSTreeBounded(ctx, dataDir, upperdir, filter, r.baselineMaxBytes)
		if err != nil {
			return fmt.Errorf("copy bounded rootfs baseline: %w", err)
		}
		raw, err := json.Marshal(rootFSBaselineMetadata{
			TeamID:    teamID,
			SandboxID: sandboxID,
			LayerID:   strings.TrimSpace(layerID),
			SizeBytes: sizeBytes,
			CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			return err
		}
		entrySize, err := checkedAddPreparedSnapshotBytes(
			sizeBytes,
			rootFSBaselineEntryOverhead+int64(len(raw)+1),
		)
		if err != nil || entrySize > r.baselineMaxBytes {
			return fmt.Errorf(
				"%w: baseline entry including metadata exceeds %d bytes",
				ErrRootFSBaselineCacheCapacity,
				r.baselineMaxBytes,
			)
		}
		if err := os.WriteFile(filepath.Join(tmpEntry, rootFSBaselineMetadataName), append(raw, '\n'), 0o600); err != nil {
			return fmt.Errorf("write rootfs baseline metadata: %w", err)
		}
		if err := os.Rename(tmpEntry, targetEntry); err != nil {
			return fmt.Errorf("publish rootfs baseline cache entry: %w", err)
		}
		removeTmp = false
		return nil
	})
}

func (r *ContainerdRuntime) makeRootFSBaselineRoomLocked(excludedTarget, teamID string) error {
	entries, total, err := r.rootFSBaselineEntriesLocked()
	if err != nil {
		return err
	}
	kept := entries[:0]
	for _, entry := range entries {
		if filepath.Clean(entry.path) == filepath.Clean(excludedTarget) {
			total -= entry.size
			continue
		}
		kept = append(kept, entry)
	}
	entries = kept
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].modTime.Equal(entries[j].modTime) {
			return entries[i].path < entries[j].path
		}
		return entries[i].modTime.Before(entries[j].modTime)
	})
	var teamBytes int64
	var teamEntries int
	for _, entry := range entries {
		if entry.teamID == teamID {
			teamBytes, err = checkedAddPreparedSnapshotBytes(teamBytes, entry.size)
			if err != nil {
				return err
			}
			teamEntries++
		}
	}
	for teamEntries+1 > r.baselineMaxTeamEntries ||
		exceedsPreparedSnapshotLimit(teamBytes, r.baselineMaxBytes, r.baselineMaxTeamBytes) {
		index := -1
		for i, entry := range entries {
			if entry.teamID == teamID {
				index = i
				break
			}
		}
		if index < 0 {
			return fmt.Errorf(
				"%w: team %s needs max_bytes=%d max_team_bytes=%d max_team_entries=%d",
				ErrRootFSBaselineCacheCapacity,
				teamID,
				r.baselineMaxBytes,
				r.baselineMaxTeamBytes,
				r.baselineMaxTeamEntries,
			)
		}
		entry := entries[index]
		entries = append(entries[:index], entries[index+1:]...)
		if err := os.RemoveAll(entry.path); err != nil {
			return err
		}
		total -= entry.size
		teamBytes -= entry.size
		teamEntries--
	}
	for len(entries)+1 > r.baselineMaxEntries ||
		exceedsPreparedSnapshotLimit(total, r.baselineMaxBytes, r.baselineMaxTotalBytes) ||
		!rootFSBaselineHasHeadroom(r.rootFSBaselineRoot(), r.baselineMinFreeBytes, r.baselineMaxBytes) {
		if len(entries) == 0 {
			return fmt.Errorf(
				"%w: need max_bytes=%d max_total_bytes=%d max_entries=%d min_free_bytes=%d",
				ErrRootFSBaselineCacheCapacity,
				r.baselineMaxBytes,
				r.baselineMaxTotalBytes,
				r.baselineMaxEntries,
				r.baselineMinFreeBytes,
			)
		}
		entry := entries[0]
		entries = entries[1:]
		if err := os.RemoveAll(entry.path); err != nil {
			return err
		}
		total -= entry.size
	}
	return nil
}

func rootFSBaselineHasHeadroom(dir string, minFreeBytes, additionalBytes int64) bool {
	ok, err := hasFilesystemHeadroom(dir, minFreeBytes, additionalBytes)
	return err == nil && ok
}

// SweepRootFSBaselines treats every baseline as disposable. Invalid legacy
// layouts and interrupted temp entries are removed on startup; valid entries
// are then evicted by TTL, total bytes, count, and filesystem pressure.
func (r *ContainerdRuntime) SweepRootFSBaselines() error {
	if r == nil {
		return nil
	}
	return r.withRootFSBaselineLock(func() error {
		return r.sweepRootFSBaselinesLocked(time.Now())
	})
}

func (r *ContainerdRuntime) sweepRootFSBaselinesLocked(now time.Time) error {
	root := r.rootFSBaselineRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	dirEntries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range dirEntries {
		if entry.Name() == rootFSBaselineLockName {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if strings.HasPrefix(entry.Name(), ".baseline-") || !entry.IsDir() {
			if err := os.RemoveAll(path); err != nil {
				return err
			}
			continue
		}
		if stat, err := os.Stat(filepath.Join(path, "data")); err != nil || !stat.IsDir() {
			if err := os.RemoveAll(path); err != nil {
				return err
			}
			continue
		}
		if _, err := readRootFSBaselineMetadata(path); err != nil {
			if err := os.RemoveAll(path); err != nil {
				return err
			}
		}
	}

	entries, total, err := r.rootFSBaselineEntriesLocked()
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].modTime.Equal(entries[j].modTime) {
			return entries[i].path < entries[j].path
		}
		return entries[i].modTime.Before(entries[j].modTime)
	})
	cutoff := now.Add(-r.baselineTTL)
	kept := entries[:0]
	for _, entry := range entries {
		if !entry.modTime.After(cutoff) {
			if err := os.RemoveAll(entry.path); err != nil {
				return err
			}
			total -= entry.size
			continue
		}
		kept = append(kept, entry)
	}
	entries = kept

	teamBytes := make(map[string]int64)
	teamEntries := make(map[string]int)
	for _, entry := range entries {
		teamBytes[entry.teamID], err = checkedAddPreparedSnapshotBytes(teamBytes[entry.teamID], entry.size)
		if err != nil {
			return err
		}
		teamEntries[entry.teamID]++
	}
	kept = entries[:0]
	for _, entry := range entries {
		if teamEntries[entry.teamID] > r.baselineMaxTeamEntries ||
			teamBytes[entry.teamID] > r.baselineMaxTeamBytes {
			if err := os.RemoveAll(entry.path); err != nil {
				return err
			}
			total -= entry.size
			teamBytes[entry.teamID] -= entry.size
			teamEntries[entry.teamID]--
			continue
		}
		kept = append(kept, entry)
	}
	entries = kept
	for len(entries) > r.baselineMaxEntries ||
		total > r.baselineMaxTotalBytes ||
		!rootFSBaselineHasHeadroom(root, r.baselineMinFreeBytes, 0) {
		if len(entries) == 0 {
			break
		}
		entry := entries[0]
		entries = entries[1:]
		if err := os.RemoveAll(entry.path); err != nil {
			return err
		}
		total -= entry.size
	}
	return nil
}

func (r *ContainerdRuntime) rootFSBaselineEntriesLocked() ([]rootFSBaselineEntry, int64, error) {
	root := r.rootFSBaselineRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, 0, err
	}
	dirEntries, err := os.ReadDir(root)
	if err != nil {
		return nil, 0, err
	}
	var entries []rootFSBaselineEntry
	var total int64
	for _, dirEntry := range dirEntries {
		if !dirEntry.IsDir() || strings.HasPrefix(dirEntry.Name(), ".") {
			continue
		}
		path := filepath.Join(root, dirEntry.Name())
		info, err := dirEntry.Info()
		if err != nil {
			return nil, 0, err
		}
		size, err := rootFSBaselineTreeSize(path)
		if err != nil {
			return nil, 0, err
		}
		total, err = checkedAddPreparedSnapshotBytes(total, size)
		if err != nil {
			return nil, 0, err
		}
		metadata, err := readRootFSBaselineMetadata(path)
		if err != nil {
			return nil, 0, err
		}
		entries = append(entries, rootFSBaselineEntry{
			path:    path,
			teamID:  strings.TrimSpace(metadata.TeamID),
			size:    size,
			modTime: info.ModTime(),
		})
	}
	return entries, total, nil
}

func readRootFSBaselineMetadata(entryPath string) (rootFSBaselineMetadata, error) {
	file, err := os.Open(filepath.Join(entryPath, rootFSBaselineMetadataName))
	if err != nil {
		return rootFSBaselineMetadata{}, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, rootFSBaselineMetadataMaxBytes+1))
	if err != nil {
		return rootFSBaselineMetadata{}, err
	}
	if len(raw) > rootFSBaselineMetadataMaxBytes {
		return rootFSBaselineMetadata{}, fmt.Errorf("rootfs baseline metadata exceeds %d bytes", rootFSBaselineMetadataMaxBytes)
	}
	var metadata rootFSBaselineMetadata
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return rootFSBaselineMetadata{}, err
	}
	if strings.TrimSpace(metadata.TeamID) == "" ||
		strings.TrimSpace(metadata.SandboxID) == "" ||
		strings.TrimSpace(metadata.LayerID) == "" ||
		metadata.SizeBytes < 0 ||
		metadata.CreatedAt.IsZero() {
		return rootFSBaselineMetadata{}, fmt.Errorf("rootfs baseline metadata is incomplete")
	}
	return metadata, nil
}

func rootFSBaselineTreeSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		size := rootFSBaselineEntryOverhead
		if info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			size, err = checkedAddPreparedSnapshotBytes(size, max(info.Size(), 0))
			if err != nil {
				return err
			}
		}
		total, err = checkedAddPreparedSnapshotBytes(total, size)
		return err
	})
	return total, err
}

func (r *ContainerdRuntime) withRootFSBaselineLock(fn func() error) error {
	root := r.rootFSBaselineRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	lockValue, _ := rootFSBaselineProcessLocks.LoadOrStore(filepath.Clean(root), &sync.Mutex{})
	processLock := lockValue.(*sync.Mutex)
	processLock.Lock()
	defer processLock.Unlock()

	lockFile, err := os.OpenFile(filepath.Join(root, rootFSBaselineLockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lockFile.Close()
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		return fmt.Errorf("lock rootfs baseline cache: %w", err)
	}
	defer func() { _ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN) }()
	return fn()
}

func (r *ContainerdRuntime) rootFSBaselineRoot() string {
	root := defaultRootFSCacheDir
	if r != nil && strings.TrimSpace(r.rootFSCacheDir) != "" {
		root = r.rootFSCacheDir
	}
	return filepath.Join(root, "baselines")
}

func copyRootFSTreeBounded(
	ctx context.Context,
	targetRoot string,
	sourceRoot string,
	filter rootFSPathFilter,
	maxBytes int64,
) (int64, error) {
	if maxBytes <= 0 {
		return 0, fmt.Errorf("%w: max bytes must be positive", ErrRootFSBaselineCacheCapacity)
	}
	sourceRoot = filepath.Clean(sourceRoot)
	targetRoot = filepath.Clean(targetRoot)
	budget := &rootFSBaselineBudget{max: maxBytes}
	hardlinks := make(map[rootFSBaselineInode]string)
	var directories []struct {
		source string
		target string
		info   os.FileInfo
		xattrs map[string][]byte
	}

	err := filepath.WalkDir(sourceRoot, func(sourcePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(sourceRoot, sourcePath)
		if err != nil {
			return err
		}
		if relative != "." {
			changePath := "/" + filepath.ToSlash(relative)
			if filter.Excludes(changePath) {
				if entry.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		targetPath := targetRoot
		if relative != "." {
			targetPath = filepath.Join(targetRoot, relative)
		}
		xattrs, xattrBytes, err := rootFSBaselineXAttrs(sourcePath)
		if err != nil {
			return err
		}
		entryBytes := rootFSBaselineEntryOverhead
		entryBytes, err = checkedAddPreparedSnapshotBytes(entryBytes, xattrBytes)
		if err != nil {
			return err
		}

		switch {
		case entry.IsDir():
			if err := budget.consume(entryBytes); err != nil {
				return err
			}
			if err := os.Mkdir(targetPath, 0o700); err != nil {
				return err
			}
			directories = append(directories, struct {
				source string
				target string
				info   os.FileInfo
				xattrs map[string][]byte
			}{source: sourcePath, target: targetPath, info: info, xattrs: xattrs})
			return nil
		case info.Mode().IsRegular():
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return fmt.Errorf("unsupported rootfs baseline stat type for %s", sourcePath)
			}
			inode := rootFSBaselineInode{device: uint64(stat.Dev), inode: stat.Ino}
			if previous := hardlinks[inode]; previous != "" {
				if err := budget.consume(entryBytes); err != nil {
					return err
				}
				if err := os.Link(previous, targetPath); err != nil {
					return err
				}
			} else {
				entryBytes, err = checkedAddPreparedSnapshotBytes(entryBytes, max(info.Size(), 0))
				if err != nil {
					return err
				}
				if err := budget.consume(entryBytes); err != nil {
					return err
				}
				if err := copyRootFSBaselineRegularFile(ctx, targetPath, sourcePath, info.Size()); err != nil {
					return err
				}
				hardlinks[inode] = targetPath
			}
		case info.Mode()&os.ModeSymlink != 0:
			linkTarget, err := os.Readlink(sourcePath)
			if err != nil {
				return err
			}
			entryBytes, err = checkedAddPreparedSnapshotBytes(entryBytes, int64(len(linkTarget)))
			if err != nil {
				return err
			}
			if err := budget.consume(entryBytes); err != nil {
				return err
			}
			if err := os.Symlink(linkTarget, targetPath); err != nil {
				return err
			}
		case info.Mode()&os.ModeDevice != 0 || info.Mode()&os.ModeNamedPipe != 0 || info.Mode()&os.ModeSocket != 0:
			if err := budget.consume(entryBytes); err != nil {
				return err
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return fmt.Errorf("unsupported rootfs baseline stat type for %s", sourcePath)
			}
			device := 0
			if info.Mode()&os.ModeDevice != 0 {
				device = int(stat.Rdev)
			}
			if err := syscall.Mknod(targetPath, uint32(stat.Mode), device); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported rootfs baseline file mode %s for %s", info.Mode(), sourcePath)
		}
		if err := copyRootFSBaselineFileInfo(info, targetPath); err != nil {
			return err
		}
		return setRootFSBaselineXAttrs(targetPath, xattrs)
	})
	if err != nil {
		return 0, err
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := copyRootFSBaselineFileInfo(directories[i].info, directories[i].target); err != nil {
			return 0, err
		}
		if err := setRootFSBaselineXAttrs(directories[i].target, directories[i].xattrs); err != nil {
			return 0, err
		}
	}
	return budget.used, nil
}

func (b *rootFSBaselineBudget) consume(size int64) error {
	if b == nil || exceedsPreparedSnapshotLimit(b.used, size, b.max) {
		return fmt.Errorf("%w: baseline exceeds %d bytes", ErrRootFSBaselineCacheCapacity, b.max)
	}
	b.used += size
	return nil
}

func copyRootFSBaselineRegularFile(ctx context.Context, target, source string, size int64) error {
	if size < 0 {
		return fmt.Errorf("rootfs baseline source file has negative size")
	}
	sourceFile, err := os.Open(source)
	if err != nil {
		return err
	}
	defer sourceFile.Close()
	targetFile, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer targetFile.Close()
	written, err := copyContext(ctx, targetFile, io.LimitReader(sourceFile, size))
	if err != nil {
		return err
	}
	if written != size {
		return fmt.Errorf("rootfs baseline source changed while copying: expected %d bytes, copied %d", size, written)
	}
	return nil
}

func rootFSBaselineXAttrs(path string) (map[string][]byte, int64, error) {
	keys, err := sysx.LListxattr(path)
	if err != nil {
		if errors.Is(err, unix.ENOTSUP) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	values := make(map[string][]byte, len(keys))
	var total int64
	for _, key := range keys {
		value, err := sysx.LGetxattr(path, key)
		if err != nil {
			return nil, 0, err
		}
		total, err = checkedAddPreparedSnapshotBytes(total, int64(len(key)+len(value)))
		if err != nil {
			return nil, 0, err
		}
		values[key] = value
	}
	return values, total, nil
}

func setRootFSBaselineXAttrs(path string, values map[string][]byte) error {
	for key, value := range values {
		if err := sysx.LSetxattr(path, key, value, 0); err != nil {
			return err
		}
	}
	return nil
}

func copyRootFSBaselineFileInfo(info os.FileInfo, target string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unsupported rootfs baseline stat type for %s", target)
	}
	if err := os.Lchown(target, int(stat.Uid), int(stat.Gid)); err != nil {
		if !os.IsPermission(err) {
			return err
		}
		targetStat, statErr := os.Lstat(target)
		if statErr != nil {
			return err
		}
		targetSys, ok := targetStat.Sys().(*syscall.Stat_t)
		if !ok || targetSys.Uid != stat.Uid || targetSys.Gid != stat.Gid {
			return err
		}
	}
	if info.Mode()&os.ModeSymlink == 0 {
		if err := os.Chmod(target, info.Mode()); err != nil {
			return err
		}
	}
	times := []unix.Timespec{
		unix.NsecToTimespec(syscall.TimespecToNsec(fs.StatAtime(stat))),
		unix.NsecToTimespec(syscall.TimespecToNsec(stat.Mtim)),
	}
	return unix.UtimesNanoAt(unix.AT_FDCWD, target, times, unix.AT_SYMLINK_NOFOLLOW)
}
