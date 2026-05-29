package s0fs

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const localDiskGuardRefreshInterval = time.Second

// LocalDiskGuard protects node-local S0FS WAL/cache space. It keeps a cached
// usage counter so the write path does not rescan the cache directory for every
// FUSE write.
type LocalDiskGuard struct {
	Path         string
	MaxBytes     int64
	MinFreeBytes int64

	mu            sync.Mutex
	usedBytes     int64
	lastRefresh   time.Time
	initialized   bool
	nextFreeCheck time.Time
}

func (g *LocalDiskGuard) Reserve(projectedBytes int64) error {
	if g == nil || (g.MaxBytes <= 0 && g.MinFreeBytes <= 0) {
		return nil
	}
	if projectedBytes < 0 {
		projectedBytes = 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.refreshLocked(false); err != nil {
		return err
	}
	if g.MaxBytes > 0 && g.usedBytes+projectedBytes > g.MaxBytes {
		if err := g.refreshLocked(true); err != nil {
			return err
		}
		if g.usedBytes+projectedBytes > g.MaxBytes {
			return fmt.Errorf("%w: projected usage %d exceeds max %d", ErrNoSpace, g.usedBytes+projectedBytes, g.MaxBytes)
		}
	}
	if g.MinFreeBytes > 0 {
		if err := g.checkFreeLocked(projectedBytes); err != nil {
			return err
		}
	}
	g.usedBytes += projectedBytes
	return nil
}

func (g *LocalDiskGuard) Refresh() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	_ = g.refreshLocked(true)
}

func (g *LocalDiskGuard) refreshLocked(force bool) error {
	if g == nil || g.Path == "" || (g.initialized && !force && time.Since(g.lastRefresh) < localDiskGuardRefreshInterval) {
		return nil
	}
	used, err := directorySize(g.Path)
	if err != nil {
		return fmt.Errorf("check local s0fs cache usage: %w", err)
	}
	g.usedBytes = used
	g.lastRefresh = time.Now()
	g.initialized = true
	return nil
}

func (g *LocalDiskGuard) checkFreeLocked(projectedBytes int64) error {
	now := time.Now()
	if now.Before(g.nextFreeCheck) && projectedBytes <= 0 {
		return nil
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(g.Path, &stat); err != nil {
		return fmt.Errorf("check local s0fs cache filesystem free space: %w", err)
	}
	freeBytes := int64(stat.Bavail) * int64(stat.Bsize)
	if freeBytes-projectedBytes < g.MinFreeBytes {
		return fmt.Errorf("%w: projected free space %d below minimum %d", ErrNoSpace, freeBytes-projectedBytes, g.MinFreeBytes)
	}
	g.nextFreeCheck = now.Add(localDiskGuardRefreshInterval)
	return nil
}

func directorySize(root string) (int64, error) {
	if root == "" {
		return 0, nil
	}
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	var total int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}
