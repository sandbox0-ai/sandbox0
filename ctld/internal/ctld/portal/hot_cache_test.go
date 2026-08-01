package portal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func TestNewManagerReadsHotCacheLimits(t *testing.T) {
	mgr := NewManager(Config{
		RootDir: t.TempDir(),
		StorageConfig: &config.StorageProxyConfig{
			S0FSHotCacheTTL:        "45s",
			S0FSHotCacheMaxEntries: 12,
			S0FSHotCacheMaxSize:    "2Gi",
		},
	})
	t.Cleanup(func() { _ = mgr.recoveryWriter.Close(context.Background()) })
	if mgr.hotCacheTTL != 45*time.Second || mgr.hotCacheMaxEntries != 12 || mgr.hotCacheMaxBytes != 2<<30 {
		t.Fatalf("hot cache limits = %s/%d/%d", mgr.hotCacheTTL, mgr.hotCacheMaxEntries, mgr.hotCacheMaxBytes)
	}
}

func TestNewManagerAllowsHotCacheDisable(t *testing.T) {
	mgr := NewManager(Config{
		RootDir:       t.TempDir(),
		StorageConfig: &config.StorageProxyConfig{S0FSHotCacheTTL: "0s"},
	})
	t.Cleanup(func() { _ = mgr.recoveryWriter.Close(context.Background()) })
	if mgr.hotCacheTTL != 0 || mgr.hotCacheEnabled() {
		t.Fatalf("hot cache TTL = %s, enabled = %v", mgr.hotCacheTTL, mgr.hotCacheEnabled())
	}
}

func TestFinishBoundVolumeCleanupRetainsCleanS0FSEngine(t *testing.T) {
	mgr := NewManager(Config{
		RootDir:                t.TempDir(),
		S0FSHotCacheTTL:        time.Minute,
		S0FSHotCacheMaxEntries: 2,
		S0FSHotCacheMaxBytes:   64 << 20,
	})
	t.Cleanup(func() { _ = mgr.recoveryWriter.Close(context.Background()) })
	bound := newHotCacheTestBound(t, mgr, "vol-hot")
	mgr.boundVolumes[bound.volumeID] = bound
	mgr.volumes.add(bound.volCtx)
	cleanup := &boundVolumeCleanup{volumeID: bound.volumeID, bound: bound, retainHot: true}

	if err := mgr.finishBoundVolumeCleanup(context.Background(), cleanup); err != nil {
		t.Fatalf("finishBoundVolumeCleanup() error = %v", err)
	}
	if mgr.boundVolumes[bound.volumeID] != nil {
		t.Fatal("active bound volume remained after hot detach")
	}
	if _, err := mgr.volumes.GetVolume(bound.volumeID); err == nil {
		t.Fatal("hot engine remained visible to file requests")
	}
	entry := mgr.hotVolumes[bound.volumeID]
	if entry == nil || entry.bound != bound {
		t.Fatal("clean S0FS engine was not retained")
	}
	claimed, ok := mgr.takeHotVolume(bound.volumeID)
	if !ok || claimed != entry {
		t.Fatal("takeHotVolume() did not return retained engine")
	}
	mgr.closeHotVolume(claimed)
}

func TestHotCacheEvictsOldestEntryAtCapacity(t *testing.T) {
	mgr := NewManager(Config{
		RootDir:                t.TempDir(),
		S0FSHotCacheTTL:        time.Minute,
		S0FSHotCacheMaxEntries: 1,
		S0FSHotCacheMaxBytes:   64 << 20,
	})
	t.Cleanup(func() { _ = mgr.recoveryWriter.Close(context.Background()) })
	first := newHotCacheTestBound(t, mgr, "vol-first")
	second := newHotCacheTestBound(t, mgr, "vol-second")
	mgr.retainHotVolume(first)
	time.Sleep(time.Millisecond)
	mgr.retainHotVolume(second)

	if mgr.hotVolumes[first.volumeID] != nil {
		t.Fatal("oldest hot engine was not evicted")
	}
	if mgr.hotVolumes[second.volumeID] == nil {
		t.Fatal("newest hot engine was not retained")
	}
	if _, err := os.Stat(first.volCtx.CacheDir); !os.IsNotExist(err) {
		t.Fatalf("evicted cache directory still exists: %v", err)
	}
	mgr.evictHotVolume(second.volumeID, "test")
}

func TestHotCacheExpiresEntry(t *testing.T) {
	mgr := NewManager(Config{
		RootDir:                t.TempDir(),
		S0FSHotCacheTTL:        10 * time.Millisecond,
		S0FSHotCacheMaxEntries: 2,
		S0FSHotCacheMaxBytes:   64 << 20,
	})
	t.Cleanup(func() { _ = mgr.recoveryWriter.Close(context.Background()) })
	bound := newHotCacheTestBound(t, mgr, "vol-expired")
	mgr.retainHotVolume(bound)
	time.Sleep(20 * time.Millisecond)
	mgr.cleanupExpiredHotVolumes()
	if mgr.hotVolumes[bound.volumeID] != nil {
		t.Fatal("expired hot engine was not evicted")
	}
	if _, err := os.Stat(bound.volCtx.CacheDir); !os.IsNotExist(err) {
		t.Fatalf("expired cache directory still exists: %v", err)
	}
}

func TestDetachHotVolumeWaitsForInflightRequests(t *testing.T) {
	mgr := NewManager(Config{RootDir: t.TempDir()})
	t.Cleanup(func() { _ = mgr.recoveryWriter.Close(context.Background()) })
	bound := newHotCacheTestBound(t, mgr, "vol-inflight")
	mgr.volumes.add(bound.volCtx)
	release, err := mgr.volumes.acquire(context.Background(), bound.volumeID)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, detachErr := mgr.volumes.DetachHotVolume(context.Background(), bound.volumeID)
		done <- detachErr
	}()
	select {
	case err := <-done:
		t.Fatalf("DetachHotVolume() returned before the request completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("DetachHotVolume() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DetachHotVolume() did not finish after the request completed")
	}
	if _, err := mgr.volumes.GetVolume(bound.volumeID); err == nil {
		t.Fatal("detached hot volume remained visible to requests")
	}
	mgr.closeHotVolume(&hotVolume{bound: bound})
}

func newHotCacheTestBound(t *testing.T, mgr *Manager, volumeID string) *boundVolume {
	t.Helper()
	cacheDir := filepath.Join(mgr.rootDir, "volumes", volumeID)
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: volumeID,
		WALPath:  filepath.Join(cacheDir, "engine.wal"),
	})
	if err != nil {
		t.Fatalf("s0fs.Open() error = %v", err)
	}
	return &boundVolume{
		volumeID: volumeID,
		teamID:   "team-1",
		access:   volume.AccessModeRWO,
		refCount: 0,
		volCtx: &volume.VolumeContext{
			VolumeID: volumeID,
			TeamID:   "team-1",
			Backend:  volume.BackendS0FS,
			S0FS:     engine,
			Access:   volume.AccessModeRWO,
			CacheDir: cacheDir,
		},
	}
}
