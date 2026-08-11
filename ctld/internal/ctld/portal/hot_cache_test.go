package portal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func TestNewManagerReadsHotCacheByteBudget(t *testing.T) {
	mgr := NewManager(Config{
		RootDir:       t.TempDir(),
		StorageConfig: &config.StorageProxyConfig{S0FSHotCacheMaxSize: "2Gi"},
	})
	t.Cleanup(func() { _ = mgr.recoveryWriter.Close(context.Background()) })
	if mgr.hotCacheMaxBytes != 2<<30 {
		t.Fatalf("hot cache byte budget = %d, want %d", mgr.hotCacheMaxBytes, int64(2<<30))
	}
}

func TestNewManagerAllowsHotCacheDisable(t *testing.T) {
	mgr := NewManager(Config{
		RootDir:       t.TempDir(),
		StorageConfig: &config.StorageProxyConfig{S0FSHotCacheMaxSize: "0"},
	})
	t.Cleanup(func() { _ = mgr.recoveryWriter.Close(context.Background()) })
	if mgr.hotCacheMaxBytes != 0 || mgr.hotCacheEnabled() {
		t.Fatalf("hot cache byte budget = %d, enabled = %v", mgr.hotCacheMaxBytes, mgr.hotCacheEnabled())
	}
}

func TestActiveMetadataAdmissionEvictsDetachedEngine(t *testing.T) {
	mgr := newHotCacheTestManager(t, 2*hotCacheMinimumEntryBytes)
	active := newHotCacheTestBound(t, mgr, "vol-active")
	detached := newHotCacheTestBound(t, mgr, "vol-detached")
	candidate := newHotCacheTestBound(t, mgr, "vol-candidate-active")
	t.Cleanup(func() {
		mgr.releaseActiveMetadata(active)
		mgr.releaseActiveMetadata(candidate)
		_ = active.volCtx.S0FS.Close()
		_ = candidate.volCtx.S0FS.Close()
	})
	if err := mgr.admitActiveMetadata(active); err != nil {
		t.Fatalf("admitActiveMetadata(active) error = %v", err)
	}
	mgr.retainHotVolume(detached)
	if err := mgr.admitActiveMetadata(candidate); err != nil {
		t.Fatalf("admitActiveMetadata(candidate) error = %v", err)
	}
	if mgr.hotVolumes[detached.volumeID] != nil {
		t.Fatal("detached engine was not evicted for active admission")
	}
	if got := mgr.activeMetadataBytes; got != 2*hotCacheMinimumEntryBytes {
		t.Fatalf("active metadata bytes = %d, want %d", got, 2*hotCacheMinimumEntryBytes)
	}
}

func TestActiveMetadataAdmissionRejectsWhenActiveBudgetIsFull(t *testing.T) {
	mgr := newHotCacheTestManager(t, hotCacheMinimumEntryBytes)
	active := newHotCacheTestBound(t, mgr, "vol-active-full")
	candidate := newHotCacheTestBound(t, mgr, "vol-active-rejected")
	t.Cleanup(func() {
		mgr.releaseActiveMetadata(active)
		_ = active.volCtx.S0FS.Close()
		_ = candidate.volCtx.S0FS.Close()
	})
	if err := mgr.admitActiveMetadata(active); err != nil {
		t.Fatalf("admitActiveMetadata(active) error = %v", err)
	}
	if err := mgr.admitActiveMetadata(candidate); err == nil {
		t.Fatal("admitActiveMetadata(candidate) succeeded with a full active budget")
	}
	if candidate.metadataCharged {
		t.Fatal("rejected candidate retained an active metadata charge")
	}
}

func TestActiveMetadataReservationIsChargedBeforeEngineOpen(t *testing.T) {
	reservation := s0fs.EngineMemoryReservationBytes(0)
	mgr := newHotCacheTestManager(t, reservation)
	bound := &boundVolume{volumeID: "vol-reserved-before-open"}
	if err := mgr.admitActiveMetadataBytes(bound, reservation); err != nil {
		t.Fatalf("admitActiveMetadataBytes() error = %v", err)
	}
	t.Cleanup(func() { mgr.releaseActiveMetadata(bound) })
	if !bound.metadataCharged || bound.metadataBytes != reservation {
		t.Fatalf("active reservation = charged:%v bytes:%d, want true/%d", bound.metadataCharged, bound.metadataBytes, reservation)
	}
	if err := mgr.admitActiveMetadataBytes(&boundVolume{volumeID: "vol-over-budget"}, reservation); err == nil {
		t.Fatal("second active reservation succeeded with the budget full")
	}
}

func TestFinishBoundVolumeCleanupRetainsCleanS0FSEngine(t *testing.T) {
	mgr := newHotCacheTestManager(t, 64<<20)
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
	if entry == nil || entry.bound != bound || entry.segment != hotCacheSegmentProbation {
		t.Fatalf("clean S0FS engine was not retained in probation: %#v", entry)
	}
	claimed, ok := mgr.takeHotVolume(bound.volumeID)
	if !ok || claimed != entry {
		t.Fatal("takeHotVolume() did not return retained engine")
	}
	mgr.closeHotVolume(claimed)
}

func TestHotCacheDoesNotExpireByAge(t *testing.T) {
	mgr := newHotCacheTestManager(t, 64<<20)
	bound := newHotCacheTestBound(t, mgr, "vol-old")
	mgr.retainHotVolume(bound)
	mgr.mu.Lock()
	mgr.hotVolumes[bound.volumeID].cachedAt = time.Now().Add(-24 * time.Hour)
	mgr.mu.Unlock()

	entry, ok := mgr.takeHotVolume(bound.volumeID)
	if !ok {
		t.Fatal("old entry expired without memory pressure")
	}
	mgr.closeHotVolume(entry)
}

func TestHotCacheRejectsShortLivedProbationChurn(t *testing.T) {
	mgr := newHotCacheTestManager(t, hotCacheMinimumEntryBytes)
	valuable := newHotCacheTestBound(t, mgr, "vol-valuable")
	valuable.mountedAt = time.Now().Add(-time.Hour)
	churn := newHotCacheTestBound(t, mgr, "vol-churn")
	churn.mountedAt = time.Now().Add(-time.Second)

	mgr.retainHotVolume(valuable)
	mgr.retainHotVolume(churn)

	if mgr.hotVolumes[valuable.volumeID] == nil {
		t.Fatal("long-running probation entry was displaced by short-lived churn")
	}
	if mgr.hotVolumes[churn.volumeID] != nil {
		t.Fatal("short-lived churn candidate was admitted over a more valuable entry")
	}
	if _, err := os.Stat(churn.volCtx.CacheDir); !os.IsNotExist(err) {
		t.Fatalf("rejected churn cache directory still exists: %v", err)
	}
}

func TestHotCacheAdmitsLongRunningCandidateOverShortLivedEntry(t *testing.T) {
	mgr := newHotCacheTestManager(t, hotCacheMinimumEntryBytes)
	shortLived := newHotCacheTestBound(t, mgr, "vol-short")
	shortLived.mountedAt = time.Now().Add(-time.Second)
	longRunning := newHotCacheTestBound(t, mgr, "vol-long")
	longRunning.mountedAt = time.Now().Add(-time.Hour)

	mgr.retainHotVolume(shortLived)
	mgr.retainHotVolume(longRunning)

	if mgr.hotVolumes[shortLived.volumeID] != nil {
		t.Fatal("short-lived probation entry survived a higher-value candidate")
	}
	if mgr.hotVolumes[longRunning.volumeID] == nil {
		t.Fatal("long-running probation candidate was not admitted")
	}
}

func TestHotCacheProbationValueUsesColdOpenCostAndAgeAsTieBreakers(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	cheap := &hotVolume{
		mountedDuration:  4 * time.Minute,
		coldOpenDuration: 10 * time.Millisecond,
		cachedAt:         base.Add(time.Minute),
	}
	expensive := &hotVolume{
		mountedDuration:  31 * time.Second,
		coldOpenDuration: time.Second,
		cachedAt:         base,
	}
	if !lessValuableHotVolume(cheap, expensive) {
		t.Fatal("cheap cold-open entry was not ranked below expensive entry")
	}
	if lessValuableHotVolume(expensive, cheap) {
		t.Fatal("expensive cold-open entry was ranked below cheap entry")
	}

	shorter := &hotVolume{mountedDuration: time.Minute, coldOpenDuration: time.Second, cachedAt: base.Add(time.Minute)}
	longer := &hotVolume{mountedDuration: 4 * time.Minute, coldOpenDuration: time.Second, cachedAt: base}
	if !lessValuableHotVolume(shorter, longer) {
		t.Fatal("shorter equal-cost entry was not ranked below longer entry")
	}

	newer := &hotVolume{mountedDuration: time.Minute, coldOpenDuration: time.Second, cachedAt: base.Add(time.Minute)}
	older := &hotVolume{mountedDuration: time.Minute, coldOpenDuration: time.Second, cachedAt: base}
	if !lessValuableHotVolume(older, newer) {
		t.Fatal("oldest equal-value entry was not selected first")
	}
}

func TestHotCacheProtectedEntriesResistProbationScan(t *testing.T) {
	mgr := newHotCacheTestManager(t, 4*hotCacheMinimumEntryBytes)
	protectedIDs := []string{"vol-protected-1", "vol-protected-2", "vol-protected-3"}
	for _, volumeID := range protectedIDs {
		bound := newHotCacheTestBound(t, mgr, volumeID)
		bound.hotReuse = true
		mgr.retainHotVolume(bound)
	}
	valuable := newHotCacheTestBound(t, mgr, "vol-probation-valuable")
	valuable.mountedAt = time.Now().Add(-time.Hour)
	mgr.retainHotVolume(valuable)
	churn := newHotCacheTestBound(t, mgr, "vol-probation-churn")
	churn.mountedAt = time.Now().Add(-time.Second)
	mgr.retainHotVolume(churn)

	for _, volumeID := range protectedIDs {
		entry := mgr.hotVolumes[volumeID]
		if entry == nil || entry.segment != hotCacheSegmentProtected {
			t.Fatalf("protected entry %s was displaced by probation scan: %#v", volumeID, entry)
		}
	}
	if mgr.hotVolumes[valuable.volumeID] == nil || mgr.hotVolumes[churn.volumeID] != nil {
		t.Fatal("probation scan did not retain the higher-value candidate")
	}
}

func TestHotCacheHitPromotesEntryAfterNextPause(t *testing.T) {
	mgr := newHotCacheTestManager(t, 4*hotCacheMinimumEntryBytes)
	bound := newHotCacheTestBound(t, mgr, "vol-promote")
	mgr.retainHotVolume(bound)
	entry, ok := mgr.takeHotVolume(bound.volumeID)
	if !ok {
		t.Fatal("probation entry was not reusable")
	}
	entry.bound.hotReuse = true
	entry.bound.mountedAt = time.Now().Add(-time.Minute)
	mgr.retainHotVolume(entry.bound)

	promoted := mgr.hotVolumes[bound.volumeID]
	if promoted == nil || promoted.segment != hotCacheSegmentProtected {
		t.Fatalf("cache hit was not promoted after the next pause: %#v", promoted)
	}
}

func TestHotCacheDemotesOldestProtectedEntryToPreserveAdmissionWindow(t *testing.T) {
	mgr := newHotCacheTestManager(t, 4*hotCacheMinimumEntryBytes)
	ids := []string{"vol-1", "vol-2", "vol-3", "vol-4"}
	base := time.Now().Add(-time.Hour)
	for index, volumeID := range ids {
		bound := newHotCacheTestBound(t, mgr, volumeID)
		bound.hotReuse = true
		mgr.retainHotVolume(bound)
		if entry := mgr.hotVolumes[volumeID]; entry != nil {
			entry.cachedAt = base.Add(time.Duration(index) * time.Minute)
		}
	}

	if entry := mgr.hotVolumes[ids[0]]; entry == nil || entry.segment != hotCacheSegmentProbation {
		t.Fatalf("oldest protected entry was not demoted: %#v", entry)
	}
	for _, volumeID := range ids[1:] {
		if entry := mgr.hotVolumes[volumeID]; entry == nil || entry.segment != hotCacheSegmentProtected {
			t.Fatalf("newer protected entry %s was not retained: %#v", volumeID, entry)
		}
	}
}

func TestHotCacheRejectsEntryLargerThanBudget(t *testing.T) {
	mgr := newHotCacheTestManager(t, hotCacheMinimumEntryBytes/2)
	bound := newHotCacheTestBound(t, mgr, "vol-oversize")
	mgr.retainHotVolume(bound)
	if mgr.hotVolumes[bound.volumeID] != nil {
		t.Fatal("entry larger than the byte budget was admitted")
	}
	if _, err := os.Stat(bound.volCtx.CacheDir); !os.IsNotExist(err) {
		t.Fatalf("oversize cache directory still exists: %v", err)
	}
}

func TestHotCacheAdmissionDoesNotWaitForBusyVictim(t *testing.T) {
	mgr := newHotCacheTestManager(t, hotCacheMinimumEntryBytes)
	victim := newHotCacheTestBound(t, mgr, "vol-busy-victim")
	candidate := newHotCacheTestBound(t, mgr, "vol-candidate")
	victim.mountedAt = time.Now().Add(-time.Second)
	candidate.mountedAt = time.Now().Add(-time.Hour)
	mgr.retainHotVolume(victim)

	victimRelease, err := mgr.acquireVolumeActivation(context.Background(), victim.volumeID)
	if err != nil {
		t.Fatal(err)
	}
	defer victimRelease()
	candidateRelease, err := mgr.acquireVolumeActivation(context.Background(), candidate.volumeID)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer candidateRelease()
		mgr.retainHotVolume(candidate)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cache admission waited for a busy victim")
	}

	if mgr.hotVolumes[victim.volumeID] == nil || mgr.hotVolumes[candidate.volumeID] != nil {
		t.Fatalf("busy victim was displaced: victim=%#v candidate=%#v", mgr.hotVolumes[victim.volumeID], mgr.hotVolumes[candidate.volumeID])
	}
}

func TestDeleteUnbindWithoutPortalEvictsPausedHotEntry(t *testing.T) {
	mgr := newHotCacheTestManager(t, 4*hotCacheMinimumEntryBytes)
	bound := newHotCacheTestBound(t, mgr, "vol-paused-delete")
	mgr.retainHotVolume(bound)

	resp, err := mgr.Unbind(context.Background(), ctldapi.UnbindVolumePortalRequest{
		PodUID:          "deleted-pod",
		PortalName:      "workspace",
		SandboxVolumeID: bound.volumeID,
		RetainHot:       false,
	})
	if err != nil {
		t.Fatalf("Unbind() error = %v", err)
	}
	if !resp.Unbound || mgr.hotVolumes[bound.volumeID] != nil {
		t.Fatalf("delete unbind did not invalidate paused cache: response=%+v", resp)
	}
	if _, err := os.Stat(bound.volCtx.CacheDir); !os.IsNotExist(err) {
		t.Fatalf("invalidated cache directory still exists: %v", err)
	}
}

func TestRetriedPauseUnbindWithoutPortalKeepsHotEntry(t *testing.T) {
	mgr := newHotCacheTestManager(t, 4*hotCacheMinimumEntryBytes)
	bound := newHotCacheTestBound(t, mgr, "vol-paused-retry")
	mgr.retainHotVolume(bound)

	resp, err := mgr.Unbind(context.Background(), ctldapi.UnbindVolumePortalRequest{
		PodUID:          "paused-pod",
		PortalName:      "workspace",
		SandboxVolumeID: bound.volumeID,
		RetainHot:       true,
	})
	if err != nil {
		t.Fatalf("Unbind() error = %v", err)
	}
	if !resp.Unbound || mgr.hotVolumes[bound.volumeID] == nil {
		t.Fatalf("idempotent pause unbind discarded hot entry: response=%+v", resp)
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

func newHotCacheTestManager(t *testing.T, maxBytes int64) *Manager {
	t.Helper()
	mgr := NewManager(Config{RootDir: t.TempDir(), S0FSHotCacheMaxBytes: maxBytes})
	t.Cleanup(func() {
		for _, entry := range mgr.drainHotVolumes("test_cleanup") {
			mgr.closeHotVolume(entry)
		}
		_ = mgr.recoveryWriter.Close(context.Background())
	})
	return mgr
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
		volumeID:         volumeID,
		teamID:           "team-1",
		access:           volume.AccessModeRWO,
		mountedAt:        time.Now().Add(-time.Minute),
		refCount:         0,
		coldOpenDuration: 100 * time.Millisecond,
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
