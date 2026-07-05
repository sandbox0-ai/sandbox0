package portal

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func TestNewManagerConfiguresLocalDiskGuard(t *testing.T) {
	mgr := NewManager(Config{
		RootDir: t.TempDir(),
		StorageConfig: &apiconfig.StorageProxyConfig{
			VolumePortalCacheSizeLimit: "128Mi",
			VolumePortalRootMinFree:    "64Mi",
		},
	})

	cacheDir := filepath.Join(t.TempDir(), "vol")
	guard := mgr.localDiskGuard(cacheDir)
	if guard == nil {
		t.Fatal("expected local disk guard")
	}
	if guard.Path != cacheDir {
		t.Fatalf("guard path = %q, want %q", guard.Path, cacheDir)
	}
	if guard.MaxBytes != 128*1024*1024 {
		t.Fatalf("guard max bytes = %d, want 128Mi", guard.MaxBytes)
	}
	if guard.MinFreeBytes != 64*1024*1024 {
		t.Fatalf("guard min free bytes = %d, want 64Mi", guard.MinFreeBytes)
	}
}

func TestUnbindLockedSnapshotKeepsSharedVolumeUntilLastPortal(t *testing.T) {
	mgr := &Manager{
		portals:      make(map[string]*portalMount),
		boundVolumes: make(map[string]*boundVolume),
		volumes:      newLocalVolumeManager(),
	}
	volCtx := &volume.VolumeContext{
		VolumeID: "vol-1",
	}
	mgr.volumes.add(volCtx)
	mgr.boundVolumes["vol-1"] = &boundVolume{
		volumeID: "vol-1",
		refCount: 2,
		volCtx:   volCtx,
	}

	pmA := &portalMount{
		mountPath: "/workspace/a",
		volumeID:  "vol-1",
		teamID:    "team-a",
		mountedAt: time.Now().UTC(),
		fs:        volumefuse.New("portal-a", time.Second, unboundSession{}),
	}
	pmB := &portalMount{
		mountPath: "/workspace/b",
		volumeID:  "vol-1",
		teamID:    "team-a",
		mountedAt: time.Now().UTC(),
		fs:        volumefuse.New("portal-b", time.Second, unboundSession{}),
	}

	if err := mgr.unbindLockedSnapshot(pmA); err != nil {
		t.Fatalf("unbindLockedSnapshot(first) error = %v", err)
	}
	if pmA.volumeID != "" {
		t.Fatalf("first portal volumeID = %q, want cleared", pmA.volumeID)
	}
	bound := mgr.boundVolumes["vol-1"]
	if bound == nil {
		t.Fatal("bound volume removed after first unbind, want shared binding to remain")
	}
	if bound.refCount != 1 {
		t.Fatalf("bound refCount after first unbind = %d, want 1", bound.refCount)
	}
	if _, err := mgr.volumes.GetVolume("vol-1"); err != nil {
		t.Fatalf("GetVolume() after first unbind error = %v, want mounted volume to remain", err)
	}

	if err := mgr.unbindLockedSnapshot(pmB); err != nil {
		t.Fatalf("unbindLockedSnapshot(last) error = %v", err)
	}
	if _, ok := mgr.boundVolumes["vol-1"]; ok {
		t.Fatal("bound volume still present after last unbind")
	}
	if _, err := mgr.volumes.GetVolume("vol-1"); err == nil {
		t.Fatal("GetVolume() after last unbind error = nil, want volume removed")
	}
}

func TestCheckPublishedReportsMissingPortals(t *testing.T) {
	mgr := &Manager{
		portals: make(map[string]*portalMount),
		repo:    &db.Repository{},
	}
	mgr.portals[portalKey("pod-uid", "workspace")] = &portalMount{
		podUID: "pod-uid",
		name:   "workspace",
	}

	resp, err := mgr.CheckPublished(context.Background(), ctldapi.CheckVolumePortalsRequest{
		PodUID: "pod-uid",
		Portals: []ctldapi.VolumePortalRef{
			{PortalName: "workspace", MountPath: "/workspace"},
			{PortalName: "cache", MountPath: "/cache"},
		},
	})
	if err != nil {
		t.Fatalf("CheckPublished() error = %v", err)
	}
	if resp.Ready {
		t.Fatal("CheckPublished() ready = true, want false")
	}
	if len(resp.Missing) != 1 || resp.Missing[0] != "cache" {
		t.Fatalf("CheckPublished() missing = %v, want [cache]", resp.Missing)
	}
}

func TestCheckPublishedRequiresVolumeRegistryForPortals(t *testing.T) {
	mgr := &Manager{
		portals: make(map[string]*portalMount),
	}
	_, err := mgr.CheckPublished(context.Background(), ctldapi.CheckVolumePortalsRequest{
		PodUID: "pod-uid",
		Portals: []ctldapi.VolumePortalRef{
			{PortalName: "workspace", MountPath: "/workspace"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "ctld volume registry unavailable") {
		t.Fatalf("CheckPublished() error = %v, want registry unavailable", err)
	}
}

func TestCleanupIdleOwnerOnlyVolumesRemovesIdleOwner(t *testing.T) {
	mgr := &Manager{
		ownerOnlyIdleTTL: 50 * time.Millisecond,
		boundVolumes:     make(map[string]*boundVolume),
		volumes:          newLocalVolumeManager(),
	}
	volCtx := &volume.VolumeContext{VolumeID: "vol-1"}
	mgr.volumes.add(volCtx)
	mgr.volumes.requests["vol-1"].lastAccess = time.Now().UTC().Add(-time.Minute)
	mgr.boundVolumes["vol-1"] = &boundVolume{
		volumeID: "vol-1",
		refCount: 0,
		volCtx:   volCtx,
	}

	mgr.cleanupIdleOwnerOnlyVolumes(context.Background())

	if _, ok := mgr.boundVolumes["vol-1"]; ok {
		t.Fatal("bound volume still present after idle owner-only cleanup")
	}
	if _, err := mgr.volumes.GetVolume("vol-1"); err == nil {
		t.Fatal("GetVolume() after idle owner-only cleanup error = nil, want volume removed")
	}
}

func TestCleanupIdleOwnerOnlyVolumesKeepsOwnerOnMaterializeFailure(t *testing.T) {
	engine, cacheDir := newDirtyConflictS0FSEngine(t, "vol-1")
	defer engine.Close()

	mgr := &Manager{
		ownerOnlyIdleTTL: 50 * time.Millisecond,
		boundVolumes:     make(map[string]*boundVolume),
		volumes:          newLocalVolumeManager(),
	}
	volCtx := &volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
	}
	mgr.volumes.add(volCtx)
	mgr.volumes.requests["vol-1"].lastAccess = time.Now().UTC().Add(-time.Minute)
	mgr.boundVolumes["vol-1"] = &boundVolume{
		volumeID: "vol-1",
		refCount: 0,
		volCtx:   volCtx,
	}

	mgr.cleanupIdleOwnerOnlyVolumes(context.Background())

	if _, ok := mgr.boundVolumes["vol-1"]; !ok {
		t.Fatal("bound volume removed after failed materialize, want owner to remain active")
	}
	if _, err := mgr.volumes.GetVolume("vol-1"); err != nil {
		t.Fatalf("GetVolume() after failed cleanup error = %v, want mounted volume to remain", err)
	}
}

func TestReleaseOwnerRemovesOwnerOnlyVolumeBeforeIdleTTL(t *testing.T) {
	mgr := &Manager{
		ownerOnlyIdleTTL: time.Minute,
		boundVolumes:     make(map[string]*boundVolume),
		volumes:          newLocalVolumeManager(),
	}
	volCtx := &volume.VolumeContext{VolumeID: "vol-1"}
	mgr.volumes.add(volCtx)
	mgr.boundVolumes["vol-1"] = &boundVolume{
		volumeID: "vol-1",
		refCount: 0,
		volCtx:   volCtx,
	}

	resp, err := mgr.ReleaseOwner(context.Background(), ctldapi.ReleaseVolumeOwnerRequest{SandboxVolumeID: "vol-1"})
	if err != nil {
		t.Fatalf("ReleaseOwner() error = %v", err)
	}
	if !resp.Released || resp.Busy {
		t.Fatalf("ReleaseOwner() response = %+v, want released", resp)
	}
	if _, ok := mgr.boundVolumes["vol-1"]; ok {
		t.Fatal("bound volume still present after ReleaseOwner")
	}
	if _, err := mgr.volumes.GetVolume("vol-1"); err == nil {
		t.Fatal("GetVolume() after ReleaseOwner error = nil, want volume removed")
	}
}

func TestReleaseOwnerReturnsBusyForActivePortal(t *testing.T) {
	mgr := &Manager{
		boundVolumes: make(map[string]*boundVolume),
		volumes:      newLocalVolumeManager(),
	}
	volCtx := &volume.VolumeContext{VolumeID: "vol-1"}
	mgr.volumes.add(volCtx)
	mgr.boundVolumes["vol-1"] = &boundVolume{
		volumeID: "vol-1",
		refCount: 1,
		volCtx:   volCtx,
	}

	resp, err := mgr.ReleaseOwner(context.Background(), ctldapi.ReleaseVolumeOwnerRequest{SandboxVolumeID: "vol-1"})
	if err == nil {
		t.Fatal("ReleaseOwner() error = nil, want busy error")
	}
	if !resp.Busy || resp.Released {
		t.Fatalf("ReleaseOwner() response = %+v, want busy", resp)
	}
	if _, ok := mgr.boundVolumes["vol-1"]; !ok {
		t.Fatal("bound volume removed after busy ReleaseOwner")
	}
}

func TestReleaseOwnerReturnsBusyForInFlightDirectRequest(t *testing.T) {
	mgr := &Manager{
		boundVolumes: make(map[string]*boundVolume),
		volumes:      newLocalVolumeManager(),
	}
	volCtx := &volume.VolumeContext{VolumeID: "vol-1"}
	mgr.volumes.add(volCtx)
	release, err := mgr.volumes.acquire(context.Background(), "vol-1")
	if err != nil {
		t.Fatalf("acquire() error = %v", err)
	}
	defer release()
	mgr.boundVolumes["vol-1"] = &boundVolume{
		volumeID: "vol-1",
		refCount: 0,
		volCtx:   volCtx,
	}

	resp, err := mgr.ReleaseOwner(context.Background(), ctldapi.ReleaseVolumeOwnerRequest{SandboxVolumeID: "vol-1"})
	if err == nil {
		t.Fatal("ReleaseOwner() error = nil, want busy error")
	}
	if !resp.Busy || resp.Released {
		t.Fatalf("ReleaseOwner() response = %+v, want busy", resp)
	}
	if _, ok := mgr.boundVolumes["vol-1"]; !ok {
		t.Fatal("bound volume removed after in-flight ReleaseOwner")
	}
}

func TestNewManagerDefaultsClusterID(t *testing.T) {
	mgr := NewManager(Config{
		StorageConfig: &apiconfig.StorageProxyConfig{},
	})
	if mgr.clusterID != naming.DefaultClusterID {
		t.Fatalf("clusterID = %q, want %q", mgr.clusterID, naming.DefaultClusterID)
	}
}
