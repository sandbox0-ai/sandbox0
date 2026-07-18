package portal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

type testStorageObserver struct{}

func (*testStorageObserver) ObserveVolumeState(context.Context, string, string, *s0fs.SnapshotState, time.Time) error {
	return nil
}

func TestPortalMountOptionsDisableUnsupportedIDMapCapability(t *testing.T) {
	opts := portalMountOptions()
	if opts.DisabledCapabilities&fuse.CAP_ALLOW_IDMAP == 0 {
		t.Fatal("portal mount options enable FUSE_ALLOW_IDMAP without default_permissions")
	}
}

func TestNewS0FSVolumeContextWiresStorageObserver(t *testing.T) {
	observer := &testStorageObserver{}
	mgr := NewManager(Config{
		RootDir:         t.TempDir(),
		StorageObserver: observer,
	})
	mountedAt := time.Now().UTC()
	volCtx := mgr.newS0FSVolumeContext("vol-1", "team-1", nil, volume.AccessModeRWO, mountedAt, "/cache")

	if volCtx.Observer != observer {
		t.Fatal("newS0FSVolumeContext() did not wire the configured storage observer")
	}
	if volCtx.VolumeID != "vol-1" || volCtx.TeamID != "team-1" || volCtx.Backend != volume.BackendS0FS {
		t.Fatalf("newS0FSVolumeContext() identity = %#v", volCtx)
	}
	if volCtx.Access != volume.AccessModeRWO || !volCtx.MountedAt.Equal(mountedAt) || volCtx.CacheDir != "/cache" {
		t.Fatalf("newS0FSVolumeContext() mount metadata = %#v", volCtx)
	}
}

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

	cleanup := mgr.unbindLockedSnapshot(pmA)
	if cleanup != nil {
		t.Fatalf("unbindLockedSnapshot(first) cleanup = %+v, want nil", cleanup)
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

	cleanup = mgr.unbindLockedSnapshot(pmB)
	if err := mgr.finishBoundVolumeCleanup(context.Background(), cleanup); err != nil {
		t.Fatalf("finishBoundVolumeCleanup(last) error = %v", err)
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

func TestRootFSPortalPathsIncludesUnboundSystemState(t *testing.T) {
	mgr := &Manager{portals: map[string]*portalMount{
		"system": {
			podUID:            "pod-uid",
			name:              volumeportal.WebhookStatePortalName,
			mountPath:         volumeportal.WebhookStateMountPath,
			rootfsBackingPath: "/var/lib/sandbox0/ctld/rootfs/pod-uid/system",
		},
		"bound": {
			podUID:            "pod-uid",
			name:              "workspace",
			mountPath:         "/workspace",
			rootfsBackingPath: "/var/lib/sandbox0/ctld/rootfs/pod-uid/workspace",
			volumeID:          "vol-1",
		},
	}}

	got := mgr.RootFSPortalPaths("pod-uid")
	if len(got) != 1 {
		t.Fatalf("RootFSPortalPaths() = %#v, want one unbound portal", got)
	}
	if got[0].PortalName != volumeportal.WebhookStatePortalName ||
		got[0].MountPath != volumeportal.WebhookStateMountPath {
		t.Fatalf("RootFSPortalPaths()[0] = %#v, want system state portal", got[0])
	}
}

func TestShutdownDrainsPublishedAndOwnerOnlyVolumes(t *testing.T) {
	rootDir := t.TempDir()
	var detachedTarget string
	mgr := &Manager{
		staleMountCleaner: func(path string) error {
			detachedTarget = path
			return os.RemoveAll(path)
		},
		portals:         make(map[string]*portalMount),
		portalsByTarget: make(map[string]*portalMount),
		boundVolumes:    make(map[string]*boundVolume),
		volumes:         newLocalVolumeManager(),
	}
	portalVolCtx := &volume.VolumeContext{VolumeID: "vol-portal"}
	ownerVolCtx := &volume.VolumeContext{VolumeID: "vol-owner"}
	mgr.volumes.add(portalVolCtx)
	mgr.volumes.add(ownerVolCtx)
	pm := &portalMount{
		podUID:            "pod-1",
		name:              "workspace",
		targetPath:        filepath.Join(rootDir, "target"),
		rootfsBackingPath: filepath.Join(rootDir, "rootfs"),
		volumeID:          "vol-portal",
	}
	mgr.portals[portalKey(pm.podUID, pm.name)] = pm
	mgr.portalsByTarget[pm.targetPath] = pm
	mgr.boundVolumes["vol-portal"] = &boundVolume{
		volumeID: "vol-portal",
		refCount: 1,
		volCtx:   portalVolCtx,
	}
	mgr.boundVolumes["vol-owner"] = &boundVolume{
		volumeID: "vol-owner",
		refCount: 0,
		volCtx:   ownerVolCtx,
	}
	for _, path := range []string{pm.targetPath, pm.rootfsBackingPath} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", path, err)
		}
	}

	if err := mgr.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if len(mgr.portals) != 0 || len(mgr.portalsByTarget) != 0 {
		t.Fatalf("portals after Shutdown = %d/%d, want empty", len(mgr.portals), len(mgr.portalsByTarget))
	}
	if len(mgr.boundVolumes) != 0 {
		t.Fatalf("boundVolumes after Shutdown = %#v, want empty", mgr.boundVolumes)
	}
	if detachedTarget != pm.targetPath {
		t.Fatalf("detached target = %q, want %q", detachedTarget, pm.targetPath)
	}
	for _, volumeID := range []string{"vol-portal", "vol-owner"} {
		if _, err := mgr.volumes.GetVolume(volumeID); err == nil {
			t.Fatalf("GetVolume(%q) after Shutdown error = nil, want removed", volumeID)
		}
	}
	if _, err := os.Stat(pm.rootfsBackingPath); !os.IsNotExist(err) {
		t.Fatalf("rootfs backing stat error = %v, want not exist", err)
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
	processConfigPath := filepath.Join(t.TempDir(), "ctld.yaml")
	if err := os.WriteFile(processConfigPath, []byte("default_cluster_id: unrelated\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("CONFIG_PATH", processConfigPath)

	mgr := NewManager(Config{})
	if mgr.clusterID != naming.DefaultClusterID {
		t.Fatalf("clusterID = %q, want %q", mgr.clusterID, naming.DefaultClusterID)
	}
	if mgr.storage.DefaultClusterId != "" {
		t.Fatalf("storage default cluster ID = %q, want empty explicit default", mgr.storage.DefaultClusterId)
	}
}
