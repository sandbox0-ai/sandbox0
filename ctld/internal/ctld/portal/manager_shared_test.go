package portal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/volumefuse"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
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
	if opts.MaxWrite != 1<<20 {
		t.Fatalf("portal max write = %d, want %d", opts.MaxWrite, 1<<20)
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
		fs:        volumefuse.New("portal-a", time.Second, &localSession{}),
	}
	pmB := &portalMount{
		mountPath: "/workspace/b",
		volumeID:  "vol-1",
		teamID:    "team-a",
		mountedAt: time.Now().UTC(),
		fs:        volumefuse.New("portal-b", time.Second, &localSession{}),
	}

	cleanup := mgr.unbindLockedSnapshot(pmA, false)
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

	cleanup = mgr.unbindLockedSnapshot(pmB, false)
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

func TestShutdownDrainsPublishedAndOwnerOnlyVolumes(t *testing.T) {
	rootDir := t.TempDir()
	var detachedTarget string
	mgr := &Manager{
		rootDir: rootDir,
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
		rootfsBackingPath: filepath.Join(rootDir, "rootfs-portals", "pod-1", "workspace"),
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

func TestUnpublishPortalPreservesOverlayAttachedRootFSBacking(t *testing.T) {
	managerRoot := t.TempDir()
	overlayRoot := filepath.Join(t.TempDir(), "upper", "workspace")
	if err := os.MkdirAll(overlayRoot, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", overlayRoot, err)
	}
	marker := filepath.Join(overlayRoot, "marker.txt")
	if err := os.WriteFile(marker, []byte("persistent"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", marker, err)
	}
	session, err := newRootFSBackedSessionWithState(overlayRoot, "")
	if err != nil {
		t.Fatalf("newRootFSBackedSessionWithState() error = %v", err)
	}
	mgr := NewManager(Config{RootDir: managerRoot})
	pm := &portalMount{
		podUID:            "pod-1",
		name:              "workspace",
		targetPath:        filepath.Join(managerRoot, "target"),
		rootfsBackingPath: overlayRoot,
		rootfsSession:     session,
	}
	mgr.portals[portalKey(pm.podUID, pm.name)] = pm
	mgr.portalsByTarget[pm.targetPath] = pm

	if err := mgr.UnpublishPortal(pm.targetPath); err != nil {
		t.Fatalf("UnpublishPortal() error = %v", err)
	}
	payload, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", marker, err)
	}
	if string(payload) != "persistent" {
		t.Fatalf("marker content = %q, want persistent", string(payload))
	}
}

func TestEnsureRootFSBackingTargetCreatesRealDirectories(t *testing.T) {
	upper := t.TempDir()
	target, err := ensureRootFSBackingTarget(upper, "/workspace/project")
	if err != nil {
		t.Fatalf("ensureRootFSBackingTarget() error = %v", err)
	}
	want := filepath.Join(upper, "workspace", "project")
	if target != want {
		t.Fatalf("target = %q, want %q", target, want)
	}
	info, err := os.Stat(target)
	if err != nil || !info.IsDir() {
		t.Fatalf("target directory stat = %#v, %v", info, err)
	}
}

func TestEnsureRootFSBackingTargetRejectsSymlinkAncestor(t *testing.T) {
	upper := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(upper, "workspace")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if _, err := ensureRootFSBackingTarget(upper, "/workspace/project"); err == nil {
		t.Fatal("ensureRootFSBackingTarget() error = nil, want symlink rejection")
	}
	if _, err := os.Stat(filepath.Join(outside, "project")); !os.IsNotExist(err) {
		t.Fatalf("outside project stat error = %v, want not exist", err)
	}
}

func TestAttachRootFSBackingsFailsClosedAfterEarlyPortalWrite(t *testing.T) {
	mgr := NewManager(Config{RootDir: t.TempDir()})
	staging := mgr.unboundRootFSBackingPath("pod-1", "workspace")
	if err := os.MkdirAll(staging, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", staging, err)
	}
	marker := filepath.Join(staging, "early.txt")
	if err := os.WriteFile(marker, []byte("early"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", marker, err)
	}
	session, err := newRootFSBackedSessionWithState(staging, "")
	if err != nil {
		t.Fatalf("newRootFSBackedSessionWithState() error = %v", err)
	}
	defer session.Close()
	pm := &portalMount{
		podUID:            "pod-1",
		name:              "workspace",
		mountPath:         "/workspace",
		rootfsBackingPath: staging,
		rootfsSession:     session,
	}
	mgr.portals[portalKey(pm.podUID, pm.name)] = pm

	err = mgr.AttachRootFSBackings(context.Background(), "pod-1", t.TempDir())
	if err == nil {
		t.Fatal("AttachRootFSBackings() error = nil, want early-write rejection")
	}
	payload, readErr := os.ReadFile(marker)
	if readErr != nil || string(payload) != "early" {
		t.Fatalf("staging marker = %q, %v, want early", string(payload), readErr)
	}
	if session.rootPath() != staging {
		t.Fatalf("session root = %q, want %q", session.rootPath(), staging)
	}
}

func TestAttachRootFSBackingsRebasesUnboundPortalOntoMergedRoot(t *testing.T) {
	mgr := NewManager(Config{RootDir: t.TempDir()})
	// This test exercises attachment behavior independently from recovery-store
	// validation; production portals have a FUSE INIT request in their manifest.
	mgr.recoveryStore = nil
	staging := mgr.unboundRootFSBackingPath("pod-1", "workspace")
	if err := os.MkdirAll(staging, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", staging, err)
	}
	session, err := newRootFSBackedSessionWithState(staging, "")
	if err != nil {
		t.Fatalf("newRootFSBackedSessionWithState() error = %v", err)
	}
	defer session.Close()
	pm := &portalMount{
		podUID:            "pod-1",
		name:              "workspace",
		mountPath:         "/workspace",
		rootfsBackingPath: staging,
		rootfsSession:     session,
	}
	mgr.portals[portalKey(pm.podUID, pm.name)] = pm
	merged := t.TempDir()
	want := filepath.Join(merged, "workspace")
	if err := os.MkdirAll(want, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", want, err)
	}
	restored := filepath.Join(want, "restored.txt")
	if err := os.WriteFile(restored, []byte("from-head"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", restored, err)
	}

	if err := mgr.AttachRootFSBackings(context.Background(), "pod-1", merged); err != nil {
		t.Fatalf("AttachRootFSBackings() error = %v", err)
	}
	if pm.rootfsBackingPath != want || session.rootPath() != want {
		t.Fatalf("attached roots = %q/%q, want %q", pm.rootfsBackingPath, session.rootPath(), want)
	}
	payload, err := os.ReadFile(session.hostPath("restored.txt"))
	if err != nil || string(payload) != "from-head" {
		t.Fatalf("restored portal content = %q, %v, want from-head", string(payload), err)
	}
	if _, err := os.Stat(staging); !os.IsNotExist(err) {
		t.Fatalf("staging root stat error = %v, want not exist", err)
	}
}

func TestAttachRootFSBackingsSkipsBoundVolumes(t *testing.T) {
	mgr := NewManager(Config{RootDir: t.TempDir()})
	pm := &portalMount{
		podUID:    "pod-1",
		name:      "workspace",
		mountPath: "/workspace",
		volumeID:  "volume-1",
	}
	mgr.portals[portalKey(pm.podUID, pm.name)] = pm
	if err := mgr.AttachRootFSBackings(context.Background(), "pod-1", t.TempDir()); err != nil {
		t.Fatalf("AttachRootFSBackings() error = %v", err)
	}
}

func TestAttachRootFSBackingsSkipsRuntimeOwnedWebhookPortal(t *testing.T) {
	mgr := NewManager(Config{RootDir: t.TempDir()})
	staging := mgr.unboundRootFSBackingPath("pod-1", volumeportal.WebhookStatePortalName)
	if err := os.MkdirAll(staging, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", staging, err)
	}
	marker := filepath.Join(staging, "runtime-state")
	if err := os.WriteFile(marker, []byte("runtime"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", marker, err)
	}
	pm := &portalMount{
		podUID:            "pod-1",
		name:              volumeportal.WebhookStatePortalName,
		mountPath:         volumeportal.WebhookStateMountPath,
		rootfsBackingPath: staging,
	}
	mgr.portals[portalKey(pm.podUID, pm.name)] = pm

	if err := mgr.AttachRootFSBackings(context.Background(), "pod-1", t.TempDir()); err != nil {
		t.Fatalf("AttachRootFSBackings() error = %v", err)
	}
	payload, err := os.ReadFile(marker)
	if err != nil || string(payload) != "runtime" {
		t.Fatalf("runtime marker = %q, %v, want runtime", string(payload), err)
	}
}

func TestPrepareRecoveryRootFSBackingDoesNotRecreateExternalPath(t *testing.T) {
	mgr := NewManager(Config{RootDir: t.TempDir()})
	external := filepath.Join(t.TempDir(), "task-root", "workspace")

	err := mgr.prepareRecoveryRootFSBacking(external)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("prepareRecoveryRootFSBacking() error = %v, want not exist", err)
	}
	if _, err := os.Stat(filepath.Dir(external)); !os.IsNotExist(err) {
		t.Fatalf("external parent stat error = %v, want not exist", err)
	}

	owned := mgr.unboundRootFSBackingPath("pod-1", "workspace")
	if err := mgr.prepareRecoveryRootFSBacking(owned); err != nil {
		t.Fatalf("prepareRecoveryRootFSBacking(owned) error = %v", err)
	}
	if info, err := os.Stat(owned); err != nil || !info.IsDir() {
		t.Fatalf("owned backing stat = %#v, %v, want directory", info, err)
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
