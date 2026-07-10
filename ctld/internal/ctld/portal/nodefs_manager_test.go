package portal

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

type fakeNodeFSMounter struct {
	mu         sync.Mutex
	binds      [][2]string
	unmounts   []string
	bindErr    error
	unmountErr error
}

type closeTrackingSession struct {
	volumefuse.Session
	closed atomic.Bool
}

func (s *closeTrackingSession) Close() {
	s.closed.Store(true)
}

func (m *fakeNodeFSMounter) EnsureBind(sourcePath, targetPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.binds = append(m.binds, [2]string{sourcePath, targetPath})
	return m.bindErr
}

func (m *fakeNodeFSMounter) Unmount(targetPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unmounts = append(m.unmounts, targetPath)
	return m.unmountErr
}

func newNodeFSTestManager(t *testing.T, rootDir string, mounter nodeFSMounter, factory nodeFSConnectionFactory) *Manager {
	t.Helper()
	mgr := NewManager(Config{
		NodeName:              "node-a",
		RootDir:               rootDir,
		KubeletPodsRoot:       filepath.Join(rootDir, "kubelet", "pods"),
		NodeFSShardCount:      1,
		NodeFSRequireRecovery: true,
	})
	mgr.nodeFSMounter = mounter
	mgr.nodeFSFactory = factory
	return mgr
}

func newNodeFSTestFactory(resume bool) (*fakeNodeFSConnectionFactory, *fakeNodeFUSEServer) {
	server := newFakeNodeFUSEServer(recoveryConnectionState())
	factory := &fakeNodeFSConnectionFactory{}
	if resume {
		factory.resumeServer = server
	} else {
		factory.newServer = server
	}
	return factory, server
}

func publishNodeFSTestPortal(t *testing.T, mgr *Manager) (*portalMount, nodeFSPortalState) {
	t.Helper()
	target := filepath.Join(mgr.kubeletPodsRoot, "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-0-workspace", "mount")
	err := mgr.PublishPortal(context.Background(), publishRequest{
		Namespace:  "default",
		PodName:    "sandbox-a",
		PodUID:     "pod-a",
		Name:       "workspace",
		MountPath:  "/workspace",
		TargetPath: target,
	})
	if err != nil {
		t.Fatalf("PublishPortal() error = %v", err)
	}
	mgr.mu.Lock()
	pm := mgr.portals[portalKey("pod-a", "workspace")]
	mgr.mu.Unlock()
	state, ok := mgr.nodeFS.journal.Portal(portalKey("pod-a", "workspace"))
	if pm == nil || !ok {
		t.Fatalf("published portal = %+v, journal found = %v", pm, ok)
	}
	return pm, state
}

func releaseNodeFSTestProcess(t *testing.T, mgr *Manager) {
	t.Helper()
	if mgr == nil || mgr.nodeFS == nil || mgr.nodeFS.lock == nil {
		return
	}
	if err := mgr.nodeFS.lock.Close(); err != nil {
		t.Fatalf("close nodefs test process lock: %v", err)
	}
}

func TestNodeFSInitializeRejectsInvalidConfiguration(t *testing.T) {
	for _, cfg := range []Config{
		{NodeFSShardCount: -1},
		{NodeFSShardCount: 65},
		{NodeFSRequireRecovery: true},
	} {
		mgr := NewManager(cfg)
		if err := mgr.Initialize(context.Background()); err == nil {
			t.Fatalf("Initialize(%+v) error = nil", cfg)
		}
	}
}

func TestNodeFSInitializePublishAndUnpublishLifecycle(t *testing.T) {
	rootDir := t.TempDir()
	mounter := &fakeNodeFSMounter{}
	factory, _ := newNodeFSTestFactory(false)
	mgr := newNodeFSTestManager(t, rootDir, mounter, factory)
	if err := mgr.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	defer releaseNodeFSTestProcess(t, mgr)

	pm, state := publishNodeFSTestPortal(t, mgr)
	if state.Phase != nodeFSPortalPublished {
		t.Fatalf("portal phase = %q, want %q", state.Phase, nodeFSPortalPublished)
	}
	if pm.nodeFSRouteName != nodeFSRouteName(state.Slot) || pm.nodeFSSourcePath == "" {
		t.Fatalf("portal nodefs identity = %+v, journal = %+v", pm, state)
	}
	if _, err := os.Stat(pm.rootfsBackingPath); err != nil {
		t.Fatalf("stat rootfs backing: %v", err)
	}
	if err := mgr.UnpublishPortalContext(context.Background(), pm.targetPath); err != nil {
		t.Fatalf("UnpublishPortalContext() error = %v", err)
	}
	if _, ok := mgr.nodeFS.journal.Portal(state.PortalKey); ok {
		t.Fatal("unpublished portal remains in journal")
	}
	if _, err := os.Stat(pm.rootfsBackingPath); !os.IsNotExist(err) {
		t.Fatalf("rootfs backing stat error = %v, want not exist", err)
	}
	if len(mounter.binds) != 1 || len(mounter.unmounts) != 1 {
		t.Fatalf("mounter calls binds=%v unmounts=%v", mounter.binds, mounter.unmounts)
	}
}

func TestNodeFSInitializeRecoversPublishedPortalBeforeReturning(t *testing.T) {
	rootDir := t.TempDir()
	firstMounter := &fakeNodeFSMounter{}
	firstFactory, _ := newNodeFSTestFactory(false)
	first := newNodeFSTestManager(t, rootDir, firstMounter, firstFactory)
	if err := first.Initialize(context.Background()); err != nil {
		t.Fatalf("first Initialize() error = %v", err)
	}
	pm, state := publishNodeFSTestPortal(t, first)
	releaseNodeFSTestProcess(t, first)

	secondMounter := &fakeNodeFSMounter{}
	secondFactory, _ := newNodeFSTestFactory(true)
	second := newNodeFSTestManager(t, rootDir, secondMounter, secondFactory)
	if err := second.Initialize(context.Background()); err != nil {
		t.Fatalf("second Initialize() error = %v", err)
	}
	defer releaseNodeFSTestProcess(t, second)
	if secondFactory.resumeCalls != 1 || secondFactory.newCalls != 0 {
		t.Fatalf("connection factory resume=%d new=%d", secondFactory.resumeCalls, secondFactory.newCalls)
	}
	second.mu.Lock()
	restored := second.portals[state.PortalKey]
	second.mu.Unlock()
	if restored == nil || restored.nodeFSSourcePath != pm.nodeFSSourcePath {
		t.Fatalf("restored portal = %+v, want source %s", restored, pm.nodeFSSourcePath)
	}
	if len(secondMounter.binds) != 1 || secondMounter.binds[0][1] != pm.targetPath {
		t.Fatalf("recovery binds = %v", secondMounter.binds)
	}
}

func TestNodeFSRecoveryStaleCleanupCompletesCommittedPortal(t *testing.T) {
	rootDir := t.TempDir()
	firstFactory, _ := newNodeFSTestFactory(false)
	first := newNodeFSTestManager(t, rootDir, &fakeNodeFSMounter{}, firstFactory)
	if err := first.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	pm, state := publishNodeFSTestPortal(t, first)
	if err := os.MkdirAll(pm.targetPath, 0o755); err != nil {
		t.Fatal(err)
	}
	releaseNodeFSTestProcess(t, first)

	mounter := &fakeNodeFSMounter{}
	secondFactory, _ := newNodeFSTestFactory(true)
	second := newNodeFSTestManager(t, rootDir, mounter, secondFactory)
	second.activePodUIDLister = func(context.Context) (map[string]struct{}, error) {
		return map[string]struct{}{}, nil
	}
	rawCleanerCalled := false
	second.staleMountCleaner = func(string) error {
		rawCleanerCalled = true
		return nil
	}
	if err := second.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer releaseNodeFSTestProcess(t, second)

	if err := second.CleanupStaleCSIMounts(context.Background()); err != nil {
		t.Fatalf("CleanupStaleCSIMounts() error = %v", err)
	}
	if rawCleanerCalled {
		t.Fatal("committed nodefs target used the raw stale mount cleaner")
	}
	if len(mounter.unmounts) != 1 || mounter.unmounts[0] != pm.targetPath {
		t.Fatalf("nodefs unmounts = %v, want %s", mounter.unmounts, pm.targetPath)
	}
	if _, ok := second.nodeFS.journal.Portal(state.PortalKey); ok {
		t.Fatal("stale recovered portal remains in journal")
	}
	second.mu.Lock()
	restored := second.portals[state.PortalKey]
	byTarget := second.portalsByTarget[pm.targetPath]
	second.mu.Unlock()
	if restored != nil || byTarget != nil {
		t.Fatalf("stale recovered portal remains registered: portal=%v target=%v", restored, byTarget)
	}
	if _, err := os.Stat(pm.rootfsBackingPath); !os.IsNotExist(err) {
		t.Fatalf("rootfs backing stat error = %v, want not exist", err)
	}
}

func TestNodeFSStaleCleanupReleasesBoundVolume(t *testing.T) {
	rootDir := t.TempDir()
	mounter := &fakeNodeFSMounter{}
	factory, _ := newNodeFSTestFactory(false)
	mgr := newNodeFSTestManager(t, rootDir, mounter, factory)
	mgr.activePodUIDLister = func(context.Context) (map[string]struct{}, error) {
		return map[string]struct{}{}, nil
	}
	if err := mgr.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer releaseNodeFSTestProcess(t, mgr)
	pm, state := publishNodeFSTestPortal(t, mgr)
	if err := os.MkdirAll(pm.targetPath, 0o755); err != nil {
		t.Fatal(err)
	}

	backendSession := &closeTrackingSession{Session: unboundSession{}}
	bound := &boundVolume{
		volumeID:  "volume-a",
		teamID:    "team-a",
		access:    volume.AccessModeRWO,
		mountedAt: time.Now().UTC(),
		refCount:  1,
		volCtx:    &volume.VolumeContext{VolumeID: "volume-a", TeamID: "team-a"},
		session:   backendSession,
	}
	mgr.mu.Lock()
	mgr.boundVolumes[bound.volumeID] = bound
	mgr.volumes.add(bound.volCtx)
	pm.volumeID = bound.volumeID
	pm.teamID = bound.teamID
	pm.mountedAt = bound.mountedAt
	mgr.mu.Unlock()
	if err := mgr.nodeFS.journal.UpdatePortalBinding(state.PortalKey, volume.BackendS0FS, bound.volumeID, bound.teamID, bound.mountedAt); err != nil {
		t.Fatal(err)
	}
	shard := &mgr.nodeFS.shards[pm.nodeFSShard]
	if _, err := shard.mux.UpdatePortalSession(pm.nodeFSRouteName, bound.volumeID, bound.session); err != nil {
		t.Fatal(err)
	}

	if err := mgr.CleanupStaleCSIMounts(context.Background()); err != nil {
		t.Fatalf("CleanupStaleCSIMounts() error = %v", err)
	}
	if !backendSession.closed.Load() {
		t.Fatal("stale cleanup did not close the final backend session")
	}
	mgr.mu.Lock()
	remaining := mgr.boundVolumes[bound.volumeID]
	mgr.mu.Unlock()
	if remaining != nil {
		t.Fatalf("stale cleanup retained bound volume: %+v", remaining)
	}
	if _, ok := mgr.nodeFS.journal.Portal(state.PortalKey); ok {
		t.Fatal("stale bound portal remains in journal")
	}
}

func TestNodeFSRecoveryFailsClosedWhenPublishedBackingIsMissing(t *testing.T) {
	rootDir := t.TempDir()
	firstFactory, _ := newNodeFSTestFactory(false)
	first := newNodeFSTestManager(t, rootDir, &fakeNodeFSMounter{}, firstFactory)
	if err := first.Initialize(context.Background()); err != nil {
		t.Fatalf("first Initialize() error = %v", err)
	}
	pm, _ := publishNodeFSTestPortal(t, first)
	if err := os.RemoveAll(pm.rootfsBackingPath); err != nil {
		t.Fatal(err)
	}
	releaseNodeFSTestProcess(t, first)

	secondFactory, _ := newNodeFSTestFactory(true)
	second := newNodeFSTestManager(t, rootDir, &fakeNodeFSMounter{}, secondFactory)
	if err := second.Initialize(context.Background()); err == nil {
		t.Fatal("second Initialize() error = nil, want missing backing failure")
	}
	defer releaseNodeFSTestProcess(t, second)
	if secondFactory.resumeCalls != 0 {
		t.Fatalf("resume calls = %d, want 0 before fail-closed restore", secondFactory.resumeCalls)
	}
}

func TestNodeFSRecoveryCompletesUnpublishWithoutBacking(t *testing.T) {
	rootDir := t.TempDir()
	firstFactory, _ := newNodeFSTestFactory(false)
	first := newNodeFSTestManager(t, rootDir, &fakeNodeFSMounter{}, firstFactory)
	if err := first.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	pm, state := publishNodeFSTestPortal(t, first)
	if err := first.nodeFS.journal.BeginPortalUnpublish(state.PortalKey); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(pm.rootfsBackingPath); err != nil {
		t.Fatal(err)
	}
	releaseNodeFSTestProcess(t, first)

	mounter := &fakeNodeFSMounter{}
	secondFactory, _ := newNodeFSTestFactory(true)
	second := newNodeFSTestManager(t, rootDir, mounter, secondFactory)
	if err := second.Initialize(context.Background()); err != nil {
		t.Fatalf("second Initialize() error = %v", err)
	}
	defer releaseNodeFSTestProcess(t, second)
	if _, ok := second.nodeFS.journal.Portal(state.PortalKey); ok {
		t.Fatal("unpublishing portal remains in journal after recovery")
	}
	if len(mounter.unmounts) != 1 || len(mounter.binds) != 0 {
		t.Fatalf("recovery mounter calls binds=%v unmounts=%v", mounter.binds, mounter.unmounts)
	}
}

func TestNodeFSRecoveryCreatesMissingAllocatingBacking(t *testing.T) {
	rootDir := t.TempDir()
	firstFactory, _ := newNodeFSTestFactory(false)
	first := newNodeFSTestManager(t, rootDir, &fakeNodeFSMounter{}, firstFactory)
	if err := first.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	backing := first.unboundRootFSBackingPath("pod-a", "workspace")
	target := filepath.Join(first.kubeletPodsRoot, "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-0-workspace", "mount")
	allocated, err := first.nodeFS.journal.AllocatePortal(nodeFSPortalState{
		PortalKey:     portalKey("pod-a", "workspace"),
		PodUID:        "pod-a",
		Name:          "workspace",
		MountPath:     "/workspace",
		TargetPath:    target,
		RootFSBacking: backing,
		Backend:       nodeFSRootBackend,
	})
	if err != nil {
		t.Fatal(err)
	}
	releaseNodeFSTestProcess(t, first)

	secondFactory, _ := newNodeFSTestFactory(true)
	second := newNodeFSTestManager(t, rootDir, &fakeNodeFSMounter{}, secondFactory)
	if err := second.Initialize(context.Background()); err != nil {
		t.Fatalf("second Initialize() error = %v", err)
	}
	defer releaseNodeFSTestProcess(t, second)
	if err := requireDirectory(backing); err != nil {
		t.Fatalf("allocating backing was not recreated: %v", err)
	}
	state, ok := second.nodeFS.journal.Portal(allocated.PortalKey)
	if !ok || state.Phase != nodeFSPortalPublished {
		t.Fatalf("reconciled portal = %+v, found=%v", state, ok)
	}
}

func TestNodeFSRestoreRejectsBackingOutsideTrustedRoot(t *testing.T) {
	mgr := NewManager(Config{RootDir: t.TempDir()})
	outside := filepath.Join(t.TempDir(), "outside")
	if _, err := mgr.validateNodeFSRootFSBacking(outside, "pod-a", "workspace"); err == nil {
		t.Fatal("validateNodeFSRootFSBacking() error = nil")
	}
	if err := removeNodeFSRootFSBacking(mgr.rootDir, outside); err == nil {
		t.Fatal("removeNodeFSRootFSBacking() error = nil")
	}
}

func TestNodeFSRestoreRejectsSymlinkedBackingParent(t *testing.T) {
	rootDir := t.TempDir()
	outside := t.TempDir()
	root := filepath.Join(rootDir, "rootfs-portals")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "pod-a")); err != nil {
		t.Fatal(err)
	}
	mgr := NewManager(Config{RootDir: rootDir})
	path := mgr.unboundRootFSBackingPath("pod-a", "workspace")
	if _, err := mgr.validateNodeFSRootFSBacking(path, "pod-a", "workspace"); err == nil {
		t.Fatal("validateNodeFSRootFSBacking() error = nil for symlinked parent")
	}
}

func TestNodeFSShutdownPreservesMountsJournalAndBacking(t *testing.T) {
	rootDir := t.TempDir()
	mounter := &fakeNodeFSMounter{}
	factory, server := newNodeFSTestFactory(false)
	mgr := newNodeFSTestManager(t, rootDir, mounter, factory)
	if err := mgr.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer releaseNodeFSTestProcess(t, mgr)
	pm, state := publishNodeFSTestPortal(t, mgr)
	heartbeatCtx, heartbeatCancel := context.WithCancel(context.Background())
	heartbeatDone := make(chan struct{})
	go func() { <-heartbeatCtx.Done(); close(heartbeatDone) }()
	materializeCtx, materializeCancel := context.WithCancel(context.Background())
	materializeDone := make(chan struct{})
	go func() { <-materializeCtx.Done(); close(materializeDone) }()
	backendSession := &closeTrackingSession{Session: unboundSession{}}
	bound := &boundVolume{
		volumeID:          "volume-a",
		teamID:            "team-a",
		refCount:          1,
		volCtx:            &volume.VolumeContext{VolumeID: "volume-a"},
		session:           backendSession,
		heartbeatCancel:   heartbeatCancel,
		heartbeatDone:     heartbeatDone,
		materializeCancel: materializeCancel,
		materializeDone:   materializeDone,
	}
	mgr.mu.Lock()
	mgr.boundVolumes[bound.volumeID] = bound
	mgr.volumes.add(bound.volCtx)
	mgr.mu.Unlock()
	if err := mgr.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if server.detached {
		t.Fatal("Shutdown() detached active nodefs shard")
	}
	if len(mounter.unmounts) != 0 {
		t.Fatalf("Shutdown() unmounted CSI targets: %v", mounter.unmounts)
	}
	if _, ok := mgr.nodeFS.journal.Portal(state.PortalKey); !ok {
		t.Fatal("Shutdown() removed portal journal state")
	}
	if err := requireDirectory(pm.rootfsBackingPath); err != nil {
		t.Fatalf("Shutdown() removed rootfs backing: %v", err)
	}
	if mgr.nodeFS.lock.file == nil {
		t.Fatal("Shutdown() released nodefs process lock")
	}
	if backendSession.closed.Load() {
		t.Fatal("Shutdown() closed durable backend session")
	}
	if mgr.boundVolumes[bound.volumeID] != bound {
		t.Fatal("Shutdown() removed durable backend state")
	}
	select {
	case <-heartbeatDone:
	default:
		t.Fatal("Shutdown() did not stop heartbeat")
	}
	select {
	case <-materializeDone:
	default:
		t.Fatal("Shutdown() did not stop materializer")
	}
}

func TestNodeFSUnbindCommitsRootFSAndReleasesLastBackend(t *testing.T) {
	rootDir := t.TempDir()
	factory, _ := newNodeFSTestFactory(false)
	mgr := newNodeFSTestManager(t, rootDir, &fakeNodeFSMounter{}, factory)
	if err := mgr.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer releaseNodeFSTestProcess(t, mgr)
	pm, state := publishNodeFSTestPortal(t, mgr)

	bound := &boundVolume{
		volumeID:  "volume-a",
		teamID:    "team-a",
		access:    volume.AccessModeRWO,
		mountedAt: time.Now().UTC(),
		refCount:  1,
		volCtx:    &volume.VolumeContext{VolumeID: "volume-a", TeamID: "team-a"},
		session:   unboundSession{},
	}
	mgr.mu.Lock()
	mgr.boundVolumes[bound.volumeID] = bound
	mgr.volumes.add(bound.volCtx)
	pm.volumeID = bound.volumeID
	pm.teamID = bound.teamID
	pm.mountedAt = bound.mountedAt
	mgr.mu.Unlock()
	if err := mgr.nodeFS.journal.UpdatePortalBinding(state.PortalKey, volume.BackendS0FS, bound.volumeID, bound.teamID, bound.mountedAt); err != nil {
		t.Fatal(err)
	}
	shard := &mgr.nodeFS.shards[pm.nodeFSShard]
	if _, err := shard.mux.UpdatePortalSession(pm.nodeFSRouteName, bound.volumeID, bound.session); err != nil {
		t.Fatal(err)
	}

	response, err := mgr.Unbind(context.Background(), ctldapi.UnbindVolumePortalRequest{
		PodUID:     pm.podUID,
		PortalName: pm.name,
	})
	if err != nil {
		t.Fatalf("Unbind() error = %v", err)
	}
	if !response.Unbound || pm.volumeID != "" {
		t.Fatalf("Unbind() response=%+v portal=%+v", response, pm)
	}
	committed, ok := mgr.nodeFS.journal.Portal(state.PortalKey)
	if !ok || normalizeNodeFSBackend(committed.Backend) != nodeFSRootBackend || committed.VolumeID != "" || committed.TeamID != "" {
		t.Fatalf("journal after Unbind = %+v, found=%v", committed, ok)
	}
	if _, ok := mgr.boundVolumes[bound.volumeID]; ok {
		t.Fatal("last bound volume remains after Unbind")
	}
}

func TestNodeFSRequiredRecoveryRejectsS3Session(t *testing.T) {
	mgr := &Manager{nodeFSRequireRecovery: true}
	_, err := mgr.restoreNodeFSBoundVolume(context.Background(), nodeFSPortalState{
		Backend:  volume.BackendS3,
		VolumeID: "volume-a",
		TeamID:   "team-a",
	})
	if err == nil || !strings.Contains(err.Error(), "S3 session recovery is not enabled") {
		t.Fatalf("restoreNodeFSBoundVolume() error = %v, want S3 recovery rejection", err)
	}
	if err := validateNodeFSRecoverableBackend(false, volume.BackendS3); err != nil {
		t.Fatalf("non-required nodefs S3 validation error = %v", err)
	}
}
