package volume

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sirupsen/logrus"
)

type mockMountRegistrar struct {
	unregistered []string
}

func (m *mockMountRegistrar) RegisterMount(context.Context, string, MountOptions) error {
	return nil
}

func (m *mockMountRegistrar) UnregisterMount(_ context.Context, volumeID string) error {
	m.unregistered = append(m.unregistered, volumeID)
	return nil
}

func (m *mockMountRegistrar) ValidateMount(context.Context, string, AccessMode) error {
	return nil
}

func TestUnmountSandboxVolumes_UnregistersAndCleansState(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})
	registrar := &mockMountRegistrar{}
	mgr.SetMountRegistrar(registrar)

	volumeID := "vol-1"
	sandboxID := "sandbox-1"
	sessionID := "session-1"

	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID}
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		sessionID: {ID: sessionID},
	}
	mgr.TrackVolumeSession(sandboxID, volumeID, sessionID)

	errs := mgr.UnmountSandboxVolumes(context.Background(), sandboxID)
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %d", len(errs))
	}

	if len(registrar.unregistered) != 1 || registrar.unregistered[0] != volumeID {
		t.Fatalf("expected unregister called once with %s, got %+v", volumeID, registrar.unregistered)
	}

	if _, ok := mgr.volumes[volumeID]; ok {
		t.Fatalf("volume %s should be removed from manager", volumeID)
	}
	if _, ok := mgr.mountSessions[volumeID]; ok {
		t.Fatalf("mount sessions for %s should be removed", volumeID)
	}
	if _, ok := mgr.sandboxToVolumes[sandboxID]; ok {
		t.Fatalf("sandbox mapping for %s should be removed", sandboxID)
	}
}

func TestUnmountSandboxVolumes_LegacyNoSessionStillCleansState(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})

	volumeID := "vol-legacy"
	sandboxID := "sandbox-legacy"
	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID}
	mgr.TrackVolume(sandboxID, volumeID)

	errs := mgr.UnmountSandboxVolumes(context.Background(), sandboxID)
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %d", len(errs))
	}

	if _, ok := mgr.volumes[volumeID]; ok {
		t.Fatalf("volume %s should be removed", volumeID)
	}
	if _, ok := mgr.sandboxToVolumes[sandboxID]; ok {
		t.Fatalf("sandbox mapping for %s should be removed", sandboxID)
	}
}

func TestUnmountSandboxVolumes_OnlyRemovesOwnedSessions(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})
	registrar := &mockMountRegistrar{}
	mgr.SetMountRegistrar(registrar)

	volumeID := "vol-shared"
	sandboxA := "sandbox-a"
	sandboxB := "sandbox-b"
	sessionA := "session-a"
	sessionB := "session-b"

	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID}
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		sessionA: {ID: sessionA, SandboxID: sandboxA},
		sessionB: {ID: sessionB, SandboxID: sandboxB},
	}
	mgr.TrackVolumeSession(sandboxA, volumeID, sessionA)
	mgr.TrackVolumeSession(sandboxB, volumeID, sessionB)

	errs := mgr.UnmountSandboxVolumes(context.Background(), sandboxA)
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %d", len(errs))
	}

	if _, ok := mgr.volumes[volumeID]; !ok {
		t.Fatalf("volume %s should remain mounted while other sessions exist", volumeID)
	}
	if _, ok := mgr.mountSessions[volumeID][sessionA]; ok {
		t.Fatalf("session %s should be removed", sessionA)
	}
	if _, ok := mgr.mountSessions[volumeID][sessionB]; !ok {
		t.Fatalf("session %s should remain", sessionB)
	}
	if len(registrar.unregistered) != 0 {
		t.Fatalf("unregister should not be called while shared sessions remain, got %+v", registrar.unregistered)
	}
	if _, ok := mgr.sandboxToVolumes[sandboxA]; ok {
		t.Fatalf("sandbox mapping for %s should be removed", sandboxA)
	}
	if _, ok := mgr.sandboxToVolumes[sandboxB]; !ok {
		t.Fatalf("sandbox mapping for %s should remain", sandboxB)
	}
}

func TestUnmountSandboxVolumes_PreservesDirectSession(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})
	registrar := &mockMountRegistrar{}
	mgr.SetMountRegistrar(registrar)

	volumeID := "vol-shared-direct"
	sandboxID := "sandbox-a"
	sandboxSessionID := "sandbox-session"
	directSessionID := "direct-session"

	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID}
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		sandboxSessionID: {ID: sandboxSessionID, SandboxID: sandboxID, Scope: MountSessionScopeSandbox},
		directSessionID:  {ID: directSessionID, Scope: MountSessionScopeDirect},
	}
	mgr.directMounts[volumeID] = &directMountLease{
		SessionID: directSessionID,
		InFlight:  0,
		LastUsed:  time.Now(),
	}
	mgr.TrackVolumeSession(sandboxID, volumeID, sandboxSessionID)

	errs := mgr.UnmountSandboxVolumes(context.Background(), sandboxID)
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %d", len(errs))
	}

	if _, ok := mgr.mountSessions[volumeID][sandboxSessionID]; ok {
		t.Fatalf("sandbox session %s should be removed", sandboxSessionID)
	}
	if _, ok := mgr.mountSessions[volumeID][directSessionID]; !ok {
		t.Fatalf("direct session %s should remain", directSessionID)
	}
	if _, ok := mgr.directMounts[volumeID]; !ok {
		t.Fatalf("direct lease for %s should remain", volumeID)
	}
	if _, ok := mgr.volumes[volumeID]; !ok {
		t.Fatalf("volume %s should remain mounted while direct session exists", volumeID)
	}
	if len(registrar.unregistered) != 0 {
		t.Fatalf("unregister should not be called while direct session remains, got %+v", registrar.unregistered)
	}
}

func TestUnmountSandboxVolumes_SkipsLegacyUnscopedSessions(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})

	volumeID := "vol-legacy-session"
	sandboxID := "sandbox-legacy-session"
	sessionID := "session-legacy"

	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID}
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		sessionID: {ID: sessionID},
	}
	mgr.TrackVolume(sandboxID, volumeID)

	errs := mgr.UnmountSandboxVolumes(context.Background(), sandboxID)
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %d", len(errs))
	}

	if _, ok := mgr.volumes[volumeID]; !ok {
		t.Fatalf("volume %s should not be removed when only legacy unscoped sessions exist", volumeID)
	}
	if _, ok := mgr.mountSessions[volumeID][sessionID]; !ok {
		t.Fatalf("legacy session %s should remain to avoid accidental unmount", sessionID)
	}
	if _, ok := mgr.sandboxToVolumes[sandboxID]; ok {
		t.Fatalf("sandbox mapping for %s should be removed", sandboxID)
	}
}

func TestAcquireDirectVolumeFileMount_ReusesSessionUntilIdleCleanup(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})
	volumeID := "vol-direct"
	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID}

	mountCalls := 0
	nextSessionID := func() string {
		mountCalls++
		return "direct-session-" + string(rune('0'+mountCalls))
	}
	mountFn := func(context.Context) (string, error) {
		sessionID := nextSessionID()
		if mgr.mountSessions[volumeID] == nil {
			mgr.mountSessions[volumeID] = make(map[string]*MountSession)
		}
		mgr.mountSessions[volumeID][sessionID] = &MountSession{
			ID:        sessionID,
			CreatedAt: time.Now(),
		}
		return sessionID, nil
	}

	releaseA, err := mgr.AcquireDirectVolumeFileMount(context.Background(), volumeID, mountFn)
	if err != nil {
		t.Fatalf("AcquireDirectVolumeFileMount() error = %v", err)
	}
	releaseB, err := mgr.AcquireDirectVolumeFileMount(context.Background(), volumeID, mountFn)
	if err != nil {
		t.Fatalf("AcquireDirectVolumeFileMount() second error = %v", err)
	}
	if mountCalls != 1 {
		t.Fatalf("mount calls = %d, want 1", mountCalls)
	}
	lease := mgr.directMounts[volumeID]
	if lease == nil || lease.SessionID == "" || lease.InFlight != 2 {
		t.Fatalf("unexpected direct lease after acquire: %+v", lease)
	}

	releaseA()
	releaseB()

	lease = mgr.directMounts[volumeID]
	if lease == nil || lease.InFlight != 0 {
		t.Fatalf("unexpected direct lease after release: %+v", lease)
	}
	lease.LastUsed = time.Now().Add(-2 * time.Minute)

	errs := mgr.CleanupIdleDirectVolumeFileMounts(context.Background(), time.Minute)
	if len(errs) != 0 {
		t.Fatalf("CleanupIdleDirectVolumeFileMounts() errors = %d", len(errs))
	}
	if _, ok := mgr.directMounts[volumeID]; ok {
		t.Fatalf("direct lease for %s should be removed", volumeID)
	}
	if _, ok := mgr.mountSessions[volumeID]; ok {
		t.Fatalf("mount sessions for %s should be removed", volumeID)
	}
	if _, ok := mgr.volumes[volumeID]; ok {
		t.Fatalf("volume %s should be unmounted after idle cleanup", volumeID)
	}
}

func TestResolveMountRootReadOnlyUsesWritableFallbackForMissingRoot(t *testing.T) {
	readOnlyMeta := newFakeVolumeRootMeta()
	fallbackCalled := false
	var fallbackPath string

	rootInode, err := resolveMountRoot(readOnlyMeta, "vol-rox", true, func(path string) (fsmeta.Ino, error) {
		fallbackCalled = true
		fallbackPath = path
		return fsmeta.Ino(42), nil
	})
	if err != nil {
		t.Fatalf("resolveMountRoot returned error: %v", err)
	}
	if rootInode != fsmeta.Ino(42) {
		t.Fatalf("root inode = %d, want 42", rootInode)
	}
	if !fallbackCalled || fallbackPath != "vol-rox" {
		t.Fatalf("writable fallback called=%v path=%q, want vol-rox", fallbackCalled, fallbackPath)
	}
	if readOnlyMeta.mkdirCalls != 0 {
		t.Fatalf("read-only meta mkdir calls = %d, want 0", readOnlyMeta.mkdirCalls)
	}
}

func TestResolveMountRootReadOnlyUsesExistingRoot(t *testing.T) {
	readOnlyMeta := newFakeVolumeRootMeta()
	readOnlyMeta.addChild(fsmeta.RootInode, "vol-rox", fsmeta.Ino(43))
	fallbackCalled := false

	rootInode, err := resolveMountRoot(readOnlyMeta, "vol-rox", true, func(path string) (fsmeta.Ino, error) {
		fallbackCalled = true
		return 0, nil
	})
	if err != nil {
		t.Fatalf("resolveMountRoot returned error: %v", err)
	}
	if rootInode != fsmeta.Ino(43) {
		t.Fatalf("root inode = %d, want 43", rootInode)
	}
	if fallbackCalled {
		t.Fatal("writable fallback should not be called when read-only lookup finds the root")
	}
	if readOnlyMeta.mkdirCalls != 0 {
		t.Fatalf("read-only meta mkdir calls = %d, want 0", readOnlyMeta.mkdirCalls)
	}
}

func TestResolveMountRootReadWriteCreatesMissingRoot(t *testing.T) {
	writableMeta := newFakeVolumeRootMeta()

	rootInode, err := resolveMountRoot(writableMeta, "vol-rwo", false, nil)
	if err != nil {
		t.Fatalf("resolveMountRoot returned error: %v", err)
	}
	if rootInode == 0 {
		t.Fatal("root inode = 0, want created inode")
	}
	if writableMeta.mkdirCalls != 1 {
		t.Fatalf("mkdir calls = %d, want 1", writableMeta.mkdirCalls)
	}
}

type fakeVolumeRootMeta struct {
	children   map[fsmeta.Ino]map[string]fsmeta.Ino
	nextInode  fsmeta.Ino
	mkdirCalls int
	mkdirErr   syscall.Errno
}

func newFakeVolumeRootMeta() *fakeVolumeRootMeta {
	return &fakeVolumeRootMeta{
		children:  map[fsmeta.Ino]map[string]fsmeta.Ino{fsmeta.RootInode: {}},
		nextInode: fsmeta.RootInode,
	}
}

func (f *fakeVolumeRootMeta) addChild(parent fsmeta.Ino, name string, inode fsmeta.Ino) {
	if f.children[parent] == nil {
		f.children[parent] = make(map[string]fsmeta.Ino)
	}
	f.children[parent][name] = inode
	if f.children[inode] == nil {
		f.children[inode] = make(map[string]fsmeta.Ino)
	}
	if inode > f.nextInode {
		f.nextInode = inode
	}
}

func (f *fakeVolumeRootMeta) Lookup(_ fsmeta.Context, parent fsmeta.Ino, name string, inode *fsmeta.Ino, attr *fsmeta.Attr, _ bool) syscall.Errno {
	children := f.children[parent]
	if len(children) == 0 {
		return syscall.ENOENT
	}
	next, ok := children[name]
	if !ok {
		return syscall.ENOENT
	}
	*inode = next
	if attr != nil {
		attr.Typ = fsmeta.TypeDirectory
	}
	return 0
}

func (f *fakeVolumeRootMeta) Mkdir(_ fsmeta.Context, parent fsmeta.Ino, name string, _ uint16, _ uint16, _ uint8, inode *fsmeta.Ino, attr *fsmeta.Attr) syscall.Errno {
	f.mkdirCalls++
	if f.mkdirErr != 0 {
		return f.mkdirErr
	}
	if f.children[parent] == nil {
		f.children[parent] = make(map[string]fsmeta.Ino)
	}
	if existing, ok := f.children[parent][name]; ok {
		*inode = existing
		return syscall.EEXIST
	}
	f.nextInode++
	f.addChild(parent, name, f.nextInode)
	*inode = f.nextInode
	if attr != nil {
		attr.Typ = fsmeta.TypeDirectory
	}
	return 0
}

func TestCleanupIdleDirectVolumeFileMounts_SkipsInflightRequests(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})
	volumeID := "vol-busy"
	sessionID := "direct-session-1"
	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID}
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		sessionID: {ID: sessionID, Scope: MountSessionScopeDirect, CreatedAt: time.Now()},
	}
	mgr.directMounts[volumeID] = &directMountLease{
		SessionID: sessionID,
		InFlight:  1,
		LastUsed:  time.Now().Add(-2 * time.Minute),
	}

	errs := mgr.CleanupIdleDirectVolumeFileMounts(context.Background(), time.Minute)
	if len(errs) != 0 {
		t.Fatalf("CleanupIdleDirectVolumeFileMounts() errors = %d", len(errs))
	}
	if _, ok := mgr.directMounts[volumeID]; !ok {
		t.Fatalf("direct lease for %s should remain while inflight requests exist", volumeID)
	}
	if _, ok := mgr.mountSessions[volumeID][sessionID]; !ok {
		t.Fatalf("direct session %s should remain mounted", sessionID)
	}
}

func TestCleanupIdleDirectVolumeFileMount_RemovesOnlyDirectSessionWhenSandboxSessionExists(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})
	registrar := &mockMountRegistrar{}
	mgr.SetMountRegistrar(registrar)

	volumeID := "vol-mixed"
	directSessionID := "direct-session-1"
	sandboxSessionID := "sandbox-session-1"

	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID}
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		directSessionID:  {ID: directSessionID, Scope: MountSessionScopeDirect, CreatedAt: time.Now()},
		sandboxSessionID: {ID: sandboxSessionID, Scope: MountSessionScopeSandbox, SandboxID: "sandbox-1", CreatedAt: time.Now()},
	}
	mgr.directMounts[volumeID] = &directMountLease{
		SessionID: directSessionID,
		InFlight:  0,
		LastUsed:  time.Now(),
	}

	cleaned, err := mgr.CleanupIdleDirectVolumeFileMount(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("CleanupIdleDirectVolumeFileMount() error = %v", err)
	}
	if !cleaned {
		t.Fatal("expected idle direct mount to be cleaned")
	}
	if _, ok := mgr.directMounts[volumeID]; ok {
		t.Fatalf("direct lease for %s should be removed", volumeID)
	}
	if _, ok := mgr.mountSessions[volumeID][directSessionID]; ok {
		t.Fatalf("direct session %s should be removed", directSessionID)
	}
	if _, ok := mgr.mountSessions[volumeID][sandboxSessionID]; !ok {
		t.Fatalf("sandbox session %s should remain", sandboxSessionID)
	}
	if _, ok := mgr.volumes[volumeID]; !ok {
		t.Fatalf("volume %s should remain mounted because sandbox session still exists", volumeID)
	}
	if len(registrar.unregistered) != 0 {
		t.Fatalf("unregister should not be called while sandbox session remains, got %+v", registrar.unregistered)
	}
}

func TestCleanupIdleDirectVolumeFileMount_SkipsInflightLease(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})
	volumeID := "vol-busy-single"
	sessionID := "direct-session-1"

	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID}
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		sessionID: {ID: sessionID, Scope: MountSessionScopeDirect, CreatedAt: time.Now()},
	}
	mgr.directMounts[volumeID] = &directMountLease{
		SessionID: sessionID,
		InFlight:  1,
		LastUsed:  time.Now(),
	}

	cleaned, err := mgr.CleanupIdleDirectVolumeFileMount(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("CleanupIdleDirectVolumeFileMount() error = %v", err)
	}
	if cleaned {
		t.Fatal("expected inflight direct mount to remain")
	}
	if _, ok := mgr.directMounts[volumeID]; !ok {
		t.Fatalf("direct lease for %s should remain", volumeID)
	}
}

func TestCleanupIdleDirectVolumeFileMount_AfterSandboxUnmountRemovesFinalSession(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})
	registrar := &mockMountRegistrar{}
	mgr.SetMountRegistrar(registrar)

	volumeID := "vol-final-direct"
	sandboxID := "sandbox-final"
	sandboxSessionID := "sandbox-session"
	directSessionID := "direct-session"

	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID}
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		sandboxSessionID: {ID: sandboxSessionID, SandboxID: sandboxID, Scope: MountSessionScopeSandbox},
		directSessionID:  {ID: directSessionID, Scope: MountSessionScopeDirect},
	}
	mgr.directMounts[volumeID] = &directMountLease{
		SessionID: directSessionID,
		InFlight:  0,
		LastUsed:  time.Now(),
	}
	mgr.TrackVolumeSession(sandboxID, volumeID, sandboxSessionID)

	errs := mgr.UnmountSandboxVolumes(context.Background(), sandboxID)
	if len(errs) != 0 {
		t.Fatalf("expected no errors during sandbox unmount, got %d", len(errs))
	}

	cleaned, err := mgr.CleanupIdleDirectVolumeFileMount(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("CleanupIdleDirectVolumeFileMount() error = %v", err)
	}
	if !cleaned {
		t.Fatal("expected final direct session to be cleaned")
	}
	if _, ok := mgr.volumes[volumeID]; ok {
		t.Fatalf("volume %s should be fully unmounted", volumeID)
	}
	if _, ok := mgr.mountSessions[volumeID]; ok {
		t.Fatalf("mount sessions for %s should be removed", volumeID)
	}
	if len(registrar.unregistered) != 1 || registrar.unregistered[0] != volumeID {
		t.Fatalf("expected unregister called once with %s, got %+v", volumeID, registrar.unregistered)
	}
}

func TestBeginInvalidate_IgnoresDirectSessions(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})
	volumeID := "vol-invalidate"
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		"sandbox-session": {ID: "sandbox-session", Scope: MountSessionScopeSandbox, CreatedAt: time.Now()},
		"direct-session":  {ID: "direct-session", Scope: MountSessionScopeDirect, CreatedAt: time.Now()},
		"legacy-session":  {ID: "legacy-session", CreatedAt: time.Now()},
	}

	pending, err := mgr.BeginInvalidate(volumeID, "invalidate-1")
	if err != nil {
		t.Fatalf("BeginInvalidate() error = %v", err)
	}
	if pending != 2 {
		t.Fatalf("pending = %d, want 2", pending)
	}

	tracker := mgr.invalidates[volumeID]["invalidate-1"]
	if tracker == nil {
		t.Fatal("expected invalidate tracker")
	}
	if _, ok := tracker.pending["direct-session"]; ok {
		t.Fatal("direct session should not require invalidate ack")
	}
	if _, ok := tracker.pending["sandbox-session"]; !ok {
		t.Fatal("sandbox session should require invalidate ack")
	}
	if _, ok := tracker.pending["legacy-session"]; !ok {
		t.Fatal("legacy unscoped session should still require invalidate ack")
	}
}

func TestAuthenticateMountSession(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})
	volumeID := "vol-auth"
	sessionID := "session-auth"

	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID, TeamID: "team-auth"}
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		sessionID: {
			ID:        sessionID,
			Secret:    "secret-auth",
			TeamID:    "team-auth",
			SandboxID: "sandbox-auth",
		},
	}

	principal, err := mgr.AuthenticateMountSession(volumeID, sessionID, "secret-auth")
	if err != nil {
		t.Fatalf("AuthenticateMountSession() error = %v", err)
	}
	if principal.TeamID != "team-auth" {
		t.Fatalf("team id = %q, want team-auth", principal.TeamID)
	}
	if principal.SandboxID != "sandbox-auth" {
		t.Fatalf("sandbox id = %q, want sandbox-auth", principal.SandboxID)
	}
}

func TestAuthenticateMountSessionRejectsInvalidSecret(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{})
	volumeID := "vol-auth"
	sessionID := "session-auth"

	mgr.volumes[volumeID] = &VolumeContext{VolumeID: volumeID, TeamID: "team-auth"}
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		sessionID: {
			ID:     sessionID,
			Secret: "secret-auth",
			TeamID: "team-auth",
		},
	}

	if _, err := mgr.AuthenticateMountSession(volumeID, sessionID, "wrong-secret"); err == nil {
		t.Fatal("AuthenticateMountSession() should reject wrong secret")
	}
}
