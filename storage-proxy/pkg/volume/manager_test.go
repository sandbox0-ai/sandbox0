package volume

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
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
