package volume

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/infra/infra-operator/api/config"
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
	mgr.TrackVolume(sandboxID, volumeID)

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
