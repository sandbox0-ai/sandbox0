package volume

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sirupsen/logrus"
)

func TestAcquireDirectVolumeFileMount_ReusesSessionUntilIdleCleanup(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{}, nil)
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
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{}, nil)
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

func TestCleanupIdleDirectVolumeFileMount_SkipsInflightLease(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{}, nil)
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

func TestBeginInvalidate_IgnoresDirectSessions(t *testing.T) {
	mgr := NewManager(logrus.New(), &config.StorageProxyConfig{}, nil)
	volumeID := "vol-invalidate"
	mgr.mountSessions[volumeID] = map[string]*MountSession{
		"remote-session": {ID: "remote-session", CreatedAt: time.Now()},
		"direct-session": {ID: "direct-session", Scope: MountSessionScopeDirect, CreatedAt: time.Now()},
	}

	pending, err := mgr.BeginInvalidate(volumeID, "invalidate-1")
	if err != nil {
		t.Fatalf("BeginInvalidate() error = %v", err)
	}
	if pending != 1 {
		t.Fatalf("pending = %d, want 1", pending)
	}

	tracker := mgr.invalidates[volumeID]["invalidate-1"]
	if tracker == nil {
		t.Fatal("expected invalidate tracker")
	}
	if _, ok := tracker.pending["direct-session"]; ok {
		t.Fatal("direct session should not require invalidate ack")
	}
	if _, ok := tracker.pending["remote-session"]; !ok {
		t.Fatal("remote session should require invalidate ack")
	}
}
