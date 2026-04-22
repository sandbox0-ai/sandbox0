package portal

import (
	"context"
	"testing"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

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

func TestNewManagerDefaultsClusterID(t *testing.T) {
	mgr := NewManager(Config{
		StorageConfig: &apiconfig.StorageProxyConfig{},
	})
	if mgr.clusterID != naming.DefaultClusterID {
		t.Fatalf("clusterID = %q, want %q", mgr.clusterID, naming.DefaultClusterID)
	}
}
