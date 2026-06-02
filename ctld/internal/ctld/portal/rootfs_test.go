package portal

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

type recordingFuseMount struct {
	unmounts int
	err      error
}

func (m *recordingFuseMount) Unmount() error {
	m.unmounts++
	return m.err
}

func TestReleaseRootFSRemovesMountedState(t *testing.T) {
	server := &recordingFuseMount{}
	mgr := &Manager{
		rootfs:       make(map[string]*rootfsMount),
		boundVolumes: make(map[string]*boundVolume),
		volumes:      newLocalVolumeManager(),
	}
	volCtx := &volume.VolumeContext{
		VolumeID:  "rootfs-vol",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
	}
	mgr.volumes.add(volCtx)
	mgr.boundVolumes["rootfs-vol"] = &boundVolume{
		volumeID: "rootfs-vol",
		teamID:   "team-a",
		access:   volume.AccessModeRWO,
		refCount: 1,
		volCtx:   volCtx,
	}
	mgr.rootfs["sandbox-a"] = &rootfsMount{
		sandboxID: "sandbox-a",
		volumeID:  "rootfs-vol",
		teamID:    "team-a",
		server:    server,
	}

	resp, err := mgr.ReleaseRootFS(context.Background(), ctldapi.ReleaseRootFSRequest{SandboxID: "sandbox-a"})
	if err != nil {
		t.Fatalf("ReleaseRootFS() error = %v", err)
	}
	if !resp.Released {
		t.Fatalf("ReleaseRootFS() response = %+v, want released", resp)
	}
	if server.unmounts != 1 {
		t.Fatalf("server unmounts = %d, want 1", server.unmounts)
	}
	if _, ok := mgr.rootfs["sandbox-a"]; ok {
		t.Fatal("rootfs entry still present after release")
	}
	if _, ok := mgr.boundVolumes["rootfs-vol"]; ok {
		t.Fatal("bound volume still present after rootfs release")
	}
	if _, err := mgr.volumes.GetVolume("rootfs-vol"); err == nil {
		t.Fatal("GetVolume() after rootfs release error = nil, want volume removed")
	}
}
