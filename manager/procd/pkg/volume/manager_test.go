package volume

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"go.uber.org/zap"
)

func TestValidateMountPoint(t *testing.T) {
	manager := NewManager(&Config{}, nil, nil)

	cases := []struct {
		name      string
		path      string
		expectErr bool
	}{
		{name: "empty", path: "", expectErr: true},
		{name: "relative", path: "tmp/volume", expectErr: true},
		{name: "root", path: "/", expectErr: true},
		{name: "parent", path: "/tmp/../volume", expectErr: false},
		{name: "valid", path: "/tmp/volume", expectErr: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := manager.validateMountPoint(tc.path)
			if tc.expectErr && err == nil {
				t.Fatalf("expected error")
			}
			if !tc.expectErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestMergeVolumeConfig(t *testing.T) {
	manager := NewManager(&Config{
		JuiceFSCacheSize:  "200",
		JuiceFSPrefetch:   3,
		JuiceFSBufferSize: "300",
		JuiceFSWriteback:  true,
	}, nil, nil)

	overridePrefetch := int32(10)
	overrideWriteback := false
	cfg := manager.mergeVolumeConfig(&VolumeConfig{
		CacheSize: "500",
		Prefetch:  &overridePrefetch,
		Writeback: &overrideWriteback,
	})

	if cfg.CacheSize != "500" {
		t.Fatalf("expected cache size override, got %q", cfg.CacheSize)
	}
	if cfg.Prefetch != 10 {
		t.Fatalf("expected prefetch override, got %d", cfg.Prefetch)
	}
	if cfg.BufferSize != "300" {
		t.Fatalf("expected buffer size default, got %q", cfg.BufferSize)
	}
	if cfg.Writeback != false {
		t.Fatalf("expected writeback override")
	}
}

func TestMergeVolumeConfigNilDefaults(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	cfg := manager.mergeVolumeConfig(nil)
	if cfg == nil {
		t.Fatalf("expected config")
	}
	if cfg.CacheSize != "" || cfg.BufferSize != "" || cfg.Prefetch != 0 || cfg.Writeback {
		t.Fatalf("expected zero defaults, got %+v", cfg)
	}
}

func TestJoinMountPath(t *testing.T) {
	if joinMountPath("/mnt", "/dir/file.txt") != "/mnt/dir/file.txt" {
		t.Fatalf("unexpected join result")
	}
	if joinMountPath("/mnt", "dir/file.txt") != "/mnt/dir/file.txt" {
		t.Fatalf("unexpected join result for relative path")
	}
	if joinMountPath("/mnt", "") != "" {
		t.Fatalf("expected empty path")
	}
}

func TestStatusDuration(t *testing.T) {
	manager := NewManager(&Config{}, nil, nil)
	now := time.Now()
	manager.mounts["vol-1"] = &mountInfo{
		volumeID:   "vol-1",
		mountPoint: "/mnt/vol-1",
		mountedAt:  now.Add(-2 * time.Second),
	}
	status := manager.GetStatus()
	if len(status) != 1 {
		t.Fatalf("expected status entry")
	}
	if status[0].MountedDurationSecs < 1 {
		t.Fatalf("expected mounted duration")
	}
}

func TestBootstrapMountsRejectsDuplicateMountPoint(t *testing.T) {
	manager := NewManager(&Config{}, nil, nil)
	_, err := manager.BootstrapMounts(context.Background(), []MountRequest{
		{SandboxVolumeID: "vol-1", MountPoint: "/tmp/one"},
		{SandboxVolumeID: "vol-2", MountPoint: "/tmp/one"},
	}, false, 0)
	if err != ErrMountPointInUse {
		t.Fatalf("BootstrapMounts() error = %v, want %v", err, ErrMountPointInUse)
	}
}

func TestBootstrapMountsWaitReturnsFailedStatus(t *testing.T) {
	manager := NewManager(&Config{}, staticTokenProvider{}, nil)
	status, err := manager.BootstrapMounts(context.Background(), []MountRequest{{
		SandboxVolumeID: "vol-1",
		SandboxID:       "sandbox-1",
		MountPoint:      t.TempDir(),
	}}, true, 2*time.Second)
	if err != nil {
		t.Fatalf("BootstrapMounts() error = %v", err)
	}
	if len(status) != 1 {
		t.Fatalf("BootstrapMounts() returned %d statuses, want 1", len(status))
	}
	if status[0].State != MountStateFailed {
		t.Fatalf("status state = %q, want %q", status[0].State, MountStateFailed)
	}
	if status[0].ErrorCode != "mount_failed" {
		t.Fatalf("error code = %q, want %q", status[0].ErrorCode, "mount_failed")
	}
	all := manager.GetStatus()
	if len(all) != 1 || all[0].State != MountStateFailed {
		t.Fatalf("GetStatus() = %+v, want failed bootstrap mount", all)
	}
}

func TestMountUsesNodeLocalBackendWhenConfigured(t *testing.T) {
	client := &fakeCtldVolumeClient{
		attachResp: &ctldapi.VolumeAttachResponse{
			Attached:       true,
			AttachmentID:   "attach-1",
			MountSessionID: "session-1",
		},
	}
	manager := NewManager(&Config{MountMode: MountModeNodeLocal}, staticTokenProvider{}, zap.NewNop())
	manager.SetCtldVolumeClient(client)

	resp, err := manager.Mount(context.Background(), &MountRequest{
		SandboxID:       "sandbox-1",
		SandboxVolumeID: "vol-1",
		MountPoint:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Mount() error = %v", err)
	}
	if resp.Backend != MountBackendNodeLocal {
		t.Fatalf("Mount() backend = %q, want %q", resp.Backend, MountBackendNodeLocal)
	}
	if client.attachReq == nil || client.attachReq.SandboxID != "sandbox-1" || client.attachReq.SandboxVolumeID != "vol-1" {
		t.Fatalf("attach request = %+v", client.attachReq)
	}

	status := manager.GetStatus()
	if len(status) != 1 || status[0].Backend != MountBackendNodeLocal || status[0].State != MountStateMounted {
		t.Fatalf("status = %+v", status)
	}

	if err := manager.Unmount(context.Background(), "vol-1", "session-1"); err != nil {
		t.Fatalf("Unmount() error = %v", err)
	}
	if client.detachReq == nil || client.detachReq.AttachmentID != "attach-1" || client.detachReq.MountSessionID != "session-1" {
		t.Fatalf("detach request = %+v", client.detachReq)
	}
}

func TestMountNodeLocalFailureFallsBackToStorageProxyWhenEnabled(t *testing.T) {
	client := &fakeCtldVolumeClient{attachErr: errors.New("ctld unavailable")}
	manager := NewManager(&Config{
		MountMode:                  MountModeNodeLocal,
		NodeLocalFallbackToStorage: true,
	}, staticTokenProvider{}, zap.NewNop())
	manager.SetCtldVolumeClient(client)

	storageProxyMounted := false
	manager.mountStorageProxyHook = func(_ context.Context, req *MountRequest, mountPoint string) (*mountInfo, error) {
		storageProxyMounted = true
		return &mountInfo{
			volumeID:       req.SandboxVolumeID,
			mountPoint:     mountPoint,
			sandboxID:      req.SandboxID,
			mountedAt:      time.Now(),
			mountSessionID: "session-fallback",
			mountSecret:    "secret-fallback",
			backend:        MountBackendStorageProxyFuse,
			watchDone:      make(chan struct{}),
		}, nil
	}

	resp, err := manager.Mount(context.Background(), &MountRequest{
		SandboxID:       "sandbox-1",
		SandboxVolumeID: "vol-1",
		MountPoint:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Mount() error = %v", err)
	}
	if !storageProxyMounted {
		t.Fatalf("expected storage-proxy fallback to run")
	}
	if resp.Backend != MountBackendStorageProxyFuse {
		t.Fatalf("Mount() backend = %q, want %q", resp.Backend, MountBackendStorageProxyFuse)
	}
}

func TestMountNodeLocalFailureDoesNotFallbackByDefault(t *testing.T) {
	client := &fakeCtldVolumeClient{attachErr: errors.New("ctld unavailable")}
	manager := NewManager(&Config{MountMode: MountModeNodeLocal}, staticTokenProvider{}, zap.NewNop())
	manager.SetCtldVolumeClient(client)
	manager.mountStorageProxyHook = func(context.Context, *MountRequest, string) (*mountInfo, error) {
		t.Fatalf("storage-proxy fallback should not run")
		return nil, nil
	}

	_, err := manager.Mount(context.Background(), &MountRequest{
		SandboxID:       "sandbox-1",
		SandboxVolumeID: "vol-1",
		MountPoint:      t.TempDir(),
	})
	if err == nil {
		t.Fatalf("Mount() expected node-local error")
	}
}

type fakeCtldVolumeClient struct {
	attachReq  *ctldapi.VolumeAttachRequest
	attachResp *ctldapi.VolumeAttachResponse
	attachErr  error
	detachReq  *ctldapi.VolumeDetachRequest
	detachErr  error
}

func (f *fakeCtldVolumeClient) Attach(_ context.Context, req *ctldapi.VolumeAttachRequest) (*ctldapi.VolumeAttachResponse, error) {
	f.attachReq = req
	if f.attachErr != nil {
		return nil, f.attachErr
	}
	if f.attachResp != nil {
		return f.attachResp, nil
	}
	return &ctldapi.VolumeAttachResponse{Attached: true, AttachmentID: "attach-1", MountSessionID: "session-1"}, nil
}

func (f *fakeCtldVolumeClient) Detach(_ context.Context, req *ctldapi.VolumeDetachRequest) error {
	f.detachReq = req
	return f.detachErr
}
