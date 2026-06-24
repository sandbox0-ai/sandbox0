package volume

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/sirupsen/logrus"
)

type fakeBackend struct {
	mountReq     BackendMountRequest
	mountCalls   int
	unmountCalls int
}

func (b *fakeBackend) MountVolume(_ context.Context, req BackendMountRequest) (*VolumeContext, error) {
	b.mountReq = req
	b.mountCalls++
	return &VolumeContext{
		VolumeID:  req.VolumeID,
		TeamID:    req.TeamID,
		Backend:   "fake",
		Access:    req.AccessMode,
		MountedAt: req.MountedAt,
		CacheDir:  "/tmp/fake-cache",
	}, nil
}

func (b *fakeBackend) UnmountVolume(_ context.Context, _ *VolumeContext) error {
	b.unmountCalls++
	return nil
}

func TestManagerMountUsesBackend(t *testing.T) {
	backend := &fakeBackend{}
	mgr := NewManagerWithBackend(logrus.New(), &config.StorageProxyConfig{}, nil, backend)

	sessionID, mountedAt, err := mgr.MountVolume(context.Background(), "team/team-a", "vol-1", "team-a", AccessModeRWO)
	if err != nil {
		t.Fatalf("MountVolume() error = %v", err)
	}
	if sessionID == "" {
		t.Fatal("MountVolume() returned empty session id")
	}
	if mountedAt.IsZero() {
		t.Fatal("MountVolume() returned zero mount time")
	}
	if backend.mountCalls != 1 {
		t.Fatalf("backend mount calls = %d, want 1", backend.mountCalls)
	}
	if backend.mountReq.S3Prefix != "team/team-a" {
		t.Fatalf("backend prefix = %q, want %q", backend.mountReq.S3Prefix, "team/team-a")
	}
	if backend.mountReq.VolumeID != "vol-1" || backend.mountReq.TeamID != "team-a" {
		t.Fatalf("backend mount request identity = %+v", backend.mountReq)
	}
	if backend.mountReq.AccessMode != AccessModeRWO {
		t.Fatalf("backend access mode = %q, want %q", backend.mountReq.AccessMode, AccessModeRWO)
	}
	if time.Since(backend.mountReq.MountedAt) > time.Minute {
		t.Fatalf("backend mount time = %s, expected recent timestamp", backend.mountReq.MountedAt)
	}

	volCtx, err := mgr.GetVolume("vol-1")
	if err != nil {
		t.Fatalf("GetVolume() error = %v", err)
	}
	if volCtx.Backend != "fake" {
		t.Fatalf("volume backend = %q, want fake", volCtx.Backend)
	}
}

func TestManagerUnmountUsesBackend(t *testing.T) {
	backend := &fakeBackend{}
	mgr := NewManagerWithBackend(logrus.New(), &config.StorageProxyConfig{}, nil, backend)

	sessionID, _, err := mgr.MountVolume(context.Background(), "", "vol-1", "team-a", AccessModeRWO)
	if err != nil {
		t.Fatalf("MountVolume() error = %v", err)
	}
	if err := mgr.UnmountVolume(context.Background(), "vol-1", sessionID); err != nil {
		t.Fatalf("UnmountVolume() error = %v", err)
	}
	if backend.unmountCalls != 1 {
		t.Fatalf("backend unmount calls = %d, want 1", backend.unmountCalls)
	}
	if _, err := mgr.GetVolume("vol-1"); err == nil {
		t.Fatal("GetVolume() after unmount returned nil error")
	}
}

func TestManagerMountUsesDefaultBackend(t *testing.T) {
	s0fsBackend := &fakeBackend{}
	mgr := NewManagerWithBackends(logrus.New(), &config.StorageProxyConfig{}, map[string]Backend{
		BackendS0FS: s0fsBackend,
	}, BackendS0FS)

	if _, _, err := mgr.MountVolume(context.Background(), "team/team-a", "vol-1", "team-a", AccessModeRWO); err != nil {
		t.Fatalf("MountVolume() error = %v", err)
	}
	if s0fsBackend.mountCalls != 1 {
		t.Fatalf("s0fs backend mount calls = %d, want 1", s0fsBackend.mountCalls)
	}
}

func TestS0FSSegmentTargetSizeDefaultsToFourMiB(t *testing.T) {
	got, err := S0FSSegmentTargetSize(&config.StorageProxyConfig{})
	if err != nil {
		t.Fatalf("S0FSSegmentTargetSize() error = %v", err)
	}
	if got != s0fs.DefaultSegmentTargetSizeBytes {
		t.Fatalf("segment target size = %d, want %d", got, s0fs.DefaultSegmentTargetSizeBytes)
	}
}

func TestS0FSSegmentTargetSizeParsesConfig(t *testing.T) {
	got, err := S0FSSegmentTargetSize(&config.StorageProxyConfig{S0FSSegmentTargetSize: "8Mi"})
	if err != nil {
		t.Fatalf("S0FSSegmentTargetSize() error = %v", err)
	}
	if want := uint64(8 << 20); got != want {
		t.Fatalf("segment target size = %d, want %d", got, want)
	}
}

func TestS0FSCompactionOptionsParseConfig(t *testing.T) {
	opts, err := S0FSCompactionOptions(&config.StorageProxyConfig{
		S0FSSegmentTargetSize:        "8Mi",
		S0FSCompactionMinDeadRatio:   "0.25",
		S0FSCompactionMinReclaimSize: "2Mi",
	})
	if err != nil {
		t.Fatalf("S0FSCompactionOptions() error = %v", err)
	}
	if opts.SegmentTargetSize != 8<<20 || opts.MinDeadRatio != 0.25 || opts.MinReclaimBytes != 2<<20 {
		t.Fatalf("unexpected compaction options: %+v", opts)
	}
}

func TestS0FSBackgroundCompactionEnabledOnlyForRWO(t *testing.T) {
	for _, tc := range []struct {
		access AccessMode
		want   bool
	}{
		{access: AccessModeRWO, want: true},
		{access: AccessModeROX, want: false},
		{access: AccessModeRWX, want: false},
		{access: "", want: true},
	} {
		if got := S0FSBackgroundCompactionEnabled(tc.access); got != tc.want {
			t.Fatalf("S0FSBackgroundCompactionEnabled(%q) = %v, want %v", tc.access, got, tc.want)
		}
	}
}

func TestVolumeContextReleaseFileHandleTracksUnlinkedInodes(t *testing.T) {
	volCtx := &VolumeContext{}
	first := volCtx.OpenFileHandle(10)
	second := volCtx.OpenFileHandle(10)
	if !volCtx.MarkUnlinkedFileIfOpen(10) {
		t.Fatal("MarkUnlinkedFileIfOpen() = false, want true")
	}

	inode, remaining, unlinked, ok := volCtx.ReleaseFileHandle(first)
	if !ok || inode != 10 || remaining != 1 || !unlinked {
		t.Fatalf("first ReleaseFileHandle() = inode %d remaining %d unlinked %v ok %v", inode, remaining, unlinked, ok)
	}
	if got := volCtx.FileOpenCount(10); got != 1 {
		t.Fatalf("FileOpenCount() after first release = %d, want 1", got)
	}

	inode, remaining, unlinked, ok = volCtx.ReleaseFileHandle(second)
	if !ok || inode != 10 || remaining != 0 || !unlinked {
		t.Fatalf("second ReleaseFileHandle() = inode %d remaining %d unlinked %v ok %v", inode, remaining, unlinked, ok)
	}
	if _, _, unlinked, ok = volCtx.ReleaseFileHandle(second); ok || unlinked {
		t.Fatalf("duplicate ReleaseFileHandle() = unlinked %v ok %v, want false false", unlinked, ok)
	}
}

func TestVolumeContextMarkUnlinkedFileIfOpenReturnsFalseForClosedInode(t *testing.T) {
	volCtx := &VolumeContext{}
	if volCtx.MarkUnlinkedFileIfOpen(10) {
		t.Fatal("MarkUnlinkedFileIfOpen() = true, want false")
	}
	handle := volCtx.OpenFileHandle(10)
	if _, _, _, ok := volCtx.ReleaseFileHandle(handle); !ok {
		t.Fatal("ReleaseFileHandle() ok = false, want true")
	}
	if volCtx.MarkUnlinkedFileIfOpen(10) {
		t.Fatal("MarkUnlinkedFileIfOpen() after release = true, want false")
	}
}
