package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/notify"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volsync"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestMountVolumeRejectsCrossTeamAccess(t *testing.T) {
	t.Parallel()

	volMgr := &fakeVolumeManager{}
	server := newTestFileSystemServer(volMgr, &fakeVolumeRepo{
		volumes: map[string]*db.SandboxVolume{
			"vol-1": {
				ID:         "vol-1",
				TeamID:     "team-a",
				AccessMode: string(volume.AccessModeRWO),
			},
		},
	}, nil)

	_, err := server.MountVolume(authContext("team-b", "sandbox-1"), &pb.MountVolumeRequest{
		VolumeId: "vol-1",
	})
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("MountVolume() code = %v, want %v (err=%v)", got, codes.PermissionDenied, err)
	}
	if volMgr.mountCalls != 0 {
		t.Fatalf("MountVolume() should not reach volume manager, got %d calls", volMgr.mountCalls)
	}
}

func TestGetAttrRejectsCrossTeamMountedVolume(t *testing.T) {
	t.Parallel()

	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": {VolumeID: "vol-1", TeamID: "team-a"},
		},
	}, nil, nil)

	_, err := server.GetAttr(authContext("team-b", ""), &pb.GetAttrRequest{
		VolumeId: "vol-1",
		Inode:    1,
	})
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("GetAttr() code = %v, want %v (err=%v)", got, codes.PermissionDenied, err)
	}
}

func TestUnmountVolumeRejectsCrossTeamMountedVolume(t *testing.T) {
	t.Parallel()

	volMgr := &fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": {VolumeID: "vol-1", TeamID: "team-a"},
		},
	}
	server := newTestFileSystemServer(volMgr, nil, nil)

	_, err := server.UnmountVolume(authContext("team-b", ""), &pb.UnmountVolumeRequest{
		VolumeId:       "vol-1",
		MountSessionId: "session-1",
	})
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("UnmountVolume() code = %v, want %v (err=%v)", got, codes.PermissionDenied, err)
	}
	if volMgr.unmountCalls != 0 {
		t.Fatalf("UnmountVolume() should not reach volume manager, got %d calls", volMgr.unmountCalls)
	}
}

func TestAckInvalidateRejectsCrossTeamMountedVolume(t *testing.T) {
	t.Parallel()

	volMgr := &fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": {VolumeID: "vol-1", TeamID: "team-a"},
		},
	}
	server := newTestFileSystemServer(volMgr, nil, nil)

	_, err := server.AckInvalidate(authContext("team-b", ""), &pb.AckInvalidateRequest{
		VolumeId:       "vol-1",
		MountSessionId: "session-1",
		InvalidateId:   "invalidate-1",
		Success:        true,
	})
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("AckInvalidate() code = %v, want %v (err=%v)", got, codes.PermissionDenied, err)
	}
	if volMgr.ackCalls != 0 {
		t.Fatalf("AckInvalidate() should not reach volume manager, got %d calls", volMgr.ackCalls)
	}
}

func TestWatchVolumeEventsRejectsCrossTeamMountedVolume(t *testing.T) {
	t.Parallel()

	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": {VolumeID: "vol-1", TeamID: "team-a"},
		},
	}, nil, notify.NewHub(logrus.New(), 1))

	err := server.WatchVolumeEvents(&pb.WatchRequest{VolumeId: "vol-1"}, &fakeWatchVolumeEventsServer{
		ctx: authContext("team-b", ""),
	})
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("WatchVolumeEvents() code = %v, want %v (err=%v)", got, codes.PermissionDenied, err)
	}
}

func TestMountVolumeTracksAuthorizedTeam(t *testing.T) {
	t.Parallel()

	volMgr := &fakeVolumeManager{
		mountSessionID: "session-1",
		mountedAt:      time.Unix(1700000000, 0),
	}
	server := newTestFileSystemServer(volMgr, &fakeVolumeRepo{
		volumes: map[string]*db.SandboxVolume{
			"vol-1": {
				ID:         "vol-1",
				TeamID:     "team-a",
				AccessMode: string(volume.AccessModeRWX),
			},
		},
	}, nil)

	resp, err := server.MountVolume(authContext("team-a", "sandbox-1"), &pb.MountVolumeRequest{
		VolumeId: "vol-1",
		Config: &pb.VolumeConfig{
			CacheSize:  "2Gi",
			Prefetch:   4,
			BufferSize: "8Mi",
			Writeback:  true,
		},
	})
	if err != nil {
		t.Fatalf("MountVolume() unexpected error: %v", err)
	}
	if resp.MountSessionId != "session-1" {
		t.Fatalf("MountVolume() session = %q, want %q", resp.MountSessionId, "session-1")
	}
	if volMgr.mountCalls != 1 {
		t.Fatalf("MountVolume() calls = %d, want 1", volMgr.mountCalls)
	}
	if volMgr.lastMount.teamID != "team-a" {
		t.Fatalf("MountVolume() teamID = %q, want %q", volMgr.lastMount.teamID, "team-a")
	}
	wantPrefix, err := naming.S3VolumePrefix("team-a", "vol-1")
	if err != nil {
		t.Fatalf("S3VolumePrefix() unexpected error: %v", err)
	}
	if volMgr.lastMount.s3Prefix != wantPrefix {
		t.Fatalf("MountVolume() prefix = %q, want %q", volMgr.lastMount.s3Prefix, wantPrefix)
	}
	if volMgr.lastMount.accessMode != volume.AccessModeRWX {
		t.Fatalf("MountVolume() accessMode = %q, want %q", volMgr.lastMount.accessMode, volume.AccessModeRWX)
	}
	if volMgr.lastMount.config == nil || volMgr.lastMount.config.CacheSize != "2Gi" || volMgr.lastMount.config.BufferSize != "8Mi" || volMgr.lastMount.config.Prefetch != 4 || !volMgr.lastMount.config.Writeback {
		t.Fatalf("MountVolume() config not forwarded correctly: %+v", volMgr.lastMount.config)
	}
	if volMgr.trackedSandboxID != "sandbox-1" || volMgr.trackedVolumeID != "vol-1" {
		t.Fatalf("TrackVolume() got (%q, %q), want (%q, %q)", volMgr.trackedSandboxID, volMgr.trackedVolumeID, "sandbox-1", "vol-1")
	}
}

func authContext(teamID, sandboxID string) context.Context {
	return internalauth.WithClaims(context.Background(), &internalauth.Claims{
		TeamID:    teamID,
		SandboxID: sandboxID,
	})
}

func newTestFileSystemServer(volMgr volumeManager, repo VolumeRepository, hub *notify.Hub) *FileSystemServer {
	return NewFileSystemServer(volMgr, repo, hub, nil, logrus.New(), nil, nil)
}

type fakeMutationBarrier struct {
	calls        int
	lastVolumeID string
}

func (f *fakeMutationBarrier) WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	f.calls++
	f.lastVolumeID = volumeID
	return fn(ctx)
}

func TestWithAuthorizedVolumeMutationUsesBarrier(t *testing.T) {
	t.Parallel()

	barrier := &fakeMutationBarrier{}
	server := &FileSystemServer{
		volMgr: &fakeVolumeManager{
			volumes: map[string]*volume.VolumeContext{
				"vol-1": {VolumeID: "vol-1", TeamID: "team-a"},
			},
		},
		mutationBarrier: barrier,
		logger:          logrus.New(),
	}

	resp, err := withAuthorizedVolumeMutation(server, authContext("team-a", ""), "vol-1", func(ctx context.Context, volCtx *volume.VolumeContext) (*pb.Empty, error) {
		if volCtx.VolumeID != "vol-1" {
			t.Fatalf("volume id = %q, want vol-1", volCtx.VolumeID)
		}
		return &pb.Empty{}, nil
	})
	if err != nil {
		t.Fatalf("withAuthorizedVolumeMutation() error = %v", err)
	}
	if resp == nil {
		t.Fatal("expected response")
	}
	if barrier.calls != 1 || barrier.lastVolumeID != "vol-1" {
		t.Fatalf("barrier = %+v, want one shared call for vol-1", barrier)
	}
}

type fakeSyncRecorder struct {
	lastValidate *volsync.NamespaceMutationRequest
	validateErr  error
}

func (f *fakeSyncRecorder) RecordRemoteChange(ctx context.Context, change *volsync.RemoteChange) error {
	return nil
}

func (f *fakeSyncRecorder) ValidateNamespaceMutation(ctx context.Context, req *volsync.NamespaceMutationRequest) error {
	f.lastValidate = req
	return f.validateErr
}

func TestValidateNamespaceMutationMapsCompatibilityErrorsToFailedPrecondition(t *testing.T) {
	t.Parallel()

	recorder := &fakeSyncRecorder{
		validateErr: &volsync.NamespaceCompatibilityError{
			Capabilities: pathnorm.FilesystemCapabilities{WindowsCompatiblePaths: true},
			Issues: []pathnorm.CompatibilityIssue{{
				Code: pathnorm.IssueCodeWindowsReservedName,
				Path: "/app/CON.txt",
			}},
		},
	}
	server := &FileSystemServer{
		syncRecorder: recorder,
		logger:       logrus.New(),
	}

	err := server.validateNamespaceMutation(authContext("team-a", "sandbox-1"), buildNamespaceMutationRequest(authContext("team-a", "sandbox-1"), "vol-1", db.SyncEventCreate, "/app/CON.txt", ""))
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("validateNamespaceMutation() code = %v, want %v (err=%v)", got, codes.FailedPrecondition, err)
	}
	if recorder.lastValidate == nil || recorder.lastValidate.Path != "/app/CON.txt" {
		t.Fatalf("lastValidate = %+v, want path /app/CON.txt", recorder.lastValidate)
	}
}

type fakeVolumeRepo struct {
	volumes map[string]*db.SandboxVolume
	err     error
}

func (r *fakeVolumeRepo) GetSandboxVolume(_ context.Context, id string) (*db.SandboxVolume, error) {
	if r.err != nil {
		return nil, r.err
	}
	if vol, ok := r.volumes[id]; ok {
		return vol, nil
	}
	return nil, db.ErrNotFound
}

type fakeVolumeManager struct {
	volumes        map[string]*volume.VolumeContext
	mountCalls     int
	unmountCalls   int
	ackCalls       int
	mountSessionID string
	mountedAt      time.Time
	lastMount      struct {
		s3Prefix   string
		volumeID   string
		teamID     string
		config     *volume.VolumeConfig
		accessMode volume.AccessMode
	}
	trackedSandboxID string
	trackedVolumeID  string
}

func (m *fakeVolumeManager) MountVolume(_ context.Context, s3Prefix, volumeID, teamID string, config *volume.VolumeConfig, accessMode volume.AccessMode) (string, time.Time, error) {
	m.mountCalls++
	m.lastMount.s3Prefix = s3Prefix
	m.lastMount.volumeID = volumeID
	m.lastMount.teamID = teamID
	m.lastMount.config = config
	m.lastMount.accessMode = accessMode
	sessionID := m.mountSessionID
	if sessionID == "" {
		sessionID = "session-test"
	}
	mountedAt := m.mountedAt
	if mountedAt.IsZero() {
		mountedAt = time.Unix(1700000000, 0)
	}
	return sessionID, mountedAt, nil
}

func (m *fakeVolumeManager) UnmountVolume(_ context.Context, _, _ string) error {
	m.unmountCalls++
	return nil
}

func (m *fakeVolumeManager) AckInvalidate(_, _, _ string, _ bool, _ string) error {
	m.ackCalls++
	return nil
}

func (m *fakeVolumeManager) GetVolume(volumeID string) (*volume.VolumeContext, error) {
	if vol, ok := m.volumes[volumeID]; ok {
		return vol, nil
	}
	return nil, status.Error(codes.NotFound, "volume not mounted")
}

func (m *fakeVolumeManager) TrackVolume(sandboxID, volumeID string) {
	m.trackedSandboxID = sandboxID
	m.trackedVolumeID = volumeID
}

type fakeWatchVolumeEventsServer struct {
	ctx context.Context
}

var _ pb.FileSystem_WatchVolumeEventsServer = (*fakeWatchVolumeEventsServer)(nil)

func (s *fakeWatchVolumeEventsServer) SetHeader(metadata.MD) error { return nil }

func (s *fakeWatchVolumeEventsServer) SendHeader(metadata.MD) error { return nil }

func (s *fakeWatchVolumeEventsServer) SetTrailer(metadata.MD) {}

func (s *fakeWatchVolumeEventsServer) Context() context.Context { return s.ctx }

func (s *fakeWatchVolumeEventsServer) Send(event *pb.WatchEvent) error { return nil }

func (s *fakeWatchVolumeEventsServer) SendMsg(any) error { return nil }

func (s *fakeWatchVolumeEventsServer) RecvMsg(any) error { return nil }
