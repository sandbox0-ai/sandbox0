package grpc

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
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
	if resp.MountSessionSecret == "" {
		t.Fatal("MountVolume() secret should not be empty")
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
	if volMgr.trackedSessionID != "session-1" {
		t.Fatalf("TrackVolumeSession() session = %q, want %q", volMgr.trackedSessionID, "session-1")
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
	lastValidate  *volsync.NamespaceMutationRequest
	remoteChanges []*volsync.RemoteChange
	validateErr   error
	recorded      chan *volsync.RemoteChange
}

func (f *fakeSyncRecorder) RecordRemoteChange(ctx context.Context, change *volsync.RemoteChange) error {
	if change != nil {
		clone := *change
		f.remoteChanges = append(f.remoteChanges, &clone)
		if f.recorded != nil {
			f.recorded <- &clone
		}
	}
	return nil
}

func (f *fakeSyncRecorder) ValidateNamespaceMutation(ctx context.Context, req *volsync.NamespaceMutationRequest) error {
	f.lastValidate = req
	return f.validateErr
}

func TestPublishEventDefaultsTimestampBeforeRecordingRemoteChange(t *testing.T) {
	t.Parallel()

	recorder := &fakeSyncRecorder{}
	fixedNow := time.Date(2026, 3, 26, 12, 34, 56, 0, time.UTC)
	server := &FileSystemServer{
		syncRecorder: recorder,
		logger:       logrus.New(),
		now:          func() time.Time { return fixedNow },
	}
	server.publishEvent(authContext("team-a", "sandbox-1"), &pb.WatchEvent{
		VolumeId:  "vol-1",
		EventType: pb.WatchEventType_WATCH_EVENT_TYPE_WRITE,
		Path:      "/hello.txt",
	})

	if len(recorder.remoteChanges) != 1 {
		t.Fatalf("remoteChanges = %d, want 1", len(recorder.remoteChanges))
	}
	got := recorder.remoteChanges[0]
	if got.OccurredAt.Unix() <= 0 {
		t.Fatalf("OccurredAt = %v, want unix timestamp > 0", got.OccurredAt)
	}
	if !got.OccurredAt.Equal(fixedNow) {
		t.Fatalf("OccurredAt = %v, want %v from injected clock", got.OccurredAt, fixedNow)
	}
}

func TestRecordRemoteSyncChangeCanRecordAsynchronously(t *testing.T) {
	t.Parallel()

	recorder := &fakeSyncRecorder{recorded: make(chan *volsync.RemoteChange, 1)}
	server := &FileSystemServer{
		syncRecorder:    recorder,
		logger:          logrus.New(),
		asyncSyncRecord: true,
	}
	ctx, cancel := context.WithCancel(authContext("team-a", "sandbox-1"))
	recordCtx := server.recordRemoteSyncChange(ctx, &volsync.RemoteChange{
		VolumeID:  "vol-1",
		EventType: db.SyncEventWrite,
		Path:      "/hello.txt",
	})
	cancel()

	if !shouldSkipSyncRecord(recordCtx) {
		t.Fatal("expected returned context to suppress duplicate sync recording")
	}
	select {
	case got := <-recorder.recorded:
		if got.TeamID != "team-a" || got.SandboxID != "sandbox-1" || got.Path != "/hello.txt" {
			t.Fatalf("recorded change = %+v, want team/sandbox/path populated", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async sync record")
	}
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

func TestCreatePropagatesNamespaceValidationAndRecordsCreateOnRelease(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	recorder := &fakeSyncRecorder{}
	server := NewFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil, nil, logrus.New(), recorder, nil)
	ctx := authContext("team-a", "sandbox-1")

	resp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "hello.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if resp == nil {
		t.Fatal("expected response")
	}
	if recorder.lastValidate == nil {
		t.Fatal("expected namespace validation request")
	}
	if recorder.lastValidate.EventType != db.SyncEventCreate || recorder.lastValidate.Path != "/hello.txt" {
		t.Fatalf("lastValidate = %+v, want create /hello.txt", recorder.lastValidate)
	}
	if len(recorder.remoteChanges) != 0 {
		t.Fatalf("remoteChanges after Create = %d, want 0", len(recorder.remoteChanges))
	}

	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    resp.Inode,
		HandleId: resp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if len(recorder.remoteChanges) != 1 {
		t.Fatalf("remoteChanges after Release = %d, want 1", len(recorder.remoteChanges))
	}
	if got := recorder.remoteChanges[0]; got.EventType != db.SyncEventCreate || got.Path != "/hello.txt" || got.SandboxID != "sandbox-1" {
		t.Fatalf("remoteChanges[0] = %+v, want create event for /hello.txt", got)
	}

	var inode meta.Ino
	var attr meta.Attr
	if st := volCtx.Meta.Lookup(meta.Background(), meta.RootInode, "hello.txt", &inode, &attr, false); st != 0 {
		t.Fatalf("Lookup(hello.txt) errno = %v, want 0", st)
	}
}

func TestCreateUsesCachedParentPathFromMkdir(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	recorder := &fakeSyncRecorder{}
	server := NewFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil, nil, logrus.New(), recorder, nil)
	ctx := authContext("team-a", "sandbox-1")

	dirResp, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "cached",
		Mode:     0o755,
	})
	if err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	if cached, ok := server.lookupCachedInodePath("vol-1", dirResp.Inode); !ok || cached != "/cached" {
		t.Fatalf("cached dir path = %q, %v; want /cached, true", cached, ok)
	}

	_, err = server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   dirResp.Inode,
		Name:     "hello.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if recorder.lastValidate == nil || recorder.lastValidate.Path != "/cached/hello.txt" {
		t.Fatalf("lastValidate = %+v, want /cached/hello.txt", recorder.lastValidate)
	}
}

func TestCreatePendingCreateFlushesBeforeChmod(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	recorder := &fakeSyncRecorder{}
	server := NewFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil, nil, logrus.New(), recorder, nil)
	ctx := authContext("team-a", "sandbox-1")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "hello.txt",
		Mode:     0o644,
		Flags:    uint32(syscall.O_RDWR),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if len(recorder.remoteChanges) != 0 {
		t.Fatalf("remoteChanges after Create = %d, want 0", len(recorder.remoteChanges))
	}

	if _, err := server.SetAttr(ctx, &pb.SetAttrRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Valid:    uint32(meta.SetAttrMode),
		Attr: &pb.GetAttrResponse{
			Mode: 0o600,
		},
	}); err != nil {
		t.Fatalf("SetAttr() error = %v", err)
	}
	if len(recorder.remoteChanges) != 2 {
		t.Fatalf("remoteChanges after chmod = %d, want 2", len(recorder.remoteChanges))
	}
	if got := recorder.remoteChanges[0]; got.EventType != db.SyncEventCreate || got.Path != "/hello.txt" {
		t.Fatalf("remoteChanges[0] = %+v, want create /hello.txt", got)
	}
	if got := recorder.remoteChanges[1]; got.EventType != db.SyncEventChmod || got.Path != "/hello.txt" {
		t.Fatalf("remoteChanges[1] = %+v, want chmod /hello.txt", got)
	}
}

func TestCreatePendingCreateFlushesBeforeUnlink(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	recorder := &fakeSyncRecorder{}
	server := NewFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil, nil, logrus.New(), recorder, nil)
	ctx := authContext("team-a", "sandbox-1")

	if _, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "hello.txt",
		Mode:     0o644,
		Flags:    uint32(syscall.O_RDWR),
	}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if len(recorder.remoteChanges) != 0 {
		t.Fatalf("remoteChanges after Create = %d, want 0", len(recorder.remoteChanges))
	}

	if _, err := server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "hello.txt",
	}); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}
	if len(recorder.remoteChanges) != 2 {
		t.Fatalf("remoteChanges after Unlink = %d, want 2", len(recorder.remoteChanges))
	}
	if got := recorder.remoteChanges[0]; got.EventType != db.SyncEventCreate || got.Path != "/hello.txt" {
		t.Fatalf("remoteChanges[0] = %+v, want create /hello.txt", got)
	}
	if got := recorder.remoteChanges[1]; got.EventType != db.SyncEventRemove || got.Path != "/hello.txt" {
		t.Fatalf("remoteChanges[1] = %+v, want remove /hello.txt", got)
	}
}

func TestCreatePendingCreateFlushesBeforeRename(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	recorder := &fakeSyncRecorder{}
	server := NewFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil, nil, logrus.New(), recorder, nil)
	ctx := authContext("team-a", "sandbox-1")

	if _, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "hello.txt",
		Mode:     0o644,
		Flags:    uint32(syscall.O_RDWR),
	}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if len(recorder.remoteChanges) != 0 {
		t.Fatalf("remoteChanges after Create = %d, want 0", len(recorder.remoteChanges))
	}

	if _, err := server.Rename(ctx, &pb.RenameRequest{
		VolumeId:  "vol-1",
		OldParent: uint64(meta.RootInode),
		OldName:   "hello.txt",
		NewParent: uint64(meta.RootInode),
		NewName:   "renamed.txt",
	}); err != nil {
		t.Fatalf("Rename() error = %v", err)
	}
	if len(recorder.remoteChanges) != 2 {
		t.Fatalf("remoteChanges after Rename = %d, want 2", len(recorder.remoteChanges))
	}
	if got := recorder.remoteChanges[0]; got.EventType != db.SyncEventCreate || got.Path != "/hello.txt" {
		t.Fatalf("remoteChanges[0] = %+v, want create /hello.txt", got)
	}
	if got := recorder.remoteChanges[1]; got.EventType != db.SyncEventRename || got.OldPath != "/hello.txt" || got.Path != "/renamed.txt" {
		t.Fatalf("remoteChanges[1] = %+v, want rename /hello.txt -> /renamed.txt", got)
	}
}

func TestCreatePendingCreateFlushRecordsCreateOnce(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	recorder := &fakeSyncRecorder{}
	server := NewFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil, nil, logrus.New(), recorder, nil)
	ctx := authContext("team-a", "sandbox-1")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "hello.txt",
		Mode:     0o644,
		Flags:    uint32(syscall.O_RDWR),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if len(recorder.remoteChanges) != 0 {
		t.Fatalf("remoteChanges after Create = %d, want 0", len(recorder.remoteChanges))
	}

	if _, err := server.Flush(ctx, &pb.FlushRequest{
		VolumeId: "vol-1",
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if len(recorder.remoteChanges) != 1 {
		t.Fatalf("remoteChanges after Flush+Release = %d, want 1", len(recorder.remoteChanges))
	}
	if got := recorder.remoteChanges[0]; got.EventType != db.SyncEventCreate || got.Path != "/hello.txt" {
		t.Fatalf("remoteChanges[0] = %+v, want create /hello.txt", got)
	}
}

func TestFlushDefersDirtyWriteReplayPayloadUntilRelease(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	recorder := &fakeSyncRecorder{}
	server := NewFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil, nil, logrus.New(), recorder, nil)
	ctx := authContext("team-a", "sandbox-1")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "hello.txt",
		Mode:     0o644,
		Flags:    uint32(syscall.O_RDWR),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	recorder.remoteChanges = nil

	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
		Data:     []byte("hello"),
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
		Offset:   5,
		Data:     []byte("!"),
	}); err != nil {
		t.Fatalf("second Write() error = %v", err)
	}
	if dirty, ok := server.peekDirtyWrite("vol-1", createResp.HandleId); !ok || !dirty.replayPayloadValid || string(dirty.replayPayload) != "hello!" {
		t.Fatalf("dirty write replay payload = %+v, %v; want buffered hello!", dirty, ok)
	}
	if _, err := server.Flush(ctx, &pb.FlushRequest{
		VolumeId: "vol-1",
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if len(recorder.remoteChanges) != 0 {
		t.Fatalf("remoteChanges after Flush = %d, want 0", len(recorder.remoteChanges))
	}

	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if len(recorder.remoteChanges) != 1 {
		t.Fatalf("remoteChanges after Release = %d, want 1", len(recorder.remoteChanges))
	}
	got := recorder.remoteChanges[0]
	if got.EventType != db.SyncEventWrite || got.Path != "/hello.txt" || got.SandboxID != "sandbox-1" {
		t.Fatalf("remoteChanges[0] = %+v, want write event for /hello.txt", got)
	}
	if !got.ContentAvailable || string(got.ContentBytes) != "hello!" {
		t.Fatalf("remoteChanges[0] content = available:%v bytes:%q, want hello!", got.ContentAvailable, string(got.ContentBytes))
	}
}

func TestSetAttrSizeOnOpenInodeDefersReplayPayloadUntilRelease(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	recorder := &fakeSyncRecorder{}
	server := NewFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil, nil, logrus.New(), recorder, nil)
	ctx := authContext("team-a", "sandbox-1")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "hello.txt",
		Mode:     0o644,
		Flags:    uint32(syscall.O_RDWR),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	recorder.remoteChanges = nil

	if _, err := server.SetAttr(ctx, &pb.SetAttrRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Valid:    uint32(meta.SetAttrSize),
		Attr: &pb.GetAttrResponse{
			Size: 0,
		},
	}); err != nil {
		t.Fatalf("SetAttr() error = %v", err)
	}
	if len(recorder.remoteChanges) != 0 {
		t.Fatalf("remoteChanges after SetAttr = %d, want 0", len(recorder.remoteChanges))
	}

	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
		Data:     []byte("hello"),
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if len(recorder.remoteChanges) != 1 {
		t.Fatalf("remoteChanges after Release = %d, want 1", len(recorder.remoteChanges))
	}
	got := recorder.remoteChanges[0]
	if got.EventType != db.SyncEventWrite || got.Path != "/hello.txt" || !got.ContentAvailable || string(got.ContentBytes) != "hello" {
		t.Fatalf("remoteChanges[0] = %+v, want final write content for /hello.txt", got)
	}
}

func TestSetAttrSizeOnOpenInodeRecordsTruncateOnReleaseWithoutWrite(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	recorder := &fakeSyncRecorder{}
	server := NewFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil, nil, logrus.New(), recorder, nil)
	ctx := authContext("team-a", "sandbox-1")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "empty.txt",
		Mode:     0o644,
		Flags:    uint32(syscall.O_RDWR),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	recorder.remoteChanges = nil

	if _, err := server.SetAttr(ctx, &pb.SetAttrRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Valid:    uint32(meta.SetAttrSize),
		Attr: &pb.GetAttrResponse{
			Size: 0,
		},
	}); err != nil {
		t.Fatalf("SetAttr() error = %v", err)
	}
	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if len(recorder.remoteChanges) != 1 {
		t.Fatalf("remoteChanges after Release = %d, want 1", len(recorder.remoteChanges))
	}
	got := recorder.remoteChanges[0]
	if got.EventType != db.SyncEventWrite || got.Path != "/empty.txt" || !got.ContentAvailable || len(got.ContentBytes) != 0 {
		t.Fatalf("remoteChanges[0] = %+v, want empty truncate write for /empty.txt", got)
	}
}

func TestCreateRejectsNamespaceIncompatiblePathBeforeMutation(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	recorder := &fakeSyncRecorder{
		validateErr: &volsync.NamespaceCompatibilityError{
			Capabilities: pathnorm.FilesystemCapabilities{WindowsCompatiblePaths: true},
			Issues: []pathnorm.CompatibilityIssue{{
				Code: pathnorm.IssueCodeWindowsReservedName,
				Path: "/CON",
			}},
		},
	}
	server := &FileSystemServer{
		volMgr: &fakeVolumeManager{
			volumes: map[string]*volume.VolumeContext{
				"vol-1": volCtx,
			},
		},
		syncRecorder: recorder,
		logger:       logrus.New(),
	}

	_, err := server.Create(authContext("team-a", "sandbox-1"), &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "CON",
		Mode:     0o644,
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("Create() code = %v, want %v (err=%v)", got, codes.FailedPrecondition, err)
	}
	if recorder.lastValidate == nil || recorder.lastValidate.Path != "/CON" {
		t.Fatalf("lastValidate = %+v, want path /CON", recorder.lastValidate)
	}
	if len(recorder.remoteChanges) != 0 {
		t.Fatalf("remoteChanges = %d, want 0", len(recorder.remoteChanges))
	}

	var inode meta.Ino
	var attr meta.Attr
	if st := volCtx.Meta.Lookup(meta.Background(), meta.RootInode, "CON", &inode, &attr, false); st != syscall.ENOENT {
		t.Fatalf("Lookup(CON) errno = %v, want %v", st, syscall.ENOENT)
	}
}

func TestRenamePropagatesNamespaceValidationAndRecordsRemoteChange(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	vfsCtx := vfs.NewLogContext(meta.Background())
	entry, handleID, errno := volCtx.VFS.Create(vfsCtx, meta.RootInode, "hello.txt", 0o644, 0, 0)
	if errno != 0 {
		t.Fatalf("setup create errno = %v", errno)
	}
	volCtx.VFS.Release(vfsCtx, entry.Inode, handleID)

	recorder := &fakeSyncRecorder{}
	server := &FileSystemServer{
		volMgr: &fakeVolumeManager{
			volumes: map[string]*volume.VolumeContext{
				"vol-1": volCtx,
			},
		},
		syncRecorder: recorder,
		logger:       logrus.New(),
	}

	_, err := server.Rename(authContext("team-a", "sandbox-1"), &pb.RenameRequest{
		VolumeId:  "vol-1",
		OldParent: uint64(meta.RootInode),
		OldName:   "hello.txt",
		NewParent: uint64(meta.RootInode),
		NewName:   "renamed.txt",
	})
	if err != nil {
		t.Fatalf("Rename() error = %v", err)
	}
	if recorder.lastValidate == nil {
		t.Fatal("expected namespace validation request")
	}
	if recorder.lastValidate.EventType != db.SyncEventRename || recorder.lastValidate.OldPath != "/hello.txt" || recorder.lastValidate.Path != "/renamed.txt" {
		t.Fatalf("lastValidate = %+v, want rename /hello.txt -> /renamed.txt", recorder.lastValidate)
	}
	if len(recorder.remoteChanges) != 1 {
		t.Fatalf("remoteChanges = %d, want 1", len(recorder.remoteChanges))
	}
	if got := recorder.remoteChanges[0]; got.EventType != db.SyncEventRename || got.OldPath != "/hello.txt" || got.Path != "/renamed.txt" {
		t.Fatalf("remoteChanges[0] = %+v, want rename event /hello.txt -> /renamed.txt", got)
	}

	var inode meta.Ino
	var attr meta.Attr
	if st := volCtx.Meta.Lookup(meta.Background(), meta.RootInode, "hello.txt", &inode, &attr, false); st != syscall.ENOENT {
		t.Fatalf("Lookup(old) errno = %v, want %v", st, syscall.ENOENT)
	}
	if st := volCtx.Meta.Lookup(meta.Background(), meta.RootInode, "renamed.txt", &inode, &attr, false); st != 0 {
		t.Fatalf("Lookup(new) errno = %v, want 0", st)
	}
}

func TestRenameRejectsNamespaceIncompatibleTargetBeforeMutation(t *testing.T) {
	volCtx := newMountedTestVolumeContext(t, "vol-1", "team-a")
	vfsCtx := vfs.NewLogContext(meta.Background())
	entry, handleID, errno := volCtx.VFS.Create(vfsCtx, meta.RootInode, "hello.txt", 0o644, 0, 0)
	if errno != 0 {
		t.Fatalf("setup create errno = %v", errno)
	}
	volCtx.VFS.Release(vfsCtx, entry.Inode, handleID)

	recorder := &fakeSyncRecorder{
		validateErr: &volsync.NamespaceCompatibilityError{
			Capabilities: pathnorm.FilesystemCapabilities{WindowsCompatiblePaths: true},
			Issues: []pathnorm.CompatibilityIssue{{
				Code: pathnorm.IssueCodeWindowsReservedName,
				Path: "/CON.txt",
			}},
		},
	}
	server := &FileSystemServer{
		volMgr: &fakeVolumeManager{
			volumes: map[string]*volume.VolumeContext{
				"vol-1": volCtx,
			},
		},
		syncRecorder: recorder,
		logger:       logrus.New(),
	}

	_, err := server.Rename(authContext("team-a", "sandbox-1"), &pb.RenameRequest{
		VolumeId:  "vol-1",
		OldParent: uint64(meta.RootInode),
		OldName:   "hello.txt",
		NewParent: uint64(meta.RootInode),
		NewName:   "CON.txt",
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("Rename() code = %v, want %v (err=%v)", got, codes.FailedPrecondition, err)
	}
	if recorder.lastValidate == nil || recorder.lastValidate.Path != "/CON.txt" || recorder.lastValidate.OldPath != "/hello.txt" {
		t.Fatalf("lastValidate = %+v, want rename /hello.txt -> /CON.txt", recorder.lastValidate)
	}
	if len(recorder.remoteChanges) != 0 {
		t.Fatalf("remoteChanges = %d, want 0", len(recorder.remoteChanges))
	}

	var inode meta.Ino
	var attr meta.Attr
	if st := volCtx.Meta.Lookup(meta.Background(), meta.RootInode, "hello.txt", &inode, &attr, false); st != 0 {
		t.Fatalf("Lookup(old) errno = %v, want 0", st)
	}
	if st := volCtx.Meta.Lookup(meta.Background(), meta.RootInode, "CON.txt", &inode, &attr, false); st != syscall.ENOENT {
		t.Fatalf("Lookup(new) errno = %v, want %v", st, syscall.ENOENT)
	}
}

type fakeVolumeRepo struct {
	volumes map[string]*db.SandboxVolume
	owners  map[string]*db.SandboxVolumeOwner
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

func (r *fakeVolumeRepo) GetSandboxVolumeOwner(_ context.Context, volumeID string) (*db.SandboxVolumeOwner, error) {
	if r.owners == nil {
		return nil, db.ErrNotFound
	}
	if owner, ok := r.owners[volumeID]; ok {
		return owner, nil
	}
	return nil, db.ErrNotFound
}

type fakeVolumeManager struct {
	volumes        map[string]*volume.VolumeContext
	mountCalls     int
	unmountCalls   int
	ackCalls       int
	mountSessionID string
	mountSecret    string
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
	trackedSessionID string
}

func (m *fakeVolumeManager) MountVolume(_ context.Context, s3Prefix, volumeID, teamID string, config *volume.VolumeConfig, accessMode volume.AccessMode) (string, string, time.Time, error) {
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
	sessionSecret := m.mountSecret
	if sessionSecret == "" {
		sessionSecret = "secret-test"
	}
	mountedAt := m.mountedAt
	if mountedAt.IsZero() {
		mountedAt = time.Unix(1700000000, 0)
	}
	return sessionID, sessionSecret, mountedAt, nil
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

func (m *fakeVolumeManager) TrackVolumeSession(sandboxID, volumeID, sessionID string) {
	m.trackedSandboxID = sandboxID
	m.trackedVolumeID = volumeID
	m.trackedSessionID = sessionID
}

func newMountedTestVolumeContext(t *testing.T, volumeID, teamID string) *volume.VolumeContext {
	t.Helper()

	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/test"

	metaClient := meta.NewClient("memkv://"+uuid.NewString(), metaConf)
	format := &meta.Format{
		Name:        "test",
		UUID:        uuid.NewString(),
		Storage:     "mem",
		BlockSize:   4096,
		Compression: "none",
		DirStats:    true,
	}
	if err := metaClient.Init(format, true); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if err := metaClient.NewSession(true); err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	chunkConf := chunk.Config{
		BlockSize:  format.BlockSize * 1024,
		Compress:   format.Compression,
		MaxUpload:  2,
		BufferSize: 8 << 20,
		CacheSize:  8 << 20,
		CacheDir:   "memory",
	}
	blob, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatalf("CreateStorage() error = %v", err)
	}
	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, chunkConf, registry)
	vfsConf := &vfs.Config{
		Meta:            metaConf,
		Format:          *format,
		Version:         "test",
		Chunk:           &chunkConf,
		FuseOpts:        &vfs.FuseOptions{},
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
		DirEntryTimeout: time.Second,
	}

	t.Cleanup(func() {
		_ = metaClient.CloseSession()
		_ = metaClient.Shutdown()
	})

	return &volume.VolumeContext{
		VolumeID:  volumeID,
		TeamID:    teamID,
		Meta:      metaClient,
		Store:     store,
		VFS:       vfs.NewVFS(vfsConf, metaClient, store, registry, registry),
		MountedAt: time.Now(),
		RootInode: meta.RootInode,
		RootPath:  "/",
	}
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
