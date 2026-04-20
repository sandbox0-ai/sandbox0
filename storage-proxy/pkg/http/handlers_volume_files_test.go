package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

type fakeHTTPVolumeMountManager struct {
	acquireCalls      int
	releaseCalls      int
	lastAcquireVolume string
	lastSessionID     string
	unmountCalls      int
	lastUnmountVol    string
	lastUnmountSes    string
	cleanupCalls      int
	lastCleanupVolume string
	cleanupDirectFunc func(context.Context, string) (bool, error)
	syncCalls         int
	lastSyncVolume    string
	syncFunc          func(context.Context, string) error
}

type fakeVolumeFilePodResolver struct {
	urls map[string]string
}

func mustMountOptionsRaw(t *testing.T, opts volume.MountOptions) *json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(opts)
	if err != nil {
		t.Fatalf("marshal mount options: %v", err)
	}
	msg := json.RawMessage(raw)
	return &msg
}

func (f *fakeVolumeFilePodResolver) ResolvePodURL(_ context.Context, podID string) (*url.URL, error) {
	if f == nil || f.urls == nil {
		return nil, errors.New("resolver unavailable")
	}
	rawURL, ok := f.urls[podID]
	if !ok {
		return nil, errors.New("pod not found")
	}
	return url.Parse(rawURL)
}

func (f *fakeHTTPVolumeMountManager) GetVolume(volumeID string) (*volume.VolumeContext, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeHTTPVolumeMountManager) UnmountVolume(_ context.Context, volumeID, sessionID string) error {
	f.unmountCalls++
	f.lastUnmountVol = volumeID
	f.lastUnmountSes = sessionID
	return nil
}

func (f *fakeHTTPVolumeMountManager) AcquireDirectVolumeFileMount(ctx context.Context, volumeID string, mountFn func(context.Context) (string, error)) (func(), error) {
	f.acquireCalls++
	f.lastAcquireVolume = volumeID
	sessionID, err := mountFn(ctx)
	if err != nil {
		return nil, err
	}
	f.lastSessionID = sessionID
	return func() {
		f.releaseCalls++
	}, nil
}

func (f *fakeHTTPVolumeMountManager) CleanupIdleDirectVolumeFileMount(ctx context.Context, volumeID string) (bool, error) {
	f.cleanupCalls++
	f.lastCleanupVolume = volumeID
	if f.cleanupDirectFunc != nil {
		return f.cleanupDirectFunc(ctx, volumeID)
	}
	return false, nil
}

func (f *fakeHTTPVolumeMountManager) SyncDirectVolumeFileMount(ctx context.Context, volumeID string) error {
	f.syncCalls++
	f.lastSyncVolume = volumeID
	if f.syncFunc != nil {
		return f.syncFunc(ctx, volumeID)
	}
	return nil
}

type fakeHTTPVolumeFileRPC struct {
	mountCalls  int
	lastMountID string
	sessionID   string
	mountFunc   func(context.Context, *pb.MountVolumeRequest) (*pb.MountVolumeResponse, error)

	getAttrFunc    func(context.Context, *pb.GetAttrRequest) (*pb.GetAttrResponse, error)
	lookupFunc     func(context.Context, *pb.LookupRequest) (*pb.NodeResponse, error)
	openFunc       func(context.Context, *pb.OpenRequest) (*pb.OpenResponse, error)
	readFunc       func(context.Context, *pb.ReadRequest) (*pb.ReadResponse, error)
	writeFunc      func(context.Context, *pb.WriteRequest) (*pb.WriteResponse, error)
	createFunc     func(context.Context, *pb.CreateRequest) (*pb.NodeResponse, error)
	mkdirFunc      func(context.Context, *pb.MkdirRequest) (*pb.NodeResponse, error)
	unlinkFunc     func(context.Context, *pb.UnlinkRequest) (*pb.Empty, error)
	rmdirFunc      func(context.Context, *pb.RmdirRequest) (*pb.Empty, error)
	readDirFunc    func(context.Context, *pb.ReadDirRequest) (*pb.ReadDirResponse, error)
	openDirFunc    func(context.Context, *pb.OpenDirRequest) (*pb.OpenDirResponse, error)
	releaseDirFunc func(context.Context, *pb.ReleaseDirRequest) (*pb.Empty, error)
	renameFunc     func(context.Context, *pb.RenameRequest) (*pb.Empty, error)
	releaseFunc    func(context.Context, *pb.ReleaseRequest) (*pb.Empty, error)
}

func (f *fakeHTTPVolumeFileRPC) MountVolume(ctx context.Context, req *pb.MountVolumeRequest) (*pb.MountVolumeResponse, error) {
	if f.mountFunc != nil {
		return f.mountFunc(ctx, req)
	}
	f.mountCalls++
	if req != nil {
		f.lastMountID = req.VolumeId
	}
	sessionID := f.sessionID
	if sessionID == "" {
		sessionID = "direct-session-1"
	}
	return &pb.MountVolumeResponse{
		VolumeId:       f.lastMountID,
		MountSessionId: sessionID,
		MountedAt:      time.Now().Unix(),
	}, nil
}

func (f *fakeHTTPVolumeFileRPC) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	if f.getAttrFunc != nil {
		return f.getAttrFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	if f.lookupFunc != nil {
		return f.lookupFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	if f.openFunc != nil {
		return f.openFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	if f.readFunc != nil {
		return f.readFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	if f.writeFunc != nil {
		return f.writeFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) Create(ctx context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	if f.createFunc != nil {
		return f.createFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
	if f.mkdirFunc != nil {
		return f.mkdirFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
	if f.unlinkFunc != nil {
		return f.unlinkFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	if f.rmdirFunc != nil {
		return f.rmdirFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	if f.readDirFunc != nil {
		return f.readDirFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) OpenDir(ctx context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	if f.openDirFunc != nil {
		return f.openDirFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) ReleaseDir(ctx context.Context, req *pb.ReleaseDirRequest) (*pb.Empty, error) {
	if f.releaseDirFunc != nil {
		return f.releaseDirFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
	if f.renameFunc != nil {
		return f.renameFunc(ctx, req)
	}
	return nil, nil
}

func (f *fakeHTTPVolumeFileRPC) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
	if f.releaseFunc != nil {
		return f.releaseFunc(ctx, req)
	}
	return nil, nil
}

func newVolumeFileTestServer(fileRPC *fakeHTTPVolumeFileRPC) (*Server, *fakeHTTPVolumeMountManager) {
	repo := newFakeHTTPRepo()
	defaultUID := int64(1000)
	defaultGID := int64(1000)
	repo.volumes["vol-1"] = &db.SandboxVolume{
		ID:              "vol-1",
		TeamID:          "team-a",
		DefaultPosixUID: &defaultUID,
		DefaultPosixGID: &defaultGID,
		AccessMode:      string(volume.AccessModeRWX),
	}
	volMgr := &fakeHTTPVolumeMountManager{}
	server := &Server{
		logger:        logrus.New(),
		repo:          repo,
		volMgr:        volMgr,
		fileRPC:       fileRPC,
		cfg:           &config.StorageProxyConfig{HeartbeatTimeout: 15},
		selfPodID:     "local-pod",
		selfClusterID: "cluster-a",
	}
	return server, volMgr
}

func volumeDirAttr() *pb.GetAttrResponse {
	return &pb.GetAttrResponse{Mode: syscall.S_IFDIR | 0o755}
}

func volumeFileAttr(size int) *pb.GetAttrResponse {
	return &pb.GetAttrResponse{
		Mode:      syscall.S_IFREG | 0o644,
		Size:      uint64(size),
		MtimeSec:  1710000000,
		MtimeNsec: 123,
	}
}

func TestPrepareVolumeFileRequestUsesSharedDirectMountLease(t *testing.T) {
	fileRPC := &fakeHTTPVolumeFileRPC{sessionID: "direct-session-1"}
	server, volMgr := newVolumeFileTestServer(fileRPC)

	ctx := internalauth.WithClaims(context.Background(), &internalauth.Claims{TeamID: "team-a"})
	_, volumeRecord, cleanup, err := server.prepareVolumeFileRequest(ctx, "vol-1")
	if err != nil {
		t.Fatalf("prepareVolumeFileRequest() error = %v", err)
	}
	if volumeRecord == nil || volumeRecord.ID != "vol-1" {
		t.Fatalf("unexpected volume record: %+v", volumeRecord)
	}
	if volMgr.acquireCalls != 1 {
		t.Fatalf("AcquireDirectVolumeFileMount() calls = %d, want 1", volMgr.acquireCalls)
	}
	if volMgr.lastAcquireVolume != "vol-1" {
		t.Fatalf("AcquireDirectVolumeFileMount() volume = %q, want %q", volMgr.lastAcquireVolume, "vol-1")
	}
	if fileRPC.mountCalls != 1 || fileRPC.lastMountID != "vol-1" {
		t.Fatalf("MountVolume() got calls=%d volume=%q", fileRPC.mountCalls, fileRPC.lastMountID)
	}

	cleanup()

	if volMgr.releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", volMgr.releaseCalls)
	}
}

func TestPrepareVolumeFileRequestRequiresDefaultPosixIdentity(t *testing.T) {
	fileRPC := &fakeHTTPVolumeFileRPC{}
	server, _ := newVolumeFileTestServer(fileRPC)
	repo := server.repo.(*fakeHTTPRepo)
	repo.volumes["vol-1"] = &db.SandboxVolume{
		ID:         "vol-1",
		TeamID:     "team-a",
		AccessMode: string(volume.AccessModeRWX),
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/files/stat?path=/docs/report.txt", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileStat(recorder, req)

	if recorder.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusPreconditionFailed)
	}
}

func TestHandleVolumeFileStatReturnsResolvedEntry(t *testing.T) {
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			if req.Actor == nil {
				t.Fatalf("Lookup actor = nil, want default volume actor")
			}
			if req.Actor.Pid != 0 || req.Actor.Uid != 1000 {
				t.Fatalf("Lookup actor = %+v, want pid=0 uid=1000", req.Actor)
			}
			if len(req.Actor.Gids) != 1 || req.Actor.Gids[0] != 1000 {
				t.Fatalf("Lookup gids = %v, want [1000]", req.Actor.Gids)
			}
			switch {
			case req.Parent == 1 && req.Name == "docs":
				return &pb.NodeResponse{Inode: 2, Attr: volumeDirAttr()}, nil
			case req.Parent == 2 && req.Name == "report.txt":
				return &pb.NodeResponse{Inode: 3, Attr: volumeFileAttr(12)}, nil
			default:
				return nil, fserror.New(fserror.NotFound, "missing")
			}
		},
	}
	server, volMgr := newVolumeFileTestServer(fileRPC)

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/files/stat?path=/docs/report.txt", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileStat(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	resp, apiErr, err := spec.DecodeResponse[volumeFileInfo](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if resp.Name != "report.txt" || resp.Path != "/docs/report.txt" || resp.Type != "file" || resp.Size != 12 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if volMgr.releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", volMgr.releaseCalls)
	}
}

func TestHandleVolumeFileListReturnsEntries(t *testing.T) {
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			if req.Parent == 1 && req.Name == "docs" {
				return &pb.NodeResponse{Inode: 2, Attr: volumeDirAttr()}, nil
			}
			return nil, fserror.New(fserror.NotFound, "missing")
		},
		openDirFunc: func(_ context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
			if req.Inode != 2 {
				t.Fatalf("OpenDir inode = %d, want 2", req.Inode)
			}
			return &pb.OpenDirResponse{HandleId: 9}, nil
		},
		readDirFunc: func(_ context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
			if req.HandleId != 9 {
				t.Fatalf("ReadDir handle = %d, want 9", req.HandleId)
			}
			return &pb.ReadDirResponse{Entries: []*pb.DirEntry{
				{Name: ".", Inode: 2, Attr: volumeDirAttr()},
				{Name: "..", Inode: 1, Attr: volumeDirAttr()},
				{Name: "a.txt", Inode: 3, Attr: volumeFileAttr(5)},
				{Name: "nested", Inode: 4, Attr: volumeDirAttr()},
			}}, nil
		},
	}
	server, _ := newVolumeFileTestServer(fileRPC)

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/files/list?path=/docs", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileList(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	resp, apiErr, err := spec.DecodeResponse[struct {
		Entries []*volumeFileInfo `json:"entries"`
	}](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if len(resp.Entries) != 2 {
		t.Fatalf("entry count = %d, want 2", len(resp.Entries))
	}
	if resp.Entries[0].Path != "/docs/a.txt" || resp.Entries[1].Path != "/docs/nested" {
		t.Fatalf("unexpected entries: %+v", resp.Entries)
	}
}

func TestReadVolumeFileReturnsBinaryBody(t *testing.T) {
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			if req.Parent == 1 && req.Name == "hello.txt" {
				return &pb.NodeResponse{Inode: 3, Attr: volumeFileAttr(5)}, nil
			}
			return nil, fserror.New(fserror.NotFound, "missing")
		},
		openFunc: func(_ context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
			return &pb.OpenResponse{HandleId: 11}, nil
		},
		readFunc: func(_ context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
			if req.HandleId != 11 || req.Size != 5 {
				t.Fatalf("unexpected Read request: %+v", req)
			}
			return &pb.ReadResponse{Data: []byte("hello")}, nil
		},
	}
	server, _ := newVolumeFileTestServer(fileRPC)

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/files?path=/hello.txt", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileOperation(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if recorder.Body.String() != "hello" {
		t.Fatalf("body = %q, want %q", recorder.Body.String(), "hello")
	}
	if recorder.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("content-type = %q, want application/octet-stream", recorder.Header().Get("Content-Type"))
	}
}

func TestWriteVolumeFileWritesExistingPath(t *testing.T) {
	var wrote *pb.WriteRequest
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			if req.Parent == 1 && req.Name == "hello.txt" {
				return &pb.NodeResponse{Inode: 3, Attr: volumeFileAttr(0)}, nil
			}
			return nil, fserror.New(fserror.NotFound, "missing")
		},
		openFunc: func(_ context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
			if req.Inode != 3 {
				t.Fatalf("Open inode = %d, want 3", req.Inode)
			}
			if req.Flags&uint32(syscall.O_TRUNC) == 0 {
				t.Fatalf("Open flags = %#x, want O_TRUNC", req.Flags)
			}
			return &pb.OpenResponse{HandleId: 15}, nil
		},
		writeFunc: func(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
			wrote = req
			return &pb.WriteResponse{BytesWritten: int64(len(req.Data))}, nil
		},
	}
	server, volMgr := newVolumeFileTestServer(fileRPC)

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/files?path=/hello.txt", bytes.NewReader([]byte("hello world")))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileOperation(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if wrote == nil || string(wrote.Data) != "hello world" || wrote.HandleId != 15 {
		t.Fatalf("unexpected write request: %+v", wrote)
	}
	if volMgr.syncCalls != 1 || volMgr.lastSyncVolume != "vol-1" {
		t.Fatalf("SyncDirectVolumeFileMount() got calls=%d volume=%q, want 1 vol-1", volMgr.syncCalls, volMgr.lastSyncVolume)
	}
}

func TestDeleteVolumeFileUnlinksResolvedPath(t *testing.T) {
	var unlinked *pb.UnlinkRequest
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			if req.Parent == 1 && req.Name == "hello.txt" {
				return &pb.NodeResponse{Inode: 3, Attr: volumeFileAttr(5)}, nil
			}
			return nil, fserror.New(fserror.NotFound, "missing")
		},
		unlinkFunc: func(_ context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
			unlinked = req
			return &pb.Empty{}, nil
		},
	}
	server, volMgr := newVolumeFileTestServer(fileRPC)

	req := httptest.NewRequest(http.MethodDelete, "/sandboxvolumes/vol-1/files?path=/hello.txt", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileOperation(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if unlinked == nil || unlinked.Parent != 1 || unlinked.Name != "hello.txt" {
		t.Fatalf("unexpected unlink request: %+v", unlinked)
	}
	if volMgr.syncCalls != 1 || volMgr.lastSyncVolume != "vol-1" {
		t.Fatalf("SyncDirectVolumeFileMount() got calls=%d volume=%q, want 1 vol-1", volMgr.syncCalls, volMgr.lastSyncVolume)
	}
}

func TestHandleVolumeFileMoveRenamesPath(t *testing.T) {
	var renamed *pb.RenameRequest
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			switch {
			case req.Parent == 1 && req.Name == "docs":
				return &pb.NodeResponse{Inode: 2, Attr: volumeDirAttr()}, nil
			case req.Parent == 2 && req.Name == "report.txt":
				return &pb.NodeResponse{Inode: 3, Attr: volumeFileAttr(12)}, nil
			case req.Parent == 1 && req.Name == "archive":
				return &pb.NodeResponse{Inode: 4, Attr: volumeDirAttr()}, nil
			default:
				return nil, fserror.New(fserror.NotFound, "missing")
			}
		},
		renameFunc: func(_ context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
			renamed = req
			return &pb.Empty{}, nil
		},
	}
	server, volMgr := newVolumeFileTestServer(fileRPC)

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/files/move", bytes.NewReader([]byte(`{"source":"/docs/report.txt","destination":"/archive/report-old.txt"}`)))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileMove(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if renamed == nil || renamed.OldParent != 2 || renamed.OldName != "report.txt" || renamed.NewParent != 4 || renamed.NewName != "report-old.txt" {
		t.Fatalf("unexpected rename request: %+v", renamed)
	}
	if volMgr.syncCalls != 1 || volMgr.lastSyncVolume != "vol-1" {
		t.Fatalf("SyncDirectVolumeFileMount() got calls=%d volume=%q, want 1 vol-1", volMgr.syncCalls, volMgr.lastSyncVolume)
	}
}

func TestHandleVolumeFileStatProxiesToRemoteOwnerPod(t *testing.T) {
	remoteSeen := make(chan *http.Request, 1)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case remoteSeen <- r.Clone(r.Context()):
		default:
		}
		_, _ = io.WriteString(w, `{"success":true,"data":{"name":"proxied.txt","path":"/proxied.txt","type":"file","size":7}}`)
	}))
	defer remote.Close()

	fileRPC := &fakeHTTPVolumeFileRPC{}
	server, volMgr := newVolumeFileTestServer(fileRPC)
	repo := server.repo.(*fakeHTTPRepo)
	repo.activeMounts["vol-1"] = []*db.VolumeMount{
		{
			VolumeID:  "vol-1",
			ClusterID: "cluster-a",
			PodID:     "remote-pod",
			MountedAt: time.Unix(10, 0),
		},
	}
	server.podResolver = &fakeVolumeFilePodResolver{
		urls: map[string]string{"remote-pod": remote.URL},
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/files/stat?path=/docs/report.txt", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileStat(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if volMgr.acquireCalls != 0 {
		t.Fatalf("AcquireDirectVolumeFileMount() calls = %d, want 0", volMgr.acquireCalls)
	}
	select {
	case seen := <-remoteSeen:
		if seen.Header.Get(volumeFileAffinityRoutedPodHeader) != "remote-pod" {
			t.Fatalf("routed pod header = %q, want %q", seen.Header.Get(volumeFileAffinityRoutedPodHeader), "remote-pod")
		}
	default:
		t.Fatal("expected request to be proxied to remote owner")
	}
}

func TestHandleVolumeFileStatPrefersCtldOwnerAndPropagatesTeamHeader(t *testing.T) {
	remoteSeen := make(chan *http.Request, 1)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case remoteSeen <- r.Clone(r.Context()):
		default:
		}
		_, _ = io.WriteString(w, `{"success":true,"data":{"name":"proxied.txt","path":"/proxied.txt","type":"file","size":7}}`)
	}))
	defer remote.Close()
	remoteURL, err := url.Parse(remote.URL)
	if err != nil {
		t.Fatalf("parse remote url: %v", err)
	}
	remotePort, err := strconv.Atoi(remoteURL.Port())
	if err != nil {
		t.Fatalf("parse remote port: %v", err)
	}

	fileRPC := &fakeHTTPVolumeFileRPC{}
	server, volMgr := newVolumeFileTestServer(fileRPC)
	repo := server.repo.(*fakeHTTPRepo)
	repo.activeMounts["vol-1"] = []*db.VolumeMount{
		{
			VolumeID:     "vol-1",
			ClusterID:    "cluster-a",
			PodID:        "local-pod",
			MountedAt:    time.Unix(20, 0),
			MountOptions: mustMountOptionsRaw(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindStorageProxy}),
		},
		{
			VolumeID:     "vol-1",
			ClusterID:    "cluster-a",
			PodID:        "sandbox0-system/ctld-node-a",
			MountedAt:    time.Unix(10, 0),
			MountOptions: mustMountOptionsRaw(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindCtld, OwnerPort: remotePort}),
		},
	}
	server.podResolver = &fakeVolumeFilePodResolver{
		urls: map[string]string{"sandbox0-system/ctld-node-a": "http://127.0.0.1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/files/stat?path=/docs/report.txt", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileStat(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if volMgr.acquireCalls != 0 {
		t.Fatalf("AcquireDirectVolumeFileMount() calls = %d, want 0", volMgr.acquireCalls)
	}
	select {
	case seen := <-remoteSeen:
		if got := seen.Header.Get(volumeFileAffinityRoutedPodHeader); got != "sandbox0-system/ctld-node-a" {
			t.Fatalf("routed pod header = %q, want %q", got, "sandbox0-system/ctld-node-a")
		}
		if got := seen.Header.Get(volumeFileAffinityTeamHeader); got != "team-a" {
			t.Fatalf("team header = %q, want %q", got, "team-a")
		}
	default:
		t.Fatal("expected request to be proxied to ctld owner")
	}
}

func TestHandleVolumeFileStatPrefersLocalOwnerPod(t *testing.T) {
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			switch {
			case req.Parent == 1 && req.Name == "docs":
				return &pb.NodeResponse{Inode: 2, Attr: volumeDirAttr()}, nil
			case req.Parent == 2 && req.Name == "report.txt":
				return &pb.NodeResponse{Inode: 3, Attr: volumeFileAttr(12)}, nil
			default:
				return nil, fserror.New(fserror.NotFound, "missing")
			}
		},
	}
	server, volMgr := newVolumeFileTestServer(fileRPC)
	repo := server.repo.(*fakeHTTPRepo)
	repo.activeMounts["vol-1"] = []*db.VolumeMount{
		{
			VolumeID:  "vol-1",
			ClusterID: "cluster-a",
			PodID:     "remote-pod",
			MountedAt: time.Unix(10, 0),
		},
		{
			VolumeID:  "vol-1",
			ClusterID: "cluster-a",
			PodID:     "local-pod",
			MountedAt: time.Unix(20, 0),
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/files/stat?path=/docs/report.txt", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileStat(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if volMgr.acquireCalls != 1 {
		t.Fatalf("AcquireDirectVolumeFileMount() calls = %d, want 1", volMgr.acquireCalls)
	}
}

func TestPrepareOrProxyVolumeFileRequestReroutesAfterMountConflict(t *testing.T) {
	remoteSeen := make(chan struct{}, 1)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case remoteSeen <- struct{}{}:
		default:
		}
		_, _ = io.WriteString(w, `{"success":true,"data":{"name":"proxied.txt","path":"/proxied.txt","type":"file","size":7}}`)
	}))
	defer remote.Close()

	fileRPC := &fakeHTTPVolumeFileRPC{
		mountFunc: func(_ context.Context, req *pb.MountVolumeRequest) (*pb.MountVolumeResponse, error) {
			return nil, errors.New("volume vol-1 already mounted on another instance")
		},
	}
	server, volMgr := newVolumeFileTestServer(fileRPC)
	repo := server.repo.(*fakeHTTPRepo)
	callCount := 0
	repo.getActiveFunc = func(ctx context.Context, volumeID string, heartbeatTimeout int) ([]*db.VolumeMount, error) {
		callCount++
		if callCount == 1 {
			return nil, nil
		}
		return []*db.VolumeMount{
			{
				VolumeID:  "vol-1",
				ClusterID: "cluster-a",
				PodID:     "remote-pod",
				MountedAt: time.Unix(10, 0),
			},
		}, nil
	}
	server.podResolver = &fakeVolumeFilePodResolver{
		urls: map[string]string{"remote-pod": remote.URL},
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/files/stat?path=/docs/report.txt", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileStat(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if volMgr.acquireCalls != 1 {
		t.Fatalf("AcquireDirectVolumeFileMount() calls = %d, want 1", volMgr.acquireCalls)
	}
	select {
	case <-remoteSeen:
	default:
		t.Fatal("expected mount conflict to reroute to remote owner")
	}
}
