package http

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

type countingByteReader struct {
	reads int64
}

func (r *countingByteReader) Read(p []byte) (int, error) {
	for index := range p {
		p[index] = 'x'
	}
	r.reads += int64(len(p))
	return len(p), nil
}

func TestReadVolumeFileBodyStopsAtConfiguredLimit(t *testing.T) {
	reader := &countingByteReader{}
	data, err := readVolumeFileBody(reader, 8)
	if !errors.Is(err, errFileTooLarge) {
		t.Fatalf("readVolumeFileBody() error = %v, want errFileTooLarge", err)
	}
	if data != nil {
		t.Fatalf("readVolumeFileBody() returned %d bytes on overflow", len(data))
	}
	if reader.reads != 9 {
		t.Fatalf("reader consumed %d bytes, want 9", reader.reads)
	}
}

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

func TestTranslateVolumeRPCErrorUsesStructuredErrno(t *testing.T) {
	if got := translateVolumeRPCError(fserror.NewErrno(syscall.ENOTEMPTY, "opaque failure")); !errors.Is(got, errDirectoryNotEmpty) {
		t.Fatalf("translateVolumeRPCError(ENOTEMPTY) = %v, want directory-not-empty", got)
	}
	if got := translateVolumeRPCError(fserror.NewErrno(syscall.ENOTDIR, "opaque failure")); !errors.Is(got, errPathNotDir) {
		t.Fatalf("translateVolumeRPCError(ENOTDIR) = %v, want path-not-directory", got)
	}

	generic := fserror.New(fserror.FailedPrecondition, "directory not empty")
	if got := translateVolumeRPCError(generic); got != generic {
		t.Fatalf("translateVolumeRPCError(generic precondition) = %v, want original error", got)
	}
}

type fakeVolumeFilePodResolver struct {
	urls map[string]string
}

type fakeVolumeWatchHub struct {
	mu            sync.Mutex
	subscriptions int
	cancellations int
	closers       []func()
}

func (h *fakeVolumeWatchHub) Subscribe(_ *pb.WatchRequest) (string, <-chan *pb.WatchEvent, func()) {
	h.mu.Lock()
	h.subscriptions++
	watchID := fmt.Sprintf("watch-%d", h.subscriptions)
	events := make(chan *pb.WatchEvent)
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			h.cancellations++
			h.mu.Unlock()
			close(events)
		})
	}
	h.closers = append(h.closers, cancel)
	h.mu.Unlock()
	return watchID, events, cancel
}

func (h *fakeVolumeWatchHub) counts() (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.subscriptions, h.cancellations
}

func (h *fakeVolumeWatchHub) closeSubscription(index int) {
	h.mu.Lock()
	if index < 0 || index >= len(h.closers) {
		h.mu.Unlock()
		return
	}
	closeSubscription := h.closers[index]
	h.mu.Unlock()
	closeSubscription()
}

type stagedStorageOperationQuota struct {
	mu           sync.Mutex
	allowedCalls int
	calls        int
}

func (q *stagedStorageOperationQuota) Admit(_ context.Context, _ string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls++
	if q.calls > q.allowedCalls {
		return errors.New("storage operation rejected")
	}
	return nil
}

func (*stagedStorageOperationQuota) Close() error { return nil }

func (q *stagedStorageOperationQuota) callCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.calls
}

type fakeHTTPSharedVolumeBarrier struct {
	sharedCalls    int
	exclusiveCalls int
	lastVolumeID   string
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

func (f *fakeHTTPSharedVolumeBarrier) WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	f.sharedCalls++
	f.lastVolumeID = volumeID
	return fn(ctx)
}

func (f *fakeHTTPSharedVolumeBarrier) WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	f.exclusiveCalls++
	f.lastVolumeID = volumeID
	return fn(ctx)
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
	symlinkFunc    func(context.Context, *pb.SymlinkRequest) (*pb.NodeResponse, error)
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

func (f *fakeHTTPVolumeFileRPC) Symlink(ctx context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	if f.symlinkFunc != nil {
		return f.symlinkFunc(ctx, req)
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
	return newVolumeFileTestServerWithBarrier(fileRPC, nil)
}

func newVolumeFileTestServerWithBarrier(fileRPC *fakeHTTPVolumeFileRPC, barrier volumeMutationBarrier) (*Server, *fakeHTTPVolumeMountManager) {
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
		barrier:       barrier,
		volMgr:        volMgr,
		fileRPC:       fileRPC,
		cfg:           &config.StorageProxyConfig{HeartbeatTimeout: 15},
		selfPodID:     "local-pod",
		selfClusterID: "cluster-a",
	}
	return server, volMgr
}

func startVolumeWatchTestServer(t *testing.T, server *Server) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /sandboxvolumes/{id}/files/watch", func(w http.ResponseWriter, r *http.Request) {
		teamID := strings.TrimSpace(r.Header.Get(volumeFileAffinityTeamHeader))
		if teamID == "" {
			teamID = strings.TrimSpace(r.URL.Query().Get("team"))
		}
		r = r.WithContext(internalauth.WithClaims(r.Context(), &internalauth.Claims{TeamID: teamID}))
		server.handleVolumeFileWatch(w, r)
	})
	return httptest.NewServer(mux)
}

func dialVolumeWatch(
	t *testing.T,
	httpServer *httptest.Server,
	volumeID string,
	teamID string,
) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") +
		"/sandboxvolumes/" + url.PathEscape(volumeID) + "/files/watch?team=" + url.QueryEscape(teamID)
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		if response != nil {
			t.Fatalf("dial volume watch: %v (status %d)", err, response.StatusCode)
		}
		t.Fatalf("dial volume watch: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set volume watch read deadline: %v", err)
	}
	return conn
}

type volumeWatchResponse struct {
	Type    string `json:"type"`
	WatchID string `json:"watch_id"`
	Error   string `json:"error"`
}

func subscribeVolumeWatch(t *testing.T, conn *websocket.Conn, path string) volumeWatchResponse {
	t.Helper()
	if err := conn.WriteJSON(map[string]any{
		"action":    "subscribe",
		"path":      path,
		"recursive": true,
	}); err != nil {
		t.Fatalf("write volume watch subscribe: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set volume watch read deadline: %v", err)
	}
	var response volumeWatchResponse
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatalf("read volume watch subscribe response: %v", err)
	}
	return response
}

func unsubscribeVolumeWatch(t *testing.T, conn *websocket.Conn, watchID string) volumeWatchResponse {
	t.Helper()
	if err := conn.WriteJSON(map[string]any{
		"action":   "unsubscribe",
		"watch_id": watchID,
	}); err != nil {
		t.Fatalf("write volume watch unsubscribe: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set volume watch read deadline: %v", err)
	}
	var response volumeWatchResponse
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatalf("read volume watch unsubscribe response: %v", err)
	}
	return response
}

func waitForVolumeWatchCount(
	t *testing.T,
	guard *volumeWatchSubscriptionGuard,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		count := guard.count()
		if count == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("watch subscription count = %d, want %d", count, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHandleVolumeFileWatchProxyAdmitsOnlyOnOwner(t *testing.T) {
	ownerRepo := newFakeHTTPRepo()
	ownerRepo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-a"}
	ownerOperations := newTestStorageOperationQuota()
	ownerConnections := &testWatchActiveConnectionQuota{}
	ownerHub := &fakeVolumeWatchHub{}
	owner := &Server{
		logger:            logrus.New(),
		repo:              ownerRepo,
		storageOperations: ownerOperations,
		activeConnections: ownerConnections,
		eventHub:          ownerHub,
	}
	ownerHTTP := startVolumeWatchTestServer(t, owner)
	defer ownerHTTP.Close()

	sourceRepo := newFakeHTTPRepo()
	sourceRepo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-a"}
	sourceRepo.activeMounts["vol-1"] = []*db.VolumeMount{{
		VolumeID:  "vol-1",
		ClusterID: "cluster-a",
		PodID:     "owner-pod",
		MountedAt: time.Unix(10, 0),
	}}
	sourceOperations := newTestStorageOperationQuota()
	source := &Server{
		logger:            logrus.New(),
		cfg:               &config.StorageProxyConfig{HeartbeatTimeout: 15},
		repo:              sourceRepo,
		storageOperations: sourceOperations,
		podResolver: &fakeVolumeFilePodResolver{
			urls: map[string]string{"owner-pod": ownerHTTP.URL},
		},
		selfPodID:     "source-pod",
		selfClusterID: "cluster-a",
	}
	sourceHTTP := startVolumeWatchTestServer(t, source)
	defer sourceHTTP.Close()

	conn := dialVolumeWatch(t, sourceHTTP, "vol-1", "team-a")
	response := subscribeVolumeWatch(t, conn, "/docs")
	if response.Type != "subscribed" || response.WatchID == "" {
		t.Fatalf("subscribe response = %+v, want subscribed", response)
	}

	if teams := sourceOperations.admittedTeams(); len(teams) != 0 {
		t.Fatalf("source admissions = %#v, want none", teams)
	}
	if teams := ownerOperations.admittedTeams(); len(teams) != 2 || teams[0] != "team-a" || teams[1] != "team-a" {
		t.Fatalf("owner admissions = %#v, want two team-a admissions", teams)
	}
	if ownerConnections.acquisitionCount() != 1 || ownerConnections.usage("team-a") != 1 {
		t.Fatalf(
			"owner active connection state = acquisitions %d usage %d, want 1 and 1",
			ownerConnections.acquisitionCount(),
			ownerConnections.usage("team-a"),
		)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close proxied watch: %v", err)
	}
	waitForVolumeWatchCount(t, &owner.watchSubscriptions, 0)
	if ownerConnections.usage("team-a") != 0 {
		t.Fatalf("owner active connection usage = %d, want 0", ownerConnections.usage("team-a"))
	}
	_, cancellations := ownerHub.counts()
	if cancellations != 1 {
		t.Fatalf("owner watch cancellations = %d, want 1", cancellations)
	}
}

func TestHandleVolumeFileWatchSubscriptionAdmissionFailsClosedAndReleasesGuard(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-a"}
	operations := &stagedStorageOperationQuota{allowedCalls: 1}
	connections := &testWatchActiveConnectionQuota{}
	hub := &fakeVolumeWatchHub{}
	server := &Server{
		logger:            logrus.New(),
		repo:              repo,
		storageOperations: operations,
		activeConnections: connections,
		eventHub:          hub,
	}
	httpServer := startVolumeWatchTestServer(t, server)
	defer httpServer.Close()

	conn := dialVolumeWatch(t, httpServer, "vol-1", "team-a")
	response := subscribeVolumeWatch(t, conn, "/docs")
	if response.Type != "error" || !strings.Contains(response.Error, "storage operation rejected") {
		t.Fatalf("subscribe response = %+v, want storage operation error", response)
	}
	if operations.callCount() != 2 {
		t.Fatalf("storage operation calls = %d, want 2", operations.callCount())
	}
	if subscriptions, _ := hub.counts(); subscriptions != 0 {
		t.Fatalf("hub subscriptions = %d, want 0", subscriptions)
	}
	if connections.acquisitionCount() != 0 {
		t.Fatalf("active connection acquisitions = %d, want 0", connections.acquisitionCount())
	}
	waitForVolumeWatchCount(t, &server.watchSubscriptions, 0)
}

func TestHandleVolumeFileWatchEnforcesPerConnectionLimit(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-a"}
	operations := newTestStorageOperationQuota()
	connections := &testWatchActiveConnectionQuota{}
	hub := &fakeVolumeWatchHub{}
	server := &Server{
		logger:            logrus.New(),
		repo:              repo,
		storageOperations: operations,
		activeConnections: connections,
		eventHub:          hub,
	}
	httpServer := startVolumeWatchTestServer(t, server)
	defer httpServer.Close()

	conn := dialVolumeWatch(t, httpServer, "vol-1", "team-a")
	for index := 0; index < maxVolumeFileWatchSubscriptionsPerConnection; index++ {
		response := subscribeVolumeWatch(t, conn, fmt.Sprintf("/path-%d", index))
		if response.Type != "subscribed" {
			t.Fatalf("subscribe %d response = %+v, want subscribed", index, response)
		}
	}
	response := subscribeVolumeWatch(t, conn, "/one-too-many")
	if response.Type != "error" || !strings.Contains(response.Error, "maximum watch subscriptions") {
		t.Fatalf("overflow response = %+v, want connection-limit error", response)
	}
	if teams := operations.admittedTeams(); len(teams) != 1+maxVolumeFileWatchSubscriptionsPerConnection {
		t.Fatalf(
			"storage operation admissions = %d, want %d",
			len(teams),
			1+maxVolumeFileWatchSubscriptionsPerConnection,
		)
	}
	if subscriptions, _ := hub.counts(); subscriptions != maxVolumeFileWatchSubscriptionsPerConnection {
		t.Fatalf("hub subscriptions = %d, want %d", subscriptions, maxVolumeFileWatchSubscriptionsPerConnection)
	}
	if connections.acquisitionCount() != maxVolumeFileWatchSubscriptionsPerConnection {
		t.Fatalf(
			"active connection acquisitions = %d, want %d",
			connections.acquisitionCount(),
			maxVolumeFileWatchSubscriptionsPerConnection,
		)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close watch: %v", err)
	}
	waitForVolumeWatchCount(t, &server.watchSubscriptions, 0)
	if connections.usage("team-a") != 0 {
		t.Fatalf("active connection usage = %d, want 0", connections.usage("team-a"))
	}
	_, cancellations := hub.counts()
	if cancellations != maxVolumeFileWatchSubscriptionsPerConnection {
		t.Fatalf("watch cancellations = %d, want %d", cancellations, maxVolumeFileWatchSubscriptionsPerConnection)
	}
}

func TestHandleVolumeFileWatchEnforcesCrossConnectionTeamAndProcessLimits(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-a"] = &db.SandboxVolume{ID: "vol-a", TeamID: "team-a"}
	repo.volumes["vol-b"] = &db.SandboxVolume{ID: "vol-b", TeamID: "team-b"}
	repo.volumes["vol-c"] = &db.SandboxVolume{ID: "vol-c", TeamID: "team-c"}
	hub := &fakeVolumeWatchHub{}
	connections := &testWatchActiveConnectionQuota{maxPerTeam: 2}
	server := &Server{
		logger:            logrus.New(),
		repo:              repo,
		storageOperations: newTestStorageOperationQuota(),
		activeConnections: connections,
		eventHub:          hub,
		watchSubscriptions: volumeWatchSubscriptionGuard{
			maxGlobal: 3,
		},
	}
	httpServer := startVolumeWatchTestServer(t, server)
	defer httpServer.Close()

	teamAFirst := dialVolumeWatch(t, httpServer, "vol-a", "team-a")
	firstA := subscribeVolumeWatch(t, teamAFirst, "/first")
	if firstA.Type != "subscribed" {
		t.Fatalf("first team-a response = %+v, want subscribed", firstA)
	}
	teamASecond := dialVolumeWatch(t, httpServer, "vol-a", "team-a")
	secondA := subscribeVolumeWatch(t, teamASecond, "/second")
	if secondA.Type != "subscribed" {
		t.Fatalf("second team-a response = %+v, want subscribed", secondA)
	}
	teamLimit := subscribeVolumeWatch(t, teamASecond, "/team-overflow")
	if teamLimit.Type != "error" || !strings.Contains(teamLimit.Error, "active_connection_count") {
		t.Fatalf("team-limit response = %+v, want active connection quota limit", teamLimit)
	}

	teamB := dialVolumeWatch(t, httpServer, "vol-b", "team-b")
	firstB := subscribeVolumeWatch(t, teamB, "/third-global")
	if firstB.Type != "subscribed" {
		t.Fatalf("team-b response = %+v, want subscribed", firstB)
	}
	teamC := dialVolumeWatch(t, httpServer, "vol-c", "team-c")
	processLimit := subscribeVolumeWatch(t, teamC, "/global-overflow")
	if processLimit.Type != "error" || !strings.Contains(processLimit.Error, "process watch subscription limit") {
		t.Fatalf("process-limit response = %+v, want process limit", processLimit)
	}

	if err := teamAFirst.Close(); err != nil {
		t.Fatalf("close first team-a connection: %v", err)
	}
	waitForVolumeWatchCount(t, &server.watchSubscriptions, 2)
	if connections.usage("team-a") != 1 {
		t.Fatalf("team-a active connection usage = %d, want 1", connections.usage("team-a"))
	}
	replacement := subscribeVolumeWatch(t, teamC, "/replacement")
	if replacement.Type != "subscribed" {
		t.Fatalf("replacement response = %+v, want subscribed", replacement)
	}

	unsubscribed := unsubscribeVolumeWatch(t, teamASecond, secondA.WatchID)
	if unsubscribed.Type != "unsubscribed" || unsubscribed.WatchID != secondA.WatchID {
		t.Fatalf("unsubscribe response = %+v, want watch %q removed", unsubscribed, secondA.WatchID)
	}
	waitForVolumeWatchCount(t, &server.watchSubscriptions, 2)
	if connections.usage("team-a") != 0 {
		t.Fatalf("team-a active connection usage = %d, want 0", connections.usage("team-a"))
	}
}

func TestHandleVolumeFileWatchLeaseLossClosesSubscriptionAndAllowsReplacement(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-a"}
	hub := &fakeVolumeWatchHub{}
	connections := &testWatchActiveConnectionQuota{}
	server := &Server{
		logger:            logrus.New(),
		repo:              repo,
		storageOperations: newTestStorageOperationQuota(),
		activeConnections: connections,
		eventHub:          hub,
	}
	httpServer := startVolumeWatchTestServer(t, server)
	defer httpServer.Close()

	conn := dialVolumeWatch(t, httpServer, "vol-1", "team-a")
	first := subscribeVolumeWatch(t, conn, "/first")
	if first.Type != "subscribed" {
		t.Fatalf("first subscribe response = %+v, want subscribed", first)
	}
	lease := connections.leaseAt(0)
	if lease == nil {
		t.Fatal("first active connection lease is nil")
	}
	lease.lose(errors.New("redis lease lost"))

	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set lease-loss read deadline: %v", err)
	}
	var lost volumeWatchResponse
	if err := conn.ReadJSON(&lost); err != nil {
		t.Fatalf("read lease-loss response: %v", err)
	}
	if lost.Type != "error" || lost.WatchID != first.WatchID || !strings.Contains(lost.Error, "redis lease lost") {
		t.Fatalf("lease-loss response = %+v, want watch-specific error", lost)
	}
	waitForVolumeWatchCount(t, &server.watchSubscriptions, 0)
	if connections.usage("team-a") != 0 {
		t.Fatalf("active connection usage after lease loss = %d, want 0", connections.usage("team-a"))
	}
	if _, cancellations := hub.counts(); cancellations != 1 {
		t.Fatalf("watch cancellations after lease loss = %d, want 1", cancellations)
	}

	replacement := subscribeVolumeWatch(t, conn, "/replacement")
	if replacement.Type != "subscribed" {
		t.Fatalf("replacement response = %+v, want subscribed", replacement)
	}
	if connections.acquisitionCount() != 2 {
		t.Fatalf("active connection acquisitions = %d, want 2", connections.acquisitionCount())
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close volume watch connection: %v", err)
	}
	waitForVolumeWatchCount(t, &server.watchSubscriptions, 0)
	if usage := connections.usage("team-a"); usage != 0 {
		t.Fatalf("active connection usage after connection close = %d, want 0", usage)
	}
}

func TestHandleVolumeFileWatchHubClosureReleasesSubscription(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-a"}
	hub := &fakeVolumeWatchHub{}
	releaseStarted := make(chan struct{})
	releaseGate := make(chan struct{})
	connections := &testWatchActiveConnectionQuota{
		nextReleaseStarted: releaseStarted,
		nextReleaseGate:    releaseGate,
	}
	server := &Server{
		logger:            logrus.New(),
		repo:              repo,
		storageOperations: newTestStorageOperationQuota(),
		activeConnections: connections,
		eventHub:          hub,
	}
	httpServer := startVolumeWatchTestServer(t, server)
	defer httpServer.Close()

	conn := dialVolumeWatch(t, httpServer, "vol-1", "team-a")
	first := subscribeVolumeWatch(t, conn, "/first")
	if first.Type != "subscribed" {
		t.Fatalf("first subscribe response = %+v, want subscribed", first)
	}

	hub.closeSubscription(0)
	select {
	case <-releaseStarted:
	case <-time.After(time.Second):
		t.Fatal("active connection lease release did not start")
	}
	if subscriptions := server.watchSubscriptions.count(); subscriptions != 1 {
		t.Fatalf("watch subscription guard during lease release = %d, want 1", subscriptions)
	}
	if usage := connections.usage("team-a"); usage != 1 {
		t.Fatalf("active connection usage during lease release = %d, want 1", usage)
	}
	close(releaseGate)
	waitForVolumeWatchCount(t, &server.watchSubscriptions, 0)
	if connections.usage("team-a") != 0 {
		t.Fatalf("active connection usage after hub close = %d, want 0", connections.usage("team-a"))
	}

	replacement := subscribeVolumeWatch(t, conn, "/replacement")
	if replacement.Type != "subscribed" {
		t.Fatalf("replacement response = %+v, want subscribed", replacement)
	}
	if connections.acquisitionCount() != 2 {
		t.Fatalf("active connection acquisitions = %d, want 2", connections.acquisitionCount())
	}
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

type tarEntry struct {
	name     string
	body     []byte
	linkname string
	mode     int64
	typeflag byte
}

func tarArchive(t *testing.T, entries []tarEntry) *bytes.Reader {
	return bytes.NewReader(tarArchiveBytes(t, entries))
}

func tarArchiveBytes(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, entry := range entries {
		header := &tar.Header{
			Name:     entry.name,
			Mode:     entry.mode,
			Typeflag: entry.typeflag,
			Size:     int64(len(entry.body)),
			Linkname: entry.linkname,
		}
		if entry.typeflag == tar.TypeDir || entry.typeflag == tar.TypeSymlink {
			header.Size = 0
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatalf("WriteHeader(%q): %v", entry.name, err)
		}
		if len(entry.body) > 0 {
			if _, err := tw.Write(entry.body); err != nil {
				t.Fatalf("Write(%q): %v", entry.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close tar writer: %v", err)
	}
	return buf.Bytes()
}

func TestReadVolumeFileUsesSharedBarrier(t *testing.T) {
	barrier := &fakeHTTPSharedVolumeBarrier{}
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			switch {
			case req.Parent == 1 && req.Name == "docs":
				return &pb.NodeResponse{Inode: 2, Attr: volumeDirAttr()}, nil
			case req.Parent == 2 && req.Name == "report.txt":
				return &pb.NodeResponse{Inode: 3, Attr: volumeFileAttr(5)}, nil
			default:
				return nil, fserror.New(fserror.NotFound, "missing")
			}
		},
		openFunc: func(_ context.Context, _ *pb.OpenRequest) (*pb.OpenResponse, error) {
			return &pb.OpenResponse{HandleId: 7}, nil
		},
		readFunc: func(_ context.Context, _ *pb.ReadRequest) (*pb.ReadResponse, error) {
			return &pb.ReadResponse{Data: []byte("hello")}, nil
		},
		releaseFunc: func(_ context.Context, _ *pb.ReleaseRequest) (*pb.Empty, error) {
			return &pb.Empty{}, nil
		},
	}
	server, _ := newVolumeFileTestServerWithBarrier(fileRPC, barrier)

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/files?path=/docs/report.txt", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileOperation(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if body := recorder.Body.String(); body != "hello" {
		t.Fatalf("body = %q, want %q", body, "hello")
	}
	if barrier.sharedCalls != 1 {
		t.Fatalf("shared calls = %d, want 1", barrier.sharedCalls)
	}
	if barrier.exclusiveCalls != 0 {
		t.Fatalf("exclusive calls = %d, want 0", barrier.exclusiveCalls)
	}
	if barrier.lastVolumeID != "vol-1" {
		t.Fatalf("barrier volume = %q, want %q", barrier.lastVolumeID, "vol-1")
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

func TestWriteVolumeFileSkipsSyncWhenContentUnchanged(t *testing.T) {
	const payload = "hello world"
	var wrote *pb.WriteRequest
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			if req.Parent == 1 && req.Name == "hello.txt" {
				return &pb.NodeResponse{Inode: 3, Attr: volumeFileAttr(len(payload))}, nil
			}
			return nil, fserror.New(fserror.NotFound, "missing")
		},
		openFunc: func(_ context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
			return &pb.OpenResponse{HandleId: 15}, nil
		},
		readFunc: func(_ context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
			return &pb.ReadResponse{Data: []byte(payload)}, nil
		},
		writeFunc: func(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
			wrote = req
			return &pb.WriteResponse{BytesWritten: int64(len(req.Data))}, nil
		},
	}
	server, volMgr := newVolumeFileTestServer(fileRPC)

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/files?path=/hello.txt", bytes.NewReader([]byte(payload)))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileOperation(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if wrote != nil {
		t.Fatalf("unexpected write request: %+v", wrote)
	}
	if volMgr.syncCalls != 0 {
		t.Fatalf("SyncDirectVolumeFileMount() calls = %d, want 0", volMgr.syncCalls)
	}
}

func TestHandleVolumeFileArchiveImportExtractsTarAndSyncsOnce(t *testing.T) {
	type node struct {
		inode uint64
		attr  *pb.GetAttrResponse
	}
	nodes := map[string]node{}
	nextInode := uint64(2)
	var writes [][]byte
	var symlink *pb.SymlinkRequest
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			if n, ok := nodes[strconv.FormatUint(req.Parent, 10)+"/"+req.Name]; ok {
				return &pb.NodeResponse{Inode: n.inode, Attr: n.attr}, nil
			}
			return nil, fserror.New(fserror.NotFound, "missing")
		},
		mkdirFunc: func(_ context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
			inode := nextInode
			nextInode++
			nodes[strconv.FormatUint(req.Parent, 10)+"/"+req.Name] = node{inode: inode, attr: volumeDirAttr()}
			return &pb.NodeResponse{Inode: inode, Attr: volumeDirAttr()}, nil
		},
		createFunc: func(_ context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
			inode := nextInode
			nextInode++
			nodes[strconv.FormatUint(req.Parent, 10)+"/"+req.Name] = node{inode: inode, attr: volumeFileAttr(0)}
			return &pb.NodeResponse{Inode: inode, Attr: volumeFileAttr(0), HandleId: 99}, nil
		},
		writeFunc: func(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
			writes = append(writes, append([]byte(nil), req.Data...))
			return &pb.WriteResponse{BytesWritten: int64(len(req.Data))}, nil
		},
		symlinkFunc: func(_ context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
			symlink = req
			return &pb.NodeResponse{Inode: nextInode, Attr: &pb.GetAttrResponse{Mode: uint32(syscall.S_IFLNK | 0o777)}}, nil
		},
	}
	server, volMgr := newVolumeFileTestServer(fileRPC)

	req := httptest.NewRequest(http.MethodPut, "/sandboxvolumes/vol-1/files/archive?path=/", tarArchive(t, []tarEntry{
		{name: "dir/", mode: 0o755, typeflag: tar.TypeDir},
		{name: "dir/hello.txt", body: []byte("hello archive"), mode: 0o644, typeflag: tar.TypeReg},
		{name: "dir/link.txt", linkname: "hello.txt", mode: 0o777, typeflag: tar.TypeSymlink},
	}))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileArchiveImport(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if len(writes) != 1 || string(writes[0]) != "hello archive" {
		t.Fatalf("writes = %q, want one archive file write", writes)
	}
	if symlink == nil || symlink.Name != "link.txt" || symlink.Target != "hello.txt" {
		t.Fatalf("symlink = %+v, want link.txt -> hello.txt", symlink)
	}
	if volMgr.syncCalls != 1 || volMgr.lastSyncVolume != "vol-1" {
		t.Fatalf("SyncDirectVolumeFileMount() got calls=%d volume=%q, want 1 vol-1", volMgr.syncCalls, volMgr.lastSyncVolume)
	}
	var envelope struct {
		Data volumeFileArchiveImportResponse `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Data.Files != 1 || envelope.Data.Directories != 1 || envelope.Data.Symlinks != 1 || envelope.Data.Bytes != int64(len("hello archive")) {
		t.Fatalf("archive result = %+v, want counts for dir/file/symlink", envelope.Data)
	}
}

func TestHandleVolumeFileArchiveImportCachesDirectoryLookups(t *testing.T) {
	type node struct {
		inode uint64
		attr  *pb.GetAttrResponse
	}
	nodes := map[string]node{}
	lookupCalls := map[string]int{}
	nextInode := uint64(2)
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			key := strconv.FormatUint(req.Parent, 10) + "/" + req.Name
			lookupCalls[key]++
			if n, ok := nodes[key]; ok {
				return &pb.NodeResponse{Inode: n.inode, Attr: n.attr}, nil
			}
			return nil, fserror.New(fserror.NotFound, "missing")
		},
		mkdirFunc: func(_ context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
			inode := nextInode
			nextInode++
			nodes[strconv.FormatUint(req.Parent, 10)+"/"+req.Name] = node{inode: inode, attr: volumeDirAttr()}
			return &pb.NodeResponse{Inode: inode, Attr: volumeDirAttr()}, nil
		},
		createFunc: func(_ context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
			inode := nextInode
			nextInode++
			nodes[strconv.FormatUint(req.Parent, 10)+"/"+req.Name] = node{inode: inode, attr: volumeFileAttr(0)}
			return &pb.NodeResponse{Inode: inode, Attr: volumeFileAttr(0), HandleId: inode + 100}, nil
		},
		writeFunc: func(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
			return &pb.WriteResponse{BytesWritten: int64(len(req.Data))}, nil
		},
	}
	server, _ := newVolumeFileTestServer(fileRPC)

	req := httptest.NewRequest(http.MethodPut, "/sandboxvolumes/vol-1/files/archive?path=/", tarArchive(t, []tarEntry{
		{name: "assets/chunks/a.js", body: []byte("a"), mode: 0o644, typeflag: tar.TypeReg},
		{name: "assets/chunks/b.js", body: []byte("b"), mode: 0o644, typeflag: tar.TypeReg},
		{name: "assets/chunks/c.js", body: []byte("c"), mode: 0o644, typeflag: tar.TypeReg},
	}))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileArchiveImport(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if lookupCalls["1/assets"] != 1 {
		t.Fatalf("assets lookup calls = %d, want 1", lookupCalls["1/assets"])
	}
	assets := nodes["1/assets"].inode
	chunksKey := strconv.FormatUint(assets, 10) + "/chunks"
	if lookupCalls[chunksKey] != 1 {
		t.Fatalf("chunks lookup calls = %d, want 1", lookupCalls[chunksKey])
	}
}

func TestHandleVolumeFileArchiveImportRejectsTraversal(t *testing.T) {
	server, volMgr := newVolumeFileTestServer(&fakeHTTPVolumeFileRPC{})

	req := httptest.NewRequest(http.MethodPut, "/sandboxvolumes/vol-1/files/archive?path=/", tarArchive(t, []tarEntry{
		{name: "../escape.txt", body: []byte("nope"), mode: 0o644, typeflag: tar.TypeReg},
	}))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileArchiveImport(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if volMgr.syncCalls != 0 {
		t.Fatalf("SyncDirectVolumeFileMount() calls = %d, want 0", volMgr.syncCalls)
	}
}

func TestWriteVolumeFileMkdirRecursiveExistingDirSkipsSync(t *testing.T) {
	fileRPC := &fakeHTTPVolumeFileRPC{
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			if req.Parent == 1 && req.Name == "skills" {
				return &pb.NodeResponse{Inode: 3, Attr: volumeDirAttr()}, nil
			}
			return nil, fserror.New(fserror.NotFound, "missing")
		},
	}
	server, volMgr := newVolumeFileTestServer(fileRPC)

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/files?path=/skills&mkdir=true&recursive=true", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileOperation(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	if volMgr.syncCalls != 0 {
		t.Fatalf("SyncDirectVolumeFileMount() calls = %d, want 0", volMgr.syncCalls)
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

func TestHandleVolumeFileMoveProxiesBodyToCtldOwner(t *testing.T) {
	remoteSeen := make(chan struct {
		header http.Header
		body   []byte
	}, 1)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read proxied body: %v", err)
		}
		select {
		case remoteSeen <- struct {
			header http.Header
			body   []byte
		}{
			header: r.Header.Clone(),
			body:   body,
		}:
		default:
		}
		_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"moved": true})
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
			VolumeID:  "vol-1",
			ClusterID: "cluster-a",
			PodID:     "ctld-node/node-a",
			MountedAt: time.Unix(10, 0),
			MountOptions: mustMountOptionsRaw(t, volume.MountOptions{
				AccessMode:   volume.AccessModeRWO,
				OwnerKind:    volume.OwnerKindCtld,
				OwnerPort:    remotePort,
				NodeName:     "node-a",
				PodNamespace: "sandbox0-system",
			}),
		},
	}
	server.ctldResolver = fakeCtldResolver{url: "http://127.0.0.1"}

	reqBody := []byte(`{"source":"/docs/report.txt","destination":"/archive/report-old.txt"}`)
	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/files/move", bytes.NewReader(reqBody))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileMove(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if volMgr.acquireCalls != 0 {
		t.Fatalf("AcquireDirectVolumeFileMount() calls = %d, want 0", volMgr.acquireCalls)
	}
	select {
	case seen := <-remoteSeen:
		if got := seen.header.Get(volumeFileAffinityRoutedPodHeader); got != "ctld-node/node-a" {
			t.Fatalf("routed pod header = %q, want %q", got, "ctld-node/node-a")
		}
		if got := seen.header.Get(volumeFileAffinityTeamHeader); got != "team-a" {
			t.Fatalf("team header = %q, want %q", got, "team-a")
		}
		if !bytes.Equal(seen.body, reqBody) {
			t.Fatalf("proxied body = %q, want %q", string(seen.body), string(reqBody))
		}
	default:
		t.Fatal("expected move request to be proxied to ctld owner")
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

func TestHandleVolumeFileStatRejectsUnownedS3Backend(t *testing.T) {
	fileRPC := &fakeHTTPVolumeFileRPC{}
	server, volMgr := newVolumeFileTestServer(fileRPC)
	repo := server.repo.(*fakeHTTPRepo)
	repo.volumes["vol-1"].Backend = volume.BackendS3
	repo.volumes["vol-1"].AccessMode = string(volume.AccessModeROX)

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/files/stat?path=/docs/report.txt", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileStat(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	if volMgr.acquireCalls != 0 {
		t.Fatalf("AcquireDirectVolumeFileMount() calls = %d, want 0", volMgr.acquireCalls)
	}
}

func TestHandleVolumeFileStatProxiesMountedS3BackendToCtldOwner(t *testing.T) {
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
	repo.volumes["vol-1"].Backend = volume.BackendS3
	repo.volumes["vol-1"].AccessMode = string(volume.AccessModeRWO)
	repo.activeMounts["vol-1"] = []*db.VolumeMount{
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
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if volMgr.acquireCalls != 0 {
		t.Fatalf("AcquireDirectVolumeFileMount() calls = %d, want 0", volMgr.acquireCalls)
	}
	select {
	case seen := <-remoteSeen:
		if got := seen.Header.Get(volumeFileAffinityRoutedPodHeader); got != "sandbox0-system/ctld-node-a" {
			t.Fatalf("routed pod header = %q, want %q", got, "sandbox0-system/ctld-node-a")
		}
	default:
		t.Fatal("expected request to be proxied to mounted s3 ctld owner")
	}
}

func TestHandleVolumeFileStatPrefersStorageProxyOwnerOverCtld(t *testing.T) {
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
			PodID:        "remote-storage-proxy",
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
		urls: map[string]string{
			"remote-storage-proxy":        remote.URL,
			"sandbox0-system/ctld-node-a": "http://127.0.0.1",
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
	if volMgr.acquireCalls != 0 {
		t.Fatalf("AcquireDirectVolumeFileMount() calls = %d, want 0", volMgr.acquireCalls)
	}
	select {
	case seen := <-remoteSeen:
		if got := seen.Header.Get(volumeFileAffinityRoutedPodHeader); got != "remote-storage-proxy" {
			t.Fatalf("routed pod header = %q, want %q", got, "remote-storage-proxy")
		}
		if got := seen.Header.Get(volumeFileAffinityTeamHeader); got != "team-a" {
			t.Fatalf("team header = %q, want %q", got, "team-a")
		}
	default:
		t.Fatal("expected request to be proxied to storage-proxy owner")
	}
}

func TestHandleVolumeFileArchiveImportPrefersStorageProxyOwnerOverCtld(t *testing.T) {
	type seenRequest struct {
		header http.Header
		body   []byte
	}
	remoteSeen := make(chan seenRequest, 1)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read proxied archive: %v", err)
		}
		select {
		case remoteSeen <- seenRequest{header: r.Header.Clone(), body: body}:
		default:
		}
		_ = spec.WriteSuccess(w, http.StatusOK, volumeFileArchiveImportResponse{Files: 1, Bytes: 5})
	}))
	defer remote.Close()
	ctldSeen := make(chan struct{}, 1)
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case ctldSeen <- struct{}{}:
		default:
		}
		_ = spec.WriteSuccess(w, http.StatusOK, volumeFileArchiveImportResponse{Files: 1, Bytes: 5})
	}))
	defer ctld.Close()
	ctldURL, err := url.Parse(ctld.URL)
	if err != nil {
		t.Fatalf("parse ctld url: %v", err)
	}
	ctldPort, err := strconv.Atoi(ctldURL.Port())
	if err != nil {
		t.Fatalf("parse ctld port: %v", err)
	}

	fileRPC := &fakeHTTPVolumeFileRPC{}
	server, volMgr := newVolumeFileTestServer(fileRPC)
	repo := server.repo.(*fakeHTTPRepo)
	repo.activeMounts["vol-1"] = []*db.VolumeMount{
		{
			VolumeID:     "vol-1",
			ClusterID:    "cluster-a",
			PodID:        "remote-storage-proxy",
			MountedAt:    time.Unix(20, 0),
			MountOptions: mustMountOptionsRaw(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindStorageProxy}),
		},
		{
			VolumeID:     "vol-1",
			ClusterID:    "cluster-a",
			PodID:        "sandbox0-system/ctld-node-a",
			MountedAt:    time.Unix(10, 0),
			MountOptions: mustMountOptionsRaw(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindCtld, OwnerPort: ctldPort}),
		},
	}
	server.podResolver = &fakeVolumeFilePodResolver{
		urls: map[string]string{
			"remote-storage-proxy":        remote.URL,
			"sandbox0-system/ctld-node-a": "http://127.0.0.1",
		},
	}

	body := tarArchiveBytes(t, []tarEntry{{name: "skill/SKILL.md", body: []byte("skill"), mode: 0o644, typeflag: tar.TypeReg}})
	req := httptest.NewRequest(http.MethodPut, "/sandboxvolumes/vol-1/files/archive?path=/.pi/skills", bytes.NewReader(body))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileArchiveImport(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if volMgr.acquireCalls != 0 {
		t.Fatalf("AcquireDirectVolumeFileMount() calls = %d, want 0", volMgr.acquireCalls)
	}
	select {
	case seen := <-remoteSeen:
		if got := seen.header.Get(volumeFileAffinityRoutedPodHeader); got != "remote-storage-proxy" {
			t.Fatalf("routed pod header = %q, want %q", got, "remote-storage-proxy")
		}
		if got := seen.header.Get(volumeFileAffinityTeamHeader); got != "team-a" {
			t.Fatalf("team header = %q, want %q", got, "team-a")
		}
		if !bytes.Equal(seen.body, body) {
			t.Fatalf("proxied archive body changed")
		}
	default:
		t.Fatal("expected archive import to be proxied to storage-proxy owner")
	}
	select {
	case <-ctldSeen:
		t.Fatal("archive import was proxied to ctld owner")
	default:
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
