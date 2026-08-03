package rootfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfscow"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

var (
	ErrNotFound   = errors.New("rootfs target not found")
	ErrConflict   = errors.New("rootfs validation conflict")
	ErrBadRequest = errors.New("invalid rootfs request")
)

type Runtime interface {
	Inspect(ctx context.Context, target ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error)
	RootFSUpperdir(ctx context.Context, info ctldapi.RootFSInfo) (string, error)
}

type PortalResolver interface {
	RootFSPortalPaths(podUID string) []ctldapi.RootFSPortalPath
}

type Config struct {
	Runtime           Runtime
	Store             objectstore.Store
	OperationTimeout  time.Duration
	PortalResolver    PortalResolver
	SnapshotDir       string
	Observer          *Observer
	ActivePodUIDs     func(context.Context) (map[string]struct{}, error)
	SessionSweepEvery time.Duration
}

type trackedSession struct {
	session *rootfscow.Session
	podUID  string
}

type Controller struct {
	runtime           Runtime
	store             objectstore.Store
	operationTimeout  time.Duration
	portalResolver    PortalResolver
	snapshotDir       string
	observer          *Observer
	activePodUIDs     func(context.Context) (map[string]struct{}, error)
	sessionSweepEvery time.Duration
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup

	mu       sync.Mutex
	sessions map[string]trackedSession
}

func NewController(cfg Config) *Controller {
	timeout := cfg.OperationTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	controllerCtx, cancel := context.WithCancel(context.Background())
	controller := &Controller{
		runtime:           cfg.Runtime,
		store:             cfg.Store,
		operationTimeout:  timeout,
		portalResolver:    cfg.PortalResolver,
		snapshotDir:       cfg.SnapshotDir,
		observer:          cfg.Observer,
		activePodUIDs:     cfg.ActivePodUIDs,
		sessionSweepEvery: cfg.SessionSweepEvery,
		ctx:               controllerCtx,
		cancel:            cancel,
		sessions:          make(map[string]trackedSession),
	}
	if controller.activePodUIDs != nil {
		if controller.sessionSweepEvery <= 0 {
			controller.sessionSweepEvery = time.Minute
		}
		controller.wg.Add(1)
		go controller.sweepSessions()
	}
	return controller
}

func (c *Controller) Close() error {
	if c == nil {
		return nil
	}
	c.cancel()
	c.wg.Wait()
	c.mu.Lock()
	sessions := c.sessions
	c.sessions = make(map[string]trackedSession)
	c.mu.Unlock()
	var closeErr error
	for _, tracked := range sessions {
		closeErr = errors.Join(closeErr, tracked.session.Close())
	}
	return closeErr
}

func (c *Controller) sweepSessions() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.sessionSweepEvery)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.removeInactiveSessions()
		}
	}
}

func (c *Controller) removeInactiveSessions() {
	ctx, cancel := context.WithTimeout(c.ctx, min(c.sessionSweepEvery, 30*time.Second))
	defer cancel()
	active, err := c.activePodUIDs(ctx)
	if err != nil {
		return
	}
	var stale []*rootfscow.Session
	c.mu.Lock()
	for key, tracked := range c.sessions {
		if tracked.podUID == "" {
			continue
		}
		if _, ok := active[tracked.podUID]; ok {
			continue
		}
		delete(c.sessions, key)
		stale = append(stale, tracked.session)
	}
	c.mu.Unlock()
	for _, session := range stale {
		_ = session.Close()
	}
}

func (c *Controller) InspectRootFS(r *http.Request, req ctldapi.InspectRootFSRequest) (ctldapi.InspectRootFSResponse, int) {
	if err := validateTarget(req.Target); err != nil {
		return ctldapi.InspectRootFSResponse{Error: err.Error()}, http.StatusBadRequest
	}
	info, err := c.inspect(requestContext(r), req.Target)
	if err != nil {
		return ctldapi.InspectRootFSResponse{Error: err.Error()}, statusForError(err)
	}
	return ctldapi.InspectRootFSResponse{Info: info}, http.StatusOK
}

// BindRootFSSync starts continuous persistence for one active writable overlay.
// It is intentionally called after claim and before procd is released to user
// workloads, so large files are uploaded while the sandbox is running instead
// of extending the pause critical path.
func (c *Controller) BindRootFSSync(r *http.Request, req ctldapi.BindRootFSSyncRequest) (ctldapi.BindRootFSSyncResponse, int) {
	if c.store == nil {
		return ctldapi.BindRootFSSyncResponse{Error: "rootfs object store is not configured"}, http.StatusNotImplemented
	}
	if err := validateSyncIdentity(req.Target, req.SandboxID, req.TeamID, req.FilesystemID); err != nil {
		return ctldapi.BindRootFSSyncResponse{Error: err.Error()}, http.StatusBadRequest
	}
	ctx, cancel := c.operationContext(requestContext(r))
	defer cancel()
	inspected, err := c.inspect(ctx, req.Target)
	if err != nil {
		return ctldapi.BindRootFSSyncResponse{Error: err.Error()}, statusForError(err)
	}
	if err := validateSupportedRuntime(inspected); err != nil {
		return ctldapi.BindRootFSSyncResponse{Error: err.Error()}, statusForError(err)
	}
	key := rootFSSessionKey(inspected)
	c.mu.Lock()
	existing := c.sessions[key].session
	c.mu.Unlock()
	if existing != nil {
		return ctldapi.BindRootFSSyncResponse{Bound: true}, http.StatusOK
	}
	info, session, err := c.newSession(ctx, sessionRequest{
		Target:        req.Target,
		SandboxID:     req.SandboxID,
		TeamID:        req.TeamID,
		FilesystemID:  req.FilesystemID,
		Parent:        req.Parent,
		ExcludedPaths: req.ExcludedPaths,
		PortalPaths:   req.PortalPaths,
	})
	if err != nil {
		return ctldapi.BindRootFSSyncResponse{Error: err.Error()}, statusForError(err)
	}
	key = rootFSSessionKey(info)
	c.mu.Lock()
	existing = c.sessions[key].session
	if existing == nil {
		c.sessions[key] = trackedSession{session: session, podUID: strings.TrimSpace(req.Target.PodUID)}
	}
	c.mu.Unlock()
	if existing != nil {
		_ = session.Close()
	}
	return ctldapi.BindRootFSSyncResponse{Bound: true}, http.StatusOK
}

// SaveRootFS is kept as a compatibility wrapper around the transactional API.
func (c *Controller) SaveRootFS(r *http.Request, req ctldapi.SaveRootFSRequest) (ctldapi.SaveRootFSResponse, int) {
	headID := strings.TrimSpace(req.HeadID)
	if headID == "" {
		headID = uuid.NewString()
	}
	filesystemID := strings.TrimSpace(req.FilesystemID)
	if filesystemID == "" {
		filesystemID = strings.TrimSpace(req.SandboxID)
	}
	prepared, status := c.PrepareRootFSSnapshot(r, ctldapi.PrepareRootFSSnapshotRequest{
		Target:        req.Target,
		HeadID:        headID,
		SandboxID:     req.SandboxID,
		TeamID:        req.TeamID,
		FilesystemID:  filesystemID,
		Parent:        req.Parent,
		ExcludedPaths: req.ExcludedPaths,
		PortalPaths:   req.PortalPaths,
	})
	if status != http.StatusOK {
		return ctldapi.SaveRootFSResponse{Info: prepared.Info, Checkpoint: prepared.Checkpoint, Error: prepared.Error}, status
	}
	published, status := c.PublishRootFSSnapshot(r, ctldapi.PublishRootFSSnapshotRequest{Handle: prepared.Handle})
	if status != http.StatusOK {
		_, _ = c.AbortRootFSSnapshot(r, ctldapi.AbortRootFSSnapshotRequest{Handle: prepared.Handle})
		return ctldapi.SaveRootFSResponse{Info: published.Info, Checkpoint: published.Checkpoint, Error: published.Error}, status
	}
	return ctldapi.SaveRootFSResponse{Info: published.Info, Checkpoint: published.Checkpoint}, http.StatusOK
}

func (c *Controller) PrepareRootFSSnapshot(r *http.Request, req ctldapi.PrepareRootFSSnapshotRequest) (response ctldapi.PrepareRootFSSnapshotResponse, status int) {
	started := time.Now()
	defer func() {
		c.observer.ObserveOperation("seal", req.Target, -1, response.Checkpoint.CreatedBytes, -1, started, status, response.Error)
	}()
	if c.store == nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Error: "rootfs object store is not configured"}, http.StatusNotImplemented
	}
	if strings.TrimSpace(req.HeadID) == "" {
		return ctldapi.PrepareRootFSSnapshotResponse{Error: fmt.Sprintf("%v: head_id is required", ErrBadRequest)}, http.StatusBadRequest
	}
	if err := validateSyncIdentity(req.Target, req.SandboxID, req.TeamID, req.FilesystemID); err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Error: err.Error()}, http.StatusBadRequest
	}
	ctx, cancel := c.operationContext(requestContext(r))
	defer cancel()
	info, err := c.inspect(ctx, req.Target)
	if err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Error: err.Error()}, statusForError(err)
	}
	if err := validateSupportedRuntime(info); err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Info: info, Error: err.Error()}, http.StatusBadRequest
	}
	key := rootFSSessionKey(info)
	c.mu.Lock()
	session := c.sessions[key].session
	c.mu.Unlock()
	if session == nil {
		// A failover or ctld restart loses only the in-memory watcher. Rebuild the
		// session from the active upper and synchronously drain it before sealing.
		var createErr error
		info, session, createErr = c.newSession(ctx, sessionRequest{
			Target:        req.Target,
			SandboxID:     req.SandboxID,
			TeamID:        req.TeamID,
			FilesystemID:  req.FilesystemID,
			Parent:        req.Parent,
			ExcludedPaths: req.ExcludedPaths,
			PortalPaths:   req.PortalPaths,
		})
		if createErr != nil {
			return ctldapi.PrepareRootFSSnapshotResponse{Info: info, Error: createErr.Error()}, statusForError(createErr)
		}
		c.mu.Lock()
		if existing := c.sessions[key].session; existing == nil {
			c.sessions[key] = trackedSession{session: session, podUID: strings.TrimSpace(req.Target.PodUID)}
		} else {
			_ = session.Close()
			session = existing
		}
		c.mu.Unlock()
	}
	result, err := session.Seal(ctx, req.HeadID)
	if err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Info: info, Error: fmt.Sprintf("seal rootfs head: %v", err)}, statusForError(err)
	}
	// Seal terminally drains the generation before returning. Forget it now so
	// an aborted prepare or lifecycle retry creates a fresh watcher instead of
	// reusing a closed session.
	c.mu.Lock()
	if tracked, ok := c.sessions[key]; ok && tracked.session == session {
		delete(c.sessions, key)
	}
	c.mu.Unlock()
	checkpoint := checkpointDescriptor(result)
	handle := uuid.NewString()
	if err := c.writePreparedSnapshot(handle, preparedRootFSSnapshot{
		Handle:     handle,
		Info:       info,
		Checkpoint: checkpoint,
		CreatedAt:  time.Now().UTC(),
	}); err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Info: info, Checkpoint: checkpoint, Error: fmt.Sprintf("persist prepared rootfs head: %v", err)}, http.StatusInternalServerError
	}
	return ctldapi.PrepareRootFSSnapshotResponse{Handle: handle, Info: info, Checkpoint: checkpoint}, http.StatusOK
}

func (c *Controller) PublishRootFSSnapshot(_ *http.Request, req ctldapi.PublishRootFSSnapshotRequest) (ctldapi.PublishRootFSSnapshotResponse, int) {
	prepared, err := c.readPreparedSnapshot(req.Handle)
	if err != nil {
		return ctldapi.PublishRootFSSnapshotResponse{Error: err.Error()}, statusForError(err)
	}
	if err := c.removePreparedSnapshot(req.Handle); err != nil {
		return ctldapi.PublishRootFSSnapshotResponse{Info: prepared.Info, Checkpoint: prepared.Checkpoint, Error: err.Error()}, statusForError(err)
	}
	return ctldapi.PublishRootFSSnapshotResponse{Info: prepared.Info, Checkpoint: prepared.Checkpoint, Published: true}, http.StatusOK
}

func (c *Controller) AbortRootFSSnapshot(_ *http.Request, req ctldapi.AbortRootFSSnapshotRequest) (ctldapi.AbortRootFSSnapshotResponse, int) {
	if err := c.removePreparedSnapshot(req.Handle); err != nil && !errors.Is(err, os.ErrNotExist) {
		return ctldapi.AbortRootFSSnapshotResponse{Error: err.Error()}, statusForError(err)
	}
	return ctldapi.AbortRootFSSnapshotResponse{Aborted: true}, http.StatusOK
}

type sessionRequest struct {
	Target        ctldapi.RootFSContainerRef
	SandboxID     string
	TeamID        string
	FilesystemID  string
	Parent        *rootfshead.HeadReference
	ExcludedPaths []string
	PortalPaths   []ctldapi.RootFSPortalPath
}

func (c *Controller) newSession(ctx context.Context, req sessionRequest) (ctldapi.RootFSInfo, *rootfscow.Session, error) {
	info, err := c.inspect(ctx, req.Target)
	if err != nil {
		return ctldapi.RootFSInfo{}, nil, err
	}
	if err := validateSupportedRuntime(info); err != nil {
		return info, nil, err
	}
	upperdir, err := c.runtime.RootFSUpperdir(ctx, info)
	if err != nil {
		return info, nil, fmt.Errorf("resolve rootfs upperdir: %w", err)
	}
	var parent *rootfshead.Head
	if req.Parent != nil {
		loaded, err := rootfscow.LoadHead(ctx, c.store, *req.Parent)
		if err != nil {
			return info, nil, fmt.Errorf("load parent rootfs head: %w", err)
		}
		parent = &loaded
	}
	portalPaths := c.portalPathsForRequest(info, req.Target, req.ExcludedPaths, req.PortalPaths)
	session, err := rootfscow.NewSession(c.ctx, rootfscow.SessionConfig{
		Root:            upperdir,
		GenerationID:    rootFSSessionKey(info),
		TeamID:          req.TeamID,
		FilesystemID:    req.FilesystemID,
		BaseImageDigest: info.BaseImageDigest,
		BaseSnapshotKey: info.SnapshotParent,
		Parent:          parent,
		ExcludedPaths:   rootFSExcludedPathsWithPortals(req.ExcludedPaths, portalPaths),
		PortalPaths:     portalPaths,
		Store:           c.store,
	})
	if err != nil {
		return info, nil, fmt.Errorf("start rootfs sync: %w", err)
	}
	return info, session, nil
}

func checkpointDescriptor(result *rootfscow.SealResult) ctldapi.RootFSCheckpointDescriptor {
	if result == nil {
		return ctldapi.RootFSCheckpointDescriptor{}
	}
	return ctldapi.RootFSCheckpointDescriptor{
		Reference:          result.Reference,
		Objects:            append([]rootfshead.Object(nil), result.Objects...),
		CreatedBytes:       result.CreatedBytes,
		CreatedObjectCount: result.CreatedObjectCount,
		DirtyPaths:         result.DirtyPaths,
		SealDurationMS:     result.Duration.Milliseconds(),
	}
}

type preparedRootFSSnapshot struct {
	Handle     string                             `json:"handle"`
	Info       ctldapi.RootFSInfo                 `json:"info"`
	Checkpoint ctldapi.RootFSCheckpointDescriptor `json:"checkpoint"`
	CreatedAt  time.Time                          `json:"created_at"`
}

func (c *Controller) writePreparedSnapshot(handle string, prepared preparedRootFSSnapshot) error {
	handle = strings.TrimSpace(handle)
	if handle == "" || filepath.Base(handle) != handle {
		return fmt.Errorf("%w: snapshot handle is invalid", ErrBadRequest)
	}
	if err := os.MkdirAll(c.preparedSnapshotDir(), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(c.preparedSnapshotDir(), ".rootfs-head-*.tmp")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := json.NewEncoder(temporary).Encode(prepared); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, c.preparedSnapshotMetaPath(handle))
}

func (c *Controller) readPreparedSnapshot(handle string) (preparedRootFSSnapshot, error) {
	handle = strings.TrimSpace(handle)
	if handle == "" || filepath.Base(handle) != handle {
		return preparedRootFSSnapshot{}, fmt.Errorf("%w: snapshot handle is required", ErrBadRequest)
	}
	metaFile, err := os.Open(c.preparedSnapshotMetaPath(handle))
	if err != nil {
		if os.IsNotExist(err) {
			return preparedRootFSSnapshot{}, fmt.Errorf("%w: rootfs snapshot handle %s", ErrNotFound, handle)
		}
		return preparedRootFSSnapshot{}, err
	}
	defer metaFile.Close()
	var prepared preparedRootFSSnapshot
	if err := json.NewDecoder(metaFile).Decode(&prepared); err != nil {
		return preparedRootFSSnapshot{}, err
	}
	if err := prepared.Checkpoint.Reference.Validate(); err != nil {
		return preparedRootFSSnapshot{}, fmt.Errorf("invalid prepared rootfs head: %w", err)
	}
	return prepared, nil
}

func (c *Controller) removePreparedSnapshot(handle string) error {
	handle = strings.TrimSpace(handle)
	if handle == "" || filepath.Base(handle) != handle {
		return nil
	}
	return os.Remove(c.preparedSnapshotMetaPath(handle))
}

func (c *Controller) preparedSnapshotMetaPath(handle string) string {
	return filepath.Join(c.preparedSnapshotDir(), filepath.Base(handle)+".json")
}

func (c *Controller) preparedSnapshotDir() string {
	if c != nil && strings.TrimSpace(c.snapshotDir) != "" {
		return c.snapshotDir
	}
	return filepath.Join(os.TempDir(), "sandbox0-rootfs-snapshots")
}

func (c *Controller) portalPathsForRequest(info ctldapi.RootFSInfo, target ctldapi.RootFSContainerRef, excludedPaths []string, requested []ctldapi.RootFSPortalPath) []ctldapi.RootFSPortalPath {
	podUID := strings.TrimSpace(info.PodUID)
	if podUID == "" {
		podUID = strings.TrimSpace(target.PodUID)
	}
	paths := append([]ctldapi.RootFSPortalPath(nil), requested...)
	if podUID != "" && c != nil && c.portalResolver != nil {
		paths = append(paths, c.portalResolver.RootFSPortalPaths(podUID)...)
	}
	return filterRootFSPortalPaths(paths, excludedPaths)
}

func (c *Controller) inspect(ctx context.Context, target ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error) {
	if c == nil || c.runtime == nil {
		return ctldapi.RootFSInfo{}, fmt.Errorf("rootfs runtime is not configured")
	}
	return c.runtime.Inspect(ctx, target)
}

func (c *Controller) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := 2 * time.Minute
	if c != nil && c.operationTimeout > 0 {
		timeout = c.operationTimeout
	}
	return context.WithTimeout(parent, timeout)
}

func requestContext(r *http.Request) context.Context {
	if r != nil && r.Context() != nil {
		return r.Context()
	}
	return context.Background()
}

func validateSyncIdentity(target ctldapi.RootFSContainerRef, sandboxID, teamID, filesystemID string) error {
	if err := validateTarget(target); err != nil {
		return err
	}
	if strings.TrimSpace(sandboxID) == "" {
		return fmt.Errorf("%w: sandbox_id is required", ErrBadRequest)
	}
	if strings.TrimSpace(teamID) == "" {
		return fmt.Errorf("%w: team_id is required", ErrBadRequest)
	}
	if strings.TrimSpace(filesystemID) == "" {
		return fmt.Errorf("%w: filesystem_id is required", ErrBadRequest)
	}
	return nil
}

func validateTarget(target ctldapi.RootFSContainerRef) error {
	if strings.TrimSpace(target.Namespace) == "" {
		return fmt.Errorf("%w: namespace is required", ErrBadRequest)
	}
	if strings.TrimSpace(target.PodName) == "" {
		return fmt.Errorf("%w: pod_name is required", ErrBadRequest)
	}
	if strings.TrimSpace(target.ContainerName) == "" {
		return fmt.Errorf("%w: container_name is required", ErrBadRequest)
	}
	return nil
}

func validateSupportedRuntime(info ctldapi.RootFSInfo) error {
	runtime := strings.ToLower(strings.TrimSpace(info.Runtime))
	switch runtime {
	case "runc", "gvisor":
		return nil
	case "":
		return fmt.Errorf("%w: runtime is required", ErrBadRequest)
	default:
		return fmt.Errorf("%w: runtime %q is not supported for rootfs checkpoints", ErrBadRequest, info.Runtime)
	}
}

func rootFSSessionKey(info ctldapi.RootFSInfo) string {
	return strings.TrimSpace(info.Snapshotter) + "\x00" + strings.TrimSpace(info.SnapshotKey)
}

func statusForError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if errors.Is(err, ErrBadRequest) {
		return http.StatusBadRequest
	}
	if errors.Is(err, ErrConflict) {
		return http.StatusConflict
	}
	if errors.Is(err, ErrNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout
	}
	return http.StatusInternalServerError
}
