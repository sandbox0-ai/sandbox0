package portal

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

const defaultRootDir = "/var/lib/sandbox0/ctld"

type Manager struct {
	nodeName          string
	rootDir           string
	logger            *zap.Logger
	logrus            *logrus.Logger
	storage           *apiconfig.StorageProxyConfig
	repo              *db.Repository
	clusterID         string
	podName           string
	podNamespace      string
	heartbeatInterval time.Duration
	volumeAPI         http.Handler

	mu              sync.Mutex
	portals         map[string]*portalMount
	portalsByTarget map[string]*portalMount
	boundVolumes    map[string]*boundVolume
	volumes         *localVolumeManager
}

type portalMount struct {
	namespace  string
	podName    string
	podUID     string
	name       string
	mountPath  string
	targetPath string
	fs         *volumefuse.FileSystem
	server     *fuse.Server

	volumeID  string
	teamID    string
	mountedAt time.Time
}

type boundVolume struct {
	volumeID  string
	teamID    string
	access    volume.AccessMode
	mountedAt time.Time
	refCount  int
	volCtx    *volume.VolumeContext

	heartbeatCancel context.CancelFunc
	heartbeatDone   chan struct{}

	materializeCancel context.CancelFunc
	materializeDone   chan struct{}
}

type Config struct {
	NodeName      string
	RootDir       string
	Logger        *zap.Logger
	StorageConfig *apiconfig.StorageProxyConfig
	Repository    *db.Repository
	PodName       string
	PodNamespace  string
}

func NewManager(cfg Config) *Manager {
	rootDir := strings.TrimSpace(cfg.RootDir)
	if rootDir == "" {
		rootDir = defaultRootDir
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	l := logrus.New()
	l.SetOutput(os.Stderr)
	storageConfig := cfg.StorageConfig
	if storageConfig == nil {
		storageConfig = apiconfig.LoadStorageProxyConfig()
	}
	heartbeatInterval, _ := time.ParseDuration(storageConfig.HeartbeatInterval)
	if heartbeatInterval <= 0 {
		heartbeatInterval = 5 * time.Second
	}
	manager := &Manager{
		nodeName:          strings.TrimSpace(cfg.NodeName),
		rootDir:           rootDir,
		logger:            logger,
		logrus:            l,
		storage:           storageConfig,
		repo:              cfg.Repository,
		clusterID:         strings.TrimSpace(storageConfig.DefaultClusterId),
		podName:           strings.TrimSpace(cfg.PodName),
		podNamespace:      strings.TrimSpace(cfg.PodNamespace),
		heartbeatInterval: heartbeatInterval,
		portals:           make(map[string]*portalMount),
		portalsByTarget:   make(map[string]*portalMount),
		boundVolumes:      make(map[string]*boundVolume),
		volumes:           newLocalVolumeManager(),
	}
	manager.volumeAPI = newMountedVolumeAPIHandler(storageConfig, cfg.Repository, manager.volumes, l)
	return manager
}

func (m *Manager) MountedVolumeHandler() http.Handler {
	if m == nil {
		return nil
	}
	return m.volumeAPI
}

func (m *Manager) PublishPortal(ctx context.Context, req publishRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if req.TargetPath == "" {
		return fmt.Errorf("target path is required")
	}
	if req.PodUID == "" {
		return fmt.Errorf("pod uid is required")
	}
	if req.Name == "" {
		return fmt.Errorf("portal name is required")
	}
	key := portalKey(req.PodUID, req.Name)

	m.mu.Lock()
	if existing := m.portals[key]; existing != nil {
		m.portalsByTarget[req.TargetPath] = existing
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	if err := os.MkdirAll(req.TargetPath, 0o755); err != nil {
		return fmt.Errorf("create portal target: %w", err)
	}

	fs := volumefuse.New(key, time.Second, unboundSession{})
	server, err := mountPortalFS(fs, req.TargetPath)
	if err != nil {
		return err
	}
	pm := &portalMount{
		namespace:  req.Namespace,
		podName:    req.PodName,
		podUID:     req.PodUID,
		name:       req.Name,
		mountPath:  req.MountPath,
		targetPath: req.TargetPath,
		fs:         fs,
		server:     server,
	}

	m.mu.Lock()
	m.portals[key] = pm
	m.portalsByTarget[req.TargetPath] = pm
	m.mu.Unlock()
	return nil
}

func mountPortalFS(fs *volumefuse.FileSystem, targetPath string) (*fuse.Server, error) {
	opts := &fuse.MountOptions{
		FsName:        "sandbox0-volume-portal",
		Name:          "sandbox0-volume",
		MaxBackground: 128,
		EnableLocks:   true,
		AllowOther:    os.Getuid() == 0,
		DirectMount:   true,
		MaxWrite:      256 * 1024,
	}
	server, err := fuse.NewServer(fs, targetPath, opts)
	if err != nil {
		return nil, fmt.Errorf("mount volume portal fuse: %w", err)
	}
	go server.Serve()
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		return nil, fmt.Errorf("wait for volume portal mount: %w", err)
	}
	return server, nil
}

func (m *Manager) UnpublishPortal(targetPath string) error {
	m.mu.Lock()
	pm := m.portalsByTarget[targetPath]
	var unbindErr error
	if pm != nil {
		delete(m.portalsByTarget, targetPath)
		delete(m.portals, portalKey(pm.podUID, pm.name))
		unbindErr = m.unbindLockedSnapshot(pm)
	}
	m.mu.Unlock()
	if pm == nil {
		return nil
	}
	if unbindErr != nil {
		return unbindErr
	}
	if pm.server != nil {
		return pm.server.Unmount()
	}
	return nil
}

func (m *Manager) Bind(ctx context.Context, req ctldapi.BindVolumePortalRequest) (ctldapi.BindVolumePortalResponse, error) {
	if err := ctx.Err(); err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}
	portalName := volumeportal.NormalizePortalName(req.PortalName, req.MountPath)
	if portalName == "" {
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("portal name or mount path is required")
	}
	if req.PodUID == "" || req.SandboxVolumeID == "" || req.TeamID == "" {
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("pod_uid, sandboxvolume_id and team_id are required")
	}
	volumeRecord, err := m.validateBindableVolume(ctx, ctldBindContext{
		volumeID: req.SandboxVolumeID,
		teamID:   req.TeamID,
	})
	if err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}
	accessMode, err := validateBindableAccessMode(volumeRecord.AccessMode)
	if err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}

	key := portalKey(req.PodUID, portalName)
	m.mu.Lock()
	pm := m.portals[key]
	m.mu.Unlock()
	if pm == nil {
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume portal %s for pod %s is not published", portalName, req.PodUID)
	}
	if pm.volumeID != "" && pm.volumeID != req.SandboxVolumeID {
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume portal already bound to %s", pm.volumeID)
	}
	if pm.volumeID == req.SandboxVolumeID {
		return boundResponse(pm), nil
	}

	mountedAt := time.Now().UTC()
	m.mu.Lock()
	pm = m.portals[key]
	if pm == nil {
		m.mu.Unlock()
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume portal %s for pod %s is not published", portalName, req.PodUID)
	}
	if pm.volumeID != "" {
		response := boundResponse(pm)
		m.mu.Unlock()
		if response.SandboxVolumeID != req.SandboxVolumeID {
			return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume portal already bound to %s", response.SandboxVolumeID)
		}
		return response, nil
	}
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		if accessMode != volume.AccessModeROX {
			conflictPath := boundMountPath(m.portals, req.SandboxVolumeID, key)
			m.mu.Unlock()
			return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
		}
		m.attachPortalLocked(pm, req.SandboxVolumeID, volumeRecord.TeamID, mountedAt)
		bound.refCount++
		response := boundResponse(pm)
		m.mu.Unlock()
		return response, nil
	}
	if existing := findBoundPortalForVolume(m.portals, req.SandboxVolumeID, key); existing != nil {
		conflictPath := existing.mountPath
		m.mu.Unlock()
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
	}
	m.mu.Unlock()

	cacheDir := filepath.Join(m.rootDir, "volumes", safePath(req.TeamID), safePath(req.SandboxVolumeID))
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("create local volume dir: %w", err)
	}
	remoteStore, err := m.createObjectStore(req.TeamID, req.SandboxVolumeID)
	if err != nil {
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("create object storage: %w", err)
	}
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:    req.SandboxVolumeID,
		WALPath:     filepath.Join(cacheDir, "engine.wal"),
		ObjectStore: remoteStore,
		HeadStore:   db.NewS0FSHeadStore(m.repo),
	})
	if err != nil {
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("open local s0fs engine: %w", err)
	}
	volCtx := &volume.VolumeContext{
		VolumeID:  req.SandboxVolumeID,
		TeamID:    volumeRecord.TeamID,
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    accessMode,
		MountedAt: mountedAt,
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
	}

	m.mu.Lock()
	pm = m.portals[key]
	if pm == nil {
		m.mu.Unlock()
		_ = engine.Close()
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume portal %s for pod %s is not published", portalName, req.PodUID)
	}
	if pm.volumeID != "" {
		response := boundResponse(pm)
		m.mu.Unlock()
		_ = engine.Close()
		if response.SandboxVolumeID != req.SandboxVolumeID {
			return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume portal already bound to %s", response.SandboxVolumeID)
		}
		return response, nil
	}
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		if accessMode != volume.AccessModeROX {
			conflictPath := boundMountPath(m.portals, req.SandboxVolumeID, key)
			m.mu.Unlock()
			_ = engine.Close()
			return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
		}
		m.attachPortalLocked(pm, req.SandboxVolumeID, volumeRecord.TeamID, mountedAt)
		bound.refCount++
		response := boundResponse(pm)
		m.mu.Unlock()
		_ = engine.Close()
		return response, nil
	}
	if existing := findBoundPortalForVolume(m.portals, req.SandboxVolumeID, key); existing != nil {
		conflictPath := existing.mountPath
		m.mu.Unlock()
		_ = engine.Close()
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
	}
	bound := &boundVolume{
		volumeID:  req.SandboxVolumeID,
		teamID:    volumeRecord.TeamID,
		access:    accessMode,
		mountedAt: mountedAt,
		refCount:  1,
		volCtx:    volCtx,
	}
	m.boundVolumes[req.SandboxVolumeID] = bound
	m.volumes.add(volCtx)
	m.attachPortalLocked(pm, req.SandboxVolumeID, volumeRecord.TeamID, mountedAt)
	if err := m.registerOwner(ctx, bound); err != nil {
		m.clearPortalLocked(pm)
		delete(m.boundVolumes, req.SandboxVolumeID)
		m.volumes.remove(req.SandboxVolumeID)
		m.mu.Unlock()
		_ = engine.Close()
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("register ctld volume owner: %w", err)
	}
	m.startMaterializer(bound)
	response := boundResponse(pm)
	m.mu.Unlock()

	return response, nil
}

func (m *Manager) Unbind(ctx context.Context, req ctldapi.UnbindVolumePortalRequest) (ctldapi.UnbindVolumePortalResponse, error) {
	if err := ctx.Err(); err != nil {
		return ctldapi.UnbindVolumePortalResponse{}, err
	}
	portalName := volumeportal.NormalizePortalName(req.PortalName, req.MountPath)
	if req.PodUID == "" || portalName == "" {
		return ctldapi.UnbindVolumePortalResponse{}, fmt.Errorf("pod_uid and portal identity are required")
	}
	m.mu.Lock()
	pm := m.portals[portalKey(req.PodUID, portalName)]
	if pm == nil {
		m.mu.Unlock()
		return ctldapi.UnbindVolumePortalResponse{Unbound: true}, nil
	}
	err := m.unbindLockedSnapshot(pm)
	m.mu.Unlock()
	if err != nil {
		return ctldapi.UnbindVolumePortalResponse{}, err
	}
	return ctldapi.UnbindVolumePortalResponse{Unbound: true}, nil
}

func (m *Manager) unbindLockedSnapshot(pm *portalMount) error {
	volumeID := pm.volumeID
	m.clearPortalLocked(pm)
	if volumeID == "" {
		return nil
	}
	bound := m.boundVolumes[volumeID]
	if bound == nil {
		return nil
	}
	if bound.refCount > 1 {
		bound.refCount--
		return nil
	}
	delete(m.boundVolumes, volumeID)
	m.unregisterOwner(bound)
	if bound.materializeCancel != nil {
		bound.materializeCancel()
		bound.materializeCancel = nil
	}
	if bound.materializeDone != nil {
		<-bound.materializeDone
		bound.materializeDone = nil
	}
	return m.volumes.UnmountVolume(context.Background(), volumeID, "")
}

type publishRequest struct {
	Namespace  string
	PodName    string
	PodUID     string
	Name       string
	MountPath  string
	TargetPath string
}

func publishRequestFromContext(targetPath string, attrs map[string]string) publishRequest {
	mountPath := strings.TrimSpace(attrs[volumeportal.AttributeMountPath])
	name := volumeportal.NormalizePortalName(attrs[volumeportal.AttributePortalName], mountPath)
	return publishRequest{
		Namespace:  strings.TrimSpace(attrs[volumeportal.PodInfoNamespace]),
		PodName:    strings.TrimSpace(attrs[volumeportal.PodInfoName]),
		PodUID:     strings.TrimSpace(attrs[volumeportal.PodInfoUID]),
		Name:       name,
		MountPath:  mountPath,
		TargetPath: targetPath,
	}
}

func portalKey(podUID, name string) string {
	return podUID + "\x00" + name
}

func (m *Manager) createObjectStore(teamID, volumeID string) (objectstore.Store, error) {
	if m == nil || m.storage == nil || strings.TrimSpace(m.storage.S3Bucket) == "" {
		return nil, nil
	}
	store, err := objectstore.Create(objectstore.Config{
		Type:         m.storage.ObjectStorageType,
		Bucket:       m.storage.S3Bucket,
		Region:       m.storage.S3Region,
		Endpoint:     m.storage.S3Endpoint,
		AccessKey:    m.storage.S3AccessKey,
		SecretKey:    m.storage.S3SecretKey,
		SessionToken: m.storage.S3SessionToken,
	})
	if err != nil {
		return nil, err
	}
	prefix, err := naming.S3VolumePrefix(teamID, volumeID)
	if err != nil {
		return nil, err
	}
	return objectstore.Prefix(store, prefix+"/s0fs/"), nil
}

func (m *Manager) startMaterializer(bound *boundVolume) {
	if bound == nil || bound.volCtx == nil || bound.volCtx.S0FS == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	bound.materializeCancel = cancel
	bound.materializeDone = done
	go func(volumeID string) {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := bound.volCtx.S0FS.SyncMaterialize(ctx); err != nil && m.logger != nil {
					m.logger.Warn("ctld volume materialize failed", zap.String("volume_id", volumeID), zap.Error(err))
				}
			}
		}
	}(bound.volumeID)
}

func (m *Manager) attachPortalLocked(pm *portalMount, volumeID, teamID string, mountedAt time.Time) {
	if pm == nil {
		return
	}
	if pm.fs != nil {
		pm.fs.SetSession(newLocalSession(volumeID, m.volumes, m.logrus))
	}
	pm.volumeID = volumeID
	pm.teamID = teamID
	pm.mountedAt = mountedAt
}

func (m *Manager) clearPortalLocked(pm *portalMount) {
	if pm == nil {
		return
	}
	if pm.fs != nil {
		pm.fs.SetSession(unboundSession{})
	}
	pm.volumeID = ""
	pm.teamID = ""
	pm.mountedAt = time.Time{}
}

func boundResponse(pm *portalMount) ctldapi.BindVolumePortalResponse {
	if pm == nil {
		return ctldapi.BindVolumePortalResponse{}
	}
	return ctldapi.BindVolumePortalResponse{
		SandboxVolumeID: pm.volumeID,
		MountPoint:      pm.mountPath,
		MountedAt:       pm.mountedAt.Format(time.RFC3339),
	}
}

func boundMountPath(portals map[string]*portalMount, volumeID, exceptKey string) string {
	if existing := findBoundPortalForVolume(portals, volumeID, exceptKey); existing != nil {
		return existing.mountPath
	}
	return volumeID
}

func safePath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "_"
	}
	return strings.NewReplacer("/", "_", "\\", "_", "\x00", "_").Replace(value)
}

func removeSocket(path string) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func listenUnix(path string) (net.Listener, error) {
	if err := removeSocket(path); err != nil {
		return nil, err
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = l.Close()
		return nil, err
	}
	return l, nil
}
