package portal

import (
	"context"
	"errors"
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
	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
)

const defaultRootDir = "/var/lib/sandbox0/ctld"
const defaultVolumePortalCacheSizeLimit = "20Gi"
const defaultVolumePortalRootMinFree = "5Gi"

type Manager struct {
	nodeName               string
	rootDir                string
	logger                 *zap.Logger
	logrus                 *logrus.Logger
	storage                *apiconfig.StorageProxyConfig
	repo                   *db.Repository
	clusterID              string
	podName                string
	podNamespace           string
	heartbeatInterval      time.Duration
	ownerOnlyIdleTTL       time.Duration
	portalCacheMaxBytes    int64
	portalRootMinFreeBytes int64
	volumeAPI              http.Handler
	rootFSBinder           RootFSVolumePortalBinder

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

	rootfsBackingPath string
	rootfsSession     volumefuse.Session

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
	RootFSBinder  RootFSVolumePortalBinder
}

type RootFSVolumePortalBindRequest struct {
	PodUID    string
	MountPath string
}

type RootFSVolumePortalBinder interface {
	BindRootFSVolumePortal(ctx context.Context, req RootFSVolumePortalBindRequest) error
	UnbindRootFSVolumePortal(ctx context.Context, req RootFSVolumePortalBindRequest) error
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
	ownerOnlyIdleTTL, _ := time.ParseDuration(storageConfig.DirectVolumeFileIdleTTL)
	portalCacheMaxBytes := parseQuantityBytesOrDefault(storageConfig.VolumePortalCacheSizeLimit, defaultVolumePortalCacheSizeLimit)
	portalRootMinFreeBytes := parseQuantityBytesOrDefault(storageConfig.VolumePortalRootMinFree, defaultVolumePortalRootMinFree)
	manager := &Manager{
		nodeName:               strings.TrimSpace(cfg.NodeName),
		rootDir:                rootDir,
		logger:                 logger,
		logrus:                 l,
		storage:                storageConfig,
		repo:                   cfg.Repository,
		clusterID:              naming.ClusterIDOrDefault(&storageConfig.DefaultClusterId),
		podName:                strings.TrimSpace(cfg.PodName),
		podNamespace:           strings.TrimSpace(cfg.PodNamespace),
		heartbeatInterval:      heartbeatInterval,
		ownerOnlyIdleTTL:       ownerOnlyIdleTTL,
		portalCacheMaxBytes:    portalCacheMaxBytes,
		portalRootMinFreeBytes: portalRootMinFreeBytes,
		rootFSBinder:           cfg.RootFSBinder,
		portals:                make(map[string]*portalMount),
		portalsByTarget:        make(map[string]*portalMount),
		boundVolumes:           make(map[string]*boundVolume),
		volumes:                newLocalVolumeManager(),
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

func (m *Manager) localDiskGuard(cacheDir string) *s0fs.LocalDiskGuard {
	if m == nil || (m.portalCacheMaxBytes <= 0 && m.portalRootMinFreeBytes <= 0) {
		return nil
	}
	return &s0fs.LocalDiskGuard{
		Path:         cacheDir,
		MaxBytes:     m.portalCacheMaxBytes,
		MinFreeBytes: m.portalRootMinFreeBytes,
	}
}

func (m *Manager) Run(ctx context.Context) {
	if m == nil || m.ownerOnlyIdleTTL <= 0 {
		return
	}
	interval := m.ownerOnlyIdleTTL / 2
	if interval <= 0 || interval > 5*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.cleanupIdleOwnerOnlyVolumes(ctx)
		}
	}
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

	rootfsBackingPath := m.unboundRootFSBackingPath(req.PodUID, req.Name)
	if err := os.MkdirAll(rootfsBackingPath, 0o755); err != nil {
		return fmt.Errorf("create unbound portal rootfs backing dir: %w", err)
	}
	rootfsSession := newRootFSBackedSession(rootfsBackingPath)
	fs := volumefuse.New(key, time.Second, rootfsSession)
	server, err := mountPortalFS(fs, req.TargetPath)
	if err != nil {
		return err
	}
	pm := &portalMount{
		namespace:         req.Namespace,
		podName:           req.PodName,
		podUID:            req.PodUID,
		name:              req.Name,
		mountPath:         req.MountPath,
		targetPath:        req.TargetPath,
		fs:                fs,
		server:            server,
		rootfsBackingPath: rootfsBackingPath,
		rootfsSession:     rootfsSession,
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
		if err := pm.server.Unmount(); err != nil {
			return err
		}
	}
	if pm.rootfsSession != nil {
		pm.rootfsSession.Close()
	}
	if pm.rootfsBackingPath != "" {
		return os.RemoveAll(pm.rootfsBackingPath)
	}
	return nil
}

func (m *Manager) RootFSPortalPaths(podUID string) []ctldapi.RootFSPortalPath {
	if m == nil || strings.TrimSpace(podUID) == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]ctldapi.RootFSPortalPath, 0)
	for _, pm := range m.portals {
		if pm == nil || pm.podUID != podUID || pm.volumeID != "" {
			continue
		}
		if pm.name == volumeportal.WebhookStatePortalName || pm.mountPath == volumeportal.WebhookStateMountPath {
			continue
		}
		if strings.TrimSpace(pm.mountPath) == "" || strings.TrimSpace(pm.rootfsBackingPath) == "" {
			continue
		}
		out = append(out, ctldapi.RootFSPortalPath{
			PortalName:  pm.name,
			MountPath:   pm.mountPath,
			BackingPath: pm.rootfsBackingPath,
		})
	}
	return out
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
		response := boundResponse(pm)
		if err := m.bindRootFSVolumePortal(ctx, pm); err != nil {
			return ctldapi.BindVolumePortalResponse{}, err
		}
		return response, nil
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
		if err := m.bindRootFSVolumePortal(ctx, pm); err != nil {
			return ctldapi.BindVolumePortalResponse{}, err
		}
		return response, nil
	}
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		if bound.refCount == 0 {
			m.attachPortalLocked(pm, req.SandboxVolumeID, volumeRecord.TeamID, mountedAt)
			bound.refCount = 1
			response := boundResponse(pm)
			m.mu.Unlock()
			if err := m.bindRootFSVolumePortal(ctx, pm); err != nil {
				return ctldapi.BindVolumePortalResponse{}, err
			}
			return response, nil
		}
		if accessMode != volume.AccessModeROX {
			conflictPath := boundMountPath(m.portals, req.SandboxVolumeID, key)
			m.mu.Unlock()
			return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
		}
		m.attachPortalLocked(pm, req.SandboxVolumeID, volumeRecord.TeamID, mountedAt)
		bound.refCount++
		response := boundResponse(pm)
		m.mu.Unlock()
		if err := m.bindRootFSVolumePortal(ctx, pm); err != nil {
			return ctldapi.BindVolumePortalResponse{}, err
		}
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
	encryption, err := volume.S0FSEncryptionConfig(m.storage)
	if err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}
	segmentTargetSize, err := volume.S0FSSegmentTargetSize(m.storage)
	if err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:          req.SandboxVolumeID,
		WALPath:           filepath.Join(cacheDir, "engine.wal"),
		ObjectStore:       remoteStore,
		SegmentTargetSize: segmentTargetSize,
		ObjectStoreForVolume: func(volumeID string) (objectstore.Store, error) {
			return m.createObjectStore(req.TeamID, volumeID)
		},
		HeadStore:      db.NewS0FSHeadStore(m.repo),
		Encryption:     encryption,
		LocalDiskGuard: m.localDiskGuard(cacheDir),
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
		if err := m.bindRootFSVolumePortal(ctx, pm); err != nil {
			return ctldapi.BindVolumePortalResponse{}, err
		}
		return response, nil
	}
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		if bound.refCount == 0 {
			m.attachPortalLocked(pm, req.SandboxVolumeID, volumeRecord.TeamID, mountedAt)
			bound.refCount = 1
			response := boundResponse(pm)
			m.mu.Unlock()
			_ = engine.Close()
			if err := m.bindRootFSVolumePortal(ctx, pm); err != nil {
				return ctldapi.BindVolumePortalResponse{}, err
			}
			return response, nil
		}
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
		if err := m.bindRootFSVolumePortal(ctx, pm); err != nil {
			return ctldapi.BindVolumePortalResponse{}, err
		}
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

	if err := m.bindRootFSVolumePortal(ctx, pm); err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}
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

func (m *Manager) CheckPublished(ctx context.Context, req ctldapi.CheckVolumePortalsRequest) (ctldapi.CheckVolumePortalsResponse, error) {
	if err := ctx.Err(); err != nil {
		return ctldapi.CheckVolumePortalsResponse{}, err
	}
	if strings.TrimSpace(req.PodUID) == "" {
		return ctldapi.CheckVolumePortalsResponse{}, fmt.Errorf("pod_uid is required")
	}
	if len(req.Portals) == 0 {
		return ctldapi.CheckVolumePortalsResponse{Ready: true}, nil
	}

	missing := make([]string, 0)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, portal := range req.Portals {
		name := volumeportal.NormalizePortalName(portal.PortalName, portal.MountPath)
		if name == "" {
			continue
		}
		if m.portals[portalKey(req.PodUID, name)] == nil {
			missing = append(missing, name)
		}
	}
	return ctldapi.CheckVolumePortalsResponse{
		Ready:   len(missing) == 0,
		Missing: missing,
	}, nil
}

func (m *Manager) AttachOwner(ctx context.Context, req ctldapi.AttachVolumeOwnerRequest) (ctldapi.AttachVolumeOwnerResponse, error) {
	if err := ctx.Err(); err != nil {
		return ctldapi.AttachVolumeOwnerResponse{}, err
	}
	if strings.TrimSpace(req.SandboxVolumeID) == "" || strings.TrimSpace(req.TeamID) == "" {
		return ctldapi.AttachVolumeOwnerResponse{}, fmt.Errorf("sandboxvolume_id and team_id are required")
	}
	volumeRecord, err := m.validateBindableVolume(ctx, ctldBindContext{
		volumeID: req.SandboxVolumeID,
		teamID:   req.TeamID,
	})
	if err != nil {
		return ctldapi.AttachVolumeOwnerResponse{}, err
	}
	accessMode, err := validateBindableAccessMode(volumeRecord.AccessMode)
	if err != nil {
		return ctldapi.AttachVolumeOwnerResponse{}, err
	}

	m.mu.Lock()
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		m.mu.Unlock()
		m.volumes.touch(req.SandboxVolumeID)
		return ctldapi.AttachVolumeOwnerResponse{Attached: true}, nil
	}
	m.mu.Unlock()

	mountedAt := time.Now().UTC()
	cacheDir := filepath.Join(m.rootDir, "volumes", safePath(req.TeamID), safePath(req.SandboxVolumeID))
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return ctldapi.AttachVolumeOwnerResponse{}, fmt.Errorf("create local volume dir: %w", err)
	}
	remoteStore, err := m.createObjectStore(req.TeamID, req.SandboxVolumeID)
	if err != nil {
		return ctldapi.AttachVolumeOwnerResponse{}, fmt.Errorf("create object storage: %w", err)
	}
	encryption, err := volume.S0FSEncryptionConfig(m.storage)
	if err != nil {
		return ctldapi.AttachVolumeOwnerResponse{}, err
	}
	segmentTargetSize, err := volume.S0FSSegmentTargetSize(m.storage)
	if err != nil {
		return ctldapi.AttachVolumeOwnerResponse{}, err
	}
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:          req.SandboxVolumeID,
		WALPath:           filepath.Join(cacheDir, "engine.wal"),
		ObjectStore:       remoteStore,
		SegmentTargetSize: segmentTargetSize,
		ObjectStoreForVolume: func(volumeID string) (objectstore.Store, error) {
			return m.createObjectStore(req.TeamID, volumeID)
		},
		HeadStore:      db.NewS0FSHeadStore(m.repo),
		Encryption:     encryption,
		LocalDiskGuard: m.localDiskGuard(cacheDir),
	})
	if err != nil {
		return ctldapi.AttachVolumeOwnerResponse{}, fmt.Errorf("open local s0fs engine: %w", err)
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
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		m.mu.Unlock()
		_ = engine.Close()
		m.volumes.touch(req.SandboxVolumeID)
		return ctldapi.AttachVolumeOwnerResponse{Attached: true}, nil
	}
	bound := &boundVolume{
		volumeID:  req.SandboxVolumeID,
		teamID:    volumeRecord.TeamID,
		access:    accessMode,
		mountedAt: mountedAt,
		refCount:  0,
		volCtx:    volCtx,
	}
	m.boundVolumes[req.SandboxVolumeID] = bound
	m.volumes.add(volCtx)
	if err := m.registerOwner(ctx, bound); err != nil {
		delete(m.boundVolumes, req.SandboxVolumeID)
		m.volumes.remove(req.SandboxVolumeID)
		m.mu.Unlock()
		_ = engine.Close()
		return ctldapi.AttachVolumeOwnerResponse{}, fmt.Errorf("register ctld volume owner: %w", err)
	}
	m.startMaterializer(bound)
	m.mu.Unlock()

	return ctldapi.AttachVolumeOwnerResponse{Attached: true}, nil
}

func (m *Manager) ReleaseOwner(ctx context.Context, req ctldapi.ReleaseVolumeOwnerRequest) (ctldapi.ReleaseVolumeOwnerResponse, error) {
	if err := ctx.Err(); err != nil {
		return ctldapi.ReleaseVolumeOwnerResponse{}, err
	}
	volumeID := strings.TrimSpace(req.SandboxVolumeID)
	if volumeID == "" {
		return ctldapi.ReleaseVolumeOwnerResponse{}, fmt.Errorf("sandboxvolume_id is required")
	}
	m.mu.Lock()
	bound := m.boundVolumes[volumeID]
	if bound == nil {
		m.mu.Unlock()
		return ctldapi.ReleaseVolumeOwnerResponse{Released: true}, nil
	}
	if bound.refCount > 0 {
		m.mu.Unlock()
		err := fmt.Errorf("volume %s is actively bound to a portal", volumeID)
		return ctldapi.ReleaseVolumeOwnerResponse{Busy: true, Error: err.Error()}, err
	}
	if ok, reason := m.volumes.canReleaseOwnerOnly(volumeID); !ok {
		m.mu.Unlock()
		err := fmt.Errorf("volume %s %s", volumeID, reason)
		return ctldapi.ReleaseVolumeOwnerResponse{Busy: true, Error: err.Error()}, err
	}
	if err := m.releaseOwnerOnlyVolumeLocked(ctx, volumeID, bound); err != nil {
		m.mu.Unlock()
		return ctldapi.ReleaseVolumeOwnerResponse{}, err
	}
	m.mu.Unlock()
	return ctldapi.ReleaseVolumeOwnerResponse{Released: true}, nil
}

func (m *Manager) PrepareSnapshotCheckpoint(ctx context.Context, req ctldapi.PrepareVolumeSnapshotCheckpointRequest) (ctldapi.PrepareVolumeSnapshotCheckpointResponse, error) {
	volumeID := strings.TrimSpace(req.SandboxVolumeID)
	if volumeID == "" {
		return ctldapi.PrepareVolumeSnapshotCheckpointResponse{}, fmt.Errorf("sandboxvolume_id is required")
	}
	m.mu.Lock()
	bound := m.boundVolumes[volumeID]
	m.mu.Unlock()
	if bound == nil || bound.volCtx == nil || bound.volCtx.S0FS == nil {
		return ctldapi.PrepareVolumeSnapshotCheckpointResponse{}, fmt.Errorf("volume %s is not owned by this ctld", volumeID)
	}
	if err := m.volumes.prepareSnapshotCheckpoint(ctx, volumeID); err != nil {
		return ctldapi.PrepareVolumeSnapshotCheckpointResponse{}, err
	}
	if _, err := bound.volCtx.S0FS.SyncMaterialize(ctx); err != nil {
		m.volumes.completeSnapshotCheckpoint(volumeID)
		return ctldapi.PrepareVolumeSnapshotCheckpointResponse{}, err
	}
	return ctldapi.PrepareVolumeSnapshotCheckpointResponse{Prepared: true}, nil
}

func (m *Manager) CompleteSnapshotCheckpoint(_ context.Context, req ctldapi.CompleteVolumeSnapshotCheckpointRequest) (ctldapi.CompleteVolumeSnapshotCheckpointResponse, error) {
	volumeID := strings.TrimSpace(req.SandboxVolumeID)
	if volumeID == "" {
		return ctldapi.CompleteVolumeSnapshotCheckpointResponse{}, fmt.Errorf("sandboxvolume_id is required")
	}
	m.volumes.completeSnapshotCheckpoint(volumeID)
	return ctldapi.CompleteVolumeSnapshotCheckpointResponse{Completed: true}, nil
}

func (m *Manager) AbortSnapshotCheckpoint(_ context.Context, req ctldapi.AbortVolumeSnapshotCheckpointRequest) (ctldapi.AbortVolumeSnapshotCheckpointResponse, error) {
	volumeID := strings.TrimSpace(req.SandboxVolumeID)
	if volumeID == "" {
		return ctldapi.AbortVolumeSnapshotCheckpointResponse{}, fmt.Errorf("sandboxvolume_id is required")
	}
	m.volumes.completeSnapshotCheckpoint(volumeID)
	return ctldapi.AbortVolumeSnapshotCheckpointResponse{Aborted: true}, nil
}

func (m *Manager) cleanupIdleOwnerOnlyVolumes(ctx context.Context) {
	if m == nil || m.ownerOnlyIdleTTL <= 0 {
		return
	}
	cutoff := time.Now().UTC().Add(-m.ownerOnlyIdleTTL)

	m.mu.Lock()
	volumeIDs := make([]string, 0, len(m.boundVolumes))
	for volumeID, bound := range m.boundVolumes {
		if bound == nil || bound.refCount > 0 || !m.volumes.canCleanupOwnerOnly(volumeID, cutoff) {
			continue
		}
		volumeIDs = append(volumeIDs, volumeID)
	}
	m.mu.Unlock()

	for _, volumeID := range volumeIDs {
		if err := m.releaseOwnerOnlyVolume(ctx, volumeID); err != nil && m.logger != nil {
			m.logger.Warn("ctld idle owner-only cleanup failed", zap.String("volume_id", volumeID), zap.Error(err))
		}
	}
}

func (m *Manager) releaseOwnerOnlyVolume(ctx context.Context, volumeID string) error {
	m.mu.Lock()
	bound := m.boundVolumes[volumeID]
	if bound == nil || bound.refCount > 0 || !m.volumes.canCleanupOwnerOnly(volumeID, time.Now().UTC().Add(-m.ownerOnlyIdleTTL)) {
		m.mu.Unlock()
		return nil
	}
	if err := m.releaseOwnerOnlyVolumeLocked(ctx, volumeID, bound); err != nil {
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) releaseOwnerOnlyVolumeLocked(ctx context.Context, volumeID string, bound *boundVolume) error {
	if bound.materializeCancel != nil {
		bound.materializeCancel()
		bound.materializeCancel = nil
	}
	done := bound.materializeDone
	bound.materializeDone = nil
	if done != nil {
		<-done
	}
	if err := m.volumes.UnmountVolume(ctx, volumeID, ""); err != nil {
		return err
	}
	delete(m.boundVolumes, volumeID)
	m.unregisterOwner(bound)
	return nil
}

func (m *Manager) unbindLockedSnapshot(pm *portalMount) error {
	volumeID := pm.volumeID
	rootFSUnbind := m.unbindRootFSVolumePortalSnapshot(pm)
	m.clearPortalLocked(pm)
	if volumeID == "" {
		return rootFSUnbind()
	}
	bound := m.boundVolumes[volumeID]
	if bound == nil {
		return rootFSUnbind()
	}
	if bound.refCount > 1 {
		bound.refCount--
		return rootFSUnbind()
	}
	if bound.materializeCancel != nil {
		bound.materializeCancel()
		bound.materializeCancel = nil
	}
	if bound.materializeDone != nil {
		<-bound.materializeDone
		bound.materializeDone = nil
	}
	if err := m.volumes.UnmountVolume(context.Background(), volumeID, ""); err != nil {
		return err
	}
	delete(m.boundVolumes, volumeID)
	m.unregisterOwner(bound)
	return rootFSUnbind()
}

func (m *Manager) bindRootFSVolumePortal(ctx context.Context, pm *portalMount) error {
	if m == nil || m.rootFSBinder == nil || pm == nil || pm.volumeID == "" {
		return nil
	}
	if strings.TrimSpace(pm.podUID) == "" || strings.TrimSpace(pm.mountPath) == "" {
		return nil
	}
	return m.rootFSBinder.BindRootFSVolumePortal(ctx, RootFSVolumePortalBindRequest{
		PodUID:    pm.podUID,
		MountPath: pm.mountPath,
	})
}

func (m *Manager) unbindRootFSVolumePortalSnapshot(pm *portalMount) func() error {
	if m == nil || m.rootFSBinder == nil || pm == nil || strings.TrimSpace(pm.podUID) == "" || strings.TrimSpace(pm.mountPath) == "" {
		return func() error { return nil }
	}
	req := RootFSVolumePortalBindRequest{
		PodUID:    pm.podUID,
		MountPath: pm.mountPath,
	}
	return func() error {
		return m.rootFSBinder.UnbindRootFSVolumePortal(context.Background(), req)
	}
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
	compactionInterval, err := volume.S0FSCompactionInterval(m.storage)
	if err != nil {
		if m.logger != nil {
			m.logger.Warn("ctld s0fs compaction disabled due to invalid configuration", zap.String("volume_id", bound.volumeID), zap.Error(err))
		}
		compactionInterval = 0
	}
	compactionOptions, err := volume.S0FSCompactionOptions(m.storage)
	if err != nil {
		if m.logger != nil {
			m.logger.Warn("ctld s0fs compaction disabled due to invalid options", zap.String("volume_id", bound.volumeID), zap.Error(err))
		}
		compactionInterval = 0
	}
	if !volume.S0FSBackgroundCompactionEnabled(bound.access) {
		compactionInterval = 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	bound.materializeCancel = cancel
	bound.materializeDone = done
	go func(volumeID string) {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		var compactionTicker *time.Ticker
		var compactionC <-chan time.Time
		if compactionInterval > 0 {
			compactionTicker = time.NewTicker(compactionInterval)
			compactionC = compactionTicker.C
			defer compactionTicker.Stop()
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				manifest, err := bound.volCtx.S0FS.SyncMaterialize(ctx)
				if err != nil && m.logger != nil {
					m.logger.Warn("ctld volume materialize failed", zap.String("volume_id", volumeID), zap.Error(err))
					continue
				}
				m.garbageCollectBoundS0FS(ctx, bound, manifest)
			case <-compactionC:
				resultManifest, result, err := bound.volCtx.S0FS.Compact(ctx, compactionOptions)
				if err != nil && m.logger != nil {
					m.logger.Warn("ctld volume compaction failed", zap.String("volume_id", volumeID), zap.Error(err))
					continue
				}
				if result != nil && len(result.CompactedSegments) > 0 && m.logger != nil {
					m.logger.Info("ctld volume compacted",
						zap.String("volume_id", volumeID),
						zap.Int("segments", len(result.CompactedSegments)),
						zap.Uint64("rewritten_bytes", result.RewrittenBytes),
						zap.Uint64("reclaimable_bytes", result.ReclaimableBytes),
					)
				}
				m.garbageCollectBoundS0FS(ctx, bound, resultManifest)
			}
		}
	}(bound.volumeID)
}

func (m *Manager) garbageCollectBoundS0FS(ctx context.Context, bound *boundVolume, manifest *s0fs.Manifest) {
	if m == nil || bound == nil || manifest == nil || manifest.State == nil {
		return
	}
	if !volume.S0FSBackgroundCompactionEnabled(bound.access) {
		return
	}
	result, err := m.garbageCollectBoundS0FSObjects(ctx, bound, manifest)
	if err != nil {
		if m.logger != nil {
			m.logger.Warn("ctld volume garbage collection failed", zap.String("volume_id", bound.volumeID), zap.Error(err))
		}
		return
	}
	if result != nil && (len(result.DeletedSegments) > 0 || len(result.DeletedManifests) > 0) && m.logger != nil {
		m.logger.Info("ctld volume garbage collected",
			zap.String("volume_id", bound.volumeID),
			zap.Int("segments", len(result.DeletedSegments)),
			zap.Int("manifests", len(result.DeletedManifests)),
		)
	}
}

func (m *Manager) garbageCollectBoundS0FSObjects(ctx context.Context, bound *boundVolume, manifest *s0fs.Manifest) (*s0fs.GarbageCollectionResult, error) {
	if m == nil || m.repo == nil || m.volumes == nil || bound == nil || bound.volCtx == nil || manifest == nil || manifest.State == nil {
		return nil, nil
	}
	if !volume.S0FSBackgroundCompactionEnabled(bound.access) || !m.volumes.canGarbageCollectS0FS(bound.volumeID) {
		return nil, nil
	}
	children, err := m.repo.ListSandboxVolumesBySource(ctx, bound.volumeID)
	if err != nil {
		return nil, err
	}
	if len(children) > 0 {
		return nil, nil
	}
	snapshots, err := m.repo.ListSnapshotsByVolume(ctx, bound.volumeID)
	if err != nil {
		return nil, err
	}
	if len(snapshots) > 0 {
		return nil, nil
	}
	store, err := m.createObjectStore(bound.teamID, bound.volumeID)
	if err != nil {
		return nil, err
	}
	encryption, err := volume.S0FSEncryptionConfig(m.storage)
	if err != nil {
		return nil, err
	}
	headStore := db.NewS0FSHeadStore(m.repo)
	materializer := s0fs.NewMaterializer(bound.volumeID, store, headStore, func(sourceVolumeID string) (objectstore.Store, error) {
		return m.createObjectStore(bound.teamID, sourceVolumeID)
	})
	if materializer == nil || !materializer.Enabled() {
		return nil, nil
	}
	materializer.SetEncryption(encryption)

	headBefore, err := headStore.LoadCommittedHead(ctx, bound.volumeID)
	if err != nil && !errors.Is(err, s0fs.ErrCommittedHeadNotFound) {
		return nil, err
	}
	retainedStates := []*s0fs.SnapshotState{manifest.State}
	cfg := s0fs.Config{
		VolumeID:    bound.volumeID,
		WALPath:     filepath.Join(bound.volCtx.CacheDir, "engine.wal"),
		ObjectStore: store,
		HeadStore:   headStore,
		Encryption:  encryption,
	}
	localSnapshots, err := s0fs.LoadLocalSnapshots(ctx, cfg)
	if err != nil {
		return nil, err
	}
	retainedStates = append(retainedStates, localSnapshots...)
	retainedManifests := map[string]struct{}{
		"manifests/latest.json": {},
	}
	if manifest.ManifestSeq > 0 {
		retainedManifests[fmt.Sprintf("manifests/%020d.json", manifest.ManifestSeq)] = struct{}{}
	}
	if headBefore != nil && strings.TrimSpace(headBefore.ManifestKey) != "" {
		retainedManifests[headBefore.ManifestKey] = struct{}{}
	}
	plan, err := materializer.PlanGarbageCollection(ctx, retainedStates, retainedManifests)
	if err != nil {
		return nil, err
	}
	if len(plan.Segments) == 0 && len(plan.Manifests) == 0 {
		return &s0fs.GarbageCollectionResult{}, nil
	}
	if !m.volumes.canGarbageCollectS0FS(bound.volumeID) {
		return nil, nil
	}
	headAfter, err := headStore.LoadCommittedHead(ctx, bound.volumeID)
	if err != nil && !errors.Is(err, s0fs.ErrCommittedHeadNotFound) {
		return nil, err
	}
	if !sameS0FSHeadKey(headBefore, headAfter) {
		return nil, nil
	}
	return plan.Apply(ctx)
}

func sameS0FSHeadKey(a, b *s0fs.CommittedHead) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return strings.TrimSpace(a.ManifestKey) == strings.TrimSpace(b.ManifestKey)
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
		pm.fs.SetSession(pm.rootfsSession)
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

func (m *Manager) unboundRootFSBackingPath(podUID, portalName string) string {
	rootDir := defaultRootDir
	if m != nil && strings.TrimSpace(m.rootDir) != "" {
		rootDir = m.rootDir
	}
	return filepath.Join(rootDir, "rootfs-portals", safePath(podUID), safePath(portalName))
}

func parseQuantityBytesOrDefault(value, fallback string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		value = fallback
	}
	quantity, err := resource.ParseQuantity(value)
	if err != nil || quantity.Sign() <= 0 {
		if value == fallback {
			return 0
		}
		quantity, err = resource.ParseQuantity(fallback)
		if err != nil || quantity.Sign() <= 0 {
			return 0
		}
	}
	return quantity.Value()
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
