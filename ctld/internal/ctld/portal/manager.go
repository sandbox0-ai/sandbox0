package portal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
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
	"github.com/sandbox0-ai/sandbox0/pkg/fuseportal"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/activeconnections"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/storageoperations"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/storagequota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
)

const defaultRootDir = "/var/lib/sandbox0/ctld"
const defaultVolumePortalCacheSizeLimit = "20Gi"
const defaultVolumePortalRootMinFree = "5Gi"
const defaultS0FSMaterializerConcurrency = 16
const defaultVolumeMaterializeInterval = 2 * time.Second

type Manager struct {
	nodeName               string
	rootDir                string
	kubeletPodsRoot        string
	logger                 *zap.Logger
	logrus                 *logrus.Logger
	storage                *apiconfig.StorageProxyConfig
	storageObserver        volume.StorageObserver
	storageQuota           *storagequota.Service
	storageOperations      storageoperations.Quota
	activeConnections      activeconnections.Quota
	s3CredentialCodec      *volume.S3BackendCredentialCodec
	s3CredentialCodecErr   error
	repo                   *db.Repository
	clusterID              string
	podName                string
	podNamespace           string
	ownerID                string
	heartbeatInterval      time.Duration
	ownerOnlyIdleTTL       time.Duration
	portalCacheMaxBytes    int64
	portalRootMinFreeBytes int64
	volumeAPI              http.Handler
	staleMountCleaner      staleMountCleaner
	staleMountChecker      staleMountChecker
	activePodUIDLister     ActivePodUIDLister
	materializerLimiter    chan struct{}
	recoveryStore          *portalRecoveryStore
	replicator             PortalReplicator
	requireStandby         bool

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
	server     *fuseportal.Server

	rootfsBackingPath string
	rootfsStatePath   string
	rootfsSession     volumefuse.Session
	volumeStatePath   string

	volumeID  string
	teamID    string
	access    volume.AccessMode
	mountedAt time.Time
}

type boundVolume struct {
	volumeID  string
	teamID    string
	access    volume.AccessMode
	mountedAt time.Time
	refCount  int
	volCtx    *volume.VolumeContext
	session   volumefuse.Session
	statePath string

	heartbeatCancel context.CancelFunc
	heartbeatDone   chan struct{}

	materializeCancel context.CancelFunc
	materializeDone   chan struct{}
	closing           bool
}

type boundVolumeCleanup struct {
	volumeID          string
	bound             *boundVolume
	materializeCancel context.CancelFunc
	materializeDone   chan struct{}
}

type Config struct {
	NodeName                string
	RootDir                 string
	KubeletPodsRoot         string
	Logger                  *zap.Logger
	StorageConfig           *apiconfig.StorageProxyConfig
	StorageObserver         volume.StorageObserver
	StorageQuota            *storagequota.Service
	StorageOperations       storageoperations.Quota
	ActiveConnections       activeconnections.Quota
	Repository              *db.Repository
	PodName                 string
	PodNamespace            string
	OwnerID                 string
	StaleMountCleaner       staleMountCleaner
	StaleMountChecker       staleMountChecker
	ActivePodUIDLister      ActivePodUIDLister
	MaterializerConcurrency int
	Replicator              PortalReplicator
	RequireStandby          bool
}

func NewManager(cfg Config) *Manager {
	rootDir := strings.TrimSpace(cfg.RootDir)
	if rootDir == "" {
		rootDir = defaultRootDir
	}
	kubeletPodsRoot := strings.TrimSpace(cfg.KubeletPodsRoot)
	if kubeletPodsRoot == "" {
		kubeletPodsRoot = defaultKubeletPodsRoot
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	l := logrus.New()
	l.SetOutput(os.Stderr)
	storageConfig := cfg.StorageConfig
	if storageConfig == nil {
		storageConfig = &apiconfig.StorageProxyConfig{}
	}
	s3CredentialCodec, s3CredentialCodecErr := volume.NewS3BackendCredentialCodecFromConfig(storageConfig)
	heartbeatInterval, _ := time.ParseDuration(storageConfig.HeartbeatInterval)
	if heartbeatInterval <= 0 {
		heartbeatInterval = 5 * time.Second
	}
	ownerOnlyIdleTTL, _ := time.ParseDuration(storageConfig.DirectVolumeFileIdleTTL)
	portalCacheMaxBytes := parseQuantityBytesOrDefault(storageConfig.VolumePortalCacheSizeLimit, defaultVolumePortalCacheSizeLimit)
	portalRootMinFreeBytes := parseQuantityBytesOrDefault(storageConfig.VolumePortalRootMinFree, defaultVolumePortalRootMinFree)
	staleCleaner := cfg.StaleMountCleaner
	if staleCleaner == nil {
		staleCleaner = defaultStaleMountCleaner
	}
	staleChecker := cfg.StaleMountChecker
	if staleChecker == nil {
		staleChecker = defaultStaleMountChecker
	}
	materializerConcurrency := cfg.MaterializerConcurrency
	if materializerConcurrency <= 0 {
		materializerConcurrency = defaultS0FSMaterializerConcurrency
	}
	manager := &Manager{
		nodeName:               strings.TrimSpace(cfg.NodeName),
		rootDir:                rootDir,
		kubeletPodsRoot:        kubeletPodsRoot,
		logger:                 logger,
		logrus:                 l,
		storage:                storageConfig,
		storageObserver:        cfg.StorageObserver,
		storageQuota:           cfg.StorageQuota,
		storageOperations:      cfg.StorageOperations,
		activeConnections:      cfg.ActiveConnections,
		s3CredentialCodec:      s3CredentialCodec,
		s3CredentialCodecErr:   s3CredentialCodecErr,
		repo:                   cfg.Repository,
		clusterID:              naming.ClusterIDOrDefault(&storageConfig.DefaultClusterId),
		podName:                strings.TrimSpace(cfg.PodName),
		podNamespace:           strings.TrimSpace(cfg.PodNamespace),
		ownerID:                strings.TrimSpace(cfg.OwnerID),
		heartbeatInterval:      heartbeatInterval,
		ownerOnlyIdleTTL:       ownerOnlyIdleTTL,
		portalCacheMaxBytes:    portalCacheMaxBytes,
		portalRootMinFreeBytes: portalRootMinFreeBytes,
		staleMountCleaner:      staleCleaner,
		staleMountChecker:      staleChecker,
		activePodUIDLister:     cfg.ActivePodUIDLister,
		materializerLimiter:    make(chan struct{}, materializerConcurrency),
		recoveryStore:          newPortalRecoveryStore(rootDir),
		replicator:             cfg.Replicator,
		requireStandby:         cfg.RequireStandby,
		portals:                make(map[string]*portalMount),
		portalsByTarget:        make(map[string]*portalMount),
		boundVolumes:           make(map[string]*boundVolume),
		volumes:                newLocalVolumeManager(),
	}
	manager.volumeAPI = newMountedVolumeAPIHandler(
		storageConfig,
		cfg.Repository,
		manager.volumes,
		cfg.StorageQuota,
		cfg.StorageOperations,
		cfg.ActiveConnections,
		l,
	)
	return manager
}

func (m *Manager) MountedVolumeHandler() http.Handler {
	if m == nil {
		return nil
	}
	return m.volumeAPI
}

type recoveryErrorReporter interface {
	RecoveryError() error
}

func (m *Manager) RecoveryError() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, pm := range m.portals {
		if pm == nil {
			continue
		}
		if reporter, ok := pm.rootfsSession.(recoveryErrorReporter); ok {
			if err := reporter.RecoveryError(); err != nil {
				return fmt.Errorf("portal %s rootfs recovery state: %w", key, err)
			}
		}
	}
	for volumeID, bound := range m.boundVolumes {
		if bound == nil {
			continue
		}
		if reporter, ok := bound.session.(recoveryErrorReporter); ok {
			if err := reporter.RecoveryError(); err != nil {
				return fmt.Errorf("volume %s recovery state: %w", volumeID, err)
			}
		}
	}
	return nil
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

// Shutdown detaches portal mounts without waiting on FUSE servers that may already be unresponsive.
func (m *Manager) Shutdown(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	targets := make([]string, 0, len(m.portalsByTarget))
	for target := range m.portalsByTarget {
		targets = append(targets, target)
	}
	ownerOnlyVolumeIDs := make([]string, 0, len(m.boundVolumes))
	for volumeID, bound := range m.boundVolumes {
		if bound == nil || bound.refCount > 0 || bound.closing {
			continue
		}
		ownerOnlyVolumeIDs = append(ownerOnlyVolumeIDs, volumeID)
	}
	m.mu.Unlock()

	var firstErr error
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		if err := m.unpublishPortalContext(ctx, target, true); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, volumeID := range ownerOnlyVolumeIDs {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		if err := m.releaseOwnerOnlyVolumeNow(ctx, volumeID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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
	rootfsStatePath := m.rootFSStatePath(key)
	rootfsSession, err := newRootFSBackedSessionWithState(rootfsBackingPath, rootfsStatePath)
	if err != nil {
		return fmt.Errorf("open rootfs portal recovery state: %w", err)
	}
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
		rootfsStatePath:   rootfsStatePath,
		rootfsSession:     rootfsSession,
	}
	if err := m.publishRecoveryState(ctx, pm); err != nil {
		_ = server.Unmount()
		rootfsSession.Close()
		return err
	}

	m.mu.Lock()
	m.portals[key] = pm
	m.portalsByTarget[req.TargetPath] = pm
	m.mu.Unlock()
	return nil
}

func mountPortalFS(fs *volumefuse.FileSystem, targetPath string) (*fuseportal.Server, error) {
	server, err := fuseportal.Mount(fs, targetPath, portalMountOptions())
	if err != nil {
		return nil, fmt.Errorf("mount volume portal fuse: %w", err)
	}
	servePortalFS(server)
	return server, nil
}

func portalMountOptions() *fuse.MountOptions {
	return &fuse.MountOptions{
		FsName:        "sandbox0-volume-portal",
		Name:          "sandbox0-volume",
		MaxBackground: 128,
		EnableLocks:   true,
		AllowOther:    os.Getuid() == 0,
		DirectMount:   true,
		MaxWrite:      256 * 1024,
		// Linux 6.17 rejects ALLOW_IDMAP without default_permissions.
		DisabledCapabilities: fuse.CAP_ALLOW_IDMAP,
	}
}

func servePortalFS(server *fuseportal.Server) {
	go func() {
		if err := server.Serve(); err != nil {
			log.Printf("volume portal FUSE server stopped with error: %v", err)
		}
	}()
}

// RestorePortal promotes a synchronized standby channel into an active portal.
// The caller must hold the node primary lock before invoking this method. The
// channel remains caller-owned on error and is consumed after a successful
// attach so transient backend failures can be retried without losing the last
// userspace reference to the kernel FUSE connection.
func (m *Manager) RestorePortal(ctx context.Context, manifest RecoveryManifest, channel *os.File) error {
	if err := validateRecoveryManifest(manifest); err != nil {
		return err
	}
	if channel == nil {
		return fmt.Errorf("portal recovery channel is required")
	}
	m.mu.Lock()
	if existing := m.portals[manifest.Key]; existing != nil {
		m.mu.Unlock()
		_ = channel.Close()
		return nil
	}
	m.mu.Unlock()

	if err := os.MkdirAll(manifest.RootFSBackingPath, 0o755); err != nil {
		return fmt.Errorf("restore rootfs portal backing directory: %w", err)
	}
	statePath := strings.TrimSpace(manifest.RootFSStatePath)
	if statePath == "" {
		statePath = m.rootFSStatePath(manifest.Key)
	}
	manifest.RootFSStatePath = statePath
	if manifest.VolumeID != "" && strings.TrimSpace(manifest.VolumeStatePath) == "" {
		manifest.VolumeStatePath = m.volumeStatePath(manifest.VolumeID)
	}
	if err := m.recoveryStore.Put(manifest); err != nil {
		return fmt.Errorf("persist restored portal recovery state: %w", err)
	}
	rootfsSession, err := newRootFSBackedSessionWithState(manifest.RootFSBackingPath, statePath)
	if err != nil {
		return fmt.Errorf("restore rootfs portal state: %w", err)
	}
	fs := volumefuse.New(manifest.Key, time.Second, rootfsSession)
	pm := &portalMount{
		namespace:         manifest.Namespace,
		podName:           manifest.PodName,
		podUID:            manifest.PodUID,
		name:              manifest.Name,
		mountPath:         manifest.MountPath,
		targetPath:        manifest.TargetPath,
		fs:                fs,
		rootfsBackingPath: manifest.RootFSBackingPath,
		rootfsStatePath:   statePath,
		rootfsSession:     rootfsSession,
		volumeStatePath:   manifest.VolumeStatePath,
	}

	var cleanupNewBound func()
	boundAttached := false
	if manifest.VolumeID != "" {
		volumeRecord, err := m.validateBindableVolume(ctx, ctldBindContext{volumeID: manifest.VolumeID, teamID: manifest.TeamID})
		if err != nil {
			rootfsSession.Close()
			return fmt.Errorf("validate restored portal volume: %w", err)
		}
		accessMode, err := validateBindableAccessMode(volumeRecord.AccessMode)
		if err != nil {
			rootfsSession.Close()
			return err
		}
		mountedAt := manifest.MountedAt
		if mountedAt.IsZero() {
			mountedAt = time.Now().UTC()
		}
		m.mu.Lock()
		bound := m.boundVolumes[manifest.VolumeID]
		m.mu.Unlock()
		newlyOwned := bound == nil
		if newlyOwned {
			bound, cleanupNewBound, err = m.openBoundVolume(ctx, ctldapi.BindVolumePortalRequest{
				PodUID:          manifest.PodUID,
				PortalName:      manifest.Name,
				MountPath:       manifest.MountPath,
				SandboxVolumeID: manifest.VolumeID,
				TeamID:          manifest.TeamID,
			}, volumeRecord, accessMode, mountedAt)
			if err != nil {
				rootfsSession.Close()
				return fmt.Errorf("open restored portal volume: %w", err)
			}
		}
		m.mu.Lock()
		if existing := m.boundVolumes[manifest.VolumeID]; existing != nil {
			bound = existing
			newlyOwned = false
			if cleanupNewBound != nil {
				cleanupNewBound()
			}
		} else {
			m.boundVolumes[manifest.VolumeID] = bound
			m.volumes.add(bound.volCtx)
			if err := m.registerOwner(ctx, bound); err != nil {
				delete(m.boundVolumes, manifest.VolumeID)
				m.volumes.remove(manifest.VolumeID)
				m.mu.Unlock()
				cleanupNewBound()
				rootfsSession.Close()
				return fmt.Errorf("register restored portal owner: %w", err)
			}
			m.startMaterializer(bound)
		}
		m.attachPortalLocked(pm, bound, mountedAt)
		if !newlyOwned {
			bound.refCount++
		}
		m.mu.Unlock()
		boundAttached = true
	}

	server, err := fuseportal.Attach(fs, channel, manifest.TargetPath, manifest.InitRequest, portalMountOptions())
	if err != nil {
		if boundAttached {
			m.mu.Lock()
			cleanup := m.unbindLockedSnapshot(pm)
			m.mu.Unlock()
			if cleanupErr := m.finishBoundVolumeHandoff(ctx, cleanup); cleanupErr != nil {
				m.logger.Warn("Failed to preserve bound volume after FUSE attach failure", zap.Error(cleanupErr), zap.String("volume_id", manifest.VolumeID))
			}
		}
		rootfsSession.Close()
		return fmt.Errorf("attach restored FUSE portal: %w", err)
	}
	_ = channel.Close()
	pm.server = server

	m.mu.Lock()
	m.portals[manifest.Key] = pm
	m.portalsByTarget[manifest.TargetPath] = pm
	m.mu.Unlock()
	servePortalFS(server)
	return nil
}

// SyncStandby sends every active portal and a fresh cloned channel to the
// currently connected standby.
func (m *Manager) SyncStandby(ctx context.Context) error {
	if m == nil || m.replicator == nil || !m.replicator.Ready() {
		return fmt.Errorf("ctld standby is not synchronized")
	}
	return m.SyncTo(ctx, m.replicator)
}

// SyncTo sends a point-in-time portal snapshot to one standby transport.
// The transport serializes this snapshot with subsequent incremental updates.
func (m *Manager) SyncTo(ctx context.Context, target PortalReplicator) error {
	if m == nil || target == nil || !target.Ready() {
		return fmt.Errorf("ctld standby is not synchronized")
	}
	if !supportsRecoveryCapability(target, RecoveryCapabilityS0FSHandleJournal) {
		if err := m.compactS0FSHandleRecoveryStates(false); err != nil {
			return fmt.Errorf("compact S0FS recovery state for legacy standby: %w", err)
		}
	}
	m.mu.Lock()
	portals := make([]*portalMount, 0, len(m.portals))
	for _, pm := range m.portals {
		portals = append(portals, pm)
	}
	m.mu.Unlock()
	for _, pm := range portals {
		if pm == nil || pm.server == nil {
			continue
		}
		channel, err := pm.server.CloneChannel()
		if err != nil {
			return err
		}
		manifest := m.snapshotRecoveryManifest(pm)
		err = target.Publish(ctx, manifest, channel)
		_ = channel.Close()
		if err != nil {
			return fmt.Errorf("sync portal %s to standby: %w", manifest.Key, err)
		}
	}
	return nil
}

func (m *Manager) UnpublishPortal(targetPath string) error {
	return m.UnpublishPortalContext(context.Background(), targetPath)
}

func (m *Manager) UnpublishPortalContext(ctx context.Context, targetPath string) error {
	return m.unpublishPortalContext(ctx, targetPath, false)
}

func (m *Manager) unpublishPortalContext(ctx context.Context, targetPath string, detach bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	pm := m.portalsByTarget[targetPath]
	var cleanup *boundVolumeCleanup
	if pm != nil {
		delete(m.portalsByTarget, targetPath)
		delete(m.portals, portalKey(pm.podUID, pm.name))
		cleanup = m.unbindLockedSnapshot(pm)
	}
	m.mu.Unlock()
	if pm == nil {
		return m.cleanUnknownStaleMountTarget(targetPath)
	}
	var firstErr error
	handoff := detach && m.replicator != nil && m.replicator.Ready()
	if !handoff {
		if err := m.removeRecoveryState(ctx, pm); err != nil {
			firstErr = err
		}
	}
	if detach {
		if pm.server != nil {
			if err := pm.server.Detach(); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		if !handoff {
			if err := m.cleanStaleMountTarget(pm.targetPath); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	} else if pm.server != nil {
		if err := pm.server.Unmount(); err != nil && firstErr == nil {
			if firstErr == nil {
				firstErr = err
			}
		}
		if cleanupErr := m.cleanStaleMountTarget(pm.targetPath); cleanupErr != nil && firstErr == nil {
			firstErr = cleanupErr
		}
	}
	if pm.rootfsSession != nil {
		pm.rootfsSession.Close()
	}
	if !handoff && pm.rootfsBackingPath != "" {
		if err := os.RemoveAll(pm.rootfsBackingPath); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if !handoff && pm.rootfsStatePath != "" {
		if err := os.Remove(pm.rootfsStatePath); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
			firstErr = err
		}
	}
	var cleanupErr error
	if handoff {
		cleanupErr = m.finishBoundVolumeHandoff(ctx, cleanup)
	} else {
		cleanupErr = m.finishBoundVolumeCleanup(ctx, cleanup)
	}
	if cleanupErr != nil && firstErr == nil {
		firstErr = cleanupErr
	}
	return firstErr
}

func (m *Manager) publishRecoveryState(ctx context.Context, pm *portalMount) error {
	manifest := recoveryManifest(pm)
	if err := m.recoveryStore.Put(manifest); err != nil {
		return fmt.Errorf("persist portal recovery state: %w", err)
	}
	if m.replicator == nil {
		if m.requireStandby {
			_ = m.recoveryStore.Delete(manifest.Key)
			return fmt.Errorf("ctld standby is required but unavailable")
		}
		return nil
	}
	if !m.replicator.Ready() {
		if m.requireStandby {
			_ = m.recoveryStore.Delete(manifest.Key)
			return fmt.Errorf("ctld standby is not synchronized")
		}
		return nil
	}
	channel, err := pm.server.CloneChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	if err := m.replicator.Publish(ctx, manifest, channel); err != nil {
		return fmt.Errorf("replicate portal recovery state: %w", err)
	}
	return nil
}

func (m *Manager) updateRecoveryState(ctx context.Context, pm *portalMount) error {
	manifest := m.snapshotRecoveryManifest(pm)
	if err := m.recoveryStore.Put(manifest); err != nil {
		return fmt.Errorf("persist portal recovery state: %w", err)
	}
	if m.replicator != nil && m.replicator.Ready() {
		if err := m.replicator.Update(ctx, manifest); err != nil {
			return fmt.Errorf("replicate portal recovery state: %w", err)
		}
	} else if m.requireStandby {
		return fmt.Errorf("ctld standby is not synchronized")
	}
	return nil
}

func (m *Manager) snapshotRecoveryManifest(pm *portalMount) RecoveryManifest {
	if m == nil {
		return RecoveryManifest{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return recoveryManifest(pm)
}

func (m *Manager) removeRecoveryState(ctx context.Context, pm *portalMount) error {
	if pm == nil {
		return nil
	}
	key := portalKey(pm.podUID, pm.name)
	var firstErr error
	if m.replicator != nil && m.replicator.Ready() {
		if err := m.replicator.Remove(ctx, key); err != nil {
			firstErr = fmt.Errorf("remove replicated portal recovery state: %w", err)
		}
	}
	if err := m.recoveryStore.Delete(key); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (m *Manager) cleanStaleMountTarget(targetPath string) error {
	if strings.TrimSpace(targetPath) == "" {
		return nil
	}
	cleaner := defaultStaleMountCleaner
	if m != nil && m.staleMountCleaner != nil {
		cleaner = m.staleMountCleaner
	}
	return cleaner(targetPath)
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
		return m.finishBindRecovery(ctx, pm, response)
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
		return m.finishBindRecovery(ctx, pm, response)
	}
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		if bound.closing {
			m.mu.Unlock()
			return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume %s is closing", req.SandboxVolumeID)
		}
		if bound.refCount == 0 {
			m.attachPortalLocked(pm, bound, mountedAt)
			bound.refCount = 1
			response := boundResponse(pm)
			m.mu.Unlock()
			return m.finishBindRecovery(ctx, pm, response)
		}
		if accessMode != volume.AccessModeROX {
			conflictPath := boundMountPath(m.portals, req.SandboxVolumeID, key)
			m.mu.Unlock()
			return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
		}
		m.attachPortalLocked(pm, bound, mountedAt)
		bound.refCount++
		response := boundResponse(pm)
		m.mu.Unlock()
		return m.finishBindRecovery(ctx, pm, response)
	}
	if existing := findBoundPortalForVolume(m.portals, req.SandboxVolumeID, key); existing != nil {
		conflictPath := existing.mountPath
		m.mu.Unlock()
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
	}
	m.mu.Unlock()

	newBound, cleanupNewBound, err := m.openBoundVolume(ctx, req, volumeRecord, accessMode, mountedAt)
	if err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}

	m.mu.Lock()
	pm = m.portals[key]
	if pm == nil {
		m.mu.Unlock()
		cleanupNewBound()
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume portal %s for pod %s is not published", portalName, req.PodUID)
	}
	if pm.volumeID != "" {
		response := boundResponse(pm)
		m.mu.Unlock()
		cleanupNewBound()
		if response.SandboxVolumeID != req.SandboxVolumeID {
			return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume portal already bound to %s", response.SandboxVolumeID)
		}
		return m.finishBindRecovery(ctx, pm, response)
	}
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		if bound.closing {
			m.mu.Unlock()
			cleanupNewBound()
			return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume %s is closing", req.SandboxVolumeID)
		}
		if bound.refCount == 0 {
			m.attachPortalLocked(pm, bound, mountedAt)
			bound.refCount = 1
			response := boundResponse(pm)
			m.mu.Unlock()
			cleanupNewBound()
			return m.finishBindRecovery(ctx, pm, response)
		}
		if accessMode != volume.AccessModeROX {
			conflictPath := boundMountPath(m.portals, req.SandboxVolumeID, key)
			m.mu.Unlock()
			cleanupNewBound()
			return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
		}
		m.attachPortalLocked(pm, bound, mountedAt)
		bound.refCount++
		response := boundResponse(pm)
		m.mu.Unlock()
		cleanupNewBound()
		return m.finishBindRecovery(ctx, pm, response)
	}
	if existing := findBoundPortalForVolume(m.portals, req.SandboxVolumeID, key); existing != nil {
		conflictPath := existing.mountPath
		m.mu.Unlock()
		cleanupNewBound()
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
	}
	bound := newBound
	m.boundVolumes[req.SandboxVolumeID] = bound
	m.volumes.add(bound.volCtx)
	m.attachPortalLocked(pm, bound, mountedAt)
	if err := m.registerOwner(ctx, bound); err != nil {
		m.clearPortalLocked(pm)
		delete(m.boundVolumes, req.SandboxVolumeID)
		m.volumes.remove(req.SandboxVolumeID)
		m.mu.Unlock()
		cleanupNewBound()
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("register ctld volume owner: %w", err)
	}
	m.startMaterializer(bound)
	response := boundResponse(pm)
	m.mu.Unlock()

	return m.finishBindRecovery(ctx, pm, response)
}

func (m *Manager) finishBindRecovery(ctx context.Context, pm *portalMount, response ctldapi.BindVolumePortalResponse) (ctldapi.BindVolumePortalResponse, error) {
	if err := m.updateRecoveryState(ctx, pm); err != nil {
		return response, err
	}
	return response, nil
}

func (m *Manager) openBoundVolume(ctx context.Context, req ctldapi.BindVolumePortalRequest, volumeRecord *db.SandboxVolume, accessMode volume.AccessMode, mountedAt time.Time) (*boundVolume, func(), error) {
	if volumeRecord == nil {
		return nil, nil, fmt.Errorf("volume record is required")
	}
	switch volume.NormalizeBackend(volumeRecord.Backend) {
	case volume.BackendS0FS:
		return m.openS0FSBoundVolume(ctx, req, volumeRecord, accessMode, mountedAt)
	case volume.BackendS3:
		return m.openS3BoundVolume(req, volumeRecord, accessMode, mountedAt)
	default:
		return nil, nil, fmt.Errorf("unsupported volume backend %q", volumeRecord.Backend)
	}
}

func (m *Manager) openS0FSBoundVolume(ctx context.Context, req ctldapi.BindVolumePortalRequest, volumeRecord *db.SandboxVolume, accessMode volume.AccessMode, mountedAt time.Time) (*boundVolume, func(), error) {
	cacheDir := filepath.Join(m.rootDir, "volumes", safePath(req.TeamID), safePath(req.SandboxVolumeID))
	statePath := m.volumeStatePath(req.SandboxVolumeID)
	handleState, err := loadS0FSHandleState(statePath, req.SandboxVolumeID)
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return nil, nil, fmt.Errorf("create local volume dir: %w", err)
	}
	remoteStore, err := m.createObjectStore(req.TeamID, req.SandboxVolumeID)
	if err != nil {
		return nil, nil, fmt.Errorf("create object storage: %w", err)
	}
	encryption, err := volume.S0FSEncryptionConfig(m.storage)
	if err != nil {
		return nil, nil, err
	}
	segmentTargetSize, err := volume.S0FSSegmentTargetSize(m.storage)
	if err != nil {
		return nil, nil, err
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
		RetainUnlinked: true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open local s0fs engine: %w", err)
	}
	volCtx := m.newS0FSVolumeContext(req.SandboxVolumeID, volumeRecord.TeamID, engine, accessMode, mountedAt, cacheDir)
	volCtx.RestoreHandleState(handleState)
	engine.PruneUnlinked(retainedUnlinkedInodes(handleState))
	session := newLocalSession(req.SandboxVolumeID, m.volumes, m.storageQuota, m.storageOperations, m.logrus)
	session.statePath = statePath
	session.incrementalReady = m.incrementalS0FSHandleRecoveryReady
	if err := compactS0FSHandleState(statePath, req.SandboxVolumeID, volCtx.SnapshotHandleState(), true, nil); err != nil {
		_ = engine.Close()
		return nil, nil, err
	}
	bound := &boundVolume{
		volumeID:  req.SandboxVolumeID,
		teamID:    volumeRecord.TeamID,
		access:    accessMode,
		mountedAt: mountedAt,
		refCount:  1,
		volCtx:    volCtx,
		session:   session,
		statePath: statePath,
	}
	return bound, func() { _ = engine.Close() }, nil
}

func (m *Manager) newS0FSVolumeContext(
	volumeID, teamID string,
	engine *s0fs.Engine,
	accessMode volume.AccessMode,
	mountedAt time.Time,
	cacheDir string,
) *volume.VolumeContext {
	return &volume.VolumeContext{
		VolumeID:  volumeID,
		TeamID:    teamID,
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    accessMode,
		MountedAt: mountedAt,
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
		Observer:  m.storageObserver,
	}
}

func (m *Manager) openS3BoundVolume(req ctldapi.BindVolumePortalRequest, volumeRecord *db.SandboxVolume, accessMode volume.AccessMode, mountedAt time.Time) (*boundVolume, func(), error) {
	if accessMode == volume.AccessModeRWX {
		return nil, nil, fmt.Errorf("s3 backend does not support RWX access_mode")
	}
	if m.s3CredentialCodecErr != nil {
		return nil, nil, fmt.Errorf("s3 backend credential encryption is not configured")
	}
	cfg, err := volume.DecodeS3BackendConfigWithCredentials(context.Background(), volumeRecord.TeamID, volumeRecord.ID, volumeRecord.BackendConfig, m.s3CredentialCodec)
	if err != nil {
		return nil, nil, err
	}
	store, err := objectstore.Create(volume.S3ObjectStoreConfig(cfg, m.storage, nil))
	if err != nil {
		return nil, nil, fmt.Errorf("create s3 object storage: %w", err)
	}
	if cfg.Prefix != "" {
		store = objectstore.Prefix(store, cfg.Prefix)
	}
	if store == nil {
		return nil, nil, fmt.Errorf("s3 object storage is not configured")
	}
	statePath := m.volumeStatePath(req.SandboxVolumeID)
	session, err := newS3SessionWithState(req.SandboxVolumeID, store, accessMode, m.logrus, statePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open s3 portal recovery state: %w", err)
	}
	volCtx := &volume.VolumeContext{
		VolumeID:  req.SandboxVolumeID,
		TeamID:    volumeRecord.TeamID,
		Backend:   volume.BackendS3,
		Access:    accessMode,
		MountedAt: mountedAt,
		RootInode: 1,
		RootPath:  "/",
	}
	bound := &boundVolume{
		volumeID:  req.SandboxVolumeID,
		teamID:    volumeRecord.TeamID,
		access:    accessMode,
		mountedAt: mountedAt,
		refCount:  1,
		volCtx:    volCtx,
		session:   session,
		statePath: statePath,
	}
	return bound, func() { _ = session.Handoff() }, nil
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
	cleanup := m.unbindLockedSnapshot(pm)
	m.mu.Unlock()
	if err := m.updateRecoveryState(ctx, pm); err != nil {
		return ctldapi.UnbindVolumePortalResponse{}, err
	}
	if err := m.finishBoundVolumeCleanup(ctx, cleanup); err != nil {
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
	if volume.NormalizeBackend(volumeRecord.Backend) != volume.BackendS0FS {
		return ctldapi.AttachVolumeOwnerResponse{}, fmt.Errorf("ctld owner attach is only supported for s0fs volumes")
	}

	m.mu.Lock()
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		if bound.closing {
			m.mu.Unlock()
			return ctldapi.AttachVolumeOwnerResponse{}, fmt.Errorf("volume %s is closing", req.SandboxVolumeID)
		}
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
	volCtx := m.newS0FSVolumeContext(req.SandboxVolumeID, volumeRecord.TeamID, engine, accessMode, mountedAt, cacheDir)

	m.mu.Lock()
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		if bound.closing {
			m.mu.Unlock()
			_ = engine.Close()
			return ctldapi.AttachVolumeOwnerResponse{}, fmt.Errorf("volume %s is closing", req.SandboxVolumeID)
		}
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
		session:   newLocalSession(req.SandboxVolumeID, m.volumes, m.storageQuota, m.storageOperations, m.logrus),
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
	if bound.closing {
		m.mu.Unlock()
		err := fmt.Errorf("volume %s is closing", volumeID)
		return ctldapi.ReleaseVolumeOwnerResponse{Busy: true, Error: err.Error()}, err
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
	cleanup := m.releaseOwnerOnlyVolumeLocked(volumeID, bound)
	m.mu.Unlock()
	if err := m.finishBoundVolumeCleanup(ctx, cleanup); err != nil {
		return ctldapi.ReleaseVolumeOwnerResponse{}, err
	}
	return ctldapi.ReleaseVolumeOwnerResponse{Released: true}, nil
}

func (m *Manager) PrepareSnapshotCheckpoint(ctx context.Context, req ctldapi.PrepareVolumeSnapshotCheckpointRequest) (ctldapi.PrepareVolumeSnapshotCheckpointResponse, error) {
	volumeID := strings.TrimSpace(req.SandboxVolumeID)
	if volumeID == "" {
		return ctldapi.PrepareVolumeSnapshotCheckpointResponse{}, fmt.Errorf("sandboxvolume_id is required")
	}
	m.mu.Lock()
	bound := m.boundVolumes[volumeID]
	closing := bound != nil && bound.closing
	m.mu.Unlock()
	if bound == nil || bound.volCtx == nil || bound.volCtx.S0FS == nil {
		return ctldapi.PrepareVolumeSnapshotCheckpointResponse{}, fmt.Errorf("volume %s is not owned by this ctld", volumeID)
	}
	if closing {
		return ctldapi.PrepareVolumeSnapshotCheckpointResponse{}, fmt.Errorf("volume %s is closing", volumeID)
	}
	if err := m.volumes.prepareSnapshotCheckpoint(ctx, volumeID); err != nil {
		return ctldapi.PrepareVolumeSnapshotCheckpointResponse{}, err
	}
	materialization, err := bound.volCtx.SyncMaterialize(ctx)
	if err != nil {
		m.volumes.completeSnapshotCheckpoint(volumeID)
		return ctldapi.PrepareVolumeSnapshotCheckpointResponse{}, err
	}
	m.logStorageObservationError(volumeID, materialization.ObservationError)
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
		if bound == nil || bound.closing || bound.refCount > 0 || !m.volumes.canCleanupOwnerOnly(volumeID, cutoff) {
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
	if bound == nil || bound.closing || bound.refCount > 0 || !m.volumes.canCleanupOwnerOnly(volumeID, time.Now().UTC().Add(-m.ownerOnlyIdleTTL)) {
		m.mu.Unlock()
		return nil
	}
	cleanup := m.releaseOwnerOnlyVolumeLocked(volumeID, bound)
	m.mu.Unlock()
	return m.finishBoundVolumeCleanup(ctx, cleanup)
}

func (m *Manager) releaseOwnerOnlyVolumeNow(ctx context.Context, volumeID string) error {
	m.mu.Lock()
	bound := m.boundVolumes[volumeID]
	if bound == nil || bound.closing {
		m.mu.Unlock()
		return nil
	}
	if bound.refCount > 0 {
		m.mu.Unlock()
		return fmt.Errorf("volume %s is actively bound to a portal", volumeID)
	}
	if ok, reason := m.volumes.canReleaseOwnerOnly(volumeID); !ok {
		m.mu.Unlock()
		return fmt.Errorf("volume %s %s", volumeID, reason)
	}
	cleanup := m.releaseOwnerOnlyVolumeLocked(volumeID, bound)
	m.mu.Unlock()
	return m.finishBoundVolumeCleanup(ctx, cleanup)
}

func (m *Manager) releaseOwnerOnlyVolumeLocked(volumeID string, bound *boundVolume) *boundVolumeCleanup {
	if bound == nil {
		return nil
	}
	bound.closing = true
	cleanup := &boundVolumeCleanup{
		volumeID:          volumeID,
		bound:             bound,
		materializeCancel: bound.materializeCancel,
		materializeDone:   bound.materializeDone,
	}
	bound.materializeCancel = nil
	bound.materializeDone = nil
	return cleanup
}

func (m *Manager) unbindLockedSnapshot(pm *portalMount) *boundVolumeCleanup {
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
	bound.refCount = 0
	return m.releaseOwnerOnlyVolumeLocked(volumeID, bound)
}

func (m *Manager) finishBoundVolumeCleanup(ctx context.Context, cleanup *boundVolumeCleanup) error {
	if cleanup == nil || cleanup.bound == nil || strings.TrimSpace(cleanup.volumeID) == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if cleanup.materializeCancel != nil {
		cleanup.materializeCancel()
	}
	if cleanup.materializeDone != nil {
		select {
		case <-cleanup.materializeDone:
		case <-ctx.Done():
			m.markBoundVolumeCleanupFailed(cleanup)
			return ctx.Err()
		}
	}
	closeBoundSession(cleanup.bound)
	if err := m.volumes.UnmountVolume(ctx, cleanup.volumeID, ""); err != nil {
		m.markBoundVolumeCleanupFailed(cleanup)
		return err
	}
	unregister := false
	m.mu.Lock()
	if m.boundVolumes[cleanup.volumeID] == cleanup.bound {
		delete(m.boundVolumes, cleanup.volumeID)
		unregister = true
	}
	m.mu.Unlock()
	if unregister {
		m.unregisterOwner(cleanup.bound)
	}
	if cleanup.bound.statePath != "" {
		if err := removeS0FSHandleRecoveryState(cleanup.bound.statePath); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) finishBoundVolumeHandoff(ctx context.Context, cleanup *boundVolumeCleanup) error {
	if cleanup == nil || cleanup.bound == nil || strings.TrimSpace(cleanup.volumeID) == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if cleanup.materializeCancel != nil {
		cleanup.materializeCancel()
	}
	if cleanup.materializeDone != nil {
		select {
		case <-cleanup.materializeDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := handoffBoundSession(cleanup.bound); err != nil {
		return err
	}
	if err := m.volumes.HandoffVolume(cleanup.volumeID); err != nil {
		return err
	}
	m.mu.Lock()
	if m.boundVolumes[cleanup.volumeID] == cleanup.bound {
		delete(m.boundVolumes, cleanup.volumeID)
	}
	m.mu.Unlock()
	m.stopOwnerHeartbeat(cleanup.bound)
	return nil
}

func (m *Manager) markBoundVolumeCleanupFailed(cleanup *boundVolumeCleanup) {
	if m == nil || cleanup == nil || cleanup.bound == nil || strings.TrimSpace(cleanup.volumeID) == "" {
		return
	}
	m.mu.Lock()
	if m.boundVolumes[cleanup.volumeID] == cleanup.bound {
		cleanup.bound.closing = false
	}
	m.mu.Unlock()
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
		if jitter := materializerInitialJitter(volumeID); jitter > 0 {
			timer := time.NewTimer(jitter)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		ticker := time.NewTicker(defaultVolumeMaterializeInterval)
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
				ran, err := m.tryRunMaterializer(ctx, func() error {
					materialization, err := bound.volCtx.SyncMaterialize(ctx)
					if err != nil {
						return err
					}
					m.logStorageObservationError(volumeID, materialization.ObservationError)
					m.garbageCollectBoundS0FS(ctx, bound, materialization.Manifest)
					return nil
				})
				if !ran {
					continue
				}
				if err != nil && m.logger != nil {
					m.logger.Warn("ctld volume materialize failed", zap.String("volume_id", volumeID), zap.Error(err))
				}
			case <-compactionC:
				var result *s0fs.CompactionResult
				ran, err := m.tryRunMaterializer(ctx, func() error {
					var materialization volume.MaterializationResult
					var compactErr error
					materialization, result, compactErr = bound.volCtx.Compact(ctx, compactionOptions)
					if compactErr != nil {
						return compactErr
					}
					m.logStorageObservationError(volumeID, materialization.ObservationError)
					m.garbageCollectBoundS0FS(ctx, bound, materialization.Manifest)
					return nil
				})
				if !ran {
					continue
				}
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
			}
		}
	}(bound.volumeID)
}

func (m *Manager) logStorageObservationError(volumeID string, err error) {
	if err == nil || m == nil || m.logger == nil {
		return
	}
	m.logger.Warn("ctld volume storage observation failed", zap.String("volume_id", volumeID), zap.Error(err))
}

func (m *Manager) tryRunMaterializer(ctx context.Context, fn func() error) (bool, error) {
	if fn == nil {
		return true, nil
	}
	if m == nil || m.materializerLimiter == nil {
		return true, fn()
	}
	select {
	case m.materializerLimiter <- struct{}{}:
		defer func() { <-m.materializerLimiter }()
	case <-ctx.Done():
		return true, ctx.Err()
	default:
		return false, nil
	}
	return true, fn()
}

func materializerInitialJitter(volumeID string) time.Duration {
	if strings.TrimSpace(volumeID) == "" || defaultVolumeMaterializeInterval <= time.Millisecond {
		return 0
	}
	slots := uint32(defaultVolumeMaterializeInterval / time.Millisecond)
	if slots == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(volumeID))
	return time.Duration(h.Sum32()%slots) * time.Millisecond
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
	if current := bound.volCtx.S0FS.SnapshotReferenceState(); current != nil {
		retainedStates = append(retainedStates, current)
	}
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

func (m *Manager) attachPortalLocked(pm *portalMount, bound *boundVolume, mountedAt time.Time) {
	if pm == nil || bound == nil {
		return
	}
	if pm.fs != nil {
		session := bound.session
		if session == nil {
			session = newLocalSession(bound.volumeID, m.volumes, m.storageQuota, m.storageOperations, m.logrus)
			if local, ok := session.(*localSession); ok {
				local.statePath = bound.statePath
				local.incrementalReady = m.incrementalS0FSHandleRecoveryReady
			}
			bound.session = session
		}
		pm.fs.SetSession(session)
	}
	pm.volumeID = bound.volumeID
	pm.teamID = bound.teamID
	pm.access = bound.access
	pm.mountedAt = mountedAt
	pm.volumeStatePath = bound.statePath
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
	pm.access = ""
	pm.mountedAt = time.Time{}
	pm.volumeStatePath = ""
}

func closeBoundSession(bound *boundVolume) {
	if bound == nil || bound.session == nil {
		return
	}
	bound.session.Close()
	bound.session = nil
}

func (m *Manager) incrementalS0FSHandleRecoveryReady() bool {
	if m == nil || m.replicator == nil {
		return true
	}
	return supportsRecoveryCapability(m.replicator, RecoveryCapabilityS0FSHandleJournal)
}

func (m *Manager) compactS0FSHandleRecoveryStates(durable bool) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	sessions := make([]*localSession, 0, len(m.boundVolumes))
	for _, bound := range m.boundVolumes {
		if bound == nil {
			continue
		}
		if session, ok := bound.session.(*localSession); ok {
			sessions = append(sessions, session)
		}
	}
	m.mu.Unlock()
	for _, session := range sessions {
		if err := session.persistRecoveryStateWithDurability(durable); err != nil {
			return err
		}
	}
	return nil
}

type handoffSession interface {
	Handoff() error
}

func handoffBoundSession(bound *boundVolume) error {
	if bound == nil || bound.session == nil {
		return nil
	}
	if session, ok := bound.session.(handoffSession); ok {
		if err := session.Handoff(); err != nil {
			return err
		}
	} else {
		bound.session.Close()
	}
	bound.session = nil
	return nil
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

func (m *Manager) rootFSStatePath(key string) string {
	digest := sha256.Sum256([]byte(key))
	return filepath.Join(m.rootDir, "ha", "rootfs", hex.EncodeToString(digest[:])+".jsonl")
}

func (m *Manager) volumeStatePath(volumeID string) string {
	digest := sha256.Sum256([]byte(volumeID))
	return filepath.Join(m.rootDir, "ha", "volumes", hex.EncodeToString(digest[:])+".json")
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
