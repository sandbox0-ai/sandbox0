package volume

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sirupsen/logrus"
)

// MountRegistrar handles registration of volume mounts for distributed coordination
type MountRegistrar interface {
	RegisterMount(ctx context.Context, volumeID string, options MountOptions) error
	UnregisterMount(ctx context.Context, volumeID string) error
	ValidateMount(ctx context.Context, volumeID string, accessMode AccessMode) error
}

// VolumeConfig holds the configuration for a volume
type VolumeConfig struct {
	CacheSize  string
	Prefetch   int
	BufferSize string
	Writeback  bool
}

// VolumeContext holds the mounted runtime state for a volume.
type VolumeContext struct {
	VolumeID  string
	TeamID    string
	Backend   string
	S0FS      *s0fs.Engine
	Meta      meta.Meta
	Store     chunk.ChunkStore
	VFS       *vfs.VFS
	Config    *VolumeConfig
	Access    AccessMode
	MountedAt time.Time
	RootInode meta.Ino
	RootPath  string
	CacheDir  string

	handleMu      sync.Mutex
	nextHandleID  uint64
	fileHandles   map[uint64]uint64
	dirHandleIDs  map[uint64]uint64
	openFileCount map[uint64]int

	materializeCancel context.CancelFunc
	materializeDone   chan struct{}
}

// MountSession tracks a single mount session on this instance.
type MountSession struct {
	ID        string
	Secret    string
	TeamID    string
	SandboxID string
	CreatedAt time.Time
	Scope     MountSessionScope
}

// MountSessionPrincipal captures the authorization identity resolved from a
// mount session credential.
type MountSessionPrincipal struct {
	TeamID    string
	SandboxID string
}

type MountSessionScope string

const (
	MountSessionScopeUnknown MountSessionScope = ""
	MountSessionScopeSandbox MountSessionScope = "sandbox"
	MountSessionScopeDirect  MountSessionScope = "direct"
)

type invalidateTracker struct {
	pending map[string]struct{}
	done    chan struct{}
	err     error
}

type directMountLease struct {
	SessionID string
	InFlight  int
	LastUsed  time.Time
}

var errVolumeRootNotFound = errors.New("volume root not found")

type volumeRootMeta interface {
	Lookup(ctx meta.Context, parent meta.Ino, name string, inode *meta.Ino, attr *meta.Attr, checkPerm bool) syscall.Errno
	Mkdir(ctx meta.Context, parent meta.Ino, name string, mode uint16, cumask uint16, copysgid uint8, inode *meta.Ino, attr *meta.Attr) syscall.Errno
}

// Manager manages mounted volumes and mount sessions.
type Manager struct {
	mu               sync.RWMutex
	volumes          map[string]*VolumeContext
	sandboxToVolumes map[string]map[string]struct{} // sandboxID -> set of volumeIDs
	mountSessions    map[string]map[string]*MountSession
	directMounts     map[string]*directMountLease
	invalidates      map[string]map[string]*invalidateTracker
	logger           *logrus.Logger
	config           *config.StorageProxyConfig
	metrics          *obsmetrics.StorageProxyMetrics
	backend          Backend
	registrar        MountRegistrar // Optional: for distributed coordination
}

// NewManager creates a new volume manager
func NewManager(logger *logrus.Logger, cfg *config.StorageProxyConfig) *Manager {
	return NewManagerWithBackend(logger, cfg, nil)
}

// NewManagerWithBackend creates a manager with an explicit storage backend.
func NewManagerWithBackend(logger *logrus.Logger, cfg *config.StorageProxyConfig, backend Backend) *Manager {
	if backend == nil {
		backend = NewJuiceFSBackend(logger, cfg)
	}
	return &Manager{
		volumes:          make(map[string]*VolumeContext),
		sandboxToVolumes: make(map[string]map[string]struct{}),
		mountSessions:    make(map[string]map[string]*MountSession),
		directMounts:     make(map[string]*directMountLease),
		invalidates:      make(map[string]map[string]*invalidateTracker),
		logger:           logger,
		config:           cfg,
		backend:          backend,
	}
}

// SetMountRegistrar sets the mount registrar for distributed coordination.
// This should be called after coordinator is initialized.
func (m *Manager) SetMountRegistrar(registrar MountRegistrar) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registrar = registrar
}

func (m *Manager) SetMetrics(metrics *obsmetrics.StorageProxyMetrics) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics = metrics
}

// MountVolume mounts a volume using the configured storage backend.
// AccessMode is enforced per storage-proxy instance (not per sandbox).
func (m *Manager) MountVolume(ctx context.Context, s3Prefix, volumeID, teamID string, config *VolumeConfig, accessMode AccessMode) (string, string, time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	accessMode = NormalizeAccessMode(string(accessMode))
	sessionID := uuid.New().String()
	sessionTime := time.Now()

	if teamID == "" {
		return "", "", time.Time{}, fmt.Errorf("missing team id for volume mount")
	}

	// Validate mount with coordinator if available.
	if m.registrar != nil {
		if err := m.registrar.ValidateMount(ctx, volumeID, accessMode); err != nil {
			return "", "", time.Time{}, err
		}
	}

	sessionSecret, err := generateMountSessionSecret()
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("generate mount session secret: %w", err)
	}

	// Check if already mounted
	if existing, exists := m.volumes[volumeID]; exists {
		if existing.TeamID != "" && existing.TeamID != teamID {
			return "", "", time.Time{}, fmt.Errorf("volume %s already mounted by another team", volumeID)
		}
		if existing.Access != accessMode {
			return "", "", time.Time{}, fmt.Errorf("volume %s already mounted with access_mode=%s", volumeID, existing.Access)
		}
		if m.mountSessions[volumeID] == nil {
			m.mountSessions[volumeID] = make(map[string]*MountSession)
		}
		m.mountSessions[volumeID][sessionID] = &MountSession{
			ID:        sessionID,
			Secret:    sessionSecret,
			TeamID:    teamID,
			CreatedAt: sessionTime,
		}
		return sessionID, sessionSecret, sessionTime, nil
	}

	m.logger.WithField("volume_id", volumeID).Info("Mounting volume")

	volCtx, err := m.backend.MountVolume(ctx, BackendMountRequest{
		S3Prefix:   s3Prefix,
		VolumeID:   volumeID,
		TeamID:     teamID,
		Config:     config,
		AccessMode: accessMode,
		MountedAt:  sessionTime,
		Metrics:    m.metrics,
	})
	if err != nil {
		return "", "", time.Time{}, err
	}

	m.volumes[volumeID] = volCtx
	if m.mountSessions[volumeID] == nil {
		m.mountSessions[volumeID] = make(map[string]*MountSession)
	}
	m.mountSessions[volumeID][sessionID] = &MountSession{
		ID:        sessionID,
		Secret:    sessionSecret,
		TeamID:    teamID,
		CreatedAt: sessionTime,
	}

	// 7. Register mount for distributed coordination (if registrar is set)
	if m.registrar != nil {
		if err := m.registrar.RegisterMount(ctx, volumeID, MountOptions{
			AccessMode: accessMode,
		}); err != nil {
			m.logger.WithError(err).Warn("Failed to register mount for coordination")
		}
	}

	m.logger.WithFields(logrus.Fields{
		"volume_id":   volumeID,
		"backend":     volCtx.Backend,
		"cache_dir":   volCtx.CacheDir,
		"access_mode": accessMode,
	}).Info("Volume mounted successfully")

	return sessionID, sessionSecret, sessionTime, nil
}

// AuthenticateMountSession validates a mount session credential for a specific
// mounted volume and returns the principal bound to that session.
func (m *Manager) AuthenticateMountSession(volumeID, sessionID, sessionSecret string) (*MountSessionPrincipal, error) {
	if volumeID == "" || sessionID == "" || sessionSecret == "" {
		return nil, fmt.Errorf("missing mount session credential")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	volCtx, ok := m.volumes[volumeID]
	if !ok || volCtx == nil {
		return nil, fmt.Errorf("volume %s not mounted", volumeID)
	}
	sessions := m.mountSessions[volumeID]
	if len(sessions) == 0 {
		return nil, fmt.Errorf("mount session %s not found for volume %s", sessionID, volumeID)
	}
	session := sessions[sessionID]
	if session == nil {
		return nil, fmt.Errorf("mount session %s not found for volume %s", sessionID, volumeID)
	}
	if subtle.ConstantTimeCompare([]byte(session.Secret), []byte(sessionSecret)) != 1 {
		return nil, fmt.Errorf("invalid mount session secret")
	}

	teamID := session.TeamID
	if teamID == "" {
		teamID = volCtx.TeamID
	}
	if teamID == "" {
		return nil, fmt.Errorf("team id not found for mount session")
	}

	return &MountSessionPrincipal{
		TeamID:    teamID,
		SandboxID: session.SandboxID,
	}, nil
}

func buildMetaConf(cfg *config.StorageProxyConfig, readOnly bool) *meta.Config {
	metaConf := meta.DefaultConf()
	if cfg != nil {
		metaConf.Retries = cfg.JuiceFSMetaRetries
	}
	if metaConf.Retries == 0 {
		metaConf.Retries = 10
	}
	metaConf.ReadOnly = readOnly
	return metaConf
}

func resolveMountRoot(metaClient volumeRootMeta, path string, readOnly bool, ensureWritable func(string) (meta.Ino, error)) (meta.Ino, error) {
	if !readOnly {
		return ensureVolumeRoot(metaClient, path)
	}

	rootInode, err := lookupVolumeRoot(metaClient, path)
	if err == nil {
		return rootInode, nil
	}
	if !errors.Is(err, errVolumeRootNotFound) {
		return 0, err
	}
	if ensureWritable == nil {
		return 0, err
	}
	return ensureWritable(path)
}

func (m *Manager) ensureWritableVolumeRoot(path string) (meta.Ino, error) {
	if m == nil || m.config == nil {
		return 0, fmt.Errorf("storage proxy config is not available")
	}

	metaClient := meta.NewClient(m.config.MetaURL, buildMetaConf(m.config, false))
	defer func() {
		if err := metaClient.Shutdown(); err != nil && m.logger != nil {
			m.logger.WithError(err).Warn("Failed to shutdown writable metadata client after root initialization")
		}
	}()
	if _, err := metaClient.Load(true); err != nil {
		return 0, fmt.Errorf("load writable metadata: %w", err)
	}

	return ensureVolumeRoot(metaClient, path)
}

func lookupVolumeRoot(metaClient volumeRootMeta, path string) (meta.Ino, error) {
	return resolveVolumeRoot(metaClient, path, false)
}

// Use meta client directly to create the internal root path.
// This avoids FUSE/VFS semantics (handles/permissions) and keeps it
// consistent with snapshot operations which also use meta clients.
func ensureVolumeRoot(metaClient volumeRootMeta, path string) (meta.Ino, error) {
	return resolveVolumeRoot(metaClient, path, true)
}

func resolveVolumeRoot(metaClient volumeRootMeta, path string, createMissing bool) (meta.Ino, error) {
	if metaClient == nil {
		return 0, fmt.Errorf("meta client is nil")
	}

	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return meta.RootInode, nil
	}

	parts := strings.Split(trimmed, "/")
	current := meta.RootInode
	var attr meta.Attr
	jfsCtx := meta.Background()

	for _, part := range parts {
		var next meta.Ino
		errno := metaClient.Lookup(jfsCtx, current, part, &next, &attr, false)
		if errno == syscall.ENOENT {
			if !createMissing {
				return 0, fmt.Errorf("%w: %s", errVolumeRootNotFound, part)
			}
			errno = metaClient.Mkdir(jfsCtx, current, part, 0o755, 0, 0, &next, &attr)
			if errno != 0 && errno != syscall.EEXIST {
				return 0, fmt.Errorf("mkdir %s: %s", part, errno.Error())
			}
			if errno == syscall.EEXIST {
				errno = metaClient.Lookup(jfsCtx, current, part, &next, &attr, false)
				if errno != 0 {
					return 0, fmt.Errorf("lookup after mkdir %s: %s", part, errno.Error())
				}
			}
		} else if errno != 0 {
			return 0, fmt.Errorf("lookup %s: %s", part, errno.Error())
		}
		current = next
	}

	return current, nil
}

// UnmountVolume unmounts a volume session and unmounts the volume if it is the last session.
func (m *Manager) UnmountVolume(ctx context.Context, volumeID, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	volCtx, ok := m.volumes[volumeID]
	if !ok {
		return fmt.Errorf("volume %s not mounted", volumeID)
	}
	if sessionID == "" {
		return fmt.Errorf("missing mount session id")
	}
	sessions, ok := m.mountSessions[volumeID]
	if !ok {
		return fmt.Errorf("mount session %s not found for volume %s", sessionID, volumeID)
	}
	if _, ok := sessions[sessionID]; !ok {
		return fmt.Errorf("mount session %s not found for volume %s", sessionID, volumeID)
	}
	delete(sessions, sessionID)
	if lease := m.directMounts[volumeID]; lease != nil && lease.SessionID == sessionID {
		delete(m.directMounts, volumeID)
	}
	m.clearSessionFromInvalidatesLocked(volumeID, sessionID)
	if len(sessions) > 0 {
		return nil
	}
	delete(m.mountSessions, volumeID)

	m.logger.WithField("volume_id", volumeID).Info("Unmounting volume")

	// Unregister mount from distributed coordination (if registrar is set)
	if m.registrar != nil {
		if err := m.registrar.UnregisterMount(ctx, volumeID); err != nil {
			m.logger.WithError(err).Warn("Failed to unregister mount from coordination")
			// Don't fail the unmount operation
		}
	}

	if err := m.backend.UnmountVolume(ctx, volCtx); err != nil {
		m.logger.WithError(err).Warn("Backend volume unmount reported errors")
	}

	// Remove from sandbox tracking
	for sandboxID, volumes := range m.sandboxToVolumes {
		delete(volumes, volumeID)
		if len(volumes) == 0 {
			delete(m.sandboxToVolumes, sandboxID)
		}
	}

	delete(m.directMounts, volumeID)
	delete(m.volumes, volumeID)

	m.logger.WithField("volume_id", volumeID).Info("Volume unmounted successfully")

	return nil
}

// GetVolume retrieves volume context
func (m *Manager) GetVolume(volumeID string) (*VolumeContext, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	volCtx, ok := m.volumes[volumeID]
	if !ok {
		return nil, fmt.Errorf("volume %s not mounted", volumeID)
	}

	return volCtx, nil
}

// BeginInvalidate registers an invalidate event that requires remount acks.
func (m *Manager) BeginInvalidate(volumeID, invalidateID string) (int, error) {
	if invalidateID == "" {
		return 0, fmt.Errorf("missing invalidate id")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	sessions := m.mountSessions[volumeID]
	if len(sessions) == 0 {
		return 0, nil
	}
	if m.invalidates[volumeID] == nil {
		m.invalidates[volumeID] = make(map[string]*invalidateTracker)
	}
	if _, exists := m.invalidates[volumeID][invalidateID]; exists {
		return 0, fmt.Errorf("invalidate %s already registered", invalidateID)
	}
	pending := make(map[string]struct{}, len(sessions))
	for sessionID, session := range sessions {
		if session != nil && session.Scope == MountSessionScopeDirect {
			continue
		}
		pending[sessionID] = struct{}{}
	}
	m.invalidates[volumeID][invalidateID] = &invalidateTracker{
		pending: pending,
		done:    make(chan struct{}),
	}
	return len(pending), nil
}

// WaitForInvalidate waits until all sessions acknowledge the invalidate event.
func (m *Manager) WaitForInvalidate(ctx context.Context, volumeID, invalidateID string) error {
	m.mu.RLock()
	tracker := m.invalidates[volumeID][invalidateID]
	m.mu.RUnlock()
	if tracker == nil {
		return nil
	}

	select {
	case <-tracker.done:
	case <-ctx.Done():
		return ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	tracker = m.invalidates[volumeID][invalidateID]
	if tracker == nil {
		return nil
	}
	err := tracker.err
	delete(m.invalidates[volumeID], invalidateID)
	if len(m.invalidates[volumeID]) == 0 {
		delete(m.invalidates, volumeID)
	}
	return err
}

// AckInvalidate acknowledges an invalidate event for a mount session.
func (m *Manager) AckInvalidate(volumeID, sessionID, invalidateID string, success bool, errorMessage string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tracker := m.invalidates[volumeID][invalidateID]
	if tracker == nil {
		return nil
	}
	if _, ok := tracker.pending[sessionID]; !ok {
		return nil
	}
	delete(tracker.pending, sessionID)
	if !success && tracker.err == nil {
		if errorMessage == "" {
			errorMessage = "remount failed"
		}
		tracker.err = fmt.Errorf("%s", errorMessage)
	}
	if tracker.err != nil || len(tracker.pending) == 0 {
		select {
		case <-tracker.done:
		default:
			close(tracker.done)
		}
	}
	return nil
}

func (m *Manager) clearSessionFromInvalidatesLocked(volumeID, sessionID string) {
	trackerMap := m.invalidates[volumeID]
	if len(trackerMap) == 0 {
		return
	}
	for _, tracker := range trackerMap {
		if _, ok := tracker.pending[sessionID]; !ok {
			continue
		}
		delete(tracker.pending, sessionID)
		if tracker.err != nil || len(tracker.pending) == 0 {
			select {
			case <-tracker.done:
			default:
				close(tracker.done)
			}
		}
	}
}

// UpdateVolumeRoot updates the root inode for a mounted volume.
func (m *Manager) UpdateVolumeRoot(volumeID string, rootInode meta.Ino) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	volCtx, ok := m.volumes[volumeID]
	if !ok {
		return fmt.Errorf("volume %s not mounted", volumeID)
	}
	volCtx.RootInode = rootInode
	return nil
}

// ListVolumes returns all mounted volumes
func (m *Manager) ListVolumes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	volumes := make([]string, 0, len(m.volumes))
	for volumeID := range m.volumes {
		volumes = append(volumes, volumeID)
	}

	return volumes
}

// ListMountSessions returns all mount session IDs for a volume.
func (m *Manager) ListMountSessions(volumeID string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sessions := m.mountSessions[volumeID]
	if len(sessions) == 0 {
		return nil
	}
	ids := make([]string, 0, len(sessions))
	for sessionID := range sessions {
		ids = append(ids, sessionID)
	}
	return ids
}

// TrackVolume associates a volume with a sandbox for automatic cleanup
func (m *Manager) TrackVolume(sandboxID, volumeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sandboxToVolumes[sandboxID] == nil {
		m.sandboxToVolumes[sandboxID] = make(map[string]struct{})
	}
	m.sandboxToVolumes[sandboxID][volumeID] = struct{}{}

	m.logger.WithFields(logrus.Fields{
		"sandbox_id": sandboxID,
		"volume_id":  volumeID,
	}).Debug("Tracking volume for sandbox")
}

// TrackVolumeSession associates a specific mount session with a sandbox for precise cleanup.
func (m *Manager) TrackVolumeSession(sandboxID, volumeID, sessionID string) {
	if sandboxID == "" || volumeID == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sandboxToVolumes[sandboxID] == nil {
		m.sandboxToVolumes[sandboxID] = make(map[string]struct{})
	}
	m.sandboxToVolumes[sandboxID][volumeID] = struct{}{}

	if sessionID == "" {
		return
	}
	if sessions := m.mountSessions[volumeID]; sessions != nil {
		if session := sessions[sessionID]; session != nil {
			session.SandboxID = sandboxID
			session.Scope = MountSessionScopeSandbox
			return
		}
	}

	m.logger.WithFields(logrus.Fields{
		"sandbox_id": sandboxID,
		"volume_id":  volumeID,
		"session_id": sessionID,
	}).Warn("TrackVolumeSession called for unknown mount session")
}

// AcquireDirectVolumeFileMount acquires a shared direct session for HTTP volume file APIs.
func (m *Manager) AcquireDirectVolumeFileMount(ctx context.Context, volumeID string, mountFn func(context.Context) (string, error)) (func(), error) {
	if volumeID == "" {
		return func() {}, fmt.Errorf("missing volume id")
	}
	if mountFn == nil {
		return func() {}, fmt.Errorf("missing direct mount function")
	}

	now := time.Now()

	m.mu.Lock()
	if lease := m.directMounts[volumeID]; lease != nil {
		if sessions := m.mountSessions[volumeID]; sessions != nil {
			if session := sessions[lease.SessionID]; session != nil {
				session.Scope = MountSessionScopeDirect
				lease.InFlight++
				lease.LastUsed = now
				m.mu.Unlock()
				return m.releaseDirectVolumeFileMount(volumeID, lease.SessionID), nil
			}
		}
		delete(m.directMounts, volumeID)
	}
	m.mu.Unlock()

	sessionID, err := mountFn(ctx)
	if err != nil {
		return func() {}, err
	}

	redundantSessionID := ""

	m.mu.Lock()
	if lease := m.directMounts[volumeID]; lease != nil {
		if sessions := m.mountSessions[volumeID]; sessions != nil {
			if session := sessions[lease.SessionID]; session != nil {
				session.Scope = MountSessionScopeDirect
				lease.InFlight++
				lease.LastUsed = now
				redundantSessionID = sessionID
				m.mu.Unlock()
				if err := m.UnmountVolume(context.Background(), volumeID, redundantSessionID); err != nil {
					m.logger.WithError(err).WithFields(logrus.Fields{
						"volume_id":  volumeID,
						"session_id": redundantSessionID,
					}).Warn("Failed to cleanup redundant direct volume session")
				}
				return m.releaseDirectVolumeFileMount(volumeID, lease.SessionID), nil
			}
		}
		delete(m.directMounts, volumeID)
	}

	if sessions := m.mountSessions[volumeID]; sessions != nil {
		if session := sessions[sessionID]; session != nil {
			session.Scope = MountSessionScopeDirect
		}
	}
	m.directMounts[volumeID] = &directMountLease{
		SessionID: sessionID,
		InFlight:  1,
		LastUsed:  now,
	}
	m.mu.Unlock()

	return m.releaseDirectVolumeFileMount(volumeID, sessionID), nil
}

func (m *Manager) releaseDirectVolumeFileMount(volumeID, sessionID string) func() {
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		lease := m.directMounts[volumeID]
		if lease == nil || lease.SessionID != sessionID {
			return
		}
		if lease.InFlight > 0 {
			lease.InFlight--
		}
		lease.LastUsed = time.Now()
	}
}

// CleanupIdleDirectVolumeFileMount releases the shared direct session for a volume when no requests are in flight.
func (m *Manager) CleanupIdleDirectVolumeFileMount(ctx context.Context, volumeID string) (bool, error) {
	if volumeID == "" {
		return false, fmt.Errorf("missing volume id")
	}

	m.mu.Lock()
	lease := m.directMounts[volumeID]
	if lease == nil {
		m.mu.Unlock()
		return false, nil
	}
	if lease.InFlight > 0 {
		m.mu.Unlock()
		return false, nil
	}
	sessionID := lease.SessionID
	delete(m.directMounts, volumeID)
	m.mu.Unlock()

	if err := m.UnmountVolume(ctx, volumeID, sessionID); err != nil {
		return false, err
	}
	return true, nil
}

// CleanupIdleDirectVolumeFileMounts unmounts idle shared direct sessions after the idle TTL elapses.
func (m *Manager) CleanupIdleDirectVolumeFileMounts(ctx context.Context, idleTTL time.Duration) []error {
	if idleTTL <= 0 {
		return nil
	}

	type pendingUnmount struct {
		volumeID  string
		sessionID string
	}
	candidates := make([]pendingUnmount, 0)
	cutoff := time.Now().Add(-idleTTL)

	m.mu.Lock()
	for volumeID, lease := range m.directMounts {
		if lease == nil || lease.InFlight > 0 || lease.LastUsed.After(cutoff) {
			continue
		}
		candidates = append(candidates, pendingUnmount{
			volumeID:  volumeID,
			sessionID: lease.SessionID,
		})
		delete(m.directMounts, volumeID)
	}
	m.mu.Unlock()

	var errs []error
	for _, candidate := range candidates {
		if err := m.UnmountVolume(ctx, candidate.volumeID, candidate.sessionID); err != nil {
			errs = append(errs, err)
			m.logger.WithError(err).WithFields(logrus.Fields{
				"volume_id":  candidate.volumeID,
				"session_id": candidate.sessionID,
			}).Warn("Failed to cleanup idle direct volume file mount")
		}
	}
	return errs
}

func (m *Manager) listSandboxMountSessions(volumeID, sandboxID string) ([]string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sessions := m.mountSessions[volumeID]
	if len(sessions) == 0 {
		return nil, false
	}

	ids := make([]string, 0, len(sessions))
	hasLegacyUnscoped := false
	for sessionID, session := range sessions {
		if session == nil || session.SandboxID == "" {
			hasLegacyUnscoped = true
			continue
		}
		if session.SandboxID == sandboxID {
			ids = append(ids, sessionID)
		}
	}
	return ids, hasLegacyUnscoped
}

// UnmountSandboxVolumes unmounts all volumes associated with a sandbox
// This is called automatically when a sandbox pod is deleted
func (m *Manager) UnmountSandboxVolumes(ctx context.Context, sandboxID string) []error {
	m.mu.RLock()
	volumes, ok := m.sandboxToVolumes[sandboxID]
	if !ok {
		m.mu.RUnlock()
		return nil // No volumes for this sandbox
	}
	volumeIDs := make([]string, 0, len(volumes))
	for volumeID := range volumes {
		volumeIDs = append(volumeIDs, volumeID)
	}
	m.mu.RUnlock()

	var errs []error
	for _, volumeID := range volumeIDs {
		m.logger.WithFields(logrus.Fields{
			"sandbox_id": sandboxID,
			"volume_id":  volumeID,
		}).Info("Auto-unmounting volume for deleted sandbox")

		sessionIDs, hasLegacyUnscoped := m.listSandboxMountSessions(volumeID, sandboxID)
		if len(sessionIDs) == 0 {
			if hasLegacyUnscoped {
				m.logger.WithFields(logrus.Fields{
					"sandbox_id": sandboxID,
					"volume_id":  volumeID,
				}).Warn("Skipping unscoped legacy mount sessions during sandbox cleanup")
				continue
			}

			// Best-effort cleanup for legacy/no-session state.
			m.mu.Lock()
			delete(m.volumes, volumeID)
			delete(m.mountSessions, volumeID)
			delete(m.directMounts, volumeID)
			delete(m.invalidates, volumeID)
			m.mu.Unlock()
			continue
		}
		for _, sessionID := range sessionIDs {
			if err := m.UnmountVolume(ctx, volumeID, sessionID); err != nil {
				errs = append(errs, err)
				m.logger.WithError(err).WithFields(logrus.Fields{
					"sandbox_id": sandboxID,
					"volume_id":  volumeID,
					"session_id": sessionID,
				}).Warn("Failed to auto-unmount volume session")
			}
		}
	}

	// Cleanup sandbox index regardless of unmount result to avoid repeated retries
	// against already-terminated sandboxes.
	m.mu.Lock()
	delete(m.sandboxToVolumes, sandboxID)
	m.mu.Unlock()

	return errs
}

// parseSizeString parses size string like "10G", "100M" to bytes
func parseSizeString(sizeStr string, defaultSize int64) int64 {
	if sizeStr == "" {
		return defaultSize
	}

	var multiplier int64 = 1
	numStr := sizeStr

	if len(sizeStr) > 0 {
		lastChar := sizeStr[len(sizeStr)-1]
		switch lastChar {
		case 'K', 'k':
			multiplier = 1 << 10
			numStr = sizeStr[:len(sizeStr)-1]
		case 'M', 'm':
			multiplier = 1 << 20
			numStr = sizeStr[:len(sizeStr)-1]
		case 'G', 'g':
			multiplier = 1 << 30
			numStr = sizeStr[:len(sizeStr)-1]
		case 'T', 't':
			multiplier = 1 << 40
			numStr = sizeStr[:len(sizeStr)-1]
		}
	}

	var size int64
	fmt.Sscanf(numStr, "%d", &size)
	return size * multiplier
}

func generateMountSessionSecret() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
