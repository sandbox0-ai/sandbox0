package volume

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/juicefs"
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

// VolumeContext holds JuiceFS VFS instance for a volume
type VolumeContext struct {
	VolumeID  string
	Meta      meta.Meta
	Store     chunk.ChunkStore
	VFS       *vfs.VFS
	Config    *VolumeConfig
	Access    AccessMode
	MountedAt time.Time
	RootInode meta.Ino
	RootPath  string
}

// MountSession tracks a single mount session on this instance.
type MountSession struct {
	ID        string
	SandboxID string
	CreatedAt time.Time
}

type invalidateTracker struct {
	pending map[string]struct{}
	done    chan struct{}
	err     error
}

// Manager manages JuiceFS volumes
type Manager struct {
	mu               sync.RWMutex
	volumes          map[string]*VolumeContext
	sandboxToVolumes map[string]map[string]struct{} // sandboxID -> set of volumeIDs
	mountSessions    map[string]map[string]*MountSession
	invalidates      map[string]map[string]*invalidateTracker
	logger           *logrus.Logger
	config           *config.StorageProxyConfig
	registrar        MountRegistrar // Optional: for distributed coordination
}

// NewManager creates a new volume manager
func NewManager(logger *logrus.Logger, cfg *config.StorageProxyConfig) *Manager {
	return &Manager{
		volumes:          make(map[string]*VolumeContext),
		sandboxToVolumes: make(map[string]map[string]struct{}),
		mountSessions:    make(map[string]map[string]*MountSession),
		invalidates:      make(map[string]map[string]*invalidateTracker),
		logger:           logger,
		config:           cfg,
	}
}

// SetMountRegistrar sets the mount registrar for distributed coordination.
// This should be called after coordinator is initialized.
func (m *Manager) SetMountRegistrar(registrar MountRegistrar) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registrar = registrar
}

// MountVolume mounts a JuiceFS volume using SDK mode (in-memory, no FUSE).
// AccessMode is enforced per storage-proxy instance (not per sandbox).
func (m *Manager) MountVolume(ctx context.Context, s3Prefix, volumeID string, config *VolumeConfig, accessMode AccessMode) (string, time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	accessMode = NormalizeAccessMode(string(accessMode))
	readOnly := accessMode == AccessModeROX
	sessionID := uuid.New().String()
	sessionTime := time.Now()

	// Validate mount with coordinator if available.
	if m.registrar != nil {
		if err := m.registrar.ValidateMount(ctx, volumeID, accessMode); err != nil {
			return "", time.Time{}, err
		}
	}

	// Check if already mounted
	if existing, exists := m.volumes[volumeID]; exists {
		if existing.Access != accessMode {
			return "", time.Time{}, fmt.Errorf("volume %s already mounted with access_mode=%s", volumeID, existing.Access)
		}
		if m.mountSessions[volumeID] == nil {
			m.mountSessions[volumeID] = make(map[string]*MountSession)
		}
		m.mountSessions[volumeID][sessionID] = &MountSession{
			ID:        sessionID,
			CreatedAt: sessionTime,
		}
		return sessionID, sessionTime, nil
	}

	m.logger.WithField("volume_id", volumeID).Info("Mounting volume")

	// 1. Initialize JuiceFS metadata client
	metaConf := meta.DefaultConf()
	metaConf.Retries = m.config.JuiceFSMetaRetries
	if metaConf.Retries == 0 {
		metaConf.Retries = 10
	}
	metaConf.ReadOnly = readOnly

	metaClient := meta.NewClient(m.config.MetaURL, metaConf)

	// Load or create format
	format, err := metaClient.Load(true)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to load juicefs format: %w", err)
	}

	// 2. Initialize S3 object storage
	blob, err := m.createS3Storage(config, s3Prefix, format)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to create S3 storage: %w", err)
	}

	// 3. Initialize chunk store with local cache
	cacheDir := filepath.Join(m.config.CacheDir, volumeID)
	defaultCacheSize := parseSizeString(m.config.DefaultCacheSize, 1<<30)

	maxUpload := m.config.JuiceFSMaxUpload
	if maxUpload == 0 {
		maxUpload = 20
	}

	chunkConf := chunk.Config{
		BlockSize:     int(format.BlockSize) * 1024,
		Compress:      format.Compression,
		MaxUpload:     maxUpload,
		MaxRetries:    10,
		UploadLimit:   0,
		DownloadLimit: 0,
		Writeback:     config.Writeback,
		Prefetch:      config.Prefetch,
		BufferSize:    uint64(parseSizeString(config.BufferSize, 32<<20)), // 32MB default
		CacheDir:      cacheDir,
		CacheSize:     uint64(parseSizeString(config.CacheSize, defaultCacheSize)),
		FreeSpace:     0.1,
		CacheMode:     0o600,
		AutoCreate:    true,
	}

	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, chunkConf, registry)

	// 4. Create JuiceFS VFS (in-memory, NO FUSE)
	attrTimeout, _ := time.ParseDuration(m.config.JuiceFSAttrTimeout)
	if attrTimeout == 0 {
		attrTimeout = time.Second
	}
	entryTimeout, _ := time.ParseDuration(m.config.JuiceFSEntryTimeout)
	if entryTimeout == 0 {
		entryTimeout = time.Second
	}
	dirEntryTimeout, _ := time.ParseDuration(m.config.JuiceFSDirEntryTimeout)
	if dirEntryTimeout == 0 {
		dirEntryTimeout = time.Second
	}

	vfsConf := &vfs.Config{
		Meta:            metaConf,
		Format:          *format,
		Chunk:           &chunkConf,
		Version:         "1.0.0",
		AttrTimeout:     attrTimeout,
		EntryTimeout:    entryTimeout,
		DirEntryTimeout: dirEntryTimeout,
	}
	vfsInst := vfs.NewVFS(vfsConf, metaClient, store, registry, registry)

	// 5. Ensure per-volume root directory exists in JuiceFS namespace
	rootPath, err := naming.JuiceFSVolumePath(volumeID)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("volume path: %w", err)
	}
	rootInode, err := ensureVolumeRoot(metaClient, rootPath)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("ensure volume root: %w", err)
	}

	// 6. Store volume context
	m.volumes[volumeID] = &VolumeContext{
		VolumeID:  volumeID,
		Meta:      metaClient,
		Store:     store,
		VFS:       vfsInst,
		Config:    config,
		Access:    accessMode,
		MountedAt: time.Now(),
		RootInode: rootInode,
		RootPath:  rootPath,
	}
	if m.mountSessions[volumeID] == nil {
		m.mountSessions[volumeID] = make(map[string]*MountSession)
	}
	m.mountSessions[volumeID][sessionID] = &MountSession{
		ID:        sessionID,
		CreatedAt: sessionTime,
	}

	// 7. Register mount for distributed coordination (if registrar is set)
	if m.registrar != nil {
		if err := m.registrar.RegisterMount(ctx, volumeID, MountOptions{
			AccessMode: accessMode,
		}); err != nil {
			m.logger.WithError(err).Warn("Failed to register mount for coordination")
			// Don't fail the mount operation, coordination is optional
		}
	}

	m.logger.WithFields(logrus.Fields{
		"volume_id":   volumeID,
		"cache_dir":   cacheDir,
		"access_mode": accessMode,
	}).Info("Volume mounted successfully")

	return sessionID, sessionTime, nil
}

// Use meta client directly to create the internal root path.
// This avoids FUSE/VFS semantics (handles/permissions) and keeps it
// consistent with snapshot operations which also use meta clients.
func ensureVolumeRoot(metaClient meta.Meta, path string) (meta.Ino, error) {
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

	// Flush all buffered data in VFS
	if volCtx.VFS != nil {
		if err := volCtx.VFS.FlushAll(""); err != nil {
			m.logger.WithError(err).Warn("Failed to flush VFS data")
		}
	}

	// Close metadata session
	if volCtx.Meta != nil {
		func() {
			defer func() {
				if recoverErr := recover(); recoverErr != nil {
					m.logger.WithField("panic", recoverErr).Warn("Metadata session close panicked")
				}
			}()
			if err := volCtx.Meta.CloseSession(); err != nil {
				m.logger.WithError(err).Warn("Failed to close metadata session")
			}
		}()
	}

	// Remove from sandbox tracking
	for sandboxID, volumes := range m.sandboxToVolumes {
		delete(volumes, volumeID)
		if len(volumes) == 0 {
			delete(m.sandboxToVolumes, sandboxID)
		}
	}

	// Note: ChunkStore doesn't have Shutdown method.
	// In writeback mode, background uploader goroutines in ChunkStore will continue
	// until all staging chunks are uploaded, as long as the process is running.
	// For absolute safety, one could wait for the staging directory to be empty.

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
	for sessionID := range sessions {
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

		sessionIDs := m.ListMountSessions(volumeID)
		if len(sessionIDs) == 0 {
			// Best-effort cleanup for legacy/no-session state.
			m.mu.Lock()
			delete(m.volumes, volumeID)
			delete(m.mountSessions, volumeID)
			delete(m.invalidates, volumeID)
			m.mu.Unlock()
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

// createS3Storage creates S3 object storage for JuiceFS
func (m *Manager) createS3Storage(config *VolumeConfig, prefix string, format *meta.Format) (object.ObjectStorage, error) {
	// Determine endpoint
	endpoint := m.config.S3Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", m.config.S3Region)
	}
	endpoint = strings.TrimRight(endpoint, "/")

	// Build S3 endpoint for JuiceFS object store.
	// JuiceFS expects either:
	// - [ENDPOINT]/[BUCKET] (recommended for S3-compatible backends), or
	// - [BUCKET].[ENDPOINT]
	bucket := m.config.S3Bucket
	s3Endpoint := fmt.Sprintf("%s/%s", endpoint, bucket)

	// Create object storage using JuiceFS object package
	obj, err := object.CreateStorage("s3", s3Endpoint, m.config.S3AccessKey, m.config.S3SecretKey, m.config.S3SessionToken)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 storage: %w", err)
	}

	// Apply an object key prefix for namespace isolation (e.g. per-team).
	if prefix != "" {
		p := strings.Trim(prefix, "/")
		if p != "" {
			p += "/"
		}
		obj = object.WithPrefix(obj, p)
	}

	if m.config.JuiceFSEncryptionEnabled {
		keyPEM, err := juicefs.LoadEncryptionKey(m.config.JuiceFSEncryptionKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load encryption key: %w", err)
		}
		encryptor, err := juicefs.NewEncryptor(keyPEM, m.config.JuiceFSEncryptionPassphrase, m.config.JuiceFSEncryptionAlgo)
		if err != nil {
			return nil, fmt.Errorf("create encryptor: %w", err)
		}
		obj = juicefs.WrapEncryptedStorage(obj, encryptor)
	}

	return obj, nil
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
