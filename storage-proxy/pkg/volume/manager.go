package volume

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sirupsen/logrus"
)

// MountRegistrar handles registration of volume mounts for distributed coordination
type MountRegistrar interface {
	RegisterMount(ctx context.Context, volumeID string) error
	UnregisterMount(ctx context.Context, volumeID string) error
}

// VolumeConfig holds the configuration for a volume
type VolumeConfig struct {
	CacheSize  string
	Prefetch   int
	BufferSize string
	Writeback  bool
	ReadOnly   bool
}

// VolumeContext holds JuiceFS VFS instance for a volume
type VolumeContext struct {
	VolumeID  string
	Meta      meta.Meta
	Store     chunk.ChunkStore
	VFS       *vfs.VFS
	Config    *VolumeConfig
	MountedAt time.Time
}

// Manager manages JuiceFS volumes
type Manager struct {
	mu               sync.RWMutex
	volumes          map[string]*VolumeContext
	sandboxToVolumes map[string]map[string]struct{} // sandboxID -> set of volumeIDs
	logger           *logrus.Logger
	config           *config.StorageProxyConfig
	registrar        MountRegistrar // Optional: for distributed coordination
}

// NewManager creates a new volume manager
func NewManager(logger *logrus.Logger, cfg *config.StorageProxyConfig) *Manager {
	return &Manager{
		volumes:          make(map[string]*VolumeContext),
		sandboxToVolumes: make(map[string]map[string]struct{}),
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

// MountVolume mounts a JuiceFS volume using SDK mode (in-memory, no FUSE)
func (m *Manager) MountVolume(ctx context.Context, s3Prefix, volumeID string, config *VolumeConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already mounted
	if _, exists := m.volumes[volumeID]; exists {
		return fmt.Errorf("volume %s already mounted", volumeID)
	}

	m.logger.WithField("volume_id", volumeID).Info("Mounting volume")

	// 1. Initialize JuiceFS metadata client
	metaConf := meta.DefaultConf()
	metaConf.Retries = m.config.JuiceFSMetaRetries
	if metaConf.Retries == 0 {
		metaConf.Retries = 10
	}
	metaConf.ReadOnly = config.ReadOnly

	metaClient := meta.NewClient(m.config.MetaURL, metaConf)

	// Load or create format
	format, err := metaClient.Load(true)
	if err != nil {
		return fmt.Errorf("failed to load juicefs format: %w", err)
	}

	// 2. Initialize S3 object storage
	blob, err := m.createS3Storage(config, s3Prefix, format)
	if err != nil {
		return fmt.Errorf("failed to create S3 storage: %w", err)
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

	// 5. Store volume context
	m.volumes[volumeID] = &VolumeContext{
		VolumeID:  volumeID,
		Meta:      metaClient,
		Store:     store,
		VFS:       vfsInst,
		Config:    config,
		MountedAt: time.Now(),
	}

	// 6. Register mount for distributed coordination (if registrar is set)
	if m.registrar != nil {
		if err := m.registrar.RegisterMount(ctx, volumeID); err != nil {
			m.logger.WithError(err).Warn("Failed to register mount for coordination")
			// Don't fail the mount operation, coordination is optional
		}
	}

	m.logger.WithFields(logrus.Fields{
		"volume_id": volumeID,
		"cache_dir": cacheDir,
		"read_only": config.ReadOnly,
	}).Info("Volume mounted successfully")

	return nil
}

// UnmountVolume unmounts a volume
func (m *Manager) UnmountVolume(ctx context.Context, volumeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	volCtx, ok := m.volumes[volumeID]
	if !ok {
		return fmt.Errorf("volume %s not mounted", volumeID)
	}

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
	m.mu.Lock()
	defer m.mu.Unlock()

	volumes, ok := m.sandboxToVolumes[sandboxID]
	if !ok {
		return nil // No volumes for this sandbox
	}

	var errs []error
	for volumeID := range volumes {
		m.logger.WithFields(logrus.Fields{
			"sandbox_id": sandboxID,
			"volume_id":  volumeID,
		}).Info("Auto-unmounting volume for deleted sandbox")

		if volCtx, exists := m.volumes[volumeID]; exists {
			// Flush all buffered data in VFS
			if err := volCtx.VFS.FlushAll(""); err != nil {
				m.logger.WithError(err).Warn("Failed to flush VFS data")
			}

			// Close metadata session
			if err := volCtx.Meta.CloseSession(); err != nil {
				m.logger.WithError(err).Warn("Failed to close metadata session")
			}

			delete(m.volumes, volumeID)
		}

		delete(m.sandboxToVolumes[sandboxID], volumeID)
	}

	// Clean up empty sandbox entry
	if len(m.sandboxToVolumes[sandboxID]) == 0 {
		delete(m.sandboxToVolumes, sandboxID)
	}

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
