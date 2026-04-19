package volume

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

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

// JuiceFSBackend adapts the existing JuiceFS SDK/VFS runtime to the storage
// backend boundary. It is kept as an implementation detail while s0fs is built.
type JuiceFSBackend struct {
	logger *logrus.Logger
	config *config.StorageProxyConfig
}

func NewJuiceFSBackend(logger *logrus.Logger, cfg *config.StorageProxyConfig) *JuiceFSBackend {
	return &JuiceFSBackend{
		logger: logger,
		config: cfg,
	}
}

func (b *JuiceFSBackend) MountVolume(_ context.Context, req BackendMountRequest) (*VolumeContext, error) {
	if req.TeamID == "" {
		return nil, fmt.Errorf("missing team id for volume mount")
	}
	if req.Config == nil {
		req.Config = &VolumeConfig{}
	}

	readOnly := req.AccessMode == AccessModeROX
	metaConf := buildMetaConf(b.config, readOnly)
	metaClient := meta.NewClient(b.config.MetaURL, metaConf)

	format, err := metaClient.Load(true)
	if err != nil {
		return nil, fmt.Errorf("failed to load juicefs format: %w", err)
	}

	blob, err := b.createObjectStorage(req)
	if err != nil {
		return nil, fmt.Errorf("failed to create object storage: %w", err)
	}

	cacheDir := filepath.Join(b.config.CacheDir, req.VolumeID)
	defaultCacheSize := parseSizeString(b.config.DefaultCacheSize, 1<<30)

	maxUpload := b.config.JuiceFSMaxUpload
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
		Writeback:     req.Config.Writeback,
		Prefetch:      req.Config.Prefetch,
		BufferSize:    uint64(parseSizeString(req.Config.BufferSize, 32<<20)),
		CacheDir:      cacheDir,
		CacheSize:     uint64(parseSizeString(req.Config.CacheSize, defaultCacheSize)),
		FreeSpace:     0.1,
		CacheMode:     0o600,
		AutoCreate:    true,
	}

	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, chunkConf, registry)

	attrTimeout, _ := time.ParseDuration(b.config.JuiceFSAttrTimeout)
	if attrTimeout == 0 {
		attrTimeout = time.Second
	}
	entryTimeout, _ := time.ParseDuration(b.config.JuiceFSEntryTimeout)
	if entryTimeout == 0 {
		entryTimeout = time.Second
	}
	dirEntryTimeout, _ := time.ParseDuration(b.config.JuiceFSDirEntryTimeout)
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

	rootPath, err := naming.JuiceFSVolumePath(req.VolumeID)
	if err != nil {
		return nil, fmt.Errorf("volume path: %w", err)
	}
	rootInode, err := resolveMountRoot(metaClient, rootPath, readOnly, b.ensureWritableVolumeRoot)
	if err != nil {
		return nil, fmt.Errorf("ensure volume root: %w", err)
	}

	mountedAt := req.MountedAt
	if mountedAt.IsZero() {
		mountedAt = time.Now()
	}

	return &VolumeContext{
		VolumeID:  req.VolumeID,
		TeamID:    req.TeamID,
		Backend:   BackendJuiceFS,
		Meta:      metaClient,
		Store:     store,
		VFS:       vfsInst,
		Config:    req.Config,
		Access:    req.AccessMode,
		MountedAt: mountedAt,
		RootInode: rootInode,
		RootPath:  rootPath,
		CacheDir:  cacheDir,
	}, nil
}

func (b *JuiceFSBackend) UnmountVolume(_ context.Context, volCtx *VolumeContext) error {
	if volCtx == nil {
		return nil
	}
	var retErr error
	if volCtx.VFS != nil {
		if err := volCtx.VFS.FlushAll(""); err != nil {
			retErr = fmt.Errorf("flush VFS data: %w", err)
		}
	}
	if volCtx.Meta != nil {
		func() {
			defer func() {
				if recoverErr := recover(); recoverErr != nil && b.logger != nil {
					b.logger.WithField("panic", recoverErr).Warn("Metadata session close panicked")
				}
			}()
			if err := volCtx.Meta.CloseSession(); err != nil {
				if retErr == nil {
					retErr = fmt.Errorf("close metadata session: %w", err)
				}
			}
		}()
	}
	return retErr
}

func (b *JuiceFSBackend) createObjectStorage(req BackendMountRequest) (object.ObjectStorage, error) {
	obj, err := juicefs.CreateObjectStorage(juicefs.ObjectStorageConfig{
		Type:         b.config.ObjectStorageType,
		Bucket:       b.config.S3Bucket,
		Region:       b.config.S3Region,
		Endpoint:     b.config.S3Endpoint,
		AccessKey:    b.config.S3AccessKey,
		SecretKey:    b.config.S3SecretKey,
		SessionToken: b.config.S3SessionToken,
		Metrics:      req.Metrics,
	})
	if err != nil {
		return nil, fmt.Errorf("create object storage: %w", err)
	}

	if req.S3Prefix != "" {
		p := strings.Trim(req.S3Prefix, "/")
		if p != "" {
			p += "/"
		}
		obj = object.WithPrefix(obj, p)
	}

	if b.config.JuiceFSEncryptionEnabled {
		keyPEM, err := juicefs.LoadEncryptionKey(b.config.JuiceFSEncryptionKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load encryption key: %w", err)
		}
		encryptor, err := juicefs.NewEncryptor(keyPEM, b.config.JuiceFSEncryptionPassphrase, b.config.JuiceFSEncryptionAlgo)
		if err != nil {
			return nil, fmt.Errorf("create encryptor: %w", err)
		}
		obj = juicefs.WrapEncryptedStorage(obj, encryptor)
	}

	return obj, nil
}

func (b *JuiceFSBackend) ensureWritableVolumeRoot(path string) (meta.Ino, error) {
	if b == nil || b.config == nil {
		return 0, fmt.Errorf("storage proxy config is not available")
	}

	metaClient := meta.NewClient(b.config.MetaURL, buildMetaConf(b.config, false))
	defer func() {
		if err := metaClient.Shutdown(); err != nil && b.logger != nil {
			b.logger.WithError(err).Warn("Failed to shutdown writable metadata client after root initialization")
		}
	}()
	if _, err := metaClient.Load(true); err != nil {
		return 0, fmt.Errorf("load writable metadata: %w", err)
	}

	return ensureVolumeRoot(metaClient, path)
}
