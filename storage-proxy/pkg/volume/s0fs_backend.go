package volume

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/resource"
)

// S0FSBackend mounts the in-process active volume engine.
type S0FSBackend struct {
	logger          *logrus.Logger
	config          *config.StorageProxyConfig
	headStore       s0fs.HeadStore
	requestObserver objectstore.RequestObserver
}

// SetObjectStoreRequestObserver configures request metering for stores created
// outside the mount path.
func (b *S0FSBackend) SetObjectStoreRequestObserver(observer objectstore.RequestObserver) {
	if b == nil {
		return
	}
	b.requestObserver = observer
}

func NewS0FSBackend(logger *logrus.Logger, cfg *config.StorageProxyConfig, repo *db.Repository) *S0FSBackend {
	if logger == nil {
		logger = logrus.New()
	}
	return &S0FSBackend{
		logger:    logger,
		config:    cfg,
		headStore: db.NewS0FSHeadStore(repo),
	}
}

func (b *S0FSBackend) MountVolume(ctx context.Context, req BackendMountRequest) (*VolumeContext, error) {
	if req.TeamID == "" {
		return nil, fmt.Errorf("missing team id for volume mount")
	}
	if b == nil || b.config == nil {
		return nil, fmt.Errorf("manager storage runtime config is not available")
	}
	cacheDir := filepath.Join(b.config.CacheDir, "s0fs", req.VolumeID)
	remoteStore, err := b.createObjectStorage(req)
	if err != nil {
		return nil, fmt.Errorf("create s0fs object storage: %w", err)
	}
	encryption, err := S0FSEncryptionConfig(b.config)
	if err != nil {
		return nil, err
	}
	segmentTargetSize, err := S0FSSegmentTargetSize(b.config)
	if err != nil {
		return nil, err
	}
	stateFormatVersion, err := S0FSStateFormatVersion(b.config)
	if err != nil {
		return nil, err
	}
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:           req.VolumeID,
		WALPath:            filepath.Join(cacheDir, "engine.wal"),
		ObjectStore:        remoteStore,
		SegmentTargetSize:  segmentTargetSize,
		StateFormatVersion: stateFormatVersion,
		ObjectStoreForVolume: func(volumeID string) (objectstore.Store, error) {
			return b.createObjectStorageForVolume(req, volumeID)
		},
		HeadStore:    b.headStore,
		Encryption:   encryption,
		MetadataPath: filepath.Join(cacheDir, "metadata.sqlite"),
	})
	if err != nil {
		return nil, fmt.Errorf("open s0fs engine: %w", err)
	}

	volCtx := &VolumeContext{
		VolumeID:  req.VolumeID,
		TeamID:    req.TeamID,
		Backend:   BackendS0FS,
		S0FS:      engine,
		Access:    req.AccessMode,
		MountedAt: req.MountedAt,
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
		Observer:  req.StorageObserver,
	}
	b.startMaterializer(volCtx)
	return volCtx, nil
}

func S0FSSegmentTargetSize(cfg *config.StorageProxyConfig) (uint64, error) {
	value := ""
	if cfg != nil {
		value = strings.TrimSpace(cfg.S0FSSegmentTargetSize)
	}
	if value == "" {
		return s0fs.DefaultSegmentTargetSizeBytes, nil
	}
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, fmt.Errorf("parse s0fs segment target size: %w", err)
	}
	bytes := quantity.Value()
	if bytes <= 0 {
		return 0, fmt.Errorf("s0fs segment target size must be > 0")
	}
	return uint64(bytes), nil
}

func S0FSStateFormatVersion(cfg *config.StorageProxyConfig) (int, error) {
	version := s0fs.StateFormatV1
	if cfg != nil && cfg.S0FSStateFormatVersion != 0 {
		version = cfg.S0FSStateFormatVersion
	}
	if version != s0fs.StateFormatV1 && version != s0fs.StateFormatV2 {
		return 0, fmt.Errorf("unsupported s0fs state format version %d", version)
	}
	return version, nil
}

func S0FSCompactionInterval(cfg *config.StorageProxyConfig) (time.Duration, error) {
	value := ""
	if cfg != nil {
		value = strings.TrimSpace(cfg.S0FSCompactionInterval)
	}
	if value == "" {
		value = "1m"
	}
	switch strings.ToLower(value) {
	case "0", "off", "disabled":
		return 0, nil
	}
	interval, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse s0fs compaction interval: %w", err)
	}
	if interval < 0 {
		return 0, fmt.Errorf("s0fs compaction interval must be >= 0")
	}
	return interval, nil
}

func S0FSCompactionOptions(cfg *config.StorageProxyConfig) (s0fs.CompactionOptions, error) {
	targetSize, err := S0FSSegmentTargetSize(cfg)
	if err != nil {
		return s0fs.CompactionOptions{}, err
	}
	minDeadRatio := 0.5
	minReclaimSize := "1Mi"
	if cfg != nil {
		if value := strings.TrimSpace(cfg.S0FSCompactionMinDeadRatio); value != "" {
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return s0fs.CompactionOptions{}, fmt.Errorf("parse s0fs compaction min dead ratio: %w", err)
			}
			minDeadRatio = parsed
		}
		if strings.TrimSpace(cfg.S0FSCompactionMinReclaimSize) != "" {
			minReclaimSize = strings.TrimSpace(cfg.S0FSCompactionMinReclaimSize)
		}
	}
	if minDeadRatio < 0 || minDeadRatio > 1 {
		return s0fs.CompactionOptions{}, fmt.Errorf("s0fs compaction min dead ratio must be between 0 and 1")
	}
	quantity, err := resource.ParseQuantity(minReclaimSize)
	if err != nil {
		return s0fs.CompactionOptions{}, fmt.Errorf("parse s0fs compaction min reclaim size: %w", err)
	}
	if quantity.Sign() < 0 {
		return s0fs.CompactionOptions{}, fmt.Errorf("s0fs compaction min reclaim size must be >= 0")
	}
	return s0fs.CompactionOptions{
		SegmentTargetSize: targetSize,
		MinDeadRatio:      minDeadRatio,
		MinReclaimBytes:   uint64(quantity.Value()),
	}, nil
}

func (b *S0FSBackend) UnmountVolume(ctx context.Context, volCtx *VolumeContext) error {
	if volCtx == nil || volCtx.S0FS == nil {
		return nil
	}
	if volCtx.materializeCancel != nil {
		volCtx.materializeCancel()
		if volCtx.materializeDone != nil {
			<-volCtx.materializeDone
		}
	}
	result, err := volCtx.SyncMaterialize(ctx)
	if err != nil {
		return fmt.Errorf("materialize s0fs volume: %w", err)
	}
	b.logObservationError(volCtx.VolumeID, result.ObservationError)
	return volCtx.S0FS.Close()
}

func (b *S0FSBackend) createObjectStorage(req BackendMountRequest) (objectstore.Store, error) {
	return b.createObjectStorageForVolume(req, req.VolumeID)
}

func (b *S0FSBackend) createObjectStorageForVolume(req BackendMountRequest, volumeID string) (objectstore.Store, error) {
	if b == nil || b.config == nil || strings.TrimSpace(b.config.S3Bucket) == "" {
		return nil, nil
	}
	store, err := objectstore.Create(objectstore.Config{
		Type:            b.config.ObjectStorageType,
		Bucket:          b.config.S3Bucket,
		Region:          b.config.S3Region,
		Endpoint:        b.config.S3Endpoint,
		AccessKey:       b.config.S3AccessKey,
		SecretKey:       b.config.S3SecretKey,
		SessionToken:    b.config.S3SessionToken,
		Metrics:         req.Metrics,
		RequestObserver: req.RequestObserver,
	})
	if err != nil {
		return nil, err
	}
	prefix := ""
	if volumeID == req.VolumeID {
		prefix = strings.Trim(req.S3Prefix, "/")
	}
	if prefix == "" {
		var err error
		prefix, err = naming.S3VolumePrefix(req.TeamID, volumeID)
		if err != nil {
			return nil, err
		}
	}
	return objectstore.Prefix(store, prefix+"/s0fs/"), nil
}

func (b *S0FSBackend) startMaterializer(volCtx *VolumeContext) {
	if volCtx == nil || volCtx.S0FS == nil {
		return
	}
	// Mounted engines can retain recovery state that is not visible to a
	// best-effort object listing, so this loop must not delete S0FS objects.
	compactionInterval, err := S0FSCompactionInterval(b.config)
	if err != nil {
		b.logger.WithError(err).WithField("volume_id", volCtx.VolumeID).Warn("Disabling s0fs compaction due to invalid configuration")
		compactionInterval = 0
	}
	compactionOptions, err := S0FSCompactionOptions(b.config)
	if err != nil {
		b.logger.WithError(err).WithField("volume_id", volCtx.VolumeID).Warn("Disabling s0fs compaction due to invalid options")
		compactionInterval = 0
	}
	if !S0FSBackgroundCompactionEnabled(volCtx.Access) {
		compactionInterval = 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	volCtx.materializeCancel = cancel
	volCtx.materializeDone = done

	go func() {
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
				result, err := volCtx.SyncMaterialize(ctx)
				if err != nil {
					b.logger.WithError(err).WithField("volume_id", volCtx.VolumeID).Warn("Failed to materialize s0fs volume")
					continue
				}
				b.logObservationError(volCtx.VolumeID, result.ObservationError)
			case <-compactionC:
				materialization, result, err := volCtx.Compact(ctx, compactionOptions)
				if err != nil {
					b.logger.WithError(err).WithField("volume_id", volCtx.VolumeID).Warn("Failed to compact s0fs volume")
					continue
				}
				if result != nil && len(result.CompactedSegments) > 0 {
					b.logger.WithFields(logrus.Fields{
						"volume_id":         volCtx.VolumeID,
						"segments":          len(result.CompactedSegments),
						"rewritten_bytes":   result.RewrittenBytes,
						"reclaimable_bytes": result.ReclaimableBytes,
					}).Info("Compacted s0fs volume")
				}
				b.logObservationError(volCtx.VolumeID, materialization.ObservationError)
			}
		}
	}()
}

func (b *S0FSBackend) logObservationError(volumeID string, err error) {
	if err == nil {
		return
	}
	b.logger.WithError(err).WithField("volume_id", volumeID).Warn("Failed to record volume storage observation")
}
