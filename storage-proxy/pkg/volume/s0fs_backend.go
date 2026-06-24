package volume

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/resource"
)

// S0FSBackend mounts the in-process active volume engine.
type S0FSBackend struct {
	logger    *logrus.Logger
	config    *config.StorageProxyConfig
	repo      *db.Repository
	headStore s0fs.HeadStore
}

func NewS0FSBackend(logger *logrus.Logger, cfg *config.StorageProxyConfig, repo *db.Repository) *S0FSBackend {
	if logger == nil {
		logger = logrus.New()
	}
	return &S0FSBackend{
		logger:    logger,
		config:    cfg,
		repo:      repo,
		headStore: db.NewS0FSHeadStore(repo),
	}
}

func (b *S0FSBackend) MountVolume(ctx context.Context, req BackendMountRequest) (*VolumeContext, error) {
	if req.TeamID == "" {
		return nil, fmt.Errorf("missing team id for volume mount")
	}
	if b == nil || b.config == nil {
		return nil, fmt.Errorf("storage proxy config is not available")
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
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:          req.VolumeID,
		WALPath:           filepath.Join(cacheDir, "engine.wal"),
		ObjectStore:       remoteStore,
		SegmentTargetSize: segmentTargetSize,
		ObjectStoreForVolume: func(volumeID string) (objectstore.Store, error) {
			return b.createObjectStorageForVolume(req, volumeID)
		},
		HeadStore:  b.headStore,
		Encryption: encryption,
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
	manifest, err := volCtx.S0FS.SyncMaterialize(ctx)
	if err != nil {
		return fmt.Errorf("materialize s0fs volume: %w", err)
	}
	b.observeMaterializedManifest(ctx, volCtx, manifest)
	b.garbageCollectRWO(ctx, volCtx, manifest)
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
				manifest, err := volCtx.S0FS.SyncMaterialize(ctx)
				if err != nil {
					b.logger.WithError(err).WithField("volume_id", volCtx.VolumeID).Warn("Failed to materialize s0fs volume")
					continue
				}
				b.observeMaterializedManifest(ctx, volCtx, manifest)
				b.garbageCollectRWO(ctx, volCtx, manifest)
			case <-compactionC:
				manifest, result, err := volCtx.S0FS.Compact(ctx, compactionOptions)
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
				b.observeMaterializedManifest(ctx, volCtx, manifest)
				b.garbageCollectRWO(ctx, volCtx, manifest)
			}
		}
	}()
}

func (b *S0FSBackend) observeMaterializedManifest(ctx context.Context, volCtx *VolumeContext, manifest *s0fs.Manifest) {
	if volCtx == nil || volCtx.Observer == nil || manifest == nil || manifest.State == nil {
		return
	}
	if err := volCtx.Observer.ObserveVolumeState(ctx, volCtx.VolumeID, volCtx.TeamID, manifest.State, time.Now().UTC()); err != nil {
		b.logger.WithError(err).WithField("volume_id", volCtx.VolumeID).Warn("Failed to record volume storage observation")
	}
}

func (b *S0FSBackend) garbageCollectRWO(ctx context.Context, volCtx *VolumeContext, manifest *s0fs.Manifest) {
	if b == nil || b.repo == nil || volCtx == nil || manifest == nil || manifest.State == nil {
		return
	}
	if volCtx.Access != AccessModeRWO {
		return
	}
	result, err := b.garbageCollectVolumeObjects(ctx, volCtx, manifest)
	if err != nil {
		b.logger.WithError(err).WithField("volume_id", volCtx.VolumeID).Warn("Failed to garbage collect s0fs volume objects")
		return
	}
	if result != nil && (len(result.DeletedSegments) > 0 || len(result.DeletedManifests) > 0) {
		b.logger.WithFields(logrus.Fields{
			"volume_id": volCtx.VolumeID,
			"segments":  len(result.DeletedSegments),
			"manifests": len(result.DeletedManifests),
		}).Info("Garbage collected s0fs volume objects")
	}
}

func (b *S0FSBackend) garbageCollectVolumeObjects(ctx context.Context, volCtx *VolumeContext, manifest *s0fs.Manifest) (*s0fs.GarbageCollectionResult, error) {
	children, err := b.repo.ListSandboxVolumesBySource(ctx, volCtx.VolumeID)
	if err != nil {
		return nil, err
	}
	if len(children) > 0 {
		return nil, nil
	}
	store, err := b.objectStoreForTeamVolume(volCtx.TeamID, volCtx.VolumeID)
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, nil
	}
	materializer := s0fs.NewMaterializer(volCtx.VolumeID, store, b.headStore, func(sourceVolumeID string) (objectstore.Store, error) {
		return b.objectStoreForTeamVolume(volCtx.TeamID, sourceVolumeID)
	})
	if materializer == nil || !materializer.Enabled() {
		return nil, nil
	}
	encryption, err := S0FSEncryptionConfig(b.config)
	if err != nil {
		return nil, err
	}
	materializer.SetEncryption(encryption)

	headBefore, err := b.headStore.LoadCommittedHead(ctx, volCtx.VolumeID)
	if err != nil && !errors.Is(err, s0fs.ErrCommittedHeadNotFound) {
		return nil, err
	}
	retainedStates := []*s0fs.SnapshotState{manifest.State}
	cfg := s0fs.Config{
		VolumeID:    volCtx.VolumeID,
		WALPath:     filepath.Join(volCtx.CacheDir, "engine.wal"),
		ObjectStore: store,
		HeadStore:   b.headStore,
		Encryption:  encryption,
	}
	localSnapshots, err := s0fs.LoadLocalSnapshots(ctx, cfg)
	if err != nil {
		return nil, err
	}
	retainedStates = append(retainedStates, localSnapshots...)
	snapshots, err := b.repo.ListSnapshotsByVolume(ctx, volCtx.VolumeID)
	if err != nil {
		return nil, err
	}
	for _, snapshot := range snapshots {
		state, err := s0fs.LoadSnapshot(ctx, cfg, snapshot.ID)
		if err != nil {
			if errors.Is(err, s0fs.ErrSnapshotNotFound) {
				return nil, nil
			}
			return nil, err
		}
		retainedStates = append(retainedStates, state)
	}
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
	headAfter, err := b.headStore.LoadCommittedHead(ctx, volCtx.VolumeID)
	if err != nil && !errors.Is(err, s0fs.ErrCommittedHeadNotFound) {
		return nil, err
	}
	if !sameHeadKey(headBefore, headAfter) {
		return nil, nil
	}
	return plan.Apply(ctx)
}

func (b *S0FSBackend) objectStoreForTeamVolume(teamID, volumeID string) (objectstore.Store, error) {
	if b == nil || b.config == nil || teamID == "" || volumeID == "" || strings.TrimSpace(b.config.S3Bucket) == "" {
		return nil, nil
	}
	prefix, err := naming.S3VolumePrefix(teamID, volumeID)
	if err != nil {
		return nil, err
	}
	store, err := objectstore.Create(objectstore.Config{
		Type:         b.config.ObjectStorageType,
		Bucket:       b.config.S3Bucket,
		Region:       b.config.S3Region,
		Endpoint:     b.config.S3Endpoint,
		AccessKey:    b.config.S3AccessKey,
		SecretKey:    b.config.S3SecretKey,
		SessionToken: b.config.S3SessionToken,
	})
	if err != nil {
		return nil, err
	}
	return objectstore.Prefix(store, prefix+"/s0fs/"), nil
}

func sameHeadKey(a, b *s0fs.CommittedHead) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return strings.TrimSpace(a.ManifestKey) == strings.TrimSpace(b.ManifestKey)
}
