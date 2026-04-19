package snapshot

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

// volumeProvider abstracts the subset of volume.Manager needed by snapshot.Manager.
type volumeProvider interface {
	GetVolume(string) (*volume.VolumeContext, error)
	UpdateVolumeRoot(volumeID string, rootInode meta.Ino) error
	BeginInvalidate(volumeID, invalidateID string) (int, error)
	WaitForInvalidate(ctx context.Context, volumeID, invalidateID string) error
}

type repository interface {
	GetSandboxVolume(context.Context, string) (*db.SandboxVolume, error)
	CreateSandboxVolume(context.Context, *db.SandboxVolume) error
	CreateSandboxVolumeTx(context.Context, pgx.Tx, *db.SandboxVolume) error
	ListSnapshotsByVolume(context.Context, string) ([]*db.Snapshot, error)
	GetSnapshot(context.Context, string) (*db.Snapshot, error)
	CreateSnapshot(context.Context, *db.Snapshot) error
	DeleteSnapshot(context.Context, string) error

	// Transaction support
	WithTx(context.Context, func(tx pgx.Tx) error) error
	GetSandboxVolumeForUpdate(context.Context, pgx.Tx, string) (*db.SandboxVolume, error)
	CreateSnapshotTx(context.Context, pgx.Tx, *db.Snapshot) error
	GetSnapshotForUpdate(context.Context, pgx.Tx, string) (*db.Snapshot, error)
	DeleteSnapshotTx(context.Context, pgx.Tx, string) error
	DeleteSandboxVolumeTx(context.Context, pgx.Tx, string) error
}

type meteringRecorder interface {
	AppendEvent(context.Context, *meteringpkg.Event) error
	AppendEventTx(context.Context, pgx.Tx, *meteringpkg.Event) error
	UpsertProducerWatermark(context.Context, string, string, time.Time) error
	UpsertProducerWatermarkTx(context.Context, pgx.Tx, string, string, time.Time) error
}

// FlushCoordinator handles distributed flush coordination across storage-proxy instances
type FlushCoordinator interface {
	// CoordinateFlush coordinates a flush across all instances that have the volume mounted.
	// Returns when all instances have flushed or timeout occurs.
	CoordinateFlush(ctx context.Context, volumeID string) error
}

// Errors
var (
	ErrVolumeNotFound            = errors.New("volume not found")
	ErrSnapshotNotFound          = errors.New("snapshot not found")
	ErrSnapshotNotBelongToVolume = errors.New("snapshot does not belong to volume")
	ErrVolumeLocked              = errors.New("volume is locked for restore")
	ErrFlushFailed               = errors.New("flush failed on one or more nodes")
	ErrCloneFailed               = errors.New("clone operation failed")
	ErrVolumeBusy                = errors.New("volume is busy, try again later")
	ErrRemountTimeout            = errors.New("remount timeout")
	ErrInvalidAccessMode         = errors.New("invalid access mode")
	errPathNotFound              = errors.New("path not found")
)

// Manager handles snapshot operations for SandboxVolumes
type Manager struct {
	mu                sync.RWMutex
	locks             map[string]time.Time // volumeID -> lock acquired time
	repo              repository
	volMgr            volumeProvider
	newArchiveSession archiveSessionFactory
	coordinator       FlushCoordinator // Optional: for distributed coordination
	config            *config.StorageProxyConfig
	logger            *logrus.Logger
	clusterID         string
	podID             string
	metaClient        metaClient // Independent meta client for snapshot operations (no mount required)
	eventPublisher    eventPublisher
	meteringRepo      meteringRecorder
	metrics           *obsmetrics.StorageProxyMetrics
}

// NewManager creates a new snapshot manager
func NewManager(
	repo repository,
	volMgr volumeProvider,
	cfg *config.StorageProxyConfig,
	logger *logrus.Logger,
	metrics *obsmetrics.StorageProxyMetrics,
) (*Manager, error) {
	// Initialize independent JuiceFS meta client for snapshot operations.
	// This allows snapshots to be created/restored/deleted without requiring the volume to be mounted.
	metaConf := meta.DefaultConf()
	metaConf.Retries = cfg.JuiceFSMetaRetries
	if metaConf.Retries == 0 {
		metaConf.Retries = 5
	}
	// Snapshot operations are read-only from the cache perspective
	metaConf.ReadOnly = false
	metaClient := meta.NewClient(cfg.MetaURL, metaConf)
	if _, err := metaClient.Load(true); err != nil {
		return nil, fmt.Errorf("load juicefs format: %w", err)
	}
	if err := metaClient.NewSession(false); err != nil {
		return nil, fmt.Errorf("create meta session: %w", err)
	}

	return &Manager{
		locks:      make(map[string]time.Time),
		repo:       repo,
		volMgr:     volMgr,
		config:     cfg,
		logger:     logger,
		clusterID:  cfg.DefaultClusterId,
		podID:      uuid.New().String(), // Unique pod identifier
		metaClient: metaClient,          // Independent meta client
		metrics:    metrics,
	}, nil
}

// SetEventPublisher wires a watcher event publisher (optional).
func (m *Manager) SetEventPublisher(publisher eventPublisher) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventPublisher = publisher
}

func (m *Manager) SetMeteringRepository(repo meteringRecorder) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.meteringRepo = repo
}

func (m *Manager) appendMeteringEvent(ctx context.Context, event *meteringpkg.Event) error {
	if m.meteringRepo == nil || event == nil {
		return nil
	}
	if err := m.meteringRepo.AppendEvent(ctx, event); err != nil {
		return err
	}
	return m.meteringRepo.UpsertProducerWatermark(ctx, event.Producer, event.RegionID, event.OccurredAt)
}

func (m *Manager) appendMeteringEventTx(ctx context.Context, tx pgx.Tx, event *meteringpkg.Event) error {
	if m.meteringRepo == nil || event == nil {
		return nil
	}
	if err := m.meteringRepo.AppendEventTx(ctx, tx, event); err != nil {
		return err
	}
	return m.meteringRepo.UpsertProducerWatermarkTx(ctx, tx, event.Producer, event.RegionID, event.OccurredAt)
}

func (m *Manager) regionID() string {
	if m.config == nil {
		return ""
	}
	return m.config.RegionID
}

// SetFlushCoordinator sets the flush coordinator for distributed coordination.
// This should be called after coordinator is initialized.
func (m *Manager) SetFlushCoordinator(coordinator FlushCoordinator) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.coordinator = coordinator
}

// CreateSnapshotRequest contains parameters for creating a snapshot
type CreateSnapshotRequest struct {
	VolumeID    string
	Name        string
	Description string
	TeamID      string
	UserID      string
}

// ForkVolumeRequest contains parameters for forking a volume.
type ForkVolumeRequest struct {
	SourceVolumeID  string
	TeamID          string
	UserID          string
	CacheSize       *string
	Prefetch        *int
	BufferSize      *string
	Writeback       *bool
	AccessMode      *string
	DefaultPosixUID *int64
	DefaultPosixGID *int64
}

// CreateSnapshot creates a new snapshot of a volume using JuiceFS COW clone.
// This operation does NOT require the volume to be mounted on this instance.
// Uses a transaction to ensure data consistency and row-level locking to avoid deadlocks.
func (m *Manager) CreateSnapshot(ctx context.Context, req *CreateSnapshotRequest) (*db.Snapshot, error) {
	startTime := time.Now()
	metrics := m.metrics
	m.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeID,
		"name":      req.Name,
	}).Info("Creating snapshot")

	// 0. Distributed flush coordination (if coordinator is set)
	// This ensures all storage-proxy instances that have this volume mounted
	// flush their local caches to S3 before we create the snapshot.
	m.mu.RLock()
	coordinator := m.coordinator
	m.mu.RUnlock()

	if coordinator != nil {
		m.logger.WithField("volume_id", req.VolumeID).Info("Coordinating flush across all instances")
		if err := coordinator.CoordinateFlush(ctx, req.VolumeID); err != nil {
			m.logger.WithError(err).Error("Distributed flush coordination failed")
			return nil, fmt.Errorf("distributed flush coordination: %w", err)
		}
		m.logger.WithField("volume_id", req.VolumeID).Info("Distributed flush coordination completed")
	}

	if m.shouldUseS0FS(req.VolumeID) {
		snapshot, err := m.createS0FSSnapshot(ctx, req)
		if err != nil {
			if metrics != nil {
				metrics.SnapshotOperationsTotal.WithLabelValues("create", "failure").Inc()
				metrics.SnapshotOperationDuration.WithLabelValues("create").Observe(time.Since(startTime).Seconds())
			}
			return nil, err
		}
		if metrics != nil {
			metrics.SnapshotOperationsTotal.WithLabelValues("create", "success").Inc()
			metrics.SnapshotOperationDuration.WithLabelValues("create").Observe(time.Since(startTime).Seconds())
			metrics.SnapshotsTotal.Inc()
		}
		return snapshot, nil
	}

	var snapshot *db.Snapshot
	var snapshotPath string

	// Execute within a transaction to ensure atomicity
	err := m.repo.WithTx(ctx, func(tx pgx.Tx) error {
		// 1. Get volume with FOR UPDATE NOWAIT lock to prevent concurrent modifications
		// This ensures exclusive access and fails immediately if locked (avoiding deadlock)
		vol, err := m.repo.GetSandboxVolumeForUpdate(ctx, tx, req.VolumeID)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				return ErrVolumeNotFound
			}
			// Check for lock timeout (55P03 is PostgreSQL lock_not_available)
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
				return ErrVolumeBusy
			}
			return fmt.Errorf("get volume: %w", err)
		}

		// Verify team ownership
		if vol.TeamID != req.TeamID {
			return ErrVolumeNotFound // Don't reveal existence
		}

		// 2. Optional: Try to flush local cached data if volume is mounted on this instance.
		// Note: If coordinator is set, distributed flush was already done above.
		// This local flush is a safety measure for instances that have the volume mounted.
		if volCtx, err := m.volMgr.GetVolume(req.VolumeID); err == nil {
			if err := volCtx.FlushAll(""); err != nil {
				m.logger.WithError(err).Warn("Failed to flush VFS data before snapshot")
				// Continue anyway - data should still be consistent
			}
		}

		// 3. Look up the volume root directory using independent meta client
		volumePath, err := naming.JuiceFSVolumePath(req.VolumeID)
		if err != nil {
			return err
		}
		parentIno, rootIno, err := m.lookupPath(volumePath)
		if err != nil {
			// A newly created volume can exist in DB before its JuiceFS namespace
			// path is materialized. Create it lazily so snapshot APIs work without
			// requiring a prior mount operation.
			if errors.Is(err, errPathNotFound) {
				if _, ensureErr := m.ensurePathExists(ctx, volumePath); ensureErr != nil {
					return fmt.Errorf("ensure volume path: %w", ensureErr)
				}
				parentIno, rootIno, err = m.lookupPath(volumePath)
			}
			if err != nil {
				return fmt.Errorf("lookup volume path: %w", err)
			}
		}

		// 4. Ensure snapshot parent directory exists
		snapshotID := uuid.New().String()
		snapshotParentPath, err := naming.JuiceFSSnapshotParentPath(req.VolumeID)
		if err != nil {
			return err
		}

		snapshotParentIno, err := m.ensurePathExists(ctx, snapshotParentPath)
		if err != nil {
			return fmt.Errorf("ensure snapshot parent path: %w", err)
		}

		// 5. Clone volume root to snapshot location using JuiceFS COW
		var cloneCount, cloneTotal uint64
		jfsCtx := meta.Background()

		errno := m.metaClient.Clone(jfsCtx, parentIno, rootIno, snapshotParentIno, snapshotID, 0, 0, &cloneCount, &cloneTotal)
		if errno != 0 {
			return fmt.Errorf("%w: %s", ErrCloneFailed, errno.Error())
		}

		m.logger.WithFields(logrus.Fields{
			"volume_id":   req.VolumeID,
			"snapshot_id": snapshotID,
			"clone_count": cloneCount,
			"clone_total": cloneTotal,
		}).Info("JuiceFS clone completed")

		// 6. Look up the new snapshot inode
		snapshotPath, err = naming.JuiceFSSnapshotPath(req.VolumeID, snapshotID)
		if err != nil {
			return err
		}
		_, snapshotIno, err := m.lookupPath(snapshotPath)
		if err != nil {
			// Cleanup on error
			m.deleteSnapshotDir(ctx, snapshotPath)
			return fmt.Errorf("lookup snapshot path: %w", err)
		}

		// 7. Save snapshot metadata to database within the transaction
		snapshot = &db.Snapshot{
			ID:          snapshotID,
			VolumeID:    req.VolumeID,
			TeamID:      req.TeamID,
			UserID:      req.UserID,
			RootInode:   int64(snapshotIno),
			SourceInode: int64(rootIno),
			Name:        req.Name,
			Description: req.Description,
			SizeBytes:   0, // Logical size, can be computed later
			CreatedAt:   time.Now(),
		}

		if err := m.repo.CreateSnapshotTx(ctx, tx, snapshot); err != nil {
			// Cleanup: delete the cloned snapshot directory
			m.logger.WithError(err).Error("Failed to save snapshot metadata, cleaning up")
			m.deleteSnapshotDir(ctx, snapshotPath)
			return fmt.Errorf("save snapshot: %w", err)
		}
		if err := m.appendMeteringEventTx(ctx, tx, snapshotCreatedEvent(m.regionID(), m.clusterID, snapshot)); err != nil {
			return fmt.Errorf("append metering event: %w", err)
		}

		return nil
	})
	if err != nil {
		if metrics != nil {
			metrics.SnapshotOperationsTotal.WithLabelValues("create", "failure").Inc()
			metrics.SnapshotOperationDuration.WithLabelValues("create").Observe(time.Since(startTime).Seconds())
		}

		// Record error type
		var errorType string
		if errors.Is(err, ErrVolumeNotFound) {
			errorType = "volume_not_found"
		} else if errors.Is(err, ErrVolumeBusy) {
			errorType = "volume_busy"
		} else {
			errorType = "internal_error"
		}
		if metrics != nil {
			metrics.SnapshotErrors.WithLabelValues("create", errorType).Inc()
		}

		return nil, err
	}

	// Record success metrics
	if metrics != nil {
		metrics.SnapshotOperationsTotal.WithLabelValues("create", "success").Inc()
		metrics.SnapshotOperationDuration.WithLabelValues("create").Observe(time.Since(startTime).Seconds())
		metrics.SnapshotsTotal.Inc()
	}

	// Record snapshot size if available
	if metrics != nil && snapshot.SizeBytes > 0 {
		metrics.SnapshotSizeBytes.WithLabelValues(req.VolumeID).Observe(float64(snapshot.SizeBytes))
	}

	m.logger.WithFields(logrus.Fields{
		"volume_id":   req.VolumeID,
		"snapshot_id": snapshot.ID,
		"name":        req.Name,
	}).Info("Snapshot created successfully")

	return snapshot, nil
}

// CreateSnapshotSimple is a simplified version for use by HTTP handlers
func (m *Manager) CreateSnapshotSimple(ctx context.Context, req *CreateSnapshotRequest) (*db.Snapshot, error) {
	return m.CreateSnapshot(ctx, req)
}

// ForkVolume creates a new volume using JuiceFS COW clone.
// This operation does NOT require the source volume to be mounted.
func (m *Manager) ForkVolume(ctx context.Context, req *ForkVolumeRequest) (*db.SandboxVolume, error) {
	startTime := time.Now()
	metrics := m.metrics
	m.logger.WithFields(logrus.Fields{
		"source_volume_id": req.SourceVolumeID,
		"team_id":          req.TeamID,
	}).Info("Forking volume")

	// 0. Distributed flush coordination (if coordinator is set)
	m.mu.RLock()
	coordinator := m.coordinator
	m.mu.RUnlock()

	if coordinator != nil {
		m.logger.WithField("volume_id", req.SourceVolumeID).Info("Coordinating flush across all instances")
		if err := coordinator.CoordinateFlush(ctx, req.SourceVolumeID); err != nil {
			m.logger.WithError(err).Error("Distributed flush coordination failed")
			return nil, fmt.Errorf("distributed flush coordination: %w", err)
		}
		m.logger.WithField("volume_id", req.SourceVolumeID).Info("Distributed flush coordination completed")
	}

	if m.shouldUseS0FS(req.SourceVolumeID) {
		vol, err := m.forkS0FSVolume(ctx, req)
		if err != nil {
			if metrics != nil {
				metrics.SnapshotOperationsTotal.WithLabelValues("fork", "failure").Inc()
				metrics.SnapshotOperationDuration.WithLabelValues("fork").Observe(time.Since(startTime).Seconds())
			}
			return nil, err
		}
		if metrics != nil {
			metrics.SnapshotOperationsTotal.WithLabelValues("fork", "success").Inc()
			metrics.SnapshotOperationDuration.WithLabelValues("fork").Observe(time.Since(startTime).Seconds())
		}
		return vol, nil
	}

	// 1. Get source volume and verify ownership
	sourceVol, err := m.repo.GetSandboxVolume(ctx, req.SourceVolumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, ErrVolumeNotFound
		}
		return nil, fmt.Errorf("get source volume: %w", err)
	}
	if sourceVol.TeamID != req.TeamID {
		return nil, ErrVolumeNotFound
	}

	// 2. Optional: Try to flush local cached data if volume is mounted on this instance.
	if volCtx, err := m.volMgr.GetVolume(req.SourceVolumeID); err == nil {
		if err := volCtx.FlushAll(""); err != nil {
			m.logger.WithError(err).Warn("Failed to flush VFS data before fork")
		}
	}

	// 3. Look up source volume path
	sourcePath, err := naming.JuiceFSVolumePath(req.SourceVolumeID)
	if err != nil {
		return nil, err
	}
	sourceParentIno, sourceRootIno, err := m.lookupPath(sourcePath)
	if err != nil {
		if errors.Is(err, errPathNotFound) {
			if _, ensureErr := m.ensurePathExists(ctx, sourcePath); ensureErr != nil {
				return nil, fmt.Errorf("ensure volume path: %w", ensureErr)
			}
			sourceParentIno, sourceRootIno, err = m.lookupPath(sourcePath)
		}
		if err != nil {
			return nil, fmt.Errorf("lookup source volume path: %w", err)
		}
	}

	// 4. Ensure volume parent directory exists
	volumesParentPath := filepath.Dir(sourcePath)
	volumesParentIno, err := m.ensurePathExists(ctx, volumesParentPath)
	if err != nil {
		return nil, fmt.Errorf("ensure volume parent path: %w", err)
	}

	// 5. Clone source to new volume using JuiceFS COW
	newVolumeID := uuid.New().String()
	var cloneCount, cloneTotal uint64
	jfsCtx := meta.Background()
	errno := m.metaClient.Clone(jfsCtx, sourceParentIno, sourceRootIno, volumesParentIno, newVolumeID, 0, 0, &cloneCount, &cloneTotal)
	if errno != 0 {
		return nil, fmt.Errorf("%w: %s", ErrCloneFailed, errno.Error())
	}

	m.logger.WithFields(logrus.Fields{
		"source_volume_id": req.SourceVolumeID,
		"new_volume_id":    newVolumeID,
		"clone_count":      cloneCount,
		"clone_total":      cloneTotal,
	}).Info("JuiceFS clone completed")

	// 6. Resolve volume configuration
	cacheSize := sourceVol.CacheSize
	if req.CacheSize != nil && strings.TrimSpace(*req.CacheSize) != "" {
		cacheSize = *req.CacheSize
	}
	if cacheSize == "" {
		cacheSize = "1G"
	}

	bufferSize := sourceVol.BufferSize
	if req.BufferSize != nil && strings.TrimSpace(*req.BufferSize) != "" {
		bufferSize = *req.BufferSize
	}
	if bufferSize == "" {
		bufferSize = "32M"
	}

	prefetch := sourceVol.Prefetch
	if req.Prefetch != nil {
		prefetch = *req.Prefetch
	}

	writeback := sourceVol.Writeback
	if req.Writeback != nil {
		writeback = *req.Writeback
	}

	defaultPosixUID := sourceVol.DefaultPosixUID
	defaultPosixGID := sourceVol.DefaultPosixGID
	if req.DefaultPosixUID != nil || req.DefaultPosixGID != nil {
		defaultPosixUID = req.DefaultPosixUID
		defaultPosixGID = req.DefaultPosixGID
	}

	accessMode := volume.AccessModeRWO
	if req.AccessMode != nil && strings.TrimSpace(*req.AccessMode) != "" {
		parsedMode, ok := volume.ParseAccessMode(*req.AccessMode)
		if !ok {
			return nil, ErrInvalidAccessMode
		}
		accessMode = parsedMode
	}

	now := time.Now()
	sourceID := sourceVol.ID
	newVol := &db.SandboxVolume{
		ID:              newVolumeID,
		TeamID:          req.TeamID,
		UserID:          req.UserID,
		SourceVolumeID:  &sourceID,
		DefaultPosixUID: defaultPosixUID,
		DefaultPosixGID: defaultPosixGID,
		CacheSize:       cacheSize,
		Prefetch:        prefetch,
		BufferSize:      bufferSize,
		Writeback:       writeback,
		AccessMode:      string(accessMode),
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	if err := m.repo.WithTx(ctx, func(tx pgx.Tx) error {
		if err := m.repo.CreateSandboxVolumeTx(ctx, tx, newVol); err != nil {
			return err
		}
		if err := m.appendMeteringEventTx(ctx, tx, volumeForkedEvent(m.regionID(), m.clusterID, newVol)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		m.logger.WithError(err).Error("Failed to create forked volume record, cleaning up")
		m.deleteVolumeDir(ctx, newVolumeID)
		if metrics != nil {
			metrics.SnapshotOperationsTotal.WithLabelValues("fork", "failure").Inc()
			metrics.SnapshotOperationDuration.WithLabelValues("fork").Observe(time.Since(startTime).Seconds())
		}
		return nil, fmt.Errorf("create forked volume: %w", err)
	}

	if metrics != nil {
		metrics.SnapshotOperationsTotal.WithLabelValues("fork", "success").Inc()
		metrics.SnapshotOperationDuration.WithLabelValues("fork").Observe(time.Since(startTime).Seconds())
	}

	return newVol, nil
}

// ListSnapshots returns all snapshots for a volume
func (m *Manager) ListSnapshots(ctx context.Context, volumeID, teamID string) ([]*db.Snapshot, error) {
	// Verify volume ownership
	vol, err := m.repo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, ErrVolumeNotFound
		}
		return nil, fmt.Errorf("get volume: %w", err)
	}

	if vol.TeamID != teamID {
		return nil, ErrVolumeNotFound
	}

	return m.repo.ListSnapshotsByVolume(ctx, volumeID)
}

// GetSnapshot retrieves a specific snapshot
func (m *Manager) GetSnapshot(ctx context.Context, volumeID, snapshotID, teamID string) (*db.Snapshot, error) {
	snapshot, err := m.repo.GetSnapshot(ctx, snapshotID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, ErrSnapshotNotFound
		}
		return nil, fmt.Errorf("get snapshot: %w", err)
	}

	// Verify ownership
	if snapshot.VolumeID != volumeID || snapshot.TeamID != teamID {
		return nil, ErrSnapshotNotFound
	}

	return snapshot, nil
}

// RestoreSnapshotRequest contains parameters for restoring a snapshot
type RestoreSnapshotRequest struct {
	VolumeID   string
	SnapshotID string
	TeamID     string
	UserID     string
}

// RestoreSnapshot restores a volume to a previous snapshot state.
// This operation does NOT require the volume to be mounted on this instance.
func (m *Manager) RestoreSnapshot(ctx context.Context, req *RestoreSnapshotRequest) error {
	m.logger.WithFields(logrus.Fields{
		"volume_id":   req.VolumeID,
		"snapshot_id": req.SnapshotID,
	}).Info("Restoring snapshot")

	// 1. Get snapshot and verify ownership
	snapshot, err := m.repo.GetSnapshot(ctx, req.SnapshotID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return ErrSnapshotNotFound
		}
		return fmt.Errorf("get snapshot: %w", err)
	}

	if snapshot.VolumeID != req.VolumeID || snapshot.TeamID != req.TeamID {
		return ErrSnapshotNotBelongToVolume
	}

	// 2. Acquire volume lock
	if !m.acquireVolumeLock(req.VolumeID, 60*time.Second) { // Increased timeout for restore
		return ErrVolumeLocked
	}
	defer m.releaseVolumeLock(req.VolumeID)

	if m.shouldUseS0FS(req.VolumeID) {
		return m.restoreS0FSSnapshot(ctx, req, snapshot)
	}

	// 3. Optional: Try to flush local cached data if volume is mounted on this instance.
	if volCtx, err := m.volMgr.GetVolume(req.VolumeID); err == nil {
		if err := volCtx.FlushAll(""); err != nil {
			m.logger.WithError(err).Warn("Failed to flush VFS data before restore")
		}
	}

	// 4. Look up paths using independent meta client
	volumePath, err := naming.JuiceFSVolumePath(req.VolumeID)
	if err != nil {
		return err
	}
	parentIno, rootIno, err := m.lookupPath(volumePath)
	if err != nil {
		return fmt.Errorf("lookup volume path: %w", err)
	}

	jfsCtx := meta.Background()
	_ = rootIno // Will be replaced by snapshot

	// 5. Backup current volume by renaming
	tempName := fmt.Sprintf(".restore_%d", time.Now().UnixNano())
	volumeName := filepath.Base(volumePath)

	var renamedIno meta.Ino
	var renamedAttr meta.Attr
	errno := m.metaClient.Rename(jfsCtx, parentIno, volumeName, parentIno, tempName, 0, &renamedIno, &renamedAttr)
	if errno != 0 {
		return fmt.Errorf("backup current volume failed: %s", errno.Error())
	}

	// 6. Clone snapshot to volume location
	var cloneCount, cloneTotal uint64
	snapshotPath, err := naming.JuiceFSSnapshotPath(req.VolumeID, req.SnapshotID)
	if err != nil {
		// Rollback: restore the backup
		m.metaClient.Rename(jfsCtx, parentIno, tempName, parentIno, volumeName, 0, &renamedIno, &renamedAttr)
		return err
	}
	snapshotParentIno, snapshotIno, err := m.lookupPath(snapshotPath)
	if err != nil {
		// Rollback: restore the backup
		m.logger.WithError(err).Error("Failed to lookup snapshot path, rolling back")
		m.metaClient.Rename(jfsCtx, parentIno, tempName, parentIno, volumeName, 0, &renamedIno, &renamedAttr)
		return fmt.Errorf("lookup snapshot path: %w", err)
	}

	errno = m.metaClient.Clone(jfsCtx, snapshotParentIno, snapshotIno, parentIno, volumeName, 0, 0, &cloneCount, &cloneTotal)
	if errno != 0 {
		// Rollback: restore the backup
		m.logger.WithError(errno).Error("Clone failed, rolling back")
		m.metaClient.Rename(jfsCtx, parentIno, tempName, parentIno, volumeName, 0, &renamedIno, &renamedAttr)
		return fmt.Errorf("%w: %s", ErrCloneFailed, errno.Error())
	}

	// 7. Refresh volume root inode for mounted volumes
	if m.volMgr != nil {
		if _, newRootIno, lookupErr := m.lookupPath(volumePath); lookupErr == nil {
			if err := m.volMgr.UpdateVolumeRoot(req.VolumeID, newRootIno); err != nil {
				m.logger.WithError(err).Warn("Failed to update volume root inode after restore")
			}
		} else {
			m.logger.WithError(lookupErr).Warn("Failed to lookup volume path after restore")
		}
	}

	// 8. Delete the backup
	var removeCount uint64
	tempPath, _ := naming.JuiceFSVolumePath(tempName)
	tempIno, _, _ := m.lookupPath(tempPath)
	if tempIno > 0 {
		errno = m.metaClient.Remove(jfsCtx, parentIno, tempName, true, 4, &removeCount)
		if errno != 0 {
			m.logger.WithError(errno).Warn("Failed to cleanup backup directory")
		}
	}

	if err := m.appendMeteringEvent(ctx, snapshotRestoredEvent(m.regionID(), m.clusterID, snapshot, req.VolumeID, req.TeamID, req.UserID)); err != nil {
		return fmt.Errorf("append metering event: %w", err)
	}

	m.logger.WithFields(logrus.Fields{
		"volume_id":   req.VolumeID,
		"snapshot_id": req.SnapshotID,
		"clone_count": cloneCount,
	}).Info("Snapshot restored successfully")

	invalidateID := uuid.New().String()
	pending := 0
	if m.volMgr != nil {
		var beginErr error
		pending, beginErr = m.volMgr.BeginInvalidate(req.VolumeID, invalidateID)
		if beginErr != nil {
			return fmt.Errorf("begin invalidate: %w", beginErr)
		}
	}
	m.publishInvalidateEvent(req.VolumeID, invalidateID)
	if pending > 0 && m.volMgr != nil {
		timeout := 30 * time.Second
		if m.config != nil && strings.TrimSpace(m.config.RestoreRemountTimeout) != "" {
			if parsed, err := time.ParseDuration(m.config.RestoreRemountTimeout); err == nil {
				timeout = parsed
			}
		}
		waitCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if err := m.volMgr.WaitForInvalidate(waitCtx, req.VolumeID, invalidateID); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return ErrRemountTimeout
			}
			return err
		}
	}
	return nil
}

// DeleteSnapshot removes a snapshot.
// This operation does NOT require the volume to be mounted on this instance.
// Uses a transaction to ensure data consistency and avoid race conditions.
func (m *Manager) DeleteSnapshot(ctx context.Context, volumeID, snapshotID, teamID string) error {
	startTime := time.Now()
	metrics := m.metrics
	m.logger.WithFields(logrus.Fields{
		"volume_id":   volumeID,
		"snapshot_id": snapshotID,
	}).Info("Deleting snapshot")

	// Execute within a transaction to ensure atomicity
	err := m.repo.WithTx(ctx, func(tx pgx.Tx) error {
		// 1. Get snapshot with FOR UPDATE NOWAIT lock to ensure exclusive access
		// This prevents concurrent delete/restore operations on the same snapshot
		snapshot, err := m.repo.GetSnapshotForUpdate(ctx, tx, snapshotID)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				return ErrSnapshotNotFound
			}
			// Check for lock timeout
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
				return ErrVolumeBusy
			}
			return fmt.Errorf("get snapshot: %w", err)
		}

		// Verify ownership
		if snapshot.VolumeID != volumeID || snapshot.TeamID != teamID {
			return ErrSnapshotNotBelongToVolume
		}

		// 2. Delete database record first within the transaction
		// This ensures that even if JuiceFS cleanup fails, the snapshot is marked as deleted
		if err := m.repo.DeleteSnapshotTx(ctx, tx, snapshotID); err != nil {
			return fmt.Errorf("delete snapshot record: %w", err)
		}
		if err := m.appendMeteringEventTx(ctx, tx, snapshotDeletedEvent(m.regionID(), m.clusterID, snapshot)); err != nil {
			return fmt.Errorf("append metering event: %w", err)
		}

		return nil
	})
	if err != nil {
		if metrics != nil {
			metrics.SnapshotOperationsTotal.WithLabelValues("delete", "failure").Inc()
			metrics.SnapshotOperationDuration.WithLabelValues("delete").Observe(time.Since(startTime).Seconds())
		}

		// Record error type
		var errorType string
		if errors.Is(err, ErrSnapshotNotFound) {
			errorType = "snapshot_not_found"
		} else if errors.Is(err, ErrVolumeBusy) {
			errorType = "volume_busy"
		} else {
			errorType = "internal_error"
		}
		if metrics != nil {
			metrics.SnapshotErrors.WithLabelValues("delete", errorType).Inc()
		}

		return err
	}

	// 3. Clean up snapshot state outside the transaction.
	// This is done after the DB transaction to avoid long-running transactions.
	if m.shouldUseS0FS(volumeID) {
		if cleanupErr := m.deleteS0FSSnapshot(ctx, volumeID, snapshotID); cleanupErr != nil {
			m.logger.WithError(cleanupErr).Warn("Failed to delete s0fs snapshot state")
		}
	} else {
		snapshotPath, err := naming.JuiceFSSnapshotPath(volumeID, snapshotID)
		if err != nil {
			m.logger.WithError(err).Warn("Invalid snapshot path, skipping JuiceFS cleanup")
		} else {
			m.deleteSnapshotDir(ctx, snapshotPath)
		}
	}

	// Record success metrics
	if metrics != nil {
		metrics.SnapshotOperationsTotal.WithLabelValues("delete", "success").Inc()
		metrics.SnapshotOperationDuration.WithLabelValues("delete").Observe(time.Since(startTime).Seconds())
		metrics.SnapshotsTotal.Dec()
	}

	m.logger.WithFields(logrus.Fields{
		"volume_id":   volumeID,
		"snapshot_id": snapshotID,
	}).Info("Snapshot deleted successfully")

	return nil
}

func snapshotCreatedEvent(regionID, clusterID string, snapshot *db.Snapshot) *meteringpkg.Event {
	return &meteringpkg.Event{
		EventID:     fmt.Sprintf("snapshot/%s/created/%d", snapshot.ID, snapshot.CreatedAt.UTC().UnixNano()),
		Producer:    "storage-proxy.snapshot",
		RegionID:    regionID,
		EventType:   meteringpkg.EventTypeSnapshotCreated,
		SubjectType: meteringpkg.SubjectTypeSnapshot,
		SubjectID:   snapshot.ID,
		TeamID:      snapshot.TeamID,
		UserID:      snapshot.UserID,
		VolumeID:    snapshot.VolumeID,
		SnapshotID:  snapshot.ID,
		ClusterID:   clusterID,
		OccurredAt:  snapshot.CreatedAt,
	}
}

func snapshotDeletedEvent(regionID, clusterID string, snapshot *db.Snapshot) *meteringpkg.Event {
	now := time.Now().UTC()
	return &meteringpkg.Event{
		EventID:     fmt.Sprintf("snapshot/%s/deleted/%d", snapshot.ID, now.UnixNano()),
		Producer:    "storage-proxy.snapshot",
		RegionID:    regionID,
		EventType:   meteringpkg.EventTypeSnapshotDeleted,
		SubjectType: meteringpkg.SubjectTypeSnapshot,
		SubjectID:   snapshot.ID,
		TeamID:      snapshot.TeamID,
		UserID:      snapshot.UserID,
		VolumeID:    snapshot.VolumeID,
		SnapshotID:  snapshot.ID,
		ClusterID:   clusterID,
		OccurredAt:  now,
	}
}

func snapshotRestoredEvent(regionID, clusterID string, snapshot *db.Snapshot, volumeID string, teamID string, userID string) *meteringpkg.Event {
	now := time.Now().UTC()
	return &meteringpkg.Event{
		EventID:     fmt.Sprintf("snapshot/%s/restored/%d", snapshot.ID, now.UnixNano()),
		Producer:    "storage-proxy.snapshot",
		RegionID:    regionID,
		EventType:   meteringpkg.EventTypeSnapshotRestored,
		SubjectType: meteringpkg.SubjectTypeSnapshot,
		SubjectID:   snapshot.ID,
		TeamID:      teamID,
		UserID:      userID,
		VolumeID:    volumeID,
		SnapshotID:  snapshot.ID,
		ClusterID:   clusterID,
		OccurredAt:  now,
	}
}

func volumeForkedEvent(regionID, clusterID string, volume *db.SandboxVolume) *meteringpkg.Event {
	return &meteringpkg.Event{
		EventID:     fmt.Sprintf("volume/%s/forked/%d", volume.ID, volume.CreatedAt.UTC().UnixNano()),
		Producer:    "storage-proxy.volume",
		RegionID:    regionID,
		EventType:   meteringpkg.EventTypeVolumeForked,
		SubjectType: meteringpkg.SubjectTypeVolume,
		SubjectID:   volume.ID,
		TeamID:      volume.TeamID,
		UserID:      volume.UserID,
		VolumeID:    volume.ID,
		ClusterID:   clusterID,
		OccurredAt:  volume.CreatedAt,
	}
}

// Helper functions

// metaClient defines the JuiceFS meta subset required by snapshot operations.
type metaClient interface {
	Lookup(meta.Context, meta.Ino, string, *meta.Ino, *meta.Attr, bool) syscall.Errno
	Mkdir(meta.Context, meta.Ino, string, uint16, uint16, uint8, *meta.Ino, *meta.Attr) syscall.Errno
	Clone(meta.Context, meta.Ino, meta.Ino, meta.Ino, string, uint8, uint16, *uint64, *uint64) syscall.Errno
	Rename(meta.Context, meta.Ino, string, meta.Ino, string, uint32, *meta.Ino, *meta.Attr) syscall.Errno
	Remove(meta.Context, meta.Ino, string, bool, int, *uint64) syscall.Errno
}

type eventPublisher interface {
	Publish(ctx context.Context, event *pb.WatchEvent)
}

// lookupPath resolves a path to parent inode and target inode
func (m *Manager) lookupPath(path string) (parentIno, targetIno meta.Ino, err error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 {
		return 0, 0, fmt.Errorf("invalid path: %s", path)
	}

	currentIno := meta.RootInode
	var attr meta.Attr

	jfsCtx := meta.Background()

	for i, part := range parts {
		var nextIno meta.Ino
		errno := m.metaClient.Lookup(jfsCtx, currentIno, part, &nextIno, &attr, true)
		if errno != 0 {
			if errno == syscall.ENOENT {
				return currentIno, 0, &pathNotFoundError{path: path}
			}
			return 0, 0, fmt.Errorf("lookup %s: %s", part, errno.Error())
		}

		if i == len(parts)-1 {
			return currentIno, nextIno, nil
		}
		currentIno = nextIno
	}

	return currentIno, currentIno, nil
}

type pathNotFoundError struct {
	path string
}

func (e *pathNotFoundError) Error() string {
	return fmt.Sprintf("path not found: %s", e.path)
}

func (e *pathNotFoundError) Is(target error) bool {
	return target == errPathNotFound
}

// ensurePathExists creates directories along a path if they don't exist
func (m *Manager) ensurePathExists(ctx context.Context, path string) (meta.Ino, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 {
		return meta.RootInode, nil
	}

	currentIno := meta.RootInode
	var attr meta.Attr

	jfsCtx := meta.Background()

	for _, part := range parts {
		var nextIno meta.Ino
		errno := m.metaClient.Lookup(jfsCtx, currentIno, part, &nextIno, &attr, false)

		if errno == syscall.ENOENT {
			// Create directory
			errno = m.metaClient.Mkdir(jfsCtx, currentIno, part, 0o755, 0, 0, &nextIno, &attr)
			if errno != 0 && errno != syscall.EEXIST {
				return 0, fmt.Errorf("mkdir %s: %s", part, errno.Error())
			}
			// If EEXIST, look it up again
			if errno == syscall.EEXIST {
				errno = m.metaClient.Lookup(jfsCtx, currentIno, part, &nextIno, &attr, false)
				if errno != 0 {
					return 0, fmt.Errorf("lookup after mkdir %s: %s", part, errno.Error())
				}
			}
		} else if errno != 0 {
			return 0, fmt.Errorf("lookup %s: %s", part, errno.Error())
		}

		currentIno = nextIno
	}

	return currentIno, nil
}

// deleteSnapshotDir removes a snapshot directory from JuiceFS
func (m *Manager) deleteSnapshotDir(ctx context.Context, snapshotPath string) {
	parentIno, snapshotIno, err := m.lookupPath(snapshotPath)
	if err != nil {
		m.logger.WithError(err).Warn("Failed to lookup snapshot path for deletion")
		return
	}

	if snapshotIno == 0 {
		return // Already deleted
	}

	jfsCtx := meta.Background()
	snapshotName := filepath.Base(snapshotPath)

	var removeCount uint64
	errno := m.metaClient.Remove(jfsCtx, parentIno, snapshotName, true, 4, &removeCount)
	if errno != 0 && errno != syscall.ENOENT {
		m.logger.WithError(errno).Warn("Failed to delete snapshot directory")
	}
}

func (m *Manager) deleteVolumeDir(ctx context.Context, volumeID string) {
	volumePath, err := naming.JuiceFSVolumePath(volumeID)
	if err != nil {
		m.logger.WithError(err).Warn("Invalid volume path, skipping JuiceFS cleanup")
		return
	}

	parentIno, volumeIno, err := m.lookupPath(volumePath)
	if err != nil {
		m.logger.WithError(err).Warn("Failed to lookup volume path for deletion")
		return
	}

	if volumeIno == 0 {
		return
	}

	jfsCtx := meta.Background()
	volumeName := filepath.Base(volumePath)

	var removeCount uint64
	errno := m.metaClient.Remove(jfsCtx, parentIno, volumeName, true, 4, &removeCount)
	if errno != 0 && errno != syscall.ENOENT {
		m.logger.WithError(errno).Warn("Failed to delete volume directory")
	}
}

func (m *Manager) publishInvalidateEvent(volumeID, invalidateID string) {
	m.mu.RLock()
	publisher := m.eventPublisher
	podID := m.podID
	m.mu.RUnlock()
	if publisher == nil {
		return
	}

	publisher.Publish(context.Background(), &pb.WatchEvent{
		VolumeId:       volumeID,
		EventType:      pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE,
		Path:           "/",
		TimestampUnix:  time.Now().Unix(),
		OriginInstance: podID,
		InvalidateId:   invalidateID,
	})
}

// acquireVolumeLock tries to acquire a lock for restore operations
func (m *Manager) acquireVolumeLock(volumeID string, timeout time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if lockTime, exists := m.locks[volumeID]; exists {
		// Check if lock has expired
		if time.Since(lockTime) < timeout {
			return false
		}
	}

	m.locks[volumeID] = time.Now()
	return true
}

// releaseVolumeLock releases a volume lock
func (m *Manager) releaseVolumeLock(volumeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.locks, volumeID)
}
