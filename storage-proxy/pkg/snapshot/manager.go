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
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

// volumeProvider abstracts the subset of volume.Manager needed by snapshot.Manager.
type volumeProvider interface {
	GetVolume(string) (*volume.VolumeContext, error)
	UpdateVolumeRoot(volumeID string, rootInode fsmeta.Ino) error
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
	metaClient        metaClient
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
	return &Manager{
		locks:     make(map[string]time.Time),
		repo:      repo,
		volMgr:    volMgr,
		config:    cfg,
		logger:    logger,
		clusterID: cfg.DefaultClusterId,
		podID:     uuid.New().String(), // Unique pod identifier
		metrics:   metrics,
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

// CreateSnapshot creates a new snapshot of a volume using the s0fs snapshot engine.
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

	snapshot, err := m.createS0FSSnapshot(ctx, req)
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

// ForkVolume creates a new volume using the s0fs snapshot engine state.
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

	return m.restoreS0FSSnapshot(ctx, req, snapshot)
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
		// This ensures that even if S0FS cleanup fails, the snapshot is marked as deleted
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
	if cleanupErr := m.deleteS0FSSnapshot(ctx, volumeID, snapshotID); cleanupErr != nil {
		m.logger.WithError(cleanupErr).Warn("Failed to delete s0fs snapshot state")
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

// metaClient defines the S0FS meta subset required by snapshot operations.
type metaClient interface {
	Lookup(fsmeta.Context, fsmeta.Ino, string, *fsmeta.Ino, *fsmeta.Attr, bool) syscall.Errno
	Mkdir(fsmeta.Context, fsmeta.Ino, string, uint16, uint16, uint8, *fsmeta.Ino, *fsmeta.Attr) syscall.Errno
	Clone(fsmeta.Context, fsmeta.Ino, fsmeta.Ino, fsmeta.Ino, string, uint8, uint16, *uint64, *uint64) syscall.Errno
	Rename(fsmeta.Context, fsmeta.Ino, string, fsmeta.Ino, string, uint32, *fsmeta.Ino, *fsmeta.Attr) syscall.Errno
	Remove(fsmeta.Context, fsmeta.Ino, string, bool, int, *uint64) syscall.Errno
}

type eventPublisher interface {
	Publish(ctx context.Context, event *pb.WatchEvent)
}

// lookupPath resolves a path to parent inode and target inode
func (m *Manager) lookupPath(path string) (parentIno, targetIno fsmeta.Ino, err error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 {
		return 0, 0, fmt.Errorf("invalid path: %s", path)
	}

	currentIno := fsmeta.RootInode
	var attr fsmeta.Attr

	jfsCtx := fsmeta.Background()

	for i, part := range parts {
		var nextIno fsmeta.Ino
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
func (m *Manager) ensurePathExists(ctx context.Context, path string) (fsmeta.Ino, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 {
		return fsmeta.RootInode, nil
	}

	currentIno := fsmeta.RootInode
	var attr fsmeta.Attr

	jfsCtx := fsmeta.Background()

	for _, part := range parts {
		var nextIno fsmeta.Ino
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

// deleteSnapshotDir removes a snapshot directory from S0FS
func (m *Manager) deleteSnapshotDir(ctx context.Context, snapshotPath string) {
	parentIno, snapshotIno, err := m.lookupPath(snapshotPath)
	if err != nil {
		m.logger.WithError(err).Warn("Failed to lookup snapshot path for deletion")
		return
	}

	if snapshotIno == 0 {
		return // Already deleted
	}

	jfsCtx := fsmeta.Background()
	snapshotName := filepath.Base(snapshotPath)

	var removeCount uint64
	errno := m.metaClient.Remove(jfsCtx, parentIno, snapshotName, true, 4, &removeCount)
	if errno != 0 && errno != syscall.ENOENT {
		m.logger.WithError(errno).Warn("Failed to delete snapshot directory")
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
