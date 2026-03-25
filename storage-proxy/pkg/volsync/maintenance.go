package volsync

import (
	"context"
	"time"

	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sirupsen/logrus"
)

const (
	defaultCompactionInterval   = 10 * time.Minute
	defaultJournalRetainEntries = int64(10000)
	defaultRequestRetention     = 24 * time.Hour
)

type maintenanceRepository interface {
	ListSyncVolumeHeads(context.Context) ([]*db.SyncVolumeHead, error)
	DeleteExpiredSyncRequests(context.Context, time.Time) (int64, error)
}

type maintenanceService interface {
	CompactJournal(context.Context, *CompactJournalRequest) (*CompactJournalResponse, error)
}

type MaintenanceConfig struct {
	CompactionInterval   time.Duration
	JournalRetainEntries int64
	RequestRetention     time.Duration
}

// Maintenance runs background retention tasks for sync journal history and idempotency keys.
type Maintenance struct {
	repo    maintenanceRepository
	service maintenanceService
	logger  *logrus.Logger
	metrics *obsmetrics.StorageProxyMetrics
	config  MaintenanceConfig
}

func NewMaintenance(repo maintenanceRepository, service maintenanceService, logger *logrus.Logger, cfg MaintenanceConfig) *Maintenance {
	if logger == nil {
		logger = logrus.New()
	}
	if cfg.CompactionInterval == 0 {
		cfg.CompactionInterval = defaultCompactionInterval
	}
	if cfg.JournalRetainEntries == 0 {
		cfg.JournalRetainEntries = defaultJournalRetainEntries
	}
	if cfg.RequestRetention == 0 {
		cfg.RequestRetention = defaultRequestRetention
	}
	return &Maintenance{
		repo:    repo,
		service: service,
		logger:  logger,
		config:  cfg,
	}
}

func (m *Maintenance) SetMetrics(metrics *obsmetrics.StorageProxyMetrics) {
	m.metrics = metrics
}

func (m *Maintenance) Enabled() bool {
	return m != nil &&
		m.repo != nil &&
		m.service != nil &&
		m.config.CompactionInterval > 0 &&
		(m.config.JournalRetainEntries > 0 || m.config.RequestRetention > 0)
}

func (m *Maintenance) Run(ctx context.Context) {
	if !m.Enabled() {
		return
	}

	ticker := time.NewTicker(m.config.CompactionInterval)
	defer ticker.Stop()

	m.runOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runOnce(ctx)
		}
	}
}

func (m *Maintenance) RunOnce(ctx context.Context) {
	if !m.Enabled() {
		return
	}
	m.runOnce(ctx)
}

func (m *Maintenance) runOnce(ctx context.Context) {
	if err := m.compactVolumes(ctx); err != nil {
		m.observeMaintenance("compaction", "failure")
		m.logger.WithError(err).Warn("Volume sync maintenance compaction sweep failed")
	} else {
		m.observeMaintenance("compaction", "success")
	}
	if err := m.cleanupRequests(ctx); err != nil {
		m.observeMaintenance("request_cleanup", "failure")
		m.logger.WithError(err).Warn("Volume sync maintenance request cleanup failed")
	} else {
		m.observeMaintenance("request_cleanup", "success")
	}
}

func (m *Maintenance) compactVolumes(ctx context.Context) error {
	if m.config.JournalRetainEntries <= 0 {
		return nil
	}

	heads, err := m.repo.ListSyncVolumeHeads(ctx)
	if err != nil {
		return err
	}
	for _, head := range heads {
		if head == nil || head.VolumeID == "" {
			continue
		}
		if head.HeadSeq <= m.config.JournalRetainEntries {
			continue
		}
		target := head.HeadSeq - m.config.JournalRetainEntries
		resp, err := m.service.CompactJournal(ctx, &CompactJournalRequest{
			VolumeID:            head.VolumeID,
			TeamID:              head.TeamID,
			CompactedThroughSeq: target,
		})
		if err != nil {
			m.logger.WithError(err).WithField("volume_id", head.VolumeID).Warn("Volume sync maintenance compaction failed")
			continue
		}
		if resp != nil && resp.DeletedEntries > 0 {
			m.logger.WithFields(logrus.Fields{
				"volume_id":             head.VolumeID,
				"compacted_through_seq": resp.CompactedThroughSeq,
				"deleted_entries":       resp.DeletedEntries,
				"head_seq":              resp.HeadSeq,
			}).Info("Compacted volume sync journal history")
		}
	}
	return nil
}

func (m *Maintenance) cleanupRequests(ctx context.Context) error {
	if m.config.RequestRetention <= 0 {
		return nil
	}
	deleted, err := m.repo.DeleteExpiredSyncRequests(ctx, time.Now().UTC().Add(-m.config.RequestRetention))
	if err != nil {
		return err
	}
	if deleted > 0 {
		m.logger.WithField("deleted_requests", deleted).Info("Deleted expired sync request idempotency records")
	}
	return nil
}

func (m *Maintenance) observeMaintenance(task, status string) {
	if m == nil || m.metrics == nil {
		return
	}
	m.metrics.VolumeSyncMaintenanceRuns.WithLabelValues(task, status).Inc()
}
