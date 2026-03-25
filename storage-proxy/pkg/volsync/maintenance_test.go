package volsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sirupsen/logrus"
)

type fakeMaintenanceRepo struct {
	heads            []*db.SyncVolumeHead
	listErr          error
	lastDeleteBefore time.Time
	deletedRequests  int64
	deleteExpiredErr error
}

func (f *fakeMaintenanceRepo) ListSyncVolumeHeads(ctx context.Context) ([]*db.SyncVolumeHead, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.heads, nil
}

func (f *fakeMaintenanceRepo) DeleteExpiredSyncRequests(ctx context.Context, before time.Time) (int64, error) {
	f.lastDeleteBefore = before
	if f.deleteExpiredErr != nil {
		return 0, f.deleteExpiredErr
	}
	return f.deletedRequests, nil
}

type fakeMaintenanceService struct {
	requests []*CompactJournalRequest
	err      error
}

func (f *fakeMaintenanceService) CompactJournal(ctx context.Context, req *CompactJournalRequest) (*CompactJournalResponse, error) {
	clone := *req
	f.requests = append(f.requests, &clone)
	if f.err != nil {
		return nil, f.err
	}
	return &CompactJournalResponse{
		HeadSeq:             req.CompactedThroughSeq + 100,
		CompactedThroughSeq: req.CompactedThroughSeq,
		DeletedEntries:      5,
	}, nil
}

func TestMaintenanceRunOnceCompactsVolumesPastRetentionWindow(t *testing.T) {
	repo := &fakeMaintenanceRepo{
		heads: []*db.SyncVolumeHead{
			{VolumeID: "vol-1", TeamID: "team-1", HeadSeq: 120},
			{VolumeID: "vol-2", TeamID: "team-2", HeadSeq: 40},
		},
	}
	service := &fakeMaintenanceService{}
	maintenance := NewMaintenance(repo, service, logrus.New(), MaintenanceConfig{
		CompactionInterval:   time.Minute,
		JournalRetainEntries: 50,
		RequestRetention:     time.Hour,
	})

	maintenance.RunOnce(context.Background())

	if len(service.requests) != 1 {
		t.Fatalf("compact requests = %d, want 1", len(service.requests))
	}
	if service.requests[0].VolumeID != "vol-1" || service.requests[0].CompactedThroughSeq != 70 {
		t.Fatalf("compact request = %+v, want vol-1 compacted_through_seq=70", service.requests[0])
	}
	if repo.lastDeleteBefore.IsZero() {
		t.Fatal("expected expired requests cleanup to run")
	}
}

func TestMaintenanceRunOnceSkipsDisabledRetentionSettings(t *testing.T) {
	repo := &fakeMaintenanceRepo{}
	service := &fakeMaintenanceService{}
	maintenance := NewMaintenance(repo, service, logrus.New(), MaintenanceConfig{
		CompactionInterval:   time.Minute,
		JournalRetainEntries: -1,
		RequestRetention:     -1,
	})

	maintenance.RunOnce(context.Background())

	if len(service.requests) != 0 {
		t.Fatalf("compact requests = %d, want 0", len(service.requests))
	}
	if !repo.lastDeleteBefore.IsZero() {
		t.Fatalf("lastDeleteBefore = %s, want zero", repo.lastDeleteBefore)
	}
}

func TestMaintenanceRunOnceContinuesAfterCompactionFailure(t *testing.T) {
	repo := &fakeMaintenanceRepo{
		heads: []*db.SyncVolumeHead{
			{VolumeID: "vol-1", TeamID: "team-1", HeadSeq: 120},
		},
		deletedRequests: 3,
	}
	service := &fakeMaintenanceService{err: errors.New("boom")}
	maintenance := NewMaintenance(repo, service, logrus.New(), MaintenanceConfig{
		CompactionInterval:   time.Minute,
		JournalRetainEntries: 50,
		RequestRetention:     time.Hour,
	})

	maintenance.RunOnce(context.Background())

	if len(service.requests) != 1 {
		t.Fatalf("compact requests = %d, want 1", len(service.requests))
	}
	if repo.lastDeleteBefore.IsZero() {
		t.Fatal("expected expired requests cleanup to run even after compaction failure")
	}
}

func TestMaintenanceRunOnceRecordsMetrics(t *testing.T) {
	repo := &fakeMaintenanceRepo{
		heads: []*db.SyncVolumeHead{
			{VolumeID: "vol-1", TeamID: "team-1", HeadSeq: 120},
		},
		deletedRequests: 2,
	}
	service := &fakeMaintenanceService{}
	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewStorageProxy(registry)
	maintenance := NewMaintenance(repo, service, logrus.New(), MaintenanceConfig{
		CompactionInterval:   time.Minute,
		JournalRetainEntries: 50,
		RequestRetention:     time.Hour,
	})
	maintenance.SetMetrics(metrics)

	maintenance.RunOnce(context.Background())

	if got := testutil.ToFloat64(metrics.VolumeSyncMaintenanceRuns.WithLabelValues("compaction", "success")); got != 1 {
		t.Fatalf("compaction maintenance metric = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.VolumeSyncMaintenanceRuns.WithLabelValues("request_cleanup", "success")); got != 1 {
		t.Fatalf("request_cleanup maintenance metric = %v, want 1", got)
	}
}
