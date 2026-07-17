package volume

import (
	"context"
	"errors"
	"testing"
	"time"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

type fakeStorageObservationRepository struct {
	volume    *db.SandboxVolume
	owner     *db.SandboxVolumeOwner
	volumeErr error
	ownerErr  error
}

func (r *fakeStorageObservationRepository) GetSandboxVolume(context.Context, string) (*db.SandboxVolume, error) {
	return r.volume, r.volumeErr
}

func (r *fakeStorageObservationRepository) GetSandboxVolumeOwner(context.Context, string) (*db.SandboxVolumeOwner, error) {
	return r.owner, r.ownerErr
}

type fakeStorageObservationRecorder struct {
	observation *meteringpkg.StorageObservation
	producer    string
	regionID    string
	watermark   time.Time
	recordErr   error
}

func (r *fakeStorageObservationRecorder) RecordStorageObservation(_ context.Context, observation *meteringpkg.StorageObservation) error {
	r.observation = observation
	return r.recordErr
}

func (r *fakeStorageObservationRecorder) UpsertProducerWatermark(_ context.Context, producer, regionID string, watermark time.Time) error {
	r.producer = producer
	r.regionID = regionID
	r.watermark = watermark
	return nil
}

func TestVolumeStorageObserverBuildsCanonicalObservation(t *testing.T) {
	createdAt := time.Date(2026, 7, 1, 2, 3, 4, 0, time.UTC)
	observedAt := createdAt.Add(time.Hour)
	repo := &fakeStorageObservationRepository{
		volume: &db.SandboxVolume{
			ID:        "vol-1",
			TeamID:    "team-1",
			UserID:    "user-1",
			CreatedAt: createdAt,
		},
		owner: &db.SandboxVolumeOwner{
			OwnerKind:      db.SandboxVolumeOwnerKindSandbox,
			OwnerSandboxID: "sandbox-1",
			OwnerClusterID: "owner-cluster",
		},
	}
	var got *meteringpkg.StorageObservation
	observer := NewVolumeStorageObserver(repo, "region-1", "default-cluster", func(_ context.Context, observation *meteringpkg.StorageObservation) error {
		got = observation
		return nil
	})
	state := &s0fs.SnapshotState{
		Data: map[uint64][]byte{2: []byte("hello")},
	}

	if err := observer.ObserveVolumeState(context.Background(), "vol-1", "team-1", state, observedAt); err != nil {
		t.Fatalf("ObserveVolumeState() error = %v", err)
	}
	if got == nil {
		t.Fatal("ObserveVolumeState() did not write an observation")
	}
	if got.SubjectType != meteringpkg.SubjectTypeVolume || got.SubjectID != "vol-1" || got.VolumeID != "vol-1" {
		t.Fatalf("observation subject = %#v", got)
	}
	if got.TeamID != "team-1" || got.UserID != "user-1" {
		t.Fatalf("observation tenant identity = %q/%q", got.TeamID, got.UserID)
	}
	if got.RegionID != "region-1" || got.ClusterID != "owner-cluster" {
		t.Fatalf("observation location = %q/%q", got.RegionID, got.ClusterID)
	}
	if got.OwnerKind != db.SandboxVolumeOwnerKindSandbox || got.SandboxID != "sandbox-1" {
		t.Fatalf("observation owner = %q/%q", got.OwnerKind, got.SandboxID)
	}
	if got.SizeBytes != 5 || !got.ResourceCreatedAt.Equal(createdAt) || !got.ObservedAt.Equal(observedAt) {
		t.Fatalf("observation usage = %#v", got)
	}
}

func TestVolumeStorageObserverIgnoresUnknownOrMismatchedVolume(t *testing.T) {
	state := &s0fs.SnapshotState{}
	tests := []struct {
		name   string
		repo   *fakeStorageObservationRepository
		teamID string
	}{
		{
			name:   "not found",
			repo:   &fakeStorageObservationRepository{volumeErr: db.ErrNotFound},
			teamID: "team-1",
		},
		{
			name: "team mismatch",
			repo: &fakeStorageObservationRepository{
				volume: &db.SandboxVolume{ID: "vol-1", TeamID: "team-2"},
			},
			teamID: "team-1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writes := 0
			observer := NewVolumeStorageObserver(tt.repo, "region-1", "cluster-1", func(context.Context, *meteringpkg.StorageObservation) error {
				writes++
				return nil
			})
			if err := observer.ObserveVolumeState(context.Background(), "vol-1", tt.teamID, state, time.Now()); err != nil {
				t.Fatalf("ObserveVolumeState() error = %v", err)
			}
			if writes != 0 {
				t.Fatalf("writes = %d, want 0", writes)
			}
		})
	}
}

func TestRecordStorageObservationUpdatesWatermarkAfterRecord(t *testing.T) {
	observedAt := time.Date(2026, 7, 2, 3, 4, 5, 0, time.UTC)
	observation := &meteringpkg.StorageObservation{
		RegionID:   "region-1",
		ObservedAt: observedAt,
	}
	recorder := &fakeStorageObservationRecorder{}
	if err := RecordStorageObservation(context.Background(), recorder, observation); err != nil {
		t.Fatalf("RecordStorageObservation() error = %v", err)
	}
	if recorder.observation != observation {
		t.Fatal("RecordStorageObservation() did not record the observation")
	}
	if recorder.producer != meteringpkg.ProducerStorage || recorder.regionID != "region-1" || !recorder.watermark.Equal(observedAt) {
		t.Fatalf("watermark = %q/%q/%s", recorder.producer, recorder.regionID, recorder.watermark)
	}

	recordErr := errors.New("record failed")
	failing := &fakeStorageObservationRecorder{recordErr: recordErr}
	if err := RecordStorageObservation(context.Background(), failing, observation); !errors.Is(err, recordErr) {
		t.Fatalf("RecordStorageObservation() error = %v, want %v", err, recordErr)
	}
	if failing.producer != "" {
		t.Fatal("watermark advanced after record failure")
	}
}
