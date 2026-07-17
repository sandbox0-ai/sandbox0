package volume

import (
	"context"
	"errors"
	"time"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

// StorageObservationRepository provides volume identity and ownership metadata
// needed to produce storage metering observations.
type StorageObservationRepository interface {
	GetSandboxVolume(context.Context, string) (*db.SandboxVolume, error)
	GetSandboxVolumeOwner(context.Context, string) (*db.SandboxVolumeOwner, error)
}

// StorageObservationRecorder stores a volume observation and advances the
// shared storage producer watermark.
type StorageObservationRecorder interface {
	RecordStorageObservation(context.Context, *meteringpkg.StorageObservation) error
	UpsertProducerWatermark(context.Context, string, string, time.Time) error
}

// StorageObservationWriter records one fully populated storage observation.
type StorageObservationWriter func(context.Context, *meteringpkg.StorageObservation) error

// VolumeStorageObserver converts materialized S0FS states into metering
// observations using the same metadata rules in every storage runtime.
type VolumeStorageObserver struct {
	repo      StorageObservationRepository
	regionID  string
	clusterID string
	write     StorageObservationWriter
}

// NewVolumeStorageObserver creates an observer with a caller-supplied writer,
// allowing runtimes to apply their own quota or persistence policy.
func NewVolumeStorageObserver(repo StorageObservationRepository, regionID, clusterID string, write StorageObservationWriter) *VolumeStorageObserver {
	return &VolumeStorageObserver{
		repo:      repo,
		regionID:  regionID,
		clusterID: clusterID,
		write:     write,
	}
}

// NewVolumeStorageObserverWithRecorder creates an observer that writes
// directly to a storage observation recorder.
func NewVolumeStorageObserverWithRecorder(
	repo StorageObservationRepository,
	recorder StorageObservationRecorder,
	regionID, clusterID string,
) *VolumeStorageObserver {
	return NewVolumeStorageObserver(repo, regionID, clusterID, func(ctx context.Context, observation *meteringpkg.StorageObservation) error {
		return RecordStorageObservation(ctx, recorder, observation)
	})
}

func (o *VolumeStorageObserver) ObserveVolumeState(ctx context.Context, volumeID, teamID string, state *s0fs.SnapshotState, observedAt time.Time) error {
	if o == nil || o.repo == nil || o.write == nil || state == nil || volumeID == "" {
		return nil
	}
	vol, err := o.repo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil
		}
		return err
	}
	if teamID != "" && vol.TeamID != teamID {
		return nil
	}
	return o.write(ctx, VolumeStorageObservation(
		ctx,
		o.repo,
		vol,
		o.regionID,
		o.clusterID,
		s0fs.StateStorageBytes(state),
		observedAt,
	))
}

// VolumeStorageObservation builds the canonical metering identity for a
// SandboxVolume and enriches it with durable ownership metadata when present.
func VolumeStorageObservation(
	ctx context.Context,
	repo StorageObservationRepository,
	vol *db.SandboxVolume,
	regionID, clusterID string,
	sizeBytes int64,
	observedAt time.Time,
) *meteringpkg.StorageObservation {
	if vol == nil {
		return nil
	}
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	obs := &meteringpkg.StorageObservation{
		SubjectType:       meteringpkg.SubjectTypeVolume,
		SubjectID:         vol.ID,
		Product:           meteringpkg.ProductSandbox,
		TeamID:            vol.TeamID,
		UserID:            vol.UserID,
		VolumeID:          vol.ID,
		RegionID:          regionID,
		ClusterID:         clusterID,
		SizeBytes:         sizeBytes,
		ResourceCreatedAt: vol.CreatedAt,
		ObservedAt:        observedAt,
	}
	if repo == nil {
		return obs
	}
	if owner, err := repo.GetSandboxVolumeOwner(ctx, vol.ID); err == nil && owner != nil {
		obs.OwnerKind = owner.OwnerKind
		obs.SandboxID = owner.OwnerSandboxID
		if owner.OwnerClusterID != "" {
			obs.ClusterID = owner.OwnerClusterID
		}
	}
	return obs
}

// RecordStorageObservation records a storage state transition, then advances
// the storage producer watermark through the same recorder.
func RecordStorageObservation(ctx context.Context, recorder StorageObservationRecorder, observation *meteringpkg.StorageObservation) error {
	if recorder == nil || observation == nil {
		return nil
	}
	if err := recorder.RecordStorageObservation(ctx, observation); err != nil {
		return err
	}
	return recorder.UpsertProducerWatermark(ctx, meteringpkg.ProducerStorage, observation.RegionID, observation.ObservedAt)
}
