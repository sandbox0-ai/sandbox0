package snapshot

import (
	"context"
	"time"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func (m *Manager) ObserveVolumeState(ctx context.Context, volumeID, teamID string, state *s0fs.SnapshotState, observedAt time.Time) error {
	if m == nil || m.volumeObserver == nil {
		return nil
	}
	return m.volumeObserver.ObserveVolumeState(ctx, volumeID, teamID, state, observedAt)
}

func (m *Manager) recordVolumeStorageState(ctx context.Context, vol *db.SandboxVolume, state *s0fs.SnapshotState, observedAt time.Time) error {
	return m.recordVolumeStorageStateWithMetadata(ctx, vol, state, observedAt, nil)
}

func (m *Manager) recordVolumeStorageStateWithMetadata(ctx context.Context, vol *db.SandboxVolume, state *s0fs.SnapshotState, observedAt time.Time, metadata *meteringpkg.StorageObservation) error {
	if vol == nil || state == nil {
		return nil
	}
	return m.appendStorageObservation(ctx, applyStorageObservationMetadata(
		m.volumeStorageObservation(ctx, vol, s0fs.StateStorageBytes(state), observedAt),
		metadata,
	))
}

func (m *Manager) recordSnapshotStorageWithMetadata(ctx context.Context, snapshot *db.Snapshot, metadata *meteringpkg.StorageObservation) error {
	if snapshot == nil {
		return nil
	}
	return m.appendStorageObservation(ctx, applyStorageObservationMetadata(
		m.snapshotStorageObservation(ctx, snapshot, snapshot.CreatedAt),
		metadata,
	))
}

func (m *Manager) volumeStorageObservation(ctx context.Context, vol *db.SandboxVolume, sizeBytes int64, observedAt time.Time) *meteringpkg.StorageObservation {
	return volume.VolumeStorageObservation(ctx, m.repo, vol, m.regionID(), m.clusterID, sizeBytes, observedAt)
}

func applyStorageObservationMetadata(obs *meteringpkg.StorageObservation, metadata *meteringpkg.StorageObservation) *meteringpkg.StorageObservation {
	if obs == nil || metadata == nil {
		return obs
	}
	if metadata.Product != "" {
		obs.Product = metadata.Product
	}
	if metadata.OwnerKind != "" {
		obs.OwnerKind = metadata.OwnerKind
	}
	return obs
}

func (m *Manager) snapshotStorageObservation(ctx context.Context, snapshot *db.Snapshot, observedAt time.Time) *meteringpkg.StorageObservation {
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	obs := &meteringpkg.StorageObservation{
		SubjectType:       meteringpkg.SubjectTypeSnapshot,
		SubjectID:         snapshot.ID,
		Product:           meteringpkg.ProductSandbox,
		TeamID:            snapshot.TeamID,
		UserID:            snapshot.UserID,
		VolumeID:          snapshot.VolumeID,
		SnapshotID:        snapshot.ID,
		RegionID:          m.regionID(),
		ClusterID:         m.clusterID,
		SizeBytes:         snapshot.SizeBytes,
		ResourceCreatedAt: snapshot.CreatedAt,
		ObservedAt:        observedAt,
	}
	if owner, err := m.repo.GetSandboxVolumeOwner(ctx, snapshot.VolumeID); err == nil && owner != nil {
		obs.OwnerKind = owner.OwnerKind
		obs.SandboxID = owner.OwnerSandboxID
		if owner.OwnerClusterID != "" {
			obs.ClusterID = owner.OwnerClusterID
		}
	}
	return obs
}
