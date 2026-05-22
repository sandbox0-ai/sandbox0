package snapshot

import (
	"context"
	"errors"
	"time"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

func (m *Manager) ObserveVolumeState(ctx context.Context, volumeID, teamID string, state *s0fs.SnapshotState, observedAt time.Time) error {
	if m == nil || state == nil || volumeID == "" {
		return nil
	}
	vol, err := m.repo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil
		}
		return err
	}
	if teamID != "" && vol.TeamID != teamID {
		return nil
	}
	return m.appendStorageObservation(ctx, m.volumeStorageObservation(ctx, vol, s0fs.StateStorageBytes(state), observedAt))
}

func (m *Manager) recordVolumeStorageState(ctx context.Context, vol *db.SandboxVolume, state *s0fs.SnapshotState, observedAt time.Time) error {
	if vol == nil || state == nil {
		return nil
	}
	return m.appendStorageObservation(ctx, m.volumeStorageObservation(ctx, vol, s0fs.StateStorageBytes(state), observedAt))
}

func (m *Manager) recordSnapshotStorage(ctx context.Context, snapshot *db.Snapshot) error {
	if snapshot == nil {
		return nil
	}
	return m.appendStorageObservation(ctx, m.snapshotStorageObservation(ctx, snapshot, snapshot.CreatedAt))
}

func (m *Manager) volumeStorageObservation(ctx context.Context, vol *db.SandboxVolume, sizeBytes int64, observedAt time.Time) *meteringpkg.StorageObservation {
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
		RegionID:          m.regionID(),
		ClusterID:         m.clusterID,
		SizeBytes:         sizeBytes,
		ResourceCreatedAt: vol.CreatedAt,
		ObservedAt:        observedAt,
	}
	if owner, err := m.repo.GetSandboxVolumeOwner(ctx, vol.ID); err == nil && owner != nil {
		obs.OwnerKind = owner.OwnerKind
		obs.SandboxID = owner.OwnerSandboxID
		if owner.OwnerClusterID != "" {
			obs.ClusterID = owner.OwnerClusterID
		}
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
