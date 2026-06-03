package snapshot

import (
	"context"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
)

func (m *Manager) enforceStorageObservationQuota(ctx context.Context, observation *meteringpkg.StorageObservation) error {
	if m == nil || observation == nil || observation.TeamID == "" {
		return nil
	}
	m.mu.RLock()
	repo := m.quotaRepo
	m.mu.RUnlock()
	if repo == nil {
		return nil
	}
	dimension, ok := storageQuotaDimension(observation.SubjectType)
	if !ok {
		return nil
	}
	_, err := repo.CheckProjectedStorageUsageGB(ctx, observation.TeamID, dimension, observation.SubjectType, observation.SubjectID, observation.SizeBytes)
	return err
}

func storageQuotaDimension(subjectType string) (quota.Dimension, bool) {
	switch subjectType {
	case meteringpkg.SubjectTypeVolume:
		return quota.DimensionVolumeStorageGB, true
	case meteringpkg.SubjectTypeSnapshot:
		return quota.DimensionSnapshotGB, true
	case meteringpkg.SubjectTypeFilesystem:
		return quota.DimensionFilesystemGB, true
	case meteringpkg.SubjectTypeFilesystemSnapshot:
		return quota.DimensionFSSnapshotGB, true
	default:
		return "", false
	}
}
