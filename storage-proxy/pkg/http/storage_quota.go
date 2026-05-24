package http

import (
	"context"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
)

func (s *Server) enforceStorageObservationQuota(ctx context.Context, observation *meteringpkg.StorageObservation) error {
	if s == nil || s.quotaRepo == nil || observation == nil || observation.TeamID == "" {
		return nil
	}
	dimension, ok := storageQuotaDimension(observation.SubjectType)
	if !ok {
		return nil
	}
	_, err := s.quotaRepo.CheckProjectedStorageUsageGB(ctx, observation.TeamID, dimension, observation.SubjectType, observation.SubjectID, observation.SizeBytes)
	return err
}

func (s *Server) enforceVolumeStorageAdditionalQuota(ctx context.Context, volume *db.SandboxVolume, additionalBytes int64) error {
	if s == nil || s.quotaRepo == nil || volume == nil || volume.TeamID == "" || additionalBytes <= 0 {
		return nil
	}
	_, err := s.quotaRepo.CheckAdditionalStorageUsageGB(ctx, volume.TeamID, quota.DimensionVolumeStorageGB, meteringpkg.SubjectTypeVolume, additionalBytes)
	return err
}

func storageQuotaDimension(subjectType string) (quota.Dimension, bool) {
	switch subjectType {
	case meteringpkg.SubjectTypeVolume:
		return quota.DimensionVolumeStorageGB, true
	case meteringpkg.SubjectTypeSnapshot:
		return quota.DimensionSnapshotGB, true
	default:
		return "", false
	}
}
