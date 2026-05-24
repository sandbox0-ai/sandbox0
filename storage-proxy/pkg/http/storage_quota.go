package http

import (
	"context"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
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

func storageQuotaDimension(subjectType string) (quota.Dimension, bool) {
	switch subjectType {
	case meteringpkg.SubjectTypeVolume:
		return quota.DimensionVolumeStorageGB, true
	default:
		return "", false
	}
}
