package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/quota"
)

func (s *SandboxService) enforceActiveSandboxQuota(ctx context.Context, teamID string) error {
	teamID = strings.TrimSpace(teamID)
	if s == nil || s.quotaStore == nil || teamID == "" {
		return nil
	}
	limit, err := s.quotaStore.GetLimit(ctx, teamID, quota.DimensionActiveSandboxes)
	if err != nil {
		return fmt.Errorf("load active sandbox quota: %w", err)
	}
	if limit == nil {
		return nil
	}
	current, err := s.quotaStore.CurrentUsage(ctx, teamID, quota.DimensionActiveSandboxes)
	if err != nil {
		return fmt.Errorf("load active sandbox usage: %w", err)
	}
	decision := quota.Check(teamID, quota.DimensionActiveSandboxes, current, 1, limit)
	if decision.Allowed {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrQuotaExceeded, decision.Err())
}
