package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
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

func (s *SandboxService) enforceSandboxCPUQuota(ctx context.Context, teamID string, template *v1alpha1.SandboxTemplate) error {
	requested := templateCPUMillicpu(template)
	return s.enforceQuota(ctx, teamID, quota.DimensionCPU, requested)
}

func (s *SandboxService) enforceSandboxMemoryQuota(ctx context.Context, teamID string, template *v1alpha1.SandboxTemplate) error {
	requested := templateMemoryMiB(template)
	return s.enforceQuota(ctx, teamID, quota.DimensionMemory, requested)
}

func (s *SandboxService) enforceQuota(ctx context.Context, teamID string, dimension quota.Dimension, requested int64) error {
	teamID = strings.TrimSpace(teamID)
	if s == nil || s.quotaStore == nil || teamID == "" {
		return nil
	}
	limit, err := s.quotaStore.GetLimit(ctx, teamID, dimension)
	if err != nil {
		return fmt.Errorf("load %s quota: %w", dimension, err)
	}
	if limit == nil {
		return nil
	}
	current, err := s.quotaStore.CurrentUsage(ctx, teamID, dimension)
	if err != nil {
		return fmt.Errorf("load %s usage: %w", dimension, err)
	}
	decision := quota.Check(teamID, dimension, current, requested, limit)
	if decision.Allowed {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrQuotaExceeded, decision.Err())
}

func templateCPUMillicpu(template *v1alpha1.SandboxTemplate) int64 {
	if template == nil {
		return 0
	}
	return template.Spec.MainContainer.Resources.CPU.MilliValue()
}

func templateMemoryMiB(template *v1alpha1.SandboxTemplate) int64 {
	if template == nil {
		return 0
	}
	return bytesToMiBRoundUp(template.Spec.MainContainer.Resources.Memory.Value())
}

func bytesToMiBRoundUp(value int64) int64 {
	if value <= 0 {
		return 0
	}
	const mib = int64(1024 * 1024)
	return (value + mib - 1) / mib
}
