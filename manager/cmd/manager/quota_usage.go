package main

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/quota"
)

type activeSandboxUsageCounter interface {
	CountActiveSandboxes(context.Context, string) (int64, error)
	CountPausedSandboxes(context.Context, string) (int64, error)
	MaxSnapshotsPerSandbox(context.Context, string) (int64, error)
}

// managerQuotaUsageStore reads current active sandbox capacity from the
// PostgreSQL control projection and delegates historical usage dimensions to
// the asynchronous metering read model.
type managerQuotaUsageStore struct {
	activeSandboxes activeSandboxUsageCounter
	metering        quota.UsageStore
}

func (s *managerQuotaUsageStore) CurrentUsage(
	ctx context.Context,
	teamID string,
	dimension quota.Dimension,
) (int64, error) {
	if dimension == quota.DimensionActiveSandboxes || dimension == quota.DimensionPausedSandboxes || dimension == quota.DimensionSnapshotsPerSandbox {
		if s == nil || s.activeSandboxes == nil {
			return 0, quota.ErrUsageStoreNotConfigured
		}
		var current int64
		var err error
		switch dimension {
		case quota.DimensionSnapshotsPerSandbox:
			current, err = s.activeSandboxes.MaxSnapshotsPerSandbox(ctx, teamID)
		case quota.DimensionPausedSandboxes:
			current, err = s.activeSandboxes.CountPausedSandboxes(ctx, teamID)
		default:
			current, err = s.activeSandboxes.CountActiveSandboxes(ctx, teamID)
		}
		if err != nil {
			return 0, fmt.Errorf("count sandbox capacity usage: %w", err)
		}
		return current, nil
	}
	if s == nil || s.metering == nil {
		return 0, quota.ErrUsageStoreNotConfigured
	}
	return s.metering.CurrentUsage(ctx, teamID, dimension)
}
