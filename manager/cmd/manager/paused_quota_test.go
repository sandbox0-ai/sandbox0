package main

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/stretchr/testify/require"
)

func TestDefaultPausedSandboxQuotaCanBeOverridden(t *testing.T) {
	defaults := defaultTeamQuotaLimits(&config.ManagerConfig{})
	require.Equal(t, []quota.DefaultLimit{
		{Dimension: quota.DimensionPausedSandboxes, LimitValue: 2000},
		{Dimension: quota.DimensionSnapshotsPerSandbox, LimitValue: 10},
	}, defaults)
	configured := defaultTeamQuotaLimits(&config.ManagerConfig{DefaultTeamQuotas: []config.TeamQuotaLimitConfig{
		{Dimension: "paused_sandboxes", LimitValue: 5000},
		{Dimension: "active_sandboxes", LimitValue: 20},
	}})
	require.Len(t, configured, 3)
	require.Equal(t, int64(5000), configured[0].LimitValue)
	require.Nil(t, defaultTeamQuotaLimits(nil))
}

func TestDefaultSnapshotRetentionQuotaCanBeOverridden(t *testing.T) {
	configured := defaultTeamQuotaLimits(&config.ManagerConfig{DefaultTeamQuotas: []config.TeamQuotaLimitConfig{
		{Dimension: "snapshots_per_sandbox", LimitValue: 25},
	}})
	require.Equal(t, []quota.DefaultLimit{
		{Dimension: quota.DimensionSnapshotsPerSandbox, LimitValue: 25},
		{Dimension: quota.DimensionPausedSandboxes, LimitValue: 2000},
	}, configured)
}

func TestManagerQuotaUsageReadsPausedCapacityFromPostgres(t *testing.T) {
	counter := &managerQuotaActiveCounter{current: 2001}
	metering := &managerQuotaMeteringStore{current: 99}
	store := &managerQuotaUsageStore{activeSandboxes: counter, metering: metering}
	current, err := store.CurrentUsage(t.Context(), "team-a", quota.DimensionPausedSandboxes)
	require.NoError(t, err)
	require.Equal(t, int64(2001), current)
	require.Equal(t, 1, counter.calls)
	require.Zero(t, metering.calls)
}
