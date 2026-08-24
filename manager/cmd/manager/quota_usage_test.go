package main

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/stretchr/testify/require"
)

type managerQuotaActiveCounter struct {
	current int64
	err     error
	calls   int
}

func (c *managerQuotaActiveCounter) CountActiveSandboxes(context.Context, string) (int64, error) {
	c.calls++
	return c.current, c.err
}

type managerQuotaMeteringStore struct {
	current   int64
	dimension quota.Dimension
	calls     int
}

func (s *managerQuotaMeteringStore) CurrentUsage(
	_ context.Context,
	_ string,
	dimension quota.Dimension,
) (int64, error) {
	s.calls++
	s.dimension = dimension
	return s.current, nil
}

func TestManagerQuotaUsageStoreReadsActiveCapacityFromPostgres(t *testing.T) {
	counter := &managerQuotaActiveCounter{current: 7}
	metering := &managerQuotaMeteringStore{current: 99}
	store := &managerQuotaUsageStore{activeSandboxes: counter, metering: metering}

	current, err := store.CurrentUsage(context.Background(), "team-a", quota.DimensionActiveSandboxes)
	require.NoError(t, err)
	require.Equal(t, int64(7), current)
	require.Equal(t, 1, counter.calls)
	require.Zero(t, metering.calls)
}

func TestManagerQuotaUsageStoreDelegatesHistoricalUsage(t *testing.T) {
	counter := &managerQuotaActiveCounter{current: 7}
	metering := &managerQuotaMeteringStore{current: 42}
	store := &managerQuotaUsageStore{activeSandboxes: counter, metering: metering}

	current, err := store.CurrentUsage(context.Background(), "team-a", quota.DimensionNetworkEgress)
	require.NoError(t, err)
	require.Equal(t, int64(42), current)
	require.Zero(t, counter.calls)
	require.Equal(t, 1, metering.calls)
	require.Equal(t, quota.DimensionNetworkEgress, metering.dimension)
}

func TestManagerQuotaUsageStoreRequiresAvailableProjection(t *testing.T) {
	store := &managerQuotaUsageStore{}

	_, err := store.CurrentUsage(context.Background(), "team-a", quota.DimensionActiveSandboxes)
	require.ErrorIs(t, err, quota.ErrUsageStoreNotConfigured)
	_, err = store.CurrentUsage(context.Background(), "team-a", quota.DimensionNetworkEgress)
	require.ErrorIs(t, err, quota.ErrUsageStoreNotConfigured)

	counterErr := errors.New("database unavailable")
	store.activeSandboxes = &managerQuotaActiveCounter{err: counterErr}
	_, err = store.CurrentUsage(context.Background(), "team-a", quota.DimensionActiveSandboxes)
	require.ErrorIs(t, err, counterErr)
}
