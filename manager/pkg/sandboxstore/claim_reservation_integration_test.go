package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReserveSandboxClaimSerializesConcurrentTeamQuotaAdmission(t *testing.T) {
	ctx := context.Background()
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	const (
		attempts = 32
		limit    = int64(5)
	)

	start := make(chan struct{})
	results := make(chan error, attempts)
	var workers sync.WaitGroup
	for i := 0; i < attempts; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-start
			_, err := store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{
				Record:             rootFSTestSandboxRecord(fmt.Sprintf("sandbox-%02d", index), "team-1"),
				ActiveSandboxLimit: int64Pointer(limit),
			})
			results <- err
		}(i)
	}
	close(start)
	workers.Wait()
	close(results)

	var admitted, rejected int
	for err := range results {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, ErrActiveSandboxQuotaExceeded):
			rejected++
		default:
			t.Fatalf("unexpected reservation error: %v", err)
		}
	}
	require.Equal(t, int(limit), admitted)
	require.Equal(t, attempts-int(limit), rejected)
	current, err := store.CountActiveSandboxes(ctx, "team-1")
	require.NoError(t, err)
	require.Equal(t, limit, current)
}

func TestReserveSandboxClaimAllowsRetryWithoutAnotherQuotaSlot(t *testing.T) {
	ctx := context.Background()
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	record := rootFSTestSandboxRecord("sandbox-retry", "team-1")
	one := int64(1)

	created, err := store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{
		Record: record, ActiveSandboxLimit: &one,
	})
	require.NoError(t, err)
	require.Equal(t, record.ID, created.ID)

	zero := int64(0)
	retried, err := store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{
		Record: record, ActiveSandboxLimit: &zero,
	})
	require.NoError(t, err)
	require.Equal(t, created.ID, retried.ID)
	current, err := store.CountActiveSandboxes(ctx, "team-1")
	require.NoError(t, err)
	require.Equal(t, int64(1), current)
}

func int64Pointer(value int64) *int64 {
	return &value
}
