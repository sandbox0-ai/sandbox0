package apikey

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestConcurrentAPIKeyValidationPreservesUsageCountsWhileUsageRowLocked(t *testing.T) {
	pool := newGatewayAPIKeyTestPool(t)
	if pool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	repo := NewRepository(pool, WithLocalTeamValidation(false))
	key, raw, err := repo.CreateAPIKey(ctx, uuid.NewString(), "test-region", uuid.NewString(), "concurrent usage", ScopeTeam, []string{"viewer"}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	blocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(ctx, "UPDATE api_keys SET usage_count=usage_count WHERE id=$1", key.ID)
	require.NoError(t, err)
	const attempts = 128
	start := time.Now()
	var workers sync.WaitGroup
	results := make(chan error, attempts)
	for wave := range 2 {
		for range attempts / 2 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				_, err := repo.ValidateAPIKey(ctx, raw)
				results <- err
			}()
		}
		workers.Wait()
		for range attempts / 2 {
			require.NoError(t, <-results)
		}
		if wave == 0 {
			// Let the first wave's asynchronous writes reach the held row.
			// A later command's authentication must still be able to read it.
			time.Sleep(100 * time.Millisecond)
		}
	}
	// Display telemetry may wait for a locked key row; authenticated reads
	// must still finish using the remaining connections in this four-slot pool.
	require.NoError(t, blocker.Rollback(ctx))
	var count int64
	var usedAt time.Time
	require.Eventually(t, func() bool {
		err := pool.QueryRow(ctx, "SELECT usage_count, last_used_at FROM api_keys WHERE id=$1", key.ID).Scan(&count, &usedAt)
		return err == nil && count >= attempts
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(attempts), count)
	require.False(t, usedAt.Before(start))
	_, err = pool.Exec(ctx, "UPDATE api_keys SET expires_at=$2 WHERE id=$1", key.ID, time.Now().Add(-time.Hour))
	require.NoError(t, err)
	_, err = repo.ValidateAPIKey(ctx, raw)
	require.True(t, errors.Is(err, ErrExpiredKey))
	_, err = repo.ValidateAPIKey(context.Background(), "invalid")
	require.ErrorIs(t, err, ErrInvalidKey)
	require.NoError(t, pool.QueryRow(ctx, "SELECT usage_count FROM api_keys WHERE id=$1", key.ID).Scan(&count))
	require.Equal(t, int64(attempts), count)
}
