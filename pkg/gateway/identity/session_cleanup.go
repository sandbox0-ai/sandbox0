package identity

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	DefaultIdentitySessionCleanupInterval  = time.Minute
	DefaultIdentitySessionCleanupBatchSize = 1_000
)

// CleanupIdentitySessionsBatch deletes one bounded batch from each ephemeral
// authentication table. Each repository method uses SKIP LOCKED, so every
// identity-owning gateway replica may run this safely.
func (r *Repository) CleanupIdentitySessionsBatch(ctx context.Context, batchSize int) error {
	if batchSize <= 0 {
		batchSize = DefaultIdentitySessionCleanupBatchSize
	}
	var cleanupErrors []error
	if _, err := r.CleanupExpiredTokensBatch(ctx, batchSize); err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("refresh tokens: %w", err))
	}
	if _, err := r.CleanupExpiredWebLoginCodesBatch(ctx, batchSize); err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("web login codes: %w", err))
	}
	if _, err := r.CleanupExpiredDeviceAuthSessionsBatch(ctx, batchSize); err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("device auth sessions: %w", err))
	}
	if _, err := r.CleanupExpiredOIDCPendingStatesBatch(ctx, batchSize); err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("OIDC pending states: %w", err))
	}
	return errors.Join(cleanupErrors...)
}

// RunIdentitySessionCleanup runs immediate and periodic bounded cleanup until
// ctx is canceled. onError is optional and must not block indefinitely.
func (r *Repository) RunIdentitySessionCleanup(
	ctx context.Context,
	interval time.Duration,
	batchSize int,
	onError func(error),
) {
	if interval <= 0 {
		interval = DefaultIdentitySessionCleanupInterval
	}
	if batchSize <= 0 {
		batchSize = DefaultIdentitySessionCleanupBatchSize
	}
	cleanup := func() {
		if err := r.CleanupIdentitySessionsBatch(ctx, batchSize); err != nil && onError != nil {
			onError(err)
		}
	}
	cleanup()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanup()
		}
	}
}
