// Package activeconnections binds the generic concurrency lease engine to the
// active_connection_count Team Quota key.
package activeconnections

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/concurrency"
)

// Lease is one active connection or session-channel allocation.
type Lease interface {
	Done() <-chan struct{}
	Err() error
	Release(context.Context) error
}

// Quota acquires exact region-shared active connection leases.
type Quota interface {
	Acquire(context.Context, string) (Lease, error)
	Close() error
}

// RedisQuota owns the concurrency limiter used for active connections.
type RedisQuota struct {
	limiter *concurrency.Limiter
}

// NewRedis creates a fail-closed region-shared active connection quota.
func NewRedis(
	ctx context.Context,
	resolver concurrency.Resolver,
	cfg concurrency.Config,
) (*RedisQuota, error) {
	limiter, err := concurrency.NewRedisLimiter(ctx, resolver, cfg)
	if err != nil {
		return nil, err
	}
	return &RedisQuota{limiter: limiter}, nil
}

// Acquire claims one active_connection_count slot for teamID.
func (q *RedisQuota) Acquire(
	ctx context.Context,
	teamID string,
) (Lease, error) {
	if q == nil || q.limiter == nil {
		return nil, &teamquota.UnavailableError{
			Operation: "acquire active connection quota",
			Err:       fmt.Errorf("active connection quota is not configured"),
		}
	}
	return q.limiter.Acquire(
		ctx,
		teamID,
		teamquota.KeyActiveConnectionCount,
	)
}

// Close releases the concurrency limiter's Redis clients.
func (q *RedisQuota) Close() error {
	if q == nil || q.limiter == nil {
		return nil
	}
	return q.limiter.Close()
}
