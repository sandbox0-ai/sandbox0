package volumelock

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/pglock"
)

// Barrier serializes volume mutations and bootstrap snapshots across instances.
type Barrier interface {
	WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error
	WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error
}

// Locker implements Barrier with PostgreSQL advisory locks.
type Locker struct {
	locks *pglock.Locker
}

func New(pool *pgxpool.Pool) *Locker {
	return &Locker{locks: pglock.New(pool)}
}

func (l *Locker) WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	if l == nil || l.locks == nil {
		return runWithoutLock(ctx, fn)
	}
	return l.locks.WithShared(ctx, volumeID, fn)
}

func (l *Locker) WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	if l == nil || l.locks == nil {
		return runWithoutLock(ctx, fn)
	}
	return l.locks.WithExclusive(ctx, volumeID, fn)
}

func runWithoutLock(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	return fn(ctx)
}
