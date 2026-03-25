package volumelock

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const unlockTimeout = 5 * time.Second

// Barrier serializes volume mutations and bootstrap snapshots across instances.
type Barrier interface {
	WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error
	WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error
}

// Locker implements Barrier with PostgreSQL advisory locks.
type Locker struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Locker {
	return &Locker{pool: pool}
}

func (l *Locker) WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	return l.withLock(ctx, volumeID, true, fn)
}

func (l *Locker) WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	return l.withLock(ctx, volumeID, false, fn)
}

func (l *Locker) withLock(ctx context.Context, volumeID string, shared bool, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	if l == nil || l.pool == nil || volumeID == "" {
		return fn(ctx)
	}

	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire advisory lock connection: %w", err)
	}
	defer conn.Release()

	key := advisoryKey(volumeID)
	lockSQL := "SELECT pg_advisory_lock($1)"
	unlockSQL := "SELECT pg_advisory_unlock($1)"
	if shared {
		lockSQL = "SELECT pg_advisory_lock_shared($1)"
		unlockSQL = "SELECT pg_advisory_unlock_shared($1)"
	}

	if _, err := conn.Exec(ctx, lockSQL, key); err != nil {
		return fmt.Errorf("acquire volume advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), unlockTimeout)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, unlockSQL, key)
	}()

	return fn(ctx)
}

func advisoryKey(volumeID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(volumeID))
	return int64(h.Sum64())
}
