package volumelock

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultUnlockTimeout = 5 * time.Second

// Barrier serializes volume mutations and bootstrap snapshots across instances.
type Barrier interface {
	WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error
	WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error
}

type advisoryConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Release()
}

type acquireFunc func(context.Context) (advisoryConn, error)

// Locker implements Barrier with PostgreSQL advisory locks.
type Locker struct {
	acquire       acquireFunc
	unlockTimeout time.Duration
}

func New(pool *pgxpool.Pool) *Locker {
	var acquire acquireFunc
	if pool != nil {
		acquire = func(ctx context.Context) (advisoryConn, error) {
			return pool.Acquire(ctx)
		}
	}
	return newLocker(acquire)
}

func newLocker(acquire acquireFunc) *Locker {
	return &Locker{
		acquire:       acquire,
		unlockTimeout: defaultUnlockTimeout,
	}
}

func (l *Locker) WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	return l.with(ctx, volumeID, "SELECT pg_advisory_lock_shared($1)", "SELECT pg_advisory_unlock_shared($1)", fn)
}

func (l *Locker) WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	return l.with(ctx, volumeID, "SELECT pg_advisory_lock($1)", "SELECT pg_advisory_unlock($1)", fn)
}

func (l *Locker) with(ctx context.Context, volumeID, lockSQL, unlockSQL string, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	if volumeID == "" || l == nil || l.acquire == nil {
		return fn(ctx)
	}

	conn, err := l.acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire advisory lock connection: %w", err)
	}
	defer conn.Release()

	key := advisoryKey(volumeID)
	if _, err := conn.Exec(ctx, lockSQL, key); err != nil {
		return fmt.Errorf("acquire advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), l.unlockTimeout)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, unlockSQL, key)
	}()

	return fn(ctx)
}

func advisoryKey(volumeID string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(volumeID))
	return int64(hash.Sum64())
}
