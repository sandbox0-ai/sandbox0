package pglock

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultUnlockTimeout = 5 * time.Second

// Mode selects the PostgreSQL advisory lock mode.
type Mode int

const (
	// Exclusive blocks both shared and exclusive lockers for the same key.
	Exclusive Mode = iota
	// Shared allows other shared lockers and blocks exclusive lockers for the same key.
	Shared
)

type advisoryConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Release()
}

type acquireFunc func(context.Context) (advisoryConn, error)

// Locker runs callbacks while holding PostgreSQL session-level advisory locks.
type Locker struct {
	acquire       acquireFunc
	unlockTimeout time.Duration
}

// Option configures a Locker.
type Option func(*Locker)

// WithUnlockTimeout sets the timeout used for best-effort unlock calls.
func WithUnlockTimeout(timeout time.Duration) Option {
	return func(l *Locker) {
		if timeout > 0 {
			l.unlockTimeout = timeout
		}
	}
}

// New creates a Locker backed by pgxpool. A nil pool degrades to executing
// callbacks without locking, which keeps optional database deployments working.
func New(pool *pgxpool.Pool, opts ...Option) *Locker {
	var acquire acquireFunc
	if pool != nil {
		acquire = func(ctx context.Context) (advisoryConn, error) {
			return pool.Acquire(ctx)
		}
	}
	return newLocker(acquire, opts...)
}

func newLocker(acquire acquireFunc, opts ...Option) *Locker {
	l := &Locker{
		acquire:       acquire,
		unlockTimeout: defaultUnlockTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(l)
		}
	}
	return l
}

// WithExclusive runs fn while holding an exclusive advisory lock for resource.
func (l *Locker) WithExclusive(ctx context.Context, resource string, fn func(context.Context) error) error {
	return l.With(ctx, resource, Exclusive, fn)
}

// WithShared runs fn while holding a shared advisory lock for resource.
func (l *Locker) WithShared(ctx context.Context, resource string, fn func(context.Context) error) error {
	return l.With(ctx, resource, Shared, fn)
}

// With runs fn while holding an advisory lock for resource.
func (l *Locker) With(ctx context.Context, resource string, mode Mode, fn func(context.Context) error) error {
	if resource == "" {
		return runWithoutLock(ctx, fn)
	}
	return l.WithKey(ctx, Key(resource), mode, fn)
}

// WithKey runs fn while holding an advisory lock for key.
func (l *Locker) WithKey(ctx context.Context, key int64, mode Mode, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	if l == nil || l.acquire == nil {
		return fn(ctx)
	}

	c, err := l.acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire advisory lock connection: %w", err)
	}
	defer c.Release()

	lockSQL, unlockSQL := lockStatements(mode)
	if _, err := c.Exec(ctx, lockSQL, key); err != nil {
		return fmt.Errorf("acquire advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), l.unlockTimeout)
		defer cancel()
		_, _ = c.Exec(unlockCtx, unlockSQL, key)
	}()

	return fn(ctx)
}

// Key converts a stable resource name into a PostgreSQL advisory lock key.
func Key(resource string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(resource))
	return int64(h.Sum64())
}

func lockStatements(mode Mode) (string, string) {
	if mode == Shared {
		return "SELECT pg_advisory_lock_shared($1)", "SELECT pg_advisory_unlock_shared($1)"
	}
	return "SELECT pg_advisory_lock($1)", "SELECT pg_advisory_unlock($1)"
}

func runWithoutLock(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	return fn(ctx)
}
