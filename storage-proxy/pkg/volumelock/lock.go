package volumelock

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
)

const unlockTimeout = 5 * time.Second

// Barrier serializes volume mutations and bootstrap snapshots across instances.
type Barrier interface {
	WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error
	WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error
}

// Locker implements Barrier with PostgreSQL advisory locks.
type Locker struct {
	pool    *pgxpool.Pool
	metrics *obsmetrics.StorageProxyMetrics
}

func New(pool *pgxpool.Pool) *Locker {
	return &Locker{pool: pool}
}

func (l *Locker) SetMetrics(metrics *obsmetrics.StorageProxyMetrics) {
	if l == nil {
		return
	}
	l.metrics = metrics
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

	mode := "exclusive"
	if shared {
		mode = "shared"
	}

	start := time.Now()
	conn, err := l.pool.Acquire(ctx)
	l.observeStage(mode, "acquire_connection", start)
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

	start = time.Now()
	if _, err := conn.Exec(ctx, lockSQL, key); err != nil {
		l.observeStage(mode, "acquire_lock", start)
		return fmt.Errorf("acquire volume advisory lock: %w", err)
	}
	l.observeStage(mode, "acquire_lock", start)
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), unlockTimeout)
		defer cancel()
		start := time.Now()
		_, _ = conn.Exec(unlockCtx, unlockSQL, key)
		l.observeStage(mode, "release_lock", start)
	}()

	start = time.Now()
	err = fn(ctx)
	l.observeStage(mode, "critical_section", start)
	return err
}

func (l *Locker) observeStage(mode, stage string, start time.Time) {
	if l == nil || l.metrics == nil {
		return
	}
	l.metrics.ObserveVolumeMutationBarrierStage(mode, stage, time.Since(start))
}

func advisoryKey(volumeID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(volumeID))
	return int64(h.Sum64())
}
