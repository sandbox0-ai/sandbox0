// Package clock provides a synchronized clock across multiple clusters
// by periodically syncing with a shared PostgreSQL database.
//
// This solves the problem where different clusters may have inconsistent
// local times, leading to incorrect expiration checks and other time-based logic.
package clock

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// DB is the minimal interface required for time synchronization. Use NewPGX
// with pgx connections and pools, whose concrete Row return type requires an
// adapter.
type DB interface {
	QueryRow(ctx context.Context, sql string, args ...any) Row
}

// Row represents a single row result from a query.
type Row interface {
	Scan(dest ...any) error
}

// Logger is the interface for logging.
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

// nopLogger is a no-op implementation of Logger.
type nopLogger struct{}

// syncError wraps an error for atomic.Value storage.
// atomic.Value requires consistent types, so we always store this wrapper.
type syncError struct {
	err error
}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// Clock provides time synchronized with a shared PostgreSQL database.
// It uses monotonic clock internally to prevent time from going backward.
type Clock struct {
	mu sync.RWMutex

	// offset is the difference between PG time and local time (PG - local)
	offset time.Duration

	// lastSync is the local time when last sync occurred
	lastSync time.Time

	// lastRTT is the round-trip time of the last sync query
	lastRTT time.Duration

	// localStart is the monotonic reference point
	localStart time.Time

	// pgTimeAtSync is the PG time at last sync (for reference)
	pgTimeAtSync time.Time

	// syncCount tracks successful sync count
	syncCount atomic.Int64

	// lastSyncError stores the last sync error
	lastSyncError atomic.Value // stores error

	// config
	syncInterval  time.Duration
	syncTimeout   time.Duration
	maxOffsetDiff time.Duration
	logger        Logger

	// lifecycle
	cancel context.CancelFunc
	done   chan struct{}
}

// Option configures the Clock.
type Option func(*Clock)

// WithSyncInterval sets the interval between sync attempts.
// Default is 30 seconds.
func WithSyncInterval(d time.Duration) Option {
	return func(c *Clock) {
		if d > 0 {
			c.syncInterval = d
		}
	}
}

// WithLogger sets the logger for the Clock.
func WithLogger(l Logger) Option {
	return func(c *Clock) {
		if l != nil {
			c.logger = l
		}
	}
}

// New creates a new Clock synchronized with the database.
// It performs an initial sync and starts background synchronization.
func New(ctx context.Context, db DB, opts ...Option) (*Clock, error) {
	c := &Clock{
		syncInterval:  30 * time.Second,
		syncTimeout:   5 * time.Second,
		maxOffsetDiff: 1 * time.Second,
		logger:        nopLogger{},
		done:          make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	// Perform initial sync
	if err := c.sync(ctx, db); err != nil {
		return nil, err
	}

	// Snapshot synced values before starting background updates.
	c.mu.RLock()
	initOffset := c.offset
	initRTT := c.lastRTT
	c.mu.RUnlock()
	c.logger.Info("clock initialized",
		"offset_ms", initOffset.Milliseconds(),
		"rtt_ms", initRTT.Milliseconds(),
	)

	// Start background sync
	bgCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go c.backgroundSync(bgCtx, db)

	return c, nil
}

// Now returns the current synchronized time.
// It uses monotonic elapsed time to prevent time from going backward.
func (c *Clock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Use monotonic elapsed time since last sync
	elapsed := time.Since(c.localStart)
	return c.pgTimeAtSync.Add(elapsed)
}

// Since returns the time elapsed since t.
// This is equivalent to c.Now().Sub(t).
func (c *Clock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

// Until returns the duration until t.
// This is equivalent to t.Sub(c.Now()).
func (c *Clock) Until(t time.Time) time.Duration {
	return t.Sub(c.Now())
}

// Offset returns the current offset between PG time and local time.
func (c *Clock) Offset() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.offset
}

// LastRTT returns the round-trip time of the last sync query.
func (c *Clock) LastRTT() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastRTT
}

// SyncCount returns the number of successful syncs.
func (c *Clock) SyncCount() int64 {
	return c.syncCount.Load()
}

// LastSyncError returns the error from the last sync attempt, or nil if successful.
func (c *Clock) LastSyncError() error {
	if v := c.lastSyncError.Load(); v != nil {
		return v.(syncError).err
	}
	return nil
}

// TimeSinceLastSync returns the duration since the last successful sync.
func (c *Clock) TimeSinceLastSync() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return time.Since(c.lastSync)
}

// Close stops the background sync goroutine.
func (c *Clock) Close() {
	if c.cancel != nil {
		c.cancel()
		<-c.done
	}
}

// sync performs a single synchronization with the database.
func (c *Clock) sync(ctx context.Context, db DB) error {
	syncCtx, cancel := context.WithTimeout(ctx, c.syncTimeout)
	defer cancel()

	// Measure round-trip time
	start := time.Now()
	var pgTime time.Time
	if err := db.QueryRow(syncCtx, "SELECT NOW()").Scan(&pgTime); err != nil {
		return err
	}
	rtt := time.Since(start)

	// Calculate offset, compensating for network delay (half RTT)
	localMidpoint := start.Add(rtt / 2)
	newOffset := pgTime.Sub(localMidpoint)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for large offset changes (potential clock drift or network issues)
	if c.syncCount.Load() > 0 {
		offsetDiff := newOffset - c.offset
		if offsetDiff < 0 {
			offsetDiff = -offsetDiff
		}
		if offsetDiff > c.maxOffsetDiff {
			c.logger.Warn("large clock offset change detected",
				"old_offset_ms", c.offset.Milliseconds(),
				"new_offset_ms", newOffset.Milliseconds(),
				"diff_ms", offsetDiff.Milliseconds(),
			)
		}
	}

	c.offset = newOffset
	c.lastSync = time.Now()
	c.lastRTT = rtt
	c.localStart = time.Now()
	c.pgTimeAtSync = pgTime

	c.syncCount.Add(1)
	c.lastSyncError.Store(syncError{err: nil})

	return nil
}

// backgroundSync periodically syncs with the database.
func (c *Clock) backgroundSync(ctx context.Context, db DB) {
	defer close(c.done)

	ticker := time.NewTicker(c.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.sync(ctx, db); err != nil {
				c.lastSyncError.Store(syncError{err: err})
				c.logger.Error("clock sync failed",
					"error", err,
					"time_since_last_sync", c.TimeSinceLastSync(),
				)
			}
		case <-ctx.Done():
			c.logger.Info("clock background sync stopped")
			return
		}
	}
}
