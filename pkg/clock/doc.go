// Package clock provides a synchronized clock across multiple clusters
// by periodically syncing with a shared PostgreSQL database.
//
// # Problem
//
// In a multi-cluster deployment where each cluster has its own Manager instance,
// using time.Now() directly can lead to inconsistent behavior:
//
//	┌─────────────────┐     ┌─────────────────┐
//	│  Cluster A      │     │  Cluster B      │
//	│  Manager        │     │  Manager        │
//	│                 │     │                 │
//	│  time.Now() ────┼─────┼── time.Now()    │
//	│       ↓         │     │       ↓         │
//	│  expiredAt check│     │  expiredAt check│
//	└─────────────────┘     └─────────────────┘
//	        │                       │
//	        └───────────┬───────────┘
//	                    ↓
//	            ┌─────────────┐
//	            │  Shared PG  │
//	            └─────────────┘
//
// The problem is that Cluster A and Cluster B may have different local times,
// leading to inconsistent expiration checks and other time-based logic.
//
// # Solution
//
// This package provides a Clock type that syncs with the shared PostgreSQL
// database's clock. All clusters using the same database will have consistent time.
//
//	┌─────────────────┐     ┌─────────────────┐
//	│  Cluster A      │     │  Cluster B      │
//	│  Manager        │     │  Manager        │
//	│                 │     │                 │
//	│  clock.Now() ───┼─────┼── clock.Now()   │
//	│       ↓         │     │       ↓         │
//	│  synchronized!  │     │  synchronized!  │
//	└─────────────────┘     └─────────────────┘
//	        │                       │
//	        └───────────┬───────────┘
//	                    ↓
//	            ┌─────────────┐
//	            │  Shared PG  │
//	            │  (time ref) │
//	            └─────────────┘
//
// # Features
//
//   - Periodic synchronization with configurable interval
//   - RTT compensation for accurate time estimation
//   - Monotonic clock to prevent time going backward
//   - Health metrics (sync count, last error, time since last sync)
//   - Thread-safe concurrent access
//   - Graceful shutdown
//
// # Usage
//
//	// Initialize with database
//	c, err := clock.NewPGX(ctx, pool,
//	    clock.WithSyncInterval(30*time.Second),
//	    clock.WithZapLogger(logger),
//	)
//	if err != nil {
//	    return err
//	}
//	defer c.Close()
//
//	// Use instead of time.Now()
//	now := c.Now()
//	if now.After(expiredAt) {
//	    // handle expiration
//	}
//
//	// Helper methods
//	elapsed := c.Since(startTime)
//	remaining := c.Until(deadline)
//
//	// Health checks
//	if c.LastSyncError() != nil || c.TimeSinceLastSync() > time.Minute {
//	    // clock may be stale
//	}
package clock
