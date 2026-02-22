package controller

import (
	"sync"
	"time"
)

// scaleRateLimiter prevents over-scaling during concurrent cold claims.
// It ensures:
// 1. Only one scale operation can run at a time (inProgress)
// 2. After a scale completes, wait minInterval before the next scale
// 3. The interval is measured from completion time, not start time
type scaleRateLimiter struct {
	mu             sync.Mutex
	lastCompleteAt map[string]time.Time // When last scale completed
	inProgress     map[string]bool      // Tracks ongoing scale operations
	minInterval    time.Duration
}

func newScaleRateLimiter(minInterval time.Duration) *scaleRateLimiter {
	return &scaleRateLimiter{
		lastCompleteAt: make(map[string]time.Time),
		inProgress:     make(map[string]bool),
		minInterval:    minInterval,
	}
}

// TryAcquire attempts to acquire the rate limiter for the given key.
// Returns true if this call should proceed with scaling, false if rate limited.
// Conditions for success:
// 1. No scale operation is currently in progress for this key
// 2. Enough time has passed since the last scale COMPLETED (minInterval)
// This is an atomic check-and-record operation to prevent race conditions.
func (r *scaleRateLimiter) TryAcquire(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if there's an ongoing scale operation
	if r.inProgress[key] {
		return false // Previous scale still in progress
	}

	// Check time interval since last completion
	lastComplete, exists := r.lastCompleteAt[key]
	if exists && time.Since(lastComplete) < r.minInterval {
		return false // Rate limited by time since last completion
	}

	// Mark as in progress
	r.inProgress[key] = true
	return true
}

// Complete marks the scale operation as done for the given key.
// This must be called after the scale operation finishes (success or failure).
// The minInterval countdown starts from this completion time.
func (r *scaleRateLimiter) Complete(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inProgress, key)
	r.lastCompleteAt[key] = time.Now()
}
