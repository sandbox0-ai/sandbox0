// Package retryqueue contains small mechanics shared by manager durable retry
// queues. Business schemas and terminal-state policies remain owned by each
// feature.
package retryqueue

import "time"

// ExponentialBackoff doubles base for each attempt and saturates at max.
func ExponentialBackoff(attempt int, base, max time.Duration) time.Duration {
	if max < base {
		max = base
	}
	if attempt <= 1 {
		return base
	}
	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

// DurationSeconds rounds a PostgreSQL interval argument to at least one
// second.
func DurationSeconds(duration time.Duration) int {
	if duration <= 0 {
		return 1
	}
	seconds := int(duration.Round(time.Second) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}
