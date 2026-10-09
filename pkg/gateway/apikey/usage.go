package apikey

import (
	"context"
	"time"
)

type apiKeyUsage struct {
	count    int64
	lastUsed time.Time
}

// Usage is best-effort display telemetry, separate from authentication and
// billing. Coalesce a burst's updates so they cannot fill the authentication
// pool with transactions waiting to update the same key row.
func (r *Repository) recordUsage(id string) {
	r.usageMu.Lock()
	if r.usagePending == nil {
		r.usagePending = make(map[string]apiKeyUsage)
	}
	usage := r.usagePending[id]
	usage.count++
	usage.lastUsed = time.Now()
	r.usagePending[id] = usage
	start := !r.usageRunning
	r.usageRunning = true
	r.usageMu.Unlock()
	if start {
		go r.flushUsage()
	}
}

func (r *Repository) flushUsage() {
	for {
		timer := time.NewTimer(50 * time.Millisecond)
		<-timer.C
		r.usageMu.Lock()
		pending := r.usagePending
		r.usagePending = nil
		r.usageMu.Unlock()
		for id, usage := range pending {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = r.pool.Exec(ctx, `
				UPDATE api_keys
				SET last_used_at = GREATEST(last_used_at, $3),
					usage_count = usage_count + $2
				WHERE id = $1
			`, id, usage.count, usage.lastUsed)
			cancel()
		}
		r.usageMu.Lock()
		if len(r.usagePending) == 0 {
			r.usageRunning = false
			r.usageMu.Unlock()
			return
		}
		r.usageMu.Unlock()
	}
}
