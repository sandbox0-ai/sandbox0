package proxy

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

type bandwidthDirection string

const (
	bandwidthEgress  bandwidthDirection = "egress"
	bandwidthIngress bandwidthDirection = "ingress"

	maxLocalBandwidthBuckets = 10_000
	localBandwidthBucketTTL  = 10 * time.Minute
)

type bandwidthKey struct {
	sandboxID string
	teamID    string
	direction bandwidthDirection
	overflow  bool
}

type bandwidthBucket struct {
	tokens   float64
	last     time.Time
	lastUsed time.Time
}

type teamNetworkByteQuotaLimiter interface {
	waitN(ctx context.Context, teamID string, direction bandwidthDirection, bytes int) error
	maxChunkBytes(ctx context.Context, teamID string, direction bandwidthDirection) (int, error)
	Close() error
}

type bandwidthLimiter struct {
	mu          sync.Mutex
	egressRate  int64
	ingressRate int64
	burst       int64
	team        teamNetworkByteQuotaLimiter
	buckets     map[bandwidthKey]*bandwidthBucket
	now         func() time.Time
}

func newBandwidthLimiter(
	cfg *config.NetdConfig,
	team teamNetworkByteQuotaLimiter,
) *bandwidthLimiter {
	if cfg == nil && team == nil {
		return nil
	}
	if cfg == nil {
		cfg = &config.NetdConfig{}
	}
	if cfg.EgressBandwidthBytesPerSecond <= 0 && cfg.IngressBandwidthBytesPerSecond <= 0 && team == nil {
		return nil
	}
	return &bandwidthLimiter{
		egressRate:  cfg.EgressBandwidthBytesPerSecond,
		ingressRate: cfg.IngressBandwidthBytesPerSecond,
		burst:       cfg.BandwidthBurstBytes,
		team:        team,
		buckets:     make(map[bandwidthKey]*bandwidthBucket),
		now:         time.Now,
	}
}

func (l *bandwidthLimiter) limitedWriter(ctx context.Context, writer io.Writer, compiled *policy.CompiledPolicy, direction bandwidthDirection) io.Writer {
	if l == nil || writer == nil || !l.enabled(direction) {
		return writer
	}
	return &bandwidthLimitedWriter{
		ctx:       ctx,
		writer:    writer,
		limiter:   l,
		compiled:  compiled,
		direction: direction,
	}
}

func (l *bandwidthLimiter) wait(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) error {
	if l == nil || bytes <= 0 {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("bandwidth limiter context is required")
	}
	var localDelay time.Duration
	rate := l.rate(direction)
	if rate > 0 {
		localDelay = l.reserve(compiled, direction, bytes, rate)
	}
	localReadyAt := time.Now().Add(localDelay)
	if l.team != nil {
		if compiled == nil || compiled.TeamID == "" {
			return networkQuotaUnavailable("take network rate tokens", "team_id is required")
		}
		if err := l.team.waitN(ctx, compiled.TeamID, direction, bytes); err != nil {
			return err
		}
	}
	return waitForBandwidthContext(ctx, time.Until(localReadyAt))
}

func waitForBandwidthContext(
	ctx context.Context,
	delay time.Duration,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (l *bandwidthLimiter) reserve(compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int, rate int64) time.Duration {
	key := bandwidthLimitKey(compiled, direction)
	burst := l.burstBytes(rate)

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.currentTime()
	bucket := l.buckets[key]
	if bucket == nil {
		l.pruneLocalBandwidthBucketsLocked(now)
		if len(l.buckets) >= maxLocalBandwidthBuckets-2 {
			// Keep local QoS state bounded even when a tenant cycles through
			// unbounded sandbox IDs. Excess identities share a conservative
			// direction-specific bucket; Team Quota remains independently
			// enforced by the regional Redis bucket.
			key = bandwidthKey{direction: direction, overflow: true}
			bucket = l.buckets[key]
		}
	}
	if bucket == nil {
		bucket = &bandwidthBucket{tokens: float64(burst), last: now, lastUsed: now}
		l.buckets[key] = bucket
	}
	bucket.lastUsed = now

	effectiveNow := now
	if bucket.last.After(now) {
		effectiveNow = bucket.last
	} else {
		elapsed := now.Sub(bucket.last).Seconds()
		bucket.tokens += elapsed * float64(rate)
		if bucket.tokens > float64(burst) {
			bucket.tokens = float64(burst)
		}
	}

	need := float64(bytes)
	if bucket.tokens >= need {
		bucket.tokens -= need
		bucket.last = effectiveNow
		return 0
	}

	deficit := need - bucket.tokens
	wait := time.Duration(deficit/float64(rate)*float64(time.Second) + 0.5)
	bucket.tokens = 0
	bucket.last = effectiveNow.Add(wait)
	return bucket.last.Sub(now)
}

func (l *bandwidthLimiter) pruneLocalBandwidthBucketsLocked(now time.Time) {
	if l == nil || len(l.buckets) == 0 {
		return
	}
	cutoff := now.Add(-localBandwidthBucketTTL)
	for key, bucket := range l.buckets {
		if bucket == nil || (!bucket.lastUsed.After(cutoff) && !bucket.last.After(now)) {
			delete(l.buckets, key)
		}
	}
}

func (l *bandwidthLimiter) enabled(direction bandwidthDirection) bool {
	if l == nil {
		return false
	}
	if l.rate(direction) > 0 {
		return true
	}
	return l.team != nil
}

func (l *bandwidthLimiter) rate(direction bandwidthDirection) int64 {
	if l == nil {
		return 0
	}
	switch direction {
	case bandwidthEgress:
		return l.egressRate
	case bandwidthIngress:
		return l.ingressRate
	default:
		return 0
	}
}

func (l *bandwidthLimiter) burstBytes(rate int64) int64 {
	if l == nil {
		return rate
	}
	if l.burst > 0 {
		return l.burst
	}
	return rate
}

func (l *bandwidthLimiter) maxChunkBytes(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection) (int, error) {
	if l == nil {
		return 0, nil
	}
	var max int64
	if rate := l.rate(direction); rate > 0 {
		max = positiveMin(max, l.burstBytes(rate))
	}
	if l.team != nil {
		if compiled == nil || strings.TrimSpace(compiled.TeamID) == "" {
			return 0, networkQuotaUnavailable("resolve network rate policy", "team_id is required")
		}
		teamMax, err := l.team.maxChunkBytes(ctx, compiled.TeamID, direction)
		if err != nil {
			return 0, err
		}
		max = positiveMin(max, int64(teamMax))
	}
	if max <= 0 {
		return 0, nil
	}
	maxInt := int64(^uint(0) >> 1)
	if max > maxInt {
		return int(maxInt), nil
	}
	return int(max), nil
}

func positiveMin(current, candidate int64) int64 {
	if candidate <= 0 {
		return current
	}
	if current <= 0 || candidate < current {
		return candidate
	}
	return current
}

func (l *bandwidthLimiter) Close() error {
	if l == nil || l.team == nil {
		return nil
	}
	return l.team.Close()
}

func (l *bandwidthLimiter) currentTime() time.Time {
	if l == nil || l.now == nil {
		return time.Now()
	}
	return l.now()
}

func bandwidthLimitKey(compiled *policy.CompiledPolicy, direction bandwidthDirection) bandwidthKey {
	if compiled == nil {
		return bandwidthKey{sandboxID: "unknown", direction: direction}
	}
	return bandwidthKey{
		sandboxID: compiled.SandboxID,
		teamID:    compiled.TeamID,
		direction: direction,
	}
}

type bandwidthLimitedWriter struct {
	ctx       context.Context
	writer    io.Writer
	limiter   *bandwidthLimiter
	compiled  *policy.CompiledPolicy
	direction bandwidthDirection
}

func (w *bandwidthLimitedWriter) Write(p []byte) (int, error) {
	if w == nil || w.writer == nil {
		return 0, fmt.Errorf("bandwidth limited writer is not configured")
	}
	if w.limiter == nil || !w.limiter.enabled(w.direction) {
		return w.writer.Write(p)
	}
	written := 0
	for written < len(p) {
		maxChunk, err := w.limiter.maxChunkBytes(w.ctx, w.compiled, w.direction)
		if err != nil {
			return written, err
		}
		if maxChunk <= 0 || maxChunk > len(p)-written {
			maxChunk = len(p) - written
		}
		end := written + maxChunk
		if end > len(p) {
			end = len(p)
		}
		chunk := p[written:end]
		if err := w.limiter.wait(w.ctx, w.compiled, w.direction, len(chunk)); err != nil {
			return written, err
		}
		n, err := w.writer.Write(chunk)
		written += n
		if err != nil {
			return written, err
		}
		if n != len(chunk) {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}
