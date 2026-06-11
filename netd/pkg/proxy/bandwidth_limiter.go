package proxy

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

type bandwidthDirection string

const (
	bandwidthEgress  bandwidthDirection = "egress"
	bandwidthIngress bandwidthDirection = "ingress"
)

type bandwidthKey struct {
	sandboxID string
	teamID    string
	direction bandwidthDirection
}

type bandwidthBucket struct {
	tokens float64
	last   time.Time
}

type teamBandwidthLimiter interface {
	reserve(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) (time.Duration, error)
	rate(direction bandwidthDirection) int64
	burstBytes(direction bandwidthDirection) int64
	Close() error
}

type bandwidthLimiter struct {
	mu          sync.Mutex
	egressRate  int64
	ingressRate int64
	burst       int64
	team        teamBandwidthLimiter
	buckets     map[bandwidthKey]*bandwidthBucket
	now         func() time.Time
	sleep       func(time.Duration)
}

func newBandwidthLimiter(ctx context.Context, cfg *config.NetdConfig) (*bandwidthLimiter, error) {
	if cfg == nil {
		return nil, nil
	}
	team, err := newRedisTeamBandwidthLimiter(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if cfg.EgressBandwidthBytesPerSecond <= 0 && cfg.IngressBandwidthBytesPerSecond <= 0 && team == nil {
		return nil, nil
	}
	return &bandwidthLimiter{
		egressRate:  cfg.EgressBandwidthBytesPerSecond,
		ingressRate: cfg.IngressBandwidthBytesPerSecond,
		burst:       cfg.BandwidthBurstBytes,
		team:        team,
		buckets:     make(map[bandwidthKey]*bandwidthBucket),
		now:         time.Now,
		sleep:       time.Sleep,
	}, nil
}

func (l *bandwidthLimiter) limitedWriter(writer io.Writer, compiled *policy.CompiledPolicy, direction bandwidthDirection) io.Writer {
	if l == nil || writer == nil || !l.enabled(direction) {
		return writer
	}
	return &bandwidthLimitedWriter{
		writer:    writer,
		limiter:   l,
		compiled:  compiled,
		direction: direction,
	}
}

func (l *bandwidthLimiter) wait(compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) error {
	if l == nil || bytes <= 0 {
		return nil
	}
	delay, err := l.reserveDelay(context.Background(), compiled, direction, bytes)
	if err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	sleep := l.sleep
	if sleep == nil {
		sleep = time.Sleep
	}
	sleep(delay)
	return nil
}

func (l *bandwidthLimiter) reserveDelay(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) (time.Duration, error) {
	if l == nil || bytes <= 0 {
		return 0, nil
	}
	var delay time.Duration
	rate := l.rate(direction)
	if rate > 0 {
		delay = l.reserve(compiled, direction, bytes, rate)
	}
	if l.team != nil && l.team.rate(direction) > 0 {
		teamDelay, err := l.team.reserve(ctx, compiled, direction, bytes)
		if err != nil {
			return 0, err
		}
		if teamDelay > delay {
			delay = teamDelay
		}
	}
	return delay, nil
}

func (l *bandwidthLimiter) reserve(compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int, rate int64) time.Duration {
	key := bandwidthLimitKey(compiled, direction)
	burst := l.burstBytes(rate)

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.currentTime()
	bucket := l.buckets[key]
	if bucket == nil {
		bucket = &bandwidthBucket{tokens: float64(burst), last: now}
		l.buckets[key] = bucket
	}

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

func (l *bandwidthLimiter) enabled(direction bandwidthDirection) bool {
	if l == nil {
		return false
	}
	if l.rate(direction) > 0 {
		return true
	}
	return l.team != nil && l.team.rate(direction) > 0
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

func (l *bandwidthLimiter) maxChunkBytes(direction bandwidthDirection) int {
	if l == nil {
		return 0
	}
	var max int64
	if rate := l.rate(direction); rate > 0 {
		max = positiveMin(max, l.burstBytes(rate))
	}
	if l.team != nil && l.team.rate(direction) > 0 {
		max = positiveMin(max, l.team.burstBytes(direction))
	}
	if max <= 0 {
		return 0
	}
	maxInt := int64(^uint(0) >> 1)
	if max > maxInt {
		return int(maxInt)
	}
	return int(max)
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
	maxChunk := w.limiter.maxChunkBytes(w.direction)
	if maxChunk <= 0 || maxChunk > len(p) {
		maxChunk = len(p)
	}
	written := 0
	for written < len(p) {
		end := written + maxChunk
		if end > len(p) {
			end = len(p)
		}
		chunk := p[written:end]
		if err := w.limiter.wait(w.compiled, w.direction, len(chunk)); err != nil {
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
