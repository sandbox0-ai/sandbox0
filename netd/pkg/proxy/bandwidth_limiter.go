package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

type bandwidthDirection string

const (
	bandwidthEgress  bandwidthDirection = "egress"
	bandwidthIngress bandwidthDirection = "ingress"
)

var errBandwidthWaitExceeded = errors.New("bandwidth quota has no tokens available for datagram")

type teamBandwidthLimiter interface {
	reserve(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) (time.Duration, error)
	tryTake(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) (bool, error)
	burstBytes(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection) (int64, error)
	Close() error
}

type bandwidthLimiter struct {
	egressRate  int64
	ingressRate int64
	burst       int64
	local       tokenbucket.Bucket
	team        teamBandwidthLimiter
	sleep       func(time.Duration)
}

func newBandwidthLimiter(_ context.Context, cfg *config.NetdConfig) (*bandwidthLimiter, error) {
	if cfg == nil {
		return nil, nil
	}
	if cfg.EgressBandwidthBytesPerSecond <= 0 && cfg.IngressBandwidthBytesPerSecond <= 0 {
		return nil, nil
	}
	return &bandwidthLimiter{
		egressRate:  cfg.EgressBandwidthBytesPerSecond,
		ingressRate: cfg.IngressBandwidthBytesPerSecond,
		burst:       cfg.BandwidthBurstBytes,
		local:       tokenbucket.NewMemoryBucket(tokenbucket.MemoryConfig{}),
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
	return l.waitContext(context.Background(), compiled, direction, bytes)
}

func (l *bandwidthLimiter) waitDatagram(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) error {
	if l == nil || bytes <= 0 {
		return nil
	}
	rate := l.rate(direction)
	if rate > 0 && l.local != nil {
		decision, err := l.local.TryTakeN(ctx,
			localBandwidthKey(compiled, direction),
			tokenbucket.Limit{
				Tokens:   rate,
				Interval: time.Second,
				Burst:    l.burstBytes(rate),
			},
			int64(bytes),
		)
		if err != nil {
			return err
		}
		if !decision.Allowed {
			return errBandwidthWaitExceeded
		}
	}
	if l.team != nil {
		allowed, err := l.team.tryTake(ctx, compiled, direction, bytes)
		if err != nil {
			return err
		}
		if !allowed {
			return errBandwidthWaitExceeded
		}
	}
	return nil
}

func (l *bandwidthLimiter) waitContext(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) error {
	if l == nil || bytes <= 0 {
		return nil
	}
	delay, err := l.reserveDelay(ctx, compiled, direction, bytes)
	if err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	if l.sleep != nil {
		l.sleep(delay)
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

func (l *bandwidthLimiter) reserveDelay(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) (time.Duration, error) {
	if l == nil || bytes <= 0 {
		return 0, nil
	}
	var delay time.Duration
	rate := l.rate(direction)
	if rate > 0 && l.local != nil {
		reservation, err := l.local.ReserveN(ctx,
			localBandwidthKey(compiled, direction),
			tokenbucket.Limit{
				Tokens:   rate,
				Interval: time.Second,
				Burst:    l.burstBytes(rate),
			},
			int64(bytes),
		)
		if err != nil {
			return 0, err
		}
		delay = reservation.Delay
	}
	if l.team != nil {
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

func (l *bandwidthLimiter) enabled(direction bandwidthDirection) bool {
	if l == nil {
		return false
	}
	return l.rate(direction) > 0 || l.team != nil
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
	var maximum int64
	if rate := l.rate(direction); rate > 0 {
		maximum = positiveMin(maximum, l.burstBytes(rate))
	}
	if l.team != nil {
		teamBurst, err := l.team.burstBytes(ctx, compiled, direction)
		if err != nil {
			return 0, err
		}
		maximum = positiveMin(maximum, teamBurst)
	}
	if maximum <= 0 {
		return 0, nil
	}
	maxInt := int64(^uint(0) >> 1)
	if maximum > maxInt {
		return int(maxInt), nil
	}
	return int(maximum), nil
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
	if l == nil {
		return nil
	}
	var err error
	if l.local != nil {
		err = l.local.Close()
	}
	if l.team != nil {
		if closeErr := l.team.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

func localBandwidthKey(compiled *policy.CompiledPolicy, direction bandwidthDirection) string {
	if compiled == nil {
		return "sandbox:unknown:direction:" + string(direction)
	}
	return "sandbox:" + compiled.SandboxID + ":team:" + compiled.TeamID + ":direction:" + string(direction)
}

type bandwidthLimitedWriter struct {
	writer    io.Writer
	limiter   *bandwidthLimiter
	compiled  *policy.CompiledPolicy
	direction bandwidthDirection
}

func (w *bandwidthLimitedWriter) Write(payload []byte) (int, error) {
	if w == nil || w.writer == nil {
		return 0, fmt.Errorf("bandwidth limited writer is not configured")
	}
	if w.limiter == nil || !w.limiter.enabled(w.direction) {
		return w.writer.Write(payload)
	}
	maxChunk, err := w.limiter.maxChunkBytes(context.Background(), w.compiled, w.direction)
	if err != nil {
		return 0, err
	}
	if maxChunk <= 0 || maxChunk > len(payload) {
		maxChunk = len(payload)
	}
	written := 0
	for written < len(payload) {
		end := written + maxChunk
		if end > len(payload) {
			end = len(payload)
		}
		chunk := payload[written:end]
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
