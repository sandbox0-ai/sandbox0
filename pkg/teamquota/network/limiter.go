// Package network applies region-shared Team Quota byte-rate policies at
// external network boundaries.
package network

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	teamquotadistributed "github.com/sandbox0-ai/sandbox0/pkg/teamquota/distributed"
	teamquotarate "github.com/sandbox0-ai/sandbox0/pkg/teamquota/rate"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

const (
	defaultGlobalWaiters = 4096
	defaultTeamWaiters   = 256
	defaultMaxWait       = 5 * time.Second
)

var (
	ErrWaiterSaturated = errors.New("network quota waiter admission is saturated")
	ErrWaitTimeout     = errors.New("network quota wait exceeded its maximum duration")
)

// Config configures a region-shared network byte limiter.
type Config struct {
	RegionID         string
	RedisURL         string
	RedisKeyPrefix   string
	RedisTimeout     time.Duration
	PolicyCacheTTL   time.Duration
	MaxGlobalWaiters int
	MaxTeamWaiters   int
	MaxWait          time.Duration
	Now              func() time.Time
	Wait             func(context.Context, time.Duration) error
}

// Limiter enforces network ingress and egress byte-rate policies.
type Limiter struct {
	rateLimiter *teamquotarate.Limiter
	marker      teamquotadistributed.AtomicAdmissionMarker
	bucket      tokenbucket.GuardedBucket
	waiters     *waiterAdmission
	maxWait     time.Duration
	now         func() time.Time
	wait        func(context.Context, time.Duration) error

	closeOnce sync.Once
	closeErr  error
}

// New creates a network byte limiter from injected distributed primitives.
// The limiter owns marker and bucket and closes both.
func New(
	resolver teamquotarate.PolicyResolver,
	marker teamquotadistributed.AtomicAdmissionMarker,
	bucket tokenbucket.GuardedBucket,
	cfg Config,
) (*Limiter, error) {
	waiters, maxWait, now, wait, err := newWaiterAdmission(cfg)
	if err != nil {
		return nil, err
	}
	rateLimiter, err := teamquotarate.NewLimiter(
		resolver,
		marker,
		bucket,
		teamquotarate.Config{
			RegionID:       cfg.RegionID,
			PolicyCacheTTL: cfg.PolicyCacheTTL,
			Now:            now,
		},
	)
	if err != nil {
		return nil, err
	}
	return &Limiter{
		rateLimiter: rateLimiter,
		marker:      marker,
		bucket:      bucket,
		waiters:     waiters,
		maxWait:     maxWait,
		now:         now,
		wait:        wait,
	}, nil
}

// NewRedis creates a fail-closed network byte limiter backed by the region's
// PostgreSQL policy resolver and Redis.
func NewRedis(
	ctx context.Context,
	resolver interface {
		teamquotarate.PolicyResolver
		teamquota.TeamAdmissionStateResolver
	},
	cfg Config,
) (*Limiter, error) {
	if ctx == nil {
		return nil, fmt.Errorf("network team quota context is required")
	}
	if resolver == nil {
		return nil, fmt.Errorf("network team quota policy resolver is required")
	}
	if strings.TrimSpace(cfg.RegionID) == "" {
		return nil, fmt.Errorf("network team quota region ID is required")
	}
	if strings.TrimSpace(cfg.RedisURL) == "" {
		return nil, fmt.Errorf("network team quota Redis URL is required")
	}
	cfg.RedisKeyPrefix = teamquota.NormalizeTeamQuotaRedisKeyPrefix(cfg.RedisKeyPrefix)
	marker, err := teamquotadistributed.NewRedisAdmissionMarker(
		ctx,
		resolver,
		teamquotadistributed.AdmissionMarkerConfig{
			RegionID:  cfg.RegionID,
			RedisURL:  cfg.RedisURL,
			KeyPrefix: cfg.RedisKeyPrefix,
			Timeout:   cfg.RedisTimeout,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create network team quota admission marker: %w", err)
	}
	bucket, err := tokenbucket.NewRedisBucket(ctx, tokenbucket.RedisConfig{
		URL:       cfg.RedisURL,
		KeyPrefix: cfg.RedisKeyPrefix,
		Timeout:   cfg.RedisTimeout,
	})
	if err != nil {
		_ = marker.Close()
		return nil, fmt.Errorf("create network team quota token bucket: %w", err)
	}
	limiter, err := New(
		resolver,
		marker,
		bucket,
		cfg,
	)
	if err != nil {
		_ = bucket.Close()
		_ = marker.Close()
		return nil, err
	}
	return limiter, nil
}

// WaitN waits until bytes tokens are admitted for teamID and key. Costs larger
// than the policy burst are split into exact, independently admitted chunks.
func (l *Limiter) WaitN(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
	bytes int,
) error {
	if bytes <= 0 {
		return nil
	}
	if ctx == nil {
		return unavailable("take network rate tokens", "context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if l == nil || l.rateLimiter == nil {
		return unavailable(
			"take network rate tokens",
			"network team quota is not configured",
		)
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return unavailable("take network rate tokens", "team_id is required")
	}
	if err := validateNetworkKey(key); err != nil {
		return err
	}

	remaining := int64(bytes)
	var (
		waiting      bool
		waitDeadline time.Time
		releaseWait  func()
	)
	defer func() {
		if releaseWait != nil {
			releaseWait()
		}
	}()
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		policy, err := l.rateLimiter.EffectivePolicy(ctx, teamID, key)
		if err != nil {
			return err
		}
		cost := remaining
		if cost > policy.Burst {
			cost = policy.Burst
		}
		if cost <= 0 {
			return unavailable(
				fmt.Sprintf("take %s rate tokens", key),
				fmt.Sprintf("invalid policy burst %d", policy.Burst),
			)
		}
		for {
			decision, err := l.rateLimiter.Take(ctx, teamID, key, cost)
			if err != nil {
				if errors.Is(err, tokenbucket.ErrCostExceedsBurst) {
					// A policy commit may lower burst between the outer
					// policy lookup and the atomic Take. Re-resolve and
					// choose a valid exact chunk.
					break
				}
				return err
			}
			if decision.Allowed {
				remaining -= cost
				break
			}
			if decision.RetryAfter <= 0 {
				return unavailable(
					fmt.Sprintf("take %s rate tokens", key),
					"token bucket denied without retry interval",
				)
			}
			if !waiting {
				release, err := l.waiters.acquire(teamID)
				if err != nil {
					return unavailableError(
						fmt.Sprintf("wait for %s rate tokens", key),
						err,
					)
				}
				releaseWait = release
				waiting = true
				waitDeadline = l.now().Add(l.maxWait)
			}
			if err := l.waitForRetry(ctx, decision.RetryAfter, waitDeadline); err != nil {
				return err
			}
		}
	}
	return nil
}

// Take immediately admits one network operation. Byte keys use WaitN;
// this method is intentionally restricted to the discrete
// network_operations key and never joins the waiter pool.
func (l *Limiter) Take(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
	cost int64,
) (tokenbucket.Decision, error) {
	if ctx == nil {
		return tokenbucket.Decision{}, unavailable(
			"take network operation rate tokens",
			"context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return tokenbucket.Decision{}, err
	}
	if l == nil || l.rateLimiter == nil {
		return tokenbucket.Decision{}, unavailable(
			"take network operation rate tokens",
			"network team quota is not configured",
		)
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return tokenbucket.Decision{}, unavailable(
			"take network operation rate tokens",
			"team_id is required",
		)
	}
	if key != teamquota.KeyNetworkOperations {
		return tokenbucket.Decision{}, fmt.Errorf(
			"team quota key %q is not the network operations key",
			key,
		)
	}
	return l.rateLimiter.Take(ctx, teamID, key, cost)
}

// MaxChunkBytes returns the largest exact single token-bucket cost for a
// network policy.
func (l *Limiter) MaxChunkBytes(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
) (int, error) {
	if ctx == nil {
		return 0, unavailable("resolve network rate policy", "context is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if l == nil || l.rateLimiter == nil {
		return 0, unavailable(
			"resolve network rate policy",
			"network team quota is not configured",
		)
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return 0, unavailable("resolve network rate policy", "team_id is required")
	}
	if err := validateNetworkKey(key); err != nil {
		return 0, err
	}
	policy, err := l.rateLimiter.EffectivePolicy(ctx, teamID, key)
	if err != nil {
		return 0, err
	}
	if policy.Burst <= 0 {
		return 0, unavailable(
			fmt.Sprintf("resolve %s rate policy", key),
			fmt.Sprintf("invalid burst %d", policy.Burst),
		)
	}
	maxInt := int64(^uint(0) >> 1)
	if policy.Burst > maxInt {
		return int(maxInt), nil
	}
	return int(policy.Burst), nil
}

// Reader returns a fail-closed reader that admits bytes after reading them and
// before exposing them to its caller.
func (l *Limiter) Reader(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
	reader io.Reader,
) io.Reader {
	return &limitedReader{
		ctx:     ctx,
		teamID:  teamID,
		key:     key,
		reader:  reader,
		limiter: l,
	}
}

// Writer returns a fail-closed writer that admits each byte chunk before
// writing it to the external boundary.
func (l *Limiter) Writer(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
	writer io.Writer,
) io.Writer {
	return &limitedWriter{
		ctx:     ctx,
		teamID:  teamID,
		key:     key,
		writer:  writer,
		limiter: l,
	}
}

// Close releases the distributed admission marker and token bucket clients.
func (l *Limiter) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		var errs []error
		if l.marker != nil {
			errs = append(errs, l.marker.Close())
		}
		if l.bucket != nil {
			errs = append(errs, l.bucket.Close())
		}
		l.closeErr = errors.Join(errs...)
	})
	return l.closeErr
}

func validateNetworkKey(key teamquota.Key) error {
	switch key {
	case teamquota.KeyNetworkIngressBytes, teamquota.KeyNetworkEgressBytes:
		return nil
	default:
		return fmt.Errorf("team quota key %q is not a network byte key", key)
	}
}

func waitForContext(ctx context.Context, delay time.Duration) error {
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

type waiterAdmission struct {
	mu          sync.Mutex
	global      int
	teams       map[string]int
	globalLimit int
	teamLimit   int
}

func newWaiterAdmission(
	cfg Config,
) (*waiterAdmission, time.Duration, func() time.Time, func(context.Context, time.Duration) error, error) {
	globalLimit := cfg.MaxGlobalWaiters
	if globalLimit == 0 {
		globalLimit = defaultGlobalWaiters
	}
	teamLimit := cfg.MaxTeamWaiters
	if teamLimit == 0 {
		teamLimit = defaultTeamWaiters
	}
	maxWait := cfg.MaxWait
	if maxWait == 0 {
		maxWait = defaultMaxWait
	}
	if globalLimit < 0 {
		return nil, 0, nil, nil, fmt.Errorf("network quota global waiter limit must be non-negative")
	}
	if teamLimit < 0 {
		return nil, 0, nil, nil, fmt.Errorf("network quota team waiter limit must be non-negative")
	}
	if maxWait < 0 {
		return nil, 0, nil, nil, fmt.Errorf("network quota maximum wait must be non-negative")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	wait := cfg.Wait
	if wait == nil {
		wait = waitForContext
	}
	return &waiterAdmission{
		teams:       make(map[string]int),
		globalLimit: globalLimit,
		teamLimit:   teamLimit,
	}, maxWait, now, wait, nil
}

func (a *waiterAdmission) acquire(teamID string) (func(), error) {
	if a == nil {
		return nil, ErrWaiterSaturated
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.global >= a.globalLimit || a.teams[teamID] >= a.teamLimit {
		return nil, ErrWaiterSaturated
	}
	a.global++
	a.teams[teamID]++
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			a.global--
			a.teams[teamID]--
			if a.teams[teamID] == 0 {
				delete(a.teams, teamID)
			}
			a.mu.Unlock()
		})
	}, nil
}

func (l *Limiter) waitForRetry(
	ctx context.Context,
	retryAfter time.Duration,
	deadline time.Time,
) error {
	remaining := deadline.Sub(l.now())
	if remaining <= 0 {
		return unavailableError("wait for network rate tokens", ErrWaitTimeout)
	}
	delay := retryAfter
	timedOut := false
	if delay >= remaining {
		delay = remaining
		timedOut = true
	}
	if err := l.wait(ctx, delay); err != nil {
		return err
	}
	if timedOut || !l.now().Before(deadline) {
		return unavailableError("wait for network rate tokens", ErrWaitTimeout)
	}
	return nil
}

func unavailable(operation, message string) error {
	return &teamquota.UnavailableError{
		Operation: operation,
		Err:       fmt.Errorf("%s", message),
	}
}

func unavailableError(operation string, err error) error {
	return &teamquota.UnavailableError{Operation: operation, Err: err}
}

type limitedReader struct {
	ctx     context.Context
	teamID  string
	key     teamquota.Key
	reader  io.Reader
	limiter *Limiter
}

func (r *limitedReader) Read(p []byte) (int, error) {
	if r == nil || r.reader == nil {
		return 0, fmt.Errorf("network quota reader is not configured")
	}
	if len(p) == 0 {
		return r.reader.Read(p)
	}
	maxChunk, err := r.limiter.MaxChunkBytes(r.ctx, r.teamID, r.key)
	if err != nil {
		return 0, err
	}
	if maxChunk < len(p) {
		p = p[:maxChunk]
	}
	n, readErr := r.reader.Read(p)
	if n > 0 {
		if err := r.limiter.WaitN(r.ctx, r.teamID, r.key, n); err != nil {
			return 0, err
		}
	}
	return n, readErr
}

type limitedWriter struct {
	ctx     context.Context
	teamID  string
	key     teamquota.Key
	writer  io.Writer
	limiter *Limiter
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w == nil || w.writer == nil {
		return 0, fmt.Errorf("network quota writer is not configured")
	}
	written := 0
	for written < len(p) {
		maxChunk, err := w.limiter.MaxChunkBytes(w.ctx, w.teamID, w.key)
		if err != nil {
			return written, err
		}
		if maxChunk <= 0 || maxChunk > len(p)-written {
			maxChunk = len(p) - written
		}
		chunk := p[written : written+maxChunk]
		if err := w.limiter.WaitN(w.ctx, w.teamID, w.key, len(chunk)); err != nil {
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
