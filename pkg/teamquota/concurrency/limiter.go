package concurrency

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/distributed"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

const (
	defaultLeaseTTL      = 15 * time.Second
	defaultRenewInterval = 5 * time.Second
	guardRetryAttempts   = 4
	guardRetryDelay      = 10 * time.Millisecond
)

// Resolver supplies effective policy and durable team-admission state.
type Resolver interface {
	EffectivePolicy(context.Context, string, teamquota.Key) (*teamquota.Policy, error)
	teamquota.TeamAdmissionStateResolver
}

// Config defines the regional namespace, Redis endpoint, cache bound, and
// short-lived lease heartbeat behavior.
type Config struct {
	RegionID              string
	RedisURL              string
	RedisKeyPrefix        string
	RedisTimeout          time.Duration
	PolicyCacheTTL        time.Duration
	PolicyCacheMaxEntries int
	LeaseTTL              time.Duration
	RenewInterval         time.Duration
}

// Limiter resolves concurrency policies and owns Redis lease clients.
type Limiter struct {
	resolver            Resolver
	marker              distributed.AtomicAdmissionMarker
	store               leaseStore
	regionID            string
	policies            *distributed.PolicyCache
	leaseTTL            time.Duration
	renewInterval       time.Duration
	renewDeadlineOffset time.Duration
	closeMarker         bool
	closeOnce           sync.Once
	closeErr            error
}

// NewRedisLimiter creates a fail-closed region-shared concurrency limiter.
func NewRedisLimiter(
	ctx context.Context,
	resolver Resolver,
	cfg Config,
) (*Limiter, error) {
	if resolver == nil {
		return nil, fmt.Errorf("team quota concurrency resolver is required")
	}
	marker, err := distributed.NewRedisAdmissionMarker(
		ctx,
		resolver,
		distributed.AdmissionMarkerConfig{
			RegionID:  cfg.RegionID,
			RedisURL:  cfg.RedisURL,
			KeyPrefix: cfg.RedisKeyPrefix,
			Timeout:   cfg.RedisTimeout,
		},
	)
	if err != nil {
		return nil, err
	}
	store, err := NewRedisStore(ctx, StoreConfig{
		RedisURL:       cfg.RedisURL,
		RedisKeyPrefix: cfg.RedisKeyPrefix,
		RedisTimeout:   cfg.RedisTimeout,
	})
	if err != nil {
		_ = marker.Close()
		return nil, err
	}
	limiter, err := newLimiter(resolver, marker, store, cfg)
	if err != nil {
		_ = store.Close()
		_ = marker.Close()
		return nil, err
	}
	limiter.closeMarker = true
	return limiter, nil
}

func newLimiter(
	resolver Resolver,
	marker distributed.AtomicAdmissionMarker,
	store leaseStore,
	cfg Config,
) (*Limiter, error) {
	regionID := strings.TrimSpace(cfg.RegionID)
	if resolver == nil {
		return nil, fmt.Errorf("team quota concurrency resolver is required")
	}
	if marker == nil {
		return nil, fmt.Errorf("team quota concurrency admission marker is required")
	}
	if store == nil {
		return nil, fmt.Errorf("team quota concurrency store is required")
	}
	if regionID == "" {
		return nil, fmt.Errorf("team quota region ID is required")
	}
	policies, err := distributed.NewPolicyCache(
		resolver,
		store,
		teamquota.KindConcurrency,
		cfg.PolicyCacheTTL,
		cfg.PolicyCacheMaxEntries,
	)
	if err != nil {
		return nil, err
	}
	leaseTTL := cfg.LeaseTTL
	if leaseTTL == 0 {
		leaseTTL = defaultLeaseTTL
	}
	renewInterval := cfg.RenewInterval
	if renewInterval == 0 {
		renewInterval = defaultRenewInterval
	}
	if leaseTTL <= 0 || leaseTTL%time.Millisecond != 0 {
		return nil, fmt.Errorf("team quota concurrency lease TTL must use positive whole milliseconds")
	}
	if renewInterval <= 0 || renewInterval%time.Millisecond != 0 {
		return nil, fmt.Errorf("team quota concurrency renew interval must use positive whole milliseconds")
	}
	if renewInterval > (leaseTTL-1)/2 {
		return nil, fmt.Errorf("team quota concurrency renew interval doubled must be less than lease TTL")
	}
	renewDeadlineOffset := conservativeRenewDeadlineOffset(leaseTTL, renewInterval)
	return &Limiter{
		resolver:            resolver,
		marker:              marker,
		store:               store,
		regionID:            regionID,
		policies:            policies,
		leaseTTL:            leaseTTL,
		renewInterval:       renewInterval,
		renewDeadlineOffset: renewDeadlineOffset,
	}, nil
}

// Acquire atomically claims one live slot. The returned lease becomes done
// when its parent context ends, its heartbeat fails, the team is disabled, or
// the Redis member is lost.
func (l *Limiter) Acquire(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
) (*Lease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	teamID = strings.TrimSpace(teamID)
	if l == nil || l.marker == nil || l.store == nil {
		return nil, unavailable("acquire concurrency lease", fmt.Errorf("limiter is not configured"))
	}
	if teamID == "" {
		return nil, unavailable("acquire concurrency lease", fmt.Errorf("team_id is required"))
	}
	leaseID := uuid.NewString()
	redisKey := l.redisKey(teamID, key)
	var (
		admissionKey   string
		policy         teamquota.Policy
		leaseValidFrom time.Time
		lastErr        error
	)
	for attempt := 0; attempt < guardRetryAttempts; attempt++ {
		var err error
		admissionKey, err = l.marker.RedisKey(teamID)
		if err != nil {
			return nil, unavailable("resolve atomic team admission marker", err)
		}
		resolved, err := l.policies.Effective(ctx, teamID, key)
		if err != nil {
			return nil, err
		}
		policy = resolved.Policy
		mutationStartedAt := time.Now()
		decision, err := l.store.Acquire(
			ctx,
			redisKey,
			leaseID,
			admissionKey,
			policy.Limit,
			l.leaseTTL,
			resolved.Version,
		)
		if err == nil {
			if !decision.allowed {
				return nil, &teamquota.ConcurrencyExceededError{
					TeamID: teamID,
					Key:    key,
					Limit:  policy.Limit,
					Used:   decision.used,
				}
			}
			// Redis applies the expiry during this call. Anchoring the local
			// fail-closed deadline at call start cannot outlive that expiry,
			// even when the successful response is delayed.
			leaseValidFrom = mutationStartedAt
			lastErr = nil
			break
		}
		switch {
		case errors.Is(err, errAdmissionMissing):
			if recoverErr := l.marker.Recover(ctx, teamID); recoverErr != nil {
				return nil, unavailable("recover atomic team admission marker", recoverErr)
			}
			lastErr = err
			continue
		case errors.Is(err, errAdmissionDisabled):
			l.policies.InvalidateTeam(teamID)
			return nil, unavailable(
				"check atomic team admission marker",
				&teamquota.TeamAdmissionDisabledError{TeamID: teamID},
			)
		case errors.Is(err, errAdmissionCorrupt):
			return nil, unavailable("check atomic team admission marker", err)
		}
		if !guardRetryable(err) {
			return nil, unavailable("acquire concurrency lease", err)
		}
		lastErr = err
		l.policies.Invalidate(teamID, key)
		if attempt+1 >= guardRetryAttempts {
			break
		}
		if !waitForGuard(ctx) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			break
		}
	}
	if lastErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, unavailable("acquire concurrency lease", fmt.Errorf("policy guard did not stabilize: %w", lastErr))
	}
	lease := &Lease{
		teamID:              teamID,
		key:                 key,
		leaseID:             leaseID,
		redisKey:            redisKey,
		parent:              ctx,
		marker:              l.marker,
		store:               l.store,
		policies:            l.policies,
		leaseTTL:            l.leaseTTL,
		renewInterval:       l.renewInterval,
		renewDeadlineOffset: l.renewDeadlineOffset,
		leaseValidFrom:      leaseValidFrom,
		stop:                make(chan struct{}),
		done:                make(chan struct{}),
		finished:            make(chan struct{}),
	}
	go lease.run()
	return lease, nil
}

// Usage purges expired leases and returns the exact current live count.
func (l *Limiter) Usage(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
) (int64, error) {
	teamID = strings.TrimSpace(teamID)
	if err := l.ensureAdmissionEnabled(ctx, teamID); err != nil {
		return 0, err
	}
	if _, err := l.policies.Effective(ctx, teamID, key); err != nil {
		return 0, err
	}
	used, err := l.store.Usage(ctx, l.redisKey(teamID, key), l.leaseTTL)
	if err != nil {
		return 0, unavailable("read concurrency usage", err)
	}
	return used, nil
}

// Invalidate evicts one locally cached effective policy.
func (l *Limiter) Invalidate(teamID string, key teamquota.Key) {
	if l == nil {
		return
	}
	l.policies.Invalidate(teamID, key)
}

// Close releases the Redis clients. Existing leases fail closed on their next
// heartbeat.
func (l *Limiter) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		var errs []error
		if l.store != nil {
			errs = append(errs, l.store.Close())
		}
		if l.closeMarker && l.marker != nil {
			errs = append(errs, l.marker.Close())
		}
		l.closeErr = errors.Join(errs...)
	})
	return l.closeErr
}

func (l *Limiter) ensureAdmissionEnabled(ctx context.Context, teamID string) error {
	if l == nil || l.marker == nil || l.store == nil {
		return unavailable("check concurrency admission", fmt.Errorf("limiter is not configured"))
	}
	if teamID == "" {
		return unavailable("check concurrency admission", fmt.Errorf("team_id is required"))
	}
	disabled, err := l.marker.Disabled(ctx, teamID)
	if err != nil {
		return unavailable("check concurrency admission", err)
	}
	if disabled {
		return unavailable(
			"check concurrency admission",
			&teamquota.TeamAdmissionDisabledError{TeamID: teamID},
		)
	}
	return nil
}

func (l *Limiter) redisKey(teamID string, key teamquota.Key) string {
	return fmt.Sprintf(
		"team-quota:v1:%d:%s:%d:%s:%s",
		len(l.regionID),
		l.regionID,
		len(teamID),
		teamID,
		key,
	)
}

func unavailable(operation string, err error) error {
	return &teamquota.UnavailableError{Operation: operation, Err: err}
}

// conservativeRenewDeadlineOffset returns the offset from the start of the last
// successful Redis lease mutation to the fail-closed deadline. The heartbeat
// starts after renewInterval. The whole-cycle budget is at most one more
// interval and at most half of the remaining TTL, leaving at least an equal
// safety window for scheduling and network delay.
func conservativeRenewDeadlineOffset(leaseTTL time.Duration, renewInterval time.Duration) time.Duration {
	cycleBudget := (leaseTTL - renewInterval) / 2
	if cycleBudget > renewInterval {
		cycleBudget = renewInterval
	}
	return renewInterval + cycleBudget
}

// Lease is one exact live-concurrency member.
type Lease struct {
	teamID              string
	key                 teamquota.Key
	leaseID             string
	redisKey            string
	parent              context.Context
	marker              distributed.AtomicAdmissionMarker
	store               leaseStore
	policies            *distributed.PolicyCache
	leaseTTL            time.Duration
	renewInterval       time.Duration
	renewDeadlineOffset time.Duration
	leaseValidFrom      time.Time

	stop chan struct{}
	// done fails the caller closed before best-effort Redis cleanup; finished
	// lets explicit Release retain its wait-for-cleanup contract.
	done     chan struct{}
	finished chan struct{}
	stopOnce sync.Once

	mu         sync.RWMutex
	err        error
	releaseErr error
}

// Done closes when the lease is released or can no longer be renewed.
func (l *Lease) Done() <-chan struct{} {
	if l == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return l.done
}

// Err reports why an active lease was lost. Explicit Release returns nil.
func (l *Lease) Err() error {
	if l == nil {
		return unavailable("use concurrency lease", fmt.Errorf("lease is nil"))
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.err
}

// Release removes the Redis member. It is idempotent and waits for the
// heartbeat goroutine unless ctx expires.
func (l *Lease) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.stopOnce.Do(func() { close(l.stop) })
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-l.finished:
		l.mu.RLock()
		defer l.mu.RUnlock()
		return l.releaseErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Lease) run() {
	ticker := time.NewTicker(l.renewInterval)
	renewDeadline := l.leaseValidFrom.Add(l.renewDeadlineOffset)
	deadlineTimer := time.NewTimer(time.Until(renewDeadline))
	defer ticker.Stop()
	defer deadlineTimer.Stop()
	defer close(l.finished)
	defer l.release()
	defer close(l.done)
	for {
		select {
		case <-l.stop:
			return
		case <-l.parent.Done():
			l.setErr(l.parent.Err())
			return
		case <-deadlineTimer.C:
			l.setErr(renewDeadlineExceeded(context.DeadlineExceeded))
			return
		case <-ticker.C:
			renewedFrom, err, stopped := l.runRenewalCycle(renewDeadline)
			if stopped {
				return
			}
			if err != nil {
				l.setErr(err)
				return
			}
			renewDeadline = renewedFrom.Add(l.renewDeadlineOffset)
			if !resetDeadlineTimer(deadlineTimer, renewDeadline) {
				l.setErr(renewDeadlineExceeded(context.DeadlineExceeded))
				return
			}
		}
	}
}

type renewalResult struct {
	renewedFrom time.Time
	err         error
}

func (l *Lease) runRenewalCycle(deadline time.Time) (time.Time, error, bool) {
	if !time.Now().Before(deadline) {
		return time.Time{}, renewDeadlineExceeded(context.DeadlineExceeded), false
	}
	ctx, cancel := context.WithDeadline(l.parent, deadline)
	defer cancel()
	result := make(chan renewalResult, 1)
	// Supervise the whole cycle so Lease.Done is bounded even if an
	// implementation accidentally ignores the supplied deadline context.
	go func() {
		renewedFrom, err := l.renew(ctx)
		result <- renewalResult{renewedFrom: renewedFrom, err: err}
	}()

	select {
	case <-l.stop:
		return time.Time{}, nil, true
	case <-l.parent.Done():
		return time.Time{}, l.parent.Err(), false
	case <-ctx.Done():
		if err := l.parent.Err(); err != nil {
			return time.Time{}, err, false
		}
		return time.Time{}, renewDeadlineExceeded(ctx.Err()), false
	case completed := <-result:
		if err := l.parent.Err(); err != nil {
			return time.Time{}, err, false
		}
		if !time.Now().Before(deadline) {
			return time.Time{}, renewDeadlineExceeded(context.DeadlineExceeded), false
		}
		return completed.renewedFrom, completed.err, false
	}
}

func (l *Lease) renew(ctx context.Context) (time.Time, error) {
	for {
		admissionKey, err := l.marker.RedisKey(l.teamID)
		if err != nil {
			return time.Time{}, unavailable("resolve atomic team admission marker", err)
		}
		resolved, err := l.policies.Effective(ctx, l.teamID, l.key)
		if err != nil {
			if guardRetryable(err) && waitForGuard(ctx) {
				l.policies.Invalidate(l.teamID, l.key)
				continue
			}
			return time.Time{}, err
		}
		policy := resolved.Policy
		mutationStartedAt := time.Now()
		decision, err := l.store.Renew(
			ctx,
			l.redisKey,
			l.leaseID,
			admissionKey,
			policy.Limit,
			l.leaseTTL,
			resolved.Version,
		)
		if err != nil {
			switch {
			case errors.Is(err, errAdmissionMissing):
				if recoverErr := l.marker.Recover(ctx, l.teamID); recoverErr != nil {
					return time.Time{}, unavailable("recover atomic team admission marker", recoverErr)
				}
				continue
			case errors.Is(err, errAdmissionDisabled):
				l.policies.InvalidateTeam(l.teamID)
				return time.Time{}, unavailable(
					"check atomic team admission marker",
					&teamquota.TeamAdmissionDisabledError{TeamID: l.teamID},
				)
			case errors.Is(err, errAdmissionCorrupt):
				return time.Time{}, unavailable("check atomic team admission marker", err)
			}
			if guardRetryable(err) && waitForGuard(ctx) {
				l.policies.Invalidate(l.teamID, l.key)
				continue
			}
			return time.Time{}, unavailable("renew concurrency lease", err)
		}
		switch decision {
		case renewed:
			return mutationStartedAt, nil
		case renewOverLimit:
			used, usageErr := l.store.Usage(ctx, l.redisKey, l.leaseTTL)
			if usageErr != nil {
				return time.Time{}, unavailable("read concurrency usage after policy lowering", usageErr)
			}
			return time.Time{}, &teamquota.ConcurrencyExceededError{
				TeamID: l.teamID,
				Key:    l.key,
				Limit:  policy.Limit,
				Used:   used,
			}
		case renewLost:
			return time.Time{}, unavailable("renew concurrency lease", fmt.Errorf("lease expired or was lost"))
		default:
			return time.Time{}, unavailable("renew concurrency lease", fmt.Errorf("unknown renewal decision"))
		}
	}
}

func renewDeadlineExceeded(err error) error {
	return unavailable("complete concurrency renewal before fail-closed deadline", err)
}

func resetDeadlineTimer(timer *time.Timer, deadline time.Time) bool {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(remaining)
	return true
}

func (l *Lease) release() {
	timeout := l.leaseTTL
	if timeout > time.Second {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := l.store.Release(ctx, l.redisKey, l.leaseID)
	l.mu.Lock()
	l.releaseErr = err
	l.mu.Unlock()
}

func (l *Lease) setErr(err error) {
	l.mu.Lock()
	if l.err == nil {
		l.err = err
	}
	l.mu.Unlock()
}

func guardRetryable(err error) bool {
	return errors.Is(err, guard.ErrMissing) ||
		errors.Is(err, guard.ErrPending) ||
		errors.Is(err, guard.ErrStale)
}

func waitForGuard(ctx context.Context) bool {
	timer := time.NewTimer(guardRetryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
