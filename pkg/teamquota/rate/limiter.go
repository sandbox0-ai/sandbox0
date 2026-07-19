// Package rate resolves Team Quota rate policies and applies them through a
// region-shared token bucket with strictly bounded process-local credits.
package rate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/distributed"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

const (
	guardRetryAttempts = 4
	guardRetryDelay    = 10 * time.Millisecond
)

// PolicyResolver returns the effective region policy for a team.
type PolicyResolver interface {
	EffectivePolicy(ctx context.Context, teamID string, key teamquota.Key) (*teamquota.Policy, error)
}

// Config defines the regional namespace, policy cache, and bounded local
// credit behavior.
type Config struct {
	RegionID              string
	PolicyCacheTTL        time.Duration
	PolicyCacheMaxEntries int
	LocalCreditTTL        time.Duration
	Now                   func() time.Time
	WaitUntil             func(context.Context, time.Time) error
	PolicyGuard           guard.Reader
}

// Limiter applies effective Team Quota rate policies.
type Limiter struct {
	resolver       PolicyResolver
	marker         distributed.AtomicAdmissionMarker
	bucket         tokenbucket.GuardedBucket
	regionID       string
	policies       *distributed.PolicyCache
	credits        *localCreditCache
	localCreditTTL time.Duration
	now            func() time.Time
	waitUntil      func(context.Context, time.Time) error
}

// NewLimiter creates a fail-closed rate limiter. Its marker and bucket must
// support the atomic policy-guard plus admission-marker Redis mutation.
func NewLimiter(
	resolver PolicyResolver,
	marker distributed.AtomicAdmissionMarker,
	bucket tokenbucket.GuardedBucket,
	cfg Config,
) (*Limiter, error) {
	regionID := strings.TrimSpace(cfg.RegionID)
	if resolver == nil {
		return nil, fmt.Errorf("team quota rate policy resolver is required")
	}
	if marker == nil {
		return nil, fmt.Errorf("team quota atomic admission marker is required")
	}
	if bucket == nil {
		return nil, fmt.Errorf("team quota guarded token bucket is required")
	}
	if regionID == "" {
		return nil, fmt.Errorf("team quota region ID is required")
	}
	policyGuard := cfg.PolicyGuard
	if policyGuard == nil {
		policyGuard = bucket
	}
	policies, err := distributed.NewPolicyCache(
		resolver,
		policyGuard,
		teamquota.KindRate,
		cfg.PolicyCacheTTL,
		cfg.PolicyCacheMaxEntries,
	)
	if err != nil {
		return nil, err
	}
	localCreditTTL := cfg.LocalCreditTTL
	if localCreditTTL == 0 {
		localCreditTTL = defaultLocalCreditTTL
	}
	if localCreditTTL < minLocalCreditTTL ||
		localCreditTTL > guard.MaxLocalCreditTTL {
		return nil, fmt.Errorf(
			"team quota local credit TTL must be between %s and %s",
			minLocalCreditTTL,
			guard.MaxLocalCreditTTL,
		)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	waiter := cfg.WaitUntil
	if waiter == nil {
		waiter = waitUntilContext
	}
	return &Limiter{
		resolver:       resolver,
		marker:         marker,
		bucket:         bucket,
		regionID:       regionID,
		policies:       policies,
		credits:        newLocalCreditCache(cfg.PolicyCacheMaxEntries),
		localCreditTTL: localCreditTTL,
		now:            now,
		waitUntil:      waiter,
	}, nil
}

// Take immediately attempts to consume cost tokens for a team and key.
func (l *Limiter) Take(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
	cost int64,
) (tokenbucket.Decision, error) {
	if l == nil || l.resolver == nil || l.bucket == nil || l.marker == nil {
		return tokenbucket.Decision{}, unavailable(
			"take rate tokens",
			fmt.Errorf("rate limiter is not configured"),
		)
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return tokenbucket.Decision{}, unavailable(
			"take rate tokens",
			fmt.Errorf("team_id is required"),
		)
	}
	if cost <= 0 {
		return tokenbucket.Decision{}, unavailable(
			fmt.Sprintf("take %s rate tokens", key),
			tokenbucket.ErrInvalidTokenCost,
		)
	}
	kind, known := teamquota.KindForKey(key)
	if !known || kind != teamquota.KindRate {
		return tokenbucket.Decision{}, fmt.Errorf("team quota key %q is not a rate key", key)
	}

	var lastErr error
retryLoop:
	for attempt := 0; attempt < guardRetryAttempts; attempt++ {
		resolved, err := l.policies.Effective(ctx, teamID, key)
		if err != nil {
			return tokenbucket.Decision{}, err
		}
		policy := resolved.Policy
		if cost > policy.Burst {
			return tokenbucket.Decision{}, unavailable(
				fmt.Sprintf("take %s rate tokens", key),
				tokenbucket.ErrCostExceedsBurst,
			)
		}
		admissionKey, err := l.marker.RedisKey(teamID)
		if err != nil {
			return tokenbucket.Decision{}, unavailable(
				"resolve atomic team admission marker",
				err,
			)
		}

		handle := l.credits.acquire(
			localCreditIdentity(teamID, key, policy.Revision, resolved.Version),
			l.now(),
		)
		handle.entry.mu.Lock()
		now := l.now()
		if handle.cached &&
			now.Before(handle.entry.expiresAt) &&
			handle.entry.balance >= cost {
			handle.entry.balance -= cost
			remaining := handle.entry.balance
			handle.entry.mu.Unlock()
			handle.release()
			return tokenbucket.Decision{
				Allowed:   true,
				Remaining: remaining,
			}, nil
		}
		// Insufficient and expired reservations are deliberately burned. They
		// are never returned to Redis.
		handle.entry.balance = 0
		handle.entry.expiresAt = time.Time{}

		grant := cost
		if handle.cached {
			grant = localCreditBatch(key)
			if grant < cost {
				grant = cost
			}
			if grant > policy.Burst {
				grant = policy.Burst
			}
		}
		decision, takeErr := l.takeDistributed(
			ctx,
			teamID,
			key,
			admissionKey,
			policy,
			resolved,
			grant,
			cost,
		)
		if takeErr == nil && decision.Allowed {
			remainder := grant - cost
			if handle.cached && remainder > 0 {
				handle.entry.balance = remainder
				// Anchor at the start of the Redis call. This is conservative:
				// the local reservation can never outlive grant time + TTL.
				handle.entry.expiresAt = now.Add(l.localCreditTTL)
				decision.Remaining += remainder
				maxRemaining := policy.Burst - cost
				if decision.Remaining > maxRemaining {
					decision.Remaining = maxRemaining
				}
			}
		}
		handle.entry.mu.Unlock()
		handle.release()

		if takeErr == nil {
			return decision, nil
		}
		switch {
		case errors.Is(takeErr, tokenbucket.ErrAdmissionMissing):
			if err := l.marker.Recover(ctx, teamID); err != nil {
				return tokenbucket.Decision{}, unavailable(
					"recover atomic team admission marker",
					err,
				)
			}
			lastErr = takeErr
			continue
		case errors.Is(takeErr, tokenbucket.ErrAdmissionDisabled):
			l.credits.invalidate(teamID, nil)
			l.policies.InvalidateTeam(teamID)
			return tokenbucket.Decision{}, unavailable(
				"check atomic team admission marker",
				&teamquota.TeamAdmissionDisabledError{TeamID: teamID},
			)
		case policyGuardRetryable(takeErr):
			lastErr = takeErr
			l.policies.Invalidate(teamID, key)
			if !waitGuardRetry(ctx, attempt) {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return tokenbucket.Decision{}, ctxErr
				}
				break retryLoop
			}
		default:
			return tokenbucket.Decision{}, unavailable(
				fmt.Sprintf("take %s rate tokens", key),
				takeErr,
			)
		}
	}
	return tokenbucket.Decision{}, unavailable(
		fmt.Sprintf("take %s rate tokens", key),
		fmt.Errorf("distributed admission did not stabilize: %w", lastErr),
	)
}

func (l *Limiter) takeDistributed(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
	admissionKey string,
	policy teamquota.Policy,
	resolved distributed.ResolvedPolicy,
	grant int64,
	cost int64,
) (tokenbucket.Decision, error) {
	bucketPolicy := tokenbucket.Policy{
		Tokens:   policy.Tokens,
		Interval: time.Duration(policy.IntervalMillis) * time.Millisecond,
		Burst:    policy.Burst,
		Revision: policy.Revision,
	}
	decision, err := l.bucket.TakeNGuarded(
		ctx,
		bucketKey(l.regionID, teamID, key),
		admissionKey,
		bucketPolicy,
		resolved.Version,
		resolved.RateRefillFrom,
		grant,
	)
	if err != nil || decision.Allowed || grant == cost {
		return decision, err
	}
	// A batch may be larger than the tokens currently available even though
	// this request's exact cost fits. Denied batches never mutate token usage,
	// so retry the exact cost atomically.
	return l.bucket.TakeNGuarded(
		ctx,
		bucketKey(l.regionID, teamID, key),
		admissionKey,
		bucketPolicy,
		resolved.Version,
		resolved.RateRefillFrom,
		cost,
	)
}

// EffectivePolicy returns the currently cached or persisted rate policy.
// Admission is checked atomically by Take, not by this read-only method.
func (l *Limiter) EffectivePolicy(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
) (teamquota.Policy, error) {
	if l == nil || l.policies == nil {
		return teamquota.Policy{}, unavailable(
			"resolve rate policy",
			fmt.Errorf("rate limiter is not configured"),
		)
	}
	resolved, err := l.policies.Effective(ctx, strings.TrimSpace(teamID), key)
	return resolved.Policy, err
}

// Invalidate burns one local reservation and evicts its policy after an admin
// change.
func (l *Limiter) Invalidate(teamID string, key teamquota.Key) {
	if l == nil {
		return
	}
	teamID = strings.TrimSpace(teamID)
	l.credits.invalidate(teamID, &key)
	l.policies.Invalidate(teamID, key)
}

// DisableTeamDistributedAdmission publishes the tombstone, burns this
// process's credits, and waits the global maximum credit TTL before returning.
func (l *Limiter) DisableTeamDistributedAdmission(ctx context.Context, teamID string) error {
	if l == nil || l.marker == nil {
		return unavailable(
			"disable distributed team admission",
			fmt.Errorf("admission marker is not configured"),
		)
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if err := l.marker.Disable(ctx, teamID); err != nil {
		return unavailable("disable distributed team admission", err)
	}
	l.credits.invalidate(teamID, nil)
	l.policies.InvalidateTeam(teamID)
	if err := l.waitUntil(ctx, l.now().Add(guard.MaxLocalCreditTTL)); err != nil {
		return unavailable("drain distributed team admission credits", err)
	}
	return nil
}

func bucketKey(regionID, teamID string, key teamquota.Key) string {
	return fmt.Sprintf(
		"team-quota:v1:%d:%s:%d:%s:%s",
		len(regionID),
		regionID,
		len(teamID),
		teamID,
		key,
	)
}

func policyGuardRetryable(err error) bool {
	return errors.Is(err, guard.ErrMissing) ||
		errors.Is(err, guard.ErrPending) ||
		errors.Is(err, guard.ErrStale)
}

func waitGuardRetry(ctx context.Context, attempt int) bool {
	if attempt+1 >= guardRetryAttempts {
		return false
	}
	timer := time.NewTimer(guardRetryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func waitUntilContext(ctx context.Context, deadline time.Time) error {
	delay := time.Until(deadline)
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

func unavailable(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &teamquota.UnavailableError{Operation: operation, Err: err}
}
