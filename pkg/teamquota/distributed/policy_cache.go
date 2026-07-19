package distributed

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

const (
	defaultPolicyCacheMaxEntries = 10_000
	policyGuardReadAttempts      = 4
	policyGuardRetryDelay        = 10 * time.Millisecond
)

// PolicyResolver returns one effective region policy for a team.
type PolicyResolver interface {
	EffectivePolicy(context.Context, string, teamquota.Key) (*teamquota.Policy, error)
}

// PolicyCache is a bounded local lookup cache for effective policies of one
// kind. PostgreSQL remains the source of truth. TTL controls ordinary lookup
// reuse only; every distributed mutation changes the exact policy guard, so a
// stale cached value is rejected and refreshed immediately after commit.
type PolicyCache struct {
	resolver PolicyResolver
	guard    guard.Reader
	kind     teamquota.Kind
	ttl      time.Duration
	max      int

	mu      sync.RWMutex
	entries map[cacheKey]cacheEntry
	now     func() time.Time
}

type cacheKey struct {
	teamID string
	key    teamquota.Key
}

type cacheEntry struct {
	resolved  ResolvedPolicy
	expiresAt time.Time
}

// ResolvedPolicy binds a PostgreSQL policy read to the stable Redis generation
// observed immediately before and after that read.
type ResolvedPolicy struct {
	Policy         teamquota.Policy
	Version        guard.Version
	RateRefillFrom time.Time
}

// NewPolicyCache validates and creates a bounded effective-policy cache.
func NewPolicyCache(
	resolver PolicyResolver,
	policyGuard guard.Reader,
	kind teamquota.Kind,
	ttl time.Duration,
	maxEntries int,
) (*PolicyCache, error) {
	if resolver == nil {
		return nil, fmt.Errorf("team quota policy resolver is required")
	}
	if policyGuard == nil {
		return nil, fmt.Errorf("team quota policy guard is required")
	}
	if kind != teamquota.KindRate && kind != teamquota.KindConcurrency {
		return nil, fmt.Errorf("unsupported distributed team quota kind %q", kind)
	}
	if ttl < 0 {
		return nil, fmt.Errorf("team quota policy cache TTL must be non-negative")
	}
	if maxEntries < 0 {
		return nil, fmt.Errorf("team quota policy cache maximum entries must be non-negative")
	}
	if maxEntries == 0 {
		maxEntries = defaultPolicyCacheMaxEntries
	}
	return &PolicyCache{
		resolver: resolver,
		guard:    policyGuard,
		kind:     kind,
		ttl:      ttl,
		max:      maxEntries,
		entries:  make(map[cacheKey]cacheEntry),
		now:      time.Now,
	}, nil
}

// Effective returns a validated policy for teamID and key.
func (c *PolicyCache) Effective(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
) (ResolvedPolicy, error) {
	if c == nil || c.resolver == nil || c.guard == nil {
		return ResolvedPolicy{}, unavailable("resolve distributed policy", fmt.Errorf("policy cache is not configured"))
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return ResolvedPolicy{}, unavailable("resolve distributed policy", fmt.Errorf("team_id is required"))
	}
	kind, known := teamquota.KindForKey(key)
	if !known || kind != c.kind {
		return ResolvedPolicy{}, fmt.Errorf("team quota key %q is not a %s key", key, c.kind)
	}

	now := c.now()
	cacheKey := cacheKey{teamID: teamID, key: key}
	if c.ttl > 0 {
		c.mu.RLock()
		entry, ok := c.entries[cacheKey]
		c.mu.RUnlock()
		if ok && now.Before(entry.expiresAt) {
			return entry.resolved, nil
		}
	}

	var lastErr error
	for attempt := 0; attempt < policyGuardReadAttempts; attempt++ {
		before, err := c.stableGuard(ctx)
		if err != nil {
			lastErr = err
			if !retryPolicyGuard(ctx, attempt) {
				break
			}
			continue
		}
		policy, err := c.resolver.EffectivePolicy(ctx, teamID, key)
		if err != nil {
			if teamquota.IsUnavailable(err) {
				return ResolvedPolicy{}, err
			}
			return ResolvedPolicy{}, unavailable(
				fmt.Sprintf("resolve %s policy", key),
				err,
			)
		}
		if policy == nil {
			return ResolvedPolicy{}, unavailable(
				fmt.Sprintf("resolve %s policy", key),
				fmt.Errorf("effective policy is missing"),
			)
		}
		if policy.Key != key || policy.Kind != c.kind {
			return ResolvedPolicy{}, unavailable(
				fmt.Sprintf("resolve %s policy", key),
				fmt.Errorf("resolver returned incompatible policy"),
			)
		}
		if policy.TeamID != "" && strings.TrimSpace(policy.TeamID) != teamID {
			return ResolvedPolicy{}, unavailable(
				fmt.Sprintf("resolve %s policy", key),
				fmt.Errorf("resolver returned policy for a different team"),
			)
		}
		if err := policy.Validate(); err != nil {
			return ResolvedPolicy{}, unavailable(
				fmt.Sprintf("validate %s policy", key),
				err,
			)
		}
		after, err := c.stableGuard(ctx)
		if err != nil {
			lastErr = err
			if !retryPolicyGuard(ctx, attempt) {
				break
			}
			continue
		}
		if !before.Version.Equal(after.Version) {
			lastErr = guard.ErrStale
			if !retryPolicyGuard(ctx, attempt) {
				break
			}
			continue
		}
		resolved := ResolvedPolicy{
			Policy:         *policy,
			Version:        after.Version,
			RateRefillFrom: after.RateRefillFrom,
		}
		if c.ttl > 0 {
			c.mu.Lock()
			c.cacheLocked(cacheKey, cacheEntry{
				resolved:  resolved,
				expiresAt: now.Add(c.ttl),
			}, now)
			c.mu.Unlock()
		}
		return resolved, nil
	}
	if err := ctx.Err(); err != nil {
		return ResolvedPolicy{}, err
	}
	return ResolvedPolicy{}, unavailable(
		fmt.Sprintf("resolve %s policy", key),
		fmt.Errorf("read stable policy guard after %d attempts: %w", policyGuardReadAttempts, lastErr),
	)
}

func (c *PolicyCache) stableGuard(ctx context.Context) (guard.State, error) {
	state, err := c.guard.ReadPolicyGuard(ctx)
	if err != nil {
		return guard.State{}, err
	}
	if !state.Stable() {
		return guard.State{}, guard.ErrPending
	}
	if err := state.Version.Validate(); err != nil {
		return guard.State{}, fmt.Errorf("%w: %v", guard.ErrCorrupt, err)
	}
	return state, nil
}

func retryPolicyGuard(ctx context.Context, attempt int) bool {
	if attempt+1 >= policyGuardReadAttempts {
		return false
	}
	timer := time.NewTimer(policyGuardRetryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Invalidate evicts one team policy after an administrative write.
func (c *PolicyCache) Invalidate(teamID string, key teamquota.Key) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.entries, cacheKey{teamID: strings.TrimSpace(teamID), key: key})
	c.mu.Unlock()
}

// InvalidateTeam evicts every locally cached policy for a disabled team.
func (c *PolicyCache) InvalidateTeam(teamID string) {
	if c == nil {
		return
	}
	teamID = strings.TrimSpace(teamID)
	c.mu.Lock()
	for key := range c.entries {
		if key.teamID == teamID {
			delete(c.entries, key)
		}
	}
	c.mu.Unlock()
}

// Len returns the number of cached entries.
func (c *PolicyCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

func (c *PolicyCache) cacheLocked(key cacheKey, entry cacheEntry, now time.Time) {
	if _, exists := c.entries[key]; exists {
		c.entries[key] = entry
		return
	}
	for candidate, cached := range c.entries {
		if !now.Before(cached.expiresAt) {
			delete(c.entries, candidate)
		}
	}
	if len(c.entries) >= c.max {
		var oldestKey cacheKey
		var oldestExpiry time.Time
		for candidate, cached := range c.entries {
			if oldestExpiry.IsZero() || cached.expiresAt.Before(oldestExpiry) {
				oldestKey = candidate
				oldestExpiry = cached.expiresAt
			}
		}
		delete(c.entries, oldestKey)
	}
	c.entries[key] = entry
}

func unavailable(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &teamquota.UnavailableError{Operation: operation, Err: err}
}
