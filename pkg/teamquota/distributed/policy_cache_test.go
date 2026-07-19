package distributed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

func TestPolicyCacheBindsPostgresReadToStableGuardVersion(t *testing.T) {
	first := guard.State{
		Phase:   guard.PhaseStable,
		Version: guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
	}
	second := guard.State{
		Phase:   guard.PhaseStable,
		Version: guard.Version{EnforcementEpoch: 2, RedisGeneration: 1},
		RateRefillFrom: time.Unix(
			1_700_000_000,
			0,
		).UTC(),
	}
	reader := &sequenceGuardReader{results: []guardReadResult{
		{state: first},
		{state: second},
		{state: second},
		{state: second},
	}}
	resolver := &cachePolicyResolver{policy: validRatePolicy()}
	cache := newTestPolicyCache(t, resolver, reader, time.Minute)

	resolved, err := cache.Effective(context.Background(), "team-a", teamquota.KeyAPIRequests)
	if err != nil {
		t.Fatalf("Effective() error = %v", err)
	}
	if !resolved.Version.Equal(second.Version) ||
		!resolved.RateRefillFrom.Equal(second.RateRefillFrom) {
		t.Fatalf("resolved guard state = %+v, want %+v", resolved, second)
	}
	if resolver.callCount() != 2 {
		t.Fatalf("resolver calls = %d, want 2 after guard changed around first read", resolver.callCount())
	}
	if reader.callCount() != 4 {
		t.Fatalf("guard reads = %d, want 4", reader.callCount())
	}

	reader.setResults([]guardReadResult{{state: guard.State{
		Phase:        guard.PhasePending,
		Version:      second.Version,
		PendingToken: "mutation",
	}}})
	cached, err := cache.Effective(context.Background(), "team-a", teamquota.KeyAPIRequests)
	if err != nil {
		t.Fatalf("cached Effective() error = %v", err)
	}
	if !cached.Version.Equal(second.Version) {
		t.Fatalf("cached version = %+v, want %+v", cached.Version, second.Version)
	}
	if reader.callCount() != 4 {
		t.Fatalf("cache hit read guard %d times, want no additional read", reader.callCount())
	}
}

func TestPolicyCacheBoundsPendingGuardRetries(t *testing.T) {
	pending := guard.State{
		Phase:        guard.PhasePending,
		Version:      guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
		PendingToken: "mutation",
	}
	reader := &sequenceGuardReader{results: []guardReadResult{{state: pending}}}
	resolver := &cachePolicyResolver{policy: validRatePolicy()}
	cache := newTestPolicyCache(t, resolver, reader, 0)

	_, err := cache.Effective(context.Background(), "team-a", teamquota.KeyAPIRequests)
	if !teamquota.IsUnavailable(err) || !errors.Is(err, guard.ErrPending) {
		t.Fatalf("Effective() error = %v, want unavailable pending guard", err)
	}
	if reader.callCount() != policyGuardReadAttempts {
		t.Fatalf("guard reads = %d, want %d", reader.callCount(), policyGuardReadAttempts)
	}
	if resolver.callCount() != 0 {
		t.Fatalf("resolver calls = %d, want 0 while guard is pending", resolver.callCount())
	}
}

func TestPolicyCacheCancellationInterruptsGuardRetry(t *testing.T) {
	pending := guard.State{
		Phase:        guard.PhasePending,
		Version:      guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
		PendingToken: "mutation",
	}
	reader := &sequenceGuardReader{results: []guardReadResult{{state: pending}}}
	cache := newTestPolicyCache(
		t,
		&cachePolicyResolver{policy: validRatePolicy()},
		reader,
		0,
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := cache.Effective(ctx, "team-a", teamquota.KeyAPIRequests)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Effective() error = %v, want context.Canceled", err)
	}
	if reader.callCount() != 1 {
		t.Fatalf("guard reads = %d, want 1 before canceled retry", reader.callCount())
	}
}

type guardReadResult struct {
	state guard.State
	err   error
}

type sequenceGuardReader struct {
	mu      sync.Mutex
	results []guardReadResult
	calls   int
}

func (r *sequenceGuardReader) ReadPolicyGuard(context.Context) (guard.State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if len(r.results) == 0 {
		return guard.State{}, guard.ErrMissing
	}
	index := r.calls - 1
	if index >= len(r.results) {
		index = len(r.results) - 1
	}
	result := r.results[index]
	return result.state, result.err
}

func (r *sequenceGuardReader) setResults(results []guardReadResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = results
}

func (r *sequenceGuardReader) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

type cachePolicyResolver struct {
	mu     sync.Mutex
	policy *teamquota.Policy
	err    error
	calls  int
}

func (r *cachePolicyResolver) EffectivePolicy(
	context.Context,
	string,
	teamquota.Key,
) (*teamquota.Policy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.policy == nil {
		return nil, r.err
	}
	policy := *r.policy
	return &policy, r.err
}

func (r *cachePolicyResolver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func validRatePolicy() *teamquota.Policy {
	return &teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       1,
		Tokens:         1,
		IntervalMillis: 1000,
		Burst:          1,
	}
}

func newTestPolicyCache(
	t *testing.T,
	resolver PolicyResolver,
	reader guard.Reader,
	ttl time.Duration,
) *PolicyCache {
	t.Helper()
	cache, err := NewPolicyCache(resolver, reader, teamquota.KindRate, ttl, 10)
	if err != nil {
		t.Fatalf("NewPolicyCache() error = %v", err)
	}
	return cache
}
