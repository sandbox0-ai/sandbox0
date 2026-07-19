package rate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/distributed"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

const testRedisRunID = "rate-limiter-test-run-id"

func TestLimiterUsesEffectivePolicyAndRegionalTeamKey(t *testing.T) {
	resolver := &fakeResolver{policy: &teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       7,
		Tokens:         10,
		IntervalMillis: 1000,
		Burst:          20,
	}}
	marker := &fakeAdmissionMarker{}
	bucket := &fakeBucket{decision: tokenbucket.Decision{Allowed: true, Remaining: 19}}
	limiter, err := NewLimiter(resolver, marker, bucket, Config{
		RegionID:       "sg",
		PolicyCacheTTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewLimiter() error = %v", err)
	}

	decision, err := limiter.Take(context.Background(), "team-a", teamquota.KeyAPIRequests, 1)
	if err != nil {
		t.Fatalf("Take() error = %v", err)
	}
	if !decision.Allowed || bucket.key != "team-quota:v1:2:sg:6:team-a:api_requests" {
		t.Fatalf("decision/key = %#v %q", decision, bucket.key)
	}
	if bucket.policy.Revision != 7 || bucket.policy.Interval != time.Second || bucket.cost != 16 {
		t.Fatalf("bucket call = policy %#v cost %d", bucket.policy, bucket.cost)
	}

	if _, err := limiter.Take(context.Background(), "team-a", teamquota.KeyAPIRequests, 1); err != nil {
		t.Fatalf("cached Take() error = %v", err)
	}
	if resolver.calls != 1 {
		t.Fatalf("EffectivePolicy calls = %d, want 1", resolver.calls)
	}
	if marker.keyCalls != 2 {
		t.Fatalf("admission marker key calls = %d, want 2 local checks without Redis GET", marker.keyCalls)
	}
}

func TestLimiterFailsClosed(t *testing.T) {
	tests := []struct {
		name     string
		teamID   string
		resolver *fakeResolver
		bucket   *fakeBucket
	}{
		{name: "missing team", resolver: &fakeResolver{}, bucket: &fakeBucket{}},
		{name: "missing policy", teamID: "team-a", resolver: &fakeResolver{}, bucket: &fakeBucket{}},
		{name: "resolver failure", teamID: "team-a", resolver: &fakeResolver{err: errors.New("postgres down")}, bucket: &fakeBucket{}},
		{name: "redis failure", teamID: "team-a", resolver: validResolver(), bucket: &fakeBucket{err: errors.New("redis down")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter, err := NewLimiter(tt.resolver, &fakeAdmissionMarker{}, tt.bucket, Config{RegionID: "sg"})
			if err != nil {
				t.Fatalf("NewLimiter() error = %v", err)
			}
			_, err = limiter.Take(context.Background(), tt.teamID, teamquota.KeyAPIRequests, 1)
			if !teamquota.IsUnavailable(err) {
				t.Fatalf("Take() error = %v, want unavailable", err)
			}
		})
	}
}

func TestLimiterRejectsDisabledTeamAtomicallyAfterCreditDrain(t *testing.T) {
	resolver := validResolver()
	marker := &fakeAdmissionMarker{}
	bucket := &fakeBucket{decision: tokenbucket.Decision{Allowed: true}}
	limiter, err := NewLimiter(resolver, marker, bucket, Config{
		RegionID:       "sg",
		PolicyCacheTTL: time.Hour,
		WaitUntil: func(context.Context, time.Time) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewLimiter() error = %v", err)
	}
	if _, err := limiter.Take(context.Background(), "team-a", teamquota.KeyAPIRequests, 1); err != nil {
		t.Fatalf("initial Take() error = %v", err)
	}

	bucket.err = tokenbucket.ErrAdmissionDisabled
	if err := limiter.DisableTeamDistributedAdmission(context.Background(), "team-a"); err != nil {
		t.Fatalf("DisableTeamDistributedAdmission() error = %v", err)
	}
	_, err = limiter.Take(context.Background(), "team-a", teamquota.KeyAPIRequests, 1)
	if !teamquota.IsUnavailable(err) || !teamquota.IsTeamAdmissionDisabled(err) {
		t.Fatalf("disabled Take() error = %v, want admission-disabled unavailable", err)
	}
	if resolver.calls != 2 {
		t.Fatalf("resolver calls = %d, want one post-invalidation lookup", resolver.calls)
	}
	if bucket.calls != 2 {
		t.Fatalf("bucket calls = %d, want atomic disabled marker check", bucket.calls)
	}
}

func TestLimiterDisableTeamDistributedAdmissionWritesMarkerAndInvalidatesTeamCache(t *testing.T) {
	resolver := validResolver()
	marker := &fakeAdmissionMarker{}
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	waitCalls := 0
	limiter, err := NewLimiter(resolver, marker, &fakeBucket{
		decision: tokenbucket.Decision{Allowed: true},
	}, Config{
		RegionID:       "sg",
		PolicyCacheTTL: time.Hour,
		Now:            clock.Now,
		WaitUntil: func(_ context.Context, deadline time.Time) error {
			waitCalls++
			if got := deadline.Sub(clock.Now()); got != guard.MaxLocalCreditTTL {
				t.Fatalf(
					"disable credit drain = %s, want %s",
					got,
					guard.MaxLocalCreditTTL,
				)
			}
			clock.Advance(guard.MaxLocalCreditTTL)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewLimiter() error = %v", err)
	}
	if _, err := limiter.Take(context.Background(), "team-a", teamquota.KeyAPIRequests, 1); err != nil {
		t.Fatalf("initial Take() error = %v", err)
	}
	if err := limiter.DisableTeamDistributedAdmission(context.Background(), "team-a"); err != nil {
		t.Fatalf("DisableTeamDistributedAdmission() error = %v", err)
	}
	if marker.disableCalls != 1 || marker.disabledTeamID != "team-a" {
		t.Fatalf("marker disable = (%d, %q), want (1, team-a)", marker.disableCalls, marker.disabledTeamID)
	}
	if limiter.policies.Len() != 0 {
		t.Fatalf("policy cache entries = %d, want 0", limiter.policies.Len())
	}
	if waitCalls != 1 {
		t.Fatalf("disable credit drain waiter calls = %d, want 1", waitCalls)
	}
}

func TestLimiterBoundsPolicyCache(t *testing.T) {
	resolver := validResolver()
	limiter, err := NewLimiter(resolver, &fakeAdmissionMarker{}, &fakeBucket{
		decision: tokenbucket.Decision{Allowed: true},
	}, Config{
		RegionID:              "sg",
		PolicyCacheTTL:        time.Minute,
		PolicyCacheMaxEntries: 1,
	})
	if err != nil {
		t.Fatalf("NewLimiter() error = %v", err)
	}

	for _, teamID := range []string{"team-a", "team-b", "team-a"} {
		if _, err := limiter.Take(
			context.Background(),
			teamID,
			teamquota.KeyAPIRequests,
			1,
		); err != nil {
			t.Fatalf("Take(%q) error = %v", teamID, err)
		}
	}
	if resolver.calls != 3 {
		t.Fatalf("EffectivePolicy calls = %d, want 3 after cache eviction", resolver.calls)
	}
}

func TestTwoLimitersCannotSpendOldCreditsAfterLoweringCommitReturns(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisServer.SetTime(time.Unix(1_700_000_000, 0))
	const prefix = "test:teamquota"
	firstVersion := guard.Version{EnforcementEpoch: 1, RedisGeneration: 1}
	setRateTestGuard(t, redisServer, prefix, guard.State{
		Phase:   guard.PhaseStable,
		Version: firstVersion,
	})
	resolver := &mutableRateResolver{policy: teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       10,
		Tokens:         1,
		IntervalMillis: int64(time.Hour / time.Millisecond),
		Burst:          32,
	}}
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	first := newRedisRateLimiterWithClock(t, redisServer, prefix, resolver, clock)
	second := newRedisRateLimiterWithClock(t, redisServer, prefix, resolver, clock)

	for index, limiter := range []*Limiter{first, second} {
		decision, err := limiter.Take(
			context.Background(),
			"team-a",
			teamquota.KeyAPIRequests,
			1,
		)
		if err != nil || !decision.Allowed {
			t.Fatalf("initial Take(%d) = (%+v, %v), want allowed", index, decision, err)
		}
	}

	setRateTestGuard(t, redisServer, prefix, guard.State{
		Phase:        guard.PhasePending,
		Version:      firstVersion,
		PendingToken: "writer",
	})
	// A policy writer keeps the guard pending for the global 100ms credit
	// drain barrier before it commits and publishes the stable new epoch.
	clock.Advance(guard.MaxLocalCreditTTL)
	resolver.setPolicy(teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       11,
		Tokens:         1,
		IntervalMillis: int64(time.Hour / time.Millisecond),
		Burst:          1,
	})
	setRateTestGuard(t, redisServer, prefix, guard.State{
		Phase: guard.PhaseStable,
		Version: guard.Version{
			EnforcementEpoch: 2,
			RedisGeneration:  1,
		},
	})

	firstDecision, err := first.Take(
		context.Background(),
		"team-a",
		teamquota.KeyAPIRequests,
		1,
	)
	if err != nil {
		t.Fatalf("first lowered Take() error = %v", err)
	}
	if firstDecision.Allowed || firstDecision.Remaining != 0 {
		t.Fatalf("first lowered Take() = %+v, want drained old credit rejected", firstDecision)
	}
	secondDecision, err := second.Take(
		context.Background(),
		"team-a",
		teamquota.KeyAPIRequests,
		1,
	)
	if err != nil {
		t.Fatalf("second lowered Take() error = %v", err)
	}
	if secondDecision.Allowed || secondDecision.Remaining != 0 {
		t.Fatalf("second lowered Take() = %+v, want denied at lowered burst", secondDecision)
	}
	if resolver.callCount() != 4 {
		t.Fatalf("resolver calls = %d, want both non-expired caches refreshed", resolver.callCount())
	}
}

func TestLimiterAcceptsOlderDefaultRevisionAfterOverrideDelete(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisServer.SetTime(time.Unix(1_700_000_000, 0))
	const prefix = "test:teamquota"
	setRateTestGuard(t, redisServer, prefix, guard.State{
		Phase:   guard.PhaseStable,
		Version: guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
	})
	resolver := &mutableRateResolver{policy: teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       20,
		Tokens:         1,
		IntervalMillis: int64(time.Hour / time.Millisecond),
		Burst:          10,
	}}
	limiter := newRedisRateLimiter(t, redisServer, prefix, resolver)

	decision, err := limiter.Take(
		context.Background(),
		"team-a",
		teamquota.KeyAPIRequests,
		9,
	)
	if err != nil || !decision.Allowed || decision.Remaining != 1 {
		t.Fatalf("override Take() = (%+v, %v), want one remaining", decision, err)
	}

	// Deleting an override can reveal an older default row revision. Revision
	// is an identity, not a value that enforcers may compare numerically.
	resolver.setPolicy(teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       2,
		Tokens:         1,
		IntervalMillis: int64(time.Hour / time.Millisecond),
		Burst:          5,
	})
	setRateTestGuard(t, redisServer, prefix, guard.State{
		Phase:   guard.PhaseStable,
		Version: guard.Version{EnforcementEpoch: 2, RedisGeneration: 1},
	})
	decision, err = limiter.Take(
		context.Background(),
		"team-a",
		teamquota.KeyAPIRequests,
		2,
	)
	if err != nil {
		t.Fatalf("fallback Take() error = %v", err)
	}
	if decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("fallback Take() = %+v, want older revision applied without refill", decision)
	}
	if resolver.callCount() != 2 {
		t.Fatalf("resolver calls = %d, want immediate fallback refresh", resolver.callCount())
	}
}

func TestTwoLimitersDeleteOverrideRevealsOlderRevisionWithoutFreeRefill(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisServer.SetTime(time.Unix(1_700_000_000, 0))
	const prefix = "test:teamquota"
	firstVersion := guard.Version{EnforcementEpoch: 1, RedisGeneration: 1}
	setRateTestGuard(t, redisServer, prefix, guard.State{
		Phase:   guard.PhaseStable,
		Version: firstVersion,
	})
	resolver := &mutableRateResolver{policy: teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       20,
		Tokens:         1,
		IntervalMillis: int64(time.Hour / time.Millisecond),
		Burst:          32,
	}}
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	first := newRedisRateLimiterWithClock(t, redisServer, prefix, resolver, clock)
	second := newRedisRateLimiterWithClock(t, redisServer, prefix, resolver, clock)
	for index, limiter := range []*Limiter{first, second} {
		decision, err := limiter.Take(
			context.Background(),
			"team-a",
			teamquota.KeyAPIRequests,
			1,
		)
		if err != nil || !decision.Allowed {
			t.Fatalf("override limiter %d Take() = (%+v, %v)", index, decision, err)
		}
	}

	setRateTestGuard(t, redisServer, prefix, guard.State{
		Phase:        guard.PhasePending,
		Version:      firstVersion,
		PendingToken: "delete",
	})
	clock.Advance(guard.MaxLocalCreditTTL)
	resolver.setPolicy(teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       2,
		Tokens:         1,
		IntervalMillis: int64(time.Hour / time.Millisecond),
		Burst:          5,
	})
	setRateTestGuard(t, redisServer, prefix, guard.State{
		Phase: guard.PhaseStable,
		Version: guard.Version{
			EnforcementEpoch: 2,
			RedisGeneration:  1,
		},
	})
	for index, limiter := range []*Limiter{first, second} {
		decision, err := limiter.Take(
			context.Background(),
			"team-a",
			teamquota.KeyAPIRequests,
			1,
		)
		if err != nil {
			t.Fatalf("fallback limiter %d Take() error = %v", index, err)
		}
		if decision.Allowed || decision.Remaining != 0 {
			t.Fatalf(
				"fallback limiter %d Take() = %+v, want no free refill",
				index,
				decision,
			)
		}
	}
	if resolver.callCount() != 4 {
		t.Fatalf("resolver calls = %d, want both stale caches refreshed", resolver.callCount())
	}
}

func TestLimiterBatchesCreditsFallsBackToExactAndBurnsExpiredRemainder(t *testing.T) {
	policy := teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       1,
		Tokens:         100,
		IntervalMillis: 1000,
		Burst:          100,
	}
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	bucket := &scriptedGuardedBucket{
		decisions: []tokenbucket.Decision{
			{Allowed: false, Remaining: 10, RetryAfter: time.Millisecond},
			{Allowed: true, Remaining: 9},
		},
	}
	limiter, err := NewLimiter(
		&mutableRateResolver{policy: policy},
		staticAtomicMarker{},
		bucket,
		Config{
			RegionID:       "region-a",
			PolicyCacheTTL: time.Hour,
			Now:            clock.Now,
		},
	)
	if err != nil {
		t.Fatalf("NewLimiter() error = %v", err)
	}
	decision, err := limiter.Take(
		context.Background(),
		"team-a",
		teamquota.KeyAPIRequests,
		1,
	)
	if err != nil || !decision.Allowed {
		t.Fatalf("Take() = (%+v, %v), want exact fallback allowed", decision, err)
	}
	if got := bucket.costSnapshot(); !equalCosts(got, []int64{16, 1}) {
		t.Fatalf("distributed costs = %#v, want batch 16 then exact 1", got)
	}

	accounting := &accountingGuardedBucket{}
	limiter, err = NewLimiter(
		&mutableRateResolver{policy: policy},
		staticAtomicMarker{},
		accounting,
		Config{
			RegionID:       "region-a",
			PolicyCacheTTL: time.Hour,
			Now:            clock.Now,
		},
	)
	if err != nil {
		t.Fatalf("NewLimiter(expiry) error = %v", err)
	}
	if _, err := limiter.Take(context.Background(), "team-a", teamquota.KeyAPIRequests, 1); err != nil {
		t.Fatalf("initial Take() error = %v", err)
	}
	clock.Advance(defaultLocalCreditTTL - time.Millisecond)
	if _, err := limiter.Take(context.Background(), "team-a", teamquota.KeyAPIRequests, 1); err != nil {
		t.Fatalf("pre-expiry Take() error = %v", err)
	}
	if got := accounting.grantedTotal(); got != 16 {
		t.Fatalf("pre-expiry Redis grants = %d, want 16", got)
	}
	clock.Advance(time.Millisecond)
	if _, err := limiter.Take(context.Background(), "team-a", teamquota.KeyAPIRequests, 1); err != nil {
		t.Fatalf("exact-expiry Take() error = %v", err)
	}
	if got := accounting.grantedTotal(); got != 32 {
		t.Fatalf("exact-expiry Redis grants = %d, want old remainder burned and new batch", got)
	}

	byteBucket := &accountingGuardedBucket{}
	bytePolicy := policy
	bytePolicy.Key = teamquota.KeyObservabilityIngestBytes
	bytePolicy.Tokens = 128 * 1024
	bytePolicy.Burst = 128 * 1024
	byteLimiter, err := NewLimiter(
		&mutableRateResolver{policy: bytePolicy},
		staticAtomicMarker{},
		byteBucket,
		Config{
			RegionID:       "region-a",
			PolicyCacheTTL: time.Hour,
			Now:            clock.Now,
		},
	)
	if err != nil {
		t.Fatalf("NewLimiter(bytes) error = %v", err)
	}
	if _, err := byteLimiter.Take(
		context.Background(),
		"team-a",
		teamquota.KeyObservabilityIngestBytes,
		1024,
	); err != nil {
		t.Fatalf("byte Take() error = %v", err)
	}
	if got := byteBucket.grantedTotal(); got != defaultByteCreditBatch {
		t.Fatalf(
			"byte Redis grant = %d, want %d",
			got,
			defaultByteCreditBatch,
		)
	}
}

func TestLimiterConcurrentConsumptionNeverExceedsRedisGrants(t *testing.T) {
	const concurrentTakes = 1000
	resolver := &mutableRateResolver{policy: teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       1,
		Tokens:         1_000_000,
		IntervalMillis: 1000,
		Burst:          1_000_000,
	}}
	bucket := &accountingGuardedBucket{}
	limiter, err := NewLimiter(
		resolver,
		staticAtomicMarker{},
		bucket,
		Config{
			RegionID:       "region-a",
			PolicyCacheTTL: time.Hour,
		},
	)
	if err != nil {
		t.Fatalf("NewLimiter() error = %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, concurrentTakes)
	for i := 0; i < concurrentTakes; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decision, err := limiter.Take(
				context.Background(),
				"team-a",
				teamquota.KeyAPIRequests,
				1,
			)
			if err == nil && !decision.Allowed {
				err = errors.New("unexpected denial")
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Take() error = %v", err)
		}
	}
	granted := bucket.grantedTotal()
	if granted < concurrentTakes {
		t.Fatalf("Redis grants = %d, consumed = %d", granted, concurrentTakes)
	}
	if remainder := granted - concurrentTakes; remainder < 0 || remainder >= defaultRequestCreditBatch {
		t.Fatalf(
			"local outstanding remainder = %d, want [0,%d)",
			remainder,
			defaultRequestCreditBatch,
		)
	}
}

func TestLimiterRecoversMissingAndFlushedAdmissionMarkerThenFailsClosedDisabled(t *testing.T) {
	redisServer := miniredis.RunT(t)
	const prefix = "test:teamquota"
	version := guard.Version{EnforcementEpoch: 1, RedisGeneration: 1}
	setRateTestGuard(t, redisServer, prefix, guard.State{
		Phase:   guard.PhaseStable,
		Version: version,
	})
	resolver := &mutableRateResolver{policy: teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       1,
		Tokens:         100,
		IntervalMillis: 1000,
		Burst:          100,
	}}
	marker, err := distributed.NewRedisAdmissionMarker(
		context.Background(),
		resolver,
		distributed.AdmissionMarkerConfig{
			RegionID:  "region-a",
			RedisURL:  "redis://" + redisServer.Addr() + "/0",
			KeyPrefix: prefix,
			Timeout:   time.Second,
		},
	)
	if err != nil {
		t.Fatalf("NewRedisAdmissionMarker() error = %v", err)
	}
	t.Cleanup(func() { _ = marker.Close() })
	bucket, err := tokenbucket.NewRedisBucket(context.Background(), tokenbucket.RedisConfig{
		URL:       "redis://" + redisServer.Addr() + "/0",
		KeyPrefix: prefix,
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisBucket() error = %v", err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	limiter, err := NewLimiter(resolver, marker, bucket, Config{
		RegionID:       "region-a",
		PolicyCacheTTL: time.Hour,
		Now:            clock.Now,
	})
	if err != nil {
		t.Fatalf("NewLimiter() error = %v", err)
	}

	if decision, err := limiter.Take(
		context.Background(),
		"team-a",
		teamquota.KeyAPIRequests,
		1,
	); err != nil || !decision.Allowed {
		t.Fatalf("missing-marker Take() = (%+v, %v)", decision, err)
	}
	if resolver.admissionCallCount() != 1 {
		t.Fatalf("admission resolver calls = %d, want 1", resolver.admissionCallCount())
	}

	clock.Advance(guard.MaxLocalCreditTTL)
	redisServer.FlushAll()
	version.RedisGeneration++
	setRateTestGuard(t, redisServer, prefix, guard.State{
		Phase:   guard.PhaseStable,
		Version: version,
	})
	limiter.Invalidate("team-a", teamquota.KeyAPIRequests)
	if decision, err := limiter.Take(
		context.Background(),
		"team-a",
		teamquota.KeyAPIRequests,
		1,
	); err != nil || !decision.Allowed {
		t.Fatalf("flushed-marker Take() = (%+v, %v)", decision, err)
	}
	if resolver.admissionCallCount() != 2 {
		t.Fatalf("admission resolver calls = %d, want post-FLUSH recovery", resolver.admissionCallCount())
	}

	resolver.setAdmissionDisabled(true)
	clock.Advance(guard.MaxLocalCreditTTL)
	redisServer.FlushAll()
	version.RedisGeneration++
	setRateTestGuard(t, redisServer, prefix, guard.State{
		Phase:   guard.PhaseStable,
		Version: version,
	})
	limiter.Invalidate("team-a", teamquota.KeyAPIRequests)
	_, err = limiter.Take(
		context.Background(),
		"team-a",
		teamquota.KeyAPIRequests,
		1,
	)
	if !teamquota.IsUnavailable(err) || !teamquota.IsTeamAdmissionDisabled(err) {
		t.Fatalf("durably disabled Take() error = %v", err)
	}
	if resolver.admissionCallCount() != 3 {
		t.Fatalf("admission resolver calls = %d, want disabled recovery", resolver.admissionCallCount())
	}
}

func validResolver() *fakeResolver {
	return &fakeResolver{policy: &teamquota.Policy{
		Key:            teamquota.KeyAPIRequests,
		Kind:           teamquota.KindRate,
		Revision:       1,
		Tokens:         1,
		IntervalMillis: 1000,
		Burst:          1,
	}}
}

type fakeResolver struct {
	policy *teamquota.Policy
	err    error
	calls  int
}

type mutableRateResolver struct {
	mu                sync.Mutex
	policy            teamquota.Policy
	calls             int
	admissionDisabled bool
	admissionCalls    int
}

func (r *mutableRateResolver) EffectivePolicy(
	context.Context,
	string,
	teamquota.Key,
) (*teamquota.Policy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	policy := r.policy
	return &policy, nil
}

func (r *mutableRateResolver) setPolicy(policy teamquota.Policy) {
	r.mu.Lock()
	r.policy = policy
	r.mu.Unlock()
}

func (r *mutableRateResolver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *mutableRateResolver) TeamAdmissionDisabled(
	context.Context,
	string,
) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.admissionCalls++
	return r.admissionDisabled, nil
}

func (r *mutableRateResolver) setAdmissionDisabled(disabled bool) {
	r.mu.Lock()
	r.admissionDisabled = disabled
	r.mu.Unlock()
}

func (r *mutableRateResolver) admissionCallCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.admissionCalls
}

func (r *fakeResolver) EffectivePolicy(context.Context, string, teamquota.Key) (*teamquota.Policy, error) {
	r.calls++
	return r.policy, r.err
}

type fakeBucket struct {
	key            string
	admissionKey   string
	policy         tokenbucket.Policy
	version        guard.Version
	rateRefillFrom time.Time
	cost           int64
	decision       tokenbucket.Decision
	err            error
	guardState     guard.State
	guardErr       error
	calls          int
}

func (b *fakeBucket) TakeN(_ context.Context, key string, policy tokenbucket.Policy, cost int64) (tokenbucket.Decision, error) {
	return b.take(key, policy, guard.Version{}, time.Time{}, cost)
}

func (b *fakeBucket) TakeNGuarded(
	_ context.Context,
	key string,
	admissionKey string,
	policy tokenbucket.Policy,
	version guard.Version,
	rateRefillFrom time.Time,
	cost int64,
) (tokenbucket.Decision, error) {
	b.admissionKey = admissionKey
	return b.take(key, policy, version, rateRefillFrom, cost)
}

func (b *fakeBucket) take(
	key string,
	policy tokenbucket.Policy,
	version guard.Version,
	rateRefillFrom time.Time,
	cost int64,
) (tokenbucket.Decision, error) {
	b.calls++
	b.key, b.policy, b.version, b.rateRefillFrom, b.cost = key, policy, version, rateRefillFrom, cost
	return b.decision, b.err
}

func (b *fakeBucket) ReadPolicyGuard(context.Context) (guard.State, error) {
	if b.guardErr != nil {
		return guard.State{}, b.guardErr
	}
	if b.guardState.Phase == "" {
		return guard.State{
			Phase:   guard.PhaseStable,
			Version: guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
		}, nil
	}
	return b.guardState, nil
}

func (*fakeBucket) Close() error { return nil }

type scriptedGuardedBucket struct {
	mu        sync.Mutex
	decisions []tokenbucket.Decision
	costs     []int64
}

func (b *scriptedGuardedBucket) TakeN(
	ctx context.Context,
	key string,
	policy tokenbucket.Policy,
	cost int64,
) (tokenbucket.Decision, error) {
	return b.TakeNGuarded(
		ctx,
		key,
		"test:admission",
		policy,
		guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
		time.Time{},
		cost,
	)
}

func (b *scriptedGuardedBucket) TakeNGuarded(
	_ context.Context,
	_ string,
	_ string,
	_ tokenbucket.Policy,
	_ guard.Version,
	_ time.Time,
	cost int64,
) (tokenbucket.Decision, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.costs = append(b.costs, cost)
	if len(b.decisions) == 0 {
		return tokenbucket.Decision{}, errors.New("missing scripted decision")
	}
	decision := b.decisions[0]
	b.decisions = b.decisions[1:]
	return decision, nil
}

func (*scriptedGuardedBucket) ReadPolicyGuard(context.Context) (guard.State, error) {
	return guard.State{
		Phase:   guard.PhaseStable,
		Version: guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
	}, nil
}

func (*scriptedGuardedBucket) Close() error { return nil }

func (b *scriptedGuardedBucket) costSnapshot() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int64(nil), b.costs...)
}

type accountingGuardedBucket struct {
	mu      sync.Mutex
	granted int64
}

func (b *accountingGuardedBucket) TakeN(
	ctx context.Context,
	key string,
	policy tokenbucket.Policy,
	cost int64,
) (tokenbucket.Decision, error) {
	return b.TakeNGuarded(
		ctx,
		key,
		"test:admission",
		policy,
		guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
		time.Time{},
		cost,
	)
}

func (b *accountingGuardedBucket) TakeNGuarded(
	_ context.Context,
	_ string,
	_ string,
	policy tokenbucket.Policy,
	_ guard.Version,
	_ time.Time,
	cost int64,
) (tokenbucket.Decision, error) {
	b.mu.Lock()
	b.granted += cost
	granted := b.granted
	b.mu.Unlock()
	return tokenbucket.Decision{
		Allowed:   true,
		Remaining: policy.Burst - granted,
	}, nil
}

func (*accountingGuardedBucket) ReadPolicyGuard(context.Context) (guard.State, error) {
	return guard.State{
		Phase:   guard.PhaseStable,
		Version: guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
	}, nil
}

func (*accountingGuardedBucket) Close() error { return nil }

func (b *accountingGuardedBucket) grantedTotal() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.granted
}

type staticAtomicMarker struct{}

func (staticAtomicMarker) RedisKey(string) (string, error) {
	return "test:team-admission", nil
}
func (staticAtomicMarker) Recover(context.Context, string) error { return nil }
func (staticAtomicMarker) Disabled(context.Context, string) (bool, error) {
	return false, nil
}
func (staticAtomicMarker) Disable(context.Context, string) error { return nil }
func (staticAtomicMarker) Forget(context.Context, string) error  { return nil }
func (staticAtomicMarker) Close() error                          { return nil }

type fakeAdmissionMarker struct {
	disabled       bool
	err            error
	keyCalls       int
	recoverCalls   int
	disableCalls   int
	forgetCalls    int
	disabledTeamID string
}

func (m *fakeAdmissionMarker) Disabled(context.Context, string) (bool, error) {
	return m.disabled, m.err
}

func (m *fakeAdmissionMarker) RedisKey(teamID string) (string, error) {
	m.keyCalls++
	if m.err != nil {
		return "", m.err
	}
	return "test:team-admission:" + teamID, nil
}

func (m *fakeAdmissionMarker) Recover(context.Context, string) error {
	m.recoverCalls++
	return m.err
}

func (m *fakeAdmissionMarker) Disable(_ context.Context, teamID string) error {
	m.disableCalls++
	m.disabledTeamID = teamID
	if m.err == nil {
		m.disabled = true
	}
	return m.err
}

func (m *fakeAdmissionMarker) Forget(context.Context, string) error {
	m.forgetCalls++
	return m.err
}

func (*fakeAdmissionMarker) Close() error { return nil }

func newRedisRateLimiter(
	t *testing.T,
	redisServer *miniredis.Miniredis,
	prefix string,
	resolver PolicyResolver,
) *Limiter {
	return newRedisRateLimiterWithClock(t, redisServer, prefix, resolver, nil)
}

func newRedisRateLimiterWithClock(
	t *testing.T,
	redisServer *miniredis.Miniredis,
	prefix string,
	resolver PolicyResolver,
	clock *manualClock,
) *Limiter {
	t.Helper()
	bucket, err := tokenbucket.NewRedisBucket(context.Background(), tokenbucket.RedisConfig{
		URL:       "redis://" + redisServer.Addr() + "/0",
		KeyPrefix: prefix,
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisBucket() error = %v", err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	redisServer.Set("test:team-admission:team-a", "active")
	cfg := Config{
		RegionID:       "region-a",
		PolicyCacheTTL: time.Hour,
	}
	if clock != nil {
		cfg.Now = clock.Now
	}
	limiter, err := NewLimiter(
		resolver,
		&fakeAdmissionMarker{},
		bucket,
		cfg,
	)
	if err != nil {
		t.Fatalf("NewLimiter() error = %v", err)
	}
	return limiter
}

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(delay time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delay)
	c.mu.Unlock()
}

func setRateTestGuard(
	t *testing.T,
	redisServer *miniredis.Miniredis,
	prefix string,
	state guard.State,
) {
	t.Helper()
	redisServer.Server().SetPreHook(func(
		peer *miniredisserver.Peer,
		command string,
		_ ...string,
	) bool {
		if command != "INFO" {
			return false
		}
		peer.WriteBulk(
			"# Server\r\nrun_id:" + testRedisRunID +
				"\r\n# Memory\r\nmaxmemory_policy:noeviction" +
				"\r\n# Stats\r\nevicted_keys:0\r\n",
		)
		return true
	})
	if state.RedisRunID == "" {
		state.RedisRunID = testRedisRunID
	}
	fields, err := guard.Fields(state)
	if err != nil {
		t.Fatalf("encode policy guard: %v", err)
	}
	key := guard.Key(prefix)
	redisServer.Del(key)
	for field, value := range fields {
		redisServer.HSet(key, field, fmt.Sprint(value))
	}
}

func equalCosts(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
