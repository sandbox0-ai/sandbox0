package tokenbucket

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

const testRedisRunID = "tokenbucket-test-run-id"

func TestRedisBucketTakeNBurstAndRetry(t *testing.T) {
	bucket, redisServer := newTestRedisBucket(t)
	now := time.Unix(1_700_000_000, 0)
	redisServer.SetTime(now)
	policy := Policy{
		Tokens:   10,
		Interval: time.Second,
		Burst:    10,
		Revision: 1,
	}

	decision, err := bucket.TakeN(context.Background(), "region:r1:team:t1:api_requests", policy, 7)
	if err != nil {
		t.Fatalf("TakeN() error = %v", err)
	}
	if !decision.Allowed || decision.Remaining != 3 || decision.RetryAfter != 0 {
		t.Fatalf("TakeN() decision = %+v, want allowed with 3 remaining", decision)
	}

	decision, err = bucket.TakeN(context.Background(), "region:r1:team:t1:api_requests", policy, 4)
	if err != nil {
		t.Fatalf("TakeN() error = %v", err)
	}
	if decision.Allowed || decision.Remaining != 3 {
		t.Fatalf("TakeN() decision = %+v, want denied with 3 remaining", decision)
	}
	if decision.RetryAfter < 99*time.Millisecond || decision.RetryAfter > 101*time.Millisecond {
		t.Fatalf("TakeN() retry after = %s, want about 100ms", decision.RetryAfter)
	}

	redisServer.SetTime(now.Add(100 * time.Millisecond))
	decision, err = bucket.TakeN(context.Background(), "region:r1:team:t1:api_requests", policy, 4)
	if err != nil {
		t.Fatalf("TakeN() after refill error = %v", err)
	}
	if !decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("TakeN() after refill = %+v, want allowed with 0 remaining", decision)
	}
}

func TestRedisBucketDeniedTakeDoesNotReserveFutureTokens(t *testing.T) {
	bucket, _ := newTestRedisBucket(t)
	policy := Policy{Tokens: 1, Interval: time.Second, Burst: 1, Revision: 1}
	key := "region:r1:team:t1:api_requests"

	if decision, err := bucket.TakeN(context.Background(), key, policy, 1); err != nil || !decision.Allowed {
		t.Fatalf("initial TakeN() = %+v, %v", decision, err)
	}
	first, err := bucket.TakeN(context.Background(), key, policy, 1)
	if err != nil {
		t.Fatalf("first denied TakeN() error = %v", err)
	}
	second, err := bucket.TakeN(context.Background(), key, policy, 1)
	if err != nil {
		t.Fatalf("second denied TakeN() error = %v", err)
	}
	if first.Allowed || second.Allowed {
		t.Fatalf("denied decisions = %+v, %+v", first, second)
	}
	if second.RetryAfter > first.RetryAfter {
		t.Fatalf("second retry after = %s, first = %s; denied take reserved future capacity", second.RetryAfter, first.RetryAfter)
	}
}

func TestRedisBucketPolicyRevisionClampsWithoutRefilling(t *testing.T) {
	bucket, _ := newTestRedisBucket(t)
	key := "region:r1:team:t1:api_requests"
	original := Policy{Tokens: 1, Interval: time.Hour, Burst: 10, Revision: 1}

	decision, err := bucket.TakeN(context.Background(), key, original, 9)
	if err != nil || !decision.Allowed || decision.Remaining != 1 {
		t.Fatalf("original TakeN() = %+v, %v", decision, err)
	}

	increased := Policy{Tokens: 2, Interval: time.Hour, Burst: 20, Revision: 2}
	decision, err = bucket.TakeN(context.Background(), key, increased, 2)
	if err != nil {
		t.Fatalf("increased policy TakeN() error = %v", err)
	}
	if decision.Allowed || decision.Remaining != 1 {
		t.Fatalf("increased policy TakeN() = %+v, want denied without a free refill", decision)
	}

	reduced := Policy{Tokens: 1, Interval: time.Hour, Burst: 1, Revision: 3}
	decision, err = bucket.TakeN(context.Background(), key, reduced, 1)
	if err != nil || !decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("reduced policy TakeN() = %+v, %v, want clamped one-token allowance", decision, err)
	}
}

func TestRedisBucketTakeNIsAtomicAcrossClients(t *testing.T) {
	redisServer := miniredis.RunT(t)
	first := newRedisBucketForAddress(t, redisServer.Addr())
	second := newRedisBucketForAddress(t, redisServer.Addr())
	policy := Policy{Tokens: 1, Interval: time.Hour, Burst: 100, Revision: 1}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bucket := first
			if i%2 == 1 {
				bucket = second
			}
			decision, err := bucket.TakeN(context.Background(), "shared", policy, 3)
			if err != nil {
				t.Errorf("TakeN() error = %v", err)
				return
			}
			if decision.Allowed {
				allowed.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if got := allowed.Load(); got != 33 {
		t.Fatalf("allowed calls = %d, want 33", got)
	}
}

func TestRedisBucketTakeNGuardedRejectsBeforeMutation(t *testing.T) {
	bucket, redisServer := newTestRedisBucket(t)
	now := time.Unix(1_700_000_000, 0)
	redisServer.SetTime(now)
	version := guard.Version{EnforcementEpoch: 3, RedisGeneration: 2}
	seedGuard(t, bucket, guard.State{Phase: guard.PhaseStable, Version: version})
	policy := Policy{Tokens: 1, Interval: time.Second, Burst: 10, Revision: 4}
	const key = "region:r1:team:t1:api_requests"
	const admissionKey = "test:team-admission:t1"
	redisServer.Set(admissionKey, "active")

	decision, err := bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		version,
		time.Time{},
		1,
	)
	if err != nil || !decision.Allowed || decision.Remaining != 9 {
		t.Fatalf("initial TakeNGuarded() = (%+v, %v), want allowed with 9 remaining", decision, err)
	}
	bucketKey := bucket.redisGuardedKey(key)
	before, err := bucket.client.HGetAll(context.Background(), bucketKey).Result()
	if err != nil {
		t.Fatalf("read guarded bucket before rejection: %v", err)
	}

	redisServer.Del(admissionKey)
	if _, err := bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		version,
		time.Time{},
		1,
	); !errors.Is(err, ErrAdmissionMissing) {
		t.Fatalf("missing-admission TakeNGuarded() error = %v, want %v", err, ErrAdmissionMissing)
	}
	redisServer.Set(admissionKey, "disabled")
	if _, err := bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		version,
		time.Time{},
		1,
	); !errors.Is(err, ErrAdmissionDisabled) {
		t.Fatalf("disabled-admission TakeNGuarded() error = %v, want %v", err, ErrAdmissionDisabled)
	}
	redisServer.Set(admissionKey, "corrupt")
	if _, err := bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		version,
		time.Time{},
		1,
	); !errors.Is(err, ErrAdmissionCorrupt) {
		t.Fatalf("corrupt-admission TakeNGuarded() error = %v, want %v", err, ErrAdmissionCorrupt)
	}
	afterAdmissionRejections, err := bucket.client.HGetAll(context.Background(), bucketKey).Result()
	if err != nil {
		t.Fatalf("read guarded bucket after admission rejection: %v", err)
	}
	if !reflect.DeepEqual(afterAdmissionRejections, before) {
		t.Fatalf(
			"admission rejection mutated bucket: before %#v after %#v",
			before,
			afterAdmissionRejections,
		)
	}
	redisServer.Set(admissionKey, "active")

	if err := bucket.client.HSet(
		context.Background(),
		bucket.guardKey,
		"phase",
		guard.PhasePending,
		"pending_token",
		"mutation",
	).Err(); err != nil {
		t.Fatalf("set guard pending: %v", err)
	}
	redisServer.SetTime(now.Add(time.Second))
	if _, err := bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		version,
		time.Time{},
		1,
	); !errors.Is(err, guard.ErrPending) {
		t.Fatalf("pending TakeNGuarded() error = %v, want %v", err, guard.ErrPending)
	}
	after, err := bucket.client.HGetAll(context.Background(), bucketKey).Result()
	if err != nil {
		t.Fatalf("read guarded bucket after rejection: %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("guard rejection mutated bucket: before %#v after %#v", before, after)
	}

	seedGuard(t, bucket, guard.State{
		Phase: guard.PhaseStable,
		Version: guard.Version{
			EnforcementEpoch: version.EnforcementEpoch + 1,
			RedisGeneration:  version.RedisGeneration,
		},
	})
	if _, err := bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		version,
		time.Time{},
		1,
	); !errors.Is(err, guard.ErrStale) {
		t.Fatalf("stale TakeNGuarded() error = %v, want %v", err, guard.ErrStale)
	}
	after, err = bucket.client.HGetAll(context.Background(), bucketKey).Result()
	if err != nil {
		t.Fatalf("read guarded bucket after stale rejection: %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("stale guard rejection mutated bucket: before %#v after %#v", before, after)
	}

	if err := bucket.client.Del(context.Background(), bucket.guardKey).Err(); err != nil {
		t.Fatalf("delete guard: %v", err)
	}
	if _, err := bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		version,
		time.Time{},
		1,
	); !errors.Is(err, guard.ErrMissing) {
		t.Fatalf("missing-guard TakeNGuarded() error = %v, want %v", err, guard.ErrMissing)
	}
	after, err = bucket.client.HGetAll(context.Background(), bucketKey).Result()
	if err != nil {
		t.Fatalf("read guarded bucket after missing rejection: %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("missing guard rejection mutated bucket: before %#v after %#v", before, after)
	}
}

func TestRedisBucketTakeNGuardedStartsNewGenerationFromZero(t *testing.T) {
	bucket, redisServer := newTestRedisBucket(t)
	now := time.Unix(1_700_000_000, 0)
	redisServer.SetTime(now)
	firstVersion := guard.Version{EnforcementEpoch: 1, RedisGeneration: 1}
	seedGuard(t, bucket, guard.State{Phase: guard.PhaseStable, Version: firstVersion})
	policy := Policy{Tokens: 1, Interval: time.Second, Burst: 10, Revision: 1}
	const key = "region:r1:team:t1:api_requests"
	const admissionKey = "test:team-admission:t1"
	redisServer.Set(admissionKey, "active")

	decision, err := bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		firstVersion,
		time.Time{},
		1,
	)
	if err != nil || !decision.Allowed || decision.Remaining != 9 {
		t.Fatalf("first generation TakeNGuarded() = (%+v, %v)", decision, err)
	}

	secondVersion := guard.Version{EnforcementEpoch: 1, RedisGeneration: 2}
	seedGuard(t, bucket, guard.State{
		Phase:          guard.PhaseStable,
		Version:        secondVersion,
		RateRefillFrom: now,
	})
	decision, err = bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		secondVersion,
		now,
		1,
	)
	if err != nil {
		t.Fatalf("new generation TakeNGuarded() error = %v", err)
	}
	if decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("new generation TakeNGuarded() = %+v, want denied from zero", decision)
	}

	redisServer.SetTime(now.Add(time.Second))
	decision, err = bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		secondVersion,
		now,
		1,
	)
	if err != nil || !decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("refilled generation TakeNGuarded() = (%+v, %v), want one token", decision, err)
	}
}

func TestRedisBucketTakeNGuardedFencesEvictedStateBeforeMutation(t *testing.T) {
	bucket, redisServer := newTestRedisBucket(t)
	version := guard.Version{EnforcementEpoch: 1, RedisGeneration: 1}
	seedGuard(t, bucket, guard.State{
		Phase:      guard.PhaseStable,
		Version:    version,
		RedisRunID: testRedisRunID,
	})
	const (
		key          = "region:r1:team:t1:api_requests"
		admissionKey = "test:team-admission:t1"
	)
	redisServer.Set(admissionKey, "active")
	policy := Policy{Tokens: 1, Interval: time.Hour, Burst: 2, Revision: 1}
	if decision, err := bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		version,
		time.Time{},
		1,
	); err != nil || !decision.Allowed {
		t.Fatalf("initial TakeNGuarded() = (%+v, %v)", decision, err)
	}
	bucketKey := bucket.redisGuardedKey(key)
	before, err := bucket.client.HGetAll(context.Background(), bucketKey).Result()
	if err != nil {
		t.Fatalf("read guarded bucket before eviction fence: %v", err)
	}

	setTestRedisRuntimeSafety(redisServer, testRedisRunID, "noeviction", 1)
	if _, err := bucket.TakeNGuarded(
		context.Background(),
		key,
		admissionKey,
		policy,
		version,
		time.Time{},
		1,
	); !errors.Is(err, guard.ErrCorrupt) {
		t.Fatalf("evicted-state TakeNGuarded() error = %v, want %v", err, guard.ErrCorrupt)
	}
	after, err := bucket.client.HGetAll(context.Background(), bucketKey).Result()
	if err != nil {
		t.Fatalf("read guarded bucket after eviction fence: %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("eviction fence mutated bucket: before %#v after %#v", before, after)
	}
}

func TestRedisBucketSeparatesKeys(t *testing.T) {
	bucket, _ := newTestRedisBucket(t)
	policy := Policy{Tokens: 1, Interval: time.Hour, Burst: 1, Revision: 1}
	for _, key := range []string{
		"region:r1:team:t1:api_requests",
		"region:r2:team:t1:api_requests",
		"region:r1:team:t2:api_requests",
		"region:r1:team:t1:network_egress_bytes",
	} {
		decision, err := bucket.TakeN(context.Background(), key, policy, 1)
		if err != nil || !decision.Allowed {
			t.Fatalf("TakeN(%q) = %+v, %v, want allowed", key, decision, err)
		}
	}
}

func TestRedisBucketRejectsInvalidInputWithoutCreatingState(t *testing.T) {
	bucket, redisServer := newTestRedisBucket(t)
	tests := []struct {
		name   string
		key    string
		policy Policy
		tokens int64
		target error
	}{
		{name: "empty key", policy: Policy{Tokens: 1, Interval: time.Second, Burst: 1}, tokens: 1, target: ErrInvalidKey},
		{name: "zero rate", key: "key", policy: Policy{Interval: time.Second, Burst: 1}, tokens: 1, target: ErrInvalidPolicy},
		{name: "zero interval", key: "key", policy: Policy{Tokens: 1, Burst: 1}, tokens: 1, target: ErrInvalidPolicy},
		{name: "sub-microsecond interval", key: "key", policy: Policy{Tokens: 1, Interval: 1500 * time.Nanosecond, Burst: 1}, tokens: 1, target: ErrInvalidPolicy},
		{name: "negative revision", key: "key", policy: Policy{Tokens: 1, Interval: time.Second, Burst: 1, Revision: -1}, tokens: 1, target: ErrInvalidPolicy},
		{name: "zero cost", key: "key", policy: Policy{Tokens: 1, Interval: time.Second, Burst: 1}, target: ErrInvalidTokenCost},
		{name: "cost exceeds burst", key: "key", policy: Policy{Tokens: 1, Interval: time.Second, Burst: 1}, tokens: 2, target: ErrCostExceedsBurst},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := bucket.TakeN(context.Background(), tt.key, tt.policy, tt.tokens)
			if !errors.Is(err, tt.target) {
				t.Fatalf("TakeN() error = %v, want %v", err, tt.target)
			}
		})
	}
	if got := len(redisServer.Keys()); got != 0 {
		t.Fatalf("Redis keys = %d, want 0 after rejected inputs", got)
	}
}

func TestRedisBucketCloseIsIdempotent(t *testing.T) {
	bucket, _ := newTestRedisBucket(t)
	if err := bucket.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := bucket.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	_, err := bucket.TakeN(context.Background(), "key", Policy{
		Tokens:   1,
		Interval: time.Second,
		Burst:    1,
	}, 1)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("TakeN() after close error = %v, want %v", err, ErrClosed)
	}
}

func TestRedisBucketTTL(t *testing.T) {
	short := Policy{Tokens: 100, Interval: time.Second, Burst: 200}
	if got := redisBucketTTL(short); got != time.Minute {
		t.Fatalf("short TTL = %s, want 1m", got)
	}
	long := Policy{Tokens: 1, Interval: time.Minute, Burst: 2}
	if got := redisBucketTTL(long); got != 4*time.Minute {
		t.Fatalf("long TTL = %s, want 4m", got)
	}
}

func newTestRedisBucket(t *testing.T) (*RedisBucket, *miniredis.Miniredis) {
	t.Helper()
	redisServer := miniredis.RunT(t)
	setTestRedisRuntimeSafety(redisServer, testRedisRunID, "noeviction", 0)
	return newRedisBucketForAddress(t, redisServer.Addr()), redisServer
}

func newRedisBucketForAddress(t *testing.T, address string) *RedisBucket {
	t.Helper()
	bucket, err := NewRedisBucket(context.Background(), RedisConfig{
		URL:       "redis://" + address + "/0",
		KeyPrefix: "test:tokenbucket",
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisBucket() error = %v", err)
	}
	t.Cleanup(func() {
		_ = bucket.Close()
	})
	return bucket
}

func seedGuard(t *testing.T, bucket *RedisBucket, state guard.State) {
	t.Helper()
	if state.RedisRunID == "" {
		state.RedisRunID = testRedisRunID
	}
	fields, err := guard.Fields(state)
	if err != nil {
		t.Fatalf("encode policy guard: %v", err)
	}
	if err := bucket.client.Del(context.Background(), bucket.guardKey).Err(); err != nil {
		t.Fatalf("clear policy guard: %v", err)
	}
	if err := bucket.client.HSet(context.Background(), bucket.guardKey, fields).Err(); err != nil {
		t.Fatalf("seed policy guard: %v", err)
	}
}

func setTestRedisRuntimeSafety(
	redisServer *miniredis.Miniredis,
	runID string,
	policy string,
	evictedKeys int64,
) {
	redisServer.Server().SetPreHook(func(
		peer *miniredisserver.Peer,
		command string,
		_ ...string,
	) bool {
		if command != "INFO" {
			return false
		}
		peer.WriteBulk(
			"# Server\r\nrun_id:" + runID +
				"\r\n# Memory\r\nmaxmemory_policy:" + policy +
				"\r\n# Stats\r\nevicted_keys:" +
				strconv.FormatInt(evictedKeys, 10) + "\r\n",
		)
		return true
	})
}
