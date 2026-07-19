package concurrency

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

var testGuardVersion = guard.Version{EnforcementEpoch: 1, RedisGeneration: 1}

const (
	testAdmissionKey = "test:teamquota:admission:team-a"
	testRedisRunID   = "concurrency-test-run-id"
)

func TestRedisStoreEnforcesExactLimitAcrossClientsAndRelease(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisServer.SetTime(time.Unix(1_700_000_000, 0))
	first := newTestRedisStore(t, redisServer)
	second := newTestRedisStore(t, redisServer)
	ctx := context.Background()
	const (
		key      = "region-team-active-connections"
		leaseTTL = time.Second
	)

	assertAcquireDecision(t, first, ctx, key, "lease-a", 2, leaseTTL, true, 1)
	assertAcquireDecision(t, second, ctx, key, "lease-b", 2, leaseTTL, true, 2)
	assertAcquireDecision(t, first, ctx, key, "lease-c", 2, leaseTTL, false, 2)

	if err := second.Release(ctx, key, "lease-a"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	assertAcquireDecision(t, first, ctx, key, "lease-c", 2, leaseTTL, true, 2)
	if used, err := second.Usage(ctx, key, leaseTTL); err != nil || used != 2 {
		t.Fatalf("Usage() = (%d, %v), want (2, nil)", used, err)
	}
}

func TestRedisStoreUsesRedisTimeToExpireCrashedLease(t *testing.T) {
	redisServer := miniredis.RunT(t)
	now := time.Unix(1_700_000_000, 0)
	redisServer.SetTime(now)
	store := newTestRedisStore(t, redisServer)
	ctx := context.Background()
	const key = "crashed-lease"

	assertAcquireDecision(t, store, ctx, key, "lease-a", 1, time.Second, true, 1)
	redisServer.SetTime(now.Add(time.Second + time.Millisecond))

	if used, err := store.Usage(ctx, key, time.Second); err != nil || used != 0 {
		t.Fatalf("Usage() after Redis time advance = (%d, %v), want (0, nil)", used, err)
	}
	assertAcquireDecision(t, store, ctx, key, "lease-b", 1, time.Second, true, 1)
}

func TestRedisStorePolicyLoweringKeepsStableLexicalWinners(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisServer.SetTime(time.Unix(1_700_000_000, 0))
	store := newTestRedisStore(t, redisServer)
	ctx := context.Background()
	const key = "lowered-policy"

	// Acquisition order deliberately differs from lexical order. The order
	// ZSET score is constant, so Redis member ordering is the stable tie-break.
	assertAcquireDecision(t, store, ctx, key, "lease-z", 2, time.Second, true, 1)
	assertAcquireDecision(t, store, ctx, key, "lease-a", 2, time.Second, true, 2)

	if decision, err := store.Renew(ctx, key, "lease-z", testAdmissionKey, 1, time.Second, testGuardVersion); err != nil || decision != renewOverLimit {
		t.Fatalf("Renew(lease-z) = (%v, %v), want (over limit, nil)", decision, err)
	}
	if decision, err := store.Renew(ctx, key, "lease-a", testAdmissionKey, 1, time.Second, testGuardVersion); err != nil || decision != renewed {
		t.Fatalf("Renew(lease-a) = (%v, %v), want (renewed, nil)", decision, err)
	}
	if used, err := store.Usage(ctx, key, time.Second); err != nil || used != 1 {
		t.Fatalf("Usage() = (%d, %v), want (1, nil)", used, err)
	}
}

func TestRedisStoreGuardRejectsAcquireAndRenewBeforeMutationButAllowsRelease(t *testing.T) {
	redisServer := miniredis.RunT(t)
	now := time.Unix(1_700_000_000, 0)
	redisServer.SetTime(now)
	store := newTestRedisStore(t, redisServer)
	ctx := context.Background()
	const key = "guarded-leases"

	assertAcquireDecision(t, store, ctx, key, "lease-a", 2, time.Second, true, 1)
	keys := store.redisStateKeys(key)
	expiryBefore, err := redisServer.ZScore(keys[0], "lease-a")
	if err != nil {
		t.Fatalf("read initial lease expiry: %v", err)
	}
	setTestGuard(t, redisServer, guard.State{
		Phase:           guard.PhasePending,
		Version:         testGuardVersion,
		PendingToken:    "mutation",
		QuarantineUntil: now.Add(time.Second),
	})
	redisServer.SetTime(now.Add(500 * time.Millisecond))

	if _, err := store.Acquire(
		ctx,
		key,
		"lease-b",
		testAdmissionKey,
		2,
		time.Second,
		testGuardVersion,
	); !errors.Is(err, guard.ErrPending) {
		t.Fatalf("pending Acquire() error = %v, want %v", err, guard.ErrPending)
	}
	if decision, err := store.Renew(
		ctx,
		key,
		"lease-a",
		testAdmissionKey,
		2,
		time.Second,
		testGuardVersion,
	); !errors.Is(err, guard.ErrPending) || decision != renewLost {
		t.Fatalf("pending Renew() = (%v, %v), want (lost, pending)", decision, err)
	}
	expiryAfter, err := redisServer.ZScore(keys[0], "lease-a")
	if err != nil {
		t.Fatalf("read rejected-renewal expiry: %v", err)
	}
	if expiryAfter != expiryBefore {
		t.Fatalf("pending guard changed lease expiry from %v to %v", expiryBefore, expiryAfter)
	}
	members, err := redisServer.ZMembers(keys[1])
	if err != nil {
		t.Fatalf("read members after pending acquire: %v", err)
	}
	if len(members) != 1 || members[0] != "lease-a" {
		t.Fatalf("pending acquire mutated members to %#v", members)
	}

	if err := store.Release(ctx, key, "lease-a"); err != nil {
		t.Fatalf("Release() while guard pending error = %v", err)
	}
	if used, err := store.Usage(ctx, key, time.Second); err != nil || used != 0 {
		t.Fatalf("Usage() after pending release = (%d, %v), want (0, nil)", used, err)
	}
}

func TestRedisStoreEvictionFenceRejectsAcquireAndRenewBeforeMutation(t *testing.T) {
	redisServer := miniredis.RunT(t)
	now := time.Unix(1_700_000_000, 0)
	redisServer.SetTime(now)
	store := newTestRedisStore(t, redisServer)
	ctx := context.Background()
	const key = "eviction-fenced-leases"

	assertAcquireDecision(t, store, ctx, key, "lease-a", 2, time.Second, true, 1)
	keys := store.redisStateKeys(key)
	expiryBefore, err := redisServer.ZScore(keys[0], "lease-a")
	if err != nil {
		t.Fatalf("read initial lease expiry: %v", err)
	}
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
				"\r\n# Stats\r\nevicted_keys:1\r\n",
		)
		return true
	})
	redisServer.SetTime(now.Add(500 * time.Millisecond))

	if _, err := store.Acquire(
		ctx,
		key,
		"lease-b",
		testAdmissionKey,
		2,
		time.Second,
		testGuardVersion,
	); !errors.Is(err, guard.ErrCorrupt) {
		t.Fatalf("eviction-fenced Acquire() error = %v, want %v", err, guard.ErrCorrupt)
	}
	if decision, err := store.Renew(
		ctx,
		key,
		"lease-a",
		testAdmissionKey,
		2,
		time.Second,
		testGuardVersion,
	); !errors.Is(err, guard.ErrCorrupt) || decision != renewLost {
		t.Fatalf("eviction-fenced Renew() = (%v, %v), want (lost, corrupt)", decision, err)
	}
	expiryAfter, err := redisServer.ZScore(keys[0], "lease-a")
	if err != nil {
		t.Fatalf("read rejected-renewal expiry: %v", err)
	}
	if expiryAfter != expiryBefore {
		t.Fatalf("eviction fence changed lease expiry from %v to %v", expiryBefore, expiryAfter)
	}
	members, err := redisServer.ZMembers(keys[1])
	if err != nil {
		t.Fatalf("read members after eviction-fenced acquire: %v", err)
	}
	if len(members) != 1 || members[0] != "lease-a" {
		t.Fatalf("eviction-fenced acquire mutated members to %#v", members)
	}
}

func TestRedisStoreAdmissionMarkerRejectsAcquireAndRenewBeforeMutation(t *testing.T) {
	redisServer := miniredis.RunT(t)
	now := time.Unix(1_700_000_000, 0)
	redisServer.SetTime(now)
	store := newTestRedisStore(t, redisServer)
	ctx := context.Background()
	const key = "admission-guarded-leases"

	assertAcquireDecision(t, store, ctx, key, "lease-a", 2, time.Second, true, 1)
	keys := store.redisStateKeys(key)
	expiryBefore, err := redisServer.ZScore(keys[0], "lease-a")
	if err != nil {
		t.Fatalf("read initial lease expiry: %v", err)
	}
	redisServer.Set(testAdmissionKey, "disabled")
	redisServer.SetTime(now.Add(500 * time.Millisecond))

	if _, err := store.Acquire(
		ctx,
		key,
		"lease-b",
		testAdmissionKey,
		2,
		time.Second,
		testGuardVersion,
	); !errors.Is(err, errAdmissionDisabled) {
		t.Fatalf("disabled Acquire() error = %v, want %v", err, errAdmissionDisabled)
	}
	if decision, err := store.Renew(
		ctx,
		key,
		"lease-a",
		testAdmissionKey,
		2,
		time.Second,
		testGuardVersion,
	); !errors.Is(err, errAdmissionDisabled) || decision != renewLost {
		t.Fatalf("disabled Renew() = (%v, %v), want (lost, disabled)", decision, err)
	}
	expiryAfter, err := redisServer.ZScore(keys[0], "lease-a")
	if err != nil {
		t.Fatalf("read rejected-renewal expiry: %v", err)
	}
	if expiryAfter != expiryBefore {
		t.Fatalf("disabled marker changed lease expiry from %v to %v", expiryBefore, expiryAfter)
	}
	members, err := redisServer.ZMembers(keys[1])
	if err != nil {
		t.Fatalf("read members after disabled acquire: %v", err)
	}
	if len(members) != 1 || members[0] != "lease-a" {
		t.Fatalf("disabled acquire mutated members to %#v", members)
	}

	redisServer.Del(testAdmissionKey)
	if _, err := store.Acquire(
		ctx,
		key,
		"lease-b",
		testAdmissionKey,
		2,
		time.Second,
		testGuardVersion,
	); !errors.Is(err, errAdmissionMissing) {
		t.Fatalf("missing-marker Acquire() error = %v, want %v", err, errAdmissionMissing)
	}
	redisServer.Set(testAdmissionKey, "unexpected")
	if _, err := store.Acquire(
		ctx,
		key,
		"lease-b",
		testAdmissionKey,
		2,
		time.Second,
		testGuardVersion,
	); !errors.Is(err, errAdmissionCorrupt) {
		t.Fatalf("corrupt-marker Acquire() error = %v, want %v", err, errAdmissionCorrupt)
	}
}

func TestRedisStoreStaleGuardDoesNotMutateAndGenerationChangeCleansLeases(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisServer.SetTime(time.Unix(1_700_000_000, 0))
	store := newTestRedisStore(t, redisServer)
	ctx := context.Background()
	const key = "generation-change"

	assertAcquireDecision(t, store, ctx, key, "old-lease", 2, time.Second, true, 1)
	keys := store.redisStateKeys(key)
	secondVersion := guard.Version{EnforcementEpoch: 2, RedisGeneration: 2}
	setTestGuard(t, redisServer, guard.State{
		Phase:   guard.PhaseStable,
		Version: secondVersion,
	})

	if _, err := store.Acquire(
		ctx,
		key,
		"rejected-lease",
		testAdmissionKey,
		2,
		time.Second,
		testGuardVersion,
	); !errors.Is(err, guard.ErrStale) {
		t.Fatalf("stale Acquire() error = %v, want %v", err, guard.ErrStale)
	}
	members, err := redisServer.ZMembers(keys[1])
	if err != nil {
		t.Fatalf("read members after stale acquire: %v", err)
	}
	if len(members) != 1 || members[0] != "old-lease" {
		t.Fatalf("stale acquire mutated members to %#v", members)
	}

	decision, err := store.Acquire(
		ctx,
		key,
		"new-lease",
		testAdmissionKey,
		2,
		time.Second,
		secondVersion,
	)
	if err != nil || !decision.allowed || decision.used != 1 {
		t.Fatalf("new generation Acquire() = (%+v, %v), want one clean lease", decision, err)
	}
	members, err = redisServer.ZMembers(keys[1])
	if err != nil {
		t.Fatalf("read members after generation reset: %v", err)
	}
	if len(members) != 1 || members[0] != "new-lease" {
		t.Fatalf("generation reset members = %#v, want only new-lease", members)
	}
	if decision, err := store.Renew(
		ctx,
		key,
		"old-lease",
		testAdmissionKey,
		2,
		time.Second,
		secondVersion,
	); err != nil || decision != renewLost {
		t.Fatalf("Renew(old lease after reset) = (%v, %v), want lost", decision, err)
	}
}

func TestRedisStoreRepairsSameCardinalityOrphans(t *testing.T) {
	redisServer := miniredis.RunT(t)
	now := time.Unix(1_700_000_000, 0)
	redisServer.SetTime(now)
	store := newTestRedisStore(t, redisServer)
	keys := store.redisStateKeys("orphaned")
	futureExpiry := float64(now.Add(time.Minute).UnixMilli())

	for _, member := range []string{"valid", "expiry-only"} {
		if _, err := redisServer.ZAdd(keys[0], futureExpiry, member); err != nil {
			t.Fatalf("seed expiry member %q: %v", member, err)
		}
	}
	for _, member := range []string{"valid", "order-only"} {
		if _, err := redisServer.ZAdd(keys[1], 0, member); err != nil {
			t.Fatalf("seed order member %q: %v", member, err)
		}
	}

	if used, err := store.Usage(context.Background(), "orphaned", time.Second); err != nil || used != 1 {
		t.Fatalf("Usage() = (%d, %v), want repaired usage 1", used, err)
	}
	expiryMembers, err := redisServer.ZMembers(keys[0])
	if err != nil {
		t.Fatalf("read expiry members: %v", err)
	}
	orderMembers, err := redisServer.ZMembers(keys[1])
	if err != nil {
		t.Fatalf("read order members: %v", err)
	}
	if len(expiryMembers) != 1 || expiryMembers[0] != "valid" ||
		len(orderMembers) != 1 || orderMembers[0] != "valid" {
		t.Fatalf("repaired members = expiry %#v order %#v, want only valid", expiryMembers, orderMembers)
	}
}

func TestRedisStoreUsesOneClusterHashTagAndRequiresRedis(t *testing.T) {
	if _, err := NewRedisStore(context.Background(), StoreConfig{}); err == nil {
		t.Fatal("NewRedisStore() error = nil, want missing Redis URL error")
	}

	redisServer := miniredis.RunT(t)
	store := newTestRedisStore(t, redisServer)
	keys := store.redisStateKeys("team-a")
	if len(keys) != 3 {
		t.Fatalf("redis keys = %#v, want three", keys)
	}
	firstStart, firstEnd := strings.IndexByte(keys[0], '{'), strings.IndexByte(keys[0], '}')
	if firstStart < 0 || firstEnd <= firstStart {
		t.Fatalf("first Redis key does not have a cluster hash tag: %#v", keys)
	}
	for _, key := range keys[1:] {
		start, end := strings.IndexByte(key, '{'), strings.IndexByte(key, '}')
		if start < 0 || end <= start || keys[0][firstStart:firstEnd+1] != key[start:end+1] {
			t.Fatalf("redis keys do not share one cluster hash tag: %#v", keys)
		}
	}
}

func newTestRedisStore(t *testing.T, redisServer *miniredis.Miniredis) *RedisStore {
	t.Helper()
	redisServer.Server().SetPreHook(func(
		peer *miniredisserver.Peer,
		command string,
		args ...string,
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
	store, err := NewRedisStore(context.Background(), StoreConfig{
		RedisURL:       "redis://" + redisServer.Addr() + "/0",
		RedisKeyPrefix: "test:teamquota",
		RedisTimeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisStore() error = %v", err)
	}
	setTestGuard(t, redisServer, guard.State{
		Phase:   guard.PhaseStable,
		Version: testGuardVersion,
	})
	redisServer.Set(testAdmissionKey, "active")
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func setTestGuard(t *testing.T, redisServer *miniredis.Miniredis, state guard.State) {
	t.Helper()
	if state.RedisRunID == "" {
		state.RedisRunID = testRedisRunID
	}
	fields, err := guard.Fields(state)
	if err != nil {
		t.Fatalf("encode policy guard: %v", err)
	}
	redisServer.Del(guard.Key("test:teamquota"))
	for field, value := range fields {
		redisServer.HSet(guard.Key("test:teamquota"), field, fmt.Sprint(value))
	}
}

func assertAcquireDecision(
	t *testing.T,
	store *RedisStore,
	ctx context.Context,
	key string,
	leaseID string,
	limit int64,
	leaseTTL time.Duration,
	wantAllowed bool,
	wantUsed int64,
) {
	t.Helper()
	decision, err := store.Acquire(
		ctx,
		key,
		leaseID,
		testAdmissionKey,
		limit,
		leaseTTL,
		testGuardVersion,
	)
	if err != nil {
		t.Fatalf("Acquire(%q) error = %v", leaseID, err)
	}
	if decision.allowed != wantAllowed || decision.used != wantUsed {
		t.Fatalf(
			"Acquire(%q) = {allowed:%t used:%d}, want {allowed:%t used:%d}",
			leaseID,
			decision.allowed,
			decision.used,
			wantAllowed,
			wantUsed,
		)
	}
}
