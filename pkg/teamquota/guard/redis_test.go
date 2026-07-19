package guard

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"
)

const testRedisRunID = "guard-test-run-id"

func TestRedisTransitionsAreExactAndNonExpiring(t *testing.T) {
	redisServer := miniredis.RunT(t)
	policyGuard := newTestRedisGuard(t, redisServer)
	ctx := context.Background()
	version := Version{EnforcementEpoch: 4, RedisGeneration: 2}
	initial := State{
		Phase:      PhaseStable,
		Version:    version,
		RedisRunID: testRedisRunID,
	}

	if err := policyGuard.Force(ctx, initial); err != nil {
		t.Fatalf("Force() error = %v", err)
	}
	if ttl := redisServer.TTL(policyGuard.key); ttl != 0 {
		t.Fatalf("Force() guard TTL = %s, want no expiry", ttl)
	}
	if err := policyGuard.SetPending(
		ctx,
		Version{EnforcementEpoch: 3, RedisGeneration: 2},
		"stale",
		time.Time{},
	); !errors.Is(err, ErrStale) {
		t.Fatalf("SetPending(stale) error = %v, want %v", err, ErrStale)
	}
	state, err := policyGuard.ReadPolicyGuard(ctx)
	if err != nil || state.Phase != PhaseStable || !state.Version.Equal(version) {
		t.Fatalf("guard after stale transition = (%+v, %v), want unchanged stable", state, err)
	}

	redisServer.SetTTL(policyGuard.key, time.Minute)
	quarantineUntil := time.Unix(1_700_000_005, 0).UTC()
	if err := policyGuard.SetPending(ctx, version, "mutation", quarantineUntil); err != nil {
		t.Fatalf("SetPending() error = %v", err)
	}
	if ttl := redisServer.TTL(policyGuard.key); ttl != 0 {
		t.Fatalf("SetPending() guard TTL = %s, want no expiry", ttl)
	}
	state, err = policyGuard.ReadPolicyGuard(ctx)
	if err != nil {
		t.Fatalf("ReadPolicyGuard(pending) error = %v", err)
	}
	if state.Phase != PhasePending ||
		state.PendingToken != "mutation" ||
		!state.QuarantineUntil.Equal(quarantineUntil) {
		t.Fatalf("pending guard = %+v", state)
	}

	target := State{
		Phase:      PhaseStable,
		Version:    Version{EnforcementEpoch: 5, RedisGeneration: 2},
		RedisRunID: testRedisRunID,
	}
	if err := policyGuard.SetStable(ctx, "other-writer", target); !errors.Is(err, ErrPending) {
		t.Fatalf("SetStable(wrong token) error = %v, want %v", err, ErrPending)
	}
	state, err = policyGuard.ReadPolicyGuard(ctx)
	if err != nil || state.PendingToken != "mutation" || state.Phase != PhasePending {
		t.Fatalf("guard after wrong publication = (%+v, %v), want original pending", state, err)
	}
	if err := policyGuard.SetStable(ctx, "mutation", target); err != nil {
		t.Fatalf("SetStable() error = %v", err)
	}
	if ttl := redisServer.TTL(policyGuard.key); ttl != 0 {
		t.Fatalf("SetStable() guard TTL = %s, want no expiry", ttl)
	}
	state, err = policyGuard.ReadPolicyGuard(ctx)
	if err != nil ||
		state.Phase != PhaseStable ||
		!state.Version.Equal(target.Version) ||
		state.PendingToken != "" {
		t.Fatalf("published guard = (%+v, %v)", state, err)
	}
}

func TestRedisTransitionsFailClosedWhenGuardIsMissing(t *testing.T) {
	redisServer := miniredis.RunT(t)
	policyGuard := newTestRedisGuard(t, redisServer)
	version := Version{EnforcementEpoch: 1, RedisGeneration: 1}

	if err := policyGuard.SetPending(
		context.Background(),
		version,
		"mutation",
		time.Time{},
	); !errors.Is(err, ErrMissing) {
		t.Fatalf("SetPending() error = %v, want %v", err, ErrMissing)
	}
	if err := policyGuard.SetStable(
		context.Background(),
		"mutation",
		State{Phase: PhaseStable, Version: version, RedisRunID: testRedisRunID},
	); !errors.Is(err, ErrMissing) {
		t.Fatalf("SetStable() error = %v, want %v", err, ErrMissing)
	}
	if redisServer.Exists(policyGuard.key) {
		t.Fatalf("failed transitions created guard key %q", policyGuard.key)
	}
}

func TestRedisInvalidateRemovesStableGuard(t *testing.T) {
	redisServer := miniredis.RunT(t)
	policyGuard := newTestRedisGuard(t, redisServer)
	state := State{
		Phase:      PhaseStable,
		Version:    Version{EnforcementEpoch: 1, RedisGeneration: 1},
		RedisRunID: testRedisRunID,
	}
	if err := policyGuard.Force(context.Background(), state); err != nil {
		t.Fatalf("Force() error = %v", err)
	}
	if err := policyGuard.Invalidate(context.Background()); err != nil {
		t.Fatalf("Invalidate() error = %v", err)
	}
	if _, err := policyGuard.ReadPolicyGuard(context.Background()); !errors.Is(err, ErrMissing) {
		t.Fatalf("ReadPolicyGuard() error = %v, want %v", err, ErrMissing)
	}
}

func TestRedisTransitionsFenceRuntimeSafetyBeforeMutation(t *testing.T) {
	tests := []struct {
		name        string
		runID       string
		policy      string
		evictedKeys int64
		want        error
	}{
		{
			name:        "server incarnation changed",
			runID:       "replacement-run-id",
			policy:      "noeviction",
			evictedKeys: 0,
			want:        ErrStale,
		},
		{
			name:        "eviction policy unsafe",
			runID:       testRedisRunID,
			policy:      "volatile-lru",
			evictedKeys: 0,
			want:        ErrCorrupt,
		},
		{
			name:        "state was evicted before policy recovered",
			runID:       testRedisRunID,
			policy:      "noeviction",
			evictedKeys: 1,
			want:        ErrCorrupt,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			redisServer := miniredis.RunT(t)
			policyGuard := newTestRedisGuard(t, redisServer)
			state := State{
				Phase:      PhaseStable,
				Version:    Version{EnforcementEpoch: 1, RedisGeneration: 1},
				RedisRunID: testRedisRunID,
			}
			if err := policyGuard.Force(context.Background(), state); err != nil {
				t.Fatalf("Force() error = %v", err)
			}

			setTestRedisRuntimeSafety(
				redisServer,
				test.runID,
				test.policy,
				test.evictedKeys,
			)
			err := policyGuard.SetPending(
				context.Background(),
				state.Version,
				"mutation",
				time.Time{},
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("SetPending() error = %v, want %v", err, test.want)
			}
			after, readErr := policyGuard.ReadPolicyGuard(context.Background())
			if readErr != nil || !after.Stable() {
				t.Fatalf("guard after rejected transition = (%+v, %v)", after, readErr)
			}
		})
	}
}

func newTestRedisGuard(t *testing.T, redisServer *miniredis.Miniredis) *Redis {
	t.Helper()
	setTestRedisRuntimeSafety(redisServer, testRedisRunID, "noeviction", 0)
	policyGuard, err := NewRedis(context.Background(), Config{
		URL:       "redis://" + redisServer.Addr() + "/0",
		KeyPrefix: "test:teamquota",
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedis() error = %v", err)
	}
	t.Cleanup(func() {
		_ = policyGuard.Close()
	})
	return policyGuard
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
