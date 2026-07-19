package distributed

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

var testAdmissionProofVersion = guard.Version{
	EnforcementEpoch: 1,
	RedisGeneration:  1,
}

const testAdmissionProofRedisRunID = "admission-proof-test-run-id"

func TestRedisAdmissionProofConsumerKeyIsHashedAndBounded(t *testing.T) {
	redisServer := miniredis.RunT(t)
	consumer := newTestAdmissionProofConsumer(t, redisServer)
	teamID := strings.Repeat("tenant-visible-", 10_000)
	proofID := strings.Repeat("proof-visible-", 10_000)
	key := consumer.redisKey(teamID, proofID)

	if strings.Contains(key, "tenant-visible") ||
		strings.Contains(key, "proof-visible") {
		t.Fatalf("proof replay key leaked unhashed identity: %q", key)
	}
	if got, want := len(key), len(consumer.proofPrefix)+1+64; got != want {
		t.Fatalf("proof replay key length = %d, want bounded length %d", got, want)
	}
}

func TestRedisAdmissionProofConsumerFirstUseWinsAndReplayIsRejected(t *testing.T) {
	redisServer := miniredis.RunT(t)
	consumer := newTestAdmissionProofConsumer(t, redisServer)
	now := time.Now().UTC()
	setTestProofGuard(t, consumer, now.Add(-time.Minute))

	trusted, err := consumer.Consume(
		context.Background(),
		"team-a",
		"proof-a",
		now.Add(-time.Second).UnixMilli(),
		now.Add(time.Minute).UnixMilli(),
		testAdmissionProofVersion,
	)
	if err != nil || !trusted {
		t.Fatalf("first Consume() = (%t, %v), want trusted", trusted, err)
	}
	trusted, err = consumer.Consume(
		context.Background(),
		"team-a",
		"proof-a",
		now.Add(-time.Second).UnixMilli(),
		now.Add(time.Minute).UnixMilli(),
		testAdmissionProofVersion,
	)
	if err != nil || trusted {
		t.Fatalf("replay Consume() = (%t, %v), want untrusted replay", trusted, err)
	}
	ttl := redisServer.TTL(consumer.redisKey("team-a", "proof-a"))
	if ttl <= 0 || ttl > time.Minute {
		t.Fatalf("proof replay-key TTL = %s, want positive and at most remaining validity", ttl)
	}
}

func TestRedisAdmissionProofConsumerAcceptsFreshBootstrapFenceAndRejectsOlderProofs(
	t *testing.T,
) {
	redisServer := miniredis.RunT(t)
	consumer := newTestAdmissionProofConsumer(t, redisServer)
	now := time.Now().UTC()
	resetAt := time.UnixMilli(now.Add(-time.Second).UnixMilli()).UTC()
	setTestProofGuard(t, consumer, resetAt)

	version, err := consumer.CurrentVersion(context.Background())
	if err != nil {
		t.Fatalf("CurrentVersion() error = %v", err)
	}
	if version != testAdmissionProofVersion {
		t.Fatalf("CurrentVersion() = %+v, want %+v", version, testAdmissionProofVersion)
	}

	for _, test := range []struct {
		name       string
		proofID    string
		issuedAtMS int64
	}{
		{
			name:       "before bootstrap fence",
			proofID:    "proof-before-bootstrap",
			issuedAtMS: resetAt.Add(-time.Millisecond).UnixMilli(),
		},
		{
			name:       "equal bootstrap fence",
			proofID:    "proof-at-bootstrap",
			issuedAtMS: resetAt.UnixMilli(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			trusted, consumeErr := consumer.Consume(
				context.Background(),
				"team-a",
				test.proofID,
				test.issuedAtMS,
				now.Add(time.Minute).UnixMilli(),
				version,
			)
			if consumeErr != nil || trusted {
				t.Fatalf(
					"Consume() = (%t, %v), want untrusted bootstrap-fenced proof",
					trusted,
					consumeErr,
				)
			}
		})
	}

	issuedAtMS := resetAt.Add(time.Millisecond).UnixMilli()
	expiresAtMS := now.Add(time.Minute).UnixMilli()
	trusted, err := consumer.Consume(
		context.Background(),
		"team-a",
		"proof-after-bootstrap",
		issuedAtMS,
		expiresAtMS,
		version,
	)
	if err != nil || !trusted {
		t.Fatalf("first post-bootstrap Consume() = (%t, %v), want trusted", trusted, err)
	}
	trusted, err = consumer.Consume(
		context.Background(),
		"team-a",
		"proof-after-bootstrap",
		issuedAtMS,
		expiresAtMS,
		version,
	)
	if err != nil || trusted {
		t.Fatalf("post-bootstrap replay Consume() = (%t, %v), want untrusted", trusted, err)
	}
}

func TestRedisAdmissionProofConsumerRejectsStalePolicyVersionWithoutError(
	t *testing.T,
) {
	redisServer := miniredis.RunT(t)
	consumer := newTestAdmissionProofConsumer(t, redisServer)
	now := time.Now().UTC()
	setTestProofGuard(t, consumer, now.Add(-time.Minute))

	trusted, err := consumer.Consume(
		context.Background(),
		"team-a",
		"proof-stale-version",
		now.Add(-time.Second).UnixMilli(),
		now.Add(time.Minute).UnixMilli(),
		guard.Version{EnforcementEpoch: 2, RedisGeneration: 1},
	)
	if err != nil || trusted {
		t.Fatalf("stale-version Consume() = (%t, %v), want normal-charge result", trusted, err)
	}
	if redisServer.Exists(consumer.redisKey("team-a", "proof-stale-version")) {
		t.Fatal("stale-version proof unexpectedly created a replay key")
	}
}

func TestRedisAdmissionProofConsumerEvictionFenceRejectsBeforeReplayMutation(t *testing.T) {
	redisServer := miniredis.RunT(t)
	consumer := newTestAdmissionProofConsumer(t, redisServer)
	now := time.Now().UTC()
	setTestProofGuard(t, consumer, now.Add(-time.Minute))
	redisServer.Server().SetPreHook(func(
		peer *miniredisserver.Peer,
		command string,
		_ ...string,
	) bool {
		if command != "INFO" {
			return false
		}
		peer.WriteBulk(
			"# Server\r\nrun_id:" + testAdmissionProofRedisRunID +
				"\r\n# Memory\r\nmaxmemory_policy:noeviction" +
				"\r\n# Stats\r\nevicted_keys:1\r\n",
		)
		return true
	})

	const proofID = "proof-after-eviction"
	trusted, err := consumer.Consume(
		context.Background(),
		"team-a",
		proofID,
		now.Add(-time.Second).UnixMilli(),
		now.Add(time.Minute).UnixMilli(),
		testAdmissionProofVersion,
	)
	if err == nil || !strings.Contains(err.Error(), "runtime safety") || trusted {
		t.Fatalf("eviction-fenced Consume() = (%t, %v), want fail-closed runtime error", trusted, err)
	}
	if redisServer.Exists(consumer.redisKey("team-a", proofID)) {
		t.Fatal("eviction-fenced proof created a replay key")
	}
}

func TestRedisAdmissionProofConsumerConcurrentUseHasExactlyOneWinner(t *testing.T) {
	redisServer := miniredis.RunT(t)
	consumer := newTestAdmissionProofConsumer(t, redisServer)
	now := time.Now().UTC()
	setTestProofGuard(t, consumer, now.Add(-time.Minute))

	const callers = 64
	var trustedCount atomic.Int64
	errs := make(chan error, callers)
	var start sync.WaitGroup
	start.Add(1)
	for range callers {
		go func() {
			start.Wait()
			trusted, err := consumer.Consume(
				context.Background(),
				"team-a",
				"proof-concurrent",
				now.Add(-time.Second).UnixMilli(),
				now.Add(time.Minute).UnixMilli(),
				testAdmissionProofVersion,
			)
			if trusted {
				trustedCount.Add(1)
			}
			errs <- err
		}()
	}
	start.Done()
	for range callers {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Consume() error = %v", err)
		}
	}
	if got := trustedCount.Load(); got != 1 {
		t.Fatalf("trusted concurrent consumers = %d, want exactly 1", got)
	}
}

func TestRedisAdmissionProofConsumerRejectsProofAtOrOlderThanReset(t *testing.T) {
	redisServer := miniredis.RunT(t)
	consumer := newTestAdmissionProofConsumer(t, redisServer)
	now := time.Now().UTC()
	resetAt := time.UnixMilli(now.UnixMilli()).UTC()
	setTestProofGuard(t, consumer, resetAt)

	tests := []struct {
		name       string
		proofID    string
		issuedAtMS int64
	}{
		{
			name:       "older",
			proofID:    "proof-before-reset",
			issuedAtMS: resetAt.Add(-time.Millisecond).UnixMilli(),
		},
		{
			name:       "same millisecond",
			proofID:    "proof-at-reset",
			issuedAtMS: resetAt.UnixMilli(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trusted, err := consumer.Consume(
				context.Background(),
				"team-a",
				test.proofID,
				test.issuedAtMS,
				now.Add(time.Minute).UnixMilli(),
				testAdmissionProofVersion,
			)
			if err != nil || trusted {
				t.Fatalf("Consume() = (%t, %v), want untrusted", trusted, err)
			}
			if redisServer.Exists(consumer.redisKey("team-a", test.proofID)) {
				t.Fatal("reset-fenced proof unexpectedly created a replay key")
			}
		})
	}
}

func TestRedisAdmissionProofConsumerDoesNotReuseOldProofAfterRedisFlush(t *testing.T) {
	redisServer := miniredis.RunT(t)
	consumer := newTestAdmissionProofConsumer(t, redisServer)
	now := time.Now().UTC()
	issuedAtMS := now.UnixMilli()
	expiresAtMS := now.Add(time.Minute).UnixMilli()
	setTestProofGuard(t, consumer, now.Add(-time.Minute))

	trusted, err := consumer.Consume(
		context.Background(),
		"team-a",
		"proof-before-flush",
		issuedAtMS,
		expiresAtMS,
		testAdmissionProofVersion,
	)
	if err != nil || !trusted {
		t.Fatalf("initial Consume() = (%t, %v), want trusted", trusted, err)
	}

	redisServer.FlushAll()
	recoveredResetAt := time.UnixMilli(issuedAtMS).UTC()
	setTestProofGuard(t, consumer, recoveredResetAt)
	if err := consumer.marker.Recover(context.Background(), "team-a"); err != nil {
		t.Fatalf("recover admission marker after flush: %v", err)
	}
	trusted, err = consumer.Consume(
		context.Background(),
		"team-a",
		"proof-before-flush",
		issuedAtMS,
		expiresAtMS,
		testAdmissionProofVersion,
	)
	if err != nil || trusted {
		t.Fatalf("post-flush Consume() = (%t, %v), want reset-fenced replay", trusted, err)
	}
	if redisServer.Exists(consumer.redisKey("team-a", "proof-before-flush")) {
		t.Fatal("post-flush old proof recreated a replay key")
	}
}

func TestRedisAdmissionProofConsumerFailsClosedWhenRedisIsUnavailable(t *testing.T) {
	redisServer := miniredis.RunT(t)
	consumer := newTestAdmissionProofConsumer(t, redisServer)
	now := time.Now().UTC()
	setTestProofGuard(t, consumer, now.Add(-time.Minute))
	redisServer.Close()

	if trusted, err := consumer.Consume(
		context.Background(),
		"team-a",
		"proof-a",
		now.UnixMilli(),
		now.Add(time.Minute).UnixMilli(),
		testAdmissionProofVersion,
	); err == nil || trusted {
		t.Fatalf("Consume() with Redis down = (%t, %v), want fail-closed error", trusted, err)
	}
}

func newTestAdmissionProofConsumer(
	t *testing.T,
	redisServer *miniredis.Miniredis,
) *RedisAdmissionProofConsumer {
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
			"# Server\r\nrun_id:" + testAdmissionProofRedisRunID +
				"\r\n# Memory\r\nmaxmemory_policy:noeviction" +
				"\r\n# Stats\r\nevicted_keys:0\r\n",
		)
		return true
	})
	marker := newTestAdmissionMarker(
		t,
		redisServer,
		&fakeAdmissionStateResolver{},
	)
	if err := marker.Recover(context.Background(), "team-a"); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	consumer, err := NewRedisAdmissionProofConsumer(
		context.Background(),
		marker,
		AdmissionProofConsumerConfig{
			RegionID:  "sg",
			RedisURL:  "redis://" + redisServer.Addr() + "/0",
			KeyPrefix: "test:teamquota",
			Timeout:   time.Second,
		},
	)
	if err != nil {
		t.Fatalf("NewRedisAdmissionProofConsumer() error = %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })
	return consumer
}

func setTestProofGuard(
	t *testing.T,
	consumer *RedisAdmissionProofConsumer,
	resetAt time.Time,
) {
	t.Helper()
	fields, err := guard.Fields(guard.State{
		Phase:      guard.PhaseStable,
		Version:    guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
		RedisRunID: testAdmissionProofRedisRunID,
		ResetAt:    resetAt,
	})
	if err != nil {
		t.Fatalf("guard.Fields() error = %v", err)
	}
	if err := consumer.client.HSet(
		context.Background(),
		consumer.policyKey,
		fields,
	).Err(); err != nil {
		t.Fatalf("set policy guard: %v", err)
	}
}
