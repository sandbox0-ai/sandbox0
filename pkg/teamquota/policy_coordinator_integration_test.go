package teamquota

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

func TestPolicyCoordinatorDefaultFenceRollsBackAndDetectsHashConflict(t *testing.T) {
	fixture := newPolicyCoordinatorFixture(t, 20*time.Millisecond)
	ctx := context.Background()
	ownerEpoch := time.Date(2026, time.July, 19, 1, 2, 3, 456_789_987, time.UTC)
	firstDefaults := completeDefaultPolicies(Policy{
		Key:            KeyAPIRequests,
		Kind:           KindRate,
		Tokens:         10,
		IntervalMillis: 1000,
		Burst:          20,
	})
	firstVersion := DefaultPolicyVersion{OwnerEpoch: ownerEpoch, Generation: 2}
	if err := fixture.coordinator.ReplaceDefaultPoliciesVersioned(
		ctx,
		firstDefaults,
		firstVersion,
	); err != nil {
		t.Fatalf("initial ReplaceDefaultPoliciesVersioned() error = %v", err)
	}
	beforeState := loadCoordinatorPolicyState(t, fixture.pool)
	beforePolicy := effectiveCoordinatorPolicy(t, fixture.coordinator, KeyAPIRequests)
	beforeGuard, err := fixture.policyGuard.ReadPolicyGuard(ctx)
	if err != nil {
		t.Fatalf("read initial policy guard: %v", err)
	}

	secondDefaults := completeDefaultPolicies(Policy{
		Key:            KeyAPIRequests,
		Kind:           KindRate,
		Tokens:         1,
		IntervalMillis: 1000,
		Burst:          1,
	})
	writeFailure := errors.New("injected Redis pending failure")
	fixture.policyGuard.failNextPending(writeFailure)
	err = fixture.coordinator.ReplaceDefaultPoliciesVersioned(
		ctx,
		secondDefaults,
		DefaultPolicyVersion{OwnerEpoch: ownerEpoch, Generation: 3},
	)
	if !IsUnavailable(err) || !errors.Is(err, writeFailure) {
		t.Fatalf("failed ReplaceDefaultPoliciesVersioned() error = %v", err)
	}
	assertCoordinatorStateUnchanged(t, fixture.pool, beforeState)
	afterRollback := effectiveCoordinatorPolicy(t, fixture.coordinator, KeyAPIRequests)
	if !policyContentEqual(afterRollback, beforePolicy) ||
		afterRollback.Revision != beforePolicy.Revision {
		t.Fatalf("default after rollback = %+v, want %+v", afterRollback, beforePolicy)
	}
	afterRollbackGuard, err := fixture.policyGuard.ReadPolicyGuard(ctx)
	if err != nil || !afterRollbackGuard.Version.Equal(beforeGuard.Version) || !afterRollbackGuard.Stable() {
		t.Fatalf("guard after rollback = (%+v, %v), want unchanged stable", afterRollbackGuard, err)
	}

	err = fixture.coordinator.ReplaceDefaultPoliciesVersioned(
		ctx,
		secondDefaults,
		firstVersion,
	)
	var conflict *DefaultPolicyHashConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("same-version conflicting defaults error = %v, want hash conflict", err)
	}
	assertCoordinatorStateUnchanged(t, fixture.pool, beforeState)

	err = fixture.coordinator.ReplaceDefaultPoliciesVersioned(
		ctx,
		secondDefaults,
		DefaultPolicyVersion{OwnerEpoch: ownerEpoch, Generation: 1},
	)
	var stale *StaleDefaultPolicyVersionError
	if !errors.As(err, &stale) {
		t.Fatalf("stale defaults error = %v, want stale version", err)
	}
	assertCoordinatorStateUnchanged(t, fixture.pool, beforeState)

	// A newer rollout with identical content advances only the defaults fence;
	// it must not perturb distributed admissions.
	if err := fixture.coordinator.ReplaceDefaultPoliciesVersioned(
		ctx,
		firstDefaults,
		DefaultPolicyVersion{OwnerEpoch: ownerEpoch, Generation: 3},
	); err != nil {
		t.Fatalf("newer identical defaults error = %v", err)
	}
	advanced := loadCoordinatorPolicyState(t, fixture.pool)
	if advanced.DefaultsGeneration != 3 ||
		advanced.EnforcementEpoch != beforeState.EnforcementEpoch ||
		advanced.RedisGeneration != beforeState.RedisGeneration {
		t.Fatalf("identical defaults advanced state = %+v, before %+v", advanced, beforeState)
	}
}

func TestPolicyCoordinatorInitialBootstrapIsImmediateAndConstructorRequiresRuntimeSafetyInfo(t *testing.T) {
	fixture := newUnbootstrappedPolicyCoordinatorFixture(t, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := fixture.coordinator.Repair(ctx); err != nil {
		t.Fatalf("initial Repair() waited for reset quarantine: %v", err)
	}
	state := loadCoordinatorPolicyState(t, fixture.pool)
	if !state.RedisInitialized || state.RedisGeneration != 1 {
		t.Fatalf("initial durable Redis state = %+v", state)
	}
	if state.RedisResetAt.IsZero() || !state.RateRefillFrom.IsZero() {
		t.Fatalf(
			"initial proof/reset state = reset %s refill %s, want real reset fence and full-burst refill sentinel",
			state.RedisResetAt,
			state.RateRefillFrom,
		)
	}
	assertCoordinatorStableVersion(t, fixture, state.Version())
	stable, err := fixture.policyGuard.ReadPolicyGuard(ctx)
	if err != nil {
		t.Fatalf("ReadPolicyGuard() error = %v", err)
	}
	if !stable.ResetAt.Equal(state.RedisResetAt.UTC().Truncate(time.Millisecond)) ||
		!stable.RateRefillFrom.IsZero() {
		t.Fatalf(
			"initial Redis guard = reset %s refill %s, want durable proof fence and zero refill origin",
			stable.ResetAt,
			stable.RateRefillFrom,
		)
	}

	bucket, err := tokenbucket.NewRedisBucket(ctx, tokenbucket.RedisConfig{
		URL:       "redis://" + fixture.redis.Addr() + "/0",
		KeyPrefix: coordinatorTestRedisPrefix,
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisBucket() error = %v", err)
	}
	defer bucket.Close()
	const admissionKey = "test:bootstrap-admission:team-a"
	fixture.redis.Set(admissionKey, "active")
	decision, err := bucket.TakeNGuarded(
		ctx,
		"team-quota:v1:bootstrap:team-a:api_requests",
		admissionKey,
		tokenbucket.Policy{
			Tokens:   1,
			Interval: time.Hour,
			Burst:    2,
			Revision: 1,
		},
		state.Version(),
		state.RateRefillFrom,
		1,
	)
	if err != nil {
		t.Fatalf("TakeNGuarded() on fresh bootstrap error = %v", err)
	}
	if !decision.Allowed || decision.Remaining != 1 {
		t.Fatalf(
			"fresh bootstrap decision = %+v, want full initial burst with one token remaining",
			decision,
		)
	}

	missingRunIDRedis := miniredis.RunT(t)
	_, err = NewPolicyCoordinator(
		context.Background(),
		fixture.pool,
		PolicyCoordinatorConfig{
			RegionID:        "region-constructor-test",
			ExpectedStateID: "6f42f8d4-3d29-4c7e-9a21-5d7b2c8e104f",
			RedisURL:        "redis://" + missingRunIDRedis.Addr() + "/0",
			RedisKeyPrefix:  coordinatorTestRedisPrefix,
			RedisTimeout:    time.Second,
			LeaseTTL:        time.Second,
		},
	)
	if err == nil || !strings.Contains(err.Error(), "INFO server memory stats") {
		t.Fatalf("NewPolicyCoordinator() without runtime safety INFO error = %v", err)
	}
}

func TestPolicyCoordinatorUninitializedPostgresQuarantinesExistingRedisGuard(t *testing.T) {
	const leaseTTL = 120 * time.Millisecond
	tests := []struct {
		name string
		seed func(*testing.T, *coordinatorFixture)
	}{
		{
			name: "stable guard",
			seed: func(t *testing.T, fixture *coordinatorFixture) {
				t.Helper()
				if err := fixture.policyGuard.Force(context.Background(), guard.State{
					Phase:      guard.PhaseStable,
					Version:    guard.Version{EnforcementEpoch: 9, RedisGeneration: 7},
					RedisRunID: "integration-run-id",
				}); err != nil {
					t.Fatalf("seed stable guard: %v", err)
				}
			},
		},
		{
			name: "pending guard",
			seed: func(t *testing.T, fixture *coordinatorFixture) {
				t.Helper()
				if err := fixture.policyGuard.Force(context.Background(), guard.State{
					Phase:        guard.PhasePending,
					Version:      guard.Version{EnforcementEpoch: 9, RedisGeneration: 7},
					RedisRunID:   "integration-run-id",
					PendingToken: "old-reset",
				}); err != nil {
					t.Fatalf("seed pending guard: %v", err)
				}
			},
		},
		{
			name: "corrupt guard",
			seed: func(t *testing.T, fixture *coordinatorFixture) {
				t.Helper()
				fixture.redis.HSet(
					guard.Key(coordinatorTestRedisPrefix),
					"phase",
					"corrupt",
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newUnbootstrappedPolicyCoordinatorFixture(t, leaseTTL)
			tt.seed(t, fixture)

			started := time.Now()
			repaired := make(chan error, 1)
			go func() {
				repaired <- fixture.coordinator.Repair(context.Background())
			}()
			pending := waitForCoordinatorResetPending(
				t,
				fixture.policyGuard,
				guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
				time.Second,
			)
			if pending.Version != (guard.Version{
				EnforcementEpoch: 1,
				RedisGeneration:  1,
			}) ||
				pending.QuarantineUntil.IsZero() ||
				pending.ResetAt.IsZero() ||
				pending.RateRefillFrom.IsZero() {
				t.Fatalf("replacement pending guard = %+v", pending)
			}
			select {
			case err := <-repaired:
				t.Fatalf("Repair() returned before retained Redis state drained: %v", err)
			case <-time.After(leaseTTL / 2):
			}
			if err := <-repaired; err != nil {
				t.Fatalf("Repair() error = %v", err)
			}
			if elapsed := time.Since(started); elapsed < leaseTTL-5*time.Millisecond {
				t.Fatalf(
					"retained Redis repair elapsed = %s, want quarantine near %s",
					elapsed,
					leaseTTL,
				)
			}
			state := loadCoordinatorPolicyState(t, fixture.pool)
			if !state.RedisInitialized ||
				state.RedisGeneration != 1 ||
				state.RedisResetAt.IsZero() ||
				state.RateRefillFrom.IsZero() {
				t.Fatalf("durable state after retained Redis repair = %+v", state)
			}
			assertCoordinatorStableVersion(t, fixture, state.Version())
		})
	}
}

func TestPolicyStateDatabaseConstraintsAndMissingSingletonFailClosed(t *testing.T) {
	fixture := newUnbootstrappedPolicyCoordinatorFixture(t, time.Second)
	ctx := context.Background()

	invalidUpdates := []string{
		`UPDATE quota.policy_state
		 SET redis_initialized = TRUE, redis_generation = 0, redis_run_id = 'run-1'
		 WHERE singleton = TRUE`,
		`UPDATE quota.policy_state
		 SET redis_initialized = FALSE, redis_generation = 1, redis_run_id = NULL
		 WHERE singleton = TRUE`,
		`UPDATE quota.policy_state
		 SET redis_initialized = FALSE, redis_generation = 0, redis_run_id = 'run-1'
		 WHERE singleton = TRUE`,
		`UPDATE quota.policy_state
		 SET redis_initialized = FALSE, redis_generation = 0,
		     redis_run_id = NULL, redis_evicted_keys = 1
		 WHERE singleton = TRUE`,
		`UPDATE quota.policy_state
		 SET redis_initialized = TRUE, redis_generation = 1,
		     redis_run_id = 'run-1', redis_evicted_keys = -1
		 WHERE singleton = TRUE`,
		`UPDATE quota.policy_state
		 SET redis_reset_at = NULL, rate_refill_from = NOW()
		 WHERE singleton = TRUE`,
		`UPDATE quota.policy_state
		 SET defaults_owner_epoch = NOW(), defaults_generation = NULL, defaults_sha256 = NULL
		 WHERE singleton = TRUE`,
		`UPDATE quota.policy_state
		 SET defaults_owner_epoch = NOW(), defaults_generation = 1,
		     defaults_sha256 = repeat('A', 64)
		 WHERE singleton = TRUE`,
	}
	for i, statement := range invalidUpdates {
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin(invalid update %d) error = %v", i, err)
		}
		if _, err := tx.Exec(ctx, statement); err == nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("invalid policy-state update %d succeeded", i)
		}
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Fatalf("Rollback(invalid update %d) error = %v", i, err)
		}
	}

	state := loadCoordinatorPolicyState(t, fixture.pool)
	if _, err := fixture.pool.Exec(ctx, `DELETE FROM quota.policy_state WHERE singleton = TRUE`); err != nil {
		t.Fatalf("delete policy-state singleton: %v", err)
	}
	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin(missing singleton) error = %v", err)
	}
	err = storePolicyState(ctx, tx, state)
	if !IsUnavailable(err) || !strings.Contains(err.Error(), "exactly 1") {
		_ = tx.Rollback(ctx)
		t.Fatalf("storePolicyState(missing singleton) error = %v", err)
	}
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Fatalf("Rollback(missing singleton) error = %v", err)
	}
}

func TestPolicyCoordinatorDrainsLocalCreditsWhileGuardIsPendingBeforeCommit(t *testing.T) {
	fixture := newPolicyCoordinatorFixture(t, time.Second)
	ctx := context.Background()
	before := loadCoordinatorPolicyState(t, fixture.pool)
	now := time.Date(2026, time.July, 19, 2, 0, 0, 0, time.UTC)
	fixture.coordinator.now = func() time.Time { return now }
	waitCalls := 0
	fixture.coordinator.waitUntil = func(_ context.Context, deadline time.Time) error {
		waitCalls++
		if !deadline.Equal(now.Add(guard.MaxLocalCreditTTL)) {
			t.Fatalf(
				"credit drain deadline = %s, want %s",
				deadline,
				now.Add(guard.MaxLocalCreditTTL),
			)
		}
		durableDuringDrain := loadCoordinatorPolicyState(t, fixture.pool)
		if durableDuringDrain.EnforcementEpoch != before.EnforcementEpoch {
			t.Fatalf(
				"durable epoch during drain = %d, want pre-commit %d",
				durableDuringDrain.EnforcementEpoch,
				before.EnforcementEpoch,
			)
		}
		pending, err := fixture.policyGuard.ReadPolicyGuard(ctx)
		if err != nil || pending.Phase != guard.PhasePending ||
			!pending.Version.Equal(before.Version()) {
			t.Fatalf(
				"guard during credit drain = (%+v, %v), want pending old version",
				pending,
				err,
			)
		}
		return nil
	}

	if err := fixture.coordinator.PutTeamPolicy(ctx, "team-a", Policy{
		Key:            KeyAPIRequests,
		Kind:           KindRate,
		Tokens:         1,
		IntervalMillis: 1000,
		Burst:          1,
	}); err != nil {
		t.Fatalf("PutTeamPolicy() error = %v", err)
	}
	if waitCalls != 1 {
		t.Fatalf("credit drain waiter calls = %d, want 1", waitCalls)
	}
	after := loadCoordinatorPolicyState(t, fixture.pool)
	if after.EnforcementEpoch != before.EnforcementEpoch+1 {
		t.Fatalf(
			"committed epoch = %d, want %d",
			after.EnforcementEpoch,
			before.EnforcementEpoch+1,
		)
	}
	assertCoordinatorStableVersion(t, fixture, after.Version())
}

func TestPolicyCoordinatorRecoversBothPostgresCommitAmbiguityOutcomes(t *testing.T) {
	fixture := newPolicyCoordinatorFixture(t, 20*time.Millisecond)
	ctx := context.Background()
	defaults := completeDefaultPolicies()
	if err := fixture.coordinator.ReplaceDefaultPoliciesVersioned(
		ctx,
		defaults,
		DefaultPolicyVersion{
			OwnerEpoch: time.Date(2026, time.July, 19, 0, 0, 0, 0, time.UTC),
			Generation: 1,
		},
	); err != nil {
		t.Fatalf("install defaults: %v", err)
	}

	beforeCommitted := loadCoordinatorPolicyState(t, fixture.pool)
	ambiguousCommitted := errors.New("injected lost commit acknowledgement")
	fixture.coordinator.commitPolicyTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return ambiguousCommitted
	}
	err := fixture.coordinator.PutTeamPolicy(ctx, "team-a", Policy{
		Key:            KeyAPIRequests,
		Kind:           KindRate,
		Tokens:         1,
		IntervalMillis: 1000,
		Burst:          1,
	})
	if !IsUnavailable(err) || !errors.Is(err, ambiguousCommitted) {
		t.Fatalf("committed ambiguous PutTeamPolicy() error = %v", err)
	}
	fixture.coordinator.commitPolicyTx = nil
	committedState := loadCoordinatorPolicyState(t, fixture.pool)
	if committedState.EnforcementEpoch != beforeCommitted.EnforcementEpoch+1 {
		t.Fatalf(
			"committed ambiguous epoch = %d, want %d",
			committedState.EnforcementEpoch,
			beforeCommitted.EnforcementEpoch+1,
		)
	}
	committedPolicy := effectiveCoordinatorPolicyForTeam(
		t,
		fixture.coordinator,
		"team-a",
		KeyAPIRequests,
	)
	if committedPolicy.Burst != 1 {
		t.Fatalf("committed ambiguous policy = %+v", committedPolicy)
	}
	pending, err := fixture.policyGuard.ReadPolicyGuard(ctx)
	if err != nil || pending.Phase != guard.PhasePending {
		t.Fatalf("guard after ambiguous commit = (%+v, %v), want pending", pending, err)
	}
	if err := fixture.coordinator.Repair(ctx); err != nil {
		t.Fatalf("Repair(committed outcome) error = %v", err)
	}
	assertCoordinatorStableVersion(t, fixture, committedState.Version())

	beforeRolledBack := loadCoordinatorPolicyState(t, fixture.pool)
	beforePolicy := committedPolicy
	ambiguousRolledBack := errors.New("injected commit failed before outcome")
	fixture.coordinator.commitPolicyTx = func(context.Context, pgx.Tx) error {
		return ambiguousRolledBack
	}
	err = fixture.coordinator.PutTeamPolicy(ctx, "team-a", Policy{
		Key:            KeyAPIRequests,
		Kind:           KindRate,
		Tokens:         2,
		IntervalMillis: 1000,
		Burst:          2,
	})
	if !IsUnavailable(err) || !errors.Is(err, ambiguousRolledBack) {
		t.Fatalf("rolled-back ambiguous PutTeamPolicy() error = %v", err)
	}
	fixture.coordinator.commitPolicyTx = nil
	assertCoordinatorStateUnchanged(t, fixture.pool, beforeRolledBack)
	rolledBackPolicy := effectiveCoordinatorPolicyForTeam(
		t,
		fixture.coordinator,
		"team-a",
		KeyAPIRequests,
	)
	if !policyContentEqual(rolledBackPolicy, beforePolicy) ||
		rolledBackPolicy.Revision != beforePolicy.Revision {
		t.Fatalf("policy after rolled-back commit = %+v, want %+v", rolledBackPolicy, beforePolicy)
	}
	pending, err = fixture.policyGuard.ReadPolicyGuard(ctx)
	if err != nil || pending.Phase != guard.PhasePending {
		t.Fatalf("guard after rolled-back ambiguous commit = (%+v, %v)", pending, err)
	}
	if err := fixture.coordinator.Repair(ctx); err != nil {
		t.Fatalf("Repair(rolled-back outcome) error = %v", err)
	}
	assertCoordinatorStableVersion(t, fixture, beforeRolledBack.Version())
}

func TestPolicyCoordinatorFlushRecoveryQuarantinesAndRefillsFromZero(t *testing.T) {
	const leaseTTL = 80 * time.Millisecond
	fixture := newPolicyCoordinatorFixture(t, leaseTTL)
	ctx := context.Background()
	before := loadCoordinatorPolicyState(t, fixture.pool)
	fixture.redis.FlushAll()

	started := time.Now()
	repaired := make(chan error, 1)
	go func() {
		repaired <- fixture.coordinator.Repair(ctx)
	}()
	pending := waitForCoordinatorGuardPhase(
		t,
		fixture.policyGuard,
		guard.PhasePending,
		500*time.Millisecond,
	)
	if pending.Version.RedisGeneration != before.RedisGeneration+1 {
		t.Fatalf(
			"pending Redis generation = %d, want %d",
			pending.Version.RedisGeneration,
			before.RedisGeneration+1,
		)
	}
	if pending.QuarantineUntil.IsZero() ||
		pending.ResetAt.IsZero() ||
		pending.RateRefillFrom.IsZero() {
		t.Fatalf("incomplete reset pending guard = %+v", pending)
	}
	remaining := time.Until(pending.QuarantineUntil)
	if remaining > 4*time.Millisecond {
		select {
		case err := <-repaired:
			t.Fatalf("Repair() returned before quarantine ended: %v", err)
		case <-time.After(remaining / 2):
		}
	}
	if err := <-repaired; err != nil {
		t.Fatalf("Repair() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < leaseTTL-5*time.Millisecond {
		t.Fatalf("FLUSH repair elapsed = %s, want quarantine near %s", elapsed, leaseTTL)
	}
	after := loadCoordinatorPolicyState(t, fixture.pool)
	if after.RedisGeneration != before.RedisGeneration+1 ||
		after.RedisResetAt.IsZero() ||
		after.RateRefillFrom.IsZero() {
		t.Fatalf("durable state after FLUSH = %+v, before %+v", after, before)
	}
	assertCoordinatorStableVersion(t, fixture, after.Version())
	if ttl := fixture.redis.TTL(guard.Key(coordinatorTestRedisPrefix)); ttl != 0 {
		t.Fatalf("repaired guard TTL = %s, want non-expiring", ttl)
	}

	bucket, err := tokenbucket.NewRedisBucket(ctx, tokenbucket.RedisConfig{
		URL:       "redis://" + fixture.redis.Addr() + "/0",
		KeyPrefix: coordinatorTestRedisPrefix,
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisBucket() error = %v", err)
	}
	defer bucket.Close()
	const admissionKey = "test:team-admission:team-a"
	fixture.redis.Set(admissionKey, "active")
	decision, err := bucket.TakeNGuarded(
		ctx,
		"team-quota:v1:6:region:6:team-a:api_requests",
		admissionKey,
		tokenbucket.Policy{
			Tokens:   1,
			Interval: time.Hour,
			Burst:    100,
			Revision: 1,
		},
		after.Version(),
		after.RateRefillFrom,
		1,
	)
	if err != nil {
		t.Fatalf("TakeNGuarded() after FLUSH error = %v", err)
	}
	if decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("TakeNGuarded() after FLUSH = %+v, want refill from zero", decision)
	}
}

const coordinatorTestRedisPrefix = "test:teamquota"

type coordinatorFixture struct {
	pool               *pgxpool.Pool
	redis              *miniredis.Miniredis
	redisRuntime       *integrationRedisRuntime
	policyGuard        *integrationPolicyGuard
	identityMaintainer *integrationIdentityMaintainer
	coordinator        *PolicyCoordinator
}

func TestPolicyCoordinatorEvictionRecoveryAdvancesGenerationAndBaseline(t *testing.T) {
	const leaseTTL = 80 * time.Millisecond
	fixture := newPolicyCoordinatorFixture(t, leaseTTL)
	before := loadCoordinatorPolicyState(t, fixture.pool)
	fixture.redisRuntime.setEvictedKeys(before.RedisEvictedKeys + 1)

	started := time.Now()
	if err := fixture.coordinator.Repair(context.Background()); err != nil {
		t.Fatalf("Repair() after eviction error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < leaseTTL-5*time.Millisecond {
		t.Fatalf("eviction repair elapsed = %s, want quarantine near %s", elapsed, leaseTTL)
	}
	after := loadCoordinatorPolicyState(t, fixture.pool)
	if after.RedisGeneration != before.RedisGeneration+1 ||
		after.RedisEvictedKeys != before.RedisEvictedKeys+1 {
		t.Fatalf("durable state after eviction = %+v, before %+v", after, before)
	}
	stable, err := fixture.policyGuard.ReadPolicyGuard(context.Background())
	if err != nil {
		t.Fatalf("ReadPolicyGuard() after eviction repair error = %v", err)
	}
	if !stable.Stable() ||
		!stable.Version.Equal(after.Version()) ||
		stable.RedisEvictedKeys != after.RedisEvictedKeys {
		t.Fatalf("guard after eviction repair = %+v, durable %+v", stable, after)
	}
}

func TestPolicyCoordinatorIdentityFailureInvalidatesGuardUntilReset(t *testing.T) {
	const leaseTTL = 20 * time.Millisecond
	fixture := newPolicyCoordinatorFixture(t, leaseTTL)
	before := loadCoordinatorPolicyState(t, fixture.pool)
	identityErr := errors.New("injected region state identity mismatch")
	fixture.identityMaintainer.setError(identityErr)

	err := fixture.coordinator.Repair(context.Background())
	if !IsUnavailable(err) || !errors.Is(err, identityErr) {
		t.Fatalf("Repair() identity failure error = %v", err)
	}
	if _, err := fixture.policyGuard.ReadPolicyGuard(context.Background()); !errors.Is(err, guard.ErrMissing) {
		t.Fatalf("policy guard after identity failure error = %v, want %v", err, guard.ErrMissing)
	}

	fixture.identityMaintainer.setError(nil)
	if err := fixture.coordinator.Repair(context.Background()); err != nil {
		t.Fatalf("Repair() after identity recovery error = %v", err)
	}
	after := loadCoordinatorPolicyState(t, fixture.pool)
	if after.RedisGeneration != before.RedisGeneration+1 {
		t.Fatalf(
			"Redis generation after identity recovery = %d, want %d",
			after.RedisGeneration,
			before.RedisGeneration+1,
		)
	}
	assertCoordinatorStableVersion(t, fixture, after.Version())
}

func TestNewPolicyCoordinatorFreshStateBootstrapsWithoutLeaseQuarantine(t *testing.T) {
	pool := newTeamQuotaTestDatabase(t)
	if err := RunMigrations(context.Background(), pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	redisServer, _ := runMutableRegionStateRedis(t, "noeviction")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	coordinator, err := NewPolicyCoordinator(ctx, pool, PolicyCoordinatorConfig{
		RegionID:        "region-fresh-constructor",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://" + redisServer.Addr() + "/0",
		RedisKeyPrefix:  "sandbox0:test:fresh-constructor",
		RedisTimeout:    time.Second,
		LeaseTTL:        time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPolicyCoordinator() fresh bootstrap error = %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	state := loadCoordinatorPolicyState(t, pool)
	if !state.RedisInitialized || state.RedisGeneration != 1 {
		t.Fatalf("fresh constructor policy state = %+v", state)
	}
	stable, err := coordinator.policyGuard.ReadPolicyGuard(context.Background())
	if err != nil {
		t.Fatalf("ReadPolicyGuard() after fresh constructor error = %v", err)
	}
	if !stable.Stable() || !stable.Version.Equal(state.Version()) {
		t.Fatalf("fresh constructor guard = %+v, durable state %+v", stable, state)
	}
}

func TestPolicyCoordinatorRedisIdentityRepairSignalForcesGenerationReset(t *testing.T) {
	const leaseTTL = 120 * time.Millisecond
	fixture := newPolicyCoordinatorFixture(t, leaseTTL)
	before := loadCoordinatorPolicyState(t, fixture.pool)
	fixture.identityMaintainer.setResult(RegionStateIdentityMaintenanceResult{
		RedisClaimRepaired: true,
	})

	started := time.Now()
	if err := fixture.coordinator.Repair(context.Background()); err != nil {
		t.Fatalf("Repair() after Redis identity repair error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < leaseTTL-5*time.Millisecond {
		t.Fatalf("identity-loss repair elapsed = %s, want quarantine near %s", elapsed, leaseTTL)
	}
	after := loadCoordinatorPolicyState(t, fixture.pool)
	if after.RedisGeneration != before.RedisGeneration+1 {
		t.Fatalf(
			"Redis generation after identity repair = %d, want %d",
			after.RedisGeneration,
			before.RedisGeneration+1,
		)
	}
	if after.RateRefillFrom.IsZero() || after.RedisResetAt.IsZero() {
		t.Fatalf("identity-loss reset state = %+v, want reset and refill fences", after)
	}
	assertCoordinatorStableVersion(t, fixture, after.Version())
}

func TestPolicyCoordinatorReleasesIdentityRowBeforeResetQuarantine(t *testing.T) {
	const leaseTTL = 300 * time.Millisecond
	pool := newTeamQuotaTestDatabase(t)
	if err := RunMigrations(context.Background(), pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	redisServer, _ := runMutableRegionStateRedis(t, "noeviction")
	cfg := PolicyCoordinatorConfig{
		RegionID:        "region-identity-lock-release",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://" + redisServer.Addr() + "/0",
		RedisKeyPrefix:  "sandbox0:test:identity-lock-release",
		RedisTimeout:    time.Second,
		LeaseTTL:        leaseTTL,
	}
	coordinator, err := NewPolicyCoordinator(context.Background(), pool, cfg)
	if err != nil {
		t.Fatalf("NewPolicyCoordinator() error = %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	before := loadCoordinatorPolicyState(t, pool)
	identity, err := NormalizeRegionStateIdentity(RegionStateIdentityConfig{
		RegionID:        cfg.RegionID,
		ExpectedStateID: cfg.ExpectedStateID,
		RedisURL:        cfg.RedisURL,
		RedisKeyPrefix:  cfg.RedisKeyPrefix,
	})
	if err != nil {
		t.Fatalf("NormalizeRegionStateIdentity() error = %v", err)
	}
	redisServer.Del(regionStateIdentityRedisKey(identity))

	repairDone := make(chan error, 1)
	go func() {
		repairDone <- coordinator.Repair(context.Background())
	}()
	waitForCoordinatorGuardPhase(
		t,
		coordinator.policyGuard,
		guard.PhasePending,
		time.Second,
	)

	lockCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	tx, err := pool.BeginTx(lockCtx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin identity row lock probe: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var singleton bool
	if err := tx.QueryRow(lockCtx, `
		SELECT singleton
		FROM quota.region_state_identity_claims
		WHERE singleton = TRUE
		FOR UPDATE
	`).Scan(&singleton); err != nil {
		t.Fatalf("identity row stayed locked during Redis quarantine: %v", err)
	}
	if !singleton {
		t.Fatal("identity row lock probe returned non-singleton row")
	}
	if err := tx.Rollback(lockCtx); err != nil {
		t.Fatalf("rollback identity row lock probe: %v", err)
	}
	if err := <-repairDone; err != nil {
		t.Fatalf("Repair() after identity loss error = %v", err)
	}
	after := loadCoordinatorPolicyState(t, pool)
	if after.RedisGeneration != before.RedisGeneration+1 {
		t.Fatalf(
			"Redis generation after identity loss = %d, want %d",
			after.RedisGeneration,
			before.RedisGeneration+1,
		)
	}
}

func TestPolicyCoordinatorLocalWriterGateBoundsPoolWaiters(t *testing.T) {
	fixture := newPolicyCoordinatorFixture(t, 20*time.Millisecond)
	var verifications atomic.Int64
	fixture.identityMaintainer.mu.Lock()
	fixture.identityMaintainer.onVerify = func() {
		verifications.Add(1)
	}
	fixture.identityMaintainer.mu.Unlock()

	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- fixture.coordinator.withWriterLock(
			context.Background(),
			func(*pgxpool.Conn) error {
				close(entered)
				<-release
				return nil
			},
		)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first writer did not enter")
	}

	const waiters = 12
	var waiterWG sync.WaitGroup
	waiterWG.Add(waiters)
	for range waiters {
		go func() {
			defer waiterWG.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			_ = fixture.coordinator.Repair(ctx)
		}()
	}
	waiterWG.Wait()
	if got := verifications.Load(); got != 1 {
		t.Fatalf("identity verifications while first writer blocked = %d, want 1", got)
	}
	if got := fixture.pool.Stat().AcquiredConns(); got > 1 {
		t.Fatalf("PostgreSQL connections acquired by local writers = %d, want at most 1", got)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first withWriterLock() error = %v", err)
	}
}

func TestPolicyCoordinatorIdentityFenceCancellationPolicy(t *testing.T) {
	fixture := newPolicyCoordinatorFixture(t, 20*time.Millisecond)
	state := loadCoordinatorPolicyState(t, fixture.pool)
	tests := []struct {
		name        string
		err         error
		wantMissing bool
	}{
		{
			name: "ordinary caller cancellation preserves global guard",
			err:  context.DeadlineExceeded,
		},
		{
			name:        "definitive mismatch still invalidates after caller cancellation",
			err:         fmt.Errorf("%w: injected", ErrRegionStateIdentityMismatch),
			wantMissing: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := fixture.policyGuard.Force(context.Background(), state.stableGuard()); err != nil {
				t.Fatalf("restore stable guard: %v", err)
			}
			coordinator := &PolicyCoordinator{
				pool:        fixture.pool,
				repository:  NewRepository(fixture.pool),
				policyGuard: fixture.policyGuard,
				stateIdentityMaintainer: identityMaintainerFunc(
					func(ctx context.Context, _ *pgxpool.Conn) (
						RegionStateIdentityMaintenanceResult,
						error,
					) {
						<-ctx.Done()
						return RegionStateIdentityMaintenanceResult{}, tt.err
					},
				),
				leaseTTL: 20 * time.Millisecond,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if err := coordinator.Repair(ctx); !IsUnavailable(err) {
				t.Fatalf("Repair() cancellation error = %v", err)
			}
			_, readErr := fixture.policyGuard.ReadPolicyGuard(context.Background())
			if tt.wantMissing {
				if !errors.Is(readErr, guard.ErrMissing) {
					t.Fatalf("guard after definitive mismatch error = %v, want missing", readErr)
				}
			} else if readErr != nil {
				t.Fatalf("guard after ordinary cancellation error = %v", readErr)
			}
		})
	}
}

func TestPolicyCoordinatorFinalIdentityFenceSerializesInvalidationAfterPublisher(t *testing.T) {
	fixture := newPolicyCoordinatorFixture(t, 20*time.Millisecond)
	state := loadCoordinatorPolicyState(t, fixture.pool)
	publisherEntered := make(chan struct{})
	allowPublish := make(chan struct{})
	publisher := &PolicyCoordinator{
		pool:        fixture.pool,
		repository:  NewRepository(fixture.pool),
		policyGuard: fixture.policyGuard,
		stateIdentityMaintainer: identityMaintainerFunc(
			func(context.Context, *pgxpool.Conn) (
				RegionStateIdentityMaintenanceResult,
				error,
			) {
				return RegionStateIdentityMaintenanceResult{}, nil
			},
		),
		leaseTTL: 20 * time.Millisecond,
	}
	verifierCalled := make(chan struct{})
	var verifierOnce sync.Once
	mismatchedOwner := &PolicyCoordinator{
		pool:        fixture.pool,
		repository:  NewRepository(fixture.pool),
		policyGuard: fixture.policyGuard,
		stateIdentityMaintainer: identityMaintainerFunc(
			func(context.Context, *pgxpool.Conn) (
				RegionStateIdentityMaintenanceResult,
				error,
			) {
				verifierOnce.Do(func() { close(verifierCalled) })
				return RegionStateIdentityMaintenanceResult{}, ErrRegionStateIdentityMismatch
			},
		),
		leaseTTL: 20 * time.Millisecond,
	}

	publisherDone := make(chan error, 1)
	go func() {
		publisherDone <- publisher.withWriterLock(
			context.Background(),
			func(*pgxpool.Conn) error {
				close(publisherEntered)
				<-allowPublish
				return fixture.policyGuard.Force(context.Background(), state.stableGuard())
			},
		)
	}()
	select {
	case <-publisherEntered:
	case <-time.After(time.Second):
		t.Fatal("publisher did not acquire the writer lock")
	}
	mismatchDone := make(chan error, 1)
	go func() {
		mismatchDone <- mismatchedOwner.Repair(context.Background())
	}()
	select {
	case <-verifierCalled:
		t.Fatal("mismatched owner verified identity outside the advisory writer lock")
	case <-time.After(40 * time.Millisecond):
	}
	close(allowPublish)
	if err := <-publisherDone; err != nil {
		t.Fatalf("publisher error = %v", err)
	}
	if err := <-mismatchDone; !errors.Is(err, ErrRegionStateIdentityMismatch) {
		t.Fatalf("mismatched owner error = %v", err)
	}
	if _, err := fixture.policyGuard.ReadPolicyGuard(context.Background()); !errors.Is(err, guard.ErrMissing) {
		t.Fatalf("policy guard after serialized mismatch error = %v, want missing", err)
	}
}

func newPolicyCoordinatorFixture(t *testing.T, leaseTTL time.Duration) *coordinatorFixture {
	t.Helper()
	fixture := newUnbootstrappedPolicyCoordinatorFixture(t, leaseTTL)
	if err := fixture.coordinator.Repair(context.Background()); err != nil {
		t.Fatalf("initial Repair() error = %v", err)
	}
	return fixture
}

func newUnbootstrappedPolicyCoordinatorFixture(
	t *testing.T,
	leaseTTL time.Duration,
) *coordinatorFixture {
	t.Helper()
	pool := newTeamQuotaTestDatabase(t)
	if err := RunMigrations(context.Background(), pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	redisServer := miniredis.RunT(t)
	redisRuntime := &integrationRedisRuntime{
		runID:           "integration-run-id",
		maxmemoryPolicy: "noeviction",
	}
	redisServer.Server().SetPreHook(func(
		peer *miniredisserver.Peer,
		command string,
		_ ...string,
	) bool {
		if command != "INFO" {
			return false
		}
		peer.WriteBulk(redisRuntime.info())
		return true
	})
	redisGuard, err := guard.NewRedis(context.Background(), guard.Config{
		URL:       "redis://" + redisServer.Addr() + "/0",
		KeyPrefix: coordinatorTestRedisPrefix,
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("guard.NewRedis() error = %v", err)
	}
	policyGuard := &integrationPolicyGuard{
		Redis: redisGuard,
	}
	identityMaintainer := &integrationIdentityMaintainer{}
	coordinator := &PolicyCoordinator{
		pool:                    pool,
		repository:              NewRepository(pool),
		policyGuard:             policyGuard,
		stateIdentityMaintainer: identityMaintainer,
		leaseTTL:                leaseTTL,
		repairInterval:          time.Second,
		now:                     time.Now,
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	return &coordinatorFixture{
		pool:               pool,
		redis:              redisServer,
		redisRuntime:       redisRuntime,
		policyGuard:        policyGuard,
		identityMaintainer: identityMaintainer,
		coordinator:        coordinator,
	}
}

type integrationPolicyGuard struct {
	*guard.Redis

	mu             sync.Mutex
	nextPendingErr error
}

type integrationIdentityMaintainer struct {
	mu       sync.Mutex
	result   RegionStateIdentityMaintenanceResult
	err      error
	onVerify func()
}

type identityMaintainerFunc func(
	context.Context,
	*pgxpool.Conn,
) (RegionStateIdentityMaintenanceResult, error)

func (f identityMaintainerFunc) verifyAndRepairOnConn(
	ctx context.Context,
	conn *pgxpool.Conn,
) (RegionStateIdentityMaintenanceResult, error) {
	return f(ctx, conn)
}

type integrationRedisRuntime struct {
	mu              sync.Mutex
	runID           string
	maxmemoryPolicy string
	evictedKeys     int64
}

func (m *integrationIdentityMaintainer) verifyAndRepairOnConn(
	context.Context,
	*pgxpool.Conn,
) (RegionStateIdentityMaintenanceResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.onVerify != nil {
		m.onVerify()
	}
	result := m.result
	m.result = RegionStateIdentityMaintenanceResult{}
	return result, m.err
}

func (m *integrationIdentityMaintainer) setError(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
}

func (m *integrationIdentityMaintainer) setResult(
	result RegionStateIdentityMaintenanceResult,
) {
	m.mu.Lock()
	m.result = result
	m.mu.Unlock()
}

func (r *integrationRedisRuntime) info() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprintf(
		"# Server\r\nrun_id:%s\r\n# Memory\r\nmaxmemory_policy:%s\r\n# Stats\r\nevicted_keys:%d\r\n",
		r.runID,
		r.maxmemoryPolicy,
		r.evictedKeys,
	)
}

func (r *integrationRedisRuntime) setEvictedKeys(value int64) {
	r.mu.Lock()
	r.evictedKeys = value
	r.mu.Unlock()
}

func (g *integrationPolicyGuard) SetPending(
	ctx context.Context,
	version guard.Version,
	token string,
	quarantineUntil time.Time,
) error {
	g.mu.Lock()
	err := g.nextPendingErr
	g.nextPendingErr = nil
	g.mu.Unlock()
	if err != nil {
		return err
	}
	return g.Redis.SetPending(ctx, version, token, quarantineUntil)
}

func (g *integrationPolicyGuard) failNextPending(err error) {
	g.mu.Lock()
	g.nextPendingErr = err
	g.mu.Unlock()
}

func loadCoordinatorPolicyState(t *testing.T, pool *pgxpool.Pool) PolicyState {
	t.Helper()
	state, err := loadPolicyState(context.Background(), pool, false)
	if err != nil {
		t.Fatalf("loadPolicyState() error = %v", err)
	}
	return state
}

func effectiveCoordinatorPolicy(
	t *testing.T,
	coordinator *PolicyCoordinator,
	key Key,
) Policy {
	t.Helper()
	return effectiveCoordinatorPolicyForTeam(t, coordinator, "team-a", key)
}

func effectiveCoordinatorPolicyForTeam(
	t *testing.T,
	coordinator *PolicyCoordinator,
	teamID string,
	key Key,
) Policy {
	t.Helper()
	policy, err := coordinator.EffectivePolicy(context.Background(), teamID, key)
	if err != nil || policy == nil {
		t.Fatalf("EffectivePolicy(%q, %q) = (%+v, %v)", teamID, key, policy, err)
	}
	return *policy
}

func assertCoordinatorStateUnchanged(
	t *testing.T,
	pool *pgxpool.Pool,
	want PolicyState,
) {
	t.Helper()
	got := loadCoordinatorPolicyState(t, pool)
	if got.EnforcementEpoch != want.EnforcementEpoch ||
		got.RedisGeneration != want.RedisGeneration ||
		!got.DefaultsOwnerEpoch.Equal(want.DefaultsOwnerEpoch) ||
		got.DefaultsGeneration != want.DefaultsGeneration ||
		got.DefaultsSHA256 != want.DefaultsSHA256 {
		t.Fatalf("durable policy state changed: got %+v, want %+v", got, want)
	}
}

func assertCoordinatorStableVersion(
	t *testing.T,
	fixture *coordinatorFixture,
	want guard.Version,
) {
	t.Helper()
	state, err := fixture.policyGuard.ReadPolicyGuard(context.Background())
	if err != nil {
		t.Fatalf("ReadPolicyGuard() error = %v", err)
	}
	if !state.Stable() || !state.Version.Equal(want) {
		t.Fatalf("policy guard = %+v, want stable version %+v", state, want)
	}
}

func waitForCoordinatorGuardPhase(
	t *testing.T,
	policyGuard guard.Reader,
	phase string,
	timeout time.Duration,
) guard.State {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := policyGuard.ReadPolicyGuard(context.Background())
		if err == nil && state.Phase == phase {
			return state
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("policy guard did not reach phase %q within %s", phase, timeout)
	return guard.State{}
}

func waitForCoordinatorResetPending(
	t *testing.T,
	policyGuard guard.Reader,
	version guard.Version,
	timeout time.Duration,
) guard.State {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := policyGuard.ReadPolicyGuard(context.Background())
		if err == nil &&
			state.Phase == guard.PhasePending &&
			state.Version.Equal(version) &&
			!state.QuarantineUntil.IsZero() {
			return state
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf(
		"policy guard did not reach reset-pending version %+v within %s",
		version,
		timeout,
	)
	return guard.State{}
}
