package teamquota

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

func TestPolicyStateValidateRejectsCorruptFences(t *testing.T) {
	validInitialized := PolicyState{
		EnforcementEpoch: 1,
		RedisGeneration:  1,
		RedisInitialized: true,
		RedisRunID:       "run-1",
	}
	validDefaults := validInitialized
	validDefaults.DefaultsOwnerEpoch = time.Date(2026, time.July, 19, 0, 0, 0, 0, time.UTC)
	validDefaults.DefaultsGeneration = 1
	validDefaults.DefaultsSHA256 = strings.Repeat("a", 64)

	tests := []struct {
		name  string
		state PolicyState
	}{
		{
			name:  "non-positive enforcement epoch",
			state: PolicyState{},
		},
		{
			name: "initialized zero generation",
			state: PolicyState{
				EnforcementEpoch: 1,
				RedisInitialized: true,
				RedisRunID:       "run-1",
			},
		},
		{
			name: "initialized missing run ID",
			state: PolicyState{
				EnforcementEpoch: 1,
				RedisGeneration:  1,
				RedisInitialized: true,
			},
		},
		{
			name: "initialized negative eviction baseline",
			state: func() PolicyState {
				state := validInitialized
				state.RedisEvictedKeys = -1
				return state
			}(),
		},
		{
			name: "uninitialized positive generation",
			state: PolicyState{
				EnforcementEpoch: 1,
				RedisGeneration:  1,
			},
		},
		{
			name: "uninitialized run ID",
			state: PolicyState{
				EnforcementEpoch: 1,
				RedisRunID:       "run-1",
			},
		},
		{
			name: "uninitialized eviction baseline",
			state: PolicyState{
				EnforcementEpoch: 1,
				RedisEvictedKeys: 1,
			},
		},
		{
			name: "refill without reset",
			state: func() PolicyState {
				state := validInitialized
				state.RateRefillFrom = time.Now()
				return state
			}(),
		},
		{
			name: "partial defaults generation",
			state: func() PolicyState {
				state := validInitialized
				state.DefaultsGeneration = 1
				return state
			}(),
		},
		{
			name: "partial defaults owner",
			state: func() PolicyState {
				state := validInitialized
				state.DefaultsOwnerEpoch = time.Now()
				return state
			}(),
		},
		{
			name: "uppercase defaults SHA",
			state: func() PolicyState {
				state := validDefaults
				state.DefaultsSHA256 = strings.Repeat("A", 64)
				return state
			}(),
		},
		{
			name: "non-hex defaults SHA",
			state: func() PolicyState {
				state := validDefaults
				state.DefaultsSHA256 = strings.Repeat("z", 64)
				return state
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.state.validate(); err == nil {
				t.Fatalf("PolicyState.validate() accepted %+v", tt.state)
			}
		})
	}
	for name, state := range map[string]PolicyState{
		"uninitialized": {
			EnforcementEpoch: 1,
		},
		"initialized": validInitialized,
		"initialized with proof reset only": func() PolicyState {
			state := validInitialized
			state.RedisResetAt = time.Now()
			return state
		}(),
		"initialized with reset": func() PolicyState {
			state := validInitialized
			state.RedisResetAt = time.Now()
			state.RateRefillFrom = state.RedisResetAt
			return state
		}(),
		"initialized with defaults": validDefaults,
	} {
		t.Run("valid "+name, func(t *testing.T) {
			if err := state.validate(); err != nil {
				t.Fatalf("PolicyState.validate() error = %v", err)
			}
		})
	}
}

func TestCompareDefaultVersionNormalizesPostgresTimestampPrecision(t *testing.T) {
	ownerEpoch := time.Date(2026, time.July, 19, 1, 2, 3, 456_789_987, time.FixedZone("test", 8*60*60))
	storedEpoch := ownerEpoch.UTC().Truncate(time.Microsecond)
	state := PolicyState{
		DefaultsOwnerEpoch: storedEpoch,
		DefaultsGeneration: 7,
		DefaultsSHA256:     "stored",
	}

	decision, err := compareDefaultVersion(
		state,
		DefaultPolicyVersion{OwnerEpoch: ownerEpoch, Generation: 7},
		"requested",
	)
	var conflict *DefaultPolicyHashConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("compareDefaultVersion() error = %v, want hash conflict", err)
	}
	if decision != 0 {
		t.Fatalf("compareDefaultVersion() decision = %v, want zero on conflict", decision)
	}
	if !conflict.Version.OwnerEpoch.Equal(storedEpoch) {
		t.Fatalf(
			"conflict owner epoch = %s, want PostgreSQL precision %s",
			conflict.Version.OwnerEpoch.Format(time.RFC3339Nano),
			storedEpoch.Format(time.RFC3339Nano),
		)
	}

	decision, err = compareDefaultVersion(
		state,
		DefaultPolicyVersion{OwnerEpoch: ownerEpoch, Generation: 7},
		"stored",
	)
	if err != nil || decision != defaultFenceIdempotent {
		t.Fatalf(
			"compareDefaultVersion() = (%v, %v), want (idempotent, nil)",
			decision,
			err,
		)
	}
}

func TestResetPendingMustMatchDurableStateExactly(t *testing.T) {
	resetAt := time.Date(2026, time.July, 19, 1, 2, 3, 456_789_000, time.UTC)
	leaseTTL := 15 * time.Second
	state := PolicyState{
		EnforcementEpoch: 8,
		RedisGeneration:  4,
		RedisRunID:       "run-4",
		RedisEvictedKeys: 7,
		RedisResetAt:     resetAt,
		RateRefillFrom:   resetAt,
	}
	pending := guard.State{
		Phase:            guard.PhasePending,
		Version:          state.Version(),
		RedisRunID:       state.RedisRunID,
		RedisEvictedKeys: state.RedisEvictedKeys,
		PendingToken:     "reset",
		ResetAt:          resetAt.Truncate(time.Millisecond),
		RateRefillFrom:   resetAt.Truncate(time.Millisecond),
		QuarantineUntil:  resetAt.Truncate(time.Millisecond).Add(leaseTTL),
	}
	if !resetPendingMatchesState(pending, state, leaseTTL) {
		t.Fatal("resetPendingMatchesState() = false for committed reset")
	}

	tests := []struct {
		name   string
		mutate func(*guard.State)
	}{
		{
			name: "uncommitted generation",
			mutate: func(candidate *guard.State) {
				candidate.Version.RedisGeneration++
			},
		},
		{
			name: "wrong Redis run ID",
			mutate: func(candidate *guard.State) {
				candidate.RedisRunID = "other-run"
			},
		},
		{
			name: "wrong eviction baseline",
			mutate: func(candidate *guard.State) {
				candidate.RedisEvictedKeys++
			},
		},
		{
			name: "pre-commit reset metadata",
			mutate: func(candidate *guard.State) {
				candidate.ResetAt = resetAt.Add(-time.Minute)
			},
		},
		{
			name: "wrong refill origin",
			mutate: func(candidate *guard.State) {
				candidate.RateRefillFrom = resetAt.Add(-time.Second)
			},
		},
		{
			name: "short quarantine",
			mutate: func(candidate *guard.State) {
				candidate.QuarantineUntil = resetAt.Add(leaseTTL - time.Millisecond)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := pending
			tt.mutate(&candidate)
			if resetPendingMatchesState(candidate, state, leaseTTL) {
				t.Fatalf("resetPendingMatchesState() = true for %s", tt.name)
			}
		})
	}
}

func TestPolicyCoordinatorResetQuarantineIncludesLocalCreditDrain(t *testing.T) {
	shortLease := &PolicyCoordinator{leaseTTL: 80 * time.Millisecond}
	if got := shortLease.resetQuarantineTTL(); got != guard.MaxLocalCreditTTL {
		t.Fatalf(
			"short-lease reset quarantine = %s, want local credit maximum %s",
			got,
			guard.MaxLocalCreditTTL,
		)
	}
	longLease := &PolicyCoordinator{leaseTTL: time.Second}
	if got := longLease.resetQuarantineTTL(); got != time.Second {
		t.Fatalf("long-lease reset quarantine = %s, want 1s", got)
	}
}
