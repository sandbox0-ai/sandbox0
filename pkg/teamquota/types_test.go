package teamquota

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRegisteredKeysHaveKindsAndUnits(t *testing.T) {
	keys := Keys()
	if len(keys) != 21 {
		t.Fatalf("Keys() length = %d, want 21", len(keys))
	}
	for _, key := range keys {
		if !KnownKey(key) {
			t.Fatalf("KnownKey(%q) = false", key)
		}
		kind, ok := KindForKey(key)
		if !ok || (kind != KindCapacity && kind != KindConcurrency && kind != KindRate) {
			t.Fatalf("KindForKey(%q) = (%q, %t)", key, kind, ok)
		}
		if UnitForKey(key) == "" {
			t.Fatalf("UnitForKey(%q) is empty", key)
		}
	}
}

func TestCapacityPolicyValidation(t *testing.T) {
	valid := Policy{
		Key:   KeySandboxMemoryBytes,
		Kind:  KindCapacity,
		Limit: 1024,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	tests := []Policy{
		{Key: Key("unknown"), Kind: KindCapacity, Limit: 1},
		{Key: KeySandboxMemoryBytes, Kind: KindRate, Tokens: 1, IntervalMillis: 1000, Burst: 1},
		{Key: KeySandboxMemoryBytes, Kind: KindCapacity, Limit: -1},
		{Key: KeySandboxMemoryBytes, Kind: KindCapacity, Limit: 1, Burst: 1},
	}
	for _, policy := range tests {
		if err := policy.Validate(); err == nil {
			t.Fatalf("Validate(%+v) error = nil, want error", policy)
		}
	}
}

func TestRatePolicyValidation(t *testing.T) {
	valid := Policy{
		Key:            KeyAPIRequests,
		Kind:           KindRate,
		Tokens:         100,
		IntervalMillis: 1000,
		Burst:          200,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	tests := []Policy{
		{Key: KeyAPIRequests, Kind: KindRate, Tokens: 0, IntervalMillis: 1000, Burst: 1},
		{Key: KeyAPIRequests, Kind: KindRate, Tokens: 1, IntervalMillis: 0, Burst: 1},
		{Key: KeyAPIRequests, Kind: KindRate, Tokens: 2, IntervalMillis: 1000, Burst: 1},
		{Key: KeyAPIRequests, Kind: KindRate, Limit: 1, Tokens: 1, IntervalMillis: 1000, Burst: 1},
		{Key: KeyAPIRequests, Kind: KindRate, Tokens: maxExactRedisInteger + 1, IntervalMillis: 1000, Burst: maxExactRedisInteger + 1},
		{Key: KeyAPIRequests, Kind: KindRate, Tokens: 1, IntervalMillis: maxRateIntervalMillis + 1, Burst: 1},
		{Key: KeyAPIRequests, Kind: KindRate, Tokens: 1, IntervalMillis: 1000, Burst: maxExactRedisInteger + 1},
	}
	for _, policy := range tests {
		if err := policy.Validate(); err == nil {
			t.Fatalf("Validate(%+v) error = nil, want error", policy)
		}
	}
}

func TestRateIntervalMillis(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		want     int64
		wantErr  bool
	}{
		{name: "minimum", interval: time.Millisecond, want: 1},
		{name: "whole milliseconds", interval: 1500 * time.Millisecond, want: 1500},
		{name: "maximum", interval: time.Hour, want: 3_600_000},
		{name: "zero", interval: 0, wantErr: true},
		{name: "negative", interval: -time.Millisecond, wantErr: true},
		{name: "fractional millisecond", interval: 1500 * time.Microsecond, wantErr: true},
		{name: "above maximum", interval: time.Hour + time.Millisecond, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RateIntervalMillis(tt.interval)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("RateIntervalMillis(%s) = %d, want error", tt.interval, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("RateIntervalMillis(%s) error = %v", tt.interval, err)
			}
			if got != tt.want {
				t.Fatalf("RateIntervalMillis(%s) = %d, want %d", tt.interval, got, tt.want)
			}
		})
	}
}

func TestConcurrencyPolicyValidation(t *testing.T) {
	valid := Policy{
		Key:   KeyActiveConnectionCount,
		Kind:  KindConcurrency,
		Limit: 2000,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	tests := []Policy{
		{Key: KeyActiveConnectionCount, Kind: KindConcurrency, Limit: -1},
		{Key: KeyActiveConnectionCount, Kind: KindConcurrency, Limit: maxExactRedisInteger + 1},
		{Key: KeyActiveConnectionCount, Kind: KindConcurrency, Limit: 1, Burst: 1},
		{Key: KeyActiveConnectionCount, Kind: KindCapacity, Limit: 1},
	}
	for _, policy := range tests {
		if err := policy.Validate(); err == nil {
			t.Fatalf("Validate(%+v) error = nil, want error", policy)
		}
	}
}

func TestValuesRejectRateKeysAndInvalidDeltas(t *testing.T) {
	if err := (Values{KeySandboxRuntimeCount: 0}).validateCapacity(true); err != nil {
		t.Fatalf("capacity target validation error = %v", err)
	}
	if err := (Values{KeyAPIRequests: 1}).validateCapacity(true); err == nil {
		t.Fatal("rate key target validation error = nil")
	}
	if err := (Values{KeySandboxRuntimeCount: 0}).validateCapacity(false); err == nil {
		t.Fatal("zero delta validation error = nil")
	}
	if err := (Values{KeySandboxRuntimeCount: -1}).validateCapacity(true); err == nil {
		t.Fatal("negative target validation error = nil")
	}
}

func TestTransferRequestTransitionReserveValidation(t *testing.T) {
	valid := TransferRequest{
		Source: Owner{
			TeamID: "team-transfer-validation",
			Kind:   "warm_pool",
			ID:     "template-1",
		},
		Destination: Owner{
			TeamID: "team-transfer-validation",
			Kind:   "sandbox",
			ID:     "sandbox-1",
		},
		Operation: Operation{
			ID:         "claim-1",
			Kind:       "hot_claim",
			Generation: 1,
		},
		SourceDecrease: Values{
			KeySandboxRuntimeCount: 1,
		},
		DestinationTarget: Values{
			KeySandboxRuntimeCount: 1,
		},
	}

	tests := []struct {
		name              string
		transitionReserve Values
		wantError         string
	}{
		{
			name:              "nil is optional",
			transitionReserve: nil,
		},
		{
			name: "negative capacity",
			transitionReserve: Values{
				KeySandboxRuntimeCount: -1,
			},
			wantError: "transition_reserve",
		},
		{
			name: "rate key",
			transitionReserve: Values{
				KeyAPIRequests: 1,
			},
			wantError: "not a capacity key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := valid
			request.TransitionReserve = tt.transitionReserve
			err := request.validate()
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("TransferRequest.validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf(
					"TransferRequest.validate() error = %v, want containing %q",
					err,
					tt.wantError,
				)
			}
		})
	}
}

func TestQuotaErrorClassification(t *testing.T) {
	exceeded := &ExceededError{Key: KeySandboxRuntimeCount, Limit: 1, Requested: 2}
	if !IsExceeded(exceeded) || !IsExceeded(errors.Join(errors.New("wrapped"), exceeded)) {
		t.Fatal("IsExceeded() = false")
	}
	unavailable := &UnavailableError{Operation: "reserve", Err: errors.New("database down")}
	if !IsUnavailable(unavailable) || !errors.Is(unavailable, unavailable.Err) {
		t.Fatal("UnavailableError classification failed")
	}
	disabled := &TeamAdmissionDisabledError{TeamID: "team-1"}
	wrappedDisabled := &UnavailableError{Operation: "reserve", Err: disabled}
	if !IsTeamAdmissionDisabled(wrappedDisabled) || !IsUnavailable(wrappedDisabled) {
		t.Fatal("TeamAdmissionDisabledError classification failed")
	}
	deletionConflict := &TeamDeletionConflictError{TeamID: "team-1", LiveAllocations: 1}
	if !IsDeletionConflict(errors.Join(errors.New("wrapped"), deletionConflict)) {
		t.Fatal("TeamDeletionConflictError classification failed")
	}
}

func TestRateStatusMarshalsNullRemaining(t *testing.T) {
	raw, err := json.Marshal(Status{
		TeamID: "team-1",
		Key:    KeyAPIRequests,
		Kind:   KindRate,
		Source: PolicySourceDefault,
		Policy: Policy{
			Key:            KeyAPIRequests,
			Kind:           KindRate,
			Tokens:         1,
			IntervalMillis: 1000,
			Burst:          1,
		},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if !strings.Contains(string(raw), `"remaining":null`) {
		t.Fatalf("JSON = %s, want explicit null remaining", raw)
	}
	if !strings.Contains(string(raw), `"source":"default"`) {
		t.Fatalf("JSON = %s, want stable policy source", raw)
	}
}

func TestDefaultPoliciesRequireCompleteKnownKeySet(t *testing.T) {
	if _, err := normalizeDefaultPolicies([]Policy{{
		Key:   KeySandboxRuntimeCount,
		Kind:  KindCapacity,
		Limit: 1,
	}}); err == nil || !strings.Contains(err.Error(), "missing:") {
		t.Fatalf("normalizeDefaultPolicies() error = %v, want missing-key error", err)
	}
	if _, err := normalizeDefaultPolicies(completeDefaultPolicies()); err != nil {
		t.Fatalf("normalizeDefaultPolicies(complete) error = %v", err)
	}
}
