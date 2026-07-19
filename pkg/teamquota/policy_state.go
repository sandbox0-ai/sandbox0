package teamquota

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

// PolicyState is the durable region policy and Redis recovery fence.
type PolicyState struct {
	EnforcementEpoch   int64
	RedisGeneration    int64
	RedisInitialized   bool
	RedisRunID         string
	RedisEvictedKeys   int64
	RedisResetAt       time.Time
	RateRefillFrom     time.Time
	DefaultsOwnerEpoch time.Time
	DefaultsGeneration int64
	DefaultsSHA256     string
}

// Version returns the exact generation accepted by distributed admission.
func (s PolicyState) Version() guard.Version {
	return guard.Version{
		EnforcementEpoch: s.EnforcementEpoch,
		RedisGeneration:  s.RedisGeneration,
	}
}

func (s PolicyState) stableGuard() guard.State {
	return guard.State{
		Phase:            guard.PhaseStable,
		Version:          s.Version(),
		RedisRunID:       s.RedisRunID,
		RedisEvictedKeys: s.RedisEvictedKeys,
		ResetAt:          s.RedisResetAt,
		RateRefillFrom:   s.RateRefillFrom,
	}
}

func (s PolicyState) validate() error {
	if s.EnforcementEpoch <= 0 {
		return fmt.Errorf("enforcement epoch must be positive")
	}
	if s.RedisInitialized {
		if s.RedisGeneration <= 0 {
			return fmt.Errorf("initialized Redis generation must be positive")
		}
		if strings.TrimSpace(s.RedisRunID) == "" {
			return fmt.Errorf("initialized Redis run ID must be non-empty")
		}
		if s.RedisEvictedKeys < 0 {
			return fmt.Errorf("initialized Redis evicted keys must be non-negative")
		}
	} else {
		if s.RedisGeneration != 0 {
			return fmt.Errorf("uninitialized Redis generation must be zero")
		}
		if strings.TrimSpace(s.RedisRunID) != "" {
			return fmt.Errorf("uninitialized Redis run ID must be empty")
		}
		if s.RedisEvictedKeys != 0 {
			return fmt.Errorf("uninitialized Redis evicted keys must be zero")
		}
	}
	if s.RedisResetAt.IsZero() && !s.RateRefillFrom.IsZero() {
		return fmt.Errorf("rate refill timestamp requires a Redis reset timestamp")
	}

	defaultsMissing := s.DefaultsOwnerEpoch.IsZero() &&
		s.DefaultsGeneration == 0 &&
		s.DefaultsSHA256 == ""
	if defaultsMissing {
		return nil
	}
	if s.DefaultsOwnerEpoch.IsZero() {
		return fmt.Errorf("defaults owner epoch must be set with defaults generation and SHA-256")
	}
	if s.DefaultsGeneration <= 0 {
		return fmt.Errorf("defaults generation must be positive")
	}
	if len(s.DefaultsSHA256) != sha256.Size*2 ||
		strings.ToLower(s.DefaultsSHA256) != s.DefaultsSHA256 {
		return fmt.Errorf("defaults SHA-256 must be 64 lowercase hexadecimal characters")
	}
	decoded, err := hex.DecodeString(s.DefaultsSHA256)
	if err != nil || len(decoded) != sha256.Size {
		return fmt.Errorf("defaults SHA-256 must be 64 lowercase hexadecimal characters")
	}
	return nil
}

func loadPolicyState(
	ctx context.Context,
	query rowQuerier,
	forUpdate bool,
) (PolicyState, error) {
	lock := ""
	if forUpdate {
		lock = " FOR UPDATE"
	}
	var (
		state              PolicyState
		redisRunID         *string
		redisEvictedKeys   *int64
		redisResetAt       *time.Time
		rateRefillFrom     *time.Time
		defaultsOwnerEpoch *time.Time
		defaultsGeneration *int64
		defaultsSHA256     *string
	)
	err := query.QueryRow(ctx, `
		SELECT
			enforcement_epoch,
			redis_generation,
			redis_initialized,
			redis_run_id,
			redis_evicted_keys,
			redis_reset_at,
			rate_refill_from,
			defaults_owner_epoch,
			defaults_generation,
			defaults_sha256
		FROM quota.policy_state
		WHERE singleton = TRUE`+lock,
	).Scan(
		&state.EnforcementEpoch,
		&state.RedisGeneration,
		&state.RedisInitialized,
		&redisRunID,
		&redisEvictedKeys,
		&redisResetAt,
		&rateRefillFrom,
		&defaultsOwnerEpoch,
		&defaultsGeneration,
		&defaultsSHA256,
	)
	if err != nil {
		return PolicyState{}, &UnavailableError{Operation: "load distributed policy state", Err: err}
	}
	if redisRunID != nil {
		state.RedisRunID = *redisRunID
	}
	if redisEvictedKeys != nil {
		state.RedisEvictedKeys = *redisEvictedKeys
	}
	if redisResetAt != nil {
		state.RedisResetAt = redisResetAt.UTC()
	}
	if rateRefillFrom != nil {
		state.RateRefillFrom = rateRefillFrom.UTC()
	}
	if defaultsOwnerEpoch != nil {
		state.DefaultsOwnerEpoch = defaultsOwnerEpoch.UTC()
	}
	if defaultsGeneration != nil {
		state.DefaultsGeneration = *defaultsGeneration
	}
	if defaultsSHA256 != nil {
		state.DefaultsSHA256 = *defaultsSHA256
	}
	if err := state.validate(); err != nil {
		return PolicyState{}, &UnavailableError{
			Operation: "validate distributed policy state",
			Err:       err,
		}
	}
	return state, nil
}

func storePolicyState(ctx context.Context, tx pgx.Tx, state PolicyState) error {
	if err := state.validate(); err != nil {
		return &UnavailableError{
			Operation: "validate distributed policy state",
			Err:       err,
		}
	}
	var (
		redisRunID         any
		redisEvictedKeys   any
		redisResetAt       any
		rateRefillFrom     any
		defaultsOwnerEpoch any
		defaultsGeneration any
		defaultsSHA256     any
	)
	if strings.TrimSpace(state.RedisRunID) != "" {
		redisRunID = strings.TrimSpace(state.RedisRunID)
		redisEvictedKeys = state.RedisEvictedKeys
	}
	if !state.RedisResetAt.IsZero() {
		redisResetAt = state.RedisResetAt.UTC()
	}
	if !state.RateRefillFrom.IsZero() {
		rateRefillFrom = state.RateRefillFrom.UTC()
	}
	if !state.DefaultsOwnerEpoch.IsZero() {
		defaultsOwnerEpoch = state.DefaultsOwnerEpoch.UTC()
		defaultsGeneration = state.DefaultsGeneration
		defaultsSHA256 = state.DefaultsSHA256
	}
	tag, err := tx.Exec(ctx, `
		UPDATE quota.policy_state
		SET
			enforcement_epoch = $1,
			redis_generation = $2,
			redis_initialized = $3,
			redis_run_id = $4,
			redis_evicted_keys = $5,
			redis_reset_at = $6,
			rate_refill_from = $7,
			defaults_owner_epoch = $8,
			defaults_generation = $9,
			defaults_sha256 = $10,
			updated_at = NOW()
		WHERE singleton = TRUE
	`,
		state.EnforcementEpoch,
		state.RedisGeneration,
		state.RedisInitialized,
		redisRunID,
		redisEvictedKeys,
		redisResetAt,
		rateRefillFrom,
		defaultsOwnerEpoch,
		defaultsGeneration,
		defaultsSHA256,
	)
	if err != nil {
		return &UnavailableError{Operation: "store distributed policy state", Err: err}
	}
	if tag.RowsAffected() != 1 {
		return &UnavailableError{
			Operation: "store distributed policy state",
			Err:       fmt.Errorf("updated %d rows, want exactly 1", tag.RowsAffected()),
		}
	}
	return nil
}

func canonicalPolicySHA256(policies []Policy) (string, error) {
	normalized := append([]Policy(nil), policies...)
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].Key < normalized[j].Key
	})
	type canonicalPolicy struct {
		Key            Key   `json:"key"`
		Kind           Kind  `json:"kind"`
		Limit          int64 `json:"limit"`
		Tokens         int64 `json:"tokens"`
		IntervalMillis int64 `json:"interval_millis"`
		Burst          int64 `json:"burst"`
	}
	canonical := make([]canonicalPolicy, 0, len(normalized))
	for _, policy := range normalized {
		canonical = append(canonical, canonicalPolicy{
			Key:            policy.Key,
			Kind:           policy.Kind,
			Limit:          policy.Limit,
			Tokens:         policy.Tokens,
			IntervalMillis: policy.IntervalMillis,
			Burst:          policy.Burst,
		})
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal canonical Team Quota defaults: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
