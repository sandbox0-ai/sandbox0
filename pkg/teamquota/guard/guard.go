// Package guard defines the Redis policy barrier shared by distributed Team
// Quota policy caches and admission scripts.
package guard

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
)

// MaxLocalCreditTTL is the longest interval for which any rate enforcer may
// spend a Redis-granted credit without another distributed check. Policy
// writers and team deletion use this value as their drain barrier.
const MaxLocalCreditTTL = 100 * time.Millisecond

// LuaRuntimeSafetyHelpers defines the runtime-safety check shared by every admission
// script. INFO is intentionally evaluated inside the same Lua execution as
// the guarded mutation. The cumulative eviction baseline detects state loss
// even if an unsafe maxmemory policy was restored to noeviction before the
// next admission.
const LuaRuntimeSafetyHelpers = `
local function team_quota_guard_runtime_status(guard_key)
  local info = redis.call("INFO", "server", "memory", "stats")
  local guard_run_id = redis.call("HGET", guard_key, "redis_run_id")
  local current_run_id = string.match(info, "\nrun_id:([^\r\n]+)")
  if guard_run_id == false or guard_run_id == ""
      or current_run_id == nil or current_run_id == "" then
    return -2
  end
  if guard_run_id ~= current_run_id then
    return -1
  end

  local maxmemory_policy = string.match(info, "\nmaxmemory_policy:([^\r\n]+)")
  if maxmemory_policy == nil or string.lower(maxmemory_policy) ~= "noeviction" then
    return -2
  end

  local guard_evicted_keys = redis.call("HGET", guard_key, "redis_evicted_keys")
  local current_evicted_keys = string.match(info, "\nevicted_keys:([0-9]+)")
  if guard_evicted_keys == false or guard_evicted_keys == ""
      or current_evicted_keys == nil
      or guard_evicted_keys ~= current_evicted_keys then
    return -2
  end
  return 1
end
`

const (
	PhaseStable  = "stable"
	PhasePending = "pending"
)

var (
	ErrMissing = errors.New("team quota policy guard is missing")
	ErrPending = errors.New("team quota policy guard is pending")
	ErrStale   = errors.New("team quota policy generation is stale")
	ErrCorrupt = errors.New("team quota policy guard is corrupt")
)

// Version identifies one durable policy state and one Redis state incarnation.
type Version struct {
	EnforcementEpoch int64
	RedisGeneration  int64
}

// Validate rejects versions that cannot identify initialized policy state.
func (v Version) Validate() error {
	if v.EnforcementEpoch <= 0 {
		return fmt.Errorf("enforcement epoch must be positive")
	}
	if v.RedisGeneration <= 0 {
		return fmt.Errorf("Redis generation must be positive")
	}
	return nil
}

// Equal reports exact version equality. Numeric ordering is intentionally not
// used by enforcers: only the durable version published as stable is accepted.
func (v Version) Equal(other Version) bool {
	return v.EnforcementEpoch == other.EnforcementEpoch &&
		v.RedisGeneration == other.RedisGeneration
}

// State is the complete non-expiring Redis guard value.
type State struct {
	Phase            string
	Version          Version
	RedisRunID       string
	RedisEvictedKeys int64
	PendingToken     string
	ResetAt          time.Time
	RateRefillFrom   time.Time
	QuarantineUntil  time.Time
}

// Stable reports whether admissions may validate against this state.
func (s State) Stable() bool {
	return s.Phase == PhaseStable
}

// Reader returns the current Redis guard without creating or repairing it.
type Reader interface {
	ReadPolicyGuard(context.Context) (State, error)
}

// Key returns the one region-scoped guard key under the claimed Redis prefix.
func Key(prefix string) string {
	return rediscache.JoinKeyPrefix(prefix, "policy-guard")
}

// Read uses an existing client to load and validate the policy guard.
func Read(ctx context.Context, client redis.Cmdable, key string) (State, error) {
	values, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		return State{}, fmt.Errorf("read Team Quota policy guard: %w", err)
	}
	if len(values) == 0 {
		return State{}, ErrMissing
	}
	return Decode(values)
}

// Decode validates one HGETALL response.
func Decode(values map[string]string) (State, error) {
	phase := strings.TrimSpace(values["phase"])
	if phase != PhaseStable && phase != PhasePending {
		return State{}, fmt.Errorf("%w: unknown phase %q", ErrCorrupt, phase)
	}
	epoch, err := positiveInt64(values["enforcement_epoch"], "enforcement_epoch")
	if err != nil {
		return State{}, err
	}
	generation, err := positiveInt64(values["redis_generation"], "redis_generation")
	if err != nil {
		return State{}, err
	}
	state := State{
		Phase:        phase,
		Version:      Version{EnforcementEpoch: epoch, RedisGeneration: generation},
		RedisRunID:   strings.TrimSpace(values["redis_run_id"]),
		PendingToken: strings.TrimSpace(values["pending_token"]),
	}
	if state.RedisRunID == "" {
		return State{}, fmt.Errorf("%w: redis_run_id is required", ErrCorrupt)
	}
	if state.RedisEvictedKeys, err = nonNegativeInt64(
		values["redis_evicted_keys"],
		"redis_evicted_keys",
	); err != nil {
		return State{}, err
	}
	if state.ResetAt, err = unixMillis(values["reset_at_ms"], "reset_at_ms"); err != nil {
		return State{}, err
	}
	if state.RateRefillFrom, err = unixMillis(values["rate_refill_from_ms"], "rate_refill_from_ms"); err != nil {
		return State{}, err
	}
	if state.QuarantineUntil, err = unixMillis(values["quarantine_until_ms"], "quarantine_until_ms"); err != nil {
		return State{}, err
	}
	if phase == PhasePending && state.PendingToken == "" {
		return State{}, fmt.Errorf("%w: pending_token is required", ErrCorrupt)
	}
	if phase == PhaseStable && state.PendingToken != "" {
		return State{}, fmt.Errorf("%w: stable guard has pending_token", ErrCorrupt)
	}
	return state, nil
}

// Fields returns the canonical Redis hash representation.
func Fields(state State) (map[string]interface{}, error) {
	if err := state.Version.Validate(); err != nil {
		return nil, err
	}
	if state.Phase != PhaseStable && state.Phase != PhasePending {
		return nil, fmt.Errorf("invalid policy guard phase %q", state.Phase)
	}
	token := strings.TrimSpace(state.PendingToken)
	runID := strings.TrimSpace(state.RedisRunID)
	if runID == "" {
		return nil, fmt.Errorf("Redis run ID is required")
	}
	if state.RedisEvictedKeys < 0 {
		return nil, fmt.Errorf("Redis evicted keys must be non-negative")
	}
	if state.Phase == PhasePending && token == "" {
		return nil, fmt.Errorf("pending policy guard token is required")
	}
	if state.Phase == PhaseStable && token != "" {
		return nil, fmt.Errorf("stable policy guard must not have a pending token")
	}
	return map[string]interface{}{
		"phase":               state.Phase,
		"enforcement_epoch":   state.Version.EnforcementEpoch,
		"redis_generation":    state.Version.RedisGeneration,
		"redis_run_id":        runID,
		"redis_evicted_keys":  state.RedisEvictedKeys,
		"pending_token":       token,
		"reset_at_ms":         millis(state.ResetAt),
		"rate_refill_from_ms": millis(state.RateRefillFrom),
		"quarantine_until_ms": millis(state.QuarantineUntil),
	}, nil
}

func positiveInt64(raw, field string) (int64, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%w: %s must be a positive integer", ErrCorrupt, field)
	}
	return value, nil
}

func nonNegativeInt64(raw, field string) (int64, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%w: %s must be a non-negative integer", ErrCorrupt, field)
	}
	return value, nil
}

func unixMillis(raw, field string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "0" {
		return time.Time{}, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return time.Time{}, fmt.Errorf("%w: %s must be a non-negative integer", ErrCorrupt, field)
	}
	return time.UnixMilli(value).UTC(), nil
}

func millis(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UTC().UnixMilli()
}
