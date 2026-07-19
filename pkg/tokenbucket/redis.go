package tokenbucket

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

const (
	DefaultRedisKeyPrefix = "sandbox0:tokenbucket"
	DefaultRedisTimeout   = 100 * time.Millisecond
	minimumRedisBucketTTL = time.Minute
)

// RedisConfig configures a distributed token bucket.
type RedisConfig struct {
	URL       string
	KeyPrefix string
	Timeout   time.Duration
}

// RedisBucket stores token bucket state in Redis.
type RedisBucket struct {
	mu        sync.RWMutex
	client    *redis.Client
	keyPrefix string
	guardKey  string
	timeout   time.Duration
}

var _ GuardedBucket = (*RedisBucket)(nil)

var redisTakeNGuardedScript = redis.NewScript(guard.LuaRuntimeSafetyHelpers + `
local key = KEYS[1]
local guard_key = KEYS[2]
local admission_key = KEYS[3]
local refill_tokens = tonumber(ARGV[1])
local interval_us = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local revision = ARGV[4]
local cost = tonumber(ARGV[5])
local ttl_ms = tonumber(ARGV[6])
local expected_epoch = ARGV[7]
local expected_generation = ARGV[8]
local refill_from_ms = tonumber(ARGV[9])

if redis.call("EXISTS", guard_key) == 0 then
  return {-3, 0, 0, 0}
end
local guard_phase = redis.call("HGET", guard_key, "phase")
if guard_phase == "pending" then
  return {-2, 0, 0, 0}
end
if guard_phase ~= "stable" then
  return {-3, 0, 0, 0}
end
local runtime_status = team_quota_guard_runtime_status(guard_key)
if runtime_status == -1 then
  return {-1, 0, 0, 0}
end
if runtime_status ~= 1 then
  return {-7, 0, 0, 0}
end
if redis.call("HGET", guard_key, "enforcement_epoch") ~= expected_epoch
    or redis.call("HGET", guard_key, "redis_generation") ~= expected_generation then
  return {-1, 0, 0, 0}
end

local admission = redis.call("GET", admission_key)
if admission == false then
  return {-4, 0, 0, 0}
end
if admission == "disabled" then
  return {-5, 0, 0, 0}
end
if admission ~= "active" then
  return {-6, 0, 0, 0}
end

local time_reply = redis.call("TIME")
local now_us = (tonumber(time_reply[1]) * 1000000) + tonumber(time_reply[2])

local tokens = tonumber(redis.call("HGET", key, "tokens"))
local updated_us = tonumber(redis.call("HGET", key, "updated_us"))
local stored_revision = redis.call("HGET", key, "revision")
local stored_generation = redis.call("HGET", key, "redis_generation")

if tokens == nil or stored_generation == false or stored_generation ~= expected_generation then
  if refill_from_ms > 0 then
    local reset_elapsed_us = math.max(0, now_us - (refill_from_ms * 1000))
    tokens = math.min(burst, (reset_elapsed_us / interval_us) * refill_tokens)
  else
    tokens = burst
  end
  updated_us = now_us
else
  if updated_us == nil then
    updated_us = now_us
  end
  if stored_revision ~= false and stored_revision ~= revision then
    tokens = math.min(tokens, burst)
    updated_us = now_us
  end
  local elapsed_us = math.max(0, now_us - updated_us)
  local refill = (elapsed_us / interval_us) * refill_tokens
  tokens = math.min(burst, tokens + refill)
end

local allowed = 0
local retry_after_us = 0
if tokens >= cost then
  allowed = 1
  tokens = tokens - cost
else
  local deficit = cost - tokens
  retry_after_us = math.ceil((deficit / refill_tokens) * interval_us)
  if retry_after_us < 1 then
    retry_after_us = 1
  end
end

redis.call(
  "HSET",
  key,
  "tokens", tokens,
  "updated_us", now_us,
  "revision", revision,
  "redis_generation", expected_generation
)
redis.call("PEXPIRE", key, ttl_ms)

return {1, allowed, math.floor(tokens), retry_after_us}
`)

var redisTakeNScript = redis.NewScript(`
local key = KEYS[1]
local refill_tokens = tonumber(ARGV[1])
local interval_us = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local revision = ARGV[4]
local cost = tonumber(ARGV[5])
local ttl_ms = tonumber(ARGV[6])

local time_reply = redis.call("TIME")
local now_us = (tonumber(time_reply[1]) * 1000000) + tonumber(time_reply[2])

local tokens = tonumber(redis.call("HGET", key, "tokens"))
local updated_us = tonumber(redis.call("HGET", key, "updated_us"))
local stored_revision = redis.call("HGET", key, "revision")

if tokens == nil then
  tokens = burst
end
if updated_us == nil then
  updated_us = now_us
end

if stored_revision ~= false and stored_revision ~= revision then
  tokens = math.min(tokens, burst)
  updated_us = now_us
end

local elapsed_us = math.max(0, now_us - updated_us)
local refill = (elapsed_us / interval_us) * refill_tokens
tokens = math.min(burst, tokens + refill)

local allowed = 0
local retry_after_us = 0
if tokens >= cost then
  allowed = 1
  tokens = tokens - cost
else
  local deficit = cost - tokens
  retry_after_us = math.ceil((deficit / refill_tokens) * interval_us)
  if retry_after_us < 1 then
    retry_after_us = 1
  end
end

redis.call(
  "HSET",
  key,
  "tokens", tokens,
  "updated_us", now_us,
  "revision", revision
)
redis.call("PEXPIRE", key, ttl_ms)

return {allowed, math.floor(tokens), retry_after_us}
`)

// NewRedisBucket creates a fail-closed Redis-backed token bucket.
func NewRedisBucket(ctx context.Context, cfg RedisConfig) (*RedisBucket, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, fmt.Errorf("redis token bucket requires redis URL")
	}
	keyPrefix := strings.TrimSpace(cfg.KeyPrefix)
	if keyPrefix == "" {
		keyPrefix = DefaultRedisKeyPrefix
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultRedisTimeout
	}
	client, normalized, err := rediscache.NewClient(ctx, rediscache.Config{
		URL:       cfg.URL,
		KeyPrefix: keyPrefix,
		Timeout:   timeout,
		FailOpen:  false,
	})
	if err != nil {
		return nil, fmt.Errorf("create redis token bucket: %w", err)
	}
	return &RedisBucket{
		client:    client,
		keyPrefix: normalized.KeyPrefix,
		guardKey:  guard.Key(normalized.KeyPrefix),
		timeout:   normalized.Timeout,
	}, nil
}

// ReadPolicyGuard lets the policy cache reuse the bucket's Redis client.
func (b *RedisBucket) ReadPolicyGuard(ctx context.Context) (guard.State, error) {
	if b == nil {
		return guard.State{}, ErrClosed
	}
	b.mu.RLock()
	client := b.client
	b.mu.RUnlock()
	if client == nil {
		return guard.State{}, ErrClosed
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, b.timeout)
	defer cancel()
	return guard.Read(callCtx, client, b.guardKey)
}

// TakeN immediately attempts to consume tokens. A denied attempt records the
// current refill state but never reserves future tokens.
func (b *RedisBucket) TakeN(ctx context.Context, key string, policy Policy, tokens int64) (Decision, error) {
	if err := validateTake(key, policy, tokens); err != nil {
		return Decision{}, err
	}
	if b == nil {
		return Decision{}, ErrClosed
	}

	b.mu.RLock()
	client := b.client
	b.mu.RUnlock()
	if client == nil {
		return Decision{}, ErrClosed
	}

	callCtx, cancel := rediscache.WithTimeout(ctx, b.timeout)
	defer cancel()
	result, err := redisTakeNScript.Run(
		callCtx,
		client,
		[]string{b.redisKey(key)},
		policy.Tokens,
		policy.Interval.Microseconds(),
		policy.Burst,
		strconv.FormatInt(policy.Revision, 10),
		tokens,
		redisBucketTTL(policy).Milliseconds(),
	).Result()
	if err != nil {
		return Decision{}, fmt.Errorf("take Redis token bucket tokens: %w", err)
	}
	return decodeDecision(result)
}

// TakeNGuarded validates Team Quota policy state atomically before touching a
// distinct guarded bucket namespace.
func (b *RedisBucket) TakeNGuarded(
	ctx context.Context,
	key string,
	admissionKey string,
	policy Policy,
	version guard.Version,
	rateRefillFrom time.Time,
	tokens int64,
) (Decision, error) {
	if err := validateTake(key, policy, tokens); err != nil {
		return Decision{}, err
	}
	if strings.TrimSpace(admissionKey) == "" {
		return Decision{}, fmt.Errorf("%w: admission marker key is required", ErrInvalidKey)
	}
	if err := version.Validate(); err != nil {
		return Decision{}, fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
	}
	if b == nil {
		return Decision{}, ErrClosed
	}
	b.mu.RLock()
	client := b.client
	b.mu.RUnlock()
	if client == nil {
		return Decision{}, ErrClosed
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, b.timeout)
	defer cancel()
	refillFromMillis := int64(0)
	if !rateRefillFrom.IsZero() {
		refillFromMillis = rateRefillFrom.UTC().UnixMilli()
	}
	result, err := redisTakeNGuardedScript.Run(
		callCtx,
		client,
		[]string{b.redisGuardedKey(key), b.guardKey, admissionKey},
		policy.Tokens,
		policy.Interval.Microseconds(),
		policy.Burst,
		strconv.FormatInt(policy.Revision, 10),
		tokens,
		redisBucketTTL(policy).Milliseconds(),
		strconv.FormatInt(version.EnforcementEpoch, 10),
		strconv.FormatInt(version.RedisGeneration, 10),
		refillFromMillis,
	).Result()
	if err != nil {
		return Decision{}, fmt.Errorf("take Redis token bucket tokens: %w", err)
	}

	values, ok := result.([]interface{})
	if !ok || len(values) != 4 {
		return Decision{}, fmt.Errorf("unexpected Redis token bucket response: %T", result)
	}
	guardResult, err := redisInt64(values[0])
	if err != nil {
		return Decision{}, fmt.Errorf("decode Redis token bucket guard result: %w", err)
	}
	switch guardResult {
	case -7:
		return Decision{}, guard.ErrCorrupt
	case -6:
		return Decision{}, ErrAdmissionCorrupt
	case -5:
		return Decision{}, ErrAdmissionDisabled
	case -4:
		return Decision{}, ErrAdmissionMissing
	case -3:
		return Decision{}, guard.ErrMissing
	case -2:
		return Decision{}, guard.ErrPending
	case -1:
		return Decision{}, guard.ErrStale
	case 1:
	default:
		return Decision{}, fmt.Errorf("unexpected Redis token bucket guard result %d", guardResult)
	}
	allowed, err := redisInt64(values[1])
	if err != nil {
		return Decision{}, fmt.Errorf("decode Redis token bucket allowed value: %w", err)
	}
	remaining, err := redisInt64(values[2])
	if err != nil {
		return Decision{}, fmt.Errorf("decode Redis token bucket remaining value: %w", err)
	}
	retryAfterMicros, err := redisInt64(values[3])
	if err != nil {
		return Decision{}, fmt.Errorf("decode Redis token bucket retry value: %w", err)
	}
	if allowed != 0 && allowed != 1 {
		return Decision{}, fmt.Errorf("unexpected Redis token bucket allowed value %d", allowed)
	}
	if remaining < 0 || retryAfterMicros < 0 {
		return Decision{}, fmt.Errorf(
			"unexpected Redis token bucket counters: remaining=%d retry_after_us=%d",
			remaining,
			retryAfterMicros,
		)
	}
	return Decision{
		Allowed:    allowed == 1,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryAfterMicros) * time.Microsecond,
	}, nil
}

func decodeDecision(result interface{}) (Decision, error) {
	values, ok := result.([]interface{})
	if !ok || len(values) != 3 {
		return Decision{}, fmt.Errorf("unexpected Redis token bucket response: %T", result)
	}
	allowed, err := redisInt64(values[0])
	if err != nil {
		return Decision{}, fmt.Errorf("decode Redis token bucket allowed value: %w", err)
	}
	remaining, err := redisInt64(values[1])
	if err != nil {
		return Decision{}, fmt.Errorf("decode Redis token bucket remaining value: %w", err)
	}
	retryAfterMicros, err := redisInt64(values[2])
	if err != nil {
		return Decision{}, fmt.Errorf("decode Redis token bucket retry value: %w", err)
	}
	if allowed != 0 && allowed != 1 {
		return Decision{}, fmt.Errorf("unexpected Redis token bucket allowed value %d", allowed)
	}
	if remaining < 0 || retryAfterMicros < 0 {
		return Decision{}, fmt.Errorf(
			"unexpected Redis token bucket counters: remaining=%d retry_after_us=%d",
			remaining,
			retryAfterMicros,
		)
	}
	return Decision{
		Allowed:    allowed == 1,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryAfterMicros) * time.Microsecond,
	}, nil
}

// Close releases the Redis client. It is safe to call more than once.
func (b *RedisBucket) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	client := b.client
	b.client = nil
	b.mu.Unlock()
	if client == nil {
		return nil
	}
	return client.Close()
}

func (b *RedisBucket) redisKey(key string) string {
	return rediscache.HashedKey(b.keyPrefix, strings.TrimSpace(key))
}

func (b *RedisBucket) redisGuardedKey(key string) string {
	return rediscache.HashedKey(
		rediscache.JoinKeyPrefix(b.keyPrefix, "guarded"),
		strings.TrimSpace(key),
	)
}

func redisBucketTTL(policy Policy) time.Duration {
	fillNanos := (float64(policy.Burst) / float64(policy.Tokens)) * float64(policy.Interval)
	ttlNanos := math.Ceil(fillNanos * 2)
	if math.IsInf(ttlNanos, 1) || ttlNanos >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	ttl := time.Duration(ttlNanos)
	if ttl < minimumRedisBucketTTL {
		return minimumRedisBucketTTL
	}
	return ttl
}

func redisInt64(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case int:
		return int64(typed), nil
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse integer %q: %w", typed, err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported integer type %T", value)
	}
}
