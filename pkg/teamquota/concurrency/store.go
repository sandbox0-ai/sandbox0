// Package concurrency enforces exact region-wide live-resource limits with
// expiring Redis leases.
package concurrency

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

const (
	defaultRedisKeyPrefix = coreteamquota.DefaultRedisKeyPrefix
	defaultRedisTimeout   = 100 * time.Millisecond
	maxExactRedisInteger  = int64(1<<53 - 1)
)

var acquireScript = redis.NewScript(guard.LuaRuntimeSafetyHelpers + `
local guard_key = KEYS[1]
local expiry_key = KEYS[2]
local order_key = KEYS[3]
local meta_key = KEYS[4]
local admission_key = KEYS[5]
local expected_epoch = ARGV[1]
local expected_generation = ARGV[2]
local member = ARGV[3]
local limit = tonumber(ARGV[4])
local lease_ttl_ms = tonumber(ARGV[5])
local key_ttl_ms = tonumber(ARGV[6])

if redis.call("EXISTS", guard_key) == 0 then
  return {-3, 0, 0}
end
local guard_phase = redis.call("HGET", guard_key, "phase")
if guard_phase == "pending" then
  return {-2, 0, 0}
end
if guard_phase ~= "stable" then
  return {-3, 0, 0}
end
local runtime_status = team_quota_guard_runtime_status(guard_key)
if runtime_status == -1 then
  return {-1, 0, 0}
end
if runtime_status ~= 1 then
  return {-7, 0, 0}
end
if redis.call("HGET", guard_key, "enforcement_epoch") ~= expected_epoch
    or redis.call("HGET", guard_key, "redis_generation") ~= expected_generation then
  return {-1, 0, 0}
end

local admission = redis.call("GET", admission_key)
if admission == false then
  return {-4, 0, 0}
end
if admission == "disabled" then
  return {-5, 0, 0}
end
if admission ~= "active" then
  return {-6, 0, 0}
end

if redis.call("HGET", meta_key, "redis_generation") ~= expected_generation then
  redis.call("DEL", expiry_key, order_key, meta_key)
end

local time_reply = redis.call("TIME")
local now_ms = (tonumber(time_reply[1]) * 1000) + math.floor(tonumber(time_reply[2]) / 1000)
redis.call("ZREMRANGEBYSCORE", expiry_key, "-inf", now_ms)
redis.call("ZINTERSTORE", order_key, 2, order_key, expiry_key, "WEIGHTS", 0, 0)
redis.call("ZINTERSTORE", expiry_key, 2, expiry_key, order_key, "WEIGHTS", 1, 0, "AGGREGATE", "MAX")

local expiry_ms = now_ms + lease_ttl_ms
if redis.call("ZSCORE", expiry_key, member) ~= false and redis.call("ZSCORE", order_key, member) ~= false then
  local rank = redis.call("ZRANK", order_key, member)
  if rank == false or rank >= limit then
    redis.call("ZREM", expiry_key, member)
    redis.call("ZREM", order_key, member)
    return {1, 0, redis.call("ZCARD", order_key)}
  end
  redis.call("ZADD", expiry_key, expiry_ms, member)
  redis.call("HSET", meta_key, "redis_generation", expected_generation)
  redis.call("PEXPIRE", expiry_key, key_ttl_ms)
  redis.call("PEXPIRE", order_key, key_ttl_ms)
  redis.call("PEXPIRE", meta_key, key_ttl_ms)
  local used = redis.call("ZCARD", order_key)
  return {1, 1, used}
end

redis.call("ZREM", expiry_key, member)
redis.call("ZREM", order_key, member)
local used = redis.call("ZCARD", order_key)
if used >= limit then
  if used == 0 then
    redis.call("DEL", expiry_key, order_key, meta_key)
  else
    redis.call("PEXPIRE", expiry_key, key_ttl_ms)
    redis.call("PEXPIRE", order_key, key_ttl_ms)
    redis.call("PEXPIRE", meta_key, key_ttl_ms)
  end
  return {1, 0, used}
end

redis.call("ZADD", expiry_key, expiry_ms, member)
redis.call("ZADD", order_key, 0, member)
redis.call("HSET", meta_key, "redis_generation", expected_generation)
redis.call("PEXPIRE", expiry_key, key_ttl_ms)
redis.call("PEXPIRE", order_key, key_ttl_ms)
redis.call("PEXPIRE", meta_key, key_ttl_ms)
return {1, 1, used + 1}
`)

var renewScript = redis.NewScript(guard.LuaRuntimeSafetyHelpers + `
local guard_key = KEYS[1]
local expiry_key = KEYS[2]
local order_key = KEYS[3]
local meta_key = KEYS[4]
local admission_key = KEYS[5]
local expected_epoch = ARGV[1]
local expected_generation = ARGV[2]
local member = ARGV[3]
local lease_ttl_ms = tonumber(ARGV[4])
local limit = tonumber(ARGV[5])
local key_ttl_ms = tonumber(ARGV[6])

if redis.call("EXISTS", guard_key) == 0 then
  return -30
end
local guard_phase = redis.call("HGET", guard_key, "phase")
if guard_phase == "pending" then
  return -20
end
if guard_phase ~= "stable" then
  return -30
end
local runtime_status = team_quota_guard_runtime_status(guard_key)
if runtime_status == -1 then
  return -10
end
if runtime_status ~= 1 then
  return -70
end
if redis.call("HGET", guard_key, "enforcement_epoch") ~= expected_epoch
    or redis.call("HGET", guard_key, "redis_generation") ~= expected_generation then
  return -10
end

local admission = redis.call("GET", admission_key)
if admission == false then
  return -40
end
if admission == "disabled" then
  return -50
end
if admission ~= "active" then
  return -60
end

if redis.call("HGET", meta_key, "redis_generation") ~= expected_generation then
  redis.call("DEL", expiry_key, order_key, meta_key)
  return 0
end

local time_reply = redis.call("TIME")
local now_ms = (tonumber(time_reply[1]) * 1000) + math.floor(tonumber(time_reply[2]) / 1000)
redis.call("ZREMRANGEBYSCORE", expiry_key, "-inf", now_ms)
redis.call("ZINTERSTORE", order_key, 2, order_key, expiry_key, "WEIGHTS", 0, 0)
redis.call("ZINTERSTORE", expiry_key, 2, expiry_key, order_key, "WEIGHTS", 1, 0, "AGGREGATE", "MAX")
if redis.call("ZSCORE", expiry_key, member) == false or redis.call("ZSCORE", order_key, member) == false then
  redis.call("ZREM", expiry_key, member)
  redis.call("ZREM", order_key, member)
  return 0
end

local member_rank = redis.call("ZRANK", order_key, member)
if member_rank == false then
  return 0
end
if member_rank >= limit then
  redis.call("ZREM", expiry_key, member)
  redis.call("ZREM", order_key, member)
  return -1
end

redis.call("ZADD", expiry_key, now_ms + lease_ttl_ms, member)
redis.call("PEXPIRE", expiry_key, key_ttl_ms)
redis.call("PEXPIRE", order_key, key_ttl_ms)
redis.call("PEXPIRE", meta_key, key_ttl_ms)
return 1
`)

var usageScript = redis.NewScript(`
local expiry_key = KEYS[1]
local order_key = KEYS[2]
local meta_key = KEYS[3]
local key_ttl_ms = tonumber(ARGV[1])

local time_reply = redis.call("TIME")
local now_ms = (tonumber(time_reply[1]) * 1000) + math.floor(tonumber(time_reply[2]) / 1000)
redis.call("ZREMRANGEBYSCORE", expiry_key, "-inf", now_ms)
redis.call("ZINTERSTORE", expiry_key, 2, expiry_key, order_key, "WEIGHTS", 1, 0, "AGGREGATE", "MAX")
redis.call("ZINTERSTORE", order_key, 2, order_key, expiry_key, "WEIGHTS", 0, 0)
local used = redis.call("ZCARD", order_key)
if used == 0 then
  redis.call("DEL", expiry_key, order_key, meta_key)
else
  redis.call("PEXPIRE", expiry_key, key_ttl_ms)
  redis.call("PEXPIRE", order_key, key_ttl_ms)
  redis.call("PEXPIRE", meta_key, key_ttl_ms)
end
return used
`)

var releaseScript = redis.NewScript(`
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
if redis.call("ZCARD", KEYS[2]) == 0 then
  redis.call("DEL", KEYS[1], KEYS[2], KEYS[3])
end
return 1
`)

// StoreConfig configures the region-shared Redis lease store.
type StoreConfig struct {
	RedisURL       string
	RedisKeyPrefix string
	RedisTimeout   time.Duration
}

type leaseDecision struct {
	allowed bool
	used    int64
}

type leaseStore interface {
	guard.Reader
	Acquire(context.Context, string, string, string, int64, time.Duration, guard.Version) (leaseDecision, error)
	Renew(context.Context, string, string, string, int64, time.Duration, guard.Version) (renewDecision, error)
	Release(context.Context, string, string) error
	Usage(context.Context, string, time.Duration) (int64, error)
	Close() error
}

type renewDecision int

const (
	renewLost renewDecision = iota
	renewed
	renewOverLimit
)

var (
	errAdmissionMissing  = errors.New("team quota admission marker is missing")
	errAdmissionDisabled = errors.New("team quota admission is disabled")
	errAdmissionCorrupt  = errors.New("team quota admission marker is corrupt")
)

// RedisStore owns one Redis client used for concurrency lease state.
type RedisStore struct {
	mu        sync.RWMutex
	client    *redis.Client
	keyPrefix string
	guardKey  string
	timeout   time.Duration
}

// NewRedisStore creates a fail-closed Redis lease store.
func NewRedisStore(ctx context.Context, cfg StoreConfig) (*RedisStore, error) {
	if strings.TrimSpace(cfg.RedisURL) == "" {
		return nil, fmt.Errorf("team quota concurrency requires Redis URL")
	}
	keyPrefix := strings.TrimSpace(cfg.RedisKeyPrefix)
	if keyPrefix == "" {
		keyPrefix = defaultRedisKeyPrefix
	}
	timeout := cfg.RedisTimeout
	if timeout <= 0 {
		timeout = defaultRedisTimeout
	}
	client, normalized, err := rediscache.NewClient(ctx, rediscache.Config{
		URL:       cfg.RedisURL,
		KeyPrefix: rediscache.JoinKeyPrefix(keyPrefix, "concurrency"),
		Timeout:   timeout,
		FailOpen:  false,
	})
	if err != nil {
		return nil, fmt.Errorf("create team quota concurrency store: %w", err)
	}
	return &RedisStore{
		client:    client,
		keyPrefix: normalized.KeyPrefix,
		guardKey:  guard.Key(keyPrefix),
		timeout:   normalized.Timeout,
	}, nil
}

func (s *RedisStore) Acquire(
	ctx context.Context,
	key string,
	leaseID string,
	admissionKey string,
	limit int64,
	leaseTTL time.Duration,
	version guard.Version,
) (leaseDecision, error) {
	if strings.TrimSpace(key) == "" ||
		strings.TrimSpace(leaseID) == "" ||
		strings.TrimSpace(admissionKey) == "" {
		return leaseDecision{}, fmt.Errorf("concurrency key, lease ID, and admission key are required")
	}
	if limit < 0 {
		return leaseDecision{}, fmt.Errorf("concurrency limit must be non-negative")
	}
	if limit > maxExactRedisInteger {
		return leaseDecision{}, fmt.Errorf("concurrency limit exceeds the exact Redis integer range")
	}
	if leaseTTL <= 0 || leaseTTL%time.Millisecond != 0 {
		return leaseDecision{}, fmt.Errorf("concurrency lease TTL must use positive whole milliseconds")
	}
	if err := version.Validate(); err != nil {
		return leaseDecision{}, fmt.Errorf("invalid concurrency policy version: %w", err)
	}
	client, err := s.redisClient()
	if err != nil {
		return leaseDecision{}, err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, s.timeout)
	defer cancel()
	result, err := acquireScript.Run(
		callCtx,
		client,
		s.guardedRedisKeys(key, admissionKey),
		version.EnforcementEpoch,
		version.RedisGeneration,
		leaseID,
		limit,
		leaseTTL.Milliseconds(),
		redisKeyTTL(leaseTTL).Milliseconds(),
	).Result()
	if err != nil {
		return leaseDecision{}, fmt.Errorf("acquire Redis concurrency lease: %w", err)
	}
	values, ok := result.([]interface{})
	if !ok || len(values) != 3 {
		return leaseDecision{}, fmt.Errorf("unexpected Redis concurrency acquire response: %T", result)
	}
	guardResult, err := redisResultInt64(values[0])
	if err != nil {
		return leaseDecision{}, fmt.Errorf("decode Redis concurrency acquire guard result: %w", err)
	}
	switch guardResult {
	case -7:
		return leaseDecision{}, guard.ErrCorrupt
	case -6:
		return leaseDecision{}, errAdmissionCorrupt
	case -5:
		return leaseDecision{}, errAdmissionDisabled
	case -4:
		return leaseDecision{}, errAdmissionMissing
	case -3:
		return leaseDecision{}, guard.ErrMissing
	case -2:
		return leaseDecision{}, guard.ErrPending
	case -1:
		return leaseDecision{}, guard.ErrStale
	case 1:
	default:
		return leaseDecision{}, fmt.Errorf("unexpected Redis concurrency acquire guard result %d", guardResult)
	}
	allowed, err := redisResultInt64(values[1])
	if err != nil || (allowed != 0 && allowed != 1) {
		return leaseDecision{}, fmt.Errorf("decode Redis concurrency acquire decision: %v", err)
	}
	used, err := redisResultInt64(values[2])
	if err != nil || used < 0 || used > maxExactRedisInteger {
		return leaseDecision{}, fmt.Errorf("decode Redis concurrency usage: %v", err)
	}
	return leaseDecision{allowed: allowed == 1, used: used}, nil
}

func (s *RedisStore) Renew(
	ctx context.Context,
	key string,
	leaseID string,
	admissionKey string,
	limit int64,
	leaseTTL time.Duration,
	version guard.Version,
) (renewDecision, error) {
	if strings.TrimSpace(key) == "" ||
		strings.TrimSpace(leaseID) == "" ||
		strings.TrimSpace(admissionKey) == "" {
		return renewLost, fmt.Errorf("concurrency key, lease ID, and admission key are required")
	}
	if limit < 0 || limit > maxExactRedisInteger {
		return renewLost, fmt.Errorf("concurrency limit is outside the exact Redis integer range")
	}
	if err := version.Validate(); err != nil {
		return renewLost, fmt.Errorf("invalid concurrency policy version: %w", err)
	}
	client, err := s.redisClient()
	if err != nil {
		return renewLost, err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, s.timeout)
	defer cancel()
	result, err := renewScript.Run(
		callCtx,
		client,
		s.guardedRedisKeys(key, admissionKey),
		version.EnforcementEpoch,
		version.RedisGeneration,
		leaseID,
		leaseTTL.Milliseconds(),
		limit,
		redisKeyTTL(leaseTTL).Milliseconds(),
	).Int64()
	if err != nil {
		return renewLost, fmt.Errorf("renew Redis concurrency lease: %w", err)
	}
	switch result {
	case -70:
		return renewLost, guard.ErrCorrupt
	case -60:
		return renewLost, errAdmissionCorrupt
	case -50:
		return renewLost, errAdmissionDisabled
	case -40:
		return renewLost, errAdmissionMissing
	case -30:
		return renewLost, guard.ErrMissing
	case -20:
		return renewLost, guard.ErrPending
	case -10:
		return renewLost, guard.ErrStale
	case -1:
		return renewOverLimit, nil
	case 0:
		return renewLost, nil
	case 1:
		return renewed, nil
	default:
		return renewLost, fmt.Errorf("unexpected Redis concurrency renew response %d", result)
	}
}

func (s *RedisStore) Release(ctx context.Context, key string, leaseID string) error {
	client, err := s.redisClient()
	if err != nil {
		return err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, s.timeout)
	defer cancel()
	keys := s.redisStateKeys(key)
	if err := releaseScript.Run(callCtx, client, keys, leaseID).Err(); err != nil {
		return fmt.Errorf("release Redis concurrency lease: %w", err)
	}
	return nil
}

func (s *RedisStore) Usage(
	ctx context.Context,
	key string,
	leaseTTL time.Duration,
) (int64, error) {
	client, err := s.redisClient()
	if err != nil {
		return 0, err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, s.timeout)
	defer cancel()
	used, err := usageScript.Run(
		callCtx,
		client,
		s.redisStateKeys(key),
		redisKeyTTL(leaseTTL).Milliseconds(),
	).Int64()
	if err != nil {
		return 0, fmt.Errorf("read Redis concurrency usage: %w", err)
	}
	if used < 0 {
		return 0, fmt.Errorf("unexpected negative Redis concurrency usage %d", used)
	}
	return used, nil
}

// Close releases the Redis client.
func (s *RedisStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	client := s.client
	s.client = nil
	s.mu.Unlock()
	if client == nil {
		return nil
	}
	return client.Close()
}

func (s *RedisStore) redisClient() (*redis.Client, error) {
	if s == nil {
		return nil, fmt.Errorf("team quota concurrency store is not configured")
	}
	s.mu.RLock()
	client := s.client
	s.mu.RUnlock()
	if client == nil {
		return nil, fmt.Errorf("team quota concurrency store is closed")
	}
	return client, nil
}

// ReadPolicyGuard lets the policy cache reuse the lease store's Redis client.
func (s *RedisStore) ReadPolicyGuard(ctx context.Context) (guard.State, error) {
	client, err := s.redisClient()
	if err != nil {
		return guard.State{}, err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, s.timeout)
	defer cancel()
	return guard.Read(callCtx, client, s.guardKey)
}

func (s *RedisStore) redisStateKeys(key string) []string {
	key = strings.TrimSpace(key)
	identity := rediscache.HashedKey("", key)
	base := rediscache.JoinKeyPrefix(s.keyPrefix, "{"+identity+"}")
	return []string{
		base + ":expiry",
		base + ":order",
		base + ":meta",
	}
}

func (s *RedisStore) guardedRedisKeys(key string, admissionKey string) []string {
	keys := append([]string{s.guardKey}, s.redisStateKeys(key)...)
	return append(keys, admissionKey)
}

func redisKeyTTL(leaseTTL time.Duration) time.Duration {
	if leaseTTL > time.Duration(1<<63-1)/2 {
		return time.Duration(1<<63 - 1)
	}
	return leaseTTL * 2
}

func redisResultInt64(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported integer type %T", value)
	}
}
