package tokenbucket

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
)

type RedisConfig struct {
	URL       string
	KeyPrefix string
	Timeout   time.Duration
	FailOpen  bool
}

type RedisBucket struct {
	client    *redis.Client
	keyPrefix string
	timeout   time.Duration
	failOpen  bool
}

var redisTakeOrReserveScript = redis.NewScript(`
local key = KEYS[1]
local tokens_per_interval = tonumber(ARGV[1])
local interval_ms = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local reserve = tonumber(ARGV[5])
local ttl_ms = tonumber(ARGV[6])

if requested == nil or requested <= 0 then
  return {1, burst, 0}
end
if tokens_per_interval == nil or tokens_per_interval <= 0 or
   interval_ms == nil or interval_ms <= 0 or burst == nil or burst <= 0 then
  return {0, 0, 0}
end

local time_reply = redis.call("TIME")
local now_ms = (tonumber(time_reply[1]) * 1000) + math.floor(tonumber(time_reply[2]) / 1000)

local tokens = tonumber(redis.call("HGET", key, "tokens"))
local remainder = tonumber(redis.call("HGET", key, "remainder"))
local updated_ms = tonumber(redis.call("HGET", key, "updated_ms"))
local stored_tokens = tonumber(redis.call("HGET", key, "tokens_per_interval"))
local stored_interval = tonumber(redis.call("HGET", key, "interval_ms"))
local stored_burst = tonumber(redis.call("HGET", key, "burst"))

if tokens == nil or stored_tokens ~= tokens_per_interval or
   stored_interval ~= interval_ms or stored_burst ~= burst then
  tokens = burst
  remainder = 0
  updated_ms = now_ms
end
if remainder == nil then
  remainder = 0
end
if updated_ms == nil then
  updated_ms = now_ms
end

if updated_ms <= now_ms and tokens < burst then
  local delta_ms = now_ms - updated_ms
  local numerator = (delta_ms * tokens_per_interval) + remainder
  local added = math.floor(numerator / interval_ms)
  remainder = numerator - (added * interval_ms)
  tokens = math.min(burst, tokens + added)
  updated_ms = now_ms
  if tokens >= burst then
    remainder = 0
  end
elseif updated_ms < now_ms then
  updated_ms = now_ms
  remainder = 0
end

local allowed = 0
local wait_ms = 0
if updated_ms <= now_ms and tokens >= requested then
  allowed = 1
  tokens = tokens - requested
else
  local effective_now = math.max(now_ms, updated_ms)
  local deficit = requested
  local available_remainder = 0
  if updated_ms <= now_ms then
    deficit = requested - tokens
    available_remainder = remainder
  end
  local numerator = math.max(0, (deficit * interval_ms) - available_remainder)
  wait_ms = math.ceil(numerator / tokens_per_interval) + (effective_now - now_ms)
  if reserve == 1 then
    allowed = 1
    tokens = 0
    remainder = 0
    updated_ms = now_ms + wait_ms
  end
end

redis.call("HSET", key,
  "tokens", tokens,
  "remainder", remainder,
  "updated_ms", updated_ms,
  "tokens_per_interval", tokens_per_interval,
  "interval_ms", interval_ms,
  "burst", burst)
redis.call("PEXPIRE", key, ttl_ms)

return {allowed, math.floor(tokens), wait_ms}
`)

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
		FailOpen:  cfg.FailOpen,
	})
	if err != nil {
		return nil, fmt.Errorf("create redis token bucket: %w", err)
	}
	return &RedisBucket{
		client:    client,
		keyPrefix: normalized.KeyPrefix,
		timeout:   normalized.Timeout,
		failOpen:  cfg.FailOpen,
	}, nil
}

func (b *RedisBucket) TryTakeN(ctx context.Context, key string, limit Limit, requested int64) (Decision, error) {
	if requested <= 0 {
		return Decision{Allowed: true, Remaining: limit.Burst}, nil
	}
	if err := limit.Validate(); err != nil {
		return Decision{}, err
	}
	values, err := b.run(ctx, key, limit, requested, false)
	if err != nil {
		if b != nil && b.failOpen {
			return Decision{Allowed: true}, nil
		}
		return Decision{}, err
	}
	return Decision{
		Allowed:    values[0] == 1,
		Remaining:  values[1],
		RetryAfter: time.Duration(values[2]) * time.Millisecond,
	}, nil
}

func (b *RedisBucket) ReserveN(ctx context.Context, key string, limit Limit, requested int64) (Reservation, error) {
	if requested <= 0 {
		return Reservation{}, nil
	}
	if err := limit.Validate(); err != nil {
		return Reservation{}, err
	}
	if limit.Tokens == 0 || limit.Burst == 0 {
		return Reservation{}, ErrLimited
	}
	values, err := b.run(ctx, key, limit, requested, true)
	if err != nil {
		if b != nil && b.failOpen {
			return Reservation{}, nil
		}
		return Reservation{}, err
	}
	return Reservation{Delay: time.Duration(values[2]) * time.Millisecond}, nil
}

func (b *RedisBucket) run(ctx context.Context, key string, limit Limit, requested int64, reserve bool) ([3]int64, error) {
	var out [3]int64
	if b == nil || b.client == nil {
		return out, ErrClosed
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, b.timeout)
	defer cancel()
	reserveValue := 0
	if reserve {
		reserveValue = 1
	}
	result, err := redisTakeOrReserveScript.Run(
		callCtx,
		b.client,
		[]string{b.redisKey(key)},
		limit.Tokens,
		limit.Interval.Milliseconds(),
		limit.Burst,
		requested,
		reserveValue,
		redisTTL(limit).Milliseconds(),
	).Result()
	if err != nil {
		return out, err
	}
	values, ok := result.([]interface{})
	if !ok || len(values) != len(out) {
		return out, fmt.Errorf("unexpected redis token bucket response: %T", result)
	}
	for i := range out {
		out[i] = redisInt64Value(values[i])
	}
	return out, nil
}

func (b *RedisBucket) Close() error {
	if b == nil || b.client == nil {
		return nil
	}
	return b.client.Close()
}

func (b *RedisBucket) redisKey(key string) string {
	return rediscache.HashedKey(b.keyPrefix, key)
}

func redisTTL(limit Limit) time.Duration {
	if limit.Tokens <= 0 {
		return time.Minute
	}
	intervals := math.Ceil(float64(limit.Burst) / float64(limit.Tokens))
	ttl := time.Duration(intervals*2) * limit.Interval
	if ttl < time.Minute {
		return time.Minute
	}
	return ttl
}

func redisInt64Value(value interface{}) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case string:
		out, _ := strconv.ParseInt(typed, 10, 64)
		return out
	default:
		return 0
	}
}
