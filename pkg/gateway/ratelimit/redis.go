package ratelimit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisConfig struct {
	URL       string
	KeyPrefix string
	Timeout   time.Duration
	FailOpen  bool
}

type RedisLimiter struct {
	client    *redis.Client
	keyPrefix string
	timeout   time.Duration
	failOpen  bool
}

var redisTokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local rps = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

local tokens = tonumber(redis.call("HGET", key, "tokens"))
local updated_ms = tonumber(redis.call("HGET", key, "updated_ms"))
if tokens == nil then
  tokens = burst
end
if updated_ms == nil then
  updated_ms = now_ms
end

local delta_ms = math.max(0, now_ms - updated_ms)
local refill = (delta_ms / 1000.0) * rps
tokens = math.min(burst, tokens + refill)

local allowed = 0
local retry_after_ms = 0
if tokens >= 1 then
  allowed = 1
  tokens = tokens - 1
else
  retry_after_ms = math.ceil(((1 - tokens) / rps) * 1000)
end

redis.call("HSET", key, "tokens", tokens, "updated_ms", now_ms)
redis.call("PEXPIRE", key, ttl_ms)

return {allowed, math.floor(tokens), retry_after_ms}
`)

func NewRedisLimiter(ctx context.Context, cfg RedisConfig) (*RedisLimiter, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, fmt.Errorf("redis rate limit backend requires redis URL")
	}
	options, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}
	client := redis.NewClient(options)
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultRedisTimeout
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		if !cfg.FailOpen {
			_ = client.Close()
			return nil, fmt.Errorf("connect redis rate limit backend: %w", err)
		}
	}
	prefix := strings.TrimSpace(cfg.KeyPrefix)
	if prefix == "" {
		prefix = DefaultRedisKeyPrefix
	}
	return &RedisLimiter{
		client:    client,
		keyPrefix: prefix,
		timeout:   timeout,
		failOpen:  cfg.FailOpen,
	}, nil
}

func (l *RedisLimiter) Allow(ctx context.Context, key string, limit Limit) (Decision, error) {
	limit, ok := normalizeLimit(limit)
	if !ok {
		return Decision{Allowed: true}, nil
	}
	if l == nil || l.client == nil {
		return Decision{}, ErrClosed
	}

	callCtx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	now := time.Now()
	ttl := redisTTL(limit)
	result, err := redisTokenBucketScript.Run(callCtx, l.client, []string{l.redisKey(key)},
		now.UnixMilli(),
		limit.RPS,
		limit.Burst,
		ttl.Milliseconds(),
	).Result()
	if err != nil {
		if l.failOpen {
			return Decision{Allowed: true, Limit: limit.RPS}, nil
		}
		return Decision{}, err
	}

	values, ok := result.([]interface{})
	if !ok || len(values) != 3 {
		if l.failOpen {
			return Decision{Allowed: true, Limit: limit.RPS}, nil
		}
		return Decision{}, fmt.Errorf("unexpected redis rate limit response: %T", result)
	}
	allowed := int64Value(values[0]) == 1
	retryAfter := time.Duration(int64Value(values[2])) * time.Millisecond
	return Decision{
		Allowed:    allowed,
		Limit:      limit.RPS,
		Remaining:  int(int64Value(values[1])),
		RetryAfter: retryAfter,
	}, nil
}

func (l *RedisLimiter) Close() error {
	if l == nil || l.client == nil {
		return nil
	}
	return l.client.Close()
}

func (l *RedisLimiter) redisKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return l.keyPrefix + ":" + hex.EncodeToString(sum[:])
}

func redisTTL(limit Limit) time.Duration {
	if limit.RPS <= 0 {
		return time.Minute
	}
	seconds := math.Ceil(float64(limit.Burst) / float64(limit.RPS) * 2)
	ttl := time.Duration(seconds) * time.Second
	if ttl < time.Minute {
		return time.Minute
	}
	return ttl
}

func int64Value(value interface{}) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case string:
		var out int64
		_, _ = fmt.Sscan(typed, &out)
		return out
	default:
		return 0
	}
}
