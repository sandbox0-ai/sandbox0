package proxy

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
)

const defaultNetdBandwidthRedisKeyPrefix = "sandbox0:netd:bandwidth"

var redisBandwidthReserveScript = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local bytes = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

if rate == nil or rate <= 0 or bytes == nil or bytes <= 0 then
  return 0
end
if burst == nil or burst <= 0 then
  burst = rate
end

local time_reply = redis.call("TIME")
local now_ms = (tonumber(time_reply[1]) * 1000) + math.floor(tonumber(time_reply[2]) / 1000)

local tokens = tonumber(redis.call("HGET", key, "tokens"))
local updated_ms = tonumber(redis.call("HGET", key, "updated_ms"))
if tokens == nil then
  tokens = burst
end
if updated_ms == nil then
  updated_ms = now_ms
end

local effective_now = now_ms
if updated_ms > now_ms then
  effective_now = updated_ms
else
  local delta_ms = math.max(0, now_ms - updated_ms)
  local refill = (delta_ms / 1000.0) * rate
  tokens = math.min(burst, tokens + refill)
end

local wait_ms = 0
if tokens >= bytes then
  tokens = tokens - bytes
  updated_ms = effective_now
else
  local deficit = bytes - tokens
  wait_ms = math.ceil((deficit / rate) * 1000)
  tokens = 0
  updated_ms = effective_now + wait_ms
end

redis.call("HSET", key, "tokens", tokens, "updated_ms", updated_ms)
redis.call("PEXPIRE", key, ttl_ms)

return wait_ms
`)

type redisTeamBandwidthLimiter struct {
	client      *redis.Client
	keyPrefix   string
	timeout     time.Duration
	failOpen    bool
	regionID    string
	clusterID   string
	egressRate  int64
	ingressRate int64
	burst       int64
}

func newRedisTeamBandwidthLimiter(ctx context.Context, cfg *config.NetdConfig) (*redisTeamBandwidthLimiter, error) {
	if cfg == nil || strings.TrimSpace(cfg.RedisURL) == "" ||
		(cfg.TeamEgressBandwidthBytesPerSecond <= 0 && cfg.TeamIngressBandwidthBytesPerSecond <= 0) {
		return nil, nil
	}
	basePrefix := strings.TrimSpace(cfg.RedisKeyPrefix)
	if basePrefix == "" {
		basePrefix = rediscache.DefaultKeyPrefix
	}
	prefix := rediscache.JoinKeyPrefix(basePrefix, "netd", "bandwidth")
	if prefix == "" {
		prefix = defaultNetdBandwidthRedisKeyPrefix
	}
	client, normalized, err := rediscache.NewClient(ctx, rediscache.Config{
		URL:       cfg.RedisURL,
		KeyPrefix: prefix,
		Timeout:   cfg.RedisTimeout.Duration,
		FailOpen:  cfg.RedisFailOpen,
	})
	if err != nil {
		return nil, fmt.Errorf("create ctld network runtime Redis bandwidth limiter: %w", err)
	}
	return &redisTeamBandwidthLimiter{
		client:      client,
		keyPrefix:   normalized.KeyPrefix,
		timeout:     normalized.Timeout,
		failOpen:    cfg.RedisFailOpen,
		regionID:    cfg.RegionID,
		clusterID:   cfg.ClusterID,
		egressRate:  cfg.TeamEgressBandwidthBytesPerSecond,
		ingressRate: cfg.TeamIngressBandwidthBytesPerSecond,
		burst:       cfg.TeamBandwidthBurstBytes,
	}, nil
}

func (l *redisTeamBandwidthLimiter) reserve(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) (time.Duration, error) {
	if l == nil || l.client == nil || bytes <= 0 || compiled == nil || strings.TrimSpace(compiled.TeamID) == "" {
		return 0, nil
	}
	rate := l.rate(direction)
	if rate <= 0 {
		return 0, nil
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, l.timeout)
	defer cancel()
	result, err := redisBandwidthReserveScript.Run(callCtx, l.client, []string{l.redisKey(compiled.TeamID, direction)},
		rate,
		l.burstBytes(direction),
		bytes,
		l.ttl(direction).Milliseconds(),
	).Result()
	if err != nil {
		if l.failOpen {
			return 0, nil
		}
		return 0, err
	}
	return time.Duration(redisInt64Value(result)) * time.Millisecond, nil
}

func (l *redisTeamBandwidthLimiter) rate(direction bandwidthDirection) int64 {
	if l == nil {
		return 0
	}
	switch direction {
	case bandwidthEgress:
		return l.egressRate
	case bandwidthIngress:
		return l.ingressRate
	default:
		return 0
	}
}

func (l *redisTeamBandwidthLimiter) burstBytes(direction bandwidthDirection) int64 {
	rate := l.rate(direction)
	if rate <= 0 {
		return 0
	}
	if l.burst > 0 {
		return l.burst
	}
	return rate
}

func (l *redisTeamBandwidthLimiter) ttl(direction bandwidthDirection) time.Duration {
	rate := l.rate(direction)
	if rate <= 0 {
		return time.Minute
	}
	burst := l.burstBytes(direction)
	ttl := time.Duration((burst/rate)+2) * time.Second
	if ttl < time.Minute {
		return time.Minute
	}
	return ttl
}

func (l *redisTeamBandwidthLimiter) redisKey(teamID string, direction bandwidthDirection) string {
	raw := rediscache.JoinKeyPrefix(
		"region", valueOrUnknown(l.regionID),
		"cluster", valueOrUnknown(l.clusterID),
		"team", teamID,
		"direction", string(direction),
	)
	return rediscache.HashedKey(l.keyPrefix, raw)
}

func (l *redisTeamBandwidthLimiter) Close() error {
	if l == nil || l.client == nil {
		return nil
	}
	return l.client.Close()
}

func valueOrUnknown(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
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
