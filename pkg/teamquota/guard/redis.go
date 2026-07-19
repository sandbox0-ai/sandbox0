package guard

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
)

const (
	defaultTimeout = 100 * time.Millisecond

	transitionMissing = int64(0)
	transitionApplied = int64(1)
	transitionPending = int64(-1)
	transitionStale   = int64(-2)
	transitionCorrupt = int64(-3)
)

var setPendingScript = redis.NewScript(LuaRuntimeSafetyHelpers + `
local key = KEYS[1]
if redis.call("EXISTS", key) == 0 then
  return 0
end
if redis.call("HGET", key, "phase") ~= "stable" then
  return -1
end
local runtime_status = team_quota_guard_runtime_status(key)
if runtime_status == -1 then
  return -2
end
if runtime_status ~= 1 then
  return -3
end
if redis.call("HGET", key, "enforcement_epoch") ~= ARGV[1]
    or redis.call("HGET", key, "redis_generation") ~= ARGV[2] then
  return -2
end
redis.call(
  "HSET",
  key,
  "phase", "pending",
  "pending_token", ARGV[3],
  "quarantine_until_ms", ARGV[4]
)
redis.call("PERSIST", key)
return 1
`)

var setStableScript = redis.NewScript(LuaRuntimeSafetyHelpers + `
local key = KEYS[1]
if redis.call("EXISTS", key) == 0 then
  return 0
end
if redis.call("HGET", key, "phase") ~= "pending"
    or redis.call("HGET", key, "pending_token") ~= ARGV[1] then
  return -1
end
local runtime_status = team_quota_guard_runtime_status(key)
if runtime_status == -1 then
  return -2
end
if runtime_status ~= 1 then
  return -3
end
redis.call(
  "HSET",
  key,
  "phase", "stable",
  "enforcement_epoch", ARGV[2],
  "redis_generation", ARGV[3],
	"redis_run_id", ARGV[4],
  "redis_evicted_keys", ARGV[5],
  "pending_token", "",
  "reset_at_ms", ARGV[6],
  "rate_refill_from_ms", ARGV[7],
  "quarantine_until_ms", "0"
)
redis.call("PERSIST", key)
return 1
`)

// Config identifies the region Redis guard.
type Config struct {
	URL       string
	KeyPrefix string
	Timeout   time.Duration
}

// Redis owns the policy-owner guard client. Distributed consumers normally
// read the same key through their existing bucket or lease-store clients.
type Redis struct {
	mu      sync.RWMutex
	client  *redis.Client
	key     string
	timeout time.Duration
}

// RuntimeSafety is the Redis process identity and cumulative eviction fence
// observed in one INFO response.
type RuntimeSafety struct {
	RunID       string
	EvictedKeys int64
}

// NewRedis creates a fail-closed guard client without creating guard state.
func NewRedis(ctx context.Context, cfg Config) (*Redis, error) {
	client, normalized, err := rediscache.NewClient(ctx, rediscache.Config{
		URL:       cfg.URL,
		KeyPrefix: cfg.KeyPrefix,
		Timeout:   cfg.Timeout,
		FailOpen:  false,
	})
	if err != nil {
		return nil, fmt.Errorf("create Team Quota policy guard client: %w", err)
	}
	timeout := normalized.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return &Redis{
		client:  client,
		key:     Key(normalized.KeyPrefix),
		timeout: timeout,
	}, nil
}

// ReadPolicyGuard implements Reader.
func (r *Redis) ReadPolicyGuard(ctx context.Context) (State, error) {
	client, err := r.redisClient()
	if err != nil {
		return State{}, err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, r.timeout)
	defer cancel()
	return Read(callCtx, client, r.key)
}

// ServerTime returns the Redis server clock used by atomic admission scripts.
func (r *Redis) ServerTime(ctx context.Context) (time.Time, error) {
	client, err := r.redisClient()
	if err != nil {
		return time.Time{}, err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, r.timeout)
	defer cancel()
	value, err := client.Time(callCtx).Result()
	if err != nil {
		return time.Time{}, fmt.Errorf("read Redis server time: %w", err)
	}
	return value.UTC(), nil
}

// ReadRuntimeSafety returns one atomic server, memory, and statistics
// snapshot. Redis 7 or newer and INFO permission are required.
func (r *Redis) ReadRuntimeSafety(ctx context.Context) (RuntimeSafety, error) {
	client, err := r.redisClient()
	if err != nil {
		return RuntimeSafety{}, err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, r.timeout)
	defer cancel()
	info, err := client.Info(callCtx, "server", "memory", "stats").Result()
	if err != nil {
		return RuntimeSafety{}, fmt.Errorf(
			"read Redis INFO server memory stats: %w",
			err,
		)
	}
	runID, ok := infoValue(info, "run_id")
	if !ok || strings.TrimSpace(runID) == "" {
		return RuntimeSafety{}, fmt.Errorf(
			"Redis INFO server memory stats did not include run_id",
		)
	}
	policy, ok := infoValue(info, "maxmemory_policy")
	if !ok || !strings.EqualFold(strings.TrimSpace(policy), "noeviction") {
		if !ok {
			policy = "<missing>"
		}
		return RuntimeSafety{}, fmt.Errorf(
			"%w: maxmemory_policy=%q",
			ErrCorrupt,
			policy,
		)
	}
	rawEvictedKeys, ok := infoValue(info, "evicted_keys")
	if !ok {
		return RuntimeSafety{}, fmt.Errorf(
			"Redis INFO server memory stats did not include evicted_keys",
		)
	}
	evictedKeys, err := strconv.ParseInt(strings.TrimSpace(rawEvictedKeys), 10, 64)
	if err != nil || evictedKeys < 0 {
		return RuntimeSafety{}, fmt.Errorf(
			"Redis INFO server memory stats returned invalid evicted_keys %q",
			rawEvictedKeys,
		)
	}
	return RuntimeSafety{
		RunID:       strings.TrimSpace(runID),
		EvictedKeys: evictedKeys,
	}, nil
}

func infoValue(info, key string) (string, bool) {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		name, value, found := strings.Cut(line, ":")
		if found && strings.EqualFold(strings.TrimSpace(name), key) {
			return strings.TrimSpace(value), true
		}
	}
	return "", false
}

// SetPending atomically closes admissions for one policy mutation.
func (r *Redis) SetPending(
	ctx context.Context,
	expected Version,
	token string,
	quarantineUntil time.Time,
) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return fmt.Errorf("policy mutation token is required")
	}
	client, err := r.redisClient()
	if err != nil {
		return err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, r.timeout)
	defer cancel()
	result, err := setPendingScript.Run(
		callCtx,
		client,
		[]string{r.key},
		strconv.FormatInt(expected.EnforcementEpoch, 10),
		strconv.FormatInt(expected.RedisGeneration, 10),
		token,
		strconv.FormatInt(millis(quarantineUntil), 10),
	).Int64()
	if err != nil {
		return fmt.Errorf("set Team Quota policy guard pending: %w", err)
	}
	return transitionError(result)
}

// SetStable atomically publishes committed policy state for a matching pending
// mutation token.
func (r *Redis) SetStable(ctx context.Context, token string, state State) error {
	token = strings.TrimSpace(token)
	if token == "" {
		return fmt.Errorf("policy mutation token is required")
	}
	state.Phase = PhaseStable
	state.PendingToken = ""
	if _, err := Fields(state); err != nil {
		return err
	}
	client, err := r.redisClient()
	if err != nil {
		return err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, r.timeout)
	defer cancel()
	result, err := setStableScript.Run(
		callCtx,
		client,
		[]string{r.key},
		token,
		strconv.FormatInt(state.Version.EnforcementEpoch, 10),
		strconv.FormatInt(state.Version.RedisGeneration, 10),
		state.RedisRunID,
		strconv.FormatInt(state.RedisEvictedKeys, 10),
		strconv.FormatInt(millis(state.ResetAt), 10),
		strconv.FormatInt(millis(state.RateRefillFrom), 10),
	).Int64()
	if err != nil {
		return fmt.Errorf("publish stable Team Quota policy guard: %w", err)
	}
	return transitionError(result)
}

// Force writes a complete guard while the caller holds the PostgreSQL session
// advisory lock. It is the only recovery path for missing or ambiguous state.
func (r *Redis) Force(ctx context.Context, state State) error {
	fields, err := Fields(state)
	if err != nil {
		return err
	}
	client, err := r.redisClient()
	if err != nil {
		return err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, r.timeout)
	defer cancel()
	pipe := client.TxPipeline()
	pipe.Del(callCtx, r.key)
	pipe.HSet(callCtx, r.key, fields)
	pipe.Persist(callCtx, r.key)
	if _, err := pipe.Exec(callCtx); err != nil {
		return fmt.Errorf("force Team Quota policy guard: %w", err)
	}
	return nil
}

// Invalidate removes the admission guard. Every guarded distributed mutation
// then fails closed until the policy owner verifies the state plane and
// publishes a newly fenced generation.
func (r *Redis) Invalidate(ctx context.Context) error {
	client, err := r.redisClient()
	if err != nil {
		return err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, r.timeout)
	defer cancel()
	if err := client.Del(callCtx, r.key).Err(); err != nil {
		return fmt.Errorf("invalidate Team Quota policy guard: %w", err)
	}
	return nil
}

// Close releases the policy-owner Redis client.
func (r *Redis) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	client := r.client
	r.client = nil
	r.mu.Unlock()
	if client == nil {
		return nil
	}
	return client.Close()
}

func (r *Redis) redisClient() (*redis.Client, error) {
	if r == nil {
		return nil, fmt.Errorf("team quota policy guard is not configured")
	}
	r.mu.RLock()
	client := r.client
	r.mu.RUnlock()
	if client == nil {
		return nil, fmt.Errorf("team quota policy guard is closed")
	}
	return client, nil
}

func transitionError(result int64) error {
	switch result {
	case transitionApplied:
		return nil
	case transitionMissing:
		return ErrMissing
	case transitionPending:
		return ErrPending
	case transitionStale:
		return ErrStale
	case transitionCorrupt:
		return ErrCorrupt
	default:
		return fmt.Errorf("unexpected Team Quota policy guard transition result %d", result)
	}
}
