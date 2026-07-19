package distributed

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
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

var errAdmissionProofMarkerMissing = errors.New("team quota admission proof marker is missing")

var consumeAdmissionProofScript = redis.NewScript(guard.LuaRuntimeSafetyHelpers + `
local guard_key = KEYS[1]
local admission_key = KEYS[2]
local proof_key = KEYS[3]

if redis.call("HGET", guard_key, "phase") ~= "stable" then
  return -1
end
local runtime_status = team_quota_guard_runtime_status(guard_key)
if runtime_status == -1 then
  return -7
end
if runtime_status ~= 1 then
  return -8
end
local current_epoch = redis.call("HGET", guard_key, "enforcement_epoch")
local current_generation = redis.call("HGET", guard_key, "redis_generation")
if current_epoch == false or current_generation == false
    or tonumber(current_epoch) == nil or tonumber(current_epoch) <= 0
    or tonumber(current_generation) == nil or tonumber(current_generation) <= 0 then
  return -6
end
local pending_token = redis.call("HGET", guard_key, "pending_token")
local rate_refill_from_ms = tonumber(redis.call("HGET", guard_key, "rate_refill_from_ms"))
local quarantine_until_ms = tonumber(redis.call("HGET", guard_key, "quarantine_until_ms"))
if pending_token ~= ""
    or rate_refill_from_ms == nil or rate_refill_from_ms < 0
    or quarantine_until_ms == nil or quarantine_until_ms ~= 0 then
  return -6
end
local reset_at_ms = tonumber(redis.call("HGET", guard_key, "reset_at_ms"))
if reset_at_ms == nil or reset_at_ms <= 0 then
  return -2
end
if current_epoch ~= ARGV[3] or current_generation ~= ARGV[4] then
  return 0
end

local admission = redis.call("GET", admission_key)
if admission == false then
  return -3
end
if admission == "disabled" then
  return 0
end
if admission ~= "active" then
  return -4
end

local issued_at_ms = tonumber(ARGV[1])
local expires_at_ms = tonumber(ARGV[2])
if issued_at_ms == nil or expires_at_ms == nil then
  return -5
end
if issued_at_ms <= reset_at_ms then
  return 0
end

local redis_time = redis.call("TIME")
local now_ms = (tonumber(redis_time[1]) * 1000)
    + math.floor(tonumber(redis_time[2]) / 1000)
local ttl_ms = expires_at_ms - now_ms
if ttl_ms <= 0 then
  return 0
end

local result = redis.call("SET", proof_key, "consumed", "PX", ttl_ms, "NX")
if result == false then
  return 0
end
return 1
`)

// AdmissionProofConsumerConfig configures the region-shared proof replay
// store. Proof keys are hashed and expire no later than their signed expiry.
type AdmissionProofConsumerConfig struct {
	RegionID  string
	RedisURL  string
	KeyPrefix string
	Timeout   time.Duration
}

// RedisAdmissionProofConsumer atomically validates the current policy reset
// fence and consumes one signed proof ID. Redis errors fail closed.
type RedisAdmissionProofConsumer struct {
	mu           sync.RWMutex
	client       *redis.Client
	marker       AtomicAdmissionMarker
	regionID     string
	proofPrefix  string
	policyKey    string
	redisTimeout time.Duration
}

// NewRedisAdmissionProofConsumer creates a one-time proof consumer without
// creating or repairing the policy guard.
func NewRedisAdmissionProofConsumer(
	ctx context.Context,
	marker AtomicAdmissionMarker,
	cfg AdmissionProofConsumerConfig,
) (*RedisAdmissionProofConsumer, error) {
	if marker == nil {
		return nil, fmt.Errorf("team quota admission proof marker is required")
	}
	regionID := strings.TrimSpace(cfg.RegionID)
	if regionID == "" {
		return nil, fmt.Errorf("team quota admission proof region ID is required")
	}
	client, normalized, err := rediscache.NewClient(ctx, rediscache.Config{
		URL:       cfg.RedisURL,
		KeyPrefix: cfg.KeyPrefix,
		Timeout:   cfg.Timeout,
		FailOpen:  false,
	})
	if err != nil {
		return nil, fmt.Errorf("create team quota admission proof consumer: %w", err)
	}
	return &RedisAdmissionProofConsumer{
		client:       client,
		marker:       marker,
		regionID:     regionID,
		proofPrefix:  rediscache.JoinKeyPrefix(normalized.KeyPrefix, "admission-proof"),
		policyKey:    guard.Key(normalized.KeyPrefix),
		redisTimeout: normalized.Timeout,
	}, nil
}

// Consume returns true only for the first use of a proof issued strictly after
// the current Redis reset fence. Equal-millisecond issuance is rejected
// conservatively because ordering cannot be proven. Replays, expired proofs,
// disabled teams, and older proofs return false and use normal quota admission.
func (c *RedisAdmissionProofConsumer) Consume(
	ctx context.Context,
	teamID string,
	proofID string,
	issuedAtMS int64,
	expiresAtMS int64,
	version guard.Version,
) (bool, error) {
	if c == nil || c.marker == nil {
		return false, fmt.Errorf("team quota admission proof consumer is not configured")
	}
	teamID = strings.TrimSpace(teamID)
	proofID = strings.TrimSpace(proofID)
	if teamID == "" {
		return false, fmt.Errorf("team_id is required")
	}
	if proofID == "" {
		return false, fmt.Errorf("proof_id is required")
	}
	if issuedAtMS <= 0 ||
		expiresAtMS <= issuedAtMS ||
		expiresAtMS-issuedAtMS > teamquota.MaxAdmissionProofLifetime.Milliseconds() {
		return false, nil
	}
	if err := version.Validate(); err != nil {
		return false, nil
	}
	client, err := c.redisClient()
	if err != nil {
		return false, err
	}
	admissionKey, err := c.marker.RedisKey(teamID)
	if err != nil {
		return false, fmt.Errorf("resolve team quota proof admission marker: %w", err)
	}
	proofKey := c.redisKey(teamID, proofID)

	for attempt := 0; attempt < 2; attempt++ {
		callCtx, cancel := rediscache.WithTimeout(ctx, c.redisTimeout)
		result, runErr := consumeAdmissionProofScript.Run(
			callCtx,
			client,
			[]string{c.policyKey, admissionKey, proofKey},
			strconv.FormatInt(issuedAtMS, 10),
			strconv.FormatInt(expiresAtMS, 10),
			strconv.FormatInt(version.EnforcementEpoch, 10),
			strconv.FormatInt(version.RedisGeneration, 10),
		).Int64()
		cancel()
		if runErr != nil {
			return false, fmt.Errorf("consume team quota admission proof: %w", runErr)
		}
		switch result {
		case 1:
			return true, nil
		case 0:
			return false, nil
		case -1:
			return false, fmt.Errorf("consume team quota admission proof: policy guard is not stable")
		case -2:
			return false, fmt.Errorf("consume team quota admission proof: policy reset fence is missing")
		case -3:
			if attempt != 0 {
				return false, errAdmissionProofMarkerMissing
			}
			if err := c.marker.Recover(ctx, teamID); err != nil {
				return false, fmt.Errorf("recover team quota proof admission marker: %w", err)
			}
		case -4:
			return false, fmt.Errorf("consume team quota admission proof: admission marker is corrupt")
		case -5:
			return false, fmt.Errorf("consume team quota admission proof: proof timestamps are corrupt")
		case -6:
			return false, fmt.Errorf("consume team quota admission proof: policy guard is corrupt")
		case -7:
			return false, fmt.Errorf("consume team quota admission proof: Redis server incarnation is stale")
		case -8:
			return false, fmt.Errorf("consume team quota admission proof: Redis runtime safety state is corrupt")
		default:
			return false, fmt.Errorf("consume team quota admission proof: unexpected result %d", result)
		}
	}
	return false, errAdmissionProofMarkerMissing
}

// CurrentVersion returns the complete stable guard version used to bind a new
// proof. Missing, pending, corrupt, or unfenced guard state fails closed.
func (c *RedisAdmissionProofConsumer) CurrentVersion(
	ctx context.Context,
) (guard.Version, error) {
	client, err := c.redisClient()
	if err != nil {
		return guard.Version{}, err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, c.redisTimeout)
	defer cancel()
	state, err := guard.Read(callCtx, client, c.policyKey)
	if err != nil {
		return guard.Version{}, fmt.Errorf(
			"read team quota admission proof policy guard: %w",
			err,
		)
	}
	if !state.Stable() {
		return guard.Version{}, fmt.Errorf(
			"read team quota admission proof policy guard: policy guard is not stable",
		)
	}
	if state.ResetAt.IsZero() {
		return guard.Version{}, fmt.Errorf(
			"read team quota admission proof policy guard: proof reset fence is missing",
		)
	}
	return state.Version, nil
}

// Close releases the consumer's Redis client. The shared admission marker
// remains owned by its creator.
func (c *RedisAdmissionProofConsumer) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	client := c.client
	c.client = nil
	c.mu.Unlock()
	if client == nil {
		return nil
	}
	return client.Close()
}

func (c *RedisAdmissionProofConsumer) redisClient() (*redis.Client, error) {
	if c == nil {
		return nil, fmt.Errorf("team quota admission proof consumer is not configured")
	}
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	if client == nil {
		return nil, fmt.Errorf("team quota admission proof consumer is closed")
	}
	return client, nil
}

func (c *RedisAdmissionProofConsumer) redisKey(teamID, proofID string) string {
	identity := fmt.Sprintf(
		"%d:%s:%d:%s:%d:%s",
		len(c.regionID),
		c.regionID,
		len(teamID),
		teamID,
		len(proofID),
		proofID,
	)
	return rediscache.HashedKey(c.proofPrefix, identity)
}
