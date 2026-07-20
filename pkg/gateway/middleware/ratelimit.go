package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
)

// RateLimiter provides rate limiting functionality
type RateLimiter struct {
	logger      *zap.Logger
	limit       ratelimit.Limit
	limiter     ratelimit.Limiter
	policyStore quota.PolicyStore
	bucket      tokenbucket.Bucket
	keyPrefix   string
	failOpen    bool
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(rps, burst int, cleanupInterval time.Duration, logger *zap.Logger) *RateLimiter {
	limiter := ratelimit.NewMemoryLimiter(ratelimit.MemoryConfig{CleanupInterval: cleanupInterval})
	return &RateLimiter{
		logger:   logger,
		limit:    ratelimit.Limit{RPS: rps, Burst: burst},
		limiter:  limiter,
		failOpen: true,
	}
}

func NewRateLimiterWithConfig(ctx context.Context, rps, burst int, cfg ratelimit.Config, logger *zap.Logger) (*RateLimiter, error) {
	limiter, err := ratelimit.New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &RateLimiter{
		logger:   logger,
		limit:    ratelimit.Limit{RPS: rps, Burst: burst},
		limiter:  limiter,
		failOpen: cfg.FailOpen,
	}, nil
}

// NewTeamQuotaRateLimiter creates the gateway admission middleware backed by
// the api_requests Team Quota policy.
func NewTeamQuotaRateLimiter(policyStore quota.PolicyStore, bucket tokenbucket.Bucket, regionID string, logger *zap.Logger) (*RateLimiter, error) {
	if policyStore == nil {
		return nil, fmt.Errorf("quota policy store is required")
	}
	if bucket == nil {
		return nil, fmt.Errorf("quota token bucket is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &RateLimiter{
		logger:      logger,
		policyStore: policyStore,
		bucket:      bucket,
		keyPrefix:   "region:" + valueOrUnknown(regionID) + ":team:",
	}, nil
}

func NewTeamQuotaRateLimiterWithConfig(ctx context.Context, pool *pgxpool.Pool, cfg apiconfig.GatewayConfig, logger *zap.Logger) (*RateLimiter, error) {
	if pool == nil {
		return nil, fmt.Errorf("quota database pool is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if !strings.EqualFold(strings.TrimSpace(cfg.RateLimitBackend), tokenbucket.BackendRedis) {
		logger.Warn("API request Team Quota is using process-local token state; configure region Redis for shared enforcement")
	}
	repo := quota.NewRepository(pool)
	if cfg.RateLimitRPS > 0 {
		burst := cfg.RateLimitBurst
		if burst <= 0 {
			burst = cfg.RateLimitRPS
		}
		if err := repo.EnsureDefaultPolicies(ctx, "gateway_rate_limit", []quota.DefaultLimit{{
			Dimension:  quota.DimensionAPIRequests,
			LimitValue: int64(cfg.RateLimitRPS),
			IntervalMS: int64(time.Second / time.Millisecond),
			BurstValue: int64(burst),
		}}); err != nil {
			return nil, err
		}
	}
	policies, err := quota.NewCachedPolicyStore(ctx, pool, repo, quota.DefaultPolicyCacheTTL)
	if err != nil {
		return nil, err
	}
	bucket, err := tokenbucket.New(ctx, TokenBucketConfigFromGatewayConfig(cfg))
	if err != nil {
		_ = policies.Close()
		return nil, err
	}
	limiter, err := NewTeamQuotaRateLimiter(policies, bucket, cfg.RegionID, logger)
	if err != nil {
		_ = bucket.Close()
		_ = policies.Close()
		return nil, err
	}
	return limiter, nil
}

func RateLimitConfigFromGatewayConfig(cfg apiconfig.GatewayConfig) ratelimit.Config {
	return ratelimit.Config{
		Backend:         cfg.RateLimitBackend,
		RedisURL:        cfg.RateLimitRedisURL,
		RedisKeyPrefix:  cfg.RateLimitRedisKeyPrefix,
		RedisTimeout:    cfg.RateLimitRedisTimeout.Duration,
		CleanupInterval: cfg.RateLimitCleanupInterval.Duration,
		FailOpen:        cfg.RateLimitFailOpen,
	}
}

func TokenBucketConfigFromGatewayConfig(cfg apiconfig.GatewayConfig) tokenbucket.Config {
	return tokenbucket.Config{
		Backend:         cfg.RateLimitBackend,
		RedisURL:        cfg.RateLimitRedisURL,
		RedisKeyPrefix:  cfg.RateLimitRedisKeyPrefix,
		RedisTimeout:    cfg.RateLimitRedisTimeout.Duration,
		CleanupInterval: cfg.RateLimitCleanupInterval.Duration,
		FailOpen:        false,
	}
}

// RateLimit returns a gin middleware that rate limits requests per team
func (rl *RateLimiter) RateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := GetAuthContext(c)
		if authCtx == nil {
			// No auth context, skip rate limiting (will fail at auth middleware)
			c.Next()
			return
		}

		teamID := authCtx.TeamID
		if teamID == "" {
			c.Next()
			return
		}

		decision, limitValue, err := rl.allow(c.Request.Context(), teamID)
		if err != nil {
			rl.logger.Warn("Rate limiter failed",
				zap.String("team_id", teamID),
				zap.String("client_ip", c.ClientIP()),
				zap.Error(err),
			)
			if rl.failOpen {
				c.Next()
				return
			}
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error": "rate limiter unavailable",
			})
			return
		}
		if !decision.Allowed {
			rl.logger.Warn("Rate limit exceeded",
				zap.String("team_id", teamID),
				zap.String("client_ip", c.ClientIP()),
			)

			c.Header("X-RateLimit-Limit", strconv.FormatInt(limitValue, 10))
			c.Header("X-RateLimit-Remaining", strconv.FormatInt(decision.Remaining, 10))
			c.Header("Retry-After", strconv.Itoa(retryAfterSeconds(decision.RetryAfter)))

			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate limit exceeded",
				"retry_after": retryAfterSeconds(decision.RetryAfter),
			})
			return
		}

		c.Next()
	}
}

type rateDecision struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
}

func (rl *RateLimiter) allow(ctx context.Context, teamID string) (rateDecision, int64, error) {
	if rl.policyStore != nil {
		policy, err := rl.policyStore.GetPolicy(ctx, teamID, quota.DimensionAPIRequests)
		if err != nil {
			return rateDecision{}, 0, err
		}
		if policy == nil {
			return rateDecision{Allowed: true}, 0, nil
		}
		if policy.Kind != quota.KindRate {
			return rateDecision{}, 0, fmt.Errorf("api_requests quota has kind %q", policy.Kind)
		}
		decision, err := rl.bucket.TryTakeN(ctx,
			rl.keyPrefix+teamID+":dimension:"+string(quota.DimensionAPIRequests),
			tokenbucket.Limit{
				Tokens:   policy.LimitValue,
				Interval: time.Duration(policy.IntervalMS) * time.Millisecond,
				Burst:    policy.BurstValue,
			},
			1,
		)
		return rateDecision{
			Allowed:    decision.Allowed,
			Remaining:  decision.Remaining,
			RetryAfter: decision.RetryAfter,
		}, policy.LimitValue, err
	}
	decision, err := rl.limiter.Allow(ctx, "gateway:team:"+teamID, rl.limit)
	return rateDecision{
		Allowed:    decision.Allowed,
		Remaining:  int64(decision.Remaining),
		RetryAfter: decision.RetryAfter,
	}, int64(rl.limit.RPS), err
}

func (rl *RateLimiter) Close() error {
	if rl == nil {
		return nil
	}
	var err error
	if rl.limiter != nil {
		err = rl.limiter.Close()
	}
	if rl.bucket != nil {
		if closeErr := rl.bucket.Close(); err == nil {
			err = closeErr
		}
	}
	if closer, ok := rl.policyStore.(interface{ Close() error }); ok {
		if closeErr := closer.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

func retryAfterSeconds(d time.Duration) int {
	return tokenbucket.RetryAfterSeconds(d)
}

func valueOrUnknown(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}
