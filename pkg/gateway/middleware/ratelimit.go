package middleware

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
	"go.uber.org/zap"
)

// RateLimiter provides rate limiting functionality
type RateLimiter struct {
	logger   *zap.Logger
	limit    ratelimit.Limit
	limiter  ratelimit.Limiter
	failOpen bool
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

		decision, err := rl.limiter.Allow(c.Request.Context(), "gateway:team:"+teamID, rl.limit)
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

			c.Header("X-RateLimit-Limit", strconv.Itoa(rl.limit.RPS))
			c.Header("X-RateLimit-Remaining", strconv.Itoa(decision.Remaining))
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

func (rl *RateLimiter) Close() error {
	if rl == nil || rl.limiter == nil {
		return nil
	}
	return rl.limiter.Close()
}

func retryAfterSeconds(d time.Duration) int {
	if d <= 0 {
		return 1
	}
	seconds := int(d.Round(time.Second) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}
