package middleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	"go.uber.org/zap"
)

const platformOverloadGuardKey = "requests"

// OverloadGuard is an aggregate safety valve for one configured platform
// scope. It is intentionally not keyed by team and is independent from Team
// Quota.
type OverloadGuard struct {
	logger        *zap.Logger
	localLimit    ratelimit.Limit
	sharedLimit   ratelimit.Limit
	localLimiter  ratelimit.Limiter
	sharedLimiter ratelimit.Limiter
	inFlight      chan struct{}
}

// SharedOverloadGuardConfig resolves the public identity guard from the
// gateway's region-shared Redis connection. Operator-injected guard settings
// take precedence over this runtime fallback.
func SharedOverloadGuardConfig(
	cfg apiconfig.GatewayConfig,
	keyNamespace string,
) apiconfig.OverloadGuardConfig {
	guardCfg := cfg.OverloadGuard
	if strings.TrimSpace(guardCfg.RedisURL) == "" {
		guardCfg.RedisURL = cfg.RedisURL
	}
	if strings.TrimSpace(guardCfg.RedisKeyPrefix) == "" {
		guardCfg.RedisKeyPrefix = rediscache.JoinKeyPrefix(
			cfg.RedisKeyPrefix,
			"overload-guard",
			keyNamespace,
		)
	}
	if guardCfg.RedisTimeout.Duration <= 0 {
		guardCfg.RedisTimeout = cfg.RedisTimeout
	}
	return apiconfig.NormalizeOverloadGuardConfig(guardCfg)
}

// NewOverloadGuard creates an aggregate platform guard. Callers that require a
// distributed scope should use NewSharedOverloadGuard.
func NewOverloadGuard(
	ctx context.Context,
	cfg apiconfig.OverloadGuardConfig,
	logger *zap.Logger,
) (*OverloadGuard, error) {
	backend := ratelimit.BackendMemory
	if strings.TrimSpace(cfg.RedisURL) != "" {
		backend = ratelimit.BackendRedis
	}
	return newOverloadGuard(ctx, cfg, backend, logger)
}

// NewSharedOverloadGuard creates a fail-closed guard backed by shared Redis.
func NewSharedOverloadGuard(
	ctx context.Context,
	cfg apiconfig.OverloadGuardConfig,
	logger *zap.Logger,
) (*OverloadGuard, error) {
	if strings.TrimSpace(cfg.RedisURL) == "" {
		return nil, fmt.Errorf("shared overload guard requires redis URL")
	}
	return newOverloadGuard(ctx, cfg, ratelimit.BackendRedis, logger)
}

func newOverloadGuard(
	ctx context.Context,
	cfg apiconfig.OverloadGuardConfig,
	sharedBackend string,
	logger *zap.Logger,
) (*OverloadGuard, error) {
	if err := apiconfig.ValidateOverloadGuardConfig(cfg); err != nil {
		return nil, err
	}
	cfg = apiconfig.NormalizeOverloadGuardConfig(cfg)
	localLimiter, err := ratelimit.New(ctx, ratelimit.Config{
		Backend:         ratelimit.BackendMemory,
		CleanupInterval: cfg.CleanupInterval.Duration,
		FailOpen:        false,
	})
	if err != nil {
		return nil, fmt.Errorf("create local overload guard: %w", err)
	}
	sharedLimiter, err := ratelimit.New(ctx, ratelimit.Config{
		Backend:         sharedBackend,
		RedisURL:        cfg.RedisURL,
		RedisKeyPrefix:  cfg.RedisKeyPrefix,
		RedisTimeout:    cfg.RedisTimeout.Duration,
		CleanupInterval: cfg.CleanupInterval.Duration,
		FailOpen:        false,
	})
	if err != nil {
		_ = localLimiter.Close()
		return nil, fmt.Errorf("create shared overload guard: %w", err)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &OverloadGuard{
		logger: logger,
		localLimit: ratelimit.Limit{
			RPS:   cfg.LocalRequestsPerSecond,
			Burst: cfg.LocalBurst,
		},
		sharedLimit: ratelimit.Limit{
			RPS:   cfg.RequestsPerSecond,
			Burst: cfg.Burst,
		},
		localLimiter:  localLimiter,
		sharedLimiter: sharedLimiter,
		inFlight:      make(chan struct{}, cfg.MaxInFlight),
	}, nil
}

// Admit applies local concurrency and rate admission before consulting the
// region-shared limiter. This ordering prevents an unauthenticated request
// flood from turning Redis into the first resource that is exhausted.
func (g *OverloadGuard) Admit() gin.HandlerFunc {
	return func(c *gin.Context) {
		if g == nil {
			c.Next()
			return
		}

		if g.inFlight != nil {
			select {
			case g.inFlight <- struct{}{}:
				defer func() { <-g.inFlight }()
			default:
				writeOverloadRateLimited(c, 0, time.Second)
				return
			}
		}

		if !g.allow(c, g.localLimiter, g.localLimit, "local") {
			return
		}
		if !g.allow(c, g.sharedLimiter, g.sharedLimit, "shared") {
			return
		}
		c.Next()
	}
}

func (g *OverloadGuard) allow(
	c *gin.Context,
	limiter ratelimit.Limiter,
	limit ratelimit.Limit,
	scope string,
) bool {
	if limiter == nil {
		return true
	}
	decision, err := limiter.Allow(c.Request.Context(), platformOverloadGuardKey, limit)
	if err != nil {
		g.logger.Warn("Overload guard failed", zap.String("scope", scope), zap.Error(err))
		c.Header("Retry-After", "1")
		spec.JSONError(
			c,
			http.StatusServiceUnavailable,
			spec.CodeUnavailable,
			"overload guard unavailable",
		)
		c.Abort()
		return false
	}
	if !decision.Allowed {
		writeOverloadRateLimited(c, limit.RPS, decision.RetryAfter)
		return false
	}
	return true
}

func writeOverloadRateLimited(c *gin.Context, limit int, retryAfter time.Duration) {
	if limit > 0 {
		c.Header("X-RateLimit-Limit", strconv.Itoa(limit))
	}
	c.Header("X-RateLimit-Remaining", "0")
	c.Header("Retry-After", strconv.Itoa(ratelimit.RetryAfterSeconds(retryAfter)))
	spec.JSONError(
		c,
		http.StatusTooManyRequests,
		spec.CodeRateLimited,
		"service overloaded",
	)
	c.Abort()
}

// Close releases both local and shared guard backends.
func (g *OverloadGuard) Close() error {
	if g == nil {
		return nil
	}
	var localErr, sharedErr error
	if g.localLimiter != nil {
		localErr = g.localLimiter.Close()
	}
	if g.sharedLimiter != nil {
		sharedErr = g.sharedLimiter.Close()
	}
	return errors.Join(localErr, sharedErr)
}
