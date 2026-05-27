package http

import (
	"context"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	"go.uber.org/zap"
)

const (
	sandboxInternalCacheTTL = 2 * time.Second
)

type sandboxInternalCache interface {
	Get(ctx context.Context, key string) (*mgr.Sandbox, bool, error)
	Set(ctx context.Context, key string, value *mgr.Sandbox) error
	Delete(ctx context.Context, key string) error
}

func newSandboxInternalCache(ctx context.Context, cfg config.GatewayConfig) (sandboxInternalCache, error) {
	redisCfg := rediscache.Config{
		URL:     cfg.RedisURL,
		Timeout: cfg.RedisTimeout.Duration,
	}
	if !rediscache.Enabled(redisCfg) {
		return nil, nil
	}
	basePrefix := strings.TrimSpace(cfg.RedisKeyPrefix)
	if basePrefix == "" {
		basePrefix = rediscache.DefaultKeyPrefix
	}
	redisCfg.KeyPrefix = rediscache.JoinKeyPrefix(basePrefix, "cluster-gateway", "get-sandbox-internal")
	return rediscache.NewJSONCache[*mgr.Sandbox](ctx, rediscache.JSONConfig{
		Config: redisCfg,
		TTL:    sandboxInternalCacheTTL,
	})
}

func (s *Server) getSandboxInternalCached(ctx context.Context, sandboxID string) (*mgr.Sandbox, error) {
	if s.sandboxInternalCache != nil {
		sandbox, ok, err := s.sandboxInternalCache.Get(ctx, sandboxID)
		if err == nil && ok && sandbox != nil {
			return sandbox, nil
		}
		if err != nil && s.logger != nil {
			s.logger.Debug("Sandbox internal cache lookup failed",
				zap.String("sandbox_id", sandboxID),
				zap.Error(err),
			)
		}
	}

	sandbox, err := s.managerClient.GetSandboxInternal(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if s.sandboxInternalCache != nil && sandbox != nil {
		if err := s.sandboxInternalCache.Set(ctx, sandboxID, sandbox); err != nil && s.logger != nil {
			s.logger.Debug("Sandbox internal cache store failed",
				zap.String("sandbox_id", sandboxID),
				zap.Error(err),
			)
		}
	}
	return sandbox, nil
}

func (s *Server) invalidateSandboxInternalCache(ctx context.Context, sandboxID string) {
	if s == nil || s.sandboxInternalCache == nil || strings.TrimSpace(sandboxID) == "" {
		return
	}
	if err := s.sandboxInternalCache.Delete(ctx, sandboxID); err != nil && s.logger != nil {
		s.logger.Debug("Sandbox internal cache invalidation failed",
			zap.String("sandbox_id", sandboxID),
			zap.Error(err),
		)
	}
}
