package rediscache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultKeyPrefix = "sandbox0"
	DefaultTimeout   = 100 * time.Millisecond
)

type Config struct {
	URL       string
	KeyPrefix string
	Timeout   time.Duration
	FailOpen  bool
}

// Enabled reports whether Redis has enough runtime config to be used.
func Enabled(cfg Config) bool {
	return strings.TrimSpace(cfg.URL) != ""
}

// SpecEnabled centralizes Redis enablement semantics for CRD-style specs.
func SpecEnabled(specPresent bool, backendType string, builtinEnabled *bool) bool {
	if !specPresent {
		return false
	}
	switch strings.TrimSpace(strings.ToLower(backendType)) {
	case "builtin", "":
		if builtinEnabled != nil {
			return *builtinEnabled
		}
		return true
	case "external":
		return true
	default:
		return true
	}
}

func NormalizeConfig(cfg Config) Config {
	cfg.URL = strings.TrimSpace(cfg.URL)
	cfg.KeyPrefix = strings.TrimSpace(cfg.KeyPrefix)
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = DefaultKeyPrefix
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	return cfg
}

// NewClient creates a Redis client and validates connectivity with the configured timeout.
func NewClient(ctx context.Context, cfg Config) (*redis.Client, Config, error) {
	cfg = NormalizeConfig(cfg)
	if !Enabled(cfg) {
		return nil, cfg, fmt.Errorf("redis URL is required")
	}
	options, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, cfg, fmt.Errorf("parse redis URL: %w", err)
	}
	client := redis.NewClient(options)
	pingCtx, cancel := WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		if !cfg.FailOpen {
			_ = client.Close()
			return nil, cfg, fmt.Errorf("connect redis: %w", err)
		}
	}
	return client, cfg, nil
}

func WithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

func JoinKeyPrefix(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(strings.TrimSpace(part), ":")
		if part == "" {
			continue
		}
		clean = append(clean, part)
	}
	return strings.Join(clean, ":")
}

func HashedKey(prefix, key string) string {
	sum := sha256.Sum256([]byte(key))
	if strings.TrimSpace(prefix) == "" {
		return hex.EncodeToString(sum[:])
	}
	return JoinKeyPrefix(prefix, hex.EncodeToString(sum[:]))
}
