package rediscache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type JSONConfig struct {
	Config
	TTL time.Duration
}

// JSONCache stores JSON-encoded values in Redis with a shared key prefix.
type JSONCache[V any] struct {
	client    *redis.Client
	keyPrefix string
	timeout   time.Duration
	ttl       time.Duration
}

func NewJSONCache[V any](ctx context.Context, cfg JSONConfig) (*JSONCache[V], error) {
	client, normalized, err := NewClient(ctx, cfg.Config)
	if err != nil {
		return nil, err
	}
	return &JSONCache[V]{
		client:    client,
		keyPrefix: normalized.KeyPrefix,
		timeout:   normalized.Timeout,
		ttl:       cfg.TTL,
	}, nil
}

func (c *JSONCache[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var zero V
	if c == nil || c.client == nil {
		return zero, false, fmt.Errorf("redis cache is closed")
	}
	callCtx, cancel := WithTimeout(ctx, c.timeout)
	defer cancel()
	data, err := c.client.Get(callCtx, c.redisKey(key)).Bytes()
	if err == redis.Nil {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	var value V
	if err := json.Unmarshal(data, &value); err != nil {
		return zero, false, err
	}
	return value, true, nil
}

func (c *JSONCache[V]) Set(ctx context.Context, key string, value V) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("redis cache is closed")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	callCtx, cancel := WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.Set(callCtx, c.redisKey(key), data, c.ttl).Err()
}

func (c *JSONCache[V]) Delete(ctx context.Context, key string) error {
	if c == nil || c.client == nil {
		return nil
	}
	callCtx, cancel := WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.Del(callCtx, c.redisKey(key)).Err()
}

func (c *JSONCache[V]) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}

func (c *JSONCache[V]) redisKey(key string) string {
	return HashedKey(c.keyPrefix, key)
}
