package http

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
)

var (
	errPreviewGrantNotFound    = errors.New("preview grant not found")
	errPreviewBootstrapInvalid = errors.New("preview bootstrap credential is invalid")
)

type previewGrantRecord struct {
	ID                string    `json:"id"`
	SandboxID         string    `json:"sandbox_id"`
	TeamID            string    `json:"team_id"`
	UserID            string    `json:"user_id,omitempty"`
	Port              int       `json:"port"`
	Protocol          string    `json:"protocol"`
	RuntimeGeneration int64     `json:"runtime_generation"`
	BootstrapHash     string    `json:"bootstrap_hash,omitempty"`
	SessionHash       string    `json:"session_hash,omitempty"`
	ExpiresAt         time.Time `json:"expires_at"`
}

type previewGrantStore interface {
	Put(context.Context, previewGrantRecord) error
	Get(context.Context, string) (previewGrantRecord, error)
	Renew(context.Context, string, time.Time) (previewGrantRecord, error)
	ConsumeBootstrap(context.Context, string, string, string) (previewGrantRecord, error)
	Delete(context.Context, string) error
}

func newPreviewGrantStore(ctx context.Context, cfg config.GatewayConfig) (previewGrantStore, error) {
	redisCfg := rediscache.Config{URL: cfg.RedisURL, Timeout: cfg.RedisTimeout.Duration}
	if !rediscache.Enabled(redisCfg) {
		return newMemoryPreviewGrantStore(time.Now), nil
	}
	basePrefix := strings.TrimSpace(cfg.RedisKeyPrefix)
	if basePrefix == "" {
		basePrefix = rediscache.DefaultKeyPrefix
	}
	redisCfg.KeyPrefix = rediscache.JoinKeyPrefix(basePrefix, "cluster-gateway", "preview-grants")
	client, normalized, err := rediscache.NewClient(ctx, redisCfg)
	if err != nil {
		return nil, err
	}
	return &redisPreviewGrantStore{client: client, cfg: normalized, now: time.Now}, nil
}

type memoryPreviewGrantStore struct {
	mu      sync.Mutex
	records map[string]previewGrantRecord
	now     func() time.Time
}

func newMemoryPreviewGrantStore(now func() time.Time) *memoryPreviewGrantStore {
	if now == nil {
		now = time.Now
	}
	return &memoryPreviewGrantStore{records: make(map[string]previewGrantRecord), now: now}
}

func (s *memoryPreviewGrantStore) Put(_ context.Context, record previewGrantRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !record.ExpiresAt.After(s.now()) {
		return errPreviewGrantNotFound
	}
	s.deleteExpiredLocked()
	s.records[record.ID] = record
	return nil
}

func (s *memoryPreviewGrantStore) Get(_ context.Context, id string) (previewGrantRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if !ok || !record.ExpiresAt.After(s.now()) {
		delete(s.records, id)
		return previewGrantRecord{}, errPreviewGrantNotFound
	}
	return record, nil
}

func (s *memoryPreviewGrantStore) Renew(_ context.Context, id string, expiresAt time.Time) (previewGrantRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	record, ok := s.records[id]
	if !ok || !record.ExpiresAt.After(now) || !expiresAt.After(now) {
		delete(s.records, id)
		return previewGrantRecord{}, errPreviewGrantNotFound
	}
	record.ExpiresAt = expiresAt
	s.records[id] = record
	return record, nil
}

func (s *memoryPreviewGrantStore) ConsumeBootstrap(_ context.Context, id, bootstrapHash, sessionHash string) (previewGrantRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if !ok || !record.ExpiresAt.After(s.now()) {
		delete(s.records, id)
		return previewGrantRecord{}, errPreviewGrantNotFound
	}
	if record.BootstrapHash == "" || !secureStringEqual(record.BootstrapHash, bootstrapHash) {
		return previewGrantRecord{}, errPreviewBootstrapInvalid
	}
	record.BootstrapHash = ""
	record.SessionHash = sessionHash
	s.records[id] = record
	return record, nil
}

func (s *memoryPreviewGrantStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	return nil
}

func (s *memoryPreviewGrantStore) deleteExpiredLocked() {
	now := s.now()
	for id, record := range s.records {
		if !record.ExpiresAt.After(now) {
			delete(s.records, id)
		}
	}
}

type redisPreviewGrantStore struct {
	client *redis.Client
	cfg    rediscache.Config
	now    func() time.Time
}

func (s *redisPreviewGrantStore) key(id string) string {
	return rediscache.JoinKeyPrefix(s.cfg.KeyPrefix, id)
}

func (s *redisPreviewGrantStore) Put(ctx context.Context, record previewGrantRecord) error {
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	ttl := time.Until(record.ExpiresAt)
	if s.now != nil {
		ttl = record.ExpiresAt.Sub(s.now())
	}
	if ttl <= 0 {
		return errPreviewGrantNotFound
	}
	opCtx, cancel := rediscache.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	return s.client.Set(opCtx, s.key(record.ID), body, ttl).Err()
}

func (s *redisPreviewGrantStore) Get(ctx context.Context, id string) (previewGrantRecord, error) {
	opCtx, cancel := rediscache.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	body, err := s.client.Get(opCtx, s.key(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return previewGrantRecord{}, errPreviewGrantNotFound
	}
	if err != nil {
		return previewGrantRecord{}, err
	}
	var record previewGrantRecord
	if err := json.Unmarshal(body, &record); err != nil {
		return previewGrantRecord{}, err
	}
	if !record.ExpiresAt.After(s.now()) {
		return previewGrantRecord{}, errPreviewGrantNotFound
	}
	return record, nil
}

func (s *redisPreviewGrantStore) Renew(ctx context.Context, id string, expiresAt time.Time) (previewGrantRecord, error) {
	key := s.key(id)
	for attempt := 0; attempt < 3; attempt++ {
		var renewed previewGrantRecord
		opCtx, cancel := rediscache.WithTimeout(ctx, s.cfg.Timeout)
		err := s.client.Watch(opCtx, func(tx *redis.Tx) error {
			body, err := tx.Get(opCtx, key).Bytes()
			if errors.Is(err, redis.Nil) {
				return errPreviewGrantNotFound
			}
			if err != nil {
				return err
			}
			var record previewGrantRecord
			if err := json.Unmarshal(body, &record); err != nil {
				return err
			}
			now := s.now()
			if !record.ExpiresAt.After(now) || !expiresAt.After(now) {
				return errPreviewGrantNotFound
			}
			record.ExpiresAt = expiresAt
			updated, err := json.Marshal(record)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(opCtx, func(pipe redis.Pipeliner) error {
				pipe.Set(opCtx, key, updated, expiresAt.Sub(now))
				return nil
			})
			if err == nil {
				renewed = record
			}
			return err
		}, key)
		cancel()
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return renewed, err
	}
	return previewGrantRecord{}, redis.TxFailedErr
}

func (s *redisPreviewGrantStore) ConsumeBootstrap(ctx context.Context, id, bootstrapHash, sessionHash string) (previewGrantRecord, error) {
	key := s.key(id)
	var consumed previewGrantRecord
	opCtx, cancel := rediscache.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	err := s.client.Watch(opCtx, func(tx *redis.Tx) error {
		body, err := tx.Get(opCtx, key).Bytes()
		if errors.Is(err, redis.Nil) {
			return errPreviewGrantNotFound
		}
		if err != nil {
			return err
		}
		var record previewGrantRecord
		if err := json.Unmarshal(body, &record); err != nil {
			return err
		}
		if !record.ExpiresAt.After(s.now()) {
			return errPreviewGrantNotFound
		}
		if record.BootstrapHash == "" || !secureStringEqual(record.BootstrapHash, bootstrapHash) {
			return errPreviewBootstrapInvalid
		}
		record.BootstrapHash = ""
		record.SessionHash = sessionHash
		updated, err := json.Marshal(record)
		if err != nil {
			return err
		}
		ttl := record.ExpiresAt.Sub(s.now())
		_, err = tx.TxPipelined(opCtx, func(pipe redis.Pipeliner) error {
			pipe.Set(opCtx, key, updated, ttl)
			return nil
		})
		if err == nil {
			consumed = record
		}
		return err
	}, key)
	if errors.Is(err, redis.TxFailedErr) {
		return previewGrantRecord{}, errPreviewBootstrapInvalid
	}
	return consumed, err
}

func (s *redisPreviewGrantStore) Delete(ctx context.Context, id string) error {
	opCtx, cancel := rediscache.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	return s.client.Del(opCtx, s.key(id)).Err()
}

func secureStringEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
