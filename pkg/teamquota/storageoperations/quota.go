// Package storageoperations enforces the region-wide per-team rate of
// persistent volume, snapshot, and filesystem operations.
package storageoperations

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/distributed"
	teamquotarate "github.com/sandbox0-ai/sandbox0/pkg/teamquota/rate"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

// Config identifies the region-shared Redis token bucket.
type Config struct {
	RegionID       string
	RedisURL       string
	RedisKeyPrefix string
	RedisTimeout   time.Duration
	PolicyCacheTTL time.Duration
}

// Quota admits one persistent volume operation.
type Quota interface {
	Admit(context.Context, string) error
	Close() error
}

// RedisQuota owns the policy cache, admission marker, and token bucket used by
// one storage process.
type RedisQuota struct {
	limiter *teamquotarate.Limiter
	marker  distributed.AtomicAdmissionMarker
	bucket  tokenbucket.GuardedBucket

	closeOnce sync.Once
	closeErr  error
}

// NewRedis creates fail-closed storage operation admission.
func NewRedis(
	ctx context.Context,
	resolver interface {
		teamquotarate.PolicyResolver
		teamquota.TeamAdmissionStateResolver
	},
	cfg Config,
) (*RedisQuota, error) {
	if ctx == nil {
		return nil, fmt.Errorf("storage operation quota context is required")
	}
	if resolver == nil {
		return nil, fmt.Errorf("storage operation quota resolver is required")
	}
	if strings.TrimSpace(cfg.RegionID) == "" {
		return nil, fmt.Errorf("storage operation quota region ID is required")
	}
	if strings.TrimSpace(cfg.RedisURL) == "" {
		return nil, fmt.Errorf("storage operation quota Redis URL is required")
	}
	cfg.RedisKeyPrefix = teamquota.NormalizeTeamQuotaRedisKeyPrefix(cfg.RedisKeyPrefix)
	marker, err := distributed.NewRedisAdmissionMarker(
		ctx,
		resolver,
		distributed.AdmissionMarkerConfig{
			RegionID:  cfg.RegionID,
			RedisURL:  cfg.RedisURL,
			KeyPrefix: cfg.RedisKeyPrefix,
			Timeout:   cfg.RedisTimeout,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create storage operation admission marker: %w", err)
	}
	bucket, err := tokenbucket.NewRedisBucket(ctx, tokenbucket.RedisConfig{
		URL:       cfg.RedisURL,
		KeyPrefix: cfg.RedisKeyPrefix,
		Timeout:   cfg.RedisTimeout,
	})
	if err != nil {
		_ = marker.Close()
		return nil, fmt.Errorf("create storage operation token bucket: %w", err)
	}
	limiter, err := teamquotarate.NewLimiter(
		resolver,
		marker,
		bucket,
		teamquotarate.Config{
			RegionID:       cfg.RegionID,
			PolicyCacheTTL: cfg.PolicyCacheTTL,
		},
	)
	if err != nil {
		_ = bucket.Close()
		_ = marker.Close()
		return nil, err
	}
	return &RedisQuota{
		limiter: limiter,
		marker:  marker,
		bucket:  bucket,
	}, nil
}

// Admit immediately consumes one storage_operations token.
func (q *RedisQuota) Admit(ctx context.Context, teamID string) error {
	if q == nil || q.limiter == nil {
		return &teamquota.UnavailableError{
			Operation: "admit storage operation",
			Err:       fmt.Errorf("storage operation quota is not configured"),
		}
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return &teamquota.UnavailableError{
			Operation: "admit storage operation",
			Err:       fmt.Errorf("team_id is required"),
		}
	}
	decision, err := q.limiter.Take(
		ctx,
		teamID,
		teamquota.KeyStorageOperations,
		1,
	)
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return &teamquota.RateExceededError{
			TeamID:     teamID,
			Key:        teamquota.KeyStorageOperations,
			Remaining:  decision.Remaining,
			RetryAfter: decision.RetryAfter,
		}
	}
	return nil
}

// Close releases the Redis clients.
func (q *RedisQuota) Close() error {
	if q == nil {
		return nil
	}
	q.closeOnce.Do(func() {
		var errs []error
		if q.marker != nil {
			errs = append(errs, q.marker.Close())
		}
		if q.bucket != nil {
			errs = append(errs, q.bucket.Close())
		}
		q.closeErr = errors.Join(errs...)
	})
	return q.closeErr
}
