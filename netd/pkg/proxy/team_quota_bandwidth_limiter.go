package proxy

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

const defaultNetdBandwidthRedisKeyPrefix = "sandbox0:quota:network"

// quotaTeamBandwidthLimiter resolves team policies dynamically and shares
// admission state through Redis when the region has Redis configured.
type quotaTeamBandwidthLimiter struct {
	policies quota.PolicyStore
	bucket   tokenbucket.Bucket
	regionID string
}

func newQuotaTeamBandwidthLimiter(ctx context.Context, cfg *config.NetdConfig, policies quota.PolicyStore) (*quotaTeamBandwidthLimiter, error) {
	if cfg == nil || policies == nil {
		return nil, nil
	}
	var bucket tokenbucket.Bucket
	if strings.TrimSpace(cfg.RedisURL) == "" {
		bucket = tokenbucket.NewMemoryBucket(tokenbucket.MemoryConfig{})
		return &quotaTeamBandwidthLimiter{
			policies: policies,
			bucket:   bucket,
			regionID: cfg.RegionID,
		}, nil
	}
	basePrefix := strings.TrimSpace(cfg.RedisKeyPrefix)
	if basePrefix == "" {
		basePrefix = rediscache.DefaultKeyPrefix
	}
	prefix := rediscache.JoinKeyPrefix(basePrefix, "quota", "network")
	if prefix == "" {
		prefix = defaultNetdBandwidthRedisKeyPrefix
	}
	redisBucket, err := tokenbucket.NewRedisBucket(ctx, tokenbucket.RedisConfig{
		URL:       cfg.RedisURL,
		KeyPrefix: prefix,
		Timeout:   cfg.RedisTimeout.Duration,
		FailOpen:  false,
	})
	if err != nil {
		return nil, fmt.Errorf("create ctld network runtime quota token bucket: %w", err)
	}
	bucket = redisBucket
	return &quotaTeamBandwidthLimiter{
		policies: policies,
		bucket:   bucket,
		regionID: cfg.RegionID,
	}, nil
}

// WithTeamQuotaBandwidth attaches region-wide per-team network quota
// backpressure to the proxy while leaving per-sandbox QoS independent.
func WithTeamQuotaBandwidth(ctx context.Context, cfg *config.NetdConfig, policies quota.PolicyStore) (ServerOption, error) {
	team, err := newQuotaTeamBandwidthLimiter(ctx, cfg, policies)
	if err != nil {
		return nil, err
	}
	if team == nil {
		return nil, nil
	}
	return func(server *Server) {
		if server == nil {
			_ = team.Close()
			return
		}
		if server.bandwidthLimiter == nil {
			server.bandwidthLimiter = &bandwidthLimiter{
				team:  team,
				sleep: time.Sleep,
			}
			return
		}
		server.bandwidthLimiter.team = team
	}, nil
}

func (l *quotaTeamBandwidthLimiter) reserve(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) (time.Duration, error) {
	if l == nil || l.bucket == nil || bytes <= 0 || compiled == nil || strings.TrimSpace(compiled.TeamID) == "" {
		return 0, nil
	}
	quotaPolicy, err := l.policy(ctx, compiled.TeamID, direction)
	if err != nil || quotaPolicy == nil {
		return 0, err
	}
	reservation, err := l.bucket.ReserveN(
		ctx,
		l.redisKey(compiled.TeamID, quotaPolicy.Dimension),
		tokenbucket.Limit{
			Tokens:   quotaPolicy.LimitValue,
			Interval: time.Duration(quotaPolicy.IntervalMS) * time.Millisecond,
			Burst:    quotaPolicy.BurstValue,
		},
		int64(bytes),
	)
	if err != nil {
		return 0, err
	}
	return reservation.Delay, nil
}

func (l *quotaTeamBandwidthLimiter) tryTake(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection, bytes int) (bool, error) {
	if l == nil || l.bucket == nil || bytes <= 0 || compiled == nil || strings.TrimSpace(compiled.TeamID) == "" {
		return true, nil
	}
	quotaPolicy, err := l.policy(ctx, compiled.TeamID, direction)
	if err != nil || quotaPolicy == nil {
		return quotaPolicy == nil, err
	}
	decision, err := l.bucket.TryTakeN(
		ctx,
		l.redisKey(compiled.TeamID, quotaPolicy.Dimension),
		tokenbucket.Limit{
			Tokens:   quotaPolicy.LimitValue,
			Interval: time.Duration(quotaPolicy.IntervalMS) * time.Millisecond,
			Burst:    quotaPolicy.BurstValue,
		},
		int64(bytes),
	)
	return decision.Allowed, err
}

func (l *quotaTeamBandwidthLimiter) burstBytes(ctx context.Context, compiled *policy.CompiledPolicy, direction bandwidthDirection) (int64, error) {
	if l == nil || compiled == nil || strings.TrimSpace(compiled.TeamID) == "" {
		return 0, nil
	}
	quotaPolicy, err := l.policy(ctx, compiled.TeamID, direction)
	if err != nil || quotaPolicy == nil {
		return 0, err
	}
	return quotaPolicy.BurstValue, nil
}

func (l *quotaTeamBandwidthLimiter) policy(ctx context.Context, teamID string, direction bandwidthDirection) (*quota.Policy, error) {
	dimension := networkQuotaDimension(direction)
	if dimension == "" {
		return nil, nil
	}
	resolved, err := l.policies.GetPolicy(ctx, teamID, dimension)
	if err != nil || resolved == nil {
		return resolved, err
	}
	if resolved.Kind != quota.KindRate {
		return nil, fmt.Errorf("%s quota has kind %q", dimension, resolved.Kind)
	}
	return resolved, nil
}

func (l *quotaTeamBandwidthLimiter) redisKey(teamID string, dimension quota.Dimension) string {
	return "region:" + valueOrUnknown(l.regionID) +
		":team:" + teamID +
		":dimension:" + string(dimension)
}

func (l *quotaTeamBandwidthLimiter) Close() error {
	if l == nil {
		return nil
	}
	var err error
	if l.bucket != nil {
		err = l.bucket.Close()
	}
	if closer, ok := l.policies.(interface{ Close() error }); ok {
		if closeErr := closer.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

func networkQuotaDimension(direction bandwidthDirection) quota.Dimension {
	switch direction {
	case bandwidthEgress:
		return quota.DimensionNetworkEgress
	case bandwidthIngress:
		return quota.DimensionNetworkIngress
	default:
		return ""
	}
}

func valueOrUnknown(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}
