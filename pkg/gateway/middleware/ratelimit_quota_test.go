package middleware

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
)

type staticQuotaPolicyStore struct {
	policy *quota.Policy
	err    error
}

func (s *staticQuotaPolicyStore) GetPolicy(context.Context, string, quota.Dimension) (*quota.Policy, error) {
	if s.policy == nil {
		return nil, s.err
	}
	out := *s.policy
	return &out, s.err
}

func TestTeamQuotaRateLimiterUsesAPIRequestPolicy(t *testing.T) {
	store := &staticQuotaPolicyStore{policy: &quota.Policy{
		Dimension:  quota.DimensionAPIRequests,
		Kind:       quota.KindRate,
		LimitValue: 1,
		IntervalMS: 1000,
		BurstValue: 1,
		Source:     quota.SourceTeamOverride,
	}}
	limiter, err := NewTeamQuotaRateLimiter(
		store,
		tokenbucket.NewMemoryBucket(tokenbucket.MemoryConfig{}),
		"region-1",
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("NewTeamQuotaRateLimiter: %v", err)
	}
	defer limiter.Close()

	first, _, err := limiter.allow(context.Background(), "team-1")
	if err != nil || !first.Allowed {
		t.Fatalf("first allow = %+v, %v", first, err)
	}
	second, _, err := limiter.allow(context.Background(), "team-1")
	if err != nil || second.Allowed || second.RetryAfter <= 0 {
		t.Fatalf("second allow = %+v, %v, want limited", second, err)
	}
}

func TestTeamQuotaRateLimiterAllowsMissingPolicy(t *testing.T) {
	limiter, err := NewTeamQuotaRateLimiter(
		&staticQuotaPolicyStore{},
		tokenbucket.NewMemoryBucket(tokenbucket.MemoryConfig{}),
		"region-1",
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("NewTeamQuotaRateLimiter: %v", err)
	}
	defer limiter.Close()

	decision, limit, err := limiter.allow(context.Background(), "team-1")
	if err != nil || !decision.Allowed || limit != 0 {
		t.Fatalf("allow = %+v, %d, %v, want unlimited", decision, limit, err)
	}
}

func TestTeamQuotaRateLimiterRejectsZeroPolicy(t *testing.T) {
	store := &staticQuotaPolicyStore{policy: &quota.Policy{
		Dimension:  quota.DimensionAPIRequests,
		Kind:       quota.KindRate,
		LimitValue: 0,
		IntervalMS: 1000,
		BurstValue: 0,
		Source:     quota.SourceTeamOverride,
	}}
	limiter, err := NewTeamQuotaRateLimiter(
		store,
		tokenbucket.NewMemoryBucket(tokenbucket.MemoryConfig{}),
		"region-1",
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("NewTeamQuotaRateLimiter: %v", err)
	}
	defer limiter.Close()

	decision, _, err := limiter.allow(context.Background(), "team-1")
	if err != nil || decision.Allowed {
		t.Fatalf("allow = %+v, %v, want rejected", decision, err)
	}
}
