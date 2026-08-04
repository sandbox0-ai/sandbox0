package http

import (
	"context"
	"testing"

	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
)

type allowAllRateLimitPolicyStore struct{}

func (allowAllRateLimitPolicyStore) GetPolicy(context.Context, string, quota.Dimension) (*quota.Policy, error) {
	return nil, nil
}

func newTestRateLimiter(t *testing.T) *gatewaymiddleware.RateLimiter {
	t.Helper()
	bucket := tokenbucket.NewMemoryBucket(tokenbucket.MemoryConfig{})
	t.Cleanup(func() {
		_ = bucket.Close()
	})
	limiter, err := gatewaymiddleware.NewTeamQuotaRateLimiter(
		allowAllRateLimitPolicyStore{},
		bucket,
		"test-region",
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("NewTeamQuotaRateLimiter: %v", err)
	}
	return limiter
}
