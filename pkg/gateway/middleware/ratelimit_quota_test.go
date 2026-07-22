package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
)

type staticQuotaPolicyStore struct {
	policies map[quota.Dimension]*quota.Policy
	err      error
}

func (s *staticQuotaPolicyStore) GetPolicy(_ context.Context, _ string, dimension quota.Dimension) (*quota.Policy, error) {
	policy := s.policies[dimension]
	if policy == nil {
		return nil, s.err
	}
	out := *policy
	return &out, s.err
}

func TestTeamQuotaRateLimiterUsesAPIRequestPolicy(t *testing.T) {
	store := &staticQuotaPolicyStore{policies: map[quota.Dimension]*quota.Policy{quota.DimensionAPIRequests: {
		Dimension:  quota.DimensionAPIRequests,
		Kind:       quota.KindRate,
		LimitValue: 1,
		IntervalMS: 1000,
		BurstValue: 1,
		Source:     quota.SourceTeamOverride,
	}}}
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
	store := &staticQuotaPolicyStore{policies: map[quota.Dimension]*quota.Policy{quota.DimensionAPIRequests: {
		Dimension:  quota.DimensionAPIRequests,
		Kind:       quota.KindRate,
		LimitValue: 0,
		IntervalMS: 1000,
		BurstValue: 0,
		Source:     quota.SourceTeamOverride,
	}}}
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

func TestTeamQuotaRateLimiterKeepsClaimAndAPIBucketsIndependent(t *testing.T) {
	store := &staticQuotaPolicyStore{policies: map[quota.Dimension]*quota.Policy{
		quota.DimensionAPIRequests: {
			Dimension: quota.DimensionAPIRequests, Kind: quota.KindRate,
			LimitValue: 1, IntervalMS: 1000, BurstValue: 1,
		},
		quota.DimensionSandboxClaims: {
			Dimension: quota.DimensionSandboxClaims, Kind: quota.KindRate,
			LimitValue: 1, IntervalMS: 1000, BurstValue: 1,
		},
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

	claim, _, err := limiter.allowDimension(context.Background(), "team-1", quota.DimensionSandboxClaims)
	if err != nil || !claim.Allowed {
		t.Fatalf("claim allow = %+v, %v", claim, err)
	}
	api, _, err := limiter.allow(context.Background(), "team-1")
	if err != nil || !api.Allowed {
		t.Fatalf("API allow = %+v, %v", api, err)
	}
	secondClaim, _, err := limiter.allowDimension(context.Background(), "team-1", quota.DimensionSandboxClaims)
	if err != nil || secondClaim.Allowed {
		t.Fatalf("second claim allow = %+v, %v, want limited", secondClaim, err)
	}
}

func TestSandboxClaimRateLimitUsesQuotaErrorContract(t *testing.T) {
	store := &staticQuotaPolicyStore{policies: map[quota.Dimension]*quota.Policy{
		quota.DimensionSandboxClaims: {
			Dimension: quota.DimensionSandboxClaims, Kind: quota.KindRate,
			LimitValue: 0, IntervalMS: 1000, BurstValue: 0,
		},
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

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{TeamID: "team-1"})
		c.Next()
	})
	router.POST("/api/v1/sandboxes", limiter.RateLimitDimension(quota.DimensionSandboxClaims), func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
	}
	if recorder.Header().Get("Retry-After") == "" {
		t.Fatal("Retry-After header is empty")
	}
	var response spec.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error == nil || response.Error.Code != "quota_exceeded" {
		t.Fatalf("response = %+v, want quota_exceeded", response)
	}
}
