package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
)

func TestRateLimiterCloseIsConcurrentAndIdempotent(t *testing.T) {
	wantErr := errors.New("close failed")
	var calls atomic.Int32
	limiter := &RateLimiter{closeOwned: func() error {
		calls.Add(1)
		return wantErr
	}}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			if err := limiter.Close(); !errors.Is(err, wantErr) {
				t.Errorf("Close error = %v, want %v", err, wantErr)
			}
		})
	}
	workers.Wait()
	if calls.Load() != 1 {
		t.Fatalf("owned resource close calls = %d, want 1", calls.Load())
	}
}

func TestTeamQuotaRateLimiterClosePreservesInjectedResources(t *testing.T) {
	bucket := tokenbucket.NewMemoryBucket(tokenbucket.MemoryConfig{})
	t.Cleanup(func() { _ = bucket.Close() })
	limiter, err := NewTeamQuotaRateLimiter(&staticQuotaPolicyStore{}, bucket, "region-1", zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := limiter.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = bucket.TryTakeN(context.Background(), "key", tokenbucket.Limit{Tokens: 1, Interval: time.Second, Burst: 1}, 1)
	if err != nil {
		t.Fatalf("injected bucket must remain usable: %v", err)
	}
}

func TestTeamQuotaRateLimiterCloseReleasesOwnedResourcesIntegration(t *testing.T) {
	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	// No caller cancellation: Close itself must stop the background listener.
	limiter, err := NewTeamQuotaRateLimiterWithConfig(context.Background(), pool, config.GatewayConfig{}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = limiter.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for pool.Stat().AcquiredConns() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("quota listener did not acquire its database connection")
		}
		time.Sleep(time.Millisecond)
	}
	if err := limiter.Close(); err != nil {
		t.Fatal(err)
	}
	// pgx destroys canceled connections asynchronously. A successful query on
	// this one-connection pool proves the listener no longer holds its slot.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("database pool remains unavailable after Close: %v", err)
	}
	if got := pool.Stat().AcquiredConns(); got != 0 {
		t.Fatalf("acquired connections after Close = %d, want 0", got)
	}
	_, err = limiter.bucket.TryTakeN(context.Background(), "key", tokenbucket.Limit{Tokens: 1, Interval: time.Second, Burst: 1}, 1)
	if !errors.Is(err, tokenbucket.ErrClosed) {
		t.Fatalf("owned bucket error = %v, want ErrClosed", err)
	}
}

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

func newTestTeamQuotaRateLimiter(t *testing.T, store quota.PolicyStore) *RateLimiter {
	t.Helper()
	bucket := tokenbucket.NewMemoryBucket(tokenbucket.MemoryConfig{})
	t.Cleanup(func() {
		_ = bucket.Close()
	})
	limiter, err := NewTeamQuotaRateLimiter(store, bucket, "region-1", zap.NewNop())
	if err != nil {
		t.Fatalf("NewTeamQuotaRateLimiter: %v", err)
	}
	return limiter
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
	limiter := newTestTeamQuotaRateLimiter(t, store)

	first, _, err := limiter.allowDimension(context.Background(), "team-1", quota.DimensionAPIRequests)
	if err != nil || !first.Allowed {
		t.Fatalf("first allow = %+v, %v", first, err)
	}
	second, _, err := limiter.allowDimension(context.Background(), "team-1", quota.DimensionAPIRequests)
	if err != nil || second.Allowed || second.RetryAfter <= 0 {
		t.Fatalf("second allow = %+v, %v, want limited", second, err)
	}
}

func TestTeamQuotaRateLimiterAllowsMissingPolicy(t *testing.T) {
	limiter := newTestTeamQuotaRateLimiter(t, &staticQuotaPolicyStore{})

	decision, limit, err := limiter.allowDimension(context.Background(), "team-1", quota.DimensionAPIRequests)
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
	limiter := newTestTeamQuotaRateLimiter(t, store)

	decision, _, err := limiter.allowDimension(context.Background(), "team-1", quota.DimensionAPIRequests)
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
	limiter := newTestTeamQuotaRateLimiter(t, store)

	claim, _, err := limiter.allowDimension(context.Background(), "team-1", quota.DimensionSandboxClaims)
	if err != nil || !claim.Allowed {
		t.Fatalf("claim allow = %+v, %v", claim, err)
	}
	api, _, err := limiter.allowDimension(context.Background(), "team-1", quota.DimensionAPIRequests)
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
	limiter := newTestTeamQuotaRateLimiter(t, store)

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
