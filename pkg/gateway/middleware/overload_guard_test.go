package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

type overloadGuardLimiter struct {
	decision ratelimit.Decision
	err      error
	mu       sync.Mutex
	keys     []string
}

func (l *overloadGuardLimiter) Allow(
	_ context.Context,
	key string,
	_ ratelimit.Limit,
) (ratelimit.Decision, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.keys = append(l.keys, key)
	return l.decision, l.err
}

func (*overloadGuardLimiter) Close() error { return nil }

func (l *overloadGuardLimiter) keySnapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.keys...)
}

func TestOverloadGuardUsesOnePlatformKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	limiter := &overloadGuardLimiter{decision: ratelimit.Decision{Allowed: true}}
	guard := &OverloadGuard{
		logger:        zap.NewNop(),
		sharedLimit:   ratelimit.Limit{RPS: 100, Burst: 200},
		sharedLimiter: limiter,
	}
	router := gin.New()
	router.Use(guard.Admit())
	router.GET("/regions", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	for _, teamID := range []string{"team-a", "team-b"} {
		request := httptest.NewRequest(http.MethodGet, "/regions", nil)
		request.Header.Set("X-Team-ID", teamID)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", recorder.Code)
		}
	}
	keys := limiter.keySnapshot()
	if len(keys) != 2 ||
		keys[0] != platformOverloadGuardKey ||
		keys[1] != platformOverloadGuardKey {
		t.Fatalf("keys = %#v, want fixed platform key", keys)
	}
}

func TestSharedOverloadGuardRequiresRedis(t *testing.T) {
	_, err := NewSharedOverloadGuard(
		context.Background(),
		apiconfig.OverloadGuardConfig{},
		zap.NewNop(),
	)
	if err == nil {
		t.Fatal("NewSharedOverloadGuard() error = nil, want shared Redis requirement")
	}
}

func TestOverloadGuardRejectsUnsafeLocalBounds(t *testing.T) {
	_, err := NewOverloadGuard(
		context.Background(),
		apiconfig.OverloadGuardConfig{MaxInFlight: -1},
		zap.NewNop(),
	)
	if err == nil {
		t.Fatal("NewOverloadGuard() error = nil, want invalid bound rejection")
	}
}

func TestSharedOverloadGuardConfigUsesServiceAndRegionNamespace(t *testing.T) {
	cfg := SharedOverloadGuardConfig(
		apiconfig.GatewayConfig{
			RegionID:       "aws-us-east-1",
			RedisURL:       "redis://region-shared:6379/0",
			RedisKeyPrefix: "sandbox0:prod",
		},
		"regional-gateway:aws-us-east-1",
	)

	if cfg.RedisURL != "redis://region-shared:6379/0" {
		t.Fatalf("RedisURL = %q", cfg.RedisURL)
	}
	if cfg.RedisKeyPrefix != "sandbox0:prod:overload-guard:regional-gateway:aws-us-east-1" {
		t.Fatalf("RedisKeyPrefix = %q", cfg.RedisKeyPrefix)
	}
	if cfg.RequestsPerSecond != apiconfig.DefaultOverloadGuardRequestsPerSecond ||
		cfg.Burst != apiconfig.DefaultOverloadGuardBurst ||
		cfg.LocalRequestsPerSecond != apiconfig.DefaultOverloadGuardLocalRequestsPerSecond ||
		cfg.LocalBurst != apiconfig.DefaultOverloadGuardLocalBurst ||
		cfg.MaxInFlight != apiconfig.DefaultOverloadGuardMaxInFlight {
		t.Fatalf("limits = %+v, want defaults", cfg)
	}
}

func TestSharedOverloadGuardsUseOneRedisBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	redisServer := miniredis.RunT(t)
	cfg := apiconfig.OverloadGuardConfig{
		RequestsPerSecond: 1,
		Burst:             1,
		RedisURL:          "redis://" + redisServer.Addr(),
		RedisKeyPrefix:    "sandbox0:test:overload-guard",
	}
	first, err := NewSharedOverloadGuard(context.Background(), cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("NewSharedOverloadGuard(first) error = %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := NewSharedOverloadGuard(context.Background(), cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("NewSharedOverloadGuard(second) error = %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	firstRouter := gin.New()
	firstRouter.Use(first.Admit())
	firstRouter.GET("/auth/providers", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	secondRouter := gin.New()
	secondRouter.Use(second.Admit())
	secondRouter.GET("/auth/providers", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	firstRecorder := httptest.NewRecorder()
	firstRouter.ServeHTTP(
		firstRecorder,
		httptest.NewRequest(http.MethodGet, "/auth/providers", nil),
	)
	if firstRecorder.Code != http.StatusNoContent {
		t.Fatalf("first status = %d, want %d", firstRecorder.Code, http.StatusNoContent)
	}

	secondRecorder := httptest.NewRecorder()
	secondRouter.ServeHTTP(
		secondRecorder,
		httptest.NewRequest(http.MethodGet, "/auth/providers", nil),
	)
	if secondRecorder.Code != http.StatusTooManyRequests {
		t.Fatalf(
			"second status = %d, want shared-budget %d; body=%s",
			secondRecorder.Code,
			http.StatusTooManyRequests,
			secondRecorder.Body.String(),
		)
	}
}

func TestOverloadGuardFailsClosedWhenBackendIsUnavailable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	guard := &OverloadGuard{
		logger:        zap.NewNop(),
		sharedLimit:   ratelimit.Limit{RPS: 100, Burst: 200},
		sharedLimiter: &overloadGuardLimiter{err: errors.New("backend unavailable")},
	}
	router := gin.New()
	router.Use(guard.Admit())
	router.GET("/regions", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/regions", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	assertOverloadGuardError(t, recorder, spec.CodeUnavailable)
}

func TestOverloadGuardReturnsRateLimitedEnvelope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	guard := &OverloadGuard{
		logger:      zap.NewNop(),
		sharedLimit: ratelimit.Limit{RPS: 100, Burst: 200},
		sharedLimiter: &overloadGuardLimiter{decision: ratelimit.Decision{
			Allowed:   false,
			Remaining: 0,
		}},
	}
	router := gin.New()
	router.Use(guard.Admit())
	router.GET("/regions", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/regions", nil))
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
	}
	assertOverloadGuardError(t, recorder, spec.CodeRateLimited)
}

func TestOverloadGuardLocalDenialDoesNotCallSharedLimiter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	local := &overloadGuardLimiter{
		decision: ratelimit.Decision{Allowed: false, RetryAfter: time.Second},
	}
	shared := &overloadGuardLimiter{decision: ratelimit.Decision{Allowed: true}}
	guard := &OverloadGuard{
		logger:        zap.NewNop(),
		localLimit:    ratelimit.Limit{RPS: 10, Burst: 20},
		sharedLimit:   ratelimit.Limit{RPS: 5, Burst: 10},
		localLimiter:  local,
		sharedLimiter: shared,
		inFlight:      make(chan struct{}, 1),
	}
	router := gin.New()
	router.Use(guard.Admit())
	router.GET("/", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
	}
	if got := len(shared.keySnapshot()); got != 0 {
		t.Fatalf("shared limiter calls = %d, want 0", got)
	}
}

func TestOverloadGuardInFlightDenialDoesNotCallLimiters(t *testing.T) {
	gin.SetMode(gin.TestMode)
	local := &overloadGuardLimiter{decision: ratelimit.Decision{Allowed: true}}
	shared := &overloadGuardLimiter{decision: ratelimit.Decision{Allowed: true}}
	guard := &OverloadGuard{
		logger:        zap.NewNop(),
		localLimit:    ratelimit.Limit{RPS: 10, Burst: 20},
		sharedLimit:   ratelimit.Limit{RPS: 5, Burst: 10},
		localLimiter:  local,
		sharedLimiter: shared,
		inFlight:      make(chan struct{}, 1),
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	router := gin.New()
	router.Use(guard.Admit())
	router.GET("/", func(c *gin.Context) {
		close(entered)
		<-release
		c.Status(http.StatusNoContent)
	})

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		router.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(http.MethodGet, "/", nil),
		)
	}()
	<-entered

	second := httptest.NewRecorder()
	router.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/", nil))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want %d", second.Code, http.StatusTooManyRequests)
	}
	if got := len(local.keySnapshot()); got != 1 {
		t.Fatalf("local limiter calls = %d, want 1", got)
	}
	if got := len(shared.keySnapshot()); got != 1 {
		t.Fatalf("shared limiter calls = %d, want 1", got)
	}

	close(release)
	<-firstDone
}

func assertOverloadGuardError(t *testing.T, recorder *httptest.ResponseRecorder, wantCode string) {
	t.Helper()
	_, apiErr, err := spec.DecodeResponse[struct{}](recorder.Body)
	if err != nil {
		t.Fatalf("DecodeResponse() error = %v", err)
	}
	if apiErr == nil || apiErr.Code != wantCode {
		t.Fatalf("error = %+v, want code %q", apiErr, wantCode)
	}
}
