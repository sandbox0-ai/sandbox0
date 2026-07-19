package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
	"go.uber.org/zap"
)

type fakeSandboxServiceAbuseGuard struct {
	decision ratelimit.Decision
	err      error
	key      string
	limit    ratelimit.Limit
	onAllow  func()
}

func (f *fakeSandboxServiceAbuseGuard) Allow(
	_ context.Context,
	key string,
	limit ratelimit.Limit,
) (ratelimit.Decision, error) {
	if f.onAllow != nil {
		f.onAllow()
	}
	f.key = key
	f.limit = limit
	return f.decision, f.err
}

func (*fakeSandboxServiceAbuseGuard) Close() error {
	return nil
}

func TestSandboxServiceAbuseGuardConfigIsIndependentAndFailClosed(t *testing.T) {
	cfg := sandboxServiceAbuseGuardConfig(config.GatewayConfig{
		RedisURL:       "redis://redis.example.com:6379",
		RedisKeyPrefix: "sandbox0",
	})
	if cfg.Backend != ratelimit.BackendRedis {
		t.Fatalf("backend = %q, want Redis", cfg.Backend)
	}
	if cfg.FailOpen {
		t.Fatal("sandbox service abuse guard must fail closed")
	}
	if !strings.HasSuffix(cfg.RedisKeyPrefix, ":sandbox-service-abuse-guard") {
		t.Fatalf("Redis key prefix = %q, want independent abuse-guard namespace", cfg.RedisKeyPrefix)
	}
	if strings.Contains(cfg.RedisKeyPrefix, "teamquota") {
		t.Fatalf("Redis key prefix = %q, must not share Team Quota namespace", cfg.RedisKeyPrefix)
	}
}

func TestSandboxServiceAbuseGuardRequiresRedis(t *testing.T) {
	_, err := ratelimit.New(
		context.Background(),
		sandboxServiceAbuseGuardConfig(config.GatewayConfig{}),
	)
	if err == nil {
		t.Fatal("expected missing Redis configuration to fail abuse-guard initialization")
	}
}

func TestSandboxServiceAbuseGuardResponsesAndKeyScope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name          string
		guard         *fakeSandboxServiceAbuseGuard
		wantStatus    int
		wantRetry     string
		wantErrorCode string
	}{
		{
			name: "limited",
			guard: &fakeSandboxServiceAbuseGuard{
				decision: ratelimit.Decision{
					Allowed:    false,
					RetryAfter: 1100 * time.Millisecond,
				},
			},
			wantStatus:    http.StatusTooManyRequests,
			wantRetry:     "2",
			wantErrorCode: `"code":"rate_limited"`,
		},
		{
			name: "backend unavailable",
			guard: &fakeSandboxServiceAbuseGuard{
				err: errors.New("redis unavailable"),
			},
			wantStatus:    http.StatusServiceUnavailable,
			wantRetry:     "1",
			wantErrorCode: `"code":"unavailable"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(recorder)
			ginCtx.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			server := &Server{
				logger:                   zap.NewNop(),
				sandboxServiceAbuseGuard: test.guard,
			}
			route := &mgr.SandboxAppServiceRoute{
				ID: "route-a",
				RateLimit: &mgr.SandboxAppServiceRouteRateLimit{
					RPS:   10,
					Burst: 20,
				},
			}

			if server.enforceSandboxServiceRoute(ginCtx, "sandbox-a", "team-a", route) {
				t.Fatal("expected request to be rejected")
			}
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			if got := recorder.Header().Get("Retry-After"); got != test.wantRetry {
				t.Fatalf("Retry-After = %q, want %q", got, test.wantRetry)
			}
			if !strings.Contains(recorder.Body.String(), test.wantErrorCode) {
				t.Fatalf("body = %s, want %s", recorder.Body.String(), test.wantErrorCode)
			}
			if test.guard.key != "sandbox-service-abuse-guard:v1:team:team-a:sandbox:sandbox-a:route:route-a" {
				t.Fatalf("key = %q, want team/sandbox/route scope", test.guard.key)
			}
			if test.guard.limit != (ratelimit.Limit{RPS: 10, Burst: 20}) {
				t.Fatalf("limit = %+v, want route policy", test.guard.limit)
			}
		})
	}
}
