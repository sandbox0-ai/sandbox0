package teamquota

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

type recordingBodyRateLimiter struct {
	key      coreteamquota.Key
	cost     int64
	decision tokenbucket.Decision
	err      error
}

func (l *recordingBodyRateLimiter) Take(
	_ context.Context,
	_ string,
	key coreteamquota.Key,
	cost int64,
) (tokenbucket.Decision, error) {
	l.key = key
	l.cost = cost
	return l.decision, l.err
}

func (*recordingBodyRateLimiter) Invalidate(string, coreteamquota.Key) {}

func TestRateLimitBodyBytesAdmitsExactBodyBeforeHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	limiter := &recordingBodyRateLimiter{
		decision: tokenbucket.Decision{Allowed: true},
	}
	controller := NewController(nil, nil, limiter, nil, nil)
	router := gin.New()
	router.POST(
		"/ingest",
		withBodyTeam("team-a"),
		controller.RateLimitBodyBytes(coreteamquota.KeyObservabilityIngestBytes, 64),
		func(c *gin.Context) {
			body, err := io.ReadAll(c.Request.Body)
			if err != nil {
				t.Fatalf("ReadAll() error = %v", err)
			}
			c.String(http.StatusAccepted, string(body))
		},
	)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"logs":[]}`))
	request.ContentLength = -1
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
	if limiter.key != coreteamquota.KeyObservabilityIngestBytes ||
		limiter.cost != int64(len(`{"logs":[]}`)) {
		t.Fatalf("admission = (%s, %d), want (%s, %d)",
			limiter.key,
			limiter.cost,
			coreteamquota.KeyObservabilityIngestBytes,
			len(`{"logs":[]}`),
		)
	}
	if recorder.Body.String() != `{"logs":[]}` {
		t.Fatalf("handler body = %q", recorder.Body.String())
	}
}

func TestRateLimitBodyBytesRejectsOversizeBeforeAdmission(t *testing.T) {
	gin.SetMode(gin.TestMode)
	limiter := &recordingBodyRateLimiter{
		decision: tokenbucket.Decision{Allowed: true},
	}
	controller := NewController(nil, nil, limiter, nil, nil)
	router := gin.New()
	router.POST(
		"/ingest",
		withBodyTeam("team-a"),
		controller.RateLimitBodyBytes(coreteamquota.KeyObservabilityIngestBytes, 4),
		func(c *gin.Context) { c.Status(http.StatusAccepted) },
	)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader("12345")),
	)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusRequestEntityTooLarge, recorder.Body.String())
	}
	if limiter.cost != 0 {
		t.Fatalf("admitted cost = %d, want 0", limiter.cost)
	}
}

func TestRateLimitBodyBytesMapsRateExhaustion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	limiter := &recordingBodyRateLimiter{
		decision: tokenbucket.Decision{
			Allowed:    false,
			Remaining:  2,
			RetryAfter: 1500 * time.Millisecond,
		},
	}
	controller := NewController(nil, nil, limiter, nil, nil)
	router := gin.New()
	router.POST(
		"/ingest",
		withBodyTeam("team-a"),
		controller.RateLimitBodyBytes(coreteamquota.KeyObservabilityIngestBytes, 64),
		func(c *gin.Context) { c.Status(http.StatusAccepted) },
	)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"logs":[]}`)),
	)

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusTooManyRequests, recorder.Body.String())
	}
	if recorder.Header().Get("Retry-After") != "2" {
		t.Fatalf("Retry-After = %q, want 2", recorder.Header().Get("Retry-After"))
	}
}

func withBodyTeam(teamID string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			AuthMethod: authn.AuthMethodInternal,
			TeamID:     teamID,
		})
		c.Next()
	}
}
