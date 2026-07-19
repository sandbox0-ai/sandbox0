package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

func TestWriteTeamQuotaMutationError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name       string
		err        error
		wantMapped bool
		wantStatus int
		wantCode   string
		wantRetry  string
	}{
		{
			name:       "capacity exceeded",
			err:        &teamquota.ExceededError{TeamID: "team-1", Key: teamquota.KeyControlPlaneObjectCount},
			wantMapped: true,
			wantStatus: http.StatusTooManyRequests,
			wantCode:   spec.CodeQuotaExceeded,
		},
		{
			name:       "classified exceeded",
			err:        service.ErrQuotaExceeded,
			wantMapped: true,
			wantStatus: http.StatusTooManyRequests,
			wantCode:   spec.CodeQuotaExceeded,
		},
		{
			name: "rate exceeded",
			err: &service.TeamQuotaRateLimitError{
				Key:        teamquota.KeySandboxStarts,
				RetryAfter: 1500 * time.Millisecond,
			},
			wantMapped: true,
			wantStatus: http.StatusTooManyRequests,
			wantCode:   spec.CodeQuotaExceeded,
			wantRetry:  "2",
		},
		{
			name:       "capacity unavailable",
			err:        &teamquota.UnavailableError{Operation: "reserve", Err: errors.New("postgres unavailable")},
			wantMapped: true,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   spec.CodeUnavailable,
			wantRetry:  "1",
		},
		{
			name:       "classified unavailable",
			err:        service.ErrTeamQuotaUnavailable,
			wantMapped: true,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   spec.CodeUnavailable,
			wantRetry:  "1",
		},
		{
			name:       "unrelated",
			err:        errors.New("other"),
			wantMapped: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			if got := writeTeamQuotaMutationError(ctx, test.err); got != test.wantMapped {
				t.Fatalf("writeTeamQuotaMutationError() = %t, want %t", got, test.wantMapped)
			}
			if !test.wantMapped {
				return
			}
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			if test.wantCode != "" && !containsJSONErrorCode(recorder.Body.String(), test.wantCode) {
				t.Fatalf("body = %q, want error code %q", recorder.Body.String(), test.wantCode)
			}
			if got := recorder.Header().Get("Retry-After"); got != test.wantRetry {
				t.Fatalf("Retry-After = %q, want %q", got, test.wantRetry)
			}
		})
	}
}

func containsJSONErrorCode(body, code string) bool {
	return strings.Contains(body, `"code":"`+code+`"`)
}
