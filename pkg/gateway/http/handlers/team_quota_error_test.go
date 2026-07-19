package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

func TestWriteTeamQuotaMutationError(t *testing.T) {
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
			err:        &teamquota.ExceededError{TeamID: "team-1", Key: teamquota.KeyControlPlaneObjectCount, Limit: 1, Requested: 1},
			wantMapped: true,
			wantStatus: http.StatusTooManyRequests,
			wantCode:   spec.CodeQuotaExceeded,
		},
		{
			name:       "quota unavailable",
			err:        &teamquota.UnavailableError{Operation: "reserve", Err: errors.New("database unavailable")},
			wantMapped: true,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   spec.CodeUnavailable,
			wantRetry:  "1",
		},
		{
			name:       "unrelated error",
			err:        errors.New("boom"),
			wantMapped: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			if got := writeTeamQuotaMutationError(ctx, tt.err); got != tt.wantMapped {
				t.Fatalf("mapped = %v, want %v", got, tt.wantMapped)
			}
			if !tt.wantMapped {
				return
			}
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
			_, apiErr, err := spec.DecodeResponse[any](recorder.Body)
			if err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if apiErr == nil || apiErr.Code != tt.wantCode {
				t.Fatalf("api error = %#v, want code %q", apiErr, tt.wantCode)
			}
			if got := recorder.Header().Get("Retry-After"); got != tt.wantRetry {
				t.Fatalf("Retry-After = %q, want %q", got, tt.wantRetry)
			}
		})
	}
}
