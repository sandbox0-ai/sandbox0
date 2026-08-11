package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/admission"
	"go.uber.org/zap"
)

type billingPauseAdmissionStore struct {
	record admission.Record
	found  bool
}

func (s billingPauseAdmissionStore) Get(context.Context, string) (admission.Record, bool, error) {
	return s.record, s.found, nil
}

func (billingPauseAdmissionStore) Put(context.Context, string, admission.Update) (admission.PutResult, error) {
	return admission.PutResult{}, nil
}

func TestPauseRunningSandboxesForRestrictedTeamSkipsSupersededRestriction(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := &Server{
		admissionStore: billingPauseAdmissionStore{
			record: admission.Record{
				TeamID:    "11111111-1111-4111-8111-111111111111",
				Version:   4,
				State:     admission.StateAllowed,
				UpdatedAt: time.Now(),
			},
			found: true,
		},
		logger: zap.NewNop(),
	}
	router := gin.New()
	router.POST("/internal/v1/teams/:team_id/pause-running-sandboxes", server.pauseRunningSandboxesForRestrictedTeam)

	request := httptest.NewRequest(
		http.MethodPost,
		"/internal/v1/teams/11111111-1111-4111-8111-111111111111/pause-running-sandboxes",
		strings.NewReader(`{"version":3}`),
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"applied":false`) {
		t.Fatalf("response = %s, want unapplied result", recorder.Body.String())
	}
}
