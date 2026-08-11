package http

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/admission"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
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

func TestPauseRunningSandboxesForRestrictedTeamUsesSystemTokenForDefaultCluster(t *testing.T) {
	gin.SetMode(gin.TestMode)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	var receivedToken, receivedTeamID string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		receivedToken = request.Header.Get(internalauth.DefaultTokenHeader)
		receivedTeamID = request.Header.Get(internalauth.TeamIDHeader)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"data":{"requested":1}}`))
	}))
	defer target.Close()
	router, err := proxy.NewRouter(target.URL, zap.NewNop(), time.Second)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	server := &Server{
		admissionStore: billingPauseAdmissionStore{
			record: admission.Record{
				TeamID:  "11111111-1111-4111-8111-111111111111",
				Version: 4,
				State:   admission.StateRestricted,
			},
			found: true,
		},
		clusterGatewayRouter: router,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		logger: zap.NewNop(),
	}
	ginRouter := gin.New()
	ginRouter.POST("/internal/v1/teams/:team_id/pause-running-sandboxes", func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{TeamID: "system", IsSystemAdmin: true})
		server.pauseRunningSandboxesForRestrictedTeam(c)
	})

	request := httptest.NewRequest(
		http.MethodPost,
		"/internal/v1/teams/11111111-1111-4111-8111-111111111111/pause-running-sandboxes",
		strings.NewReader(`{"version":4}`),
	)
	recorder := httptest.NewRecorder()
	ginRouter.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    internalauth.ServiceClusterGateway,
		PublicKey: publicKey,
	}).Validate(receivedToken)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !claims.IsSystemToken() {
		t.Fatalf("expected system token, got team_id=%q", claims.TeamID)
	}
	if receivedTeamID != "" {
		t.Fatalf("forwarded team id = %q, want empty", receivedTeamID)
	}
}

func TestPauseRunningSandboxesForRestrictedTeamUsesSystemTokenForScheduler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	var receivedToken, receivedPath string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		receivedToken = request.Header.Get(internalauth.DefaultTokenHeader)
		receivedPath = request.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"data":{"requested":1}}`))
	}))
	defer target.Close()
	router, err := proxy.NewRouter(target.URL, zap.NewNop(), time.Second)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	server := &Server{
		admissionStore: billingPauseAdmissionStore{
			record: admission.Record{
				TeamID:  "11111111-1111-4111-8111-111111111111",
				Version: 4,
				State:   admission.StateRestricted,
			},
			found: true,
		},
		schedulerRouter: router,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		logger: zap.NewNop(),
	}
	ginRouter := gin.New()
	ginRouter.POST("/internal/v1/teams/:team_id/pause-running-sandboxes", func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{TeamID: "system", IsSystemAdmin: true})
		server.pauseRunningSandboxesForRestrictedTeam(c)
	})

	request := httptest.NewRequest(
		http.MethodPost,
		"/internal/v1/teams/11111111-1111-4111-8111-111111111111/pause-running-sandboxes",
		strings.NewReader(`{"version":4}`),
	)
	recorder := httptest.NewRecorder()
	ginRouter.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    internalauth.ServiceScheduler,
		PublicKey: publicKey,
	}).Validate(receivedToken)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !claims.IsSystemToken() {
		t.Fatalf("expected system token, got team_id=%q", claims.TeamID)
	}
	if receivedPath != "/api/v1/teams/11111111-1111-4111-8111-111111111111/pause-running-sandboxes" {
		t.Fatalf("forwarded path = %q", receivedPath)
	}
}
