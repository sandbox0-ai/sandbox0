package http

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

type recordingAuditWriter struct {
	events []sandboxobservability.Event
	err    error
}

type failAfterAuditWriter struct {
	calls        int
	succeedFor   int
	storedEvents []sandboxobservability.Event
	onSuccess    func(call int)
}

func (w *failAfterAuditWriter) InsertEvents(_ context.Context, events []sandboxobservability.Event) error {
	w.calls++
	if w.calls > w.succeedFor {
		return errors.New("clickhouse unavailable")
	}
	w.storedEvents = append(w.storedEvents, events...)
	if w.onSuccess != nil {
		w.onSuccess(w.calls)
	}
	return nil
}

func (*failAfterAuditWriter) InsertLogs(context.Context, []sandboxobservability.LogEntry) error {
	return nil
}

func (*failAfterAuditWriter) InsertRuntimeSamples(context.Context, []sandboxobservability.RuntimeSample) error {
	return nil
}

func (w *recordingAuditWriter) InsertEvents(_ context.Context, events []sandboxobservability.Event) error {
	if w.err != nil {
		return w.err
	}
	w.events = append(w.events, events...)
	return nil
}

func (*recordingAuditWriter) InsertLogs(context.Context, []sandboxobservability.LogEntry) error {
	return nil
}

func (*recordingAuditWriter) InsertRuntimeSamples(context.Context, []sandboxobservability.RuntimeSample) error {
	return nil
}

func TestSandboxAuditMiddlewarePersistsTrustedAttemptAndResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &recordingAuditWriter{}
	server := &Server{
		cfg: &config.ClusterGatewayConfig{
			ClusterID:            "cluster-1",
			GatewayConfig:        config.GatewayConfig{RegionID: "region-1"},
			SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true},
		},
		auditWriter: writer, auditSigningKey: key, logger: zap.NewNop(),
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		authCtx := &authn.AuthContext{AuthMethod: authn.AuthMethodAPIKey, TeamID: "team-1", UserID: "user-1", APIKeyID: "key-1"}
		c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), authCtx))
		c.Next()
	})
	router.POST("/api/v1/sandboxes/:id/pause", server.auditSandboxRequests(), func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sb-1/pause", nil)
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
	if len(writer.events) != 2 {
		t.Fatalf("events = %d, want 2", len(writer.events))
	}
	attempt, result := writer.events[0], writer.events[1]
	if attempt.Phase != sandboxobservability.EventPhaseAttempt || result.Phase != sandboxobservability.EventPhaseResult || result.ParentEventID != attempt.EventID {
		t.Fatalf("events = %+v %+v", attempt, result)
	}
	if attempt.Outcome != sandboxobservability.OutcomeAccepted {
		t.Fatalf("attempt outcome = %q, want accepted", attempt.Outcome)
	}
	if attempt.Action != "sandbox.pause" || attempt.Actor.Kind != sandboxobservability.ActorKindAPIKey || attempt.Actor.ID != "key-1" {
		t.Fatalf("attempt identity = %+v", attempt)
	}
	if attempt.OperationID == "" || result.OperationID != attempt.OperationID || result.Outcome != sandboxobservability.OutcomeSucceeded {
		t.Fatalf("operation correlation = %+v %+v", attempt, result)
	}
	for _, event := range writer.events {
		if err := sandboxobservability.VerifyEventIntegrity(event, key.Public().(ed25519.PublicKey)); err != nil {
			t.Fatalf("VerifyEventIntegrity() error = %v", err)
		}
	}
}

func TestSandboxAuditMiddlewareRecordsUnknownResultOnPanic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &recordingAuditWriter{}
	server := &Server{
		cfg:         &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true}},
		auditWriter: writer, auditSigningKey: key, logger: zap.NewNop(),
	}
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT, TeamID: "team-1", UserID: "user-1",
		}))
		c.Next()
	})
	router.POST("/api/v1/sandboxes/:id/pause", server.auditSandboxRequests(), func(*gin.Context) {
		panic("boom")
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sb-1/pause", nil))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
	if len(writer.events) != 2 || writer.events[1].Outcome != sandboxobservability.OutcomeUnknown {
		t.Fatalf("events = %#v, want unknown result after panic", writer.events)
	}
}

func TestPublicExposureAuditAttributesActorOnlyAfterCredentialVerification(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &recordingAuditWriter{}
	server := &Server{
		cfg: &config.ClusterGatewayConfig{
			GatewayConfig: config.GatewayConfig{RegionID: "region-1"},
			ClusterID:     "cluster-1", SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true},
		},
		auditWriter: writer, auditSigningKey: key, logger: zap.NewNop(),
	}
	tokenHash := sha256.Sum256([]byte("valid-token"))
	route := &mgr.SandboxAppServiceRoute{
		ID: "route-1", PathPrefix: "/api",
		Auth: &mgr.SandboxAppServiceRouteAuth{Mode: mgr.SandboxAppServiceRouteAuthModeBearer, BearerTokenSHA256: hex.EncodeToString(tokenHash[:])},
	}
	sandbox := &mgr.Sandbox{ID: "sb-1", TeamID: "team-1"}
	service := &mgr.SandboxAppService{ID: "service-1"}

	for _, tc := range []struct {
		name     string
		token    string
		wantKind sandboxobservability.ActorKind
	}{
		{name: "invalid credential remains anonymous", token: "wrong-token", wantKind: sandboxobservability.ActorKindAnonymous},
		{name: "verified credential is attributed", token: "valid-token", wantKind: sandboxobservability.ActorKindExposureCredential},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writer.events = nil
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodGet, "http://sandbox.example/api", nil)
			c.Request.RemoteAddr = "192.0.2.10:1234"
			c.Request.Header.Set("Authorization", "Bearer "+tc.token)
			finish, ok := server.beginExposureAudit(c, sandbox, service, route)
			if !ok {
				t.Fatal("beginExposureAudit() rejected available writer")
			}
			if server.authorizeSandboxServiceRoute(c, route) {
				c.Status(http.StatusNoContent)
			}
			finish()
			if len(writer.events) != 2 {
				t.Fatalf("events = %d, want 2", len(writer.events))
			}
			if writer.events[0].Actor.Kind != sandboxobservability.ActorKindAnonymous {
				t.Fatalf("attempt actor = %+v, want anonymous before verification", writer.events[0].Actor)
			}
			if writer.events[1].Actor.Kind != tc.wantKind {
				t.Fatalf("result actor = %+v, want %s", writer.events[1].Actor, tc.wantKind)
			}
		})
	}
}

func TestSandboxAuditMiddlewareFailsClosedBeforeOperation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	server := &Server{
		cfg: &config.ClusterGatewayConfig{
			SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true},
		},
		auditWriter:     &recordingAuditWriter{err: errors.New("clickhouse unavailable")},
		auditSigningKey: key,
		logger:          zap.NewNop(),
	}
	called := false
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT, TeamID: "team-1", UserID: "user-1",
		}))
		c.Next()
	})
	router.DELETE("/api/v1/sandboxes/:id", server.auditSandboxRequests(), func(c *gin.Context) {
		called = true
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/sandboxes/sb-1", nil)
	router.ServeHTTP(recorder, request)
	if called {
		t.Fatal("operation handler ran without a canonical audit attempt")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
}

func TestSandboxAuditMiddlewareWithholdsSuccessUntilResultCanonicalACK(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &failAfterAuditWriter{succeedFor: 1}
	delivery, err := newAuditResultDelivery(t.TempDir(), writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditResultDelivery() error = %v", err)
	}
	server := &Server{
		cfg:         &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true}},
		auditWriter: writer, auditSigningKey: key, auditResults: delivery, logger: zap.NewNop(),
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT, TeamID: "team-1", UserID: "user-1",
		}))
		c.Next()
	})
	router.POST("/api/v1/sandboxes/:id/pause", server.auditSandboxRequests(), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "paused"})
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sb-1/pause", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 while canonical result is pending: %s", recorder.Code, recorder.Body.String())
	}
	if body := recorder.Body.String(); !strings.Contains(body, "canonical audit result is pending") || !strings.Contains(body, `"audit_result":"pending"`) {
		t.Fatalf("pending response body = %s", body)
	}
	entries, err := os.ReadDir(delivery.dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("result spool entries = %d, want 1", len(entries))
	}
	if len(writer.storedEvents) != 1 || writer.storedEvents[0].Phase != sandboxobservability.EventPhaseAttempt {
		t.Fatalf("canonical events = %#v, want only attempt before recovery", writer.storedEvents)
	}
}

func TestSandboxAuditMiddlewareReportsUnrecordedWhenSpoolAndCanonicalFallbackFail(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &failAfterAuditWriter{succeedFor: 1}
	dir := t.TempDir()
	delivery, err := newAuditResultDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditResultDelivery() error = %v", err)
	}
	replaceAuditSpoolDirectoryWithFile(t, dir)
	server := &Server{
		cfg:         &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true}},
		auditWriter: writer, auditSigningKey: key, auditResults: delivery, logger: zap.NewNop(),
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT, TeamID: "team-1", UserID: "user-1",
		}))
		c.Next()
	})
	router.POST("/api/v1/sandboxes/:id/pause", server.auditSandboxRequests(), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "paused"})
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sb-1/pause", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 for unrecorded audit result: %s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "audit result could not be durably recorded") || !strings.Contains(body, `"audit_result":"unrecorded"`) {
		t.Fatalf("unrecorded response body = %s", body)
	}
	if strings.Contains(body, "pending") {
		t.Fatalf("unrecorded response must not claim pending: %s", body)
	}
}

func TestSandboxAuditMiddlewareKeepsSuccessAfterCanonicalACKWhenSpoolCleanupFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	dir := t.TempDir()
	writer := &failAfterAuditWriter{succeedFor: 2}
	writer.onSuccess = func(call int) {
		if call == 2 {
			replaceAuditSpoolDirectoryWithFile(t, dir)
		}
	}
	delivery, err := newAuditResultDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditResultDelivery() error = %v", err)
	}
	server := &Server{
		cfg:         &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true}},
		auditWriter: writer, auditSigningKey: key, auditResults: delivery, logger: zap.NewNop(),
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT, TeamID: "team-1", UserID: "user-1",
		}))
		c.Next()
	})
	router.POST("/api/v1/sandboxes/:id/pause", server.auditSandboxRequests(), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "paused"})
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sb-1/pause", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"status":"paused"`) {
		t.Fatalf("response after canonical ACK = %d %s", recorder.Code, recorder.Body.String())
	}
	if len(writer.storedEvents) != 2 || writer.storedEvents[1].Phase != sandboxobservability.EventPhaseResult {
		t.Fatalf("canonical events = %#v, want attempt and result", writer.storedEvents)
	}
}
