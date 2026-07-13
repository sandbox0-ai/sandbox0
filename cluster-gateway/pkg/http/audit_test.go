package http

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

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
	calls  int
	onCall func(int)
}

type failAfterAuditWriter struct {
	calls        int
	succeedFor   int
	storedEvents []sandboxobservability.Event
	onSuccess    func(call int)
}

type contextBlockingAuditWriter struct {
	deadline chan time.Time
}

func (w *contextBlockingAuditWriter) InsertEvents(ctx context.Context, _ []sandboxobservability.Event) error {
	deadline, _ := ctx.Deadline()
	w.deadline <- deadline
	<-ctx.Done()
	return ctx.Err()
}

func (*contextBlockingAuditWriter) InsertLogs(context.Context, []sandboxobservability.LogEntry) error {
	return nil
}

func (*contextBlockingAuditWriter) InsertRuntimeSamples(context.Context, []sandboxobservability.RuntimeSample) error {
	return nil
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
	w.calls++
	if w.onCall != nil {
		w.onCall(w.calls)
	}
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

func mustNewAuditDelivery(t *testing.T, writer sandboxobservability.Writer) *auditDelivery {
	t.Helper()
	delivery, err := newAuditDelivery(t.TempDir(), writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	return delivery
}

func assertFailedAuditAdmissionPair(t *testing.T, key ed25519.PrivateKey, events []sandboxobservability.Event) (sandboxobservability.Event, sandboxobservability.Event) {
	t.Helper()
	if len(events) != 2 {
		t.Fatalf("durable admission events = %#v, want attempt and failed result", events)
	}
	byPhase := make(map[sandboxobservability.EventPhase]sandboxobservability.Event, len(events))
	for _, event := range events {
		byPhase[event.Phase] = event
	}
	attempt, attemptOK := byPhase[sandboxobservability.EventPhaseAttempt]
	result, resultOK := byPhase[sandboxobservability.EventPhaseResult]
	if !attemptOK || !resultOK || result.ParentEventID != attempt.EventID {
		t.Fatalf("durable admission phases = %#v", events)
	}
	if result.EventID == attempt.EventID || result.OperationID != attempt.OperationID || result.Action != attempt.Action || result.Resource != attempt.Resource || result.Actor != attempt.Actor {
		t.Fatalf("failed admission correlation = attempt %#v, result %#v", attempt, result)
	}
	if result.TeamID != attempt.TeamID || result.SandboxID != attempt.SandboxID || result.RegionID != attempt.RegionID || result.ClusterID != attempt.ClusterID {
		t.Fatalf("failed admission scope = attempt %#v, result %#v", attempt, result)
	}
	if result.Request.RequestID != attempt.Request.RequestID || result.Request.TraceID != attempt.Request.TraceID || result.Request.SourceIP != attempt.Request.SourceIP || result.Request.UserAgent != attempt.Request.UserAgent || result.Request.HTTPMethod != attempt.Request.HTTPMethod || result.Request.Route != attempt.Request.Route {
		t.Fatalf("failed admission request correlation = attempt %#v, result %#v", attempt.Request, result.Request)
	}
	if result.Outcome != sandboxobservability.OutcomeFailed || result.Request.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("failed admission result = %#v", result)
	}
	for attribute, want := range map[string]any{
		"execution_started":  false,
		"failure_code":       "canonical_ack_unavailable",
		"failure_stage":      "canonical_audit_admission",
		"operation_executed": false,
	} {
		if got := result.Attributes[attribute]; got != want {
			t.Fatalf("result attribute %s = %#v, want %#v", attribute, got, want)
		}
	}
	for _, event := range events {
		if err := sandboxobservability.VerifyEventIntegrity(event, key.Public().(ed25519.PublicKey)); err != nil {
			t.Fatalf("VerifyEventIntegrity() error = %v", err)
		}
	}
	return attempt, result
}

func TestSandboxAuditMiddlewarePersistsTrustedAttemptAndResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &recordingAuditWriter{}
	delivery := mustNewAuditDelivery(t, writer)
	server := &Server{
		cfg: &config.ClusterGatewayConfig{
			ClusterID:            "cluster-1",
			GatewayConfig:        config.GatewayConfig{RegionID: "region-1"},
			SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true},
		},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
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
	delivery := mustNewAuditDelivery(t, writer)
	server := &Server{
		cfg:             &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true}},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
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
	delivery := mustNewAuditDelivery(t, writer)
	server := &Server{
		cfg: &config.ClusterGatewayConfig{
			GatewayConfig: config.GatewayConfig{RegionID: "region-1"},
			ClusterID:     "cluster-1", SandboxObservability: config.SandboxObservabilityConfig{
				AuditEnabled:      true,
				AuditDeliveryMode: sandboxobservability.AuditDeliveryModeCanonicalSync,
			},
		},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
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

func TestSandboxAuditMiddlewareMutationRemainsCanonicalWithDefaultMode(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &recordingAuditWriter{err: errors.New("clickhouse unavailable")}
	delivery := mustNewAuditDelivery(t, writer)
	server := &Server{
		cfg: &config.ClusterGatewayConfig{
			SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true},
		},
		auditSigningKey: key,
		auditDelivery:   delivery,
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
	events, err := delivery.loadLocked()
	if err != nil {
		t.Fatalf("loadLocked() error = %v", err)
	}
	attempt, _ := assertFailedAuditAdmissionPair(t, key, events)
	if writer.calls != 1 {
		t.Fatalf("canonical writer calls = %d, want only the attempt call", writer.calls)
	}
	for _, fragment := range []string{
		`"audit_attempt":"pending"`,
		`"audit_result":"captured"`,
		`"execution_started":false`,
		`"failure_code":"canonical_ack_unavailable"`,
		attempt.OperationID,
	} {
		if !strings.Contains(recorder.Body.String(), fragment) {
			t.Fatalf("response body %s missing %q", recorder.Body.String(), fragment)
		}
	}
}

func TestSandboxAuditMiddlewareInitialAttemptUnrecordedDoesNotFabricateResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &recordingAuditWriter{err: errors.New("clickhouse unavailable")}
	dir := t.TempDir()
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	replaceAuditSpoolDirectoryWithFile(t, dir)
	server := &Server{
		cfg:             &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true}},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
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
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodDelete, "/api/v1/sandboxes/sb-1", nil))
	if called {
		t.Fatal("operation handler ran after an unrecorded audit attempt")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	if writer.calls != 1 {
		t.Fatalf("canonical writer calls = %d, want only the unrecorded attempt", writer.calls)
	}
	for _, fragment := range []string{`"audit_attempt":"unrecorded"`, `"audit_result":"unrecorded"`} {
		if !strings.Contains(recorder.Body.String(), fragment) {
			t.Fatalf("response body %s missing %q", recorder.Body.String(), fragment)
		}
	}
}

func TestSandboxAuditMiddlewareAdmissionResultSpoolAndFallbackFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	dir := t.TempDir()
	backupDir := dir + "-attempt"
	var hookErr error
	writer := &recordingAuditWriter{err: errors.New("clickhouse unavailable")}
	writer.onCall = func(call int) {
		if call != 1 {
			return
		}
		if hookErr = os.Rename(dir, backupDir); hookErr != nil {
			return
		}
		hookErr = os.WriteFile(dir, []byte("not a directory"), 0o600)
	}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	server := &Server{
		cfg:             &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true}},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
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
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodDelete, "/api/v1/sandboxes/sb-1", nil))
	if hookErr != nil {
		t.Fatalf("spool failure hook error = %v", hookErr)
	}
	if called {
		t.Fatal("operation handler ran after the admission result became unrecorded")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	if writer.calls != 2 {
		t.Fatalf("canonical writer calls = %d, want attempt and result fallback", writer.calls)
	}
	for _, fragment := range []string{`"audit_attempt":"pending"`, `"audit_result":"unrecorded"`} {
		if !strings.Contains(recorder.Body.String(), fragment) {
			t.Fatalf("response body %s missing %q", recorder.Body.String(), fragment)
		}
	}
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", backupDir, err)
	}
	if len(entries) != 1 {
		t.Fatalf("preserved attempt entries = %d, want 1", len(entries))
	}
	raw, err := os.ReadFile(backupDir + "/" + entries[0].Name())
	if err != nil {
		t.Fatalf("ReadFile(preserved attempt) error = %v", err)
	}
	var attempt sandboxobservability.Event
	if err := json.Unmarshal(raw, &attempt); err != nil {
		t.Fatalf("decode preserved attempt: %v", err)
	}
	if attempt.Phase != sandboxobservability.EventPhaseAttempt {
		t.Fatalf("preserved event phase = %q, want attempt", attempt.Phase)
	}
	if err := sandboxobservability.VerifyEventIntegrity(attempt, key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("VerifyEventIntegrity(preserved attempt) error = %v", err)
	}
}

func TestSandboxAuditAttemptBoundsBlockingCanonicalWriter(t *testing.T) {
	for _, tc := range []struct {
		name       string
		method     string
		path       string
		breakSpool bool
	}{
		{name: "canonical mutation", method: http.MethodDelete, path: "/api/v1/sandboxes/sb-1"},
		{name: "durable GET canonical fallback", method: http.MethodGet, path: "/api/v1/sandboxes/sb-1", breakSpool: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
			writer := &contextBlockingAuditWriter{deadline: make(chan time.Time, 1)}
			dir := t.TempDir()
			delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
			if err != nil {
				t.Fatalf("newAuditDelivery() error = %v", err)
			}
			if tc.breakSpool {
				replaceAuditSpoolDirectoryWithFile(t, dir)
			}
			server := &Server{
				cfg:             &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true}},
				auditSigningKey: key,
				auditDelivery:   delivery,
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
			router.Handle(tc.method, "/api/v1/sandboxes/:id", server.auditSandboxRequests(), func(c *gin.Context) {
				called = true
				c.Status(http.StatusNoContent)
			})

			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			request := httptest.NewRequest(tc.method, tc.path, nil).WithContext(requestCtx)
			recorder := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				defer close(done)
				router.ServeHTTP(recorder, request)
			}()

			select {
			case deadline := <-writer.deadline:
				remaining := time.Until(deadline)
				if deadline.IsZero() || remaining <= 0 || remaining > auditCanonicalDeliveryTimeout+time.Second {
					t.Fatalf("canonical writer deadline = %v, remaining = %v", deadline, remaining)
				}
				cancelRequest()
			case <-time.After(time.Second):
				t.Fatal("blocking canonical writer was not called")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("audit attempt did not stop after its context was canceled")
			}
			if called {
				t.Fatal("handler ran while canonical audit was blocked")
			}
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", recorder.Code)
			}
			if !tc.breakSpool {
				events, err := delivery.loadLocked()
				if err != nil {
					t.Fatalf("loadLocked() error = %v", err)
				}
				assertFailedAuditAdmissionPair(t, key, events)
			}
		})
	}
}

func TestSandboxAuditMiddlewareGETUsesDurableAsyncWhenClickHouseIsDown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &recordingAuditWriter{err: errors.New("clickhouse unavailable")}
	delivery := mustNewAuditDelivery(t, writer)
	server := &Server{
		cfg: &config.ClusterGatewayConfig{
			SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true},
		},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
	}
	called := false
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT, TeamID: "team-1", UserID: "user-1",
		}))
		c.Next()
	})
	router.GET("/api/v1/sandboxes/:id", server.auditSandboxRequests(), func(c *gin.Context) {
		called = true
		c.JSON(http.StatusOK, gin.H{"id": c.Param("id")})
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sb-1", nil))
	if !called || recorder.Code != http.StatusOK {
		t.Fatalf("durable GET response = %d %s, handler called = %t", recorder.Code, recorder.Body.String(), called)
	}
	events, err := delivery.loadLocked()
	if err != nil {
		t.Fatalf("loadLocked() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("durable audit events = %#v, want attempt and result", events)
	}
	phases := map[sandboxobservability.EventPhase]bool{}
	for _, event := range events {
		phases[event.Phase] = true
	}
	if !phases[sandboxobservability.EventPhaseAttempt] || !phases[sandboxobservability.EventPhaseResult] {
		t.Fatalf("durable audit phases = %#v", phases)
	}
}

func TestSandboxAuditMiddlewareGETCanonicalSyncOverrideFailsClosed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &recordingAuditWriter{err: errors.New("clickhouse unavailable")}
	delivery := mustNewAuditDelivery(t, writer)
	server := &Server{
		cfg: &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{
			AuditEnabled:      true,
			AuditDeliveryMode: sandboxobservability.AuditDeliveryModeCanonicalSync,
		}},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
	}
	called := false
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT, TeamID: "team-1", UserID: "user-1",
		}))
		c.Next()
	})
	router.GET("/api/v1/sandboxes/:id", server.auditSandboxRequests(), func(c *gin.Context) {
		called = true
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sb-1", nil))
	if called {
		t.Fatal("GET handler ran without canonical audit acknowledgement")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("strict GET status = %d, want 503", recorder.Code)
	}
	events, err := delivery.loadLocked()
	if err != nil {
		t.Fatalf("loadLocked() error = %v", err)
	}
	assertFailedAuditAdmissionPair(t, key, events)
	if writer.calls != 1 {
		t.Fatalf("canonical writer calls = %d, want only the attempt call", writer.calls)
	}
}

func TestPublicExposureAuditUsesDurableAsyncWhenClickHouseIsDown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &recordingAuditWriter{err: errors.New("clickhouse unavailable")}
	delivery := mustNewAuditDelivery(t, writer)
	server := &Server{
		cfg: &config.ClusterGatewayConfig{
			GatewayConfig:        config.GatewayConfig{RegionID: "region-1"},
			ClusterID:            "cluster-1",
			SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true},
		},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "http://sandbox.example/api", nil)
	finish, ok := server.beginExposureAudit(c,
		&mgr.Sandbox{ID: "sb-1", TeamID: "team-1"},
		&mgr.SandboxAppService{ID: "service-1"},
		&mgr.SandboxAppServiceRoute{ID: "route-1", PathPrefix: "/api"},
	)
	if !ok {
		t.Fatalf("beginExposureAudit() rejected durable event: %d %s", recorder.Code, recorder.Body.String())
	}
	c.Status(http.StatusNoContent)
	c.Writer.WriteHeaderNow()
	finish()
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("public exposure response = %d %s", recorder.Code, recorder.Body.String())
	}
	events, err := delivery.loadLocked()
	if err != nil {
		t.Fatalf("loadLocked() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("public exposure durable events = %#v, want attempt and result", events)
	}
}

func TestPublicExposureAuditCanonicalSyncFailureRecordsFailedResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &recordingAuditWriter{err: errors.New("clickhouse unavailable")}
	delivery := mustNewAuditDelivery(t, writer)
	server := &Server{
		cfg: &config.ClusterGatewayConfig{
			GatewayConfig: config.GatewayConfig{RegionID: "region-1"},
			ClusterID:     "cluster-1",
			SandboxObservability: config.SandboxObservabilityConfig{
				AuditEnabled:      true,
				AuditDeliveryMode: sandboxobservability.AuditDeliveryModeCanonicalSync,
			},
		},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "http://sandbox.example/api", nil)
	finish, ok := server.beginExposureAudit(c,
		&mgr.Sandbox{ID: "sb-1", TeamID: "team-1"},
		&mgr.SandboxAppService{ID: "service-1"},
		&mgr.SandboxAppServiceRoute{ID: "route-1", PathPrefix: "/api"},
	)
	if ok {
		t.Fatal("beginExposureAudit() admitted invocation without canonical acknowledgement")
	}
	finish()
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("public exposure response = %d %s", recorder.Code, recorder.Body.String())
	}
	events, err := delivery.loadLocked()
	if err != nil {
		t.Fatalf("loadLocked() error = %v", err)
	}
	attempt, result := assertFailedAuditAdmissionPair(t, key, events)
	if attempt.Action != "sandbox.service.invoke" || result.Resource.Type != "sandbox_service" {
		t.Fatalf("public exposure admission pair = attempt %#v, result %#v", attempt, result)
	}
	if writer.calls != 1 {
		t.Fatalf("canonical writer calls = %d, want only the attempt call", writer.calls)
	}
	for _, fragment := range []string{`"audit_attempt":"pending"`, `"audit_result":"captured"`, attempt.OperationID} {
		if !strings.Contains(recorder.Body.String(), fragment) {
			t.Fatalf("response body %s missing %q", recorder.Body.String(), fragment)
		}
	}
}

func TestSandboxAuditMiddlewareWithholdsSuccessUntilResultCanonicalACK(t *testing.T) {
	gin.SetMode(gin.TestMode)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	writer := &failAfterAuditWriter{succeedFor: 1}
	delivery, err := newAuditDelivery(t.TempDir(), writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	server := &Server{
		cfg:             &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true}},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
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
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	replaceAuditSpoolDirectoryWithFile(t, dir)
	server := &Server{
		cfg:             &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true}},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
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
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	server := &Server{
		cfg:             &config.ClusterGatewayConfig{SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true}},
		auditSigningKey: key, auditDelivery: delivery, logger: zap.NewNop(),
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
