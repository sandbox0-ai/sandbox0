package http

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	clustermiddleware "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewayhandlers "github.com/sandbox0-ai/sandbox0/pkg/gateway/http/handlers"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
)

func TestSandboxObservabilityIngestPlatformGuardDeniesBeforeBodyRead(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server, _, _ := testMeteringRouteServer(t, authModeInternal)
	signer := configureSandboxObservabilityIngestTestAuth(t, server)
	guard := newSandboxObservabilityTestOverloadGuard(
		t,
		config.OverloadGuardConfig{
			RequestsPerSecond:      1,
			Burst:                  1,
			LocalRequestsPerSecond: 1,
			LocalBurst:             1,
			MaxInFlight:            1,
		},
	)
	primePublicOverloadGuard(t, guard)
	server.publicOverloadGuard = guard
	rateLimiter := &sandboxObservabilityTestRateLimiter{}
	concurrencyLimiter := &sandboxObservabilityTestConcurrencyLimiter{}
	server.teamQuotaController = newSandboxObservabilityTestQuotaController(
		rateLimiter,
		concurrencyLimiter,
	)
	server.setupSandboxObservabilityIngestRoutes()

	body := newSandboxObservabilityTrackingBody(`{"logs":[]}`)
	request := newSandboxObservabilityIngestTestRequest(
		t,
		signer,
		"manager",
		"/internal/v1/sandbox-observability/logs",
		body,
	)
	response := httptest.NewRecorder()
	server.router.ServeHTTP(response, request)

	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", response.Code, response.Body.String())
	}
	if body.readCalls != 0 {
		t.Fatalf("body read calls = %d, want 0", body.readCalls)
	}
	if concurrencyLimiter.acquireCalls != 0 {
		t.Fatalf("active request acquisitions = %d, want 0", concurrencyLimiter.acquireCalls)
	}
	if rateLimiter.calls != 0 {
		t.Fatalf("observability byte admissions = %d, want 0", rateLimiter.calls)
	}
}

func TestSandboxObservabilityIngestActiveRequestDenialPrecedesBodyRead(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server, _, _ := testMeteringRouteServer(t, authModeInternal)
	signer := configureSandboxObservabilityIngestTestAuth(t, server)
	server.publicOverloadGuard = newSandboxObservabilityTestOverloadGuard(
		t,
		config.OverloadGuardConfig{},
	)
	rateLimiter := &sandboxObservabilityTestRateLimiter{}
	concurrencyLimiter := &sandboxObservabilityTestConcurrencyLimiter{
		acquireErr: &coreteamquota.ConcurrencyExceededError{
			TeamID: "team-1",
			Key:    coreteamquota.KeyActiveRequestCount,
			Limit:  1,
			Used:   1,
		},
	}
	server.teamQuotaController = newSandboxObservabilityTestQuotaController(
		rateLimiter,
		concurrencyLimiter,
	)
	server.setupSandboxObservabilityIngestRoutes()

	body := newSandboxObservabilityTrackingBody(`{"logs":[]}`)
	request := newSandboxObservabilityIngestTestRequest(
		t,
		signer,
		"manager",
		"/internal/v1/sandbox-observability/logs",
		body,
	)
	response := httptest.NewRecorder()
	server.router.ServeHTTP(response, request)

	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "active_request_count") {
		t.Fatalf("response does not identify active_request_count: %s", response.Body.String())
	}
	if body.readCalls != 0 {
		t.Fatalf("body read calls = %d, want 0", body.readCalls)
	}
	if concurrencyLimiter.acquireCalls != 1 {
		t.Fatalf("active request acquisitions = %d, want 1", concurrencyLimiter.acquireCalls)
	}
	if concurrencyLimiter.active != 0 || concurrencyLimiter.releaseCalls != 0 {
		t.Fatalf(
			"active requests = %d, releases = %d, want 0/0",
			concurrencyLimiter.active,
			concurrencyLimiter.releaseCalls,
		)
	}
	if rateLimiter.calls != 0 {
		t.Fatalf("observability byte admissions = %d, want 0", rateLimiter.calls)
	}
}

func TestSandboxObservabilityIngestRoutesHoldAndReleaseActiveRequestLease(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server, _, _ := testMeteringRouteServer(t, authModeInternal)
	signer := configureSandboxObservabilityIngestTestAuth(t, server)
	server.publicOverloadGuard = newSandboxObservabilityTestOverloadGuard(
		t,
		config.OverloadGuardConfig{},
	)
	server.sandboxAuditEntitlements = licensing.NewStaticEntitlements(
		licensing.FeatureSandboxAudit,
	)
	rateLimiter := &sandboxObservabilityTestRateLimiter{}
	concurrencyLimiter := &sandboxObservabilityTestConcurrencyLimiter{}
	server.teamQuotaController = newSandboxObservabilityTestQuotaController(
		rateLimiter,
		concurrencyLimiter,
	)
	server.setupSandboxObservabilityIngestRoutes()

	tests := []struct {
		path    string
		payload string
		caller  string
	}{
		{
			path:    "/internal/v1/sandbox-observability/events",
			payload: `{"events":[]}`,
			caller:  "netd",
		},
		{
			path:    "/internal/v1/sandbox-observability/logs",
			payload: `{"logs":[]}`,
			caller:  "manager",
		},
		{
			path:    "/internal/v1/sandbox-observability/runtime-samples",
			payload: `{"samples":[]}`,
			caller:  "ctld",
		},
	}
	var admittedBytes int64
	for index, test := range tests {
		body := newSandboxObservabilityTrackingBody(test.payload)
		request := newSandboxObservabilityIngestTestRequest(
			t,
			signer,
			test.caller,
			test.path,
			body,
		)
		response := httptest.NewRecorder()
		server.router.ServeHTTP(response, request)

		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf(
				"%s status = %d, want handler-level 503; body=%s",
				test.path,
				response.Code,
				response.Body.String(),
			)
		}
		if body.readCalls == 0 {
			t.Fatalf("%s body was not read after admission", test.path)
		}
		wantCalls := int64(index + 1)
		if concurrencyLimiter.acquireCalls != wantCalls ||
			concurrencyLimiter.releaseCalls != wantCalls ||
			concurrencyLimiter.active != 0 {
			t.Fatalf(
				"%s acquisitions/releases/active = %d/%d/%d, want %d/%d/0",
				test.path,
				concurrencyLimiter.acquireCalls,
				concurrencyLimiter.releaseCalls,
				concurrencyLimiter.active,
				wantCalls,
				wantCalls,
			)
		}
		admittedBytes += int64(len(test.payload))
		if rateLimiter.calls != wantCalls || rateLimiter.cost != admittedBytes {
			t.Fatalf(
				"%s byte admissions/cost = %d/%d, want %d/%d",
				test.path,
				rateLimiter.calls,
				rateLimiter.cost,
				wantCalls,
				admittedBytes,
			)
		}
		if rateLimiter.lastKey != coreteamquota.KeyObservabilityIngestBytes {
			t.Fatalf(
				"%s byte admission key = %q, want %q",
				test.path,
				rateLimiter.lastKey,
				coreteamquota.KeyObservabilityIngestBytes,
			)
		}
	}
}

func TestSandboxObservabilityIngestEmptyAndOversizedBodiesReleaseLease(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server, _, _ := testMeteringRouteServer(t, authModeInternal)
	signer := configureSandboxObservabilityIngestTestAuth(t, server)
	server.publicOverloadGuard = newSandboxObservabilityTestOverloadGuard(
		t,
		config.OverloadGuardConfig{},
	)
	rateLimiter := &sandboxObservabilityTestRateLimiter{}
	concurrencyLimiter := &sandboxObservabilityTestConcurrencyLimiter{}
	server.teamQuotaController = newSandboxObservabilityTestQuotaController(
		rateLimiter,
		concurrencyLimiter,
	)
	server.setupSandboxObservabilityIngestRoutes()

	emptyRequest := newSandboxObservabilityIngestTestRequest(
		t,
		signer,
		"manager",
		"/internal/v1/sandbox-observability/logs",
		http.NoBody,
	)
	emptyResponse := httptest.NewRecorder()
	server.router.ServeHTTP(emptyResponse, emptyRequest)
	if emptyResponse.Code != http.StatusBadRequest {
		t.Fatalf(
			"empty body status = %d, want 400; body=%s",
			emptyResponse.Code,
			emptyResponse.Body.String(),
		)
	}
	if concurrencyLimiter.acquireCalls != 1 ||
		concurrencyLimiter.releaseCalls != 1 ||
		concurrencyLimiter.active != 0 {
		t.Fatalf(
			"empty body acquisitions/releases/active = %d/%d/%d, want 1/1/0",
			concurrencyLimiter.acquireCalls,
			concurrencyLimiter.releaseCalls,
			concurrencyLimiter.active,
		)
	}
	if rateLimiter.calls != 0 || rateLimiter.cost != 0 {
		t.Fatalf(
			"empty body byte admissions/cost = %d/%d, want 0/0",
			rateLimiter.calls,
			rateLimiter.cost,
		)
	}

	oversizedBody := newSandboxObservabilityTrackingBody(`{"logs":[]}`)
	oversizedRequest := newSandboxObservabilityIngestTestRequest(
		t,
		signer,
		"manager",
		"/internal/v1/sandbox-observability/logs",
		oversizedBody,
	)
	oversizedRequest.ContentLength = gatewayhandlers.MaxSandboxObservabilityIngestBytes + 1
	oversizedResponse := httptest.NewRecorder()
	server.router.ServeHTTP(oversizedResponse, oversizedRequest)
	if oversizedResponse.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf(
			"oversized body status = %d, want 413; body=%s",
			oversizedResponse.Code,
			oversizedResponse.Body.String(),
		)
	}
	if oversizedBody.readCalls != 0 {
		t.Fatalf("oversized body read calls = %d, want 0", oversizedBody.readCalls)
	}
	if concurrencyLimiter.acquireCalls != 2 ||
		concurrencyLimiter.releaseCalls != 2 ||
		concurrencyLimiter.active != 0 {
		t.Fatalf(
			"oversized body acquisitions/releases/active = %d/%d/%d, want 2/2/0",
			concurrencyLimiter.acquireCalls,
			concurrencyLimiter.releaseCalls,
			concurrencyLimiter.active,
		)
	}
	if rateLimiter.calls != 0 || rateLimiter.cost != 0 {
		t.Fatalf(
			"oversized body byte admissions/cost = %d/%d, want 0/0",
			rateLimiter.calls,
			rateLimiter.cost,
		)
	}
}

func TestSandboxObservabilityIngestAdmissionDependenciesFailClosedBeforeBodyRead(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name      string
		configure func(*testing.T, *Server)
	}{
		{
			name: "missing platform overload guard",
			configure: func(_ *testing.T, server *Server) {
				server.publicOverloadGuard = nil
				server.teamQuotaController = newSandboxObservabilityTestQuotaController(
					&sandboxObservabilityTestRateLimiter{},
					&sandboxObservabilityTestConcurrencyLimiter{},
				)
			},
		},
		{
			name: "missing team quota controller",
			configure: func(t *testing.T, server *Server) {
				server.publicOverloadGuard = newSandboxObservabilityTestOverloadGuard(
					t,
					config.OverloadGuardConfig{},
				)
				server.teamQuotaController = nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, _, _ := testMeteringRouteServer(t, authModeInternal)
			signer := configureSandboxObservabilityIngestTestAuth(t, server)
			test.configure(t, server)
			server.setupSandboxObservabilityIngestRoutes()

			body := newSandboxObservabilityTrackingBody(`{"logs":[]}`)
			request := newSandboxObservabilityIngestTestRequest(
				t,
				signer,
				"manager",
				"/internal/v1/sandbox-observability/logs",
				body,
			)
			response := httptest.NewRecorder()
			server.router.ServeHTTP(response, request)

			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf(
					"status = %d, want 503; body=%s",
					response.Code,
					response.Body.String(),
				)
			}
			if body.readCalls != 0 {
				t.Fatalf("body read calls = %d, want 0", body.readCalls)
			}
		})
	}
}

func configureSandboxObservabilityIngestTestAuth(
	t *testing.T,
	server *Server,
) sandboxObservabilityIngestTestSigner {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ingest auth key: %v", err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             internalauth.ServiceClusterGateway,
		PublicKey:          publicKey,
		AllowedCallers:     []string{"ctld", "manager", "netd"},
		ClockSkewTolerance: 5 * time.Second,
	})
	auth := clustermiddleware.NewInternalAuthMiddleware(validator, zap.NewNop())
	server.sandboxAuditIngestAuthMiddleware = auth
	server.sandboxObservabilityIngestAuthMiddleware = auth
	return sandboxObservabilityIngestTestSigner{privateKey: privateKey}
}

func newSandboxObservabilityIngestTestRequest(
	t *testing.T,
	signer sandboxObservabilityIngestTestSigner,
	caller string,
	path string,
	body io.ReadCloser,
) *http.Request {
	t.Helper()
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     caller,
		PrivateKey: signer.privateKey,
		TTL:        time.Minute,
	})
	token, err := generator.Generate(
		internalauth.ServiceClusterGateway,
		"team-1",
		"producer-1",
		internalauth.GenerateOptions{
			Permissions: []string{authn.PermSandboxObservabilityWrite},
		},
	)
	if err != nil {
		t.Fatalf("generate ingest token: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, path, body)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(internalauth.DefaultTokenHeader, token)
	return request
}

type sandboxObservabilityIngestTestSigner struct {
	privateKey ed25519.PrivateKey
}

func newSandboxObservabilityTestOverloadGuard(
	t *testing.T,
	cfg config.OverloadGuardConfig,
) *gatewaymiddleware.OverloadGuard {
	t.Helper()
	guard, err := gatewaymiddleware.NewOverloadGuard(
		context.Background(),
		cfg,
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("create overload guard: %v", err)
	}
	t.Cleanup(func() { _ = guard.Close() })
	return guard
}

func newSandboxObservabilityTestQuotaController(
	rateLimiter *sandboxObservabilityTestRateLimiter,
	concurrencyLimiter *sandboxObservabilityTestConcurrencyLimiter,
) *gatewayteamquota.Controller {
	return gatewayteamquota.NewController(
		nil,
		nil,
		rateLimiter,
		nil,
		zap.NewNop(),
		gatewayteamquota.WithConcurrencyLimiter(concurrencyLimiter),
	)
}

type sandboxObservabilityTrackingBody struct {
	reader    *bytes.Reader
	readCalls int
}

func newSandboxObservabilityTrackingBody(value string) *sandboxObservabilityTrackingBody {
	return &sandboxObservabilityTrackingBody{reader: bytes.NewReader([]byte(value))}
}

func (b *sandboxObservabilityTrackingBody) Read(dst []byte) (int, error) {
	b.readCalls++
	return b.reader.Read(dst)
}

func (*sandboxObservabilityTrackingBody) Close() error {
	return nil
}

type sandboxObservabilityTestRateLimiter struct {
	mu      sync.Mutex
	calls   int64
	cost    int64
	lastKey coreteamquota.Key
}

func (l *sandboxObservabilityTestRateLimiter) Take(
	_ context.Context,
	_ string,
	key coreteamquota.Key,
	cost int64,
) (tokenbucket.Decision, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	l.cost += cost
	l.lastKey = key
	return tokenbucket.Decision{Allowed: true, Remaining: 1 << 30}, nil
}

func (*sandboxObservabilityTestRateLimiter) Invalidate(string, coreteamquota.Key) {}

type sandboxObservabilityTestConcurrencyLimiter struct {
	mu sync.Mutex

	acquireErr   error
	acquireCalls int64
	releaseCalls int64
	active       int64
}

func (l *sandboxObservabilityTestConcurrencyLimiter) Acquire(
	_ context.Context,
	_ string,
	_ coreteamquota.Key,
) (gatewayteamquota.ConnectionLease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acquireCalls++
	if l.acquireErr != nil {
		return nil, l.acquireErr
	}
	l.active++
	return &sandboxObservabilityTestLease{
		limiter: l,
		done:    make(chan struct{}),
	}, nil
}

func (*sandboxObservabilityTestConcurrencyLimiter) Usage(
	context.Context,
	string,
	coreteamquota.Key,
) (int64, error) {
	return 0, nil
}

func (*sandboxObservabilityTestConcurrencyLimiter) Invalidate(string, coreteamquota.Key) {}
func (*sandboxObservabilityTestConcurrencyLimiter) Close() error                         { return nil }

type sandboxObservabilityTestLease struct {
	limiter *sandboxObservabilityTestConcurrencyLimiter
	done    chan struct{}
	once    sync.Once
}

func (l *sandboxObservabilityTestLease) Done() <-chan struct{} {
	return l.done
}

func (*sandboxObservabilityTestLease) Err() error {
	return nil
}

func (l *sandboxObservabilityTestLease) Release(context.Context) error {
	l.once.Do(func() {
		l.limiter.mu.Lock()
		l.limiter.active--
		l.limiter.releaseCalls++
		l.limiter.mu.Unlock()
		close(l.done)
	})
	return nil
}
