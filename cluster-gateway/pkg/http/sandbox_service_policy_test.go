package http

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxfunction"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSandboxServiceProxiesAuthorizedRouteWithRewrite(t *testing.T) {
	upstreamHits := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		if r.URL.Path != "/v1/users" {
			t.Fatalf("upstream path = %q, want /v1/users", r.URL.Path)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	port := serverPort(t, upstream.URL)
	tokenHash := sha256Hex("secret-token")
	rewritePrefix := "/v1"
	gateway := newSandboxServiceExposureTestServer(t, &mgr.Sandbox{
		ID:           "sb-demo",
		TeamID:       "team-1",
		InternalAddr: "http://127.0.0.1:1",
		AutoResume:   true,
		Services: []mgr.SandboxAppService{{
			ID:   "api",
			Port: port,
			Ingress: mgr.SandboxAppServiceIngress{
				Public: true,
				Routes: []mgr.SandboxAppServiceRoute{{
					ID:            "api",
					PathPrefix:    "/api",
					Methods:       []string{http.MethodGet},
					RewritePrefix: &rewritePrefix,
					Auth: &mgr.SandboxAppServiceRouteAuth{
						Mode:              mgr.SandboxAppServiceRouteAuthModeBearer,
						BearerTokenSHA256: tokenHash,
					},
				}},
			},
		}},
	})

	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()
	req := newGatewayRequest(t, http.MethodGet, gatewayServer.URL, fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port), "/api/users")
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if upstreamHits != 1 {
		t.Fatalf("upstream hits = %d, want 1", upstreamHits)
	}
}

func TestSandboxServiceInvalidRouteCredentialConsumesTeamQuotaAndReleasesLease(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream should not be called")
	}))
	defer upstream.Close()
	port := serverPort(t, upstream.URL)
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/sandboxes/sb-demo" {
			spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
			return
		}
		_ = spec.WriteSuccess(w, http.StatusOK, &mgr.Sandbox{
			ID:           "sb-demo",
			TeamID:       "team-1",
			InternalAddr: "http://127.0.0.1:1",
			Services: []mgr.SandboxAppService{{
				ID:   "api",
				Port: port,
				Ingress: mgr.SandboxAppServiceIngress{
					Public: true,
					Routes: []mgr.SandboxAppServiceRoute{{
						ID:         "api",
						PathPrefix: "/",
						Auth: &mgr.SandboxAppServiceRouteAuth{
							Mode:              mgr.SandboxAppServiceRouteAuthModeBearer,
							BearerTokenSHA256: sha256Hex("correct"),
						},
					}},
				},
			}},
		})
	}))
	defer manager.Close()
	controller, rateLimiter, concurrencyLimiter, networkLimiter :=
		newCountingPublicExposureTeamQuotaController(zap.NewNop())
	gateway := newSandboxServiceExposureTestServerWithManagerURLAndQuota(
		t,
		manager.URL,
		controller,
	)
	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()

	request := newGatewayRequest(
		t,
		http.MethodPost,
		gatewayServer.URL,
		fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port),
		"/",
	)
	const requestBody = "rejected-request"
	request.Body = io.NopCloser(strings.NewReader(requestBody))
	request.ContentLength = int64(len(requestBody))
	request.Header.Set("Authorization", "Bearer wrong")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.StatusCode)
	}
	assertRejectedSandboxServiceTeamQuota(
		t,
		rateLimiter,
		concurrencyLimiter,
		networkLimiter,
		len(requestBody),
		len(responseBody),
	)
}

func TestSandboxServicePolicyRejectionsCannotBypassTeamQuota(t *testing.T) {
	tests := []struct {
		name           string
		requestMethod  string
		requestPath    string
		configure      func(*Server)
		configureReq   func(*http.Request)
		configureRoute func(*mgr.SandboxAppServiceRoute)
		observeAbuse   bool
		wantStatus     int
	}{
		{
			name:          "unmatched path",
			requestMethod: http.MethodPost,
			requestPath:   "/not-exposed",
			configureRoute: func(route *mgr.SandboxAppServiceRoute) {
				route.PathPrefix = "/api"
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:          "disallowed method",
			requestMethod: http.MethodPost,
			requestPath:   "/api",
			configureRoute: func(route *mgr.SandboxAppServiceRoute) {
				route.PathPrefix = "/api"
				route.Methods = []string{http.MethodGet}
			},
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:          "rejected CORS preflight",
			requestMethod: http.MethodOptions,
			requestPath:   "/api",
			configureReq: func(request *http.Request) {
				request.Header.Set("Origin", "https://blocked.example")
				request.Header.Set("Access-Control-Request-Method", http.MethodGet)
			},
			configureRoute: func(route *mgr.SandboxAppServiceRoute) {
				route.PathPrefix = "/api"
				route.Methods = []string{http.MethodGet}
				route.CORS = &mgr.SandboxAppServiceRouteCORS{
					AllowedOrigins: []string{"https://allowed.example"},
					AllowedMethods: []string{http.MethodGet},
				}
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:          "abuse guard rejection",
			requestMethod: http.MethodPost,
			requestPath:   "/api",
			configureRoute: func(route *mgr.SandboxAppServiceRoute) {
				route.PathPrefix = "/api"
				route.RateLimit = &mgr.SandboxAppServiceRouteRateLimit{
					RPS:   1,
					Burst: 1,
				}
			},
			observeAbuse: true,
			wantStatus:   http.StatusTooManyRequests,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("upstream should not be called")
			}))
			defer upstream.Close()

			port := serverPort(t, upstream.URL)
			route := mgr.SandboxAppServiceRoute{
				ID:         "api",
				PathPrefix: "/",
			}
			test.configureRoute(&route)
			manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/internal/v1/sandboxes/sb-demo" {
					spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
					return
				}
				_ = spec.WriteSuccess(w, http.StatusOK, &mgr.Sandbox{
					ID:           "sb-demo",
					TeamID:       "team-1",
					InternalAddr: "http://127.0.0.1:1",
					Services: []mgr.SandboxAppService{{
						ID:   "api",
						Port: port,
						Ingress: mgr.SandboxAppServiceIngress{
							Public: true,
							Routes: []mgr.SandboxAppServiceRoute{route},
						},
					}},
				})
			}))
			defer manager.Close()

			controller, rateLimiter, concurrencyLimiter, networkLimiter :=
				newCountingPublicExposureTeamQuotaController(zap.NewNop())
			var rateAdmissionsDuringAbuse atomic.Int64
			var activeConnectionsDuringAbuse atomic.Int64
			configure := test.configure
			if test.observeAbuse {
				configure = func(server *Server) {
					server.sandboxServiceAbuseGuard = &fakeSandboxServiceAbuseGuard{
						decision: ratelimit.Decision{Allowed: false},
						onAllow: func() {
							rateAdmissionsDuringAbuse.Store(rateLimiter.calls.Load())
							activeConnectionsDuringAbuse.Store(concurrencyLimiter.active.Load())
						},
					}
				}
			}
			gateway := newSandboxServiceExposureTestServerWithManagerURLQuotaAndConfigure(
				t,
				manager.URL,
				controller,
				configure,
			)
			gatewayServer := httptest.NewServer(gateway)
			defer gatewayServer.Close()

			const requestBody = "rejected-request"
			request := newGatewayRequest(
				t,
				test.requestMethod,
				gatewayServer.URL,
				fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port),
				test.requestPath,
			)
			request.Body = io.NopCloser(strings.NewReader(requestBody))
			request.ContentLength = int64(len(requestBody))
			if test.configureReq != nil {
				test.configureReq(request)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatalf("gateway request: %v", err)
			}
			defer response.Body.Close()
			responseBody, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatalf("read response: %v", err)
			}

			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", response.StatusCode, test.wantStatus, responseBody)
			}
			assertRejectedSandboxServiceTeamQuota(
				t,
				rateLimiter,
				concurrencyLimiter,
				networkLimiter,
				len(requestBody),
				len(responseBody),
			)
			if test.observeAbuse {
				if got := rateAdmissionsDuringAbuse.Load(); got != 1 {
					t.Fatalf("sandbox_service_requests admissions during abuse check = %d, want 1", got)
				}
				if got := activeConnectionsDuringAbuse.Load(); got != 1 {
					t.Fatalf("active_connection_count leases during abuse check = %d, want 1", got)
				}
			}
		})
	}
}

func TestSandboxServiceTeamQuotaDenialPrecedesRouteResolution(t *testing.T) {
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/internal/v1/sandboxes/sb-demo" {
			spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
			return
		}
		_ = spec.WriteSuccess(w, http.StatusOK, &mgr.Sandbox{
			ID:     "sb-demo",
			TeamID: "team-1",
		})
	}))
	defer manager.Close()

	gateway := newSandboxServiceExposureTestServerWithManagerURLAndQuota(
		t,
		manager.URL,
		newZeroRateTeamQuotaController(zap.NewNop()),
	)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "http://gateway/not-exposed", nil)
	request.Host = "sb-demo--p3000.aws-us-east-1.sandbox0.app"
	gateway.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf(
			"status = %d, want quota rejection %d before route rejection; body=%s",
			recorder.Code,
			http.StatusTooManyRequests,
			recorder.Body.String(),
		)
	}
}

func assertRejectedSandboxServiceTeamQuota(
	t *testing.T,
	rateLimiter *countingTeamQuotaRateLimiter,
	concurrencyLimiter *countingTeamQuotaConcurrencyLimiter,
	networkLimiter *countingTeamQuotaNetworkLimiter,
	requestBytes int,
	responseBytes int,
) {
	t.Helper()
	if calls := rateLimiter.calls.Load(); calls != 1 {
		t.Fatalf("sandbox_service_requests admissions = %d, want 1", calls)
	}
	if teamID := rateLimiter.lastTeamID.Load(); teamID != "team-1" {
		t.Fatalf("sandbox_service_requests team = %v, want team-1", teamID)
	}
	if key := rateLimiter.lastKey.Load(); key != coreteamquota.KeySandboxServiceRequests {
		t.Fatalf("rate quota key = %v, want %s", key, coreteamquota.KeySandboxServiceRequests)
	}
	if calls := concurrencyLimiter.acquireCalls.Load(); calls != 1 {
		t.Fatalf("active_connection_count acquisitions = %d, want 1", calls)
	}
	if teamID := concurrencyLimiter.lastTeamID.Load(); teamID != "team-1" {
		t.Fatalf("active_connection_count team = %v, want team-1", teamID)
	}
	if key := concurrencyLimiter.lastKey.Load(); key != coreteamquota.KeyActiveConnectionCount {
		t.Fatalf("concurrency quota key = %v, want %s", key, coreteamquota.KeyActiveConnectionCount)
	}
	if calls := concurrencyLimiter.releaseCalls.Load(); calls != 1 {
		t.Fatalf("active_connection_count releases = %d, want 1", calls)
	}
	if active := concurrencyLimiter.active.Load(); active != 0 {
		t.Fatalf("active_connection_count live leases = %d, want 0", active)
	}
	if got := networkLimiter.BytesFor(coreteamquota.KeyNetworkIngressBytes); got != int64(requestBytes) {
		t.Fatalf("network ingress bytes = %d, want %d", got, requestBytes)
	}
	if got := networkLimiter.BytesFor(coreteamquota.KeyNetworkEgressBytes); got != int64(responseBytes) {
		t.Fatalf("network egress bytes = %d, want %d", got, responseBytes)
	}
	if got := networkLimiter.callsWithoutActiveLease.Load(); got != 0 {
		t.Fatalf("network accounting calls without an active_connection_count lease = %d, want 0", got)
	}
}

func TestSandboxServiceRejectsDisallowedMethod(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("upstream should not be called")
	}))
	defer upstream.Close()

	port := serverPort(t, upstream.URL)
	gateway := newSandboxServiceExposureTestServer(t, &mgr.Sandbox{
		ID:           "sb-demo",
		TeamID:       "team-1",
		InternalAddr: "http://127.0.0.1:1",
		AutoResume:   true,
		Services: []mgr.SandboxAppService{{
			ID:   "api",
			Port: port,
			Ingress: mgr.SandboxAppServiceIngress{
				Public: true,
				Routes: []mgr.SandboxAppServiceRoute{{
					ID:         "api",
					PathPrefix: "/api",
					Methods:    []string{http.MethodGet},
				}},
			},
		}},
	})

	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()
	req := newGatewayRequest(t, http.MethodPost, gatewayServer.URL, fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port), "/api/users")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestSandboxServiceHandlesCORSPreflight(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("upstream should not be called")
	}))
	defer upstream.Close()

	port := serverPort(t, upstream.URL)
	gateway := newSandboxServiceExposureTestServer(t, &mgr.Sandbox{
		ID:           "sb-demo",
		TeamID:       "team-1",
		InternalAddr: "http://127.0.0.1:1",
		AutoResume:   true,
		Services: []mgr.SandboxAppService{{
			ID:   "api",
			Port: port,
			Ingress: mgr.SandboxAppServiceIngress{
				Public: true,
				Routes: []mgr.SandboxAppServiceRoute{{
					ID:         "api",
					PathPrefix: "/api",
					Methods:    []string{http.MethodGet},
					CORS: &mgr.SandboxAppServiceRouteCORS{
						AllowedOrigins: []string{"https://app.example"},
						AllowedMethods: []string{http.MethodGet},
						AllowedHeaders: []string{"Authorization"},
						MaxAgeSeconds:  60,
					},
				}},
			},
		}},
	})

	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()
	req := newGatewayRequest(t, http.MethodOptions, gatewayServer.URL, fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port), "/api/users")
	req.Header.Set("Origin", "https://app.example")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Fatalf("Access-Control-Allow-Origin = %q", got)
	}
}

func TestSandboxServiceExposureReturnsNotFoundForMissingSandbox(t *testing.T) {
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/internal/v1/sandboxes/sb-missing" {
			t.Fatalf("unexpected manager request %s %s", r.Method, r.URL.Path)
		}
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
	}))
	defer manager.Close()

	controller, rateLimiter, concurrencyLimiter, networkLimiter :=
		newCountingPublicExposureTeamQuotaController(zap.NewNop())
	gateway := newSandboxServiceExposureTestServerWithManagerURLAndQuota(
		t,
		manager.URL,
		controller,
	)
	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()

	req := newGatewayRequest(t, http.MethodGet, gatewayServer.URL, "sb-missing--p3000.aws-us-east-1.sandbox0.app", "/")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
	if calls := rateLimiter.calls.Load(); calls != 0 {
		t.Fatalf("sandbox_service_requests admissions = %d, want 0 before ownership resolves", calls)
	}
	if calls := concurrencyLimiter.acquireCalls.Load(); calls != 0 {
		t.Fatalf("active_connection_count acquisitions = %d, want 0 before ownership resolves", calls)
	}
	if bytes := networkLimiter.Bytes(); bytes != 0 {
		t.Fatalf("network bytes = %d, want 0 before ownership resolves", bytes)
	}
}

func TestSandboxServiceRejectsResumeRouteWithoutRestartableRuntime(t *testing.T) {
	var resumed atomic.Bool
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/sandboxes/sb-demo":
			_ = spec.WriteSuccess(w, http.StatusOK, &mgr.Sandbox{
				ID:         "sb-demo",
				TeamID:     "team-1",
				AutoResume: true,
				Status:     mgr.SandboxStatusPaused,
				Paused:     true,
				Services: []mgr.SandboxAppService{{
					ID:   "api",
					Port: 3000,
					Ingress: mgr.SandboxAppServiceIngress{
						Public: true,
						Routes: []mgr.SandboxAppServiceRoute{{
							ID:         "api",
							PathPrefix: "/",
							Resume:     true,
						}},
					},
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sandboxes/sb-demo/resume":
			resumed.Store(true)
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"sandbox_id": "sb-demo"})
		default:
			t.Fatalf("unexpected manager request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer manager.Close()

	gateway := newSandboxServiceExposureTestServerWithManagerURL(t, manager.URL)
	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()

	req := newGatewayRequest(t, http.MethodGet, gatewayServer.URL, "sb-demo--p3000.aws-us-east-1.sandbox0.app", "/")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d body=%s, want %d", resp.StatusCode, string(body), http.StatusConflict)
	}
	if !strings.Contains(string(body), "restartable service runtime") {
		t.Fatalf("body = %q, want restartable runtime error", string(body))
	}
	if resumed.Load() {
		t.Fatal("manager resume was called for an unrestartable route")
	}
}

func TestSandboxFunctionServiceExecutesThroughProcdPort(t *testing.T) {
	var execReq sandboxfunction.ExecuteRequest
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/functions/execute" {
			t.Fatalf("procd path = %q, want /api/v1/functions/execute", r.URL.Path)
		}
		if r.Header.Get(internalauth.DefaultTokenHeader) == "" {
			t.Fatal("missing internal token")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read procd body: %v", err)
		}
		if err := json.Unmarshal(body, &execReq); err != nil {
			t.Fatalf("decode execute request: %v", err)
		}
		_ = spec.WriteSuccess(w, http.StatusOK, sandboxfunction.ExecuteResponse{
			Status:     http.StatusCreated,
			Headers:    map[string][]string{"content-type": {"text/plain"}, "x-function": {"ok"}},
			BodyBase64: "ZnVuY3Rpb24gb2s=",
		})
	}))
	defer procd.Close()

	port := serverPort(t, procd.URL)
	gateway := newSandboxServiceExposureTestServer(t, &mgr.Sandbox{
		ID:           "sb-demo",
		TeamID:       "team-1",
		UserID:       "user-1",
		InternalAddr: procd.URL,
		AutoResume:   true,
		Services: []mgr.SandboxAppService{{
			ID:   "webhook",
			Port: port,
			Runtime: &mgr.SandboxAppServiceRuntime{
				Type: mgr.SandboxAppServiceRuntimeFunction,
				Function: &mgr.SandboxFunction{
					Runtime:        "python",
					Handler:        "handler",
					MaxConcurrency: 2,
					Source: mgr.SandboxFunctionSource{
						Type: "inline",
						Code: "def handler(request):\n    return {'status': 201}\n",
					},
				},
			},
			Ingress: mgr.SandboxAppServiceIngress{
				Public: true,
				Routes: []mgr.SandboxAppServiceRoute{{
					ID:         "root",
					PathPrefix: "/events",
					Methods:    []string{http.MethodPost},
				}},
			},
		}},
	})

	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()
	req, err := http.NewRequest(http.MethodPost, gatewayServer.URL+"/events/stripe?source=test", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d body=%s", resp.StatusCode, string(body))
	}
	if string(body) != "function ok" {
		t.Fatalf("body = %q, want function ok", string(body))
	}
	if resp.Header.Get("x-function") != "ok" {
		t.Fatalf("x-function = %q, want ok", resp.Header.Get("x-function"))
	}
	if execReq.ServiceID != "webhook" || execReq.RouteID != "root" {
		t.Fatalf("execute service/route = %q/%q, want webhook/root", execReq.ServiceID, execReq.RouteID)
	}
	if execReq.MaxConcurrency != 2 {
		t.Fatalf("execute max_concurrency = %d, want 2", execReq.MaxConcurrency)
	}
	if execReq.Request.Path != "/events/stripe" || execReq.Request.RawQuery != "source=test" {
		t.Fatalf("execute path/query = %q/%q", execReq.Request.Path, execReq.Request.RawQuery)
	}
	decodedBody, err := base64.StdEncoding.DecodeString(execReq.Request.BodyBase64)
	if err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	if string(decodedBody) != "payload" {
		t.Fatalf("execute body = %q, want payload", string(decodedBody))
	}
}

func TestSandboxFunctionServiceStreamsSSEThroughProcdPort(t *testing.T) {
	var execReq sandboxfunction.ExecuteRequest
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/functions/stream" {
			t.Fatalf("procd path = %q, want /api/v1/functions/stream", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&execReq); err != nil {
			t.Fatalf("decode execute request: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("data: one\n\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		_, _ = w.Write([]byte("data: two\n\n"))
	}))
	defer procd.Close()

	port := serverPort(t, procd.URL)
	gateway := newSandboxServiceExposureTestServer(t, newFunctionServiceSandbox(procd.URL, port))
	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/events", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", resp.StatusCode, string(body))
	}
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", got)
	}
	if string(body) != "data: one\n\ndata: two\n\n" {
		t.Fatalf("body = %q, want streamed SSE body", string(body))
	}
	if execReq.Request.Path != "/events" {
		t.Fatalf("execute path = %q, want /events", execReq.Request.Path)
	}
}

func TestSandboxFunctionServiceProxiesWebSocketThroughProcdPort(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	initCh := make(chan sandboxfunction.ExecuteRequest, 1)
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/functions/ws" {
			t.Fatalf("procd path = %q, want /api/v1/functions/ws", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade procd websocket: %v", err)
		}
		defer conn.Close()
		var initReq sandboxfunction.ExecuteRequest
		if err := conn.ReadJSON(&initReq); err != nil {
			t.Fatalf("read init request: %v", err)
		}
		initCh <- initReq
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read proxied message: %v", err)
		}
		if err := conn.WriteMessage(messageType, []byte("echo:"+string(data))); err != nil {
			t.Fatalf("write proxied message: %v", err)
		}
	}))
	defer procd.Close()

	port := serverPort(t, procd.URL)
	gateway := newSandboxServiceExposureTestServer(t, newFunctionServiceSandbox(procd.URL, port))
	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()

	wsURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http") + "/events/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Host": {fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port)},
	})
	if err != nil {
		t.Fatalf("dial gateway websocket: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write gateway websocket: %v", err)
	}
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read gateway websocket: %v", err)
	}
	if string(data) != "echo:ping" {
		t.Fatalf("websocket data = %q, want echo:ping", string(data))
	}
	select {
	case initReq := <-initCh:
		if initReq.Request.Path != "/events/ws" || initReq.ServiceID != "webhook" {
			t.Fatalf("init request = %+v, want function service path", initReq)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for init request")
	}
}

func TestSandboxFunctionServiceExecutesAfterPausedAutoResume(t *testing.T) {
	var executeCalled atomic.Bool
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/functions/execute" {
			t.Fatalf("unexpected procd request %s %s", r.Method, r.URL.Path)
		}
		executeCalled.Store(true)
		_ = spec.WriteSuccess(w, http.StatusOK, sandboxfunction.ExecuteResponse{
			Status:     http.StatusCreated,
			BodyBase64: "cmVzdW1lZCBmdW5jdGlvbiBvaw==",
		})
	}))
	defer procd.Close()

	port := serverPort(t, procd.URL)
	activeSandbox := newFunctionServiceSandbox(procd.URL, port)
	activeSandbox.Status = mgr.SandboxStatusRunning
	activeSandbox.Services[0].Ingress.Routes[0].Resume = true
	managerURL, resumed := newPausedFunctionManager(t, activeSandbox)

	gateway := newSandboxServiceExposureTestServerWithManagerURL(t, managerURL)
	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodPost, gatewayServer.URL+"/events/resume", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d body=%s", resp.StatusCode, string(body))
	}
	if string(body) != "resumed function ok" {
		t.Fatalf("body = %q, want resumed function ok", string(body))
	}
	if !resumed.Load() {
		t.Fatal("manager resume was not called")
	}
	if !executeCalled.Load() {
		t.Fatal("function execute was not called")
	}
}

func TestSandboxFunctionServiceProxiesWebSocketAfterPausedAutoResume(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/functions/ws" {
			t.Fatalf("unexpected procd request %s %s", r.Method, r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade procd websocket: %v", err)
		}
		defer conn.Close()
		var initReq sandboxfunction.ExecuteRequest
		if err := conn.ReadJSON(&initReq); err != nil {
			t.Fatalf("read init request: %v", err)
		}
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read proxied message: %v", err)
		}
		if err := conn.WriteMessage(messageType, []byte("echo:"+string(data))); err != nil {
			t.Fatalf("write proxied message: %v", err)
		}
	}))
	defer procd.Close()

	port := serverPort(t, procd.URL)
	activeSandbox := newFunctionServiceSandbox(procd.URL, port)
	activeSandbox.Status = mgr.SandboxStatusRunning
	activeSandbox.Services[0].Ingress.Routes[0].Resume = true
	managerURL, resumed := newPausedFunctionManager(t, activeSandbox)

	gateway := newSandboxServiceExposureTestServerWithManagerURL(t, managerURL)
	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()

	wsURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http") + "/events/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Host": {fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port)},
	})
	if err != nil {
		t.Fatalf("dial gateway websocket: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("after-pause")); err != nil {
		t.Fatalf("write gateway websocket: %v", err)
	}
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read gateway websocket: %v", err)
	}
	if string(data) != "echo:after-pause" {
		t.Fatalf("websocket data = %q, want echo:after-pause", string(data))
	}
	if !resumed.Load() {
		t.Fatal("manager resume was not called")
	}
}

func TestSandboxCMDServiceStartsSessionBeforeProxy(t *testing.T) {
	var created atomic.Bool
	var createReq procdSessionSpec
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sessions":
			_ = spec.WriteSuccess(w, http.StatusOK, procdSessionListResponse{})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sessions":
			if err := json.NewDecoder(r.Body).Decode(&createReq); err != nil {
				t.Fatalf("decode create session request: %v", err)
			}
			if got := r.Header.Get("Idempotency-Key"); got != "sandbox-service-runtime:api" {
				t.Fatalf("Idempotency-Key = %q", got)
			}
			created.Store(true)
			_ = spec.WriteSuccess(w, http.StatusCreated, procdSessionResponse{
				ID: "ses-service", Spec: createReq, Phase: "running",
			})
		default:
			t.Fatalf("unexpected procd request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer procd.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !created.Load() {
			t.Fatalf("upstream hit before cmd session was created")
		}
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = w.Write([]byte("cmd ok"))
	}))
	defer upstream.Close()

	port := serverPort(t, upstream.URL)
	gateway := newSandboxServiceExposureTestServer(t, &mgr.Sandbox{
		ID:           "sb-demo",
		TeamID:       "team-1",
		UserID:       "user-1",
		InternalAddr: procd.URL,
		AutoResume:   true,
		Services: []mgr.SandboxAppService{{
			ID:   "api",
			Port: port,
			Runtime: &mgr.SandboxAppServiceRuntime{
				Type:    mgr.SandboxAppServiceRuntimeCMD,
				Command: []string{"python3", "-m", "http.server", strconv.Itoa(port)},
				CWD:     "/workspace",
				EnvVars: map[string]string{"APP_ENV": "test"},
			},
			HealthCheck: &mgr.SandboxAppServiceHealth{Path: "/healthz"},
			Ingress: mgr.SandboxAppServiceIngress{
				Public: true,
				Routes: []mgr.SandboxAppServiceRoute{{
					ID:             "root",
					PathPrefix:     "/",
					TimeoutSeconds: 2,
				}},
			},
		}},
	})
	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()

	req := newGatewayRequest(t, http.MethodGet, gatewayServer.URL, fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port), "/hello")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", resp.StatusCode, string(body))
	}
	if string(body) != "cmd ok" {
		t.Fatalf("body = %q, want cmd ok", string(body))
	}
	if createReq.Name != "sandbox-service:api" || len(createReq.Command) == 0 || createReq.IO.Mode != "pipes" {
		t.Fatalf("create session request = %+v, want supervised pipe command", createReq)
	}
	if createReq.Env[sandboxServiceRuntimeServiceIDEnv] != "api" || createReq.Env[sandboxServiceRuntimePortEnv] != strconv.Itoa(port) {
		t.Fatalf("service env vars = %#v, want service identity and port", createReq.Env)
	}
	if createReq.Env["APP_ENV"] != "test" {
		t.Fatalf("APP_ENV = %q, want test", createReq.Env["APP_ENV"])
	}
	if createReq.Lifecycle.Restart.Policy != "on_failure" || createReq.Lifecycle.RuntimeRecovery != "restart" {
		t.Fatalf("lifecycle = %#v, want restartable session", createReq.Lifecycle)
	}
}

func TestEnsureSandboxCMDServiceReconcilesExistingSession(t *testing.T) {
	service := &mgr.SandboxAppService{
		ID:   "api",
		Port: 8080,
		Runtime: &mgr.SandboxAppServiceRuntime{
			Type:    mgr.SandboxAppServiceRuntimeCMD,
			Command: []string{"server", "--port", "8080"},
			CWD:     "/workspace",
			EnvVars: map[string]string{"APP_ENV": "test"},
		},
	}
	desired := sandboxServiceCMDSessionSpec(service)

	tests := []struct {
		name          string
		candidate     procdSessionResponse
		wantPath      string
		assertRequest func(*testing.T, *http.Request)
	}{
		{
			name:      "restart exited matching session",
			candidate: procdSessionResponse{ID: "ses-service", Spec: desired, Phase: "exited"},
			wantPath:  "/api/v1/sessions/ses-service/desired-state",
			assertRequest: func(t *testing.T, r *http.Request) {
				var request map[string]string
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Fatal(err)
				}
				if request["state"] != "running" {
					t.Fatalf("desired state request = %#v", request)
				}
			},
		},
		{
			name: "replace drifted session spec",
			candidate: procdSessionResponse{
				ID: "ses-service",
				Spec: procdSessionSpec{
					Name:      desired.Name,
					Command:   []string{"old-server"},
					CWD:       desired.CWD,
					Env:       desired.Env,
					IO:        desired.IO,
					Lifecycle: desired.Lifecycle,
				},
				Phase: "running",
			},
			wantPath: "/api/v1/sessions/ses-service",
			assertRequest: func(t *testing.T, r *http.Request) {
				var request procdSessionSpec
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Fatal(err)
				}
				if !sandboxServiceSessionSpecMatches(request, desired) {
					t.Fatalf("updated spec = %#v, want %#v", request, desired)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reconciled atomic.Bool
			procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sessions":
					_ = spec.WriteSuccess(w, http.StatusOK, procdSessionListResponse{Sessions: []procdSessionResponse{tt.candidate}})
				case r.Method == http.MethodPut && r.URL.Path == tt.wantPath:
					tt.assertRequest(t, r)
					reconciled.Store(true)
					_ = spec.WriteSuccess(w, http.StatusOK, procdSessionResponse{ID: tt.candidate.ID, Spec: desired, Phase: "running"})
				default:
					t.Fatalf("unexpected procd request %s %s", r.Method, r.URL.Path)
				}
			}))
			defer procd.Close()

			_, privateKey, err := ed25519.GenerateKey(nil)
			if err != nil {
				t.Fatal(err)
			}
			server := &Server{
				logger: zap.NewNop(),
				internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
					Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute,
				}),
			}
			sandbox := &mgr.Sandbox{ID: "sb-demo", TeamID: "team-1", UserID: "user-1", InternalAddr: procd.URL}
			if err := server.ensureSandboxServiceCMDSession(t.Context(), sandbox, service); err != nil {
				t.Fatal(err)
			}
			if !reconciled.Load() {
				t.Fatal("existing session was not reconciled")
			}
		})
	}
}

func TestSandboxCMDServiceStartsAfterPausedAutoResume(t *testing.T) {
	var created atomic.Bool
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sessions":
			_ = spec.WriteSuccess(w, http.StatusOK, procdSessionListResponse{})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sessions":
			created.Store(true)
			_ = spec.WriteSuccess(w, http.StatusCreated, procdSessionResponse{ID: "ses-service", Phase: "running"})
		default:
			t.Fatalf("unexpected procd request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer procd.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !created.Load() {
			t.Fatalf("upstream hit before cmd session was created")
		}
		_, _ = w.Write([]byte("resumed cmd ok"))
	}))
	defer upstream.Close()

	port := serverPort(t, upstream.URL)
	activeSandbox := &mgr.Sandbox{
		ID:           "sb-demo",
		TeamID:       "team-1",
		UserID:       "user-1",
		InternalAddr: procd.URL,
		AutoResume:   true,
		Status:       mgr.SandboxStatusRunning,
		Services:     []mgr.SandboxAppService{newCMDServiceForTest(port, true)},
	}
	pausedSandbox := *activeSandbox
	pausedSandbox.InternalAddr = ""
	pausedSandbox.Status = mgr.SandboxStatusPaused
	pausedSandbox.Paused = true

	var resumed atomic.Bool
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/sandboxes/sb-demo":
			if resumed.Load() {
				_ = spec.WriteSuccess(w, http.StatusOK, activeSandbox)
				return
			}
			_ = spec.WriteSuccess(w, http.StatusOK, &pausedSandbox)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sandboxes/sb-demo/resume":
			resumed.Store(true)
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"sandbox_id": "sb-demo"})
		default:
			t.Fatalf("unexpected manager request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer manager.Close()

	gateway := newSandboxServiceExposureTestServerWithManagerURL(t, manager.URL)
	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()

	req := newGatewayRequest(t, http.MethodGet, gatewayServer.URL, fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port), "/")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", resp.StatusCode, string(body))
	}
	if string(body) != "resumed cmd ok" {
		t.Fatalf("body = %q, want resumed cmd ok", string(body))
	}
	if !resumed.Load() {
		t.Fatal("manager resume was not called")
	}
	if !created.Load() {
		t.Fatal("cmd session was not created after resume")
	}
}

func newFunctionServiceSandbox(internalAddr string, port int) *mgr.Sandbox {
	return &mgr.Sandbox{
		ID:           "sb-demo",
		TeamID:       "team-1",
		UserID:       "user-1",
		InternalAddr: internalAddr,
		AutoResume:   true,
		Services: []mgr.SandboxAppService{{
			ID:   "webhook",
			Port: port,
			Runtime: &mgr.SandboxAppServiceRuntime{
				Type: mgr.SandboxAppServiceRuntimeFunction,
				Function: &mgr.SandboxFunction{
					Runtime: "python",
					Handler: "handler",
					Source: mgr.SandboxFunctionSource{
						Type: "inline",
						Code: "def handler(request):\n    return {'status': 201}\n",
					},
				},
			},
			Ingress: mgr.SandboxAppServiceIngress{
				Public: true,
				Routes: []mgr.SandboxAppServiceRoute{{
					ID:         "root",
					PathPrefix: "/events",
				}},
			},
		}},
	}
}

func newPausedFunctionManager(t *testing.T, activeSandbox *mgr.Sandbox) (string, *atomic.Bool) {
	t.Helper()
	pausedSandbox := *activeSandbox
	pausedSandbox.InternalAddr = ""
	pausedSandbox.Status = mgr.SandboxStatusPaused
	pausedSandbox.Paused = true

	var resumed atomic.Bool
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/sandboxes/sb-demo":
			if resumed.Load() {
				_ = spec.WriteSuccess(w, http.StatusOK, activeSandbox)
				return
			}
			_ = spec.WriteSuccess(w, http.StatusOK, &pausedSandbox)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sandboxes/sb-demo/resume":
			resumed.Store(true)
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"sandbox_id": "sb-demo"})
		default:
			t.Fatalf("unexpected manager request %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(manager.Close)
	return manager.URL, &resumed
}

func newCMDServiceForTest(port int, resume bool) mgr.SandboxAppService {
	return mgr.SandboxAppService{
		ID:   "api",
		Port: port,
		Runtime: &mgr.SandboxAppServiceRuntime{
			Type:    mgr.SandboxAppServiceRuntimeCMD,
			Command: []string{"python3", "-m", "http.server", strconv.Itoa(port)},
		},
		Ingress: mgr.SandboxAppServiceIngress{
			Public: true,
			Routes: []mgr.SandboxAppServiceRoute{{
				ID:             "root",
				PathPrefix:     "/",
				TimeoutSeconds: 2,
				Resume:         resume,
			}},
		},
	}
}

func newSandboxServiceExposureTestServer(t *testing.T, sandbox *mgr.Sandbox) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)

	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/sandboxes/sb-demo" {
			spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
			return
		}
		_ = spec.WriteSuccess(w, http.StatusOK, sandbox)
	}))
	t.Cleanup(manager.Close)

	return newSandboxServiceExposureTestServerWithManagerURL(t, manager.URL)
}

func newSandboxServiceExposureTestServerWithManagerURL(t *testing.T, managerURL string) http.Handler {
	return newSandboxServiceExposureTestServerWithManagerURLAndQuota(
		t,
		managerURL,
		newAllowingTeamQuotaController(zap.NewNop()),
	)
}

func newSandboxServiceExposureTestServerWithManagerURLAndQuota(
	t *testing.T,
	managerURL string,
	teamQuotaController *gatewayteamquota.Controller,
) http.Handler {
	return newSandboxServiceExposureTestServerWithManagerURLQuotaAndConfigure(
		t,
		managerURL,
		teamQuotaController,
		nil,
	)
}

func newSandboxServiceExposureTestServerWithManagerURLQuotaAndConfigure(
	t *testing.T,
	managerURL string,
	teamQuotaController *gatewayteamquota.Controller,
	configure func(*Server),
) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)

	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	gen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	abuseGuard := ratelimit.NewMemoryLimiter(ratelimit.MemoryConfig{})
	t.Cleanup(func() { _ = abuseGuard.Close() })

	s := &Server{
		cfg: &config.ClusterGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				PublicExposureEnabled: true,
				PublicRootDomain:      "sandbox0.app",
				PublicRegionID:        "aws-us-east-1",
			},
			ProxyTimeout: metav1.Duration{Duration: 10 * time.Second},
		},
		logger:                   zap.NewNop(),
		managerClient:            client.NewManagerClient(managerURL, gen, zap.NewNop(), time.Second),
		internalAuthGen:          gen,
		teamQuotaController:      teamQuotaController,
		sandboxServiceAbuseGuard: abuseGuard,
	}
	if configure != nil {
		configure(s)
	}
	s.router = gin.New()
	s.setupPublicExposureNoRoute()
	return s.router
}

func serverPort(t *testing.T, rawURL string) int {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	_, portString, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return port
}

func newGatewayRequest(t *testing.T, method, baseURL, host, path string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, baseURL+path, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = host
	return req
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:])
}
