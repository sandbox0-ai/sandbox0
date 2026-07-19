package teamquota

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

func TestForwardedAPIAdmissionProofIsExactAndPerKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name     string
		mutate   func(*authn.AuthContext, *internalauth.Claims)
		spoof    bool
		wantSkip bool
	}{
		{
			name:     "regional exact proof",
			wantSkip: true,
		},
		{
			name: "scheduler exact re-signed proof",
			mutate: func(authCtx *authn.AuthContext, claims *internalauth.Claims) {
				authCtx.Caller = internalauth.ServiceScheduler
				claims.Caller = internalauth.ServiceScheduler
			},
			wantSkip: true,
		},
		{
			name: "missing proof",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.QuotaAdmissionProof = nil
			},
		},
		{
			name: "team mismatch",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.QuotaAdmissionProof.TeamID = "team-b"
			},
		},
		{
			name: "method mismatch",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.QuotaAdmissionProof.Method = http.MethodPost
			},
		},
		{
			name: "path mismatch",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.QuotaAdmissionProof.Path = "/api/v1/other"
			},
		},
		{
			name: "operation ID mismatch",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.QuotaAdmissionProof.OperationID = "operation-b"
			},
		},
		{
			name: "request ID mismatch",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.QuotaAdmissionProof.RequestID = "request-b"
			},
		},
		{
			name: "key mismatch",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.QuotaAdmissionProof.Keys = []coreteamquota.Key{
					coreteamquota.KeyNetworkIngressBytes,
				}
			},
		},
		{
			name: "caller mismatch",
			mutate: func(authCtx *authn.AuthContext, claims *internalauth.Claims) {
				authCtx.Caller = internalauth.ServiceManager
				claims.Caller = internalauth.ServiceManager
			},
		},
		{
			name: "auth context caller differs from signed caller",
			mutate: func(authCtx *authn.AuthContext, _ *internalauth.Claims) {
				authCtx.Caller = internalauth.ServiceScheduler
			},
		},
		{
			name: "origin mismatch",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.Audit.Origin = internalauth.ServiceScheduler
				claims.QuotaAdmissionProof.Origin = internalauth.ServiceScheduler
			},
		},
		{
			name: "edge class cannot use system claim",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.IsSystem = true
			},
		},
		{
			name: "nonallowlisted system lane",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.IsSystem = true
				claims.QuotaAdmissionProof.Class = internalauth.QuotaAdmissionClassSystem
			},
		},
		{
			name: "unknown key is untrusted",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.QuotaAdmissionProof.Keys = []coreteamquota.Key{"unknown"}
			},
		},
		{
			name: "public proof header cannot inject",
			mutate: func(_ *authn.AuthContext, claims *internalauth.Claims) {
				claims.QuotaAdmissionProof = nil
			},
			spoof: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requestTemplate := httptest.NewRequest(
				http.MethodGet,
				"/api/v1/sandboxes?limit=200",
				nil,
			)
			proof, err := internalauth.NewQuotaAdmissionProof(
				internalauth.QuotaAdmissionClassEdgeAdmitted,
				requestTemplate,
				"team-a",
				"operation-a",
				"request-a",
				internalauth.ServiceRegionalGateway,
				[]coreteamquota.Key{coreteamquota.KeyAPIRequests},
				guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
			)
			if err != nil {
				t.Fatalf("NewQuotaAdmissionProof() error = %v", err)
			}
			authCtx := &authn.AuthContext{
				AuthMethod:  authn.AuthMethodInternal,
				TeamID:      "team-a",
				Caller:      internalauth.ServiceRegionalGateway,
				OperationID: "operation-a",
				RequestID:   "request-a",
			}
			claims := &internalauth.Claims{
				Caller: internalauth.ServiceRegionalGateway,
				TeamID: "team-a",
				Audit: &internalauth.AuditContext{
					OperationID: "operation-a",
					RequestID:   "request-a",
					Origin:      internalauth.ServiceRegionalGateway,
				},
				QuotaAdmissionProof: proof,
			}
			if test.mutate != nil {
				test.mutate(authCtx, claims)
			}
			limiter := &fakeRateLimiter{
				decision: tokenbucket.Decision{Allowed: true},
			}
			if test.wantSkip {
				limiter.err = errors.New("must not charge a proven key")
			}
			controller := NewController(nil, nil, limiter, nil, nil)
			controller.proofConsumer = &fakeAdmissionProofConsumer{
				trusted: true,
			}
			router := gin.New()
			router.Use(internalQuotaClaimsMiddleware(authCtx, claims))
			router.Use(controller.ConsumeForwardedAdmissionProof())
			router.Use(controller.RateLimitAPIRequests(true))
			router.GET(
				"/api/v1/sandboxes",
				func(c *gin.Context) { c.Status(http.StatusNoContent) },
			)

			request := httptest.NewRequest(
				http.MethodGet,
				"/api/v1/sandboxes?limit=200",
				nil,
			)
			if test.spoof {
				request.Header.Set(
					"X-Quota-Admission-Proof",
					`{"keys":["api_requests"]}`,
				)
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			if response.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204; body=%s", response.Code, response.Body.String())
			}
			wantCalls := 1
			if test.wantSkip {
				wantCalls = 0
			}
			if limiter.calls != wantCalls {
				t.Fatalf("limiter calls = %d, want %d", limiter.calls, wantCalls)
			}
		})
	}
}

func TestForwardedNetworkAdmissionProofSkipsOnlyListedDirection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	network := &trafficNetworkLimiter{}
	controller := newTrafficTestController(nil, network)
	controller.proofConsumer = &fakeAdmissionProofConsumer{trusted: true}
	router := gin.New()
	router.Use(forwardedQuotaProofMiddleware(
		t,
		http.MethodPost,
		"/api/v1/files",
		internalauth.ServiceRegionalGateway,
		[]coreteamquota.Key{coreteamquota.KeyNetworkIngressBytes},
	))
	router.Use(controller.ConsumeForwardedAdmissionProof())
	router.Use(controller.LimitNetworkTraffic(true))
	router.POST("/api/v1/files", func(c *gin.Context) {
		payload, err := io.ReadAll(c.Request.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		_, _ = c.Writer.Write(append([]byte("reply:"), payload...))
	})

	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodPost,
			"/api/v1/files",
			strings.NewReader("payload"),
		),
	)

	if got := network.total(coreteamquota.KeyNetworkIngressBytes); got != 0 {
		t.Fatalf("ingress bytes = %d, want 0", got)
	}
	if got := network.total(coreteamquota.KeyNetworkEgressBytes); got != int64(len("reply:payload")) {
		t.Fatalf("egress bytes = %d, want %d", got, len("reply:payload"))
	}
}

func TestForwardedAdmissionProofFirstUseSkipsAndReplayIsCharged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/sandboxes?limit=10",
		nil,
	)
	authCtx, claims := testForwardedProofIdentity(
		t,
		request,
		[]coreteamquota.Key{coreteamquota.KeyAPIRequests},
	)
	limiter := &fakeRateLimiter{
		decision: tokenbucket.Decision{Allowed: true},
	}
	consumer := &fakeAdmissionProofConsumer{
		results: []bool{true, false},
	}
	controller := NewController(
		nil,
		nil,
		limiter,
		nil,
		nil,
		WithAdmissionProofConsumer(consumer),
	)
	router := gin.New()
	router.Use(internalQuotaClaimsMiddleware(authCtx, claims))
	router.Use(controller.ConsumeForwardedAdmissionProof())
	router.Use(controller.RateLimitAPIRequests(true))
	router.GET(
		"/api/v1/sandboxes",
		func(c *gin.Context) { c.Status(http.StatusNoContent) },
	)

	for attempt := 0; attempt < 2; attempt++ {
		response := httptest.NewRecorder()
		router.ServeHTTP(
			response,
			httptest.NewRequest(
				http.MethodGet,
				"/api/v1/sandboxes?limit=10",
				nil,
			),
		)
		if response.Code != http.StatusNoContent {
			t.Fatalf(
				"attempt %d status = %d, want 204; body=%s",
				attempt,
				response.Code,
				response.Body.String(),
			)
		}
	}
	if consumer.calls != 2 {
		t.Fatalf("proof consumer calls = %d, want 2", consumer.calls)
	}
	if limiter.calls != 1 {
		t.Fatalf("rate limiter calls = %d, want replay charged once", limiter.calls)
	}
}

func TestForwardedAdmissionProofConsumerErrorFailsClosed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil)
	authCtx, claims := testForwardedProofIdentity(
		t,
		request,
		[]coreteamquota.Key{coreteamquota.KeyAPIRequests},
	)
	limiter := &fakeRateLimiter{
		decision: tokenbucket.Decision{Allowed: true},
	}
	controller := NewController(
		nil,
		nil,
		limiter,
		nil,
		nil,
		WithAdmissionProofConsumer(&fakeAdmissionProofConsumer{
			err: errors.New("redis unavailable"),
		}),
	)
	router := gin.New()
	router.Use(internalQuotaClaimsMiddleware(authCtx, claims))
	router.Use(controller.ConsumeForwardedAdmissionProof())
	router.Use(controller.RateLimitAPIRequests(true))
	router.GET(
		"/api/v1/sandboxes",
		func(c *gin.Context) { c.Status(http.StatusNoContent) },
	)

	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil),
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"status = %d, want 503; body=%s",
			response.Code,
			response.Body.String(),
		)
	}
	if response.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", response.Header().Get("Retry-After"))
	}
	if limiter.calls != 0 {
		t.Fatalf("rate limiter calls = %d, want request stopped before quota middleware", limiter.calls)
	}
}

func TestForwardedAdmissionProofTransportMismatchIsCharged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name      string
		proofReq  *http.Request
		actualReq func() *http.Request
	}{
		{
			name: "query",
			proofReq: httptest.NewRequest(
				http.MethodGet,
				"/api/v1/sandboxes?limit=10",
				nil,
			),
			actualReq: func() *http.Request {
				return httptest.NewRequest(
					http.MethodGet,
					"/api/v1/sandboxes?limit=20",
					nil,
				)
			},
		},
		{
			name: "replayable body",
			proofReq: func() *http.Request {
				request := httptest.NewRequest(
					http.MethodPost,
					"/api/v1/sandboxes",
					bytes.NewReader([]byte("alpha")),
				)
				request.GetBody = func() (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader([]byte("alpha"))), nil
				}
				request.Header.Set("Content-Type", "application/json")
				return request
			}(),
			actualReq: func() *http.Request {
				request := httptest.NewRequest(
					http.MethodPost,
					"/api/v1/sandboxes",
					bytes.NewReader([]byte("bravo")),
				)
				request.GetBody = func() (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader([]byte("bravo"))), nil
				}
				request.Header.Set("Content-Type", "application/json")
				return request
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authCtx, claims := testForwardedProofIdentity(
				t,
				test.proofReq,
				[]coreteamquota.Key{coreteamquota.KeyAPIRequests},
			)
			limiter := &fakeRateLimiter{
				decision: tokenbucket.Decision{Allowed: true},
			}
			consumer := &fakeAdmissionProofConsumer{trusted: true}
			controller := NewController(
				nil,
				nil,
				limiter,
				nil,
				nil,
				WithAdmissionProofConsumer(consumer),
			)
			router := gin.New()
			router.Use(internalQuotaClaimsMiddleware(authCtx, claims))
			router.Use(controller.ConsumeForwardedAdmissionProof())
			router.Use(controller.RateLimitAPIRequests(true))
			router.Any(
				"/api/v1/sandboxes",
				func(c *gin.Context) { c.Status(http.StatusNoContent) },
			)

			response := httptest.NewRecorder()
			router.ServeHTTP(response, test.actualReq())
			if response.Code != http.StatusNoContent {
				t.Fatalf(
					"status = %d, want 204; body=%s",
					response.Code,
					response.Body.String(),
				)
			}
			if consumer.calls != 0 {
				t.Fatalf("proof consumer calls = %d, want mismatch rejected before consume", consumer.calls)
			}
			if limiter.calls != 1 {
				t.Fatalf("rate limiter calls = %d, want mismatch charged", limiter.calls)
			}
		})
	}
}

func TestAdmittedKeysContainOnlyActuallyOwnedKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controller := newTrafficTestController(nil, &trafficNetworkLimiter{})
	var got []coreteamquota.Key
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodAPIKey))
	router.Use(controller.AdmitEdgeTraffic(false))
	router.Use(controller.RateLimitAPIRequests(false))
	router.GET("/api/v1/sandboxes", func(c *gin.Context) {
		got = AdmittedKeys(c.Request.Context())
		c.Status(http.StatusNoContent)
	})

	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil),
	)
	want := []coreteamquota.Key{
		coreteamquota.KeyAPIRequests,
		coreteamquota.KeyNetworkEgressBytes,
		coreteamquota.KeyNetworkIngressBytes,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("admitted keys = %v, want %v", got, want)
	}
}

func forwardedQuotaProofMiddleware(
	t *testing.T,
	method string,
	path string,
	caller string,
	keys []coreteamquota.Key,
) gin.HandlerFunc {
	t.Helper()
	return func(c *gin.Context) {
		if c.Request.Method != method ||
			internalauth.CanonicalRequestPath(c.Request) != path {
			t.Fatalf(
				"forwarded request = %s %s, want %s %s",
				c.Request.Method,
				internalauth.CanonicalRequestPath(c.Request),
				method,
				path,
			)
		}
		authCtx, claims := testForwardedProofIdentity(t, c.Request, keys)
		authCtx.Caller = caller
		claims.Caller = caller
		c.Set("auth_context", authCtx)
		ctx := authn.WithAuthContext(c.Request.Context(), authCtx)
		ctx = internalauth.WithClaims(ctx, claims)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

func testForwardedProofIdentity(
	t *testing.T,
	request *http.Request,
	keys []coreteamquota.Key,
) (*authn.AuthContext, *internalauth.Claims) {
	t.Helper()
	proof, err := internalauth.NewQuotaAdmissionProof(
		internalauth.QuotaAdmissionClassEdgeAdmitted,
		request,
		"team-a",
		"operation-a",
		"request-a",
		internalauth.ServiceRegionalGateway,
		keys,
		guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
	)
	if err != nil {
		t.Fatalf("NewQuotaAdmissionProof() error = %v", err)
	}
	authCtx := &authn.AuthContext{
		AuthMethod:  authn.AuthMethodInternal,
		TeamID:      "team-a",
		Caller:      internalauth.ServiceRegionalGateway,
		OperationID: "operation-a",
		RequestID:   "request-a",
	}
	claims := &internalauth.Claims{
		Caller: internalauth.ServiceRegionalGateway,
		TeamID: "team-a",
		Audit: &internalauth.AuditContext{
			OperationID: "operation-a",
			RequestID:   "request-a",
			Origin:      internalauth.ServiceRegionalGateway,
		},
		QuotaAdmissionProof: proof,
	}
	return authCtx, claims
}

type fakeAdmissionProofConsumer struct {
	trusted bool
	results []bool
	err     error
	calls   int
}

func (*fakeAdmissionProofConsumer) CurrentVersion(
	context.Context,
) (guard.Version, error) {
	return guard.Version{EnforcementEpoch: 1, RedisGeneration: 1}, nil
}

func (c *fakeAdmissionProofConsumer) Consume(
	context.Context,
	string,
	string,
	int64,
	int64,
	guard.Version,
) (bool, error) {
	c.calls++
	if c.err != nil {
		return false, c.err
	}
	if len(c.results) > 0 {
		result := c.results[0]
		c.results = c.results[1:]
		return result, nil
	}
	return c.trusted, nil
}

func (*fakeAdmissionProofConsumer) Close() error { return nil }

func internalQuotaClaimsMiddleware(
	authCtx *authn.AuthContext,
	claims *internalauth.Claims,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("auth_context", authCtx)
		ctx := authn.WithAuthContext(c.Request.Context(), authCtx)
		ctx = internalauth.WithClaims(ctx, claims)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
