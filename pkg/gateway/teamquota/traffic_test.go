package teamquota

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

func TestLongLivedAdmissionSkipsTrustedInternalForward(t *testing.T) {
	limiter := &trafficConcurrencyLimiter{lease: newTrafficLease()}
	controller := newTrafficTestController(limiter, &trafficNetworkLimiter{})
	controller.proofConsumer = &fakeAdmissionProofConsumer{trusted: true}
	router := gin.New()
	router.Use(forwardedQuotaProofMiddleware(
		t,
		http.MethodGet,
		"/stream",
		internalauth.ServiceRegionalGateway,
		[]coreteamquota.Key{coreteamquota.KeyActiveConnectionCount},
	))
	router.Use(controller.ConsumeForwardedAdmissionProof())
	router.Use(controller.AdmitLongLivedConnections(true))
	router.GET("/stream", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	request := httptest.NewRequest(http.MethodGet, "/stream", nil)
	request = request.WithContext(proxy.WithLongLivedRequest(request.Context()))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
	if limiter.acquireCallCount() != 0 {
		t.Fatalf("Acquire() calls = %d, want 0", limiter.acquireCallCount())
	}
}

func TestLongLivedAdmissionAcquiresAndReleasesExternalLease(t *testing.T) {
	lease := newTrafficLease()
	limiter := &trafficConcurrencyLimiter{lease: lease}
	controller := newTrafficTestController(limiter, &trafficNetworkLimiter{})
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodAPIKey))
	router.Use(controller.AdmitLongLivedConnections(true))
	router.GET("/stream", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	request := httptest.NewRequest(http.MethodGet, "/stream", nil)
	request = request.WithContext(proxy.WithLongLivedRequest(request.Context()))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
	if limiter.acquireCallCount() != 1 || lease.releaseCallCount() != 1 {
		t.Fatalf(
			"calls = acquire %d release %d, want 1 each",
			limiter.acquireCallCount(),
			lease.releaseCallCount(),
		)
	}
}

func TestLongLivedAdmissionCancelsHandlerWhenLeaseIsLost(t *testing.T) {
	lease := newTrafficLease()
	limiter := &trafficConcurrencyLimiter{lease: lease}
	controller := newTrafficTestController(limiter, &trafficNetworkLimiter{})
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodAPIKey))
	router.Use(controller.AdmitLongLivedConnections(false))
	handlerStarted := make(chan struct{})
	router.GET("/stream", func(c *gin.Context) {
		close(handlerStarted)
		<-c.Request.Context().Done()
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/stream", nil)
	request = request.WithContext(proxy.WithLongLivedRequest(request.Context()))
	response := httptest.NewRecorder()
	requestDone := make(chan struct{})
	go func() {
		router.ServeHTTP(response, request)
		close(requestDone)
	}()
	<-handlerStarted
	lease.fail(errors.New("lease renewal failed"))

	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("handler context was not canceled after lease loss")
	}
	if cause := context.Cause(request.Context()); cause != nil {
		t.Fatalf("original request context cause = %v, want unchanged", cause)
	}
}

func TestAttachEdgeTrafficLeaseLossClosesHijackedConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	baseWriter := &hijackableHTTPWriter{
		header: make(http.Header),
		conn:   serverConn,
	}
	ginCtx, _ := gin.CreateTestContext(baseWriter)
	request := httptest.NewRequest(http.MethodGet, "/ws", nil)
	ginCtx.Request = request.WithContext(proxy.WithLongLivedRequest(request.Context()))
	ginCtx.Set("auth_context", &authn.AuthContext{
		TeamID:     "team-a",
		AuthMethod: authn.AuthMethodAPIKey,
	})

	lease := newTrafficLease()
	controller := newTrafficTestController(
		&trafficConcurrencyLimiter{lease: lease},
		&trafficNetworkLimiter{},
	)
	cleanup, ok := controller.AttachEdgeTraffic(ginCtx, false)
	if !ok {
		t.Fatal("AttachEdgeTraffic() rejected test request")
	}
	defer cleanup()

	hijacked, _, err := ginCtx.Writer.Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	defer hijacked.Close()

	peerRead := make(chan error, 1)
	go func() {
		_, err := clientConn.Read(make([]byte, 1))
		peerRead <- err
	}()
	lease.fail(errors.New("lease renewal failed"))

	select {
	case err := <-peerRead:
		if err == nil {
			t.Fatal("hijacked peer read succeeded after lease loss, want connection close")
		}
	case <-time.After(time.Second):
		t.Fatal("hijacked connection remained open after lease loss")
	}
}

func TestAttachEdgeTrafficRequestCancellationClosesHijackedConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	baseWriter := &hijackableHTTPWriter{
		header: make(http.Header),
		conn:   serverConn,
	}
	ginCtx, _ := gin.CreateTestContext(baseWriter)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/ws", nil).WithContext(requestCtx)
	ginCtx.Request = request.WithContext(proxy.WithLongLivedRequest(request.Context()))
	ginCtx.Set("auth_context", &authn.AuthContext{
		TeamID:     "team-a",
		AuthMethod: authn.AuthMethodAPIKey,
	})

	controller := newTrafficTestController(
		&trafficConcurrencyLimiter{lease: newTrafficLease()},
		&trafficNetworkLimiter{},
	)
	cleanup, ok := controller.AttachEdgeTraffic(ginCtx, false)
	if !ok {
		t.Fatal("AttachEdgeTraffic() rejected test request")
	}
	defer cleanup()

	hijacked, _, err := ginCtx.Writer.Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	defer hijacked.Close()

	peerRead := make(chan error, 1)
	go func() {
		_, err := clientConn.Read(make([]byte, 1))
		peerRead <- err
	}()
	cancelRequest()

	select {
	case err := <-peerRead:
		if err == nil {
			t.Fatal("hijacked peer read succeeded after request cancellation, want connection close")
		}
	case <-time.After(time.Second):
		t.Fatal("hijacked connection remained open after request cancellation")
	}
}

func TestPublicExposureAdmissionCountsRateConcurrencyAndDirectionalBytes(t *testing.T) {
	lease := newTrafficLease()
	concurrency := &trafficConcurrencyLimiter{lease: lease}
	network := &trafficNetworkLimiter{}
	rate := &fakeRateLimiter{
		decision: tokenbucket.Decision{Allowed: true, Remaining: 10},
	}
	controller := NewController(
		nil,
		nil,
		rate,
		nil,
		nil,
		WithConcurrencyLimiter(concurrency),
		WithNetworkLimiter(network),
	)
	router := gin.New()
	router.POST("/invoke", func(c *gin.Context) {
		cleanup, ok := controller.AdmitPublicExposure(c, "team-a")
		if !ok {
			return
		}
		defer cleanup()
		payload, err := io.ReadAll(c.Request.Body)
		if err != nil {
			t.Errorf("ReadAll(body) error = %v", err)
			return
		}
		_, _ = c.Writer.Write(append([]byte("reply:"), payload...))
	})

	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodPost, "/invoke", bytes.NewBufferString("hello")),
	)

	if response.Code != http.StatusOK || response.Body.String() != "reply:hello" {
		t.Fatalf("response = %d %q, want 200 reply:hello", response.Code, response.Body.String())
	}
	if rate.calls != 1 || rate.lastKey != coreteamquota.KeySandboxServiceRequests {
		t.Fatalf("rate calls/key = %d/%q, want one sandbox_service_requests", rate.calls, rate.lastKey)
	}
	if concurrency.acquireCallCount() != 1 || lease.releaseCallCount() != 1 {
		t.Fatalf(
			"concurrency calls = acquire %d release %d, want 1 each",
			concurrency.acquireCallCount(),
			lease.releaseCallCount(),
		)
	}
	if got := network.total(coreteamquota.KeyNetworkIngressBytes); got != int64(len("hello")) {
		t.Fatalf("ingress bytes = %d, want %d", got, len("hello"))
	}
	if got := network.total(coreteamquota.KeyNetworkEgressBytes); got != int64(len("reply:hello")) {
		t.Fatalf("egress bytes = %d, want %d", got, len("reply:hello"))
	}
}

func TestPublicExposureAdmissionKeepsLeaseWhileAccountingRateRejection(t *testing.T) {
	tests := []struct {
		name         string
		rateDecision tokenbucket.Decision
		rateErr      error
		wantStatus   int
	}{
		{
			name:         "rate denied",
			rateDecision: tokenbucket.Decision{Allowed: false, RetryAfter: time.Second},
			wantStatus:   http.StatusTooManyRequests,
		},
		{
			name:         "rate backend unavailable",
			rateDecision: tokenbucket.Decision{Allowed: true},
			rateErr:      errors.New("redis unavailable"),
			wantStatus:   http.StatusServiceUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rate := &fakeRateLimiter{decision: tt.rateDecision, err: tt.rateErr}
			lease := newTrafficLease()
			concurrency := &trafficConcurrencyLimiter{lease: lease}
			waitsAfterRelease := 0
			network := &trafficNetworkLimiter{
				onWait: func(coreteamquota.Key, int) {
					if lease.releaseCallCount() != 0 {
						waitsAfterRelease++
					}
				},
			}
			controller := NewController(
				nil,
				nil,
				rate,
				nil,
				nil,
				WithConcurrencyLimiter(concurrency),
				WithNetworkLimiter(network),
			)
			router := gin.New()
			router.POST("/invoke", func(c *gin.Context) {
				cleanup, ok := controller.AdmitPublicExposure(c, "team-a")
				if ok {
					cleanup()
					t.Error("AdmitPublicExposure() allowed rejected request")
				}
			})

			const requestBody = "rejected-request"
			response := httptest.NewRecorder()
			router.ServeHTTP(
				response,
				httptest.NewRequest(
					http.MethodPost,
					"/invoke",
					bytes.NewBufferString(requestBody),
				),
			)
			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, tt.wantStatus, response.Body.String())
			}
			if response.Body.Len() == 0 {
				t.Fatal("rejection response body is empty")
			}
			if got := network.total(coreteamquota.KeyNetworkIngressBytes); got != int64(len(requestBody)) {
				t.Fatalf("ingress bytes = %d, want drained request body size %d", got, len(requestBody))
			}
			if got := network.total(coreteamquota.KeyNetworkEgressBytes); got != int64(response.Body.Len()) {
				t.Fatalf("egress bytes = %d, want rejection response size %d", got, response.Body.Len())
			}
			if got := concurrency.acquireCallCount(); got != 1 {
				t.Fatalf("concurrency acquire calls = %d, want 1", got)
			}
			if got := lease.releaseCallCount(); got != 1 {
				t.Fatalf("concurrency release calls = %d, want 1", got)
			}
			if waitsAfterRelease != 0 {
				t.Fatalf("network accounting calls after lease release = %d, want 0", waitsAfterRelease)
			}
			if rate.calls != 1 || rate.lastKey != coreteamquota.KeySandboxServiceRequests {
				t.Fatalf("rate calls/key = %d/%q, want one sandbox_service_requests", rate.calls, rate.lastKey)
			}
		})
	}
}

func TestPublicExposureConcurrencyRejectionDoesNotReadRequestBody(t *testing.T) {
	tests := []struct {
		name           string
		concurrencyErr error
		wantStatus     int
	}{
		{
			name: "concurrency denied",
			concurrencyErr: &coreteamquota.ConcurrencyExceededError{
				TeamID: "team-a",
				Key:    coreteamquota.KeyActiveConnectionCount,
				Limit:  1,
				Used:   1,
			},
			wantStatus: http.StatusTooManyRequests,
		},
		{
			name:           "concurrency backend unavailable",
			concurrencyErr: errors.New("redis unavailable"),
			wantStatus:     http.StatusServiceUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rate := &fakeRateLimiter{decision: tokenbucket.Decision{Allowed: true}}
			concurrency := &trafficConcurrencyLimiter{err: tt.concurrencyErr}
			network := &trafficNetworkLimiter{}
			controller := NewController(
				nil,
				nil,
				rate,
				nil,
				nil,
				WithConcurrencyLimiter(concurrency),
				WithNetworkLimiter(network),
			)
			router := gin.New()
			router.POST("/invoke", func(c *gin.Context) {
				cleanup, ok := controller.AdmitPublicExposure(c, "team-a")
				if ok {
					cleanup()
					t.Error("AdmitPublicExposure() allowed rejected request")
				}
			})

			const requestPayload = "unread-request"
			requestBody := bytes.NewBufferString(requestPayload)
			response := httptest.NewRecorder()
			router.ServeHTTP(
				response,
				httptest.NewRequest(http.MethodPost, "/invoke", requestBody),
			)
			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, tt.wantStatus, response.Body.String())
			}
			if requestBody.Len() != len(requestPayload) {
				t.Fatalf(
					"unread request bytes = %d, want %d after fast concurrency rejection",
					requestBody.Len(),
					len(requestPayload),
				)
			}
			if got := concurrency.acquireCallCount(); got != 1 {
				t.Fatalf("concurrency acquire calls = %d, want 1", got)
			}
			if rate.calls != 0 {
				t.Fatalf("sandbox_service_requests calls = %d, want 0 before concurrency admission", rate.calls)
			}
			if got := network.total(coreteamquota.KeyNetworkIngressBytes); got != 0 {
				t.Fatalf("ingress bytes = %d, want 0 before concurrency admission", got)
			}
			if got := network.total(coreteamquota.KeyNetworkEgressBytes); got != 0 {
				t.Fatalf("egress bytes = %d, want 0 before concurrency admission", got)
			}
		})
	}
}

func TestPublicExposureAdmissionReleasesLeaseWhenNetworkCleanupPanics(t *testing.T) {
	lease := newTrafficLease()
	concurrency := &trafficConcurrencyLimiter{lease: lease}
	network := &trafficNetworkLimiter{}
	controller := NewController(
		nil,
		nil,
		&fakeRateLimiter{decision: tokenbucket.Decision{Allowed: true}},
		nil,
		nil,
		WithConcurrencyLimiter(concurrency),
		WithNetworkLimiter(network),
	)
	router := gin.New()
	router.POST("/invoke", func(c *gin.Context) {
		cleanup, ok := controller.AdmitPublicExposure(c, "team-a")
		if !ok {
			t.Error("AdmitPublicExposure() rejected test request")
			return
		}
		cleanup()
	})

	const requestBody = "cleanup-panic"
	request := httptest.NewRequest(
		http.MethodPost,
		"/invoke",
		&panicCloseTrafficBody{Reader: bytes.NewReader([]byte(requestBody))},
	)
	response := httptest.NewRecorder()
	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		router.ServeHTTP(response, request)
	}()

	if recovered == nil {
		t.Fatal("network cleanup did not propagate body close panic")
	}
	if got := lease.releaseCallCount(); got != 1 {
		t.Fatalf("concurrency release calls = %d, want 1 after cleanup panic", got)
	}
	if got := network.total(coreteamquota.KeyNetworkIngressBytes); got != int64(len(requestBody)) {
		t.Fatalf("ingress bytes = %d, want %d before cleanup panic", got, len(requestBody))
	}
}

func TestEdgeTrafficActiveOwnTeamDeletionUsesNormalQuota(t *testing.T) {
	network := &trafficNetworkLimiter{}
	rate := &fakeRateLimiter{
		decision: tokenbucket.Decision{Allowed: true, Remaining: 9},
	}
	bucket := &fakeTokenBucket{
		decision: tokenbucket.Decision{Allowed: true},
	}
	controller := NewController(
		nil,
		nil,
		rate,
		bucket,
		nil,
		WithRegionID("region-a"),
		WithNetworkLimiter(network),
	)
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodJWT))
	router.Use(controller.AdmitEdgeTraffic(false))
	router.Use(controller.RateLimitAPIRequests(false))
	router.DELETE("/teams/:id", func(c *gin.Context) {
		payload, err := io.ReadAll(c.Request.Body)
		if err != nil {
			t.Errorf("ReadAll(body) error = %v", err)
			return
		}
		c.String(http.StatusOK, "deleted:"+string(payload))
	})

	request := httptest.NewRequest(
		http.MethodDelete,
		"/teams/team-a",
		bytes.NewBufferString("retry"),
	)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK || response.Body.String() != "deleted:retry" {
		t.Fatalf("response = %d %q, want 200 deleted:retry", response.Code, response.Body.String())
	}
	if rate.calls != 1 {
		t.Fatalf("normal api_requests calls = %d, want 1", rate.calls)
	}
	if bucket.calls != 0 {
		t.Fatalf("deletion retry bucket calls = %d, want 0", bucket.calls)
	}
	if got := network.total(coreteamquota.KeyNetworkIngressBytes); got != int64(len("retry")) {
		t.Fatalf("ingress bytes = %d, want %d", got, len("retry"))
	}
	if got := network.total(coreteamquota.KeyNetworkEgressBytes); got != int64(len("deleted:retry")) {
		t.Fatalf("egress bytes = %d, want %d", got, len("deleted:retry"))
	}
}

func TestEdgeTrafficActiveOwnTeamDeletionReturnsJSONAfterTombstone(t *testing.T) {
	disabled := trafficTeamAdmissionDisabled("team-a")
	network := &trafficNetworkLimiter{
		errByKey: map[coreteamquota.Key]error{
			coreteamquota.KeyNetworkEgressBytes: disabled,
		},
	}
	rate := &fakeRateLimiter{
		decision: tokenbucket.Decision{Allowed: true, Remaining: 9},
	}
	bucket := &fakeTokenBucket{
		decision: tokenbucket.Decision{Allowed: true},
	}
	lease := newTrafficLease()
	concurrency := &trafficConcurrencyLimiter{lease: lease}
	controller := NewController(
		nil,
		nil,
		rate,
		bucket,
		nil,
		WithRegionID("region-a"),
		WithConcurrencyLimiter(concurrency),
		WithNetworkLimiter(network),
	)
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodJWT))
	router.Use(controller.AdmitEdgeTraffic(false))
	router.Use(controller.AdmitActiveRequests(false))
	router.Use(controller.RateLimitAPIRequests(false))
	router.DELETE("/teams/:id", func(c *gin.Context) {
		spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "team deleted"})
	})

	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodDelete, "/teams/team-a", nil),
	)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want JSON", got)
	}
	var payload spec.Response
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v; body=%q", err, response.Body.String())
	}
	data, ok := payload.Data.(map[string]any)
	if !payload.Success || !ok || data["message"] != "team deleted" {
		t.Fatalf("response payload = %#v, want team deletion success", payload)
	}
	if rate.calls != 1 || bucket.calls != 0 {
		t.Fatalf("normal/retry rate calls = %d/%d, want 1/0", rate.calls, bucket.calls)
	}
	if got := concurrency.acquireCallCount(); got != 1 {
		t.Fatalf("active request acquire calls = %d, want 1", got)
	}
	if got := lease.releaseCallCount(); got != 1 {
		t.Fatalf("active request release calls = %d, want 1", got)
	}
	if got := network.callCount(coreteamquota.KeyNetworkEgressBytes); got != 1 {
		t.Fatalf("transition egress calls = %d, want 1", got)
	}
	if got := network.total(coreteamquota.KeyNetworkEgressBytes); got != 0 {
		t.Fatalf("unadmitted transition egress bytes = %d, want 0", got)
	}
}

func TestEdgeTrafficOwnTeamDeletionDoesNotBypassOrdinaryEgressFailure(t *testing.T) {
	networkErr := errors.New("redis unavailable")
	network := &trafficNetworkLimiter{
		errByKey: map[coreteamquota.Key]error{
			coreteamquota.KeyNetworkEgressBytes: networkErr,
		},
	}
	rate := &fakeRateLimiter{
		decision: tokenbucket.Decision{Allowed: true},
	}
	bucket := &fakeTokenBucket{
		decision: tokenbucket.Decision{Allowed: true},
	}
	controller := NewController(
		nil,
		nil,
		rate,
		bucket,
		nil,
		WithRegionID("region-a"),
		WithNetworkLimiter(network),
	)
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodJWT))
	router.Use(controller.AdmitEdgeTraffic(false))
	router.Use(controller.RateLimitAPIRequests(false))
	var writeErr error
	router.DELETE("/teams/:id", func(c *gin.Context) {
		_, writeErr = c.Writer.Write([]byte("must not escape"))
	})

	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodDelete, "/teams/team-a", nil),
	)

	if !errors.Is(writeErr, networkErr) {
		t.Fatalf("write error = %v, want network failure", writeErr)
	}
	if response.Body.Len() != 0 {
		t.Fatalf("response body = %q, want failed egress to remain blocked", response.Body.String())
	}
	if rate.calls != 1 || bucket.calls != 0 {
		t.Fatalf("normal/retry rate calls = %d/%d, want 1/0", rate.calls, bucket.calls)
	}
}

func TestEdgeTrafficActiveOwnTeamDeletionTransitionResponseHasHardLimit(t *testing.T) {
	disabled := trafficTeamAdmissionDisabled("team-a")
	network := &trafficNetworkLimiter{
		errByKey: map[coreteamquota.Key]error{
			coreteamquota.KeyNetworkEgressBytes: disabled,
		},
	}
	rate := &fakeRateLimiter{
		decision: tokenbucket.Decision{Allowed: true},
	}
	bucket := &fakeTokenBucket{
		decision: tokenbucket.Decision{Allowed: true},
	}
	controller := NewController(
		nil,
		nil,
		rate,
		bucket,
		nil,
		WithRegionID("region-a"),
		WithNetworkLimiter(network),
	)
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodJWT))
	router.Use(controller.AdmitEdgeTraffic(false))
	router.Use(controller.RateLimitAPIRequests(false))
	router.DELETE("/teams/:id", func(c *gin.Context) {
		_, _ = c.Writer.Write(bytes.Repeat([]byte("x"), deletionRetryResponseMaxBytes+1))
	})

	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodDelete, "/teams/team-a", nil),
	)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"status = %d, want 503 after transition response exceeds hard limit; body=%s",
			response.Code,
			response.Body.String(),
		)
	}
	if response.Body.Len() == 0 || response.Body.Len() > deletionRetryResponseMaxBytes {
		t.Fatalf(
			"response bytes = %d, want bounded non-empty error up to %d bytes",
			response.Body.Len(),
			deletionRetryResponseMaxBytes,
		)
	}
	if rate.calls != 1 || bucket.calls != 0 {
		t.Fatalf("normal/retry rate calls = %d/%d, want 1/0", rate.calls, bucket.calls)
	}
}

func TestEdgeTrafficTombstonedOwnTeamDeletionRetryCanReturn(t *testing.T) {
	disabled := trafficTeamAdmissionDisabled("team-a")
	network := &trafficNetworkLimiter{
		errByKey: map[coreteamquota.Key]error{
			coreteamquota.KeyNetworkIngressBytes: disabled,
			coreteamquota.KeyNetworkEgressBytes:  disabled,
		},
	}
	rate := &fakeRateLimiter{err: disabled}
	bucket := &fakeTokenBucket{
		decision: tokenbucket.Decision{Allowed: true, Remaining: 2},
	}
	controller := NewController(
		nil,
		nil,
		rate,
		bucket,
		nil,
		WithRegionID("region-a"),
		WithNetworkLimiter(network),
	)
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodJWT))
	router.Use(controller.AdmitEdgeTraffic(false))
	router.Use(controller.RateLimitAPIRequests(false))
	router.DELETE("/teams/:id", func(c *gin.Context) {
		c.String(http.StatusOK, "team deleted")
	})

	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodDelete, "/teams/team-a", nil),
	)

	if response.Code != http.StatusOK || response.Body.String() != "team deleted" {
		t.Fatalf("response = %d %q, want bounded deletion retry response", response.Code, response.Body.String())
	}
	if rate.calls != 1 || bucket.calls != 1 {
		t.Fatalf("rate/retry calls = %d/%d, want 1/1", rate.calls, bucket.calls)
	}
	if got := network.callCount(coreteamquota.KeyNetworkIngressBytes); got != 0 {
		t.Fatalf("empty retry ingress calls = %d, want 0", got)
	}
	if got := network.callCount(coreteamquota.KeyNetworkEgressBytes); got != 0 {
		t.Fatalf("special retry egress calls = %d, want 0", got)
	}
}

func TestEdgeTrafficTombstonedOwnTeamDeletionRetryBypassesActiveRequestLease(t *testing.T) {
	disabled := trafficTeamAdmissionDisabled("team-a")
	network := &trafficNetworkLimiter{
		errByKey: map[coreteamquota.Key]error{
			coreteamquota.KeyNetworkIngressBytes: disabled,
			coreteamquota.KeyNetworkEgressBytes:  disabled,
		},
	}
	rate := &fakeRateLimiter{err: disabled}
	bucket := &fakeTokenBucket{
		decision: tokenbucket.Decision{Allowed: true, Remaining: 2},
	}
	concurrency := &trafficConcurrencyLimiter{err: disabled}
	controller := NewController(
		nil,
		nil,
		rate,
		bucket,
		nil,
		WithRegionID("region-a"),
		WithConcurrencyLimiter(concurrency),
		WithNetworkLimiter(network),
	)
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodJWT))
	router.Use(controller.AdmitEdgeTraffic(false))
	router.Use(controller.AdmitActiveRequests(false))
	router.Use(controller.RateLimitAPIRequests(false))
	router.DELETE("/teams/:id", func(c *gin.Context) {
		spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "team deleted"})
	})

	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodDelete, "/teams/team-a", nil),
	)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	var payload spec.Response
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v; body=%q", err, response.Body.String())
	}
	if !payload.Success {
		t.Fatalf("response payload = %#v, want success", payload)
	}
	if got := concurrency.acquireCallCount(); got != 1 {
		t.Fatalf("active request acquire calls = %d, want 1", got)
	}
	if rate.calls != 0 || bucket.calls != 1 {
		t.Fatalf("normal/retry rate calls = %d/%d, want 0/1", rate.calls, bucket.calls)
	}
	if got := network.callCount(coreteamquota.KeyNetworkIngressBytes); got != 0 {
		t.Fatalf("retry ingress calls = %d, want 0", got)
	}
	if got := network.callCount(coreteamquota.KeyNetworkEgressBytes); got != 0 {
		t.Fatalf("retry egress calls = %d, want 0", got)
	}
}

func TestEdgeTrafficTombstonedDeletionRetryWithBodyFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*http.Request)
	}{
		{name: "content length"},
		{
			name: "chunked",
			configure: func(request *http.Request) {
				request.ContentLength = -1
				request.TransferEncoding = []string{"chunked"}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			disabled := trafficTeamAdmissionDisabled("team-a")
			network := &trafficNetworkLimiter{
				errByKey: map[coreteamquota.Key]error{
					coreteamquota.KeyNetworkIngressBytes: disabled,
					coreteamquota.KeyNetworkEgressBytes:  disabled,
				},
			}
			rate := &fakeRateLimiter{err: disabled}
			bucket := &fakeTokenBucket{
				decision: tokenbucket.Decision{Allowed: true},
			}
			controller := NewController(
				nil,
				nil,
				rate,
				bucket,
				nil,
				WithRegionID("region-a"),
				WithConcurrencyLimiter(&trafficConcurrencyLimiter{err: disabled}),
				WithNetworkLimiter(network),
			)
			router := gin.New()
			router.Use(authContextMiddleware("team-a", authn.AuthMethodJWT))
			router.Use(controller.AdmitEdgeTraffic(false))
			router.Use(controller.AdmitActiveRequests(false))
			router.Use(controller.RateLimitAPIRequests(false))
			handlerCalled := false
			router.DELETE("/teams/:id", func(c *gin.Context) {
				handlerCalled = true
				c.String(http.StatusOK, "must not escape")
			})

			request := httptest.NewRequest(
				http.MethodDelete,
				"/teams/team-a",
				bytes.NewBufferString("unmetered ingress"),
			)
			if test.configure != nil {
				test.configure(request)
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			if handlerCalled {
				t.Fatal("team deletion handler ran before retry ingress was rejected")
			}
			if rate.calls != 0 || bucket.calls != 0 {
				t.Fatalf("rate/retry calls = %d/%d, want 0/0", rate.calls, bucket.calls)
			}
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want fail-closed 503; body=%s", response.Code, response.Body.String())
			}
			if response.Body.String() == "must not escape" {
				t.Fatal("handler success escaped after tombstoned ingress admission failed")
			}
			if got := network.callCount(coreteamquota.KeyNetworkIngressBytes); got != 1 {
				t.Fatalf("ingress admission calls = %d, want 1", got)
			}
			if got := network.total(coreteamquota.KeyNetworkIngressBytes); got != 0 {
				t.Fatalf("unadmitted ingress bytes = %d, want 0", got)
			}
			if got := network.callCount(coreteamquota.KeyNetworkEgressBytes); got != 0 {
				t.Fatalf("special retry egress calls = %d, want 0", got)
			}
		})
	}
}

func TestEdgeTrafficDrainsUnreadRequestBodyBeforeCleanup(t *testing.T) {
	network := &trafficNetworkLimiter{}
	controller := NewController(
		nil,
		nil,
		&fakeRateLimiter{decision: tokenbucket.Decision{Allowed: true}},
		&fakeTokenBucket{},
		nil,
		WithRegionID("region-a"),
		WithNetworkLimiter(network),
	)
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodJWT))
	router.Use(controller.AdmitEdgeTraffic(false))
	router.Use(controller.RateLimitAPIRequests(false))
	router.POST("/ignore-body", func(c *gin.Context) {
		if err := c.Request.Body.Close(); err != nil {
			t.Errorf("handler Close(body) error = %v", err)
		}
		c.String(http.StatusAccepted, "accepted")
	})

	payload := "handler deliberately ignores this body"
	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodPost,
			"/ignore-body",
			bytes.NewBufferString(payload),
		),
	)

	if response.Code != http.StatusAccepted || response.Body.String() != "accepted" {
		t.Fatalf("response = %d %q, want 202 accepted", response.Code, response.Body.String())
	}
	if got := network.total(coreteamquota.KeyNetworkIngressBytes); got != int64(len(payload)) {
		t.Fatalf("drained ingress bytes = %d, want %d", got, len(payload))
	}
	if got := network.total(coreteamquota.KeyNetworkEgressBytes); got != int64(len("accepted")) {
		t.Fatalf("egress bytes = %d, want %d", got, len("accepted"))
	}
}

func TestEdgeTrafficDeletionRetryResponseHasHardLimit(t *testing.T) {
	disabled := trafficTeamAdmissionDisabled("team-a")
	network := &trafficNetworkLimiter{
		errByKey: map[coreteamquota.Key]error{
			coreteamquota.KeyNetworkIngressBytes: disabled,
			coreteamquota.KeyNetworkEgressBytes:  disabled,
		},
	}
	controller := NewController(
		nil,
		nil,
		&fakeRateLimiter{err: disabled},
		&fakeTokenBucket{decision: tokenbucket.Decision{Allowed: true}},
		nil,
		WithRegionID("region-a"),
		WithNetworkLimiter(network),
	)
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodJWT))
	router.Use(controller.AdmitEdgeTraffic(false))
	router.Use(controller.RateLimitAPIRequests(false))
	router.DELETE("/teams/:id", func(c *gin.Context) {
		_, _ = c.Writer.Write(bytes.Repeat([]byte("x"), deletionRetryResponseMaxBytes+1))
	})

	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodDelete, "/teams/team-a", nil),
	)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 after retry response exceeds hard limit", response.Code)
	}
	if response.Body.Len() > deletionRetryResponseMaxBytes {
		t.Fatalf("response bytes = %d, exceed hard limit %d", response.Body.Len(), deletionRetryResponseMaxBytes)
	}
}

func TestNetworkTrafficCountsHijackedHandshakeAndStreamOnce(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	baseWriter := &hijackableHTTPWriter{
		header: make(http.Header),
		conn:   serverConn,
	}
	ginCtx, _ := gin.CreateTestContext(baseWriter)
	ginCtx.Request = httptest.NewRequest(http.MethodGet, "/ws", nil)
	network := &trafficNetworkLimiter{}
	controller := newTrafficTestController(
		&trafficConcurrencyLimiter{lease: newTrafficLease()},
		network,
	)
	cleanup, ok := controller.attachNetworkTraffic(ginCtx, "team-a")
	if !ok {
		t.Fatal("attachNetworkTraffic() rejected test request")
	}
	defer cleanup()

	quotaConn, buffered, err := ginCtx.Writer.Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	defer quotaConn.Close()
	deadline := time.Now().Add(time.Second)
	_ = quotaConn.SetDeadline(deadline)
	_ = clientConn.SetDeadline(deadline)

	writeErr := make(chan error, 1)
	go func() {
		_, err := clientConn.Write([]byte("in"))
		writeErr <- err
	}()
	incoming := make([]byte, len("in"))
	if _, err := io.ReadFull(quotaConn, incoming); err != nil {
		t.Fatalf("read hijacked stream: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write hijacked stream peer: %v", err)
	}

	readResult := make(chan struct {
		data string
		err  error
	}, 1)
	go func() {
		payload := make([]byte, len("out"))
		_, err := io.ReadFull(clientConn, payload)
		readResult <- struct {
			data string
			err  error
		}{data: string(payload), err: err}
	}()
	if _, err := quotaConn.Write([]byte("out")); err != nil {
		t.Fatalf("write hijacked stream: %v", err)
	}
	if result := <-readResult; result.err != nil || result.data != "out" {
		t.Fatalf("read hijacked stream peer = (%q, %v)", result.data, result.err)
	}

	go func() {
		_, err := clientConn.Write([]byte("hs"))
		writeErr <- err
	}()
	handshakeIn := make([]byte, len("hs"))
	if _, err := io.ReadFull(buffered, handshakeIn); err != nil {
		t.Fatalf("read buffered handshake: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write buffered handshake peer: %v", err)
	}
	go func() {
		payload := make([]byte, len("ok"))
		_, err := io.ReadFull(clientConn, payload)
		if string(payload) != "ok" && err == nil {
			err = errors.New("unexpected buffered handshake payload")
		}
		writeErr <- err
	}()
	if _, err := buffered.WriteString("ok"); err != nil {
		t.Fatalf("write buffered handshake: %v", err)
	}
	if err := buffered.Flush(); err != nil {
		t.Fatalf("flush buffered handshake: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("read buffered handshake peer: %v", err)
	}

	if got := network.total(coreteamquota.KeyNetworkIngressBytes); got != int64(len("in")+len("hs")) {
		t.Fatalf("hijacked ingress bytes = %d, want %d", got, len("in")+len("hs"))
	}
	if got := network.total(coreteamquota.KeyNetworkEgressBytes); got != int64(len("out")+len("ok")) {
		t.Fatalf("hijacked egress bytes = %d, want %d", got, len("out")+len("ok"))
	}
}

func authContextMiddleware(teamID string, method authn.AuthMethod) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			TeamID:     teamID,
			AuthMethod: method,
		})
		c.Next()
	}
}

func newTrafficTestController(
	concurrency ConcurrencyLimiter,
	network NetworkLimiter,
) *Controller {
	return NewController(
		nil,
		nil,
		&fakeRateLimiter{decision: tokenbucket.Decision{Allowed: true}},
		nil,
		nil,
		WithConcurrencyLimiter(concurrency),
		WithNetworkLimiter(network),
	)
}

func trafficTeamAdmissionDisabled(teamID string) error {
	return &coreteamquota.UnavailableError{
		Operation: "take team quota tokens",
		Err:       &coreteamquota.TeamAdmissionDisabledError{TeamID: teamID},
	}
}

type trafficConcurrencyLimiter struct {
	mu    sync.Mutex
	lease ConnectionLease
	err   error
	calls int
}

func (l *trafficConcurrencyLimiter) Acquire(
	context.Context,
	string,
	coreteamquota.Key,
) (ConnectionLease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	return l.lease, l.err
}

func (*trafficConcurrencyLimiter) Usage(context.Context, string, coreteamquota.Key) (int64, error) {
	return 0, nil
}

func (*trafficConcurrencyLimiter) Invalidate(string, coreteamquota.Key) {}
func (*trafficConcurrencyLimiter) Close() error                         { return nil }

func (l *trafficConcurrencyLimiter) acquireCallCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

type trafficLease struct {
	done chan struct{}
	once sync.Once

	mu           sync.Mutex
	err          error
	releaseCalls int
}

func newTrafficLease() *trafficLease {
	return &trafficLease{done: make(chan struct{})}
}

func (l *trafficLease) Done() <-chan struct{} { return l.done }

func (l *trafficLease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

func (l *trafficLease) Release(context.Context) error {
	l.mu.Lock()
	l.releaseCalls++
	l.mu.Unlock()
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *trafficLease) fail(err error) {
	l.mu.Lock()
	l.err = err
	l.mu.Unlock()
	l.once.Do(func() { close(l.done) })
}

func (l *trafficLease) releaseCallCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.releaseCalls
}

type trafficNetworkLimiter struct {
	mu       sync.Mutex
	totals   map[coreteamquota.Key]int64
	calls    map[coreteamquota.Key]int
	err      error
	errByKey map[coreteamquota.Key]error
	onWait   func(coreteamquota.Key, int)
}

type panicCloseTrafficBody struct {
	*bytes.Reader
}

func (*panicCloseTrafficBody) Close() error {
	panic("body close panic")
}

func (l *trafficNetworkLimiter) WaitN(
	_ context.Context,
	_ string,
	key coreteamquota.Key,
	bytes int,
) error {
	if l.onWait != nil {
		l.onWait(key, bytes)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.calls == nil {
		l.calls = make(map[coreteamquota.Key]int)
	}
	l.calls[key]++
	if err := l.errByKey[key]; err != nil {
		return err
	}
	if l.err != nil {
		return l.err
	}
	if l.totals == nil {
		l.totals = make(map[coreteamquota.Key]int64)
	}
	l.totals[key] += int64(bytes)
	return nil
}

func (*trafficNetworkLimiter) Close() error { return nil }

func (l *trafficNetworkLimiter) total(key coreteamquota.Key) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.totals[key]
}

func (l *trafficNetworkLimiter) callCount(key coreteamquota.Key) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls[key]
}

type hijackableHTTPWriter struct {
	header http.Header
	conn   net.Conn
}

func (w *hijackableHTTPWriter) Header() http.Header             { return w.header }
func (*hijackableHTTPWriter) Write(payload []byte) (int, error) { return len(payload), nil }
func (*hijackableHTTPWriter) WriteHeader(int)                   {}
func (w *hijackableHTTPWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}
