package teamquota

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

func TestActiveRequestAdmissionHoldsExactLeaseForHandlerLifetime(t *testing.T) {
	gin.SetMode(gin.TestMode)
	lease := newTrafficLease()
	limiter := &activeRequestConcurrencyLimiter{lease: lease}
	controller := newTrafficTestController(limiter, &trafficNetworkLimiter{})
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodAPIKey))
	router.Use(controller.AdmitActiveRequests(false))
	router.GET("/work", func(c *gin.Context) {
		if limiter.activeCount() != 1 {
			t.Fatalf("active leases in handler = %d, want 1", limiter.activeCount())
		}
		keys := AdmittedKeys(c.Request.Context())
		if len(keys) != 1 || keys[0] != coreteamquota.KeyActiveRequestCount {
			t.Fatalf("admitted keys = %v, want active_request_count", keys)
		}
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/work", nil))

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", recorder.Code)
	}
	if limiter.lastKey != coreteamquota.KeyActiveRequestCount {
		t.Fatalf("acquired key = %q, want active_request_count", limiter.lastKey)
	}
	if limiter.activeCount() != 0 || lease.releaseCallCount() != 1 {
		t.Fatalf("active = %d releases = %d, want 0/1", limiter.activeCount(), lease.releaseCallCount())
	}
}

func TestActiveRequestAdmissionSkipsOnlyMatchingForwardedKey(t *testing.T) {
	limiter := &activeRequestConcurrencyLimiter{lease: newTrafficLease()}
	controller := newTrafficTestController(limiter, &trafficNetworkLimiter{})
	controller.proofConsumer = &fakeAdmissionProofConsumer{trusted: true}
	router := gin.New()
	router.Use(forwardedQuotaProofMiddleware(
		t,
		http.MethodGet,
		"/work",
		internalauth.ServiceRegionalGateway,
		[]coreteamquota.Key{coreteamquota.KeyActiveRequestCount},
	))
	router.Use(controller.ConsumeForwardedAdmissionProof())
	router.Use(controller.AdmitActiveRequests(true))
	router.GET("/work", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/work", nil))

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", recorder.Code)
	}
	if limiter.acquireCalls != 0 {
		t.Fatalf("Acquire() calls = %d, want 0", limiter.acquireCalls)
	}
}

func TestActiveRequestAdmissionDeniesAtConfiguredLimit(t *testing.T) {
	limiter := &activeRequestConcurrencyLimiter{
		err: &coreteamquota.ConcurrencyExceededError{
			TeamID: "team-a",
			Key:    coreteamquota.KeyActiveRequestCount,
			Limit:  2,
			Used:   2,
		},
	}
	controller := newTrafficTestController(limiter, &trafficNetworkLimiter{})
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodAPIKey))
	router.Use(controller.AdmitActiveRequests(false))
	router.GET("/work", func(c *gin.Context) {
		t.Fatal("handler ran after active request denial")
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/work", nil))

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "active_request_count") {
		t.Fatalf("response does not identify active_request_count: %s", recorder.Body.String())
	}
}

func TestActiveRequestLeaseLossCancelsHandler(t *testing.T) {
	lease := newTrafficLease()
	limiter := &activeRequestConcurrencyLimiter{lease: lease}
	controller := newTrafficTestController(limiter, &trafficNetworkLimiter{})
	router := gin.New()
	router.Use(authContextMiddleware("team-a", authn.AuthMethodAPIKey))
	router.Use(controller.AdmitActiveRequests(false))
	handlerStarted := make(chan struct{})
	router.GET("/work", func(c *gin.Context) {
		close(handlerStarted)
		<-c.Request.Context().Done()
	})

	requestDone := make(chan struct{})
	go func() {
		router.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(http.MethodGet, "/work", nil),
		)
		close(requestDone)
	}()
	<-handlerStarted
	lease.fail(errors.New("lease renewal failed"))

	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("handler context was not canceled after active request lease loss")
	}
}

type activeRequestConcurrencyLimiter struct {
	mu sync.Mutex

	lease        ConnectionLease
	err          error
	lastKey      coreteamquota.Key
	acquireCalls int
	active       int
}

func (l *activeRequestConcurrencyLimiter) Acquire(
	_ context.Context,
	_ string,
	key coreteamquota.Key,
) (ConnectionLease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acquireCalls++
	l.lastKey = key
	if l.err != nil {
		return nil, l.err
	}
	l.active++
	return &countedActiveRequestLease{ConnectionLease: l.lease, owner: l}, nil
}

func (*activeRequestConcurrencyLimiter) Usage(context.Context, string, coreteamquota.Key) (int64, error) {
	return 0, nil
}

func (*activeRequestConcurrencyLimiter) Invalidate(string, coreteamquota.Key) {}
func (*activeRequestConcurrencyLimiter) Close() error                         { return nil }

func (l *activeRequestConcurrencyLimiter) activeCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active
}

type countedActiveRequestLease struct {
	ConnectionLease
	owner *activeRequestConcurrencyLimiter
	once  sync.Once
}

func (l *countedActiveRequestLease) Release(ctx context.Context) error {
	err := l.ConnectionLease.Release(ctx)
	l.once.Do(func() {
		l.owner.mu.Lock()
		l.owner.active--
		l.owner.mu.Unlock()
	})
	return err
}
