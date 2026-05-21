package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func TestRouterProxyToTargetTimesOutByDefault(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	router, err := NewRouter(upstream.URL, zap.NewNop(), 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	engine := gin.New()
	engine.GET("/", router.ProxyToTarget)

	server := httptest.NewServer(engine)
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/")
	if err != nil {
		t.Fatalf("GET() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusGatewayTimeout)
	}
}

func TestRouterProxyToTargetSkipsTimeoutWhenDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	router, err := NewRouter(upstream.URL, zap.NewNop(), 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	engine := gin.New()
	engine.GET("/", func(c *gin.Context) {
		c.Request = WithUpstreamTimeoutDisabledRequest(c.Request)
		router.ProxyToTarget(c)
	})

	server := httptest.NewServer(engine)
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/")
	if err != nil {
		t.Fatalf("GET() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if body := string(bodyBytes); body != "ok" {
		t.Fatalf("body = %q, want %q", body, "ok")
	}
}

func TestRouterProxyToTargetClearsWriteDeadlineWhenStreaming(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "late stream\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	router, err := NewRouter(upstream.URL, zap.NewNop(), time.Second)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	engine := gin.New()
	engine.GET("/", func(c *gin.Context) {
		c.Request = WithUpstreamTimeoutDisabledRequest(c.Request)
		router.ProxyToTarget(c)
	})

	server := httptest.NewUnstartedServer(engine)
	server.Config.WriteTimeout = 50 * time.Millisecond
	server.Start()
	defer server.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(server.URL + "/")
	if err != nil {
		t.Fatalf("GET() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if body := string(bodyBytes); body != "late stream\n" {
		t.Fatalf("body = %q, want %q", body, "late stream\n")
	}
}

func TestRouterProxyToTargetClearsDeadlinesWhenLongLivedRequestMarked(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "long lived\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	router, err := NewRouter(upstream.URL, zap.NewNop(), time.Second)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	engine := gin.New()
	engine.GET("/", func(c *gin.Context) {
		c.Request = WithLongLivedRequestRequest(c.Request)
		router.ProxyToTarget(c)
	})

	server := httptest.NewUnstartedServer(engine)
	server.Config.ReadTimeout = 50 * time.Millisecond
	server.Config.WriteTimeout = 50 * time.Millisecond
	server.Start()
	defer server.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(server.URL + "/")
	if err != nil {
		t.Fatalf("GET() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if body := string(bodyBytes); body != "long lived\n" {
		t.Fatalf("body = %q, want %q", body, "long lived\n")
	}
}

func TestRouterProxyToTargetClearsWriteDeadlineWithoutDisablingTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		_, _ = io.WriteString(w, "bounded stream\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	router, err := NewRouter(upstream.URL, zap.NewNop(), time.Second)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	engine := gin.New()
	engine.GET("/", func(c *gin.Context) {
		c.Request = WithStreamingResponseDeadlinesDisabledRequest(c.Request)
		router.ProxyToTarget(c)
	})

	server := httptest.NewUnstartedServer(engine)
	server.Config.WriteTimeout = 50 * time.Millisecond
	server.Start()
	defer server.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(server.URL + "/")
	if err != nil {
		t.Fatalf("GET() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if body := string(bodyBytes); body != "bounded stream\n" {
		t.Fatalf("body = %q, want %q", body, "bounded stream\n")
	}
}

func TestRouterProxyToTargetStillTimesOutWhenStreamingDeadlinesDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = io.WriteString(w, "too late")
	}))
	defer upstream.Close()

	router, err := NewRouter(upstream.URL, zap.NewNop(), 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	engine := gin.New()
	engine.GET("/", func(c *gin.Context) {
		c.Request = WithStreamingResponseDeadlinesDisabledRequest(c.Request)
		router.ProxyToTarget(c)
	})

	server := httptest.NewServer(engine)
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/")
	if err != nil {
		t.Fatalf("GET() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusGatewayTimeout)
	}
}

func TestRouterProxyToTargetRewritesUntrustedForwardedHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)

	gotHeaders := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders <- r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	router, err := NewRouter(upstream.URL, zap.NewNop(), time.Second)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	engine := gin.New()
	engine.GET("/", router.ProxyToTarget)
	server := httptest.NewServer(engine)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Host = "fn.example.test"
	req.Header.Set("Forwarded", "for=203.0.113.10;proto=https")
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	req.Header.Set("X-Forwarded-Host", "evil.example")
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Real-IP", "203.0.113.11")
	req.Header.Set("Connection", "X-Smuggled")
	req.Header.Set("X-Smuggled", "keep-me")

	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	headers := <-gotHeaders
	if got := headers.Get("Forwarded"); got != "" {
		t.Fatalf("Forwarded = %q, want empty", got)
	}
	if got := headers.Get("X-Real-IP"); got != "" {
		t.Fatalf("X-Real-IP = %q, want empty", got)
	}
	if got := headers.Get("X-Smuggled"); got != "" {
		t.Fatalf("X-Smuggled = %q, want empty", got)
	}
	if got := headers.Get("X-Forwarded-For"); strings.Contains(got, "203.0.113.10") || got == "" {
		t.Fatalf("X-Forwarded-For = %q, want gateway remote address only", got)
	}
	if got := headers.Get("X-Forwarded-Host"); got != "fn.example.test" {
		t.Fatalf("X-Forwarded-Host = %q, want fn.example.test", got)
	}
	if got := headers.Get("X-Forwarded-Proto"); got != "http" {
		t.Fatalf("X-Forwarded-Proto = %q, want http", got)
	}
}

func TestRouterProxyToTargetPreservesTrustedForwardedHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)

	gotHeaders := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders <- r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	router, err := NewRouter(upstream.URL, zap.NewNop(), time.Second, WithTrustedForwardedHeaders())
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	engine := gin.New()
	engine.GET("/", router.ProxyToTarget)
	server := httptest.NewServer(engine)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Host = "cluster-gateway.internal"
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	req.Header.Set("X-Forwarded-Host", "fn.example.test")
	req.Header.Set("X-Forwarded-Proto", "https")

	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	headers := <-gotHeaders
	if got := headers.Get("X-Forwarded-For"); !strings.HasPrefix(got, "203.0.113.10, ") {
		t.Fatalf("X-Forwarded-For = %q, want trusted value plus proxy hop", got)
	}
	if got := headers.Get("X-Forwarded-Host"); got != "fn.example.test" {
		t.Fatalf("X-Forwarded-Host = %q, want fn.example.test", got)
	}
	if got := headers.Get("X-Forwarded-Proto"); got != "https" {
		t.Fatalf("X-Forwarded-Proto = %q, want https", got)
	}
}

func TestRouterProxyToTargetMapsMaxBytesErrorToRequestTooLarge(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	router, err := NewRouter(upstream.URL, zap.NewNop(), time.Second)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	engine := gin.New()
	engine.POST("/", func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4)
		router.ProxyToTarget(c)
	})
	server := httptest.NewServer(engine)
	defer server.Close()

	resp, err := server.Client().Post(server.URL+"/", "text/plain", strings.NewReader("12345"))
	if err != nil {
		t.Fatalf("Post() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d, body = %s", resp.StatusCode, http.StatusRequestEntityTooLarge, string(body))
	}
}
