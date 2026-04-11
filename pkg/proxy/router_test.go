package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
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
