package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

func TestWebSocketProxyRewritesUntrustedForwardedHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)

	gotHeaders := make(chan http.Header, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders <- r.Header.Clone()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		_ = conn.Close()
	}))
	defer upstream.Close()

	targetURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	engine := gin.New()
	engine.GET("/ws", NewWebSocketProxy(zap.NewNop()).Proxy(targetURL))
	server := httptest.NewServer(engine)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Forwarded":         []string{"for=203.0.113.10;proto=https"},
		"X-Forwarded-For":   []string{"203.0.113.10"},
		"X-Forwarded-Host":  []string{"evil.example"},
		"X-Forwarded-Proto": []string{"https"},
		"X-Real-IP":         []string{"203.0.113.11"},
	})
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("Dial() error = %v status=%d", err, status)
	}
	_ = conn.Close()

	select {
	case headers := <-gotHeaders:
		if got := headers.Get("Forwarded"); got != "" {
			t.Fatalf("Forwarded = %q, want empty", got)
		}
		if got := headers.Get("X-Real-IP"); got != "" {
			t.Fatalf("X-Real-IP = %q, want empty", got)
		}
		if got := headers.Get("X-Forwarded-For"); got == "" || strings.Contains(got, "203.0.113.10") {
			t.Fatalf("X-Forwarded-For = %q, want gateway remote address only", got)
		}
		if got := headers.Get("X-Forwarded-Proto"); got != "http" {
			t.Fatalf("X-Forwarded-Proto = %q, want http", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for upstream headers")
	}
}
