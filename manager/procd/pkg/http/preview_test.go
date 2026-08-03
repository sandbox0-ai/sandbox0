package http

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/procdconfig"
	"go.uber.org/zap"
)

func TestPreviewProxyForwardsLoopbackHTTPAndSanitizesBoundaries(t *testing.T) {
	var upstreamPort string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(internalauth.DefaultTokenHeader); got != "" {
			t.Errorf("internal token leaked to preview app: %q", got)
		}
		if got := r.Header.Get(previewOriginHeader); got != "" {
			t.Errorf("preview origin header leaked to preview app: %q", got)
		}
		if got := r.URL.RequestURI(); got != "/dashboard?q=1" {
			t.Errorf("request URI = %q, want /dashboard?q=1", got)
		}
		w.Header().Set("Location", "http://localhost:"+upstreamPort+"/next")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; script-src 'self'")
		http.SetCookie(w, &http.Cookie{Name: "app", Value: "value", Domain: "localhost", Path: "/"})
		http.SetCookie(w, &http.Cookie{Name: previewCookieName, Value: "overwrite", Path: "/"})
		w.WriteHeader(http.StatusFound)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()
	upstreamPort = portFromTestURL(t, upstream.URL)

	preview := &Server{cfg: &procdconfig.Config{HTTPPort: 49983}, logger: zap.NewNop()}
	request := httptest.NewRequest(http.MethodGet, "http://procd/api/v1/preview/http/"+upstreamPort+"/dashboard?q=1", nil)
	request.Header.Set(internalauth.DefaultTokenHeader, "internal-secret")
	request.Header.Set(previewOriginHeader, "https://sandbox--p3000.test.sandbox0.app")
	request = mux.SetURLVars(request, map[string]string{"scheme": "http", "port": upstreamPort, "path": "dashboard"})
	request = request.WithContext(internalauth.WithClaims(request.Context(), previewClaims()))
	recorder := httptest.NewRecorder()
	preview.previewProxyHandler(recorder, request)

	response := recorder.Result()
	defer response.Body.Close()
	if response.StatusCode != http.StatusFound {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("status = %d, body = %s", response.StatusCode, body)
	}
	if got, want := response.Header.Get("Location"), "https://sandbox--p3000.test.sandbox0.app/next"; got != want {
		t.Fatalf("Location = %q, want %q", got, want)
	}
	if got := response.Header.Get("X-Frame-Options"); got != "" {
		t.Fatalf("X-Frame-Options = %q, want empty", got)
	}
	csp := response.Header.Get("Content-Security-Policy")
	if strings.Contains(strings.ToLower(csp), "frame-ancestors") || !strings.Contains(csp, "script-src") {
		t.Fatalf("Content-Security-Policy = %q", csp)
	}
	cookies := response.Cookies()
	if len(cookies) != 1 || cookies[0].Name != "app" || cookies[0].Domain != "" || !cookies[0].Secure || !cookies[0].Partitioned {
		t.Fatalf("rewritten cookies = %#v", cookies)
	}
}

func TestPreviewProxySupportsWebSocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		messageType, payload, err := conn.ReadMessage()
		if err == nil {
			_ = conn.WriteMessage(messageType, payload)
		}
	}))
	defer upstream.Close()
	port := portFromTestURL(t, upstream.URL)

	preview := &Server{cfg: &procdconfig.Config{HTTPPort: 49983}, logger: zap.NewNop()}
	router := mux.NewRouter()
	router.HandleFunc("/preview/{scheme:http|https}/{port:[0-9]+}/{path:.*}", func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(internalauth.WithClaims(r.Context(), previewClaims()))
		preview.previewProxyHandler(w, r)
	})
	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/preview/http/" + port + "/socket"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{previewOriginHeader: []string{"https://sandbox--p3000.test.sandbox0.app"}})
	if err != nil {
		t.Fatalf("dial websocket preview: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Fatalf("write websocket message: %v", err)
	}
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket message: %v", err)
	}
	if string(payload) != "hello" {
		t.Fatalf("payload = %q, want hello", payload)
	}
}

func TestPreviewProxyRequiresPermissionAndRejectsProcdPort(t *testing.T) {
	preview := &Server{cfg: &procdconfig.Config{HTTPPort: 49983}, logger: zap.NewNop()}
	request := httptest.NewRequest(http.MethodGet, "http://procd/preview/http/3000/", nil)
	request = mux.SetURLVars(request, map[string]string{"scheme": "http", "port": "3000"})
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{SandboxID: "sandbox-1"}))
	recorder := httptest.NewRecorder()
	preview.previewProxyHandler(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("missing permission status = %d, want 403", recorder.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "http://procd/preview/http/49983/", nil)
	request = mux.SetURLVars(request, map[string]string{"scheme": "http", "port": "49983"})
	request = request.WithContext(internalauth.WithClaims(request.Context(), previewClaims()))
	recorder = httptest.NewRecorder()
	preview.previewProxyHandler(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("reserved port status = %d, want 403", recorder.Code)
	}
}

func TestPreviewLoopbackDialerFallsBackToIPv6(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- connection
		}
		close(accepted)
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	connection, err := dialPreviewLoopback(context.Background(), "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("dial IPv6 loopback fallback: %v", err)
	}
	defer connection.Close()
	var upstream net.Conn
	select {
	case upstream = <-accepted:
		if upstream == nil {
			t.Fatal("IPv6 listener did not accept the fallback connection")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the IPv6 fallback connection")
	}
	defer upstream.Close()
	if remote := connection.RemoteAddr().(*net.TCPAddr).IP; !remote.IsLoopback() || remote.To4() != nil {
		t.Fatalf("fallback remote address = %s, want IPv6 loopback", remote)
	}
}

func previewClaims() *internalauth.Claims {
	return &internalauth.Claims{SandboxID: "sandbox-1", Permissions: []string{previewProxyPermission}}
}

func portFromTestURL(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := strconv.Atoi(parsed.Port()); err != nil {
		t.Fatalf("invalid test port %q", parsed.Port())
	}
	return parsed.Port()
}
