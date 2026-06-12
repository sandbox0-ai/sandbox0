package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHTTPAdapterDeniesBlockedHTTPRequest(t *testing.T) {
	upstreamHit := make(chan struct{}, 1)
	upstream := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamHit <- struct{}{}
		w.WriteHeader(http.StatusTeapot)
	})
	defer upstream.Close()

	addr := upstream.Listener.Addr().(*net.TCPAddr)
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	compiled := compileHTTPProxyTestPolicy(t)
	server := &Server{
		cfg:    &config.NetdConfig{ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second}},
		logger: zap.NewNop(),
	}

	done := make(chan error, 1)
	go func() {
		conn, acceptErr := proxyListener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()
		rawReq := "POST /v1/read/files HTTP/1.1\r\nHost: api.example.com\r\nContent-Length: 0\r\n\r\n"
		req := &adapterRequest{
			Server:   server,
			Compiled: compiled,
			SrcIP:    "10.0.0.2",
			DestIP:   addr.IP,
			DestPort: addr.Port,
			Host:     "api.example.com",
			Conn:     conn,
			Prefix:   bytes.NewReader([]byte(rawReq)),
		}
		done <- (&httpAdapter{}).Handle(req)
	}()

	clientConn, err := net.Dial("tcp4", proxyListener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer clientConn.Close()
	resp, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
	if !bytes.Contains(body, []byte("HTTP request denied")) {
		t.Fatalf("response body = %q", body)
	}
	if err := <-done; !errors.Is(err, errProtocolPolicyDenied) {
		t.Fatalf("adapter error = %v, want protocol policy denied", err)
	}
	select {
	case <-upstreamHit:
		t.Fatal("blocked HTTP request reached upstream")
	default:
	}
}

func TestHTTPAdapterAllowsHTTPProtocolRequest(t *testing.T) {
	requestPath := make(chan string, 1)
	upstream := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		requestPath <- r.URL.Path
		_, _ = w.Write([]byte("ok"))
	})
	defer upstream.Close()

	addr := upstream.Listener.Addr().(*net.TCPAddr)
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	compiled := compileHTTPProxyTestPolicy(t)
	server := &Server{
		cfg:    &config.NetdConfig{ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second}},
		logger: zap.NewNop(),
	}

	done := make(chan error, 1)
	go func() {
		conn, acceptErr := proxyListener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()
		rawReq := "GET /v1/read/files HTTP/1.1\r\nHost: api.example.com\r\n\r\n"
		req := &adapterRequest{
			Server:   server,
			Compiled: compiled,
			SrcIP:    "10.0.0.2",
			DestIP:   addr.IP,
			DestPort: addr.Port,
			Host:     "api.example.com",
			Conn:     conn,
			Prefix:   bytes.NewReader([]byte(rawReq)),
		}
		done <- (&httpAdapter{}).Handle(req)
	}()

	clientConn, err := net.Dial("tcp4", proxyListener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer clientConn.Close()
	resp, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if string(body) != "ok" {
		t.Fatalf("response body = %q", body)
	}
	if err := <-done; err != nil {
		t.Fatalf("adapter handle: %v", err)
	}
	if got := <-requestPath; got != "/v1/read/files" {
		t.Fatalf("upstream path = %q", got)
	}
}

func compileHTTPProxyTestPolicy(t *testing.T) *policy.CompiledPolicy {
	t.Helper()
	compiled, err := policy.CompileNetworkPolicy(&v1alpha1.NetworkPolicySpec{
		Egress: &v1alpha1.NetworkEgressPolicy{
			ProtocolRules: []v1alpha1.ProtocolRule{{
				Name:     "api-readonly",
				Protocol: v1alpha1.ProtocolRuleProtocolHTTP,
				Domains:  []string{"api.example.com"},
				HTTP: &v1alpha1.HTTPProtocolRule{
					Methods: &v1alpha1.HTTPMethodPolicy{
						Allowed: []string{http.MethodGet, http.MethodHead},
						Denied:  []string{http.MethodPost},
					},
					Paths: &v1alpha1.HTTPPathPolicy{
						AllowedPrefixes: []string{"/v1/read"},
						Denied:          []string{"/v1/read/private"},
					},
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("compile policy: %v", err)
	}
	return compiled
}
