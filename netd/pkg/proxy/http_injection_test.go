package proxy

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHTTPAdapterInjectsResolvedHeaders(t *testing.T) {
	requestHeaders := make(chan http.Header, 1)
	requestBody := make(chan string, 1)
	upstream := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		requestHeaders <- r.Header.Clone()
		requestBody <- string(body)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok"))
	})
	defer upstream.Close()

	addr := upstream.Listener.Addr().(*net.TCPAddr)
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
		},
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
		rawReq := "POST /v1/test HTTP/1.1\r\nHost: api.example.com\r\nContent-Length: 7\r\n\r\npayload"
		req := &adapterRequest{
			Server:   server,
			Compiled: &policy.CompiledPolicy{SandboxID: "sbx_123", TeamID: "team_123"},
			SrcIP:    "10.0.0.2",
			DestIP:   addr.IP,
			DestPort: addr.Port,
			Host:     "api.example.com",
			Conn:     conn,
			Prefix:   bytes.NewReader([]byte(rawReq)),
			EgressAuth: &egressAuthContext{
				Rule: &policy.CompiledEgressAuthRule{
					Name:    "example-http",
					AuthRef: "example-api",
				},
				Resolved: egressauth.NewHTTPHeadersResolveResponse("example-api", map[string]string{
					"Authorization": "Bearer injected-token",
					"X-Auth-Ref":    "example-api",
				}, nil),
			},
		}
		done <- (&httpAdapter{}).Handle(req)
	}()

	clientConn, err := net.Dial("tcp4", proxyListener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer clientConn.Close()
	if _, err := io.WriteString(clientConn, ""); err != nil {
		t.Fatalf("write noop: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	_ = resp.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("response body = %q", body)
	}
	if err := <-done; err != nil {
		t.Fatalf("adapter handle: %v", err)
	}

	headers := <-requestHeaders
	if got := headers.Get("Authorization"); got != "Bearer injected-token" {
		t.Fatalf("authorization header = %q", got)
	}
	if got := headers.Get("X-Auth-Ref"); got != "example-api" {
		t.Fatalf("x-auth-ref header = %q", got)
	}
	if got := <-requestBody; got != "payload" {
		t.Fatalf("request body = %q", got)
	}
}

func TestHTTPAdapterSubstitutesPlaceholdersInQueryHeaderAndBody(t *testing.T) {
	type observedRequest struct {
		Query  string
		Header string
		Body   string
	}
	requests := make(chan observedRequest, 1)
	upstream := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		requests <- observedRequest{
			Query:  r.URL.Query().Get("token"),
			Header: r.Header.Get("X-Api-Key"),
			Body:   string(body),
		}
		_, _ = w.Write([]byte("ok"))
	})
	defer upstream.Close()

	addr := upstream.Listener.Addr().(*net.TCPAddr)
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
		},
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

		body := "body=s0env_test_token"
		rawReq := "POST /v1/test?token=s0env_test_token HTTP/1.1\r\n" +
			"Host: api.example.com\r\n" +
			"X-Api-Key: s0env_test_token\r\n" +
			"Content-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n" +
			body
		req := &adapterRequest{
			Server:   server,
			Compiled: &policy.CompiledPolicy{SandboxID: "sbx_123", TeamID: "team_123"},
			SrcIP:    "10.0.0.2",
			DestIP:   addr.IP,
			DestPort: addr.Port,
			Host:     "api.example.com",
			Conn:     conn,
			Prefix:   bytes.NewReader([]byte(rawReq)),
			EgressAuth: &egressAuthContext{
				Rule: &policy.CompiledEgressAuthRule{
					Name:    "example-http",
					AuthRef: "example-api",
				},
				Resolved: egressauth.NewPlaceholderSubstitutionResolveResponse("example-api", &egressauth.PlaceholderSubstitutionDirective{
					Replacements: []egressauth.PlaceholderSubstitutionReplacement{{
						Placeholder: "s0env_test_token",
						Value:       "resolved-secret",
						Locations: []egressauth.PlaceholderSubstitutionLocation{
							egressauth.PlaceholderSubstitutionLocationHeader,
							egressauth.PlaceholderSubstitutionLocationQuery,
							egressauth.PlaceholderSubstitutionLocationBody,
						},
					}},
				}, nil),
			},
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
		t.Fatalf("read response body: %v", err)
	}
	_ = resp.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("response body = %q", body)
	}
	if err := <-done; err != nil {
		t.Fatalf("adapter handle: %v", err)
	}

	observed := <-requests
	if observed.Query != "resolved-secret" {
		t.Fatalf("query token = %q", observed.Query)
	}
	if observed.Header != "resolved-secret" {
		t.Fatalf("x-api-key = %q", observed.Header)
	}
	if observed.Body != "body=resolved-secret" {
		t.Fatalf("body = %q", observed.Body)
	}
}

func TestHTTPAdapterReturns503WhenAuthResolutionFails(t *testing.T) {
	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: time.Second},
		},
		logger: zap.NewNop(),
	}
	clientConn, upstreamConn := net.Pipe()
	defer clientConn.Close()
	defer upstreamConn.Close()

	errCh := make(chan error, 1)
	go func() {
		req := &adapterRequest{
			Server: upstreamConnServer(server),
			Conn:   upstreamConn,
			EgressAuth: &egressAuthContext{
				Rule:         &policy.CompiledEgressAuthRule{Name: "example-http", AuthRef: "example-api"},
				ResolveError: io.EOF,
			},
		}
		errCh <- (&httpAdapter{}).Handle(req)
	}()

	resp, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if !strings.Contains(string(body), "egress auth resolution failed") {
		t.Fatalf("body = %q", body)
	}
	if err := <-errCh; err == nil {
		t.Fatal("expected adapter error")
	}
}

func TestHTTPAdapterFailOpenBypassesInjectionOnResolveError(t *testing.T) {
	requestHeaders := make(chan http.Header, 1)
	upstream := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		requestHeaders <- r.Header.Clone()
		_, _ = w.Write([]byte("ok"))
	})
	defer upstream.Close()

	addr := upstream.Listener.Addr().(*net.TCPAddr)
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
		},
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
		rawReq := "GET /v1/test HTTP/1.1\r\nHost: api.example.com\r\n\r\n"
		req := &adapterRequest{
			Server:   server,
			Compiled: &policy.CompiledPolicy{SandboxID: "sbx_123", TeamID: "team_123"},
			SrcIP:    "10.0.0.2",
			DestIP:   addr.IP,
			DestPort: addr.Port,
			Host:     "api.example.com",
			Conn:     conn,
			Prefix:   bytes.NewReader([]byte(rawReq)),
			EgressAuth: &egressAuthContext{
				Rule: &policy.CompiledEgressAuthRule{
					Name:          "example-http",
					AuthRef:       "example-api",
					FailurePolicy: v1alpha1.EgressAuthFailurePolicyFailOpen,
				},
				FailurePolicy: string(v1alpha1.EgressAuthFailurePolicyFailOpen),
				ResolveError:  io.EOF,
				BypassReason:  "resolve_error",
			},
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
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err := <-done; err != nil {
		t.Fatalf("adapter handle: %v", err)
	}

	headers := <-requestHeaders
	if got := headers.Get("Authorization"); got != "" {
		t.Fatalf("authorization header = %q, want empty", got)
	}
}

func TestHTTPAdapterInjectsHeadersOnlyWhenRequestMatcherMatches(t *testing.T) {
	requestHeaders := make(chan http.Header, 1)
	upstream := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		requestHeaders <- r.Header.Clone()
		_, _ = w.Write([]byte("ok"))
	})
	defer upstream.Close()

	addr := upstream.Listener.Addr().(*net.TCPAddr)
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	resolver := &stubEgressAuthResolver{
		resp: egressauth.NewHTTPHeadersResolveResponse("example-api", map[string]string{
			"Authorization": "Bearer matched-token",
		}, nil),
	}
	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
			EgressAuthEnabled:    true,
		},
		authResolver: resolver,
		authCache:    newMemoryEgressAuthCache(),
		logger:       zap.NewNop(),
	}

	done := make(chan error, 1)
	go func() {
		conn, acceptErr := proxyListener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()
		rawReq := "POST /v1/write?confirm=true HTTP/1.1\r\nHost: api.example.com\r\nX-Mode: write\r\n\r\n"
		req := &adapterRequest{
			Server:   server,
			Compiled: &policy.CompiledPolicy{SandboxID: "sbx_123", TeamID: "team_123"},
			SrcIP:    "10.0.0.2",
			DestIP:   addr.IP,
			DestPort: addr.Port,
			Host:     "api.example.com",
			Conn:     conn,
			Prefix:   bytes.NewReader([]byte(rawReq)),
			EgressAuth: &egressAuthContext{
				Rule: &policy.CompiledEgressAuthRule{
					Name:    "example-http",
					AuthRef: "example-api",
					HTTPMatch: &policy.CompiledHTTPMatch{
						Methods:      []string{http.MethodPost},
						PathPrefixes: []string{"/v1/write"},
						Query: []policy.CompiledHTTPValueMatch{{
							Name:   "confirm",
							Values: []string{"true"},
						}},
						Headers: []policy.CompiledHTTPValueMatch{{
							Name:   "x-mode",
							Values: []string{"write"},
						}},
					},
				},
				FailurePolicy: string(v1alpha1.EgressAuthFailurePolicyFailClosed),
			},
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
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err := <-done; err != nil {
		t.Fatalf("adapter handle: %v", err)
	}

	headers := <-requestHeaders
	if got := headers.Get("Authorization"); got != "Bearer matched-token" {
		t.Fatalf("authorization header = %q", got)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolver.calls)
	}
}

func TestHTTPAdapterSkipsResolverWhenRequestMatcherDoesNotMatch(t *testing.T) {
	requestHeaders := make(chan http.Header, 1)
	upstream := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		requestHeaders <- r.Header.Clone()
		_, _ = w.Write([]byte("ok"))
	})
	defer upstream.Close()

	addr := upstream.Listener.Addr().(*net.TCPAddr)
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	resolver := &stubEgressAuthResolver{
		resp: egressauth.NewHTTPHeadersResolveResponse("example-api", map[string]string{
			"Authorization": "Bearer should-not-be-used",
		}, nil),
	}
	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
			EgressAuthEnabled:    true,
		},
		authResolver: resolver,
		authCache:    newMemoryEgressAuthCache(),
		logger:       zap.NewNop(),
	}

	done := make(chan error, 1)
	go func() {
		conn, acceptErr := proxyListener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()
		rawReq := "GET /v1/read HTTP/1.1\r\nHost: api.example.com\r\n\r\n"
		req := &adapterRequest{
			Server:   server,
			Compiled: &policy.CompiledPolicy{SandboxID: "sbx_123", TeamID: "team_123"},
			SrcIP:    "10.0.0.2",
			DestIP:   addr.IP,
			DestPort: addr.Port,
			Host:     "api.example.com",
			Conn:     conn,
			Prefix:   bytes.NewReader([]byte(rawReq)),
			EgressAuth: &egressAuthContext{
				Rule: &policy.CompiledEgressAuthRule{
					Name:    "example-http",
					AuthRef: "example-api",
					HTTPMatch: &policy.CompiledHTTPMatch{
						Methods:      []string{http.MethodPost},
						PathPrefixes: []string{"/v1/write"},
					},
				},
				FailurePolicy: string(v1alpha1.EgressAuthFailurePolicyFailClosed),
			},
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
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err := <-done; err != nil {
		t.Fatalf("adapter handle: %v", err)
	}

	headers := <-requestHeaders
	if got := headers.Get("Authorization"); got != "" {
		t.Fatalf("authorization header = %q, want empty", got)
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver calls = %d, want 0", resolver.calls)
	}
}

func TestHTTPAdapterFallsThroughCredentialCandidatesByRequestMatcher(t *testing.T) {
	requestHeaders := make(chan http.Header, 1)
	upstream := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		requestHeaders <- r.Header.Clone()
		_, _ = w.Write([]byte("ok"))
	})
	defer upstream.Close()

	addr := upstream.Listener.Addr().(*net.TCPAddr)
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	emuRule := &policy.CompiledEgressAuthRule{
		Name:    "github-emu-auth",
		AuthRef: "github_emu",
		HTTPMatch: &policy.CompiledHTTPMatch{
			PathPrefixes: []string{"/emu-org/"},
		},
	}
	cloudRule := &policy.CompiledEgressAuthRule{
		Name:    "github-cloud-auth",
		AuthRef: "github_cloud",
		HTTPMatch: &policy.CompiledHTTPMatch{
			PathPrefixes: []string{"/cloud-org/"},
		},
	}
	resolver := &stubEgressAuthResolver{
		responses: map[string]*egressauth.ResolveResponse{
			"github_emu": egressauth.NewHTTPHeadersResolveResponse("github_emu", map[string]string{
				"Authorization": "Bearer emu-token",
			}, nil),
			"github_cloud": egressauth.NewHTTPHeadersResolveResponse("github_cloud", map[string]string{
				"Authorization": "Bearer cloud-token",
			}, nil),
		},
	}
	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
			EgressAuthEnabled:    true,
		},
		authResolver: resolver,
		authCache:    newMemoryEgressAuthCache(),
		logger:       zap.NewNop(),
	}

	done := make(chan error, 1)
	go func() {
		conn, acceptErr := proxyListener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()
		rawReq := "GET /cloud-org/repo.git/info/refs HTTP/1.1\r\nHost: github.com\r\n\r\n"
		req := &adapterRequest{
			Server:   server,
			Compiled: &policy.CompiledPolicy{SandboxID: "sbx_123", TeamID: "team_123"},
			SrcIP:    "10.0.0.2",
			DestIP:   addr.IP,
			DestPort: addr.Port,
			Host:     "github.com",
			Conn:     conn,
			Prefix:   bytes.NewReader([]byte(rawReq)),
		}
		server.attachEgressAuth(req, trafficDecision{
			Transport:          "tcp",
			Protocol:           "http",
			MatchedAuthRule:    emuRule,
			AuthRuleCandidates: []*policy.CompiledEgressAuthRule{emuRule, cloudRule},
			NeedsEgressAuth:    true,
		})
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
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err := <-done; err != nil {
		t.Fatalf("adapter handle: %v", err)
	}

	headers := <-requestHeaders
	if got := headers.Get("Authorization"); got != "Bearer cloud-token" {
		t.Fatalf("authorization header = %q, want cloud token", got)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolver.calls)
	}
	if len(resolver.requests) != 1 {
		t.Fatalf("resolver requests = %d, want 1", len(resolver.requests))
	}
	if got := resolver.requests[0].AuthRef; got != "github_cloud" {
		t.Fatalf("resolver auth ref = %q, want github_cloud", got)
	}
	if got := resolver.requests[0].RuleName; got != "github-cloud-auth" {
		t.Fatalf("resolver rule name = %q, want github-cloud-auth", got)
	}
}

func TestHTTPAdapterReturns503WhenDirectiveUnsupported(t *testing.T) {
	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: time.Second},
		},
		logger: zap.NewNop(),
	}
	clientConn, upstreamConn := net.Pipe()
	defer clientConn.Close()
	defer upstreamConn.Close()

	errCh := make(chan error, 1)
	go func() {
		req := &adapterRequest{
			Server: upstreamConnServer(server),
			Conn:   upstreamConn,
			EgressAuth: &egressAuthContext{
				Rule: &policy.CompiledEgressAuthRule{Name: "example-http", AuthRef: "example-api"},
				Resolved: &egressauth.ResolveResponse{
					AuthRef: "example-api",
					Directives: []egressauth.ResolveDirective{{
						Kind: egressauth.ResolveDirectiveKindCustom,
					}},
				},
			},
		}
		errCh <- (&httpAdapter{}).Handle(req)
	}()

	resp, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if !strings.Contains(string(body), "egress auth directives unsupported") {
		t.Fatalf("body = %q", body)
	}
	if err := <-errCh; err == nil {
		t.Fatal("expected adapter error")
	}
}

func httptestServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(handler)
}

func upstreamConnServer(server *Server) *Server {
	if server != nil {
		return server
	}
	return &Server{}
}
