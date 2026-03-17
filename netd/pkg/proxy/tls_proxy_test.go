package proxy

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPrefixedConnReadsPrefixBeforeConn(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = client.Write([]byte("tail"))
	}()

	conn := newPrefixedConn(server, []byte("head-"))
	buf := make([]byte, len("head-tail"))
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		t.Fatalf("read full: %v", err)
	}
	if got := string(buf[:n]); got != "head-tail" {
		t.Fatalf("prefixed conn read = %q", got)
	}
}

func TestCertificateAuthorityIssuesCertificateForHost(t *testing.T) {
	certPEM, keyPEM, err := newSelfSignedCertificateAuthority("sandbox0-test-root", time.Hour)
	if err != nil {
		t.Fatalf("newSelfSignedCertificateAuthority: %v", err)
	}
	authority, err := newCertificateAuthority(certPEM, keyPEM, time.Hour)
	if err != nil {
		t.Fatalf("newCertificateAuthority: %v", err)
	}
	cert, err := authority.CertificateForHost("api.example.com")
	if err != nil {
		t.Fatalf("CertificateForHost: %v", err)
	}
	if cert == nil || len(cert.Certificate) == 0 {
		t.Fatal("expected issued certificate")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	if len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != "api.example.com" {
		t.Fatalf("unexpected dns names: %#v", leaf.DNSNames)
	}
}

func TestTLSAdapterInterceptsHTTPSAndInjectsHeaders(t *testing.T) {
	mitmCertPEM, mitmKeyPEM, err := newSelfSignedCertificateAuthority("sandbox0-mitm", time.Hour)
	if err != nil {
		t.Fatalf("new mitm ca: %v", err)
	}
	mitmAuthority, err := newCertificateAuthority(mitmCertPEM, mitmKeyPEM, time.Hour)
	if err != nil {
		t.Fatalf("new mitm authority: %v", err)
	}

	upstreamCAPEM, upstreamCAKeyPEM, err := newSelfSignedCertificateAuthority("sandbox0-upstream", time.Hour)
	if err != nil {
		t.Fatalf("new upstream ca: %v", err)
	}
	upstreamAuthority, err := newCertificateAuthority(upstreamCAPEM, upstreamCAKeyPEM, time.Hour)
	if err != nil {
		t.Fatalf("new upstream authority: %v", err)
	}
	upstreamLeaf, err := upstreamAuthority.CertificateForHost("api.example.com")
	if err != nil {
		t.Fatalf("upstream leaf: %v", err)
	}
	upstreamRootPool := x509.NewCertPool()
	if !upstreamRootPool.AppendCertsFromPEM(upstreamCAPEM) {
		t.Fatal("append upstream ca")
	}

	requestHeaders := make(chan http.Header, 1)
	requestBody := make(chan string, 1)
	upstreamListener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{*upstreamLeaf},
		NextProtos:   []string{"http/1.1"},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("tls listen upstream: %v", err)
	}
	defer upstreamListener.Close()
	upstreamServer := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, readErr := io.ReadAll(r.Body)
			if readErr != nil {
				t.Errorf("read upstream body: %v", readErr)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			requestHeaders <- r.Header.Clone()
			requestBody <- string(body)
			_, _ = w.Write([]byte("secure-ok"))
		}),
	}
	defer upstreamServer.Close()
	go func() {
		_ = upstreamServer.Serve(upstreamListener)
	}()

	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
		},
		logger:            zap.NewNop(),
		tlsAuthority:      mitmAuthority,
		upstreamTLSConfig: &tls.Config{RootCAs: upstreamRootPool, NextProtos: []string{"http/1.1"}},
	}

	done := make(chan error, 1)
	go func() {
		conn, acceptErr := proxyListener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()
		req := &adapterRequest{
			Server:   server,
			Compiled: &policy.CompiledPolicy{SandboxID: "sbx_123", TeamID: "team_123", Mode: v1alpha1.NetworkModeAllowAll},
			SrcIP:    "10.0.0.2",
			DestIP:   upstreamListener.Addr().(*net.TCPAddr).IP,
			DestPort: upstreamListener.Addr().(*net.TCPAddr).Port,
			Host:     "api.example.com",
			Conn:     conn,
			EgressAuth: &egressAuthContext{
				Rule: &policy.CompiledEgressAuthRule{
					Name:    "example-https",
					AuthRef: "example-api",
					TLSMode: v1alpha1.EgressTLSModeTerminateReoriginate,
				},
				Resolved: &egressauth.ResolveResponse{
					AuthRef: "example-api",
					Headers: map[string]string{
						"Authorization": "Bearer secure-token",
					},
				},
			},
		}
		done <- (&tlsAdapter{}).Handle(req)
	}()

	clientRootPool := x509.NewCertPool()
	if !clientRootPool.AppendCertsFromPEM(mitmCertPEM) {
		t.Fatal("append mitm ca")
	}
	rawConn, err := net.Dial("tcp4", proxyListener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer rawConn.Close()
	clientTLS := tls.Client(rawConn, &tls.Config{
		ServerName: "api.example.com",
		RootCAs:    clientRootPool,
		NextProtos: []string{"http/1.1"},
		MinVersion: tls.VersionTLS12,
	})
	defer clientTLS.Close()

	if _, err := io.WriteString(clientTLS, "POST /v1/test HTTP/1.1\r\nHost: api.example.com\r\nContent-Length: 7\r\n\r\npayload"); err != nil {
		t.Fatalf("write tls request: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(clientTLS), nil)
	if err != nil {
		t.Fatalf("read https response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read https response body: %v", err)
	}
	_ = resp.Body.Close()
	if got := string(body); got != "secure-ok" {
		t.Fatalf("response body = %q", got)
	}
	if err := <-done; err != nil {
		t.Fatalf("tls adapter handle: %v", err)
	}

	headers := <-requestHeaders
	if got := headers.Get("Authorization"); got != "Bearer secure-token" {
		t.Fatalf("authorization header = %q", got)
	}
	if got := <-requestBody; got != "payload" {
		t.Fatalf("request body = %q", got)
	}
}

func TestTLSAdapterInterceptsGRPCAndInjectsMetadata(t *testing.T) {
	mitmCertPEM, mitmKeyPEM, err := newSelfSignedCertificateAuthority("sandbox0-mitm", time.Hour)
	if err != nil {
		t.Fatalf("new mitm ca: %v", err)
	}
	mitmAuthority, err := newCertificateAuthority(mitmCertPEM, mitmKeyPEM, time.Hour)
	if err != nil {
		t.Fatalf("new mitm authority: %v", err)
	}

	upstreamCAPEM, upstreamCAKeyPEM, err := newSelfSignedCertificateAuthority("sandbox0-upstream", time.Hour)
	if err != nil {
		t.Fatalf("new upstream ca: %v", err)
	}
	upstreamAuthority, err := newCertificateAuthority(upstreamCAPEM, upstreamCAKeyPEM, time.Hour)
	if err != nil {
		t.Fatalf("new upstream authority: %v", err)
	}
	upstreamLeaf, err := upstreamAuthority.CertificateForHost("api.example.com")
	if err != nil {
		t.Fatalf("upstream leaf: %v", err)
	}
	upstreamRootPool := x509.NewCertPool()
	if !upstreamRootPool.AppendCertsFromPEM(upstreamCAPEM) {
		t.Fatal("append upstream ca")
	}

	receivedAuth := make(chan string, 1)
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc upstream: %v", err)
	}
	defer grpcListener.Close()
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{*upstreamLeaf},
			NextProtos:   []string{"h2"},
			MinVersion:   tls.VersionTLS12,
		})),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			values := md.Get("authorization")
			if len(values) > 0 {
				receivedAuth <- values[0]
			} else {
				receivedAuth <- ""
			}
			return handler(ctx, req)
		}),
	)
	defer grpcServer.Stop()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	go func() {
		_ = grpcServer.Serve(grpcListener)
	}()

	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 3 * time.Second},
		},
		logger:            zap.NewNop(),
		tlsAuthority:      mitmAuthority,
		upstreamTLSConfig: &tls.Config{RootCAs: upstreamRootPool, NextProtos: []string{"h2"}},
	}

	done := make(chan error, 1)
	go func() {
		conn, acceptErr := proxyListener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()
		req := &adapterRequest{
			Server:   server,
			Compiled: &policy.CompiledPolicy{SandboxID: "sbx_123", TeamID: "team_123", Mode: v1alpha1.NetworkModeAllowAll},
			SrcIP:    "10.0.0.2",
			DestIP:   grpcListener.Addr().(*net.TCPAddr).IP,
			DestPort: grpcListener.Addr().(*net.TCPAddr).Port,
			Host:     "api.example.com",
			Conn:     conn,
			EgressAuth: &egressAuthContext{
				Rule: &policy.CompiledEgressAuthRule{
					Name:     "example-grpc",
					AuthRef:  "example-api",
					Protocol: v1alpha1.EgressAuthProtocolGRPC,
					TLSMode:  v1alpha1.EgressTLSModeTerminateReoriginate,
				},
				Resolved: &egressauth.ResolveResponse{
					AuthRef: "example-api",
					Headers: map[string]string{
						"Authorization": "Bearer grpc-token",
					},
				},
			},
		}
		done <- (&tlsAdapter{}).Handle(req)
	}()

	clientRootPool := x509.NewCertPool()
	if !clientRootPool.AppendCertsFromPEM(mitmCertPEM) {
		t.Fatal("append mitm ca")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientConn, err := grpc.DialContext(
		ctx,
		proxyListener.Addr().String(),
		grpc.WithBlock(),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			ServerName: "api.example.com",
			RootCAs:    clientRootPool,
			NextProtos: []string{"h2"},
			MinVersion: tls.VersionTLS12,
		})),
	)
	if err != nil {
		t.Fatalf("dial grpc proxy: %v", err)
	}

	client := healthpb.NewHealthClient(clientConn)
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("grpc health check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("unexpected health status %v", resp.GetStatus())
	}
	if got := <-receivedAuth; got != "Bearer grpc-token" {
		t.Fatalf("authorization metadata = %q", got)
	}
	if err := clientConn.Close(); err != nil {
		t.Fatalf("close grpc client: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("tls adapter handle: %v", err)
	}
}

func TestTLSAdapterReturnsErrorWhenMITMCAIsMissing(t *testing.T) {
	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: time.Second},
		},
		logger: zap.NewNop(),
	}
	clientConn, upstreamConn := net.Pipe()
	defer clientConn.Close()
	defer upstreamConn.Close()

	err := (&tlsAdapter{}).Handle(&adapterRequest{
		Server:   upstreamConnServer(server),
		Conn:     upstreamConn,
		Host:     "api.example.com",
		DestIP:   net.ParseIP("127.0.0.1"),
		DestPort: 443,
		EgressAuth: &egressAuthContext{
			Rule: &policy.CompiledEgressAuthRule{
				Name:    "example-https",
				AuthRef: "example-api",
				TLSMode: v1alpha1.EgressTLSModeTerminateReoriginate,
			},
			Resolved: &egressauth.ResolveResponse{AuthRef: "example-api"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "authority") {
		t.Fatalf("expected tls authority error, got %v", err)
	}
}
