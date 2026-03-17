package proxy

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

func (s *Server) proxyHTTPSRequest(req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if req == nil || req.Conn == nil {
		return fmt.Errorf("https proxy request is nil")
	}
	if req.DestIP == nil || req.DestPort <= 0 {
		return fmt.Errorf("https proxy destination is missing")
	}
	if req.Host == "" {
		return fmt.Errorf("https interception requires host")
	}
	if s.tlsAuthority == nil {
		return fmt.Errorf("https interception authority is not configured")
	}
	prefixBytes, err := readPrefixBytes(req.Prefix)
	if err != nil {
		return fmt.Errorf("read tls client hello prefix: %w", err)
	}
	clientConn := newPrefixedConn(req.Conn, prefixBytes)
	cert, err := s.tlsAuthority.CertificateForHost(req.Host)
	if err != nil {
		return fmt.Errorf("issue downstream tls certificate: %w", err)
	}
	downstreamTLS := tls.Server(clientConn, &tls.Config{
		Certificates: []tls.Certificate{*cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"http/1.1"},
	})
	if err := downstreamTLS.Handshake(); err != nil {
		return fmt.Errorf("handshake downstream tls: %w", err)
	}
	defer downstreamTLS.Close()
	if req.EgressAuth != nil && req.EgressAuth.Rule != nil {
		if req.EgressAuth.ResolveError != nil {
			_ = writeHTTPProxyError(downstreamTLS, http.StatusServiceUnavailable, "egress auth resolution failed")
			return fmt.Errorf("resolve egress auth for %q: %w", req.EgressAuth.Rule.AuthRef, req.EgressAuth.ResolveError)
		}
		if req.EgressAuth.Resolved == nil {
			_ = writeHTTPProxyError(downstreamTLS, http.StatusServiceUnavailable, "egress auth material unavailable")
			return fmt.Errorf("egress auth material missing for %q", req.EgressAuth.Rule.AuthRef)
		}
	}
	upstream, err := s.dialUpstreamTLS(req)
	if err != nil {
		_ = writeHTTPProxyError(downstreamTLS, http.StatusBadGateway, "upstream tls connection failed")
		return err
	}
	defer upstream.Close()
	return s.proxyHTTPFromConn(downstreamTLS, req, upstream)
}

func (s *Server) dialUpstreamTLS(req *adapterRequest) (net.Conn, error) {
	if s == nil || req == nil || req.DestIP == nil || req.DestPort <= 0 {
		return nil, fmt.Errorf("upstream tls request is incomplete")
	}
	if req.Host == "" {
		return nil, fmt.Errorf("upstream tls server name is required")
	}
	cfg := cloneTLSConfig(s.upstreamTLSConfig)
	if cfg.ServerName == "" {
		cfg.ServerName = req.Host
	}
	if len(cfg.NextProtos) == 0 {
		cfg.NextProtos = []string{"http/1.1"}
	}
	dialer := &net.Dialer{Timeout: s.cfg.ProxyUpstreamTimeout.Duration}
	conn, err := tls.DialWithDialer(dialer, "tcp", net.JoinHostPort(req.DestIP.String(), fmt.Sprintf("%d", req.DestPort)), cfg)
	if err != nil {
		return nil, fmt.Errorf("dial upstream tls: %w", err)
	}
	return &countingConn{Conn: conn}, nil
}

func (s *Server) proxyHTTPFromConn(downstream net.Conn, req *adapterRequest, upstream net.Conn) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if downstream == nil || upstream == nil {
		return fmt.Errorf("http proxy connections are required")
	}
	reader := bufio.NewReader(downstream)
	httpReq, err := http.ReadRequest(reader)
	if err != nil {
		return fmt.Errorf("read downstream http request: %w", err)
	}
	httpReq.RequestURI = ""
	httpReq.Close = true
	httpReq.Header.Set("Connection", "close")
	if httpReq.Host == "" && req.Host != "" {
		httpReq.Host = req.Host
	}
	if req.EgressAuth != nil && req.EgressAuth.Resolved != nil {
		injectHTTPHeaders(httpReq, req.EgressAuth.Resolved.Headers)
	}
	if err := httpReq.Write(upstream); err != nil {
		if counter, ok := upstream.(*countingConn); ok {
			s.recordEgressBytes(req.Compiled, counter.written, req.Audit)
		}
		return fmt.Errorf("write upstream http request: %w", err)
	}
	if counter, ok := upstream.(*countingConn); ok {
		s.recordEgressBytes(req.Compiled, counter.written, req.Audit)
	}
	clientCounter := &countingWriter{writer: downstream}
	n, err := io.Copy(clientCounter, upstream)
	s.recordIngressBytes(req.Compiled, n, req.Audit)
	return normalizeRelayError(err)
}

func readPrefixBytes(prefix io.Reader) ([]byte, error) {
	if prefix == nil {
		return nil, nil
	}
	return io.ReadAll(prefix)
}

func tlsTerminationRequired(req *adapterRequest) bool {
	if req == nil || req.EgressAuth == nil || req.EgressAuth.Rule == nil {
		return false
	}
	return req.EgressAuth.Rule.TLSMode == "terminate-reoriginate"
}

func handshakeTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 5 * time.Second
	}
	return timeout
}
