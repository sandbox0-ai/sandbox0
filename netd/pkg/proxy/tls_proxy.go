package proxy

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
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
	prefixBytes, err := readPrefixBytes(req.Prefix)
	if err != nil {
		return fmt.Errorf("read tls client hello prefix: %w", err)
	}
	prefixReader := bytes.NewReader(prefixBytes)
	if req.EgressAuth != nil && req.EgressAuth.ShouldBypass() {
		return s.relayTCPRequestWithPrefix(req, prefixReader)
	}
	clientConn := newPrefixedConn(req.Conn, prefixBytes)
	if s.tlsAuthority == nil {
		if req.EgressAuth != nil && req.EgressAuth.FailOpen() {
			req.EgressAuth.BypassReason = "tls_intercept_unavailable"
			return s.relayTCPRequestWithPrefix(req, prefixReader)
		}
		return fmt.Errorf("https interception authority is not configured")
	}
	cert, err := s.tlsAuthority.CertificateForHost(req.Host)
	if err != nil {
		if req.EgressAuth != nil && req.EgressAuth.FailOpen() {
			req.EgressAuth.BypassReason = "tls_certificate_issue"
			return s.relayTCPRequestWithPrefix(req, prefixReader)
		}
		return fmt.Errorf("issue downstream tls certificate: %w", err)
	}
	downstreamTLS := tls.Server(clientConn, &tls.Config{
		Certificates: []tls.Certificate{*cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   downstreamTLSNextProtos(req),
	})
	if err := downstreamTLS.Handshake(); err != nil {
		return fmt.Errorf("handshake downstream tls: %w", err)
	}
	defer downstreamTLS.Close()
	if req.EgressAuth != nil && req.EgressAuth.Rule != nil && !egressAuthNeedsHTTPMatch(req) {
		if req.EgressAuth.ResolveError != nil {
			_ = writeHTTPProxyError(downstreamTLS, http.StatusServiceUnavailable, "egress auth resolution failed")
			return fmt.Errorf("resolve egress auth for %q: %w", req.EgressAuth.Rule.AuthRef, req.EgressAuth.ResolveError)
		}
		if req.EgressAuth.Resolved == nil || len(req.EgressAuth.ResolvedHeaders) == 0 {
			_ = writeHTTPProxyError(downstreamTLS, http.StatusServiceUnavailable, "egress auth material unavailable")
			return fmt.Errorf("egress auth material missing for %q", req.EgressAuth.Rule.AuthRef)
		}
	}
	if req.EgressAuth != nil && req.EgressAuth.Rule != nil && req.EgressAuth.Rule.Protocol == v1alpha1.EgressAuthProtocolGRPC && downstreamTLS.ConnectionState().NegotiatedProtocol != "h2" {
		_ = writeHTTPProxyError(downstreamTLS, http.StatusBadRequest, "grpc interception requires h2 alpn")
		return fmt.Errorf("grpc interception requires h2 alpn for host %q", req.Host)
	}
	if shouldProxyHTTP2(req, downstreamTLS.ConnectionState()) {
		return s.proxyHTTP2FromConn(downstreamTLS, req)
	}
	upstream, err := s.dialUpstreamTLS(req)
	if err != nil {
		_ = writeHTTPProxyError(downstreamTLS, http.StatusBadGateway, "upstream tls connection failed")
		return err
	}
	defer upstream.Close()
	return s.proxyHTTPFromConn(downstreamTLS, req, upstream)
}

func (s *Server) proxyTLSStream(req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if req == nil || req.Conn == nil {
		return fmt.Errorf("tls proxy request is nil")
	}
	if req.DestIP == nil || req.DestPort <= 0 {
		return fmt.Errorf("tls proxy destination is missing")
	}
	if req.Host == "" {
		return fmt.Errorf("tls interception requires host")
	}
	prefixBytes, err := readPrefixBytes(req.Prefix)
	if err != nil {
		return fmt.Errorf("read tls client hello prefix: %w", err)
	}
	prefixReader := bytes.NewReader(prefixBytes)
	if req.EgressAuth != nil && req.EgressAuth.ShouldBypass() {
		return s.relayTCPRequestWithPrefix(req, prefixReader)
	}
	clientConn := newPrefixedConn(req.Conn, prefixBytes)
	if s.tlsAuthority == nil {
		if req.EgressAuth != nil && req.EgressAuth.FailOpen() {
			req.EgressAuth.BypassReason = "tls_intercept_unavailable"
			return s.relayTCPRequestWithPrefix(req, prefixReader)
		}
		return fmt.Errorf("tls interception authority is not configured")
	}
	cert, err := s.tlsAuthority.CertificateForHost(req.Host)
	if err != nil {
		if req.EgressAuth != nil && req.EgressAuth.FailOpen() {
			req.EgressAuth.BypassReason = "tls_certificate_issue"
			return s.relayTCPRequestWithPrefix(req, prefixReader)
		}
		return fmt.Errorf("issue downstream tls certificate: %w", err)
	}
	downstreamTLS := tls.Server(clientConn, &tls.Config{
		Certificates: []tls.Certificate{*cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   downstreamTLSNextProtos(req),
	})
	if err := downstreamTLS.Handshake(); err != nil {
		return fmt.Errorf("handshake downstream tls: %w", err)
	}
	defer downstreamTLS.Close()

	upstream, err := s.dialUpstreamTLS(req)
	if err != nil {
		return err
	}
	defer upstream.Close()
	return s.pipeWithReader(downstreamTLS, upstream, downstreamTLS, req.Compiled, req.Audit)
}

func downstreamTLSNextProtos(req *adapterRequest) []string {
	if req != nil && req.EgressAuth != nil && req.EgressAuth.Rule != nil && req.EgressAuth.Rule.Protocol == v1alpha1.EgressAuthProtocolTLS {
		return nil
	}
	if req != nil && req.EgressAuth != nil && req.EgressAuth.Rule != nil && req.EgressAuth.Rule.Protocol == v1alpha1.EgressAuthProtocolGRPC {
		return []string{"h2"}
	}
	return []string{"http/1.1"}
}

func shouldProxyHTTP2(req *adapterRequest, state tls.ConnectionState) bool {
	if req == nil || req.EgressAuth == nil || req.EgressAuth.Rule == nil {
		return false
	}
	if state.NegotiatedProtocol == "h2" {
		return true
	}
	return req.EgressAuth.Rule.Protocol == v1alpha1.EgressAuthProtocolGRPC
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
	if req.EgressAuth != nil && req.EgressAuth.ResolvedTLSClientCertificate != nil {
		cfg.Certificates = []tls.Certificate{req.EgressAuth.ResolvedTLSClientCertificate.Certificate}
		if req.EgressAuth.ResolvedTLSClientCertificate.RootCAs != nil {
			cfg.RootCAs = req.EgressAuth.ResolvedTLSClientCertificate.RootCAs
		}
	}
	if len(cfg.NextProtos) == 0 && (req.EgressAuth == nil || req.EgressAuth.Rule == nil || req.EgressAuth.Rule.Protocol != v1alpha1.EgressAuthProtocolTLS) {
		cfg.NextProtos = []string{"http/1.1"}
	}
	rawConn, err := s.dialTCPUpstreamForRequest(req)
	if err != nil {
		return nil, fmt.Errorf("dial upstream tls: %w", err)
	}
	conn := tls.Client(rawConn, cfg)
	if err := conn.Handshake(); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("handshake upstream tls: %w", err)
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
	s.prepareEgressAuthForHTTPRequest(req, httpReq, "tls")
	if req.EgressAuth != nil && egressAuthNeedsHTTPMatch(req) {
		if err := prepareHTTPHeaderDirectives(req.EgressAuth, "tls", true); err != nil {
			if !req.EgressAuth.ShouldBypass() {
				_ = writeHTTPProxyError(downstream, http.StatusServiceUnavailable, "egress auth resolution failed")
				return fmt.Errorf("resolve egress auth for %q: %w", req.EgressAuth.Rule.AuthRef, err)
			}
		}
	}
	if req.EgressAuth != nil && len(req.EgressAuth.ResolvedHeaders) > 0 {
		injectHTTPHeaders(httpReq, req.EgressAuth.ResolvedHeaders)
	}
	if err := httpReq.Write(upstream); err != nil {
		if counter, ok := upstream.(*countingConn); ok {
			s.recordEgressBytes(req.Compiled, counter.WrittenBytes(), req.Audit)
		}
		return fmt.Errorf("write upstream http request: %w", err)
	}
	if counter, ok := upstream.(*countingConn); ok {
		s.recordEgressBytes(req.Compiled, counter.WrittenBytes(), req.Audit)
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
