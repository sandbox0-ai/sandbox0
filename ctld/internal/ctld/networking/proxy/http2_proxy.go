package proxy

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

type countingTLSConn struct {
	*tls.Conn
	read           int64
	written        int64
	limiter        *bandwidthLimiter
	compiled       *policy.CompiledPolicy
	readDirection  bandwidthDirection
	writeDirection bandwidthDirection
}

func (c *countingTLSConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.limiter != nil && c.readDirection != "" {
		if waitErr := c.limiter.wait(c.compiled, c.readDirection, n); waitErr != nil && err == nil {
			err = waitErr
		}
	}
	atomic.AddInt64(&c.read, int64(n))
	return n, err
}

func (c *countingTLSConn) Write(p []byte) (int, error) {
	if len(p) > 0 && c.limiter != nil {
		if err := c.limiter.wait(c.compiled, c.writeDirection, len(p)); err != nil {
			return 0, err
		}
	}
	n, err := c.Conn.Write(p)
	atomic.AddInt64(&c.written, int64(n))
	return n, err
}

func (c *countingTLSConn) ReadBytes() int64 {
	if c == nil {
		return 0
	}
	return atomic.LoadInt64(&c.read)
}

func (c *countingTLSConn) WrittenBytes() int64 {
	if c == nil {
		return 0
	}
	return atomic.LoadInt64(&c.written)
}

type flushWriter struct {
	http.ResponseWriter
	flusher http.Flusher
}

func newFlushWriter(w http.ResponseWriter) io.Writer {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return w
	}
	return &flushWriter{
		ResponseWriter: w,
		flusher:        flusher,
	}
}

func (w *flushWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if err == nil {
		w.flusher.Flush()
	}
	return n, err
}

func (s *Server) proxyHTTP2FromConn(downstream *tls.Conn, req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if downstream == nil || req == nil {
		return fmt.Errorf("http2 proxy requires downstream connection and request")
	}

	downstreamCounter := &countingTLSConn{
		Conn:     downstream,
		limiter:  s.bandwidthLimiter,
		compiled: req.Compiled,
		// Egress is charged when the upstream connection is written. Leaving
		// reads unclassified avoids charging the same payload on both legs.
		readDirection:  "",
		writeDirection: bandwidthIngress,
	}
	var upstreamCounter *countingConn
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := s.handleHTTP2ProxyRequest(w, r, req, &upstreamCounter); err != nil {
			if errors.Is(err, errProtocolPolicyDenied) {
				return
			}
			s.logger.Warn("HTTP/2 downstream proxy request failed",
				zap.Error(err),
				zap.String("host", req.Host),
				zap.Int("port", req.DestPort),
				zap.String("sandbox_id", compiledSandboxID(req.Compiled)),
			)
			http.Error(w, "upstream http request failed", http.StatusBadGateway)
		}
	})

	server := &http2.Server{}
	server.ServeConn(downstreamCounter, &http2.ServeConnOpts{
		Context: context.Background(),
		Handler: handler,
	})

	if upstreamCounter != nil {
		s.recordEgressBytes(req.Compiled, upstreamCounter.WrittenBytes(), req.Audit)
		s.recordIngressBytes(req.Compiled, upstreamCounter.ReadBytes(), req.Audit)
	}
	return nil
}

func (s *Server) handleHTTP2ProxyRequest(w http.ResponseWriter, downstreamReq *http.Request, req *adapterRequest, upstreamCounter **countingConn) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if w == nil || downstreamReq == nil || req == nil {
		return fmt.Errorf("http2 proxy request is incomplete")
	}

	requestScoped := *req
	if req.EgressAuth != nil {
		copiedAuth := *req.EgressAuth
		requestScoped.EgressAuth = &copiedAuth
	}
	upstreamReq, err := buildHTTP2UpstreamRequest(downstreamReq, &requestScoped)
	if err != nil {
		return err
	}
	s.prepareEgressAuthForHTTPRequest(&requestScoped, downstreamReq, "tls")
	if requestScoped.EgressAuth != nil && egressAuthNeedsHTTPMatch(&requestScoped) {
		if err := prepareHTTPHeaderDirectives(requestScoped.EgressAuth, "tls", true); err != nil {
			if !requestScoped.EgressAuth.ShouldBypass() {
				return fmt.Errorf("prepare http2 egress auth for %q: %w", requestScoped.EgressAuth.Rule.AuthRef, err)
			}
		}
	}
	if requestScoped.EgressAuth != nil && len(requestScoped.EgressAuth.ResolvedHeaders) > 0 {
		injectHTTPHeaders(upstreamReq, requestScoped.EgressAuth.ResolvedHeaders)
	}
	if err := applyResolvedHTTPPlaceholderSubstitutions(requestScoped.EgressAuth, "tls", upstreamReq); err != nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("egress auth placeholder substitution failed"))
		return fmt.Errorf("apply http2 egress auth placeholder substitutions for %q: %w", egressAuthRuleRef(requestScoped.EgressAuth), err)
	}
	if err := s.enforceHTTPPolicyForHTTPRequest(&requestScoped, upstreamReq, func(status int, body []byte) error {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(status)
		_, writeErr := w.Write(body)
		return writeErr
	}); err != nil {
		return err
	}
	if err := s.enforceMCPPolicyForHTTPRequest(&requestScoped, upstreamReq, func(status int, body []byte) error {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, writeErr := w.Write(body)
		return writeErr
	}); err != nil {
		return err
	}

	resp, closeUpstream, err := s.roundTripHTTP2DownstreamRequest(upstreamReq, &requestScoped, upstreamCounter)
	if err != nil {
		return fmt.Errorf("round trip upstream http request: %w", err)
	}
	defer closeUpstream()
	defer resp.Body.Close()

	declareResponseTrailers(w, resp.Trailer)
	copyHTTPHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, copyErr := io.Copy(newFlushWriter(w), resp.Body)
	writeHTTPResponseTrailers(w, resp.Trailer)
	if copyErr != nil {
		return fmt.Errorf("copy upstream http2 response body: %w", copyErr)
	}
	return nil
}

// roundTripHTTP2DownstreamRequest negotiates the upstream HTTP version
// independently from the downstream HTTP/2 connection. Ordinary HTTPS can
// bridge to an HTTP/1.1-only origin, while gRPC remains HTTP/2-only.
func (s *Server) roundTripHTTP2DownstreamRequest(
	httpReq *http.Request,
	req *adapterRequest,
	upstreamCounter **countingConn,
) (*http.Response, func(), error) {
	if s == nil || httpReq == nil || req == nil {
		return nil, nil, fmt.Errorf("upstream http request is incomplete")
	}
	cfg := cloneTLSConfig(s.upstreamTLSConfig)
	if cfg.ServerName == "" {
		cfg.ServerName = req.Host
	}
	requiresHTTP2 := req.EgressAuth != nil &&
		req.EgressAuth.Rule != nil &&
		req.EgressAuth.Rule.Protocol == v1alpha1.EgressAuthProtocolGRPC
	if requiresHTTP2 {
		cfg.NextProtos = []string{"h2"}
	} else {
		cfg.NextProtos = []string{"h2", "http/1.1"}
	}

	rawConn, err := s.dialTCPUpstreamForRequest(req)
	if err != nil {
		return nil, nil, fmt.Errorf("dial upstream tls: %w", err)
	}
	tlsConn := tls.Client(rawConn, cfg)
	if err := tlsConn.HandshakeContext(httpReq.Context()); err != nil {
		_ = rawConn.Close()
		return nil, nil, fmt.Errorf("handshake upstream tls: %w", err)
	}
	wrapped := &countingConn{
		Conn:     tlsConn,
		limiter:  s.bandwidthLimiter,
		compiled: req.Compiled,
		// Ingress is charged when the downstream connection is written.
		// Reads remain counted for metering only.
		readDirection:  "",
		writeDirection: bandwidthEgress,
	}
	if upstreamCounter != nil && *upstreamCounter == nil {
		*upstreamCounter = wrapped
	}

	switch negotiated := tlsConn.ConnectionState().NegotiatedProtocol; negotiated {
	case "h2":
		transport := &http2.Transport{}
		clientConn, err := transport.NewClientConn(wrapped)
		if err != nil {
			_ = wrapped.Close()
			return nil, nil, fmt.Errorf("initialize upstream http2 connection: %w", err)
		}
		resp, err := clientConn.RoundTrip(httpReq)
		if err != nil {
			_ = clientConn.Close()
			return nil, nil, fmt.Errorf("round trip upstream http2 request: %w", err)
		}
		return resp, func() { _ = clientConn.Close() }, nil
	case "", "http/1.1":
		if requiresHTTP2 {
			_ = wrapped.Close()
			return nil, nil, fmt.Errorf("grpc upstream did not negotiate h2")
		}
		httpReq.Close = true
		if err := httpReq.Write(wrapped); err != nil {
			_ = wrapped.Close()
			return nil, nil, fmt.Errorf("write upstream http1 request: %w", err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(wrapped), httpReq)
		if err != nil {
			_ = wrapped.Close()
			return nil, nil, fmt.Errorf("read upstream http1 response: %w", err)
		}
		return resp, func() { _ = wrapped.Close() }, nil
	default:
		_ = wrapped.Close()
		return nil, nil, fmt.Errorf("upstream negotiated unsupported ALPN protocol %q", negotiated)
	}
}

func buildHTTP2UpstreamRequest(downstreamReq *http.Request, req *adapterRequest) (*http.Request, error) {
	if downstreamReq == nil || req == nil {
		return nil, fmt.Errorf("http2 upstream request is incomplete")
	}
	upstreamReq := downstreamReq.Clone(downstreamReq.Context())
	upstreamReq.RequestURI = ""
	upstreamReq.URL = &url.URL{
		Scheme:   "https",
		Host:     authorityForRequest(req.Host, req.DestPort),
		Path:     downstreamReq.URL.Path,
		RawPath:  downstreamReq.URL.RawPath,
		RawQuery: downstreamReq.URL.RawQuery,
		Fragment: downstreamReq.URL.Fragment,
	}
	upstreamReq.Host = authorityForRequest(req.Host, req.DestPort)
	return upstreamReq, nil
}

func authorityForRequest(host string, port int) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if port <= 0 || port == 443 || strings.Contains(host, ":") {
		return host
	}
	return net.JoinHostPort(host, fmt.Sprintf("%d", port))
}

func copyHTTPHeader(dst, src http.Header) {
	if dst == nil || src == nil {
		return
	}
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func declareResponseTrailers(w http.ResponseWriter, trailers http.Header) {
	if w == nil || len(trailers) == 0 {
		return
	}
	for key := range trailers {
		w.Header().Add("Trailer", key)
	}
}

func writeHTTPResponseTrailers(w http.ResponseWriter, trailers http.Header) {
	if w == nil || len(trailers) == 0 {
		return
	}
	for key, values := range trailers {
		trailerKey := http.TrailerPrefix + key
		for _, value := range values {
			w.Header().Add(trailerKey, value)
		}
	}
}
