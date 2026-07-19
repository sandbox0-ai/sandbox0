package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

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

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := s.handleHTTP2ProxyRequest(w, r, req); err != nil {
			if errors.Is(err, errProtocolPolicyDenied) {
				return
			}
			s.logger.Warn("HTTP/2 proxy request failed",
				zap.Error(err),
				zap.String("host", req.Host),
				zap.Int("port", req.DestPort),
			)
			http.Error(w, "upstream http2 request failed", http.StatusBadGateway)
		}
	})

	server := &http2.Server{}
	serveCtx := req.Context
	if serveCtx == nil {
		serveCtx = context.TODO()
	}
	server.ServeConn(downstream, &http2.ServeConnOpts{
		Context: serveCtx,
		Handler: handler,
	})
	return nil
}

func (s *Server) handleHTTP2ProxyRequest(w http.ResponseWriter, downstreamReq *http.Request, req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if w == nil || downstreamReq == nil || req == nil {
		return fmt.Errorf("http2 proxy request is incomplete")
	}
	transport := s.newHTTP2Transport(req)
	defer transport.CloseIdleConnections()

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

	resp, err := transport.RoundTrip(upstreamReq)
	if err != nil {
		return fmt.Errorf("round trip upstream http2 request: %w", err)
	}
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

func (s *Server) newHTTP2Transport(req *adapterRequest) *http2.Transport {
	cfg := cloneTLSConfig(s.upstreamTLSConfig)
	if cfg.ServerName == "" {
		cfg.ServerName = req.Host
	}
	cfg.NextProtos = []string{"h2"}
	return &http2.Transport{
		TLSClientConfig: cfg,
		DialTLSContext: func(ctx context.Context, network, addr string, tlsCfg *tls.Config) (net.Conn, error) {
			_ = network
			_ = addr
			_ = tlsCfg
			rawConn, err := s.dialTCPUpstreamForRequest(req)
			if err != nil {
				return nil, fmt.Errorf("dial upstream http2 tls: %w", err)
			}
			onWireConn := s.wrapUpstreamOnWireConn(ctx, rawConn, req.Compiled, req.Audit)
			conn := tls.Client(onWireConn, cfg)
			if err := conn.HandshakeContext(ctx); err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("handshake upstream http2 tls: %w", err)
			}
			return conn, nil
		},
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
