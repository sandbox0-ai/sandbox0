package http

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

const (
	previewProxyPermission = "sandbox.preview.proxy"
	previewCookieName      = "__Host-sandbox0-preview"
	previewOriginHeader    = "X-Sandbox0-Preview-Origin"
)

var previewLoopbackTransport = &http.Transport{
	Proxy:               nil,
	DialContext:         dialPreviewLoopback,
	ForceAttemptHTTP2:   false,
	MaxIdleConns:        32,
	MaxIdleConnsPerHost: 16,
	IdleConnTimeout:     90 * time.Second,
	TLSClientConfig: &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, // Preview targets are fixed to sandbox loopback.
	},
}

func dialPreviewLoopback(ctx context.Context, _ string, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		return nil, fmt.Errorf("invalid preview loopback address")
	}
	dialer := &net.Dialer{}
	connection, ipv4Err := dialer.DialContext(ctx, "tcp4", net.JoinHostPort("127.0.0.1", port))
	if ipv4Err == nil {
		return connection, nil
	}
	connection, ipv6Err := dialer.DialContext(ctx, "tcp6", net.JoinHostPort("::1", port))
	if ipv6Err == nil {
		return connection, nil
	}
	return nil, fmt.Errorf("dial preview loopback: IPv4: %v; IPv6: %w", ipv4Err, ipv6Err)
}

func (s *Server) previewProxyHandler(w http.ResponseWriter, r *http.Request) {
	if !internalauth.HasPermission(r.Context(), previewProxyPermission) {
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "preview proxy permission is required")
		return
	}
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil || strings.TrimSpace(claims.SandboxID) == "" {
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "sandbox-bound preview authorization is required")
		return
	}
	vars := mux.Vars(r)
	port, err := strconv.Atoi(vars["port"])
	if err != nil || port <= 0 || port > 65535 {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "invalid preview port")
		return
	}
	if s.cfg != nil && port == s.cfg.HTTPPort {
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "reserved port is not previewable")
		return
	}
	scheme := vars["scheme"]
	target := &url.URL{Scheme: scheme, Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}
	externalOrigin := validPreviewOrigin(r.Header.Get(previewOriginHeader))
	path := "/" + strings.TrimPrefix(vars["path"], "/")
	if vars["path"] == "" {
		path = "/"
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	baseDirector := proxy.Director
	proxy.Director = func(request *http.Request) {
		baseDirector(request)
		request.URL.Path = path
		request.URL.RawPath = ""
		request.Host = "localhost:" + strconv.Itoa(port)
		request.Header.Del(internalauth.DefaultTokenHeader)
		request.Header.Del(internalauth.TeamIDHeader)
		request.Header.Del(previewOriginHeader)
		request.Header.Del("X-Sandbox-ID")
		request.Header.Del("X-Exposure-Port")
		if externalOrigin != nil {
			request.Header.Set("X-Forwarded-Host", externalOrigin.Host)
			request.Header.Set("X-Forwarded-Proto", externalOrigin.Scheme)
		}
	}
	proxy.Transport = previewLoopbackTransport
	proxy.FlushInterval = -1
	proxy.ModifyResponse = func(response *http.Response) error {
		rewritePreviewLocation(response, externalOrigin, port)
		rewritePreviewCookies(response)
		response.Header.Del("X-Frame-Options")
		rewriteFrameAncestors(response.Header)
		return nil
	}
	proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, proxyErr error) {
		s.logger.Debug("Preview loopback proxy failed",
			zap.String("sandbox_id", claims.SandboxID),
			zap.Int("port", port),
			zap.Error(proxyErr),
		)
		_ = spec.WriteError(writer, http.StatusBadGateway, spec.CodeUnavailable, "preview server is not accepting connections")
	}
	proxy.ServeHTTP(w, r)
}

func validPreviewOrigin(raw string) *url.URL {
	origin, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || origin == nil || origin.Host == "" || (origin.Scheme != "http" && origin.Scheme != "https") {
		return nil
	}
	origin.Path = ""
	origin.RawPath = ""
	origin.RawQuery = ""
	origin.Fragment = ""
	return origin
}

func rewritePreviewLocation(response *http.Response, externalOrigin *url.URL, port int) {
	if externalOrigin == nil {
		return
	}
	raw := response.Header.Get("Location")
	if raw == "" {
		return
	}
	location, err := url.Parse(raw)
	if err != nil || !location.IsAbs() {
		return
	}
	host := location.Hostname()
	locationPort, _ := strconv.Atoi(location.Port())
	if !strings.EqualFold(host, "localhost") && net.ParseIP(host) == nil {
		return
	}
	if ip := net.ParseIP(host); ip != nil && !ip.IsLoopback() {
		return
	}
	if locationPort != 0 && locationPort != port {
		return
	}
	location.Scheme = externalOrigin.Scheme
	location.Host = externalOrigin.Host
	response.Header.Set("Location", location.String())
}

func rewritePreviewCookies(response *http.Response) {
	cookies := response.Cookies()
	if len(cookies) == 0 {
		return
	}
	response.Header.Del("Set-Cookie")
	for _, cookie := range cookies {
		if cookie.Name == previewCookieName {
			continue
		}
		cookie.Domain = ""
		cookie.Raw = ""
		cookie.Secure = true
		cookie.SameSite = http.SameSiteNoneMode
		cookie.Partitioned = true
		response.Header.Add("Set-Cookie", cookie.String())
	}
}

func rewriteFrameAncestors(header http.Header) {
	values := header.Values("Content-Security-Policy")
	if len(values) == 0 {
		return
	}
	header.Del("Content-Security-Policy")
	for _, value := range values {
		directives := strings.Split(value, ";")
		kept := directives[:0]
		for _, directive := range directives {
			fields := strings.Fields(strings.TrimSpace(directive))
			if len(fields) == 0 {
				continue
			}
			if strings.EqualFold(fields[0], "frame-ancestors") {
				continue
			}
			kept = append(kept, strings.TrimSpace(directive))
		}
		if len(kept) > 0 {
			header.Add("Content-Security-Policy", strings.Join(kept, "; "))
		}
	}
}
