package proxy

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Router handles request routing to upstream services
type Router struct {
	targetUrl *url.URL
	logger    *zap.Logger
	timeout   time.Duration
	// requestModifiers are applied before proxying.
	requestModifiers []RequestModifier
	// httpClient is the HTTP client used for proxy requests
	httpClient *http.Client
}

// NewRouter creates a new router
func NewRouter(targetUrl string, logger *zap.Logger, timeout time.Duration, opts ...Option) (*Router, error) {
	tu, err := url.Parse(targetUrl)
	if err != nil {
		return nil, fmt.Errorf("parse target URL: %w", err)
	}

	parsedOpts := collectOptions(opts...)
	return &Router{
		targetUrl:        tu,
		logger:           logger,
		timeout:          timeout,
		requestModifiers: parsedOpts.requestModifiers,
		httpClient:       parsedOpts.httpClient,
	}, nil
}

// ProxyToTarget creates a reverse proxy handler for target service
func (r *Router) ProxyToTarget(c *gin.Context) {
	proxy := r.createReverseProxyDirector(r.targetUrl)
	proxy.ServeHTTP(c.Writer, c.Request)
}

// createReverseProxyDirector creates an httputil.ReverseProxy with proper configuration
func (r *Router) createReverseProxyDirector(target *url.URL) *httputil.ReverseProxy {
	director := func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host

		applyRequestModifiers(req, r.requestModifiers)

		// Preserve the original path (don't rewrite it here)
		// Path rewriting should be done by specific handlers if needed

		// Forward auth headers and request ID
		if req.Header.Get("X-Request-ID") == "" {
			if reqID := req.Context().Value("request_id"); reqID != nil {
				req.Header.Set("X-Request-ID", reqID.(string))
			}
		}

		// Forward auth context
		if authCtx := req.Context().Value("auth_context"); authCtx != nil {
			// The upstream service can validate this header
			// In production, use mutual TLS or signed tokens
			req.Header.Set("X-Team-ID", req.Header.Get("X-Team-ID"))
		}

		r.logger.Debug("Proxying request",
			zap.String("method", req.Method),
			zap.String("target", req.URL.String()),
		)
	}

	// Use custom HTTP client's transport if provided, otherwise use default transport
	var transport http.RoundTripper
	if r.httpClient != nil && r.httpClient.Transport != nil {
		transport = r.httpClient.Transport
	} else {
		transport = &http.Transport{
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		}
	}

	proxy := &httputil.ReverseProxy{
		Director:  director,
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
			r.logger.Error("Proxy error",
				zap.String("target", req.URL.String()),
				zap.Error(err),
			)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(`{"error": "upstream service unavailable"}`))
		},
	}

	return proxy
}
