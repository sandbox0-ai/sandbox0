package proxy

import (
	"context"
	"errors"
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
	// trustForwardedHeader preserves forwarding headers from an authenticated
	// upstream proxy before appending this proxy hop.
	trustForwardedHeader bool
}

// NewRouter creates a new router
func NewRouter(targetUrl string, logger *zap.Logger, timeout time.Duration, opts ...Option) (*Router, error) {
	tu, err := url.Parse(targetUrl)
	if err != nil {
		return nil, fmt.Errorf("parse target URL: %w", err)
	}

	parsedOpts := collectOptions(opts...)
	return &Router{
		targetUrl:            tu,
		logger:               logger,
		timeout:              timeout,
		requestModifiers:     parsedOpts.requestModifiers,
		httpClient:           parsedOpts.httpClient,
		trustForwardedHeader: parsedOpts.trustForwardedHeader,
	}, nil
}

// ProxyToTarget creates a reverse proxy handler for target service
func (r *Router) ProxyToTarget(c *gin.Context) {
	if isWebSocketUpgrade(c.Request) {
		if err := PrepareStreamingProxyResponse(c.Writer, c.Request); err != nil {
			r.logger.Debug("Failed to disable proxy response deadlines", zap.Error(err))
		}
		opts := make([]Option, 0, len(r.requestModifiers))
		for _, mod := range r.requestModifiers {
			opts = append(opts, WithRequestModifier(mod))
		}
		if r.trustForwardedHeader {
			opts = append(opts, WithTrustedForwardedHeaders())
		}
		NewWebSocketProxy(r.logger, opts...).Proxy(r.targetUrl)(c)
		return
	}

	req := c.Request
	if err := PrepareStreamingProxyResponse(c.Writer, req); err != nil {
		r.logger.Debug("Failed to disable proxy response deadlines", zap.Error(err))
	}
	cancel := context.CancelFunc(func() {})
	if r.timeout > 0 {
		req, cancel = ApplyRequestTimeout(c.Request, r.timeout)
		c.Request = req
	}
	defer cancel()

	proxy := r.createReverseProxyDirector(r.targetUrl)
	proxy.ServeHTTP(c.Writer, req)
}

// createReverseProxyDirector creates an httputil.ReverseProxy with proper configuration
func (r *Router) createReverseProxyDirector(target *url.URL) *httputil.ReverseProxy {
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
		Rewrite: func(req *httputil.ProxyRequest) {
			req.Out.URL.Scheme = target.Scheme
			req.Out.URL.Host = target.Host
			req.Out.Host = target.Host
			setForwardedHeaders(req.Out, req.In, r.trustForwardedHeader)
			applyRequestModifiers(req.Out, r.requestModifiers)

			r.logger.Debug("Proxying request",
				zap.String("method", req.Out.Method),
				zap.String("target", req.Out.URL.String()),
			)
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
			status := http.StatusBadGateway
			body := `{"error": "upstream service unavailable"}`
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				status = http.StatusRequestEntityTooLarge
				body = `{"error": "request body too large"}`
			} else if IsTimeoutError(err) {
				status = http.StatusGatewayTimeout
				body = `{"error": "upstream request timed out"}`
			}
			r.logger.Error("Proxy error",
				zap.String("target", req.URL.String()),
				zap.Int("status", status),
				zap.Error(err),
			)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		},
	}

	return proxy
}
