package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// TargetService identifies the target service for routing
type TargetService string

const (
	TargetManager      TargetService = "manager"
	TargetProcd        TargetService = "procd"
	TargetStorageProxy TargetService = "storage_proxy"
)

// Router handles request routing to upstream services
type Router struct {
	managerURL      *url.URL
	storageProxyURL *url.URL
	logger          *zap.Logger
	timeout         time.Duration
}

// NewRouter creates a new router
func NewRouter(managerURL, storageProxyURL string, logger *zap.Logger, timeout time.Duration) (*Router, error) {
	mgrURL, err := url.Parse(managerURL)
	if err != nil {
		return nil, fmt.Errorf("parse manager URL: %w", err)
	}

	spURL, err := url.Parse(storageProxyURL)
	if err != nil {
		return nil, fmt.Errorf("parse storage proxy URL: %w", err)
	}

	return &Router{
		managerURL:      mgrURL,
		storageProxyURL: spURL,
		logger:          logger,
		timeout:         timeout,
	}, nil
}

// ProxyToManager creates a reverse proxy handler for Manager service
func (r *Router) ProxyToManager() gin.HandlerFunc {
	return r.createReverseProxy(r.managerURL)
}

// ProxyToStorageProxy creates a reverse proxy handler for Storage Proxy service
func (r *Router) ProxyToStorageProxy() gin.HandlerFunc {
	return r.createReverseProxy(r.storageProxyURL)
}

// ProxyToProcd is deprecated - use handlers with manager client instead
// This method is no longer used as procd routing is handled by handlers
// that call the manager service to resolve sandbox addresses

// createReverseProxy creates a gin handler that proxies to the given URL
func (r *Router) createReverseProxy(target *url.URL) gin.HandlerFunc {
	proxy := r.createReverseProxyDirector(target)

	return func(c *gin.Context) {
		proxy.ServeHTTP(c.Writer, c.Request)
	}
}

// createReverseProxyDirector creates an httputil.ReverseProxy with proper configuration
func (r *Router) createReverseProxyDirector(target *url.URL) *httputil.ReverseProxy {
	director := func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host

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

	proxy := &httputil.ReverseProxy{
		Director: director,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
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

// RewritePath returns a middleware that rewrites the request path
func RewritePath(from, to string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.URL.Path = strings.Replace(c.Request.URL.Path, from, to, 1)
		c.Next()
	}
}

// ForwardRequest forwards a request to an upstream service and returns the response
func (r *Router) ForwardRequest(ctx context.Context, method, targetURL string, body io.Reader, headers map[string]string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, targetURL, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{
		Timeout: r.timeout,
	}

	return client.Do(req)
}
