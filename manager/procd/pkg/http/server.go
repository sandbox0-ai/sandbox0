// Package http provides the HTTP server for Procd.
package http

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/http/handlers"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/volume"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// TokenProvider provides internal token for gRPC authentication.
// It is thread-safe and can be shared between HTTP server and volume manager.
type TokenProvider struct {
	mu    sync.RWMutex
	token string
}

// NewTokenProvider creates a new token provider.
func NewTokenProvider() *TokenProvider {
	return &TokenProvider{}
}

// GetInternalToken returns the current internal token.
func (p *TokenProvider) GetInternalToken() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.token
}

// SetInternalToken sets the internal token.
func (p *TokenProvider) SetInternalToken(token string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.token = token
}

// Server is the Procd HTTP server.
type Server struct {
	router      *mux.Router
	httpServer  *http.Server
	logger      *zap.Logger
	cfg         *config.ProcdConfig
	obsProvider *observability.Provider

	// Managers
	contextManager *ctxpkg.Manager
	volumeManager  *volume.Manager
	fileManager    *file.Manager

	// Token provider for storage-proxy communication
	tokenProvider *TokenProvider

	// Internal auth validator
	authValidator *internalauth.Validator

	// Webhook dispatcher
	webhookDispatcher *webhook.Dispatcher

	readyChecker func() error
}

// NewServer creates a new HTTP server.
func NewServer(
	cfg *config.ProcdConfig,
	contextManager *ctxpkg.Manager,
	volumeManager *volume.Manager,
	fileManager *file.Manager,
	authValidator *internalauth.Validator,
	tokenProvider *TokenProvider,
	webhookDispatcher *webhook.Dispatcher,
	logger *zap.Logger,
	obsProvider *observability.Provider,
	readyChecker func() error,
) *Server {
	s := &Server{
		router:            mux.NewRouter(),
		cfg:               cfg,
		contextManager:    contextManager,
		volumeManager:     volumeManager,
		fileManager:       fileManager,
		authValidator:     authValidator,
		tokenProvider:     tokenProvider,
		webhookDispatcher: webhookDispatcher,
		logger:            logger,
		obsProvider:       obsProvider,
		readyChecker:      readyChecker,
	}

	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	// Global middleware (applied to all routes)
	s.router.Use(httpobs.ServerMiddleware(httpobs.ServerConfig{
		Tracer: s.obsProvider.Tracer(),
	}))
	s.router.Use(s.loggingMiddleware)
	s.router.Use(s.recoveryMiddleware)

	// Health check endpoints (no auth required)
	s.router.HandleFunc("/healthz", s.healthHandler).Methods("GET")
	s.router.HandleFunc("/readyz", s.readyHandler).Methods("GET")

	// Local-only API (localhost access only, no auth)
	local := s.router.PathPrefix("/api/v1").Subrouter()
	local.Use(s.localhostOnlyMiddleware)

	webhookHandler := handlers.NewWebhookHandler(s.webhookDispatcher)
	local.HandleFunc("/webhook/publish", webhookHandler.Publish).Methods("POST")

	// API v1 (auth required if enabled)
	api := s.router.PathPrefix("/api/v1").Subrouter()

	// Apply auth middleware to all API routes
	api.Use(s.authMiddleware)
	api.Use(s.internalTokenMiddleware)

	// Sandbox-level handlers (pause/resume all processes)
	sandboxHandler := handlers.NewSandboxHandler(s.contextManager, s.webhookDispatcher, s.logger)
	api.HandleFunc("/sandbox/pause", sandboxHandler.Pause).Methods("POST")
	api.HandleFunc("/sandbox/resume", sandboxHandler.Resume).Methods("POST")
	api.HandleFunc("/sandbox/stats", sandboxHandler.Stats).Methods("GET")

	// Context/Process handlers
	contextHandler := handlers.NewContextHandler(s.contextManager, s.logger)
	api.HandleFunc("/contexts", contextHandler.List).Methods("GET")
	api.HandleFunc("/contexts", contextHandler.Create).Methods("POST")
	api.HandleFunc("/contexts/{id}", contextHandler.Get).Methods("GET")
	api.HandleFunc("/contexts/{id}", contextHandler.Delete).Methods("DELETE")
	api.HandleFunc("/contexts/{id}/restart", contextHandler.Restart).Methods("POST")
	api.HandleFunc("/contexts/{id}/input", contextHandler.WriteInput).Methods("POST")
	api.HandleFunc("/contexts/{id}/exec", contextHandler.Exec).Methods("POST")
	api.HandleFunc("/contexts/{id}/resize", contextHandler.ResizePTY).Methods("POST")
	api.HandleFunc("/contexts/{id}/signal", contextHandler.SendSignal).Methods("POST")
	api.HandleFunc("/contexts/{id}/stats", contextHandler.Stats).Methods("GET")
	api.HandleFunc("/contexts/{id}/ws", contextHandler.WebSocket).Methods("GET")

	// Initialize handler
	initializeHandler := handlers.NewInitializeHandler(s.webhookDispatcher, s.fileManager, s.volumeManager, s.cfg.HTTPPort, s.logger)
	api.HandleFunc("/initialize", initializeHandler.Initialize).Methods("POST")

	// SandboxVolume handlers
	volumeHandler := handlers.NewVolumeHandler(s.volumeManager, s.logger)
	volumeRouter := api.PathPrefix("/sandboxvolumes").Subrouter()
	volumeRouter.Use(s.storageProxyUpstreamMiddleware)
	volumeRouter.HandleFunc("/mount", volumeHandler.Mount).Methods("POST")
	volumeRouter.HandleFunc("/unmount", volumeHandler.Unmount).Methods("POST")
	volumeRouter.HandleFunc("/status", volumeHandler.Status).Methods("GET")

	// File handlers
	fileHandler := handlers.NewFileHandler(s.fileManager, s.logger)
	api.HandleFunc("/files/watch", fileHandler.Watch).Methods("GET")
	api.HandleFunc("/files/move", fileHandler.Move).Methods("POST")
	api.HandleFunc("/files", fileHandler.Handle).Methods("GET", "POST", "DELETE")
	api.HandleFunc("/files/stat", fileHandler.Stat).Methods("GET")
	api.HandleFunc("/files/list", fileHandler.List).Methods("GET")
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.cfg.HTTPPort)

	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // Disabled for SSE streaming support
		IdleTimeout:  120 * time.Second,
	}

	s.logger.Info("Starting HTTP server",
		zap.String("addr", addr),
	)

	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	if s.readyChecker != nil {
		if err := s.readyChecker(); err != nil {
			_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
			return
		}
	}
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip logging for health check and readiness check
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()

		// Wrap response writer to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		fields := []zap.Field{
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", wrapped.statusCode),
			zap.Duration("duration", time.Since(start)),
		}

		spanCtx := trace.SpanFromContext(r.Context()).SpanContext()
		if spanCtx.IsValid() {
			fields = append(fields,
				zap.String("trace_id", spanCtx.TraceID().String()),
				zap.String("span_id", spanCtx.SpanID().String()),
			)
		}

		s.logger.Info("HTTP request", fields...)
	})
}

func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				fields := []zap.Field{
					zap.Any("error", err),
					zap.String("path", r.URL.Path),
				}
				spanCtx := trace.SpanFromContext(r.Context()).SpanContext()
				if spanCtx.IsValid() {
					fields = append(fields,
						zap.String("trace_id", spanCtx.TraceID().String()),
						zap.String("span_id", spanCtx.SpanID().String()),
					)
				}
				s.logger.Error("Panic recovered", fields...)
				_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) localhostOnlyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isLoopbackAddress(r.RemoteAddr) {
			_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "forbidden")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isLoopbackAddress(addr string) bool {
	if addr == "" {
		return false
	}
	host := addr
	if strings.HasPrefix(host, "[") {
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
	} else if strings.Count(host, ":") > 0 {
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}

// internalTokenMiddleware extracts and stores the internal token from request headers.
// This token is used for authenticating requests to storage-proxy gRPC service.
func (s *Server) internalTokenMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract token from X-Token-For-Procd header
		token := r.Header.Get("X-Token-For-Procd")
		if token == "" {
			s.logger.Warn("Missing internal token",
				zap.String("path", r.URL.Path),
			)
			_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "missing internal token")
			return
		}

		if s.tokenProvider != nil {
			s.tokenProvider.SetInternalToken(token)
		}

		s.logger.Debug("Updated internal token for storage-proxy",
			zap.String("path", r.URL.Path),
		)

		next.ServeHTTP(w, r)
	})
}

func (s *Server) storageProxyUpstreamMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		baseURL := strings.TrimSpace(s.cfg.StorageProxyBaseURL)
		port := s.cfg.StorageProxyPort
		if baseURL != "" && port > 0 {
			next.ServeHTTP(w, r)
			return
		}

		s.logger.Error("Storage-proxy upstream not configured",
			zap.String("proxy_base_url", baseURL),
			zap.Int("proxy_port", port),
		)
		_ = spec.WriteError(w, http.StatusServiceUnavailable, "storage_proxy_unavailable",
			fmt.Sprintf("storage-proxy upstream not configured (base_url=%q port=%d)", baseURL, port),
		)
	})
}

// authMiddleware validates incoming requests using internalauth.
// This middleware should only be applied to API routes, not health checks.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract token from multiple possible headers
		token := s.extractAuthToken(r)
		if token == "" {
			s.logger.Warn("Missing authentication token",
				zap.String("path", r.URL.Path),
				zap.String("method", r.Method),
			)
			_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication token")
			return
		}

		// Validate token
		claims, err := s.authValidator.Validate(token)
		if err != nil {
			s.logger.Warn("Authentication failed",
				zap.String("path", r.URL.Path),
				zap.String("method", r.Method),
				zap.Error(err),
			)
			_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, fmt.Sprintf("unauthorized: %v", err))
			return
		}

		// Add claims to request context for handlers
		r = r.WithContext(internalauth.WithClaims(r.Context(), claims))

		s.logger.Debug("Request authenticated",
			zap.String("path", r.URL.Path),
			zap.String("team_id", claims.TeamID),
			zap.String("caller", claims.Caller),
		)

		next.ServeHTTP(w, r)
	})
}

// extractAuthToken extracts authentication token from request headers.
func (s *Server) extractAuthToken(r *http.Request) string {
	return r.Header.Get("X-Internal-Token")
}

// GetInternalToken returns the current internal token for storage-proxy communication.
// This method is thread-safe and can be called by gRPC clients.
func (s *Server) GetInternalToken() string {
	if s.tokenProvider == nil {
		return ""
	}
	return s.tokenProvider.GetInternalToken()
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hijacker.Hijack()
}

func (rw *responseWriter) Flush() {
	if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (rw *responseWriter) Push(target string, opts *http.PushOptions) error {
	if pusher, ok := rw.ResponseWriter.(http.Pusher); ok {
		return pusher.Push(target, opts)
	}
	return http.ErrNotSupported
}
