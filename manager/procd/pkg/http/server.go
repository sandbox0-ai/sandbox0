// Package http provides the HTTP server for Procd.
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/config"
	ctxpkg "github.com/sandbox0-ai/infra/manager/procd/pkg/context"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/file"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/http/handlers"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/volume"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
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
	router     *mux.Router
	httpServer *http.Server
	logger     *zap.Logger
	cfg        *config.Config

	// Managers
	contextManager *ctxpkg.Manager
	volumeManager  *volume.Manager
	fileManager    *file.Manager

	// Token provider for storage-proxy communication
	tokenProvider *TokenProvider

	// Internal auth validator
	authValidator *internalauth.Validator
}

// NewServer creates a new HTTP server.
func NewServer(
	cfg *config.Config,
	contextManager *ctxpkg.Manager,
	volumeManager *volume.Manager,
	fileManager *file.Manager,
	authValidator *internalauth.Validator,
	tokenProvider *TokenProvider,
	logger *zap.Logger,
) *Server {
	s := &Server{
		router:         mux.NewRouter(),
		cfg:            cfg,
		contextManager: contextManager,
		volumeManager:  volumeManager,
		fileManager:    fileManager,
		authValidator:  authValidator,
		tokenProvider:  tokenProvider,
		logger:         logger,
	}

	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	// Global middleware (applied to all routes)
	s.router.Use(s.loggingMiddleware)
	s.router.Use(s.recoveryMiddleware)

	// Health check endpoints (no auth required)
	s.router.HandleFunc("/healthz", s.healthHandler).Methods("GET")
	s.router.HandleFunc("/readyz", s.readyHandler).Methods("GET")

	// API v1 (auth required if enabled)
	api := s.router.PathPrefix("/api/v1").Subrouter()

	// Apply auth middleware to all API routes
	api.Use(s.authMiddleware)
	api.Use(s.internalTokenMiddleware)

	// Sandbox-level handlers (pause/resume all processes)
	sandboxHandler := handlers.NewSandboxHandler(s.contextManager, s.logger)
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
	api.HandleFunc("/contexts/{id}/stats", contextHandler.Stats).Methods("GET")
	api.HandleFunc("/contexts/{id}/ws", contextHandler.WebSocket).Methods("GET")

	// Exec handlers (synchronous execution)
	execHandler := handlers.NewExecHandler(s.logger)
	api.HandleFunc("/exec", execHandler.Exec).Methods("POST")
	api.HandleFunc("/exec/stream", execHandler.ExecStream).Methods("POST")

	// SandboxVolume handlers
	volumeHandler := handlers.NewVolumeHandler(s.volumeManager, s.logger)
	api.HandleFunc("/sandboxvolumes/mount", volumeHandler.Mount).Methods("POST")
	api.HandleFunc("/sandboxvolumes/unmount", volumeHandler.Unmount).Methods("POST")
	api.HandleFunc("/sandboxvolumes/status", volumeHandler.Status).Methods("GET")

	// File handlers
	fileHandler := handlers.NewFileHandler(s.fileManager, s.logger)
	api.HandleFunc("/files/watch", fileHandler.Watch).Methods("GET")
	api.HandleFunc("/files/move", fileHandler.Move).Methods("POST")
	api.PathPrefix("/files/").HandlerFunc(fileHandler.Handle)
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
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		s.logger.Info("HTTP request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", wrapped.statusCode),
			zap.Duration("duration", time.Since(start)),
		)
	})
}

func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				s.logger.Error("Panic recovered",
					zap.Any("error", err),
					zap.String("path", r.URL.Path),
				)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
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
			http.Error(w, "missing internal token", http.StatusUnauthorized)
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
			http.Error(w, "missing authentication token", http.StatusUnauthorized)
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
			http.Error(w, fmt.Sprintf("unauthorized: %v", err), http.StatusUnauthorized)
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
