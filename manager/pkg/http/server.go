package http

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/manager/pkg/service"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// Server represents the HTTP server
type Server struct {
	router          *gin.Engine
	sandboxService  *service.SandboxService
	templateService *service.TemplateService
	clusterService  *service.ClusterService
	authValidator   *internalauth.Validator
	logger          *zap.Logger
	port            int
}

// NewServer creates a new HTTP server
func NewServer(
	sandboxService *service.SandboxService,
	templateService *service.TemplateService,
	clusterService *service.ClusterService,
	authValidator *internalauth.Validator,
	logger *zap.Logger,
	port int,
) *Server {
	// Set gin mode based on log level
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(requestLogger(logger))

	server := &Server{
		router:          router,
		sandboxService:  sandboxService,
		templateService: templateService,
		clusterService:  clusterService,
		authValidator:   authValidator,
		logger:          logger,
		port:            port,
	}

	server.setupRoutes()

	return server
}

// setupRoutes sets up the HTTP routes
func (s *Server) setupRoutes() {
	// Health check (no auth required)
	s.router.GET("/healthz", s.healthCheck)
	s.router.GET("/readyz", s.readinessCheck)

	// API v1 (requires auth)
	v1 := s.router.Group("/api/v1")
	v1.Use(s.authMiddleware())
	{
		// Sandbox management
		sandboxes := v1.Group("/sandboxes")
		{
			sandboxes.POST("", s.claimSandbox)
			sandboxes.GET("/:id", s.getSandbox)
			sandboxes.GET("/:id/status", s.getSandboxStatus)
			sandboxes.GET("/:id/stats", s.getSandboxStats)
			sandboxes.POST("/:id/pause", s.pauseSandbox)
			sandboxes.POST("/:id/resume", s.resumeSandbox)
			sandboxes.POST("/:id/refresh", s.refreshSandbox)
			sandboxes.DELETE("/:id", s.terminateSandbox)
		}

		// Template management
		templates := v1.Group("/templates")
		{
			templates.GET("", s.listTemplates)
			templates.GET("/:id", s.getTemplate)
			templates.POST("", s.createTemplate)
			templates.PUT("/:id", s.updateTemplate)
			templates.DELETE("/:id", s.deleteTemplate)
			templates.POST("/:id/pool/warm", s.warmPool)
			templates.GET("/stats", s.getTemplateStats) // Template stats for scheduler
		}

		// Cluster management (for scheduler)
		cluster := v1.Group("/cluster")
		{
			cluster.GET("/summary", s.getClusterSummary)
		}
	}
}

// Start starts the HTTP server
func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.port)
	s.logger.Info("Starting HTTP server", zap.String("addr", addr))

	server := &http.Server{
		Addr:    addr,
		Handler: s.router,
	}

	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	// Wait for context cancellation or error
	select {
	case <-ctx.Done():
		s.logger.Info("Shutting down HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}

// Handler functions

func (s *Server) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
	})
}

func (s *Server) readinessCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ready",
	})
}

// Middleware

func requestLogger(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Skip logging for health check and readiness check
		path := c.Request.URL.Path
		if path == "/healthz" || path == "/readyz" {
			c.Next()
			return
		}

		// Start timer
		start := c.Request.Context().Value("start")
		if start == nil {
			start = c.Request.Context()
		}

		// Process request
		c.Next()

		// Log request
		logger.Info("HTTP request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.String("client_ip", c.ClientIP()),
		)
	}
}

// authMiddleware validates internal authentication tokens
func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Extract token from multiple possible headers
		token := s.extractAuthToken(c.Request)
		if token == "" {
			s.logger.Warn("Missing authentication token",
				zap.String("path", c.Request.URL.Path),
				zap.String("method", c.Request.Method),
			)
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "missing authentication token",
			})
			c.Abort()
			return
		}

		// Validate token
		claims, err := s.authValidator.Validate(token)
		if err != nil {
			s.logger.Warn("Authentication failed",
				zap.String("path", c.Request.URL.Path),
				zap.String("method", c.Request.Method),
				zap.Error(err),
			)
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": fmt.Sprintf("unauthorized: %v", err),
			})
			c.Abort()
			return
		}

		// Add claims to request context for handlers
		ctx := internalauth.WithClaims(c.Request.Context(), claims)
		c.Request = c.Request.WithContext(ctx)

		s.logger.Debug("Request authenticated",
			zap.String("path", c.Request.URL.Path),
			zap.String("team_id", claims.TeamID),
			zap.String("caller", claims.Caller),
		)

		c.Next()
	}
}

// extractAuthToken extracts authentication token from request headers
// Supports both X-Internal-Token and Authorization: Bearer <token>
func (s *Server) extractAuthToken(r *http.Request) string {
	// Try X-Internal-Token header first
	if token := r.Header.Get("X-Internal-Token"); token != "" {
		return token
	}

	// Try Authorization header with Bearer prefix
	if auth := r.Header.Get("Authorization"); auth != "" {
		if strings.HasPrefix(auth, "Bearer ") {
			return strings.TrimPrefix(auth, "Bearer ")
		}
	}

	return ""
}
