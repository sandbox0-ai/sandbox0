package http

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/manager/pkg/service"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// Server represents the HTTP server
type Server struct {
	router         *gin.Engine
	sandboxService *service.SandboxService
	authValidator  *internalauth.Validator
	logger         *zap.Logger
	port           int
}

// NewServer creates a new HTTP server
func NewServer(
	sandboxService *service.SandboxService,
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
		router:         router,
		sandboxService: sandboxService,
		authValidator:  authValidator,
		logger:         logger,
		port:           port,
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
			sandboxes.POST("/claim", s.claimSandbox)
			sandboxes.GET("", s.listSandboxes)
			sandboxes.GET("/:id", s.getSandbox)
			sandboxes.GET("/:id/status", s.getSandboxStatus)
			sandboxes.GET("/:id/stats", s.getSandboxStats)
			sandboxes.POST("/:id/pause", s.pauseSandbox)
			sandboxes.POST("/:id/resume", s.resumeSandbox)
			sandboxes.DELETE("/:id", s.terminateSandbox)
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30)
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

func (s *Server) claimSandbox(c *gin.Context) {
	var req service.ClaimRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("invalid request: %v", err),
		})
		return
	}

	// Validate required fields
	if req.TemplateID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "template_id is required",
		})
		return
	}
	if req.SandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}
	if req.TeamID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "team_id is required",
		})
		return
	}
	if req.Namespace == "" {
		req.Namespace = req.TeamID
	}

	resp, err := s.sandboxService.ClaimSandbox(c.Request.Context(), &req)
	if err != nil {
		s.logger.Error("Failed to claim sandbox",
			zap.String("templateID", req.TemplateID),
			zap.String("sandboxID", req.SandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to claim sandbox: %v", err),
		})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (s *Server) listSandboxes(c *gin.Context) {
	// Get team ID from claims
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	sandboxes, err := s.sandboxService.ListSandboxes(c.Request.Context(), claims.TeamID)
	if err != nil {
		s.logger.Error("Failed to list sandboxes",
			zap.String("teamID", claims.TeamID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to list sandboxes: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"sandboxes": sandboxes,
		"count":     len(sandboxes),
	})
}

func (s *Server) getSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	// Verify team ownership
	if sandbox.TeamID != claims.TeamID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "sandbox belongs to a different team",
		})
		return
	}

	c.JSON(http.StatusOK, sandbox)
}

func (s *Server) getSandboxStatus(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	// Verify team ownership
	if sandbox.TeamID != claims.TeamID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "sandbox belongs to a different team",
		})
		return
	}

	status, err := s.sandboxService.GetSandboxStatus(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox status",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, status)
}

func (s *Server) terminateSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	err := s.sandboxService.TerminateSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to terminate sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to terminate sandbox: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "sandbox terminated successfully",
	})
}

func (s *Server) pauseSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	// Verify ownership
	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	if sandbox.TeamID != claims.TeamID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "sandbox belongs to a different team",
		})
		return
	}

	resp, err := s.sandboxService.PauseSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to pause sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to pause sandbox: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (s *Server) resumeSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	// Verify ownership
	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	if sandbox.TeamID != claims.TeamID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "sandbox belongs to a different team",
		})
		return
	}

	resp, err := s.sandboxService.ResumeSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to resume sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to resume sandbox: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (s *Server) getSandboxStats(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	// Verify ownership
	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	if sandbox.TeamID != claims.TeamID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "sandbox belongs to a different team",
		})
		return
	}

	stats, err := s.sandboxService.GetSandboxResourceUsage(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox stats",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to get sandbox stats: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, stats)
}

// Middleware

func requestLogger(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
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
