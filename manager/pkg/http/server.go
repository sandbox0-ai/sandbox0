package http

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0ai/infra/manager/pkg/service"
	"go.uber.org/zap"
)

// Server represents the HTTP server
type Server struct {
	router         *gin.Engine
	sandboxService *service.SandboxService
	logger         *zap.Logger
	port           int
}

// NewServer creates a new HTTP server
func NewServer(
	sandboxService *service.SandboxService,
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
		logger:         logger,
		port:           port,
	}

	server.setupRoutes()

	return server
}

// setupRoutes sets up the HTTP routes
func (s *Server) setupRoutes() {
	// Health check
	s.router.GET("/healthz", s.healthCheck)
	s.router.GET("/readyz", s.readinessCheck)

	// API v1
	v1 := s.router.Group("/api/v1")
	{
		// Sandbox management
		sandboxes := v1.Group("/sandboxes")
		{
			sandboxes.POST("/claim", s.claimSandbox)
			sandboxes.GET("/:id/status", s.getSandboxStatus)
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

func (s *Server) getSandboxStatus(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
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
