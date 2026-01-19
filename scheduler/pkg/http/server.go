package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/scheduler/pkg/config"
	"github.com/sandbox0-ai/infra/scheduler/pkg/db"
	"go.uber.org/zap"
)

// Server represents the HTTP server for scheduler
type Server struct {
	router        *gin.Engine
	cfg           *config.Config
	repo          *db.Repository
	authValidator *internalauth.Validator
	reconciler    Reconciler
	logger        *zap.Logger
}

// Reconciler interface for triggering reconciliation
type Reconciler interface {
	TriggerReconcile(ctx context.Context)
}

// NewServer creates a new HTTP server
func NewServer(
	cfg *config.Config,
	repo *db.Repository,
	authValidator *internalauth.Validator,
	reconciler Reconciler,
	logger *zap.Logger,
) *Server {
	// Set gin mode
	gin.SetMode(gin.ReleaseMode)

	// Create router
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(requestLogger(logger))

	server := &Server{
		router:        router,
		cfg:           cfg,
		repo:          repo,
		authValidator: authValidator,
		reconciler:    reconciler,
		logger:        logger,
	}

	server.setupRoutes()

	return server
}

// setupRoutes configures all HTTP routes
func (s *Server) setupRoutes() {
	// Health check endpoints (no auth required)
	s.router.GET("/healthz", s.healthCheck)
	s.router.GET("/readyz", s.readinessCheck)

	// Metrics endpoint
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// API v1 routes
	v1 := s.router.Group("/api/v1")
	{
		// Apply internal auth to all v1 routes (requests come from edge-gateway)
		v1.Use(s.authMiddleware())

		// Template Management (source of truth for multi-cluster)
		templates := v1.Group("/templates")
		{
			templates.GET("", s.listTemplates)
			templates.GET("/:id", s.getTemplate)
			templates.POST("", s.createTemplate)
			templates.PUT("/:id", s.updateTemplate)
			templates.DELETE("/:id", s.deleteTemplate)
			templates.GET("/:id/allocations", s.getTemplateAllocations)
		}

		// Cluster Management (admin API)
		clusters := v1.Group("/clusters")
		{
			clusters.GET("", s.listClusters)
			clusters.GET("/:id", s.getCluster)
			clusters.POST("", s.createCluster)
			clusters.PUT("/:id", s.updateCluster)
			clusters.DELETE("/:id", s.deleteCluster)
		}
	}
}

// Start starts the HTTP server
func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.cfg.HTTPPort)
	s.logger.Info("Starting HTTP server",
		zap.String("addr", addr),
		zap.Int("port", s.cfg.HTTPPort),
	)

	server := &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}

// Health check handlers
func (s *Server) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
	})
}

func (s *Server) readinessCheck(c *gin.Context) {
	// Check database connectivity
	if err := s.repo.Ping(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "not ready",
			"error":  "database unavailable",
		})
		return
	}

	response := gin.H{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
	}

	// Include reconciler status if available
	if s.reconciler != nil {
		if statusGetter, ok := s.reconciler.(interface {
			GetStatus() (time.Time, error)
		}); ok {
			lastReconcile, lastErr := statusGetter.GetStatus()
			response["last_reconcile"] = lastReconcile.Unix()
			if lastErr != nil {
				response["last_reconcile_error"] = lastErr.Error()
			}

			// Warn if reconcile hasn't run in a long time (e.g., 5 minutes)
			if time.Since(lastReconcile) > 5*time.Minute && !lastReconcile.IsZero() {
				response["warning"] = "reconcile hasn't run recently"
			}
		}
	}

	c.JSON(http.StatusOK, response)
}

// Middleware

func requestLogger(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// Process request
		c.Next()

		// Log request
		logger.Info("HTTP request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.String("client_ip", c.ClientIP()),
			zap.Duration("latency", time.Since(start)),
		)
	}
}

// authMiddleware validates internal authentication tokens
func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Extract internal token from header
		token := c.GetHeader(internalauth.DefaultTokenHeader)
		if token == "" {
			// Try Authorization header as fallback
			authHeader := c.GetHeader("Authorization")
			if authHeader != "" && len(authHeader) > 7 && authHeader[:7] == "Bearer " {
				token = authHeader[7:]
			}
		}

		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing internal authentication token",
			})
			return
		}

		// Validate token
		claims, err := s.authValidator.Validate(token)
		if err != nil {
			s.logger.Warn("Internal auth validation failed",
				zap.String("error", err.Error()),
				zap.String("client_ip", c.ClientIP()),
			)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "unauthorized: " + err.Error(),
			})
			return
		}

		// Store claims in context
		ctx := internalauth.WithClaims(c.Request.Context(), claims)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}
