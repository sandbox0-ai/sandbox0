package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/infra/edge-gateway/pkg/config"
	"github.com/sandbox0-ai/infra/edge-gateway/pkg/db"
	"github.com/sandbox0-ai/infra/edge-gateway/pkg/middleware"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/proxy"
	"go.uber.org/zap"
)

// Server represents the HTTP server for edge-gateway
type Server struct {
	router          *gin.Engine
	cfg             *config.Config
	pool            *pgxpool.Pool
	repo            *db.Repository
	proxyRouter     *proxy.Router
	authMiddleware  *middleware.AuthMiddleware
	rateLimiter     *middleware.RateLimiter
	requestLogger   *middleware.RequestLogger
	logger          *zap.Logger
	internalAuthGen *internalauth.Generator
}

// NewServer creates a new HTTP server
func NewServer(
	cfg *config.Config,
	pool *pgxpool.Pool,
	logger *zap.Logger,
) (*Server, error) {
	// Set gin mode
	gin.SetMode(gin.ReleaseMode)

	// Create router
	router := gin.New()

	// Create repository
	repo := db.NewRepository(pool)

	// Create proxy router to internal-gateway
	proxyRouter, err := proxy.NewRouter(
		cfg.InternalGatewayURL,
		logger,
		cfg.ProxyTimeout,
	)
	if err != nil {
		return nil, fmt.Errorf("create proxy router: %w", err)
	}

	// Create middleware
	authMiddleware := middleware.NewAuthMiddleware(repo, cfg.JWTSecret, logger)
	rateLimiter := middleware.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst, logger)
	requestLogger := middleware.NewRequestLogger(logger)

	// Initialize internal auth generator
	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(cfg.InternalJWTPrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load internal JWT private key: %w", err)
	}
	internalAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "edge-gateway",
		PrivateKey: privateKey,
		TTL:        30 * time.Second,
	})

	server := &Server{
		router:          router,
		cfg:             cfg,
		pool:            pool,
		repo:            repo,
		proxyRouter:     proxyRouter,
		authMiddleware:  authMiddleware,
		rateLimiter:     rateLimiter,
		requestLogger:   requestLogger,
		logger:          logger,
		internalAuthGen: internalAuthGen,
	}

	server.setupRoutes()

	return server, nil
}

// setupRoutes configures all HTTP routes
func (s *Server) setupRoutes() {
	// Global middleware (order matters)
	s.router.Use(middleware.Recovery(s.logger))
	s.router.Use(s.requestLogger.Logger())

	// Health check endpoints (no auth required)
	s.router.GET("/healthz", s.healthCheck)
	s.router.GET("/readyz", s.readinessCheck)

	// Metrics endpoint
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// API routes - authenticate then proxy to internal-gateway
	api := s.router.Group("/api")
	{
		// Apply auth and rate limiting to all API routes
		api.Use(s.authMiddleware.Authenticate())
		api.Use(s.rateLimiter.RateLimit())
		api.Use(s.injectInternalToken())

		// Wildcard proxy - forward all requests to internal-gateway
		api.Any("/*path", s.proxyRouter.ProxyToTarget())
	}
}

// injectInternalToken adds internal auth token to forwarded requests
func (s *Server) injectInternalToken() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := middleware.GetAuthContext(c)
		if authCtx == nil {
			c.Next()
			return
		}

		// Generate internal token for internal-gateway
		token, err := s.internalAuthGen.Generate(
			"internal-gateway",
			authCtx.TeamID,
			authCtx.UserID,
			internalauth.GenerateOptions{
				Permissions: authCtx.Permissions,
				RequestID:   middleware.GetRequestID(c),
			},
		)
		if err != nil {
			s.logger.Error("Failed to generate internal token", zap.Error(err))
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error": "internal server error",
			})
			return
		}

		// Set internal token header for downstream service
		c.Request.Header.Set(internalauth.DefaultTokenHeader, token)

		// Also forward team/user info in headers for logging
		c.Request.Header.Set("X-Team-ID", authCtx.TeamID)
		c.Request.Header.Set("X-User-ID", authCtx.UserID)
		c.Request.Header.Set("X-Auth-Method", string(authCtx.AuthMethod))

		c.Next()
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
	if err := s.pool.Ping(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "not ready",
			"error":  "database unavailable",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
	})
}
