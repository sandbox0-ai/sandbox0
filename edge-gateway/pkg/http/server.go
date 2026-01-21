package http

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/infra/edge-gateway/pkg/auth/builtin"
	"github.com/sandbox0-ai/infra/edge-gateway/pkg/auth/jwt"
	"github.com/sandbox0-ai/infra/edge-gateway/pkg/auth/oidc"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/edge-gateway/pkg/db"
	"github.com/sandbox0-ai/infra/edge-gateway/pkg/http/handlers"
	"github.com/sandbox0-ai/infra/edge-gateway/pkg/middleware"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/proxy"
	"go.uber.org/zap"
)

// Server represents the HTTP server for edge-gateway
type Server struct {
	router          *gin.Engine
	cfg             *config.EdgeGatewayConfig
	pool            *pgxpool.Pool
	repo            *db.Repository
	igRouter        *proxy.Router
	schedulerRouter *proxy.Router // Optional: proxy to scheduler for templates
	authMiddleware  *middleware.AuthMiddleware
	rateLimiter     *middleware.RateLimiter
	requestLogger   *middleware.RequestLogger
	logger          *zap.Logger
	internalAuthGen *internalauth.Generator

	internalGatewayProxies   map[string]*proxy.Router
	internalGatewayProxiesMu sync.RWMutex

	clusterCache   map[string]string
	clusterCacheAt time.Time
	clusterCacheMu sync.RWMutex

	// Auth components
	builtinProvider *builtin.Provider
	oidcManager     *oidc.Manager
	jwtIssuer       *jwt.Issuer

	// Handlers
	authHandler   *handlers.AuthHandler
	userHandler   *handlers.UserHandler
	teamHandler   *handlers.TeamHandler
	apiKeyHandler *handlers.APIKeyHandler
}

// NewServer creates a new HTTP server
func NewServer(
	cfg *config.EdgeGatewayConfig,
	pool *pgxpool.Pool,
	logger *zap.Logger,
) (*Server, error) {
	ctx := context.Background()

	// Set gin mode
	gin.SetMode(gin.ReleaseMode)

	// Create router
	router := gin.New()

	// Create repository
	repo := db.NewRepository(pool)

	// Create proxy router to internal-gateway
	igRouter, err := proxy.NewRouter(
		cfg.DefaultInternalGatewayURL,
		logger,
		cfg.ProxyTimeout,
	)
	if err != nil {
		return nil, fmt.Errorf("create proxy router: %w", err)
	}

	// Create scheduler proxy router (optional, for multi-cluster mode)
	var schedulerRouter *proxy.Router
	if cfg.SchedulerEnabled && cfg.SchedulerURL != "" {
		schedulerRouter, err = proxy.NewRouter(
			cfg.SchedulerURL,
			logger,
			cfg.ProxyTimeout,
		)
		if err != nil {
			return nil, fmt.Errorf("create scheduler proxy router: %w", err)
		}
		logger.Info("Scheduler mode enabled",
			zap.String("scheduler_url", cfg.SchedulerURL),
		)
	}

	// Create middleware
	authMiddleware := middleware.NewAuthMiddleware(repo, cfg.JWTSecret, logger)
	rateLimiter := middleware.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst, logger)
	requestLogger := middleware.NewRequestLogger(logger)

	// Initialize internal auth generator
	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load internal JWT private key: %w", err)
	}
	internalAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "edge-gateway",
		PrivateKey: privateKey,
		TTL:        30 * time.Second,
	})

	// Initialize JWT issuer
	jwtIssuer := jwt.NewIssuer(cfg.JWTSecret, cfg.JWTAccessTokenTTL, cfg.JWTRefreshTokenTTL)

	// Initialize built-in auth provider
	builtinProvider := builtin.NewProvider(repo, &cfg.BuiltInAuth)

	// Initialize OIDC manager
	oidcManager, err := oidc.NewManager(ctx, cfg, repo, logger)
	if err != nil {
		logger.Warn("Failed to initialize OIDC manager", zap.Error(err))
		// Continue without OIDC support
	}

	// Ensure initial user exists (for self-hosted deployments)
	if cfg.BuiltInAuth.Enabled && cfg.BuiltInAuth.InitUser != nil {
		if err := builtinProvider.EnsureInitUser(ctx); err != nil {
			logger.Warn("Failed to ensure init user", zap.Error(err))
		}
	}

	// Create handlers
	authHandler := handlers.NewAuthHandler(repo, builtinProvider, oidcManager, jwtIssuer, logger)
	userHandler := handlers.NewUserHandler(repo, logger)
	teamHandler := handlers.NewTeamHandler(repo, logger)
	apiKeyHandler := handlers.NewAPIKeyHandler(repo, logger)

	server := &Server{
		router:                 router,
		cfg:                    cfg,
		pool:                   pool,
		repo:                   repo,
		igRouter:               igRouter,
		schedulerRouter:        schedulerRouter,
		authMiddleware:         authMiddleware,
		rateLimiter:            rateLimiter,
		requestLogger:          requestLogger,
		logger:                 logger,
		internalAuthGen:        internalAuthGen,
		internalGatewayProxies: make(map[string]*proxy.Router),
		clusterCache:           make(map[string]string),

		builtinProvider: builtinProvider,
		oidcManager:     oidcManager,
		jwtIssuer:       jwtIssuer,

		authHandler:   authHandler,
		userHandler:   userHandler,
		teamHandler:   teamHandler,
		apiKeyHandler: apiKeyHandler,
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

	// ===== Public Auth Routes (no authentication required) =====
	auth := s.router.Group("/auth")
	{
		// Get available auth providers
		auth.GET("/providers", s.authHandler.GetAuthProviders)

		// Built-in auth
		auth.POST("/login", s.authHandler.Login)
		auth.POST("/register", s.authHandler.Register)
		auth.POST("/refresh", s.authHandler.RefreshToken)

		// OIDC auth
		auth.GET("/oidc/:provider/login", s.authHandler.OIDCLogin)
		auth.GET("/oidc/:provider/callback", s.authHandler.OIDCCallback)
	}

	// ===== Protected Auth Routes =====
	authProtected := s.router.Group("/auth")
	authProtected.Use(s.authMiddleware.Authenticate())
	{
		authProtected.POST("/logout", s.authHandler.Logout)
		authProtected.POST("/change-password", s.authHandler.ChangePassword)
	}

	// ===== User Management Routes =====
	users := s.router.Group("/users")
	users.Use(s.authMiddleware.Authenticate())
	{
		users.GET("/me", s.userHandler.GetCurrentUser)
		users.PUT("/me", s.userHandler.UpdateCurrentUser)
		users.GET("/me/identities", s.userHandler.GetUserIdentities)
		users.DELETE("/me/identities/:id", s.userHandler.DeleteUserIdentity)
	}

	// ===== Team Management Routes =====
	teams := s.router.Group("/teams")
	teams.Use(s.authMiddleware.Authenticate())
	{
		teams.GET("", s.teamHandler.ListTeams)
		teams.POST("", s.teamHandler.CreateTeam)
		teams.GET("/:id", s.teamHandler.GetTeam)
		teams.PUT("/:id", s.teamHandler.UpdateTeam)
		teams.DELETE("/:id", s.teamHandler.DeleteTeam)

		// Team members
		teams.GET("/:id/members", s.teamHandler.ListTeamMembers)
		teams.POST("/:id/members", s.teamHandler.AddTeamMember)
		teams.PUT("/:id/members/:userId", s.teamHandler.UpdateTeamMember)
		teams.DELETE("/:id/members/:userId", s.teamHandler.RemoveTeamMember)
	}

	// ===== API Key Management Routes =====
	apiKeys := s.router.Group("/api-keys")
	apiKeys.Use(s.authMiddleware.Authenticate())
	{
		apiKeys.GET("", s.apiKeyHandler.ListAPIKeys)
		apiKeys.POST("", s.apiKeyHandler.CreateAPIKey)
		apiKeys.DELETE("/:id", s.apiKeyHandler.DeleteAPIKey)
		apiKeys.POST("/:id/deactivate", s.apiKeyHandler.DeactivateAPIKey)
	}

	// ===== API Proxy Routes =====
	// These routes proxy to internal-gateway (or scheduler for templates) after authentication
	api := s.router.Group("/api")
	{
		// Apply auth and rate limiting to all API routes
		api.Use(s.authMiddleware.Authenticate())
		api.Use(s.rateLimiter.RateLimit())

		// If scheduler is enabled, route /api/v1/templates* to scheduler
		if s.schedulerRouter != nil {
			// Template routes go to scheduler
			templates := api.Group("/v1/templates")
			templates.Use(s.injectInternalTokenForTarget("scheduler"))
			templates.Any("", s.schedulerRouter.ProxyToTarget)
			templates.Any("/*path", s.schedulerRouter.ProxyToTarget)

			// Cluster management routes also go to scheduler
			clusters := api.Group("/v1/clusters")
			clusters.Use(s.injectInternalTokenForTarget("scheduler"))
			clusters.Any("", s.schedulerRouter.ProxyToTarget)
			clusters.Any("/*path", s.schedulerRouter.ProxyToTarget)

			// Sandbox creation goes to scheduler, others route to internal-gateway
			sandboxes := api.Group("/v1/sandboxes")
			sandboxes.POST("", s.injectInternalTokenForTarget("scheduler"), s.schedulerRouter.ProxyToTarget)
			sandboxes.Any("/:id", s.proxySandbox)
			sandboxes.Any("/:id/*path", s.proxySandbox)
		}

		// All other API routes go to default internal-gateway
		api.Use(s.injectInternalToken())
		api.Any("/*path", s.igRouter.ProxyToTarget)
	}
}

// injectInternalToken adds internal auth token to forwarded requests (default: internal-gateway)
func (s *Server) injectInternalToken() gin.HandlerFunc {
	return s.injectInternalTokenForTarget("internal-gateway")
}

// injectInternalTokenForTarget adds internal auth token for a specific target service
func (s *Server) injectInternalTokenForTarget(target string) gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := middleware.GetAuthContext(c)
		if authCtx == nil {
			c.Next()
			return
		}

		// Generate internal token for the target service
		token, err := s.internalAuthGen.Generate(
			target,
			authCtx.TeamID,
			authCtx.UserID,
			internalauth.GenerateOptions{
				Permissions: authCtx.Permissions,
				RequestID:   middleware.GetRequestID(c),
			},
		)
		if err != nil {
			s.logger.Error("Failed to generate internal token",
				zap.String("target", target),
				zap.Error(err),
			)
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
