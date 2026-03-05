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
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/builtin"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/jwt"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/oidc"
	"github.com/sandbox0-ai/infra/pkg/gateway/db"
	"github.com/sandbox0-ai/infra/pkg/gateway/middleware"
	"github.com/sandbox0-ai/infra/pkg/gateway/public"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/license"
	"github.com/sandbox0-ai/infra/pkg/observability"
	httpobs "github.com/sandbox0-ai/infra/pkg/observability/http"
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
	obsProvider     *observability.Provider

	internalGatewayProxies   map[string]*proxy.Router
	internalGatewayProxiesMu sync.RWMutex

	clusterCache   map[string]string
	clusterCacheAt time.Time
	clusterCacheMu sync.RWMutex
	ssoEnabled     bool

	// Auth components
	builtinProvider *builtin.Provider
	oidcManager     *oidc.Manager
	jwtIssuer       *jwt.Issuer
}

// NewServer creates a new HTTP server
func NewServer(
	cfg *config.EdgeGatewayConfig,
	pool *pgxpool.Pool,
	logger *zap.Logger,
	obsProvider *observability.Provider,
) (*Server, error) {
	ctx := context.Background()

	// Set gin mode
	gin.SetMode(gin.ReleaseMode)

	// Create router
	router := gin.New()

	// Create repository
	repo := db.NewRepository(pool)

	licenseCheckerByFile := make(map[string]*license.Checker)
	loadLicense := func(path string) (*license.Checker, error) {
		if checker, ok := licenseCheckerByFile[path]; ok {
			return checker, nil
		}
		checker, err := license.LoadFromFile(path)
		if err != nil {
			return nil, err
		}
		licenseCheckerByFile[path] = checker
		return checker, nil
	}

	// Create observable HTTP client for proxy
	httpClient := obsProvider.HTTP.NewClient(httpobs.Config{
		Timeout: cfg.ProxyTimeout.Duration,
	})

	// Create proxy router to internal-gateway
	igRouter, err := proxy.NewRouter(
		cfg.DefaultInternalGatewayURL,
		logger,
		cfg.ProxyTimeout.Duration,
		proxy.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("create proxy router: %w", err)
	}

	// Create scheduler proxy router (optional, for multi-cluster mode)
	var schedulerRouter *proxy.Router
	if cfg.SchedulerEnabled && cfg.SchedulerURL != "" {
		licenseChecker, err := loadLicense(cfg.LicenseFile)
		if err != nil {
			return nil, fmt.Errorf("load enterprise license for multi-cluster: %w", err)
		}
		if !licenseChecker.HasFeature(license.FeatureMultiCluster) {
			return nil, fmt.Errorf("enterprise license missing required feature: %s", license.FeatureMultiCluster)
		}

		schedulerRouter, err = proxy.NewRouter(
			cfg.SchedulerURL,
			logger,
			cfg.ProxyTimeout.Duration,
			proxy.WithHTTPClient(httpClient),
		)
		if err != nil {
			return nil, fmt.Errorf("create scheduler proxy router: %w", err)
		}
		logger.Info("Scheduler mode enabled",
			zap.String("scheduler_url", cfg.SchedulerURL),
		)
	}

	// Initialize internal auth generator
	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load internal JWT private key: %w", err)
	}
	internalAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     cfg.InternalAuthCaller,
		PrivateKey: privateKey,
		TTL:        cfg.InternalAuthTTL.Duration,
	})

	// Initialize JWT issuer
	jwtIssuer := jwt.NewIssuer(cfg.JWTIssuer, cfg.JWTSecret, cfg.JWTAccessTokenTTL.Duration, cfg.JWTRefreshTokenTTL.Duration)

	// Create middleware
	authMiddleware := middleware.NewAuthMiddleware(repo, cfg.JWTSecret, jwtIssuer, logger)
	rateLimiter := middleware.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst, cfg.RateLimitCleanupInterval.Duration, logger)
	requestLogger := middleware.NewRequestLogger(logger)

	// Initialize built-in auth provider
	builtinProvider := builtin.NewProvider(repo, &cfg.BuiltInAuth, cfg.DefaultTeamName)

	// Initialize OIDC manager
	ssoEnabled := true
	oidcConfigured := config.HasEnabledOIDCProviders(cfg.OIDCProviders)
	if oidcConfigured {
		licenseChecker, err := loadLicense(cfg.LicenseFile)
		if err != nil {
			ssoEnabled = false
			logger.Warn("Disabling OIDC SSO because enterprise license cannot be loaded",
				zap.String("feature", license.FeatureSSO),
				zap.Error(err),
			)
		} else if !licenseChecker.HasFeature(license.FeatureSSO) {
			ssoEnabled = false
			logger.Warn("Disabling OIDC SSO because license does not include required feature",
				zap.String("feature", license.FeatureSSO),
			)
		}
	}

	var oidcManager *oidc.Manager
	if ssoEnabled {
		oidcManager, err = oidc.NewManager(ctx, &cfg.GatewayConfig, repo, logger)
		if err != nil {
			logger.Warn("Failed to initialize OIDC manager", zap.Error(err))
			// Continue without OIDC support
		}
	}

	// Ensure initial user exists (for self-hosted deployments)
	if cfg.BuiltInAuth.Enabled && cfg.BuiltInAuth.InitUser != nil {
		if err := builtinProvider.EnsureInitUser(ctx); err != nil {
			logger.Warn("Failed to ensure init user", zap.Error(err))
		}
	}

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
		obsProvider:            obsProvider,
		internalGatewayProxies: make(map[string]*proxy.Router),
		clusterCache:           make(map[string]string),
		ssoEnabled:             ssoEnabled,

		builtinProvider: builtinProvider,
		oidcManager:     oidcManager,
		jwtIssuer:       jwtIssuer,
	}

	server.setupRoutes()

	return server, nil
}

// setupRoutes configures all HTTP routes
func (s *Server) setupRoutes() {
	// Global middleware (order matters)
	s.router.Use(httpobs.GinMiddleware(httpobs.ServerConfig{
		Tracer: s.obsProvider.Tracer(),
	}))
	s.router.Use(middleware.Recovery(s.logger))
	s.router.Use(s.requestLogger.Logger())

	// Health check endpoints (no auth required)
	s.router.GET("/healthz", s.healthCheck)
	s.router.GET("/readyz", s.readinessCheck)

	// Metrics endpoint
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	public.RegisterRoutes(s.router, public.Deps{
		Repo:            s.repo,
		AuthMiddleware:  s.authMiddleware,
		BuiltinProvider: s.builtinProvider,
		OIDCManager:     s.oidcManager,
		SSOEnabled:      s.ssoEnabled,
		JWTIssuer:       s.jwtIssuer,
		Logger:          s.logger,
	})

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

			// Sandbox creation and listing go to scheduler, others route to internal-gateway
			sandboxes := api.Group("/v1/sandboxes")
			sandboxes.GET("", s.injectInternalTokenForTarget("scheduler"), s.schedulerRouter.ProxyToTarget)
			sandboxes.POST("", s.injectInternalTokenForTarget("scheduler"), s.schedulerRouter.ProxyToTarget)
			sandboxes.Any("/:id", s.proxySandbox)
			sandboxes.Any("/:id/*path", s.proxySandbox)
		}

		// All other API routes go to default internal-gateway
		api.Use(s.injectInternalToken())
		api.Any("/*path", s.igRouter.ProxyToTarget)
	}

	// Host-based public exposure fallback (non-/api paths).
	s.router.NoRoute(s.proxyPublicExposureNoRoute)
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
			},
		)
		if err != nil {
			s.logger.Error("Failed to generate internal token",
				zap.String("target", target),
				zap.Error(err),
			)
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
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
		ReadTimeout:  s.cfg.ServerReadTimeout.Duration,
		WriteTimeout: s.cfg.ServerWriteTimeout.Duration,
		IdleTimeout:  s.cfg.ServerIdleTimeout.Duration,
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout.Duration)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}

// Health check handlers
func (s *Server) healthCheck(c *gin.Context) {
	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
	})
}

func (s *Server) readinessCheck(c *gin.Context) {
	// Check database connectivity
	if err := s.pool.Ping(c.Request.Context()); err != nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "database unavailable", gin.H{
			"status": "not ready",
		})
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
	})
}
