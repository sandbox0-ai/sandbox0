package http

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	internalmiddleware "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	registryprovider "github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/builtin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/oidc"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewayhandlers "github.com/sandbox0-ai/sandbox0/pkg/gateway/http/handlers"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/public"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

// Server represents the HTTP server for regional-gateway
type Server struct {
	router               *gin.Engine
	cfg                  *config.RegionalGatewayConfig
	pool                 *pgxpool.Pool
	identityRepo         *identity.Repository
	apiKeyRepo           *apikey.Repository
	clusterGatewayRouter *proxy.Router
	schedulerRouter      *proxy.Router // Optional: proxy to scheduler for templates
	authMiddleware       *middleware.AuthMiddleware
	internalAuth         *internalmiddleware.InternalAuthMiddleware
	rateLimiter          *middleware.RateLimiter
	requestLogger        *middleware.RequestLogger
	logger               *zap.Logger
	internalAuthGen      *internalauth.Generator
	meteringHandler      *gatewayhandlers.MeteringHandler
	obsProvider          *observability.Provider

	clusterGatewayProxies   map[string]*proxy.Router
	clusterGatewayProxiesMu sync.RWMutex

	clusterCache   map[string]string
	clusterCacheAt time.Time
	clusterCacheMu sync.RWMutex
	entitlements   licensing.Entitlements
	registry       registryprovider.Provider
	teamMembership teamMembershipLookup

	// Auth components
	builtinProvider *builtin.Provider
	oidcManager     *oidc.Manager
	jwtIssuer       *authn.Issuer
}

type teamMembershipLookup interface {
	GetTeamMember(ctx context.Context, teamID, userID string) (*identity.TeamMember, error)
}

// NewServer creates a new HTTP server
func NewServer(
	cfg *config.RegionalGatewayConfig,
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
	identityRepo := identity.NewRepository(pool)
	apiKeyRepo := apikey.NewRepository(pool)
	var meteringRepo *metering.Repository
	if pool != nil {
		meteringRepo = metering.NewRepository(pool)
	}
	registryProvider, err := registryprovider.NewProvider(cfg.Registry, nil, logger)
	if err != nil {
		logger.Warn("Registry provider disabled", zap.Error(err))
	}
	oidcConfigured := config.HasEnabledOIDCProviders(cfg.OIDCProviders)
	schedulerConfigured := cfg.SchedulerEnabled && cfg.SchedulerURL != ""
	selfHostedAuthEnabled := edgeAuthModeUsesSelfHostedIdentity(cfg.AuthMode)
	enterpriseFeaturesEnabled := schedulerConfigured || (selfHostedAuthEnabled && oidcConfigured)

	licenseEntitlements := licensing.NewStaticEntitlements()
	if enterpriseFeaturesEnabled {
		if err := licensing.RequireLicenseFile(cfg.LicenseFile); err != nil {
			return nil, fmt.Errorf("license_file is required when enterprise features are enabled: %w", err)
		}
		licenseEntitlements = licensing.LoadFileEntitlements(cfg.LicenseFile)
	}

	// Keep self-hosted auth endpoints consistent when OIDC is not configured.
	publicEntitlements := licensing.NewStaticEntitlements(licensing.FeatureSSO)
	if selfHostedAuthEnabled && oidcConfigured {
		publicEntitlements = licenseEntitlements
	}

	// Create observable HTTP client for proxy
	httpClient := obsProvider.HTTP.NewClient(httpobs.Config{
		Timeout: cfg.ProxyTimeout.Duration,
	})

	// Create proxy router to cluster-gateway
	clusterGatewayRouter, err := proxy.NewRouter(
		cfg.DefaultClusterGatewayURL,
		logger,
		cfg.ProxyTimeout.Duration,
		proxy.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("create proxy router: %w", err)
	}

	// Create scheduler proxy router (optional, for multi-cluster mode)
	var schedulerRouter *proxy.Router
	if schedulerConfigured {
		if err := licenseEntitlements.Require(licensing.FeatureMultiCluster); err != nil {
			return nil, fmt.Errorf("enterprise multi-cluster feature is required for scheduler mode: %w", err)
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

	publicKey, err := internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultInternalJWTPublicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load internal JWT public key: %w", err)
	}
	internalValidator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             internalauth.ServiceRegionalGateway,
		PublicKey:          publicKey,
		AllowedCallers:     []string{internalauth.ServiceSSHGateway},
		ClockSkewTolerance: 10 * time.Second,
	})
	internalAuth := internalmiddleware.NewInternalAuthMiddleware(internalValidator, logger)

	// Initialize JWT issuer
	jwtIssuer, err := authn.NewIssuerFromConfig(cfg.JWTIssuer, cfg.JWTSecret, cfg.JWTPrivateKeyPEM, cfg.JWTPublicKeyPEM, cfg.JWTPrivateKeyFile, cfg.JWTPublicKeyFile, cfg.JWTAccessTokenTTL.Duration, cfg.JWTRefreshTokenTTL.Duration)
	if err != nil {
		return nil, fmt.Errorf("create jwt issuer: %w", err)
	}

	// Create middleware
	authMiddlewareOptions := []middleware.AuthMiddlewareOption(nil)
	if !selfHostedAuthEnabled {
		if strings.TrimSpace(cfg.RegionID) != "" {
			authMiddlewareOptions = append(authMiddlewareOptions, middleware.WithRequiredTeamRegionID(cfg.RegionID))
		}
	}
	authMiddleware := middleware.NewAuthMiddleware(apiKeyRepo, cfg.JWTSecret, jwtIssuer, logger, authMiddlewareOptions...)
	rateLimiter := middleware.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst, cfg.RateLimitCleanupInterval.Duration, logger)
	requestLogger := middleware.NewRequestLogger(logger)

	// Initialize built-in auth provider
	var builtinProvider *builtin.Provider
	if selfHostedAuthEnabled {
		builtinProvider = builtin.NewProvider(identityRepo, &cfg.BuiltInAuth, cfg.DefaultTeamName)
	}

	// Initialize OIDC manager
	if selfHostedAuthEnabled && oidcConfigured {
		if err := licenseEntitlements.Require(licensing.FeatureSSO); err != nil {
			return nil, fmt.Errorf("enterprise SSO feature is required when OIDC providers are configured: %w", err)
		}
	}

	var oidcManager *oidc.Manager
	if selfHostedAuthEnabled && oidcConfigured {
		oidcManager, err = oidc.NewManager(ctx, &cfg.GatewayConfig, identityRepo, logger)
		if err != nil {
			logger.Warn("Failed to initialize OIDC manager", zap.Error(err))
			// Continue without OIDC support
		}
	}

	// Ensure the bootstrap admin exists for self-hosted identity, including OIDC-only deployments.
	if selfHostedAuthEnabled && cfg.BuiltInAuth.InitUser != nil && (cfg.BuiltInAuth.Enabled || oidcConfigured) {
		if err := builtinProvider.EnsureInitUser(ctx); err != nil {
			logger.Warn("Failed to ensure init user", zap.Error(err))
		}
	}

	server := &Server{
		router:                router,
		cfg:                   cfg,
		pool:                  pool,
		identityRepo:          identityRepo,
		apiKeyRepo:            apiKeyRepo,
		clusterGatewayRouter:  clusterGatewayRouter,
		schedulerRouter:       schedulerRouter,
		authMiddleware:        authMiddleware,
		internalAuth:          internalAuth,
		rateLimiter:           rateLimiter,
		requestLogger:         requestLogger,
		logger:                logger,
		internalAuthGen:       internalAuthGen,
		meteringHandler:       gatewayhandlers.NewMeteringHandler(meteringRepo, cfg.RegionID, logger),
		obsProvider:           obsProvider,
		clusterGatewayProxies: make(map[string]*proxy.Router),
		clusterCache:          make(map[string]string),
		entitlements:          publicEntitlements,
		registry:              registryProvider,
		teamMembership:        identityRepo,

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
	s.router.Use(middleware.UpstreamTimeoutWhitelist())

	// Health check endpoints (no auth required)
	s.router.GET("/healthz", s.healthCheck)
	s.router.GET("/readyz", s.readinessCheck)
	s.router.GET("/metadata", gatewayhandlers.GatewayMetadata("regional-gateway", gatewayhandlers.GatewayModeDirect))

	// Metrics endpoint
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	s.setupPublicRoutes()
	s.setupMeteringRoutes()
	s.setupInternalSSHRoutes()

	// ===== API Proxy Routes =====
	// These routes proxy to cluster-gateway (or scheduler for templates) after authentication
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

			// Sandbox creation and listing go to scheduler, others route to cluster-gateway
			sandboxes := api.Group("/v1/sandboxes")
			sandboxes.GET("", s.injectInternalTokenForTarget("scheduler"), s.schedulerRouter.ProxyToTarget)
			sandboxes.POST("", s.injectInternalTokenForTarget("scheduler"), s.schedulerRouter.ProxyToTarget)
			sandboxes.GET("/:id", s.getSandboxDetail)
			// GET /:id is handled by regional-gateway so it can enrich the response with
			// region-scoped connection details. Mutating exact-ID operations proxy to the
			// owning cluster-gateway.
			sandboxes.DELETE("/:id", s.proxySandbox)
			sandboxes.PUT("/:id", s.proxySandbox)
			sandboxes.Any("/:id/*path", s.proxySandbox)
		}

		// Registry credentials are served by regional-gateway in control plane.
		registry := api.Group("/v1/registry")
		{
			registry.POST("/credentials", s.authMiddleware.RequirePermission(authn.PermRegistryWrite), s.getRegistryCredentials)
		}

		credentialSources := api.Group("/v1/credential-sources")
		{
			credentialSources.GET("", s.authMiddleware.RequirePermission(authn.PermCredentialSourceRead), s.injectInternalToken(), s.clusterGatewayRouter.ProxyToTarget)
			credentialSources.POST("", s.authMiddleware.RequirePermission(authn.PermCredentialSourceWrite), s.injectInternalToken(), s.clusterGatewayRouter.ProxyToTarget)
			credentialSources.GET("/:name", s.authMiddleware.RequirePermission(authn.PermCredentialSourceRead), s.injectInternalToken(), s.clusterGatewayRouter.ProxyToTarget)
			credentialSources.PUT("/:name", s.authMiddleware.RequirePermission(authn.PermCredentialSourceWrite), s.injectInternalToken(), s.clusterGatewayRouter.ProxyToTarget)
			credentialSources.DELETE("/:name", s.authMiddleware.RequirePermission(authn.PermCredentialSourceDelete), s.injectInternalToken(), s.clusterGatewayRouter.ProxyToTarget)
		}

	}

	// Unmatched API routes fall back to the default cluster-gateway. Everything
	// else goes through the public exposure fallback.
	s.router.NoRoute(s.handleNoRoute)
}

func (s *Server) setupInternalSSHRoutes() {
	if s.internalAuth == nil {
		return
	}

	internal := s.router.Group("/internal/v1")
	internal.Use(s.internalAuth.Authenticate())
	{
		internal.GET("/sandboxes/:id/ssh-target", s.resolveInternalSSHTarget)
	}
}

func (s *Server) setupMeteringRoutes() {
	if s.meteringHandler == nil {
		regionID := ""
		if s.cfg != nil {
			regionID = s.cfg.RegionID
		}
		s.meteringHandler = gatewayhandlers.NewMeteringHandler(nil, regionID, s.logger)
	}

	internal := s.router.Group("/internal/v1")
	internal.Use(s.authMiddleware.Authenticate())
	internal.Use(s.authMiddleware.RequireSystemAdmin())
	{
		internal.GET("/metering/status", s.meteringHandler.GetStatus)
		internal.GET("/metering/events", s.meteringHandler.ListEvents)
		internal.GET("/metering/windows", s.meteringHandler.ListWindows)
	}
}

func (s *Server) setupPublicRoutes() {
	deps := public.Deps{
		IdentityRepo:    s.identityRepo,
		APIKeyRepo:      s.apiKeyRepo,
		AuthMiddleware:  s.authMiddleware,
		BuiltinProvider: s.builtinProvider,
		OIDCManager:     s.oidcManager,
		Entitlements:    s.entitlements,
		JWTIssuer:       s.jwtIssuer,
		RegionID:        s.cfg.RegionID,
		Logger:          s.logger,
	}

	if edgeAuthModeUsesSelfHostedIdentity(s.cfg.AuthMode) {
		public.RegisterRoutes(s.router, deps)
		return
	}

	public.RegisterUserSSHKeyRoutes(s.router, deps)
	public.RegisterAPIKeyRoutes(s.router, deps)
}

func (s *Server) handleNoRoute(c *gin.Context) {
	if s.handleAPINoRoute(c) {
		return
	}
	s.proxyPublicExposureNoRoute(c)
}

func (s *Server) handleAPINoRoute(c *gin.Context) bool {
	if c == nil {
		return false
	}
	if c.Request == nil {
		return false
	}
	if c.Request.URL == nil {
		return false
	}
	path := c.Request.URL.Path
	if path != "/api" && !strings.HasPrefix(path, "/api/") {
		return false
	}

	authCtx, err := s.authMiddleware.AuthenticateRequest(c)
	if err != nil {
		s.logger.Warn("Authentication failed",
			zap.String("error", err.Error()),
			zap.String("client_ip", c.ClientIP()),
		)
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			"error": err.Error(),
		})
		return true
	}
	c.Set("auth_context", authCtx)
	ctx := authn.WithAuthContext(c.Request.Context(), authCtx)
	c.Request = c.Request.WithContext(ctx)

	s.rateLimiter.RateLimit()(c)
	if c.IsAborted() {
		return true
	}
	token, err := s.generateInternalToken(authCtx, "cluster-gateway")
	if err != nil {
		s.logger.Error("Failed to generate internal token for cluster-gateway fallback", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return true
	}
	s.applyInternalHeaders(c, token, authCtx)
	s.clusterGatewayRouter.ProxyToTarget(c)
	return true
}

// injectInternalToken adds internal auth token to forwarded requests (default: cluster-gateway)
func (s *Server) injectInternalToken() gin.HandlerFunc {
	return s.injectInternalTokenForTarget("cluster-gateway")
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
		token, err := s.generateInternalToken(authCtx, target)
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
	servers := make([]*http.Server, 0, 2)
	errChan := make(chan error, 2)
	tlsEnabled := strings.TrimSpace(s.cfg.TLSCertPath) != "" && strings.TrimSpace(s.cfg.TLSKeyPath) != ""
	tlsPort := s.cfg.TLSPort
	if tlsEnabled {
		if tlsPort == 0 {
			tlsPort = s.cfg.HTTPPort + 1
		}
		if tlsPort == s.cfg.HTTPPort {
			return fmt.Errorf("tls_port must differ from http_port")
		}
	}

	httpServer := s.newHTTPServer(s.cfg.HTTPPort)
	servers = append(servers, httpServer)
	s.logger.Info("Starting HTTP server",
		zap.String("addr", httpServer.Addr),
		zap.Int("port", s.cfg.HTTPPort),
	)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	if tlsEnabled {
		tlsServer := s.newHTTPServer(tlsPort)
		servers = append(servers, tlsServer)
		s.logger.Info("Starting HTTPS server",
			zap.String("addr", tlsServer.Addr),
			zap.Int("port", tlsPort),
			zap.String("cert_path", s.cfg.TLSCertPath),
			zap.String("key_path", s.cfg.TLSKeyPath),
		)
		go func() {
			if err := tlsServer.ListenAndServeTLS(s.cfg.TLSCertPath, s.cfg.TLSKeyPath); err != nil && err != http.ErrServerClosed {
				errChan <- err
			}
		}()
	}

	// Wait for context cancellation or error
	select {
	case <-ctx.Done():
		s.logger.Info("Shutting down HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout.Duration)
		defer cancel()
		var shutdownErr error
		for _, server := range servers {
			if err := server.Shutdown(shutdownCtx); err != nil && shutdownErr == nil {
				shutdownErr = err
			}
		}
		return shutdownErr
	case err := <-errChan:
		return err
	}
}

func (s *Server) newHTTPServer(port int) *http.Server {
	return &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      s.router,
		ReadTimeout:  s.cfg.ServerReadTimeout.Duration,
		WriteTimeout: s.cfg.ServerWriteTimeout.Duration,
		IdleTimeout:  s.cfg.ServerIdleTimeout.Duration,
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

const (
	edgeAuthModeSelfHosted      = "self_hosted"
	edgeAuthModeFederatedGlobal = "federated_global"
)

func normalizeEdgeAuthMode(mode string) string {
	mode = strings.TrimSpace(strings.ToLower(mode))
	switch mode {
	case "", edgeAuthModeSelfHosted:
		return edgeAuthModeSelfHosted
	case edgeAuthModeFederatedGlobal:
		return edgeAuthModeFederatedGlobal
	default:
		return edgeAuthModeSelfHosted
	}
}

func edgeAuthModeUsesSelfHostedIdentity(mode string) bool {
	return normalizeEdgeAuthMode(mode) == edgeAuthModeSelfHosted
}
