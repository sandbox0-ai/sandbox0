package http

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/internal-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/internal-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/cache"
	gatewayapikey "github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	gatewaybuiltin "github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/builtin"
	gatewayoidc "github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/oidc"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewayidentity "github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/public"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

// Server represents the HTTP server for internal-gateway
type Server struct {
	router              *gin.Engine
	cfg                 *config.InternalGatewayConfig
	proxy2Mgr           *proxy.Router
	proxy2sp            *proxy.Router
	managerClient       *client.ManagerClient
	authMiddleware      *middleware.InternalAuthMiddleware
	publicAuth          *gatewaymiddleware.AuthMiddleware
	compositeAuth       *middleware.CompositeAuthMiddleware
	publicIdentityRepo  *gatewayidentity.Repository
	publicAPIKeyRepo    *gatewayapikey.Repository
	rateLimiter         *gatewaymiddleware.RateLimiter
	externalLimiter     *middleware.ExternalRateLimiter
	publicBuiltin       *gatewaybuiltin.Provider
	publicOIDC          *gatewayoidc.Manager
	publicJWT           *gatewayauthn.Issuer
	requestLogger       *middleware.RequestLogger
	logger              *zap.Logger
	internalAuthGen     *internalauth.Generator
	procdAuthGen        *internalauth.Generator
	internalAuthEnabled bool
	entitlements        licensing.Entitlements
	sandboxAddrCache    *cache.Cache[string, *url.URL]
	obsProvider         *observability.Provider
}

// NewServer creates a new HTTP server
func NewServer(
	cfg *config.InternalGatewayConfig,
	pool *pgxpool.Pool,
	logger *zap.Logger,
	obsProvider *observability.Provider,
) (*Server, error) {
	// Set gin mode
	gin.SetMode(gin.ReleaseMode)

	// Create router
	router := gin.New()

	// Create proxy router
	proxyTimeout := cfg.ProxyTimeout.Duration
	if proxyTimeout == 0 {
		proxyTimeout = 10 * time.Second
	}

	// Create observable HTTP client for proxy
	httpClient := obsProvider.HTTP.NewClient(httpobs.Config{
		Timeout: proxyTimeout,
	})

	var proxy2Mgr *proxy.Router
	if strings.TrimSpace(cfg.ManagerURL) != "" {
		var err error
		proxy2Mgr, err = proxy.NewRouter(
			cfg.ManagerURL,
			logger,
			proxyTimeout,
			proxy.WithHTTPClient(httpClient),
		)
		if err != nil {
			return nil, fmt.Errorf("create manager proxy router: %w", err)
		}
	}

	var proxy2sp *proxy.Router
	if strings.TrimSpace(cfg.StorageProxyURL) != "" {
		var err error
		proxy2sp, err = proxy.NewRouter(
			cfg.StorageProxyURL,
			logger,
			proxyTimeout,
			proxy.WithHTTPClient(httpClient),
		)
		if err != nil {
			return nil, fmt.Errorf("create storage-proxy proxy router: %w", err)
		}
	}

	internalAuthEnabled := authModeEnabled(cfg.AuthMode, authModeInternal)
	publicAuthEnabled := authModeEnabled(cfg.AuthMode, authModePublic)

	// Initialize internal auth keys
	var publicKey ed25519.PublicKey
	if internalAuthEnabled {
		var err error
		publicKey, err = internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultInternalJWTPublicKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load internal JWT public key: %w", err)
		}
	}

	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load internal JWT private key: %w", err)
	}

	// Create internal auth validator (for validating tokens from edge-gateway and optionally scheduler)
	allowedCallers := cfg.AllowedCallers
	if len(allowedCallers) == 0 {
		allowedCallers = []string{"edge-gateway", "scheduler"}
	}
	var validator *internalauth.Validator
	if internalAuthEnabled {
		validator = internalauth.NewValidator(internalauth.ValidatorConfig{
			Target:             "internal-gateway",
			PublicKey:          publicKey,
			AllowedCallers:     allowedCallers,
			ClockSkewTolerance: 10 * time.Second,
		})
	}

	// Create middleware
	authMiddleware := middleware.NewInternalAuthMiddleware(validator, logger)
	requestLogger := middleware.NewRequestLogger(logger)

	// Initialize internal auth generator (for downstream services)
	internalAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "internal-gateway",
		PrivateKey: privateKey,
		TTL:        10 * time.Second,
	})
	procdAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "procd",
		PrivateKey: privateKey,
		TTL:        10 * time.Second,
	})

	// Create manager client
	var managerClient *client.ManagerClient
	if strings.TrimSpace(cfg.ManagerURL) != "" {
		managerClient = client.NewManagerClient(cfg.ManagerURL, internalAuthGen, logger, proxyTimeout)
	}

	// Create sandbox cache to reduce manager API calls
	// TTL of 2 minutes balances performance and data freshness
	sandboxAddrCache := cache.New[string, *url.URL](cache.Config{
		MaxSize:         10000,           // Support up to 10k active sandboxes
		TTL:             2 * time.Minute, // Cache sandbox info for 2 minutes
		CleanupInterval: 1 * time.Minute, // Cleanup expired entries every minute
	})

	var publicIdentityRepo *gatewayidentity.Repository
	var publicAPIKeyRepo *gatewayapikey.Repository
	var publicAuth *gatewaymiddleware.AuthMiddleware
	var compositeAuth *middleware.CompositeAuthMiddleware
	var rateLimiter *gatewaymiddleware.RateLimiter
	var externalLimiter *middleware.ExternalRateLimiter
	var publicBuiltin *gatewaybuiltin.Provider
	var publicOIDC *gatewayoidc.Manager
	entitlements := licensing.NewStaticEntitlements(licensing.FeatureSSO)
	var publicJWT *gatewayauthn.Issuer

	if pool != nil {
		publicIdentityRepo = gatewayidentity.NewRepository(pool)
		publicAPIKeyRepo = gatewayapikey.NewRepository(pool)
	}

	if publicAuthEnabled {
		if publicIdentityRepo == nil || publicAPIKeyRepo == nil {
			return nil, fmt.Errorf("public auth requires database connection")
		}

		edgeCfg := &config.GatewayConfig{
			DefaultTeamName:          cfg.DefaultTeamName,
			OIDCProviders:            cfg.OIDCProviders,
			OIDCStateTTL:             cfg.OIDCStateTTL,
			OIDCStateCleanupInterval: cfg.OIDCStateCleanupInterval,
			BaseURL:                  cfg.BaseURL,
			RegionID:                 cfg.RegionID,
			PublicExposureEnabled:    cfg.PublicExposureEnabled,
			PublicRootDomain:         cfg.PublicRootDomain,
			PublicRegionID:           cfg.PublicRegionID,
		}

		builtinProvider := gatewaybuiltin.NewProvider(publicIdentityRepo, &cfg.BuiltInAuth, cfg.DefaultTeamName)
		oidcConfigured := config.HasEnabledOIDCProviders(cfg.OIDCProviders)
		if oidcConfigured {
			if err := licensing.RequireLicenseFile(cfg.LicenseFile); err != nil {
				return nil, fmt.Errorf("license_file is required when OIDC providers are configured: %w", err)
			}
			entitlements = licensing.LoadFileEntitlements(cfg.LicenseFile)
			if err := entitlements.Require(licensing.FeatureSSO); err != nil {
				return nil, fmt.Errorf("enterprise SSO feature is required when OIDC providers are configured: %w", err)
			}
		}

		var oidcManager *gatewayoidc.Manager
		if oidcConfigured {
			oidcManager, err = gatewayoidc.NewManager(context.Background(), edgeCfg, publicIdentityRepo, logger)
			if err != nil {
				logger.Warn("Failed to initialize OIDC manager", zap.Error(err))
			}
		}
		if cfg.BuiltInAuth.Enabled && cfg.BuiltInAuth.InitUser != nil {
			if err := builtinProvider.EnsureInitUser(context.Background()); err != nil {
				logger.Warn("Failed to ensure init user", zap.Error(err))
			}
		}

		jwtIssuer := gatewayauthn.NewIssuer(cfg.JWTIssuer, cfg.JWTSecret, cfg.JWTAccessTokenTTL.Duration, cfg.JWTRefreshTokenTTL.Duration)

		publicAuth = gatewaymiddleware.NewAuthMiddleware(publicAPIKeyRepo, cfg.JWTSecret, jwtIssuer, logger)
		compositeAuth = middleware.NewCompositeAuthMiddleware(authMiddleware, publicAuth, logger)
		rateLimiter = gatewaymiddleware.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst, cfg.RateLimitCleanupInterval.Duration, logger)
		externalLimiter = middleware.NewExternalRateLimiter(rateLimiter)
		publicBuiltin = builtinProvider
		publicOIDC = oidcManager
		publicJWT = jwtIssuer
	}

	server := &Server{
		router:              router,
		cfg:                 cfg,
		proxy2Mgr:           proxy2Mgr,
		proxy2sp:            proxy2sp,
		managerClient:       managerClient,
		authMiddleware:      authMiddleware,
		publicAuth:          publicAuth,
		compositeAuth:       compositeAuth,
		publicIdentityRepo:  publicIdentityRepo,
		publicAPIKeyRepo:    publicAPIKeyRepo,
		rateLimiter:         rateLimiter,
		externalLimiter:     externalLimiter,
		publicBuiltin:       publicBuiltin,
		publicOIDC:          publicOIDC,
		publicJWT:           publicJWT,
		requestLogger:       requestLogger,
		logger:              logger,
		internalAuthGen:     internalAuthGen,
		procdAuthGen:        procdAuthGen,
		internalAuthEnabled: internalAuthEnabled,
		entitlements:        entitlements,
		sandboxAddrCache:    sandboxAddrCache,
		obsProvider:         obsProvider,
	}

	server.setupRoutes()

	return server, nil
}

// Handler exposes the HTTP handler for tests.
func (s *Server) Handler() http.Handler {
	return s.router
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

	if authModeEnabled(s.cfg.AuthMode, authModePublic) {
		public.RegisterRoutes(s.router, public.Deps{
			IdentityRepo:    s.publicIdentityRepo,
			APIKeyRepo:      s.publicAPIKeyRepo,
			AuthMiddleware:  s.publicAuth,
			BuiltinProvider: s.publicBuiltin,
			OIDCManager:     s.publicOIDC,
			Entitlements:    s.entitlements,
			JWTIssuer:       s.publicJWT,
			Logger:          s.logger,
		})
	}

	// API v1 routes
	v1 := s.router.Group("/api/v1")
	{
		if authModeEnabled(s.cfg.AuthMode, authModePublic) {
			v1.Use(s.compositeAuth.Authenticate())
			if s.externalLimiter != nil {
				v1.Use(s.externalLimiter.RateLimit())
			}
		} else {
			// Apply internal auth to all v1 routes (requests come from edge-gateway)
			v1.Use(s.authMiddleware.Authenticate())
		}

		// === Sandbox Management (→ Manager) ===
		sandboxes := v1.Group("/sandboxes")
		sandboxes.Use(s.managerUpstreamMiddleware())
		{
			sandboxes.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.listSandboxes)
			sandboxes.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxCreate), s.createSandbox)
			sandboxes.GET("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.getSandbox)
			sandboxes.GET("/:id/status", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.getSandboxStatus)
			sandboxes.PUT("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.updateSandbox)
			sandboxes.DELETE("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxDelete), s.deleteSandbox)
			sandboxes.POST("/:id/pause", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.pauseSandbox)
			sandboxes.POST("/:id/resume", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.resumeSandbox)
			sandboxes.POST("/:id/refresh", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.refreshSandbox)

			// === Network Policy (→ Manager) ===
			sandboxes.GET("/:id/network", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.getNetworkPolicy)
			sandboxes.PUT("/:id/network", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.updateNetworkPolicy)

			// === Exposed Ports (→ Manager) ===
			sandboxes.GET("/:id/exposed-ports", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.getExposedPorts)
			sandboxes.PUT("/:id/exposed-ports", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.updateExposedPorts)
			sandboxes.DELETE("/:id/exposed-ports", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.clearExposedPorts)
			sandboxes.DELETE("/:id/exposed-ports/:port", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.deleteExposedPort)

			// === Process/Context Management (→ Procd) ===
			contexts := sandboxes.Group("/:id/contexts")
			{
				contexts.POST("", s.createContext)
				contexts.GET("", s.listContexts)
				contexts.GET("/:ctx_id", s.getContext)
				contexts.DELETE("/:ctx_id", s.deleteContext)
				contexts.POST("/:ctx_id/restart", s.restartContext)
				contexts.POST("/:ctx_id/input", s.contextInput)
				contexts.POST("/:ctx_id/exec", s.contextExec)
				contexts.POST("/:ctx_id/resize", s.contextResize)
				contexts.POST("/:ctx_id/signal", s.contextSignal)
				contexts.GET("/:ctx_id/stats", s.contextStats)
				contexts.GET("/:ctx_id/ws", s.contextWebSocket)
			}

			// === SandboxVolume Management (→ Procd) ===
			sandboxvolumes := sandboxes.Group("/:id/sandboxvolumes")
			{
				sandboxvolumes.POST("/mount", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.mountSandboxVolume)
				sandboxvolumes.POST("/unmount", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.unmountSandboxVolume)
				sandboxvolumes.GET("/status", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.getSandboxVolumeStatus)
			}

			// === File System (→ Procd) ===
			files := sandboxes.Group("/:id/files")
			{
				files.GET("", s.handleFileOperation)
				files.POST("", s.handleFileOperation)
				files.DELETE("", s.handleFileOperation)
				files.GET("/watch", s.handleFileWatch)
				files.POST("/move", s.handleFileMove)
				files.GET("/stat", s.handleFileStat)
				files.GET("/list", s.handleFileList)
			}
		}

		// === Template Management (→ Manager) ===
		templates := v1.Group("/templates")
		templates.Use(s.managerUpstreamMiddleware())
		{
			templates.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermTemplateRead), s.listTemplates)
			templates.GET("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermTemplateRead), s.getTemplate)
			templates.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermTemplateCreate), s.createTemplate)
			templates.PUT("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermTemplateWrite), s.updateTemplate)
			templates.DELETE("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermTemplateDelete), s.deleteTemplate)
		}

		// === Registry Credentials (→ Manager) ===
		registry := v1.Group("/registry")
		registry.Use(s.managerUpstreamMiddleware())
		{
			registry.POST("/credentials", s.getRegistryCredentials)
		}

		// === SandboxVolume Management (→ Storage Proxy) ===
		sandboxvolumes := v1.Group("/sandboxvolumes")
		sandboxvolumes.Use(s.storageProxyUpstreamMiddleware())
		{
			sandboxvolumes.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeCreate), s.createSandboxVolume)
			sandboxvolumes.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeRead), s.listSandboxVolumes)
			sandboxvolumes.GET("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeRead), s.getSandboxVolume)
			sandboxvolumes.DELETE("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeDelete), s.deleteSandboxVolume)
			sandboxvolumes.POST("/:id/fork", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeWrite), s.forkSandboxVolume)
			// Snapshot/Restore (→ Storage Proxy)
			snapshots := sandboxvolumes.Group("/:id/snapshots")
			{
				snapshots.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeWrite), s.createSandboxVolumeSnapshot)
				snapshots.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeRead), s.listSandboxVolumeSnapshots)
				snapshots.GET("/:snapshot_id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeRead), s.getSandboxVolumeSnapshot)
				snapshots.POST("/:snapshot_id/restore", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeWrite), s.restoreSandboxVolumeSnapshot)
				snapshots.DELETE("/:snapshot_id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeDelete), s.deleteSandboxVolumeSnapshot)
			}
		}
	}

	// Internal API routes (for scheduler to call)
	// These routes are authenticated but don't require specific permissions
	// (scheduler uses *:* permissions)
	if s.internalAuthEnabled {
		internal := s.router.Group("/internal/v1")
		internal.Use(s.managerUpstreamMiddleware())
		internal.Use(s.authMiddleware.Authenticate())
		{

			// Cluster information (→ Manager)
			internal.GET("/cluster/summary", s.getClusterSummary)

			// Template statistics (→ Manager)
			internal.GET("/templates/stats", s.getTemplateStats)
		}
	}

	// Host-based public exposure fallback (for non-/api paths)
	s.router.NoRoute(s.handlePublicExposureNoRoute)
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

		// Close sandbox cache to stop cleanup goroutine
		if s.sandboxAddrCache != nil {
			s.sandboxAddrCache.Close()
		}

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
	if authModeEnabled(s.cfg.AuthMode, authModePublic) && s.publicIdentityRepo != nil {
		if err := s.publicIdentityRepo.Pool().Ping(c.Request.Context()); err != nil {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "database unavailable", gin.H{
				"status": "not ready",
			})
			return
		}
	}

	// Include cache stats in readiness check
	cacheStats := s.sandboxAddrCache.Stats()

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
		"sandbox_addr_cache": gin.H{
			"size":     cacheStats.Size,
			"hits":     cacheStats.Hits,
			"misses":   cacheStats.Misses,
			"hit_rate": cacheStats.HitRate,
		},
	})
}

func (s *Server) managerUpstreamMiddleware() gin.HandlerFunc {
	return s.requireUpstream(
		func() bool {
			return strings.TrimSpace(s.cfg.ManagerURL) != "" && s.proxy2Mgr != nil && s.managerClient != nil
		},
		func() []zap.Field {
			return []zap.Field{zap.String("manager_url", s.cfg.ManagerURL)}
		},
		"Manager upstream not configured",
		"manager upstream not configured",
		"manager_url is empty",
	)
}

func (s *Server) storageProxyUpstreamMiddleware() gin.HandlerFunc {
	return s.requireUpstream(
		func() bool {
			return strings.TrimSpace(s.cfg.StorageProxyURL) != "" && s.proxy2sp != nil
		},
		func() []zap.Field {
			return []zap.Field{zap.String("storage_proxy_url", s.cfg.StorageProxyURL)}
		},
		"Storage-proxy upstream not configured",
		"storage-proxy upstream not configured",
		"storage_proxy_url is empty",
	)
}

func (s *Server) requireUpstream(
	ready func() bool,
	logFields func() []zap.Field,
	logMessage string,
	clientMessage string,
	detail any,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		if ready != nil && ready() {
			c.Next()
			return
		}

		if s.logger != nil {
			fields := []zap.Field(nil)
			if logFields != nil {
				fields = logFields()
			}
			s.logger.Error(logMessage, fields...)
		}
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, clientMessage, detail)
		c.Abort()
	}
}

const (
	authModeInternal = "internal"
	authModePublic   = "public"
	authModeBoth     = "both"
)

func normalizeAuthMode(mode string) string {
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		return authModeInternal
	}
	switch mode {
	case authModeInternal, authModePublic, authModeBoth:
		return mode
	default:
		return authModeInternal
	}
}

func authModeEnabled(mode, target string) bool {
	mode = normalizeAuthMode(mode)
	if mode == authModeBoth {
		return true
	}
	return mode == target
}
