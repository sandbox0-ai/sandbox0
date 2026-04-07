package http

import (
	"context"
	"fmt"
	stdhttp "net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	gatewaybuiltin "github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/builtin"
	gatewayoidc "github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/oidc"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/http/handlers"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/public"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
)

// Server provides the global gateway HTTP API.
type Server struct {
	router          *gin.Engine
	cfg             *config.GlobalGatewayConfig
	pool            *pgxpool.Pool
	identityRepo    *identity.Repository
	regionRepo      *tenantdir.Repository
	regionLookup    regionDirectory
	authMiddleware  *gatewaymiddleware.AuthMiddleware
	requestLogger   *gatewaymiddleware.RequestLogger
	builtinProvider *gatewaybuiltin.Provider
	oidcManager     *gatewayoidc.Manager
	jwtIssuer       *authn.Issuer
	entitlements    licensing.Entitlements
	obsProvider     *observability.Provider
	logger          *zap.Logger
	proxyTimeout    time.Duration
	regionProxies   map[string]*proxy.Router
	regionProxiesMu sync.RWMutex
	regionRoutes    map[string]cachedRegionRoute
	regionRoutesMu  sync.RWMutex
	now             func() time.Time
}

type regionDirectory interface {
	GetRegion(ctx context.Context, regionID string) (*tenantdir.Region, error)
}

type cachedRegionRoute struct {
	region    tenantdir.Region
	expiresAt time.Time
}

const regionRouteCacheTTL = 8 * time.Hour

// NewServer creates a new global-gateway server.
func NewServer(
	cfg *config.GlobalGatewayConfig,
	pool *pgxpool.Pool,
	logger *zap.Logger,
	obsProvider *observability.Provider,
) (*Server, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}
	if pool == nil {
		return nil, fmt.Errorf("database pool is required")
	}

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	identityRepo := identity.NewRepository(pool)
	regionRepo := tenantdir.NewRepository(pool)
	jwtIssuer, err := authn.NewIssuerFromConfig(cfg.JWTIssuer, cfg.JWTSecret, cfg.JWTPrivateKeyPEM, cfg.JWTPublicKeyPEM, cfg.JWTPrivateKeyFile, cfg.JWTPublicKeyFile, cfg.JWTAccessTokenTTL.Duration, cfg.JWTRefreshTokenTTL.Duration)
	if err != nil {
		return nil, fmt.Errorf("create jwt issuer: %w", err)
	}
	authMiddleware := gatewaymiddleware.NewAuthMiddleware(nil, cfg.JWTSecret, jwtIssuer, logger)
	requestLogger := gatewaymiddleware.NewRequestLogger(logger)
	builtinProvider := gatewaybuiltin.NewProvider(identityRepo, &cfg.BuiltInAuth, cfg.DefaultTeamName)

	entitlements := licensing.NewStaticEntitlements()
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
		oidcManager, err = gatewayoidc.NewManager(context.Background(), &cfg.GatewayConfig, identityRepo, logger)
		if err != nil {
			logger.Warn("Failed to initialize OIDC manager", zap.Error(err))
		}
	}
	if cfg.BuiltInAuth.InitUser != nil && (cfg.BuiltInAuth.Enabled || oidcConfigured) {
		if userCount, err := identityRepo.CountUsers(context.Background()); err == nil && userCount == 0 {
			homeRegionID := strings.TrimSpace(cfg.BuiltInAuth.InitUser.HomeRegionID)
			if err := handlers.ValidateInitUserHomeRegion(context.Background(), regionRepo, homeRegionID); err != nil {
				return nil, err
			}
		}
		if err := builtinProvider.EnsureInitUser(context.Background()); err != nil {
			logger.Warn("Failed to ensure init user", zap.Error(err))
		}
	}

	server := &Server{
		router:          router,
		cfg:             cfg,
		pool:            pool,
		identityRepo:    identityRepo,
		regionRepo:      regionRepo,
		regionLookup:    regionRepo,
		authMiddleware:  authMiddleware,
		requestLogger:   requestLogger,
		builtinProvider: builtinProvider,
		oidcManager:     oidcManager,
		jwtIssuer:       jwtIssuer,
		entitlements:    entitlements,
		obsProvider:     obsProvider,
		logger:          logger,
		proxyTimeout:    effectiveProxyTimeout(cfg.ServerWriteTimeout.Duration),
		regionProxies:   make(map[string]*proxy.Router),
		regionRoutes:    make(map[string]cachedRegionRoute),
		now:             time.Now,
	}
	server.setupRoutes()
	return server, nil
}

// Handler returns the HTTP handler for tests.
func (s *Server) Handler() stdhttp.Handler {
	return s.router
}

func (s *Server) setupRoutes() {
	s.router.Use(httpobs.GinMiddleware(httpobs.ServerConfig{
		Tracer: s.obsProvider.Tracer(),
	}))
	s.router.Use(gatewaymiddleware.Recovery(s.logger))
	s.router.Use(s.requestLogger.Logger())

	s.router.GET("/healthz", s.healthCheck)
	s.router.GET("/readyz", s.readinessCheck)
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	s.router.GET("/metadata", handlers.GatewayMetadata("global-gateway", handlers.GatewayModeGlobal))

	public.RegisterIdentityRoutes(s.router, public.Deps{
		IdentityRepo:            s.identityRepo,
		AuthMiddleware:          s.authMiddleware,
		BuiltinProvider:         s.builtinProvider,
		OIDCManager:             s.oidcManager,
		Entitlements:            s.entitlements,
		JWTIssuer:               s.jwtIssuer,
		RegionRepo:              s.regionRepo,
		RequireCreateHomeRegion: true,
		Logger:                  s.logger,
	})

	regionHandler := handlers.NewRegionHandler(s.regionRepo, s.logger)
	regions := s.router.Group("/regions")
	regions.Use(s.authMiddleware.Authenticate())
	regions.Use(s.authMiddleware.RequireJWTAuth())
	{
		regions.GET("", regionHandler.ListRegions)
		regionsAdmin := regions.Group("")
		regionsAdmin.Use(s.authMiddleware.RequireSystemAdmin())
		{
			regionsAdmin.POST("", s.invalidateRegionRouteCacheOnWrite(regionHandler.CreateRegion))
			regionsAdmin.GET("/:id", regionHandler.GetRegion)
			regionsAdmin.PUT("/:id", s.invalidateRegionRouteCacheOnWrite(regionHandler.UpdateRegion))
			regionsAdmin.DELETE("/:id", s.invalidateRegionRouteCacheOnWrite(regionHandler.DeleteRegion))
		}
	}

	s.router.NoRoute(s.handleNoRoute)
}

func effectiveProxyTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 30 * time.Second
	}
	return timeout
}

func (s *Server) handleNoRoute(c *gin.Context) {
	if s.handleAPIKeyRegionProxy(c) {
		return
	}
	c.AbortWithStatus(stdhttp.StatusNotFound)
}

func (s *Server) handleAPIKeyRegionProxy(c *gin.Context) bool {
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return false
	}
	path := c.Request.URL.Path
	if path != "/api" && !strings.HasPrefix(path, "/api/") {
		return false
	}

	token, ok := extractBearerToken(c.GetHeader("Authorization"))
	if !ok || !strings.HasPrefix(token, "s0_") {
		return false
	}

	regionID, err := apikey.ParseRegionIDFromKey(token)
	if err != nil {
		c.AbortWithStatusJSON(stdhttp.StatusUnauthorized, gin.H{"error": "invalid api key"})
		return true
	}
	if s.regionLookup == nil {
		c.AbortWithStatusJSON(stdhttp.StatusInternalServerError, gin.H{"error": "region directory unavailable"})
		return true
	}

	region, err := s.resolveRoutableRegion(c.Request.Context(), regionID)
	if err != nil {
		if err == tenantdir.ErrRegionNotFound {
			c.AbortWithStatusJSON(stdhttp.StatusNotFound, gin.H{"error": "region not found"})
			return true
		}
		s.logger.Error("Failed to resolve API key region", zap.Error(err), zap.String("region_id", regionID))
		c.AbortWithStatusJSON(stdhttp.StatusInternalServerError, gin.H{"error": "failed to resolve region"})
		return true
	}
	if !region.Enabled || strings.TrimSpace(region.RegionalGatewayURL) == "" {
		c.AbortWithStatusJSON(stdhttp.StatusServiceUnavailable, gin.H{"error": "region gateway unavailable"})
		return true
	}

	router, err := s.getRegionProxy(region.RegionalGatewayURL)
	if err != nil {
		s.logger.Error("Failed to initialize region proxy", zap.Error(err), zap.String("region_id", region.ID), zap.String("url", region.RegionalGatewayURL))
		c.AbortWithStatusJSON(stdhttp.StatusInternalServerError, gin.H{"error": "proxy initialization failed"})
		return true
	}

	router.ProxyToTarget(c)
	return true
}

func (s *Server) resolveRoutableRegion(ctx context.Context, regionID string) (*tenantdir.Region, error) {
	if cached, ok := s.getCachedRoutableRegion(regionID); ok {
		return cached, nil
	}

	region, err := s.regionLookup.GetRegion(ctx, regionID)
	if err != nil {
		return nil, err
	}
	if region.Enabled && strings.TrimSpace(region.RegionalGatewayURL) != "" {
		s.putCachedRoutableRegion(regionID, region)
	}
	return region, nil
}

func (s *Server) getCachedRoutableRegion(regionID string) (*tenantdir.Region, bool) {
	now := time.Now
	if s.now != nil {
		now = s.now
	}

	s.regionRoutesMu.RLock()
	entry, ok := s.regionRoutes[regionID]
	s.regionRoutesMu.RUnlock()
	if !ok || !entry.expiresAt.After(now()) {
		return nil, false
	}
	region := entry.region
	return &region, true
}

func (s *Server) putCachedRoutableRegion(regionID string, region *tenantdir.Region) {
	if region == nil {
		return
	}
	now := time.Now
	if s.now != nil {
		now = s.now
	}

	s.regionRoutesMu.Lock()
	if s.regionRoutes == nil {
		s.regionRoutes = make(map[string]cachedRegionRoute)
	}
	s.regionRoutes[regionID] = cachedRegionRoute{
		region:    *region,
		expiresAt: now().Add(regionRouteCacheTTL),
	}
	s.regionRoutesMu.Unlock()
}

func (s *Server) invalidateRegionRouteCache() {
	s.regionRoutesMu.Lock()
	clear(s.regionRoutes)
	s.regionRoutesMu.Unlock()
}

func (s *Server) invalidateRegionRouteCacheOnWrite(next gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		next(c)
		status := c.Writer.Status()
		if status >= stdhttp.StatusOK && status < stdhttp.StatusBadRequest {
			s.invalidateRegionRouteCache()
		}
	}
}

func extractBearerToken(authHeader string) (string, bool) {
	parts := strings.SplitN(strings.TrimSpace(authHeader), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", false
	}
	return strings.TrimSpace(parts[1]), true
}

func (s *Server) getRegionProxy(targetURL string) (*proxy.Router, error) {
	normalizedTargetURL := strings.TrimSpace(targetURL)
	s.regionProxiesMu.RLock()
	existing := s.regionProxies[normalizedTargetURL]
	s.regionProxiesMu.RUnlock()
	if existing != nil {
		return existing, nil
	}

	router, err := proxy.NewRouter(normalizedTargetURL, s.logger, s.proxyTimeout)
	if err != nil {
		return nil, err
	}

	s.regionProxiesMu.Lock()
	defer s.regionProxiesMu.Unlock()
	if existing = s.regionProxies[normalizedTargetURL]; existing != nil {
		return existing, nil
	}
	s.regionProxies[normalizedTargetURL] = router
	return router, nil
}

func (s *Server) healthCheck(c *gin.Context) {
	c.JSON(stdhttp.StatusOK, gin.H{
		"status":  "ok",
		"service": "global-gateway",
	})
}

func (s *Server) readinessCheck(c *gin.Context) {
	if err := s.pool.Ping(c.Request.Context()); err != nil {
		c.JSON(stdhttp.StatusServiceUnavailable, gin.H{
			"error": "database not ready",
		})
		return
	}
	c.JSON(stdhttp.StatusOK, gin.H{
		"status": "ready",
	})
}

// Start starts the HTTP server and blocks until it exits or the context is canceled.
func (s *Server) Start(ctx context.Context) error {
	server := &stdhttp.Server{
		Addr:         fmt.Sprintf(":%d", s.cfg.HTTPPort),
		Handler:      s.router,
		ReadTimeout:  s.cfg.ServerReadTimeout.Duration,
		WriteTimeout: s.cfg.ServerWriteTimeout.Duration,
		IdleTimeout:  s.cfg.ServerIdleTimeout.Duration,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout.Duration)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("Failed to shutdown HTTP server", zap.Error(err))
		}
	}()

	if err := server.ListenAndServe(); err != nil && err != stdhttp.ErrServerClosed {
		return err
	}
	return nil
}
