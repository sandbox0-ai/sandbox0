package http

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	memcachepkg "github.com/sandbox0-ai/sandbox0/global-gateway/pkg/memcache"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	gatewaybuiltin "github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/builtin"
	gatewayoidc "github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/oidc"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/http/handlers"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/public"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
)

// Server provides the global gateway HTTP API.
type Server struct {
	router          *gin.Engine
	cfg             *config.GlobalGatewayConfig
	pool            *pgxpool.Pool
	identityRepo    *identity.Repository
	teamLookup      teamDirectory
	regionRepo      *tenantdir.Repository
	regionLookup    regionDirectory
	authMiddleware  *gatewaymiddleware.AuthMiddleware
	rateLimiter     *gatewaymiddleware.RateLimiter
	requestLogger   *gatewaymiddleware.RequestLogger
	builtinProvider *gatewaybuiltin.Provider
	oidcManager     *gatewayoidc.Manager
	jwtIssuer       *authn.Issuer
	entitlements    licensing.Entitlements
	obsProvider     *observability.Provider
	logger          *zap.Logger
	proxyTimeout    time.Duration
	httpClient      *stdhttp.Client
	regionProxies   map[string]*proxy.Router
	regionProxiesMu sync.RWMutex
	regionRoutes    *memcachepkg.Cache[string, tenantdir.Region]
}

type regionDirectory interface {
	GetRegion(ctx context.Context, regionID string) (*tenantdir.Region, error)
}

type teamDirectory interface {
	GetTeamByID(ctx context.Context, teamID string) (*identity.Team, error)
}

type serverOptions struct {
	teamCreationHook identity.TeamCreationHook
}

// ServerOption configures optional global-gateway integrations.
type ServerOption func(*serverOptions)

// WithTeamCreationHook extends all team creation transactions in the global
// identity repository.
func WithTeamCreationHook(hook identity.TeamCreationHook) ServerOption {
	return func(options *serverOptions) {
		options.teamCreationHook = hook
	}
}

const regionRouteCacheTTL = 8 * time.Hour
const regionRouteCacheMaxEntries = 256

// NewServer creates a new global-gateway server.
func NewServer(
	cfg *config.GlobalGatewayConfig,
	pool *pgxpool.Pool,
	logger *zap.Logger,
	obsProvider *observability.Provider,
	opts ...ServerOption,
) (*Server, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}
	if pool == nil {
		return nil, fmt.Errorf("database pool is required")
	}

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	var options serverOptions
	for _, opt := range opts {
		opt(&options)
	}
	identityOptions := make([]identity.RepositoryOption, 0, 1)
	if options.teamCreationHook != nil {
		identityOptions = append(identityOptions, identity.WithTeamCreationHook(options.teamCreationHook))
	}
	identityRepo := identity.NewRepository(pool, identityOptions...)
	regionRepo := tenantdir.NewRepository(pool)
	jwtIssuer, err := authn.NewIssuerFromConfig(cfg.JWTIssuer, cfg.JWTSecret, cfg.JWTPrivateKeyPEM, cfg.JWTPublicKeyPEM, cfg.JWTPrivateKeyFile, cfg.JWTPublicKeyFile, cfg.JWTAccessTokenTTL.Duration, cfg.JWTRefreshTokenTTL.Duration)
	if err != nil {
		return nil, fmt.Errorf("create jwt issuer: %w", err)
	}
	authMiddleware := gatewaymiddleware.NewAuthMiddleware(nil, cfg.JWTSecret, jwtIssuer, logger)
	rateLimiter, err := gatewaymiddleware.NewRateLimiterWithConfig(context.Background(), cfg.RateLimitRPS, cfg.RateLimitBurst, gatewaymiddleware.RateLimitConfigFromGatewayConfig(cfg.GatewayConfig), logger)
	if err != nil {
		return nil, fmt.Errorf("create rate limiter: %w", err)
	}
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
			if err := ValidateInitUserHomeRegion(context.Background(), regionRepo, homeRegionID); err != nil {
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
		teamLookup:      identityRepo,
		regionRepo:      regionRepo,
		regionLookup:    regionRepo,
		authMiddleware:  authMiddleware,
		rateLimiter:     rateLimiter,
		requestLogger:   requestLogger,
		builtinProvider: builtinProvider,
		oidcManager:     oidcManager,
		jwtIssuer:       jwtIssuer,
		entitlements:    entitlements,
		obsProvider:     obsProvider,
		logger:          logger,
		proxyTimeout:    effectiveProxyTimeout(cfg.ServerWriteTimeout.Duration),
		httpClient:      obsProvider.HTTP.NewClient(httpobs.Config{Timeout: effectiveProxyTimeout(cfg.ServerWriteTimeout.Duration)}),
		regionProxies:   make(map[string]*proxy.Router),
		regionRoutes: memcachepkg.New[string, tenantdir.Region](memcachepkg.Config{
			MaxSize: regionRouteCacheMaxEntries,
			TTL:     regionRouteCacheTTL,
		}),
	}
	server.setupRoutes()
	return server, nil
}

// Handler returns the global gateway HTTP handler for embedders.
func (s *Server) Handler() stdhttp.Handler {
	return s.router
}

func (s *Server) setupRoutes() {
	s.router.Use(httpobs.GinMiddleware(s.obsProvider.HTTPServerConfig(nil)))
	s.router.Use(gatewaymiddleware.Recovery(s.logger))
	s.router.Use(s.requestLogger.Logger())
	s.router.Use(gatewaymiddleware.MarkLongLivedRequests())

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

	regionHandler := NewRegionHandler(s.regionRepo, s.logger)
	regions := s.router.Group("/regions")
	regions.Use(s.authMiddleware.Authenticate())
	if s.rateLimiter != nil {
		regions.Use(s.rateLimiter.RateLimit())
	}
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
	if s.handleRegionProxy(c) {
		return
	}
	c.AbortWithStatus(stdhttp.StatusNotFound)
}

func (s *Server) handleRegionProxy(c *gin.Context) bool {
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return false
	}
	path := c.Request.URL.Path
	apiKeyRoute := shouldProxyAPIKeyRegionRequest(c.Request.Method, path)
	humanRoute := shouldProxyHumanRegionRequest(c.Request.Method, path)
	if !apiKeyRoute && !humanRoute {
		return false
	}

	token, ok := extractBearerToken(c.GetHeader("Authorization"))
	if !ok {
		spec.JSONError(c, stdhttp.StatusUnauthorized, spec.CodeUnauthorized, "missing or invalid bearer credentials")
		return true
	}

	var regionID string
	apiKeyAuth := false
	if strings.HasPrefix(token, "s0_") {
		if !apiKeyRoute {
			spec.JSONError(c, stdhttp.StatusUnauthorized, spec.CodeUnauthorized, "JWT authentication required")
			return true
		}
		parsedRegionID, err := apikey.ParseRegionIDFromKey(token)
		if err != nil {
			spec.JSONError(c, stdhttp.StatusUnauthorized, spec.CodeUnauthorized, "invalid API key")
			return true
		}
		regionID = parsedRegionID
		apiKeyAuth = true
	} else {
		if !humanRoute {
			spec.JSONError(c, stdhttp.StatusUnauthorized, spec.CodeUnauthorized, "API key authentication required")
			return true
		}
		if s.authMiddleware == nil {
			spec.JSONError(c, stdhttp.StatusServiceUnavailable, spec.CodeUnavailable, "regional routing unavailable")
			return true
		}
		authCtx, err := s.authMiddleware.AuthenticateRequest(c)
		if err != nil || authCtx.AuthMethod != authn.AuthMethodJWT {
			spec.JSONError(c, stdhttp.StatusUnauthorized, spec.CodeUnauthorized, "invalid bearer credentials")
			return true
		}
		if strings.TrimSpace(authCtx.TeamID) == "" {
			spec.JSONError(c, stdhttp.StatusBadRequest, spec.CodeBadRequest, internalauth.TeamIDHeader+" is required")
			return true
		}
		if s.teamLookup == nil {
			spec.JSONError(c, stdhttp.StatusServiceUnavailable, spec.CodeUnavailable, "regional routing unavailable")
			return true
		}
		team, err := s.teamLookup.GetTeamByID(c.Request.Context(), authCtx.TeamID)
		if err != nil || team == nil || team.HomeRegionID == nil || strings.TrimSpace(*team.HomeRegionID) == "" {
			if err != nil && !errors.Is(err, identity.ErrTeamNotFound) {
				s.logger.Error("Failed to resolve team home region", zap.Error(err), zap.String("team_id", authCtx.TeamID))
			}
			spec.JSONError(c, stdhttp.StatusServiceUnavailable, spec.CodeUnavailable, "team home region unavailable")
			return true
		}
		regionID = strings.TrimSpace(*team.HomeRegionID)
	}

	return s.proxyToRegion(c, regionID, apiKeyAuth)
}

func (s *Server) proxyToRegion(c *gin.Context, regionID string, apiKeyAuth bool) bool {
	if s.regionLookup == nil {
		spec.JSONError(c, stdhttp.StatusServiceUnavailable, spec.CodeUnavailable, "region directory unavailable")
		return true
	}

	region, err := s.resolveRoutableRegion(c.Request.Context(), regionID)
	if err != nil {
		if errors.Is(err, tenantdir.ErrRegionNotFound) {
			if apiKeyAuth {
				spec.JSONError(c, stdhttp.StatusUnauthorized, spec.CodeUnauthorized, "invalid API key")
				return true
			}
			spec.JSONError(c, stdhttp.StatusServiceUnavailable, spec.CodeUnavailable, "region gateway unavailable")
			return true
		}
		s.logger.Error("Failed to resolve request region", zap.Error(err), zap.String("region_id", regionID))
		spec.JSONError(c, stdhttp.StatusServiceUnavailable, spec.CodeUnavailable, "failed to resolve region")
		return true
	}
	if region == nil || !region.Enabled || strings.TrimSpace(region.RegionalGatewayURL) == "" {
		spec.JSONError(c, stdhttp.StatusServiceUnavailable, spec.CodeUnavailable, "region gateway unavailable")
		return true
	}

	router, err := s.getRegionProxy(region.RegionalGatewayURL)
	if err != nil {
		s.logger.Error("Failed to initialize region proxy", zap.Error(err), zap.String("region_id", region.ID), zap.String("url", region.RegionalGatewayURL))
		spec.JSONError(c, stdhttp.StatusInternalServerError, spec.CodeInternal, "proxy initialization failed")
		return true
	}

	router.ProxyToTarget(c)
	return true
}

func shouldProxyAPIKeyRegionRequest(method string, path string) bool {
	if path == "/api" || strings.HasPrefix(path, "/api/") {
		return true
	}
	return method == stdhttp.MethodGet && path == "/api-keys/current"
}

func shouldProxyHumanRegionRequest(_ string, path string) bool {
	if path == "/api" || strings.HasPrefix(path, "/api/") {
		return true
	}
	if path == "/users/me/ssh-keys" || strings.HasPrefix(path, "/users/me/ssh-keys/") {
		return true
	}
	return path != "/api-keys/current" && (path == "/api-keys" || strings.HasPrefix(path, "/api-keys/"))
}

func (s *Server) resolveRoutableRegion(ctx context.Context, regionID string) (*tenantdir.Region, error) {
	if cached, ok := s.getCachedRoutableRegion(regionID); ok {
		return cached, nil
	}

	region, err := s.regionLookup.GetRegion(ctx, regionID)
	if err != nil {
		return nil, err
	}
	if region != nil && region.Enabled && strings.TrimSpace(region.RegionalGatewayURL) != "" {
		s.putCachedRoutableRegion(regionID, region)
	}
	return region, nil
}

func (s *Server) getCachedRoutableRegion(regionID string) (*tenantdir.Region, bool) {
	if s.regionRoutes == nil {
		return nil, false
	}
	region, ok := s.regionRoutes.Get(regionID)
	if !ok {
		return nil, false
	}
	return &region, true
}

func (s *Server) putCachedRoutableRegion(regionID string, region *tenantdir.Region) {
	if region == nil || s.regionRoutes == nil {
		return
	}
	s.regionRoutes.Set(regionID, *region)
}

func (s *Server) invalidateRegionRouteCache() {
	if s.regionRoutes != nil {
		s.regionRoutes.Clear()
	}
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

	router, err := proxy.NewRouter(normalizedTargetURL, s.logger, s.proxyTimeout, proxy.WithHTTPClient(s.httpClient))
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
	spec.JSONSuccess(c, stdhttp.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
	})
}

func (s *Server) readinessCheck(c *gin.Context) {
	if err := s.pool.Ping(c.Request.Context()); err != nil {
		spec.JSONError(c, stdhttp.StatusServiceUnavailable, spec.CodeUnavailable, "database unavailable", gin.H{
			"status": "not ready",
		})
		return
	}
	spec.JSONSuccess(c, stdhttp.StatusOK, gin.H{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
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
	server.ConnState = httpobs.NewConnStateTracker(s.obsProvider.HTTPServerConfig(nil)).Wrap(server.ConnState)

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
