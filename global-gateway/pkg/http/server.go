package http

import (
	"context"
	"fmt"
	stdhttp "net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	tenantResolver  *tenantdir.Directory
	authMiddleware  *gatewaymiddleware.AuthMiddleware
	requestLogger   *gatewaymiddleware.RequestLogger
	builtinProvider *gatewaybuiltin.Provider
	oidcManager     *gatewayoidc.Manager
	jwtIssuer       *authn.Issuer
	entitlements    licensing.Entitlements
	obsProvider     *observability.Provider
	logger          *zap.Logger
}

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
	tenantResolver := tenantdir.NewResolver(identityRepo, regionRepo)
	jwtIssuer := authn.NewIssuer(cfg.JWTIssuer, cfg.JWTSecret, cfg.JWTAccessTokenTTL.Duration, cfg.JWTRefreshTokenTTL.Duration)
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
	var err error
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
		tenantResolver:  tenantResolver,
		authMiddleware:  authMiddleware,
		requestLogger:   requestLogger,
		builtinProvider: builtinProvider,
		oidcManager:     oidcManager,
		jwtIssuer:       jwtIssuer,
		entitlements:    entitlements,
		obsProvider:     obsProvider,
		logger:          logger,
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

	tenantHandler := handlers.NewTenantHandler(s.tenantResolver, s.jwtIssuer, s.cfg.RegionTokenTTL.Duration, s.logger)
	authProtected := s.router.Group("/auth")
	authProtected.Use(s.authMiddleware.Authenticate())
	authProtected.Use(s.authMiddleware.RequireJWTAuth())
	{
		authProtected.POST("/region-token", tenantHandler.IssueRegionToken)
	}

	regionHandler := handlers.NewRegionHandler(s.regionRepo, s.logger)
	regions := s.router.Group("/regions")
	regions.Use(s.authMiddleware.Authenticate())
	regions.Use(s.authMiddleware.RequireJWTAuth())
	{
		regions.GET("", regionHandler.ListRegions)
		regionsAdmin := regions.Group("")
		regionsAdmin.Use(s.authMiddleware.RequireSystemAdmin())
		{
			regionsAdmin.POST("", regionHandler.CreateRegion)
			regionsAdmin.GET("/:id", regionHandler.GetRegion)
			regionsAdmin.PUT("/:id", regionHandler.UpdateRegion)
			regionsAdmin.DELETE("/:id", regionHandler.DeleteRegion)
		}
	}
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
