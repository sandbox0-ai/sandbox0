package http

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	gatewayhandlers "github.com/sandbox0-ai/sandbox0/pkg/gateway/http/handlers"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"go.uber.org/zap"
)

// Server represents the HTTP server for function-gateway.
type Server struct {
	router          *gin.Engine
	cfg             *config.FunctionGatewayConfig
	pool            *pgxpool.Pool
	functionRepo    *functions.Repository
	apiKeyRepo      *apikey.Repository
	authMiddleware  *middleware.AuthMiddleware
	rateLimiter     *middleware.RateLimiter
	requestLogger   *middleware.RequestLogger
	internalAuthGen *internalauth.Generator
	obsProvider     *observability.Provider
	httpClient      *http.Client
	routeLimiters   sync.Map
	logger          *zap.Logger
}

// NewServer creates a new HTTP server.
func NewServer(
	cfg *config.FunctionGatewayConfig,
	pool *pgxpool.Pool,
	logger *zap.Logger,
	obsProvider *observability.Provider,
) (*Server, error) {
	gin.SetMode(gin.ReleaseMode)

	if cfg.HTTPPort == 0 {
		cfg.HTTPPort = 8080
	}
	if strings.TrimSpace(cfg.InternalAuthCaller) == "" {
		cfg.InternalAuthCaller = internalauth.ServiceFunctionGateway
	}
	if cfg.InternalAuthTTL.Duration == 0 {
		cfg.InternalAuthTTL.Duration = 30 * time.Second
	}
	if cfg.ProxyTimeout.Duration == 0 {
		cfg.ProxyTimeout.Duration = 30 * time.Second
	}
	if strings.TrimSpace(cfg.FunctionRootDomain) == "" {
		cfg.FunctionRootDomain = config.DefaultFunctionRootDomain
	}

	httpClient := obsProvider.HTTP.NewClient(httpobs.Config{
		Timeout: cfg.ProxyTimeout.Duration,
	})

	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load internal JWT private key: %w", err)
	}
	internalAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     cfg.InternalAuthCaller,
		PrivateKey: privateKey,
		TTL:        cfg.InternalAuthTTL.Duration,
	})

	jwtIssuer, err := authn.NewIssuerFromConfig(cfg.JWTIssuer, cfg.JWTSecret, cfg.JWTPrivateKeyPEM, cfg.JWTPublicKeyPEM, cfg.JWTPrivateKeyFile, cfg.JWTPublicKeyFile, cfg.JWTAccessTokenTTL.Duration, cfg.JWTRefreshTokenTTL.Duration)
	if err != nil {
		return nil, fmt.Errorf("create jwt issuer: %w", err)
	}

	apiKeyRepo := apikey.NewRepository(pool)
	authMiddlewareOptions := []middleware.AuthMiddlewareOption(nil)
	if strings.TrimSpace(cfg.RegionID) != "" {
		authMiddlewareOptions = append(authMiddlewareOptions, middleware.WithRequiredTeamRegionID(cfg.RegionID))
	}

	server := &Server{
		router:          gin.New(),
		cfg:             cfg,
		pool:            pool,
		functionRepo:    functions.NewRepository(pool),
		apiKeyRepo:      apiKeyRepo,
		authMiddleware:  middleware.NewAuthMiddleware(apiKeyRepo, cfg.JWTSecret, jwtIssuer, logger, authMiddlewareOptions...),
		rateLimiter:     middleware.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst, cfg.RateLimitCleanupInterval.Duration, logger),
		requestLogger:   middleware.NewRequestLogger(logger),
		internalAuthGen: internalAuthGen,
		obsProvider:     obsProvider,
		httpClient:      httpClient,
		logger:          logger,
	}

	server.setupRoutes()
	return server, nil
}

func (s *Server) setupRoutes() {
	s.router.Use(httpobs.GinMiddleware(s.obsProvider.HTTPServerConfig(nil)))
	s.router.Use(middleware.Recovery(s.logger))
	s.router.Use(s.requestLogger.Logger())
	s.router.Use(middleware.MarkLongLivedRequests())
	s.router.Use(middleware.UpstreamTimeoutWhitelist())

	s.router.GET("/healthz", s.healthCheck)
	s.router.GET("/readyz", s.readinessCheck)
	s.router.GET("/metadata", gatewayhandlers.GatewayMetadata("function-gateway", gatewayhandlers.GatewayModeDirect))
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	api := s.router.Group("/api/v1")
	api.Use(s.authMiddleware.Authenticate())
	api.Use(s.rateLimiter.RateLimit())
	api.Use(s.requireTeamContextForTeamScopedAPI())
	{
		functionsGroup := api.Group("/functions")
		{
			functionsGroup.GET("", s.authMiddleware.RequirePermission(authn.PermFunctionRead), s.listFunctions)
			functionsGroup.POST("", s.authMiddleware.RequirePermission(authn.PermFunctionCreate), s.createFunction)
			functionsGroup.GET("/:id", s.authMiddleware.RequirePermission(authn.PermFunctionRead), s.getFunction)
			functionsGroup.GET("/:id/revisions", s.authMiddleware.RequirePermission(authn.PermFunctionRead), s.listFunctionRevisions)
			functionsGroup.POST("/:id/revisions", s.authMiddleware.RequirePermission(authn.PermFunctionWrite), s.createFunctionRevision)
			functionsGroup.PUT("/:id/aliases/:alias", s.authMiddleware.RequirePermission(authn.PermFunctionWrite), s.setFunctionAlias)
		}
	}

	s.router.NoRoute(s.handleNoRoute)
}

// Handler exposes the HTTP handler for tests.
func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.cfg.HTTPPort)
	s.logger.Info("Starting HTTP server", zap.String("addr", addr), zap.Int("port", s.cfg.HTTPPort))

	server := &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  durationOrDefault(s.cfg.ServerReadTimeout.Duration, 30*time.Second),
		WriteTimeout: durationOrDefault(s.cfg.ServerWriteTimeout.Duration, 60*time.Second),
		IdleTimeout:  durationOrDefault(s.cfg.ServerIdleTimeout.Duration, 120*time.Second),
	}

	errChan := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		s.logger.Info("Shutting down HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), durationOrDefault(s.cfg.ShutdownTimeout.Duration, 30*time.Second))
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}

func (s *Server) outboundHTTPClient() *http.Client {
	if s != nil && s.httpClient != nil {
		return s.httpClient
	}
	return &http.Client{}
}

func durationOrDefault(value, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}
	return value
}

func (s *Server) healthCheck(c *gin.Context) {
	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
	})
}

func (s *Server) readinessCheck(c *gin.Context) {
	if s.pool != nil {
		if err := s.pool.Ping(c.Request.Context()); err != nil {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "database unavailable", gin.H{
				"status": "not ready",
			})
			return
		}
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
	})
}

func (s *Server) requireTeamContextForTeamScopedAPI() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := middleware.GetAuthContext(c)
		if authCtx == nil || strings.TrimSpace(authCtx.TeamID) != "" {
			c.Next()
			return
		}
		if authCtx.AuthMethod == authn.AuthMethodAPIKey && authCtx.IsSystemAdmin {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "x-team-id is required for team-scoped platform API key requests")
			c.Abort()
			return
		}
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "team context is required")
		c.Abort()
	}
}

func (s *Server) handleNoRoute(c *gin.Context) {
	if c.Request != nil && c.Request.URL != nil {
		path := c.Request.URL.Path
		if path == "/api" || strings.HasPrefix(path, "/api/") {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "not found")
			return
		}
	}

	label, ok := s.functionDomainLabelFromRequest(c)
	if !ok {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "not found")
		return
	}
	fn, err := s.functionRepo.GetFunctionByDomainLabel(c.Request.Context(), label)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
		return
	}
	rev, err := s.functionRepo.GetActiveRevision(c.Request.Context(), fn)
	if err != nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "function revision is not available")
		return
	}
	s.serveFunctionRevision(c, fn, rev)
}

func (s *Server) functionDomainLabelFromRequest(c *gin.Context) (string, bool) {
	host := hostWithoutPort(c.Request.Host)
	rootDomain := strings.Trim(strings.ToLower(s.cfg.FunctionRootDomain), ".")
	if rootDomain == "" {
		rootDomain = config.DefaultFunctionRootDomain
	}
	regionID := strings.Trim(strings.ToLower(s.cfg.FunctionRegionID), ".")
	if regionID == "" {
		regionID = strings.Trim(strings.ToLower(s.cfg.PublicRegionID), ".")
	}
	if regionID == "" {
		regionID = strings.Trim(strings.ToLower(s.cfg.RegionID), ".")
	}

	for _, suffix := range functionHostSuffixes(regionID, rootDomain) {
		if !strings.HasSuffix(host, suffix) {
			continue
		}
		label := strings.TrimSuffix(host, suffix)
		label = strings.TrimSuffix(label, ".")
		if label == "" || strings.Contains(label, ".") {
			return "", false
		}
		return label, true
	}
	return "", false
}

func functionHostSuffixes(regionID, rootDomain string) []string {
	if regionID == "" {
		return []string{"." + rootDomain}
	}
	return []string{"." + regionID + "." + rootDomain, "." + rootDomain}
}

func functionHost(domainLabel, regionID, rootDomain string) string {
	rootDomain = strings.Trim(strings.ToLower(rootDomain), ".")
	if rootDomain == "" {
		rootDomain = config.DefaultFunctionRootDomain
	}
	regionID = strings.Trim(strings.ToLower(regionID), ".")
	if regionID == "" {
		return strings.ToLower(domainLabel) + "." + rootDomain
	}
	return strings.ToLower(domainLabel) + "." + regionID + "." + rootDomain
}

func hostWithoutPort(hostport string) string {
	host := hostport
	if strings.Contains(hostport, ":") {
		if h, _, err := net.SplitHostPort(hostport); err == nil && h != "" {
			host = h
		}
	}
	return strings.ToLower(strings.TrimSpace(host))
}
