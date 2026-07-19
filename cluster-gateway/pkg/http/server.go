package http

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	gatewayapikey "github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	gatewaybuiltin "github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/builtin"
	gatewayoidc "github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/oidc"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewayhandlers "github.com/sandbox0-ai/sandbox0/pkg/gateway/http/handlers"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/httpclient"
	gatewayidentity "github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/public"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/teamresources"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	licensinghttp "github.com/sandbox0-ai/sandbox0/pkg/licensing/http"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap"
)

type ServerOption func(*serverOptions)

type serverOptions struct {
	sandboxObservabilityRepo sandboxobservability.Repository
	meteringReader           gatewayhandlers.MeteringReader
	teamQuotaController      *gatewayteamquota.Controller
	teamQuotaPolicyManager   coreteamquota.PolicyManager
	publicOverloadGuard      *gatewaymiddleware.OverloadGuard
	sandboxServiceAbuseGuard ratelimit.Limiter
	apiKeyRepositoryFactory  func(*pgxpool.Pool) *gatewayapikey.Repository
}

func WithSandboxObservabilityRepository(repo sandboxobservability.Repository) ServerOption {
	return func(opts *serverOptions) {
		opts.sandboxObservabilityRepo = repo
	}
}

func WithMeteringReader(reader gatewayhandlers.MeteringReader) ServerOption {
	return func(opts *serverOptions) {
		opts.meteringReader = reader
	}
}

// WithTeamQuotaController overrides Team Quota dependencies for tests.
func WithTeamQuotaController(controller *gatewayteamquota.Controller) ServerOption {
	return func(opts *serverOptions) {
		opts.teamQuotaController = controller
	}
}

// WithTeamQuotaPolicyManager supplies the serialized region policy owner used by
// fullmode admin writes.
func WithTeamQuotaPolicyManager(manager coreteamquota.PolicyManager) ServerOption {
	return func(opts *serverOptions) {
		opts.teamQuotaPolicyManager = manager
	}
}

// WithPublicOverloadGuard overrides the public identity and exposure guard for tests.
func WithPublicOverloadGuard(guard *gatewaymiddleware.OverloadGuard) ServerOption {
	return func(opts *serverOptions) {
		opts.publicOverloadGuard = guard
	}
}

// WithSandboxServiceAbuseGuard overrides the distributed public-route guard
// for tests.
func WithSandboxServiceAbuseGuard(limiter ratelimit.Limiter) ServerOption {
	return func(opts *serverOptions) {
		opts.sandboxServiceAbuseGuard = limiter
	}
}

func withAPIKeyRepositoryFactoryForTest(
	factory func(*pgxpool.Pool) *gatewayapikey.Repository,
) ServerOption {
	return func(opts *serverOptions) {
		opts.apiKeyRepositoryFactory = factory
	}
}

// Server represents the HTTP server for cluster-gateway
type Server struct {
	router                                   *gin.Engine
	cfg                                      *config.ClusterGatewayConfig
	proxy2Mgr                                *proxy.Router
	proxy2ManagerStorage                     *proxy.Router
	managerClient                            *client.ManagerClient
	authMiddleware                           *middleware.InternalAuthMiddleware
	sandboxAuditIngestAuthMiddleware         *middleware.InternalAuthMiddleware
	sandboxObservabilityIngestAuthMiddleware *middleware.InternalAuthMiddleware
	publicAuth                               *gatewaymiddleware.AuthMiddleware
	compositeAuth                            *middleware.CompositeAuthMiddleware
	publicIdentityRepo                       *gatewayidentity.Repository
	publicAPIKeyRepo                         *gatewayapikey.Repository
	ownsPublicAPIKeyRepo                     bool
	teamQuotaController                      *gatewayteamquota.Controller
	ownsTeamQuotaController                  bool
	publicOverloadGuard                      *gatewaymiddleware.OverloadGuard
	ownsPublicOverloadGuard                  bool
	ownsSandboxServiceAbuseGuard             bool
	publicBuiltin                            *gatewaybuiltin.Provider
	publicOIDC                               *gatewayoidc.Manager
	publicJWT                                *gatewayauthn.Issuer
	requestLogger                            *middleware.RequestLogger
	logger                                   *zap.Logger
	meteringHandler                          *gatewayhandlers.MeteringHandler
	observabilityHandler                     *gatewayhandlers.SandboxObservabilityHandler
	auditSigningKey                          ed25519.PrivateKey
	auditDelivery                            *auditDelivery
	internalAuthGen                          *internalauth.Generator
	entitlements                             licensing.Entitlements
	sandboxAuditEntitlements                 licensing.Entitlements
	obsProvider                              *observability.Provider
	httpClient                               *http.Client
	sandboxServiceAbuseGuard                 ratelimit.Limiter
	sandboxInternalCache                     sandboxInternalCache
}

// NewServer creates a new HTTP server
func NewServer(
	cfg *config.ClusterGatewayConfig,
	pool *pgxpool.Pool,
	logger *zap.Logger,
	obsProvider *observability.Provider,
	opts ...ServerOption,
) (*Server, error) {
	options := serverOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

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

	var proxy2ManagerStorage *proxy.Router
	if strings.TrimSpace(cfg.ManagerStorageURL) != "" {
		var err error
		proxy2ManagerStorage, err = proxy.NewRouter(
			cfg.ManagerStorageURL,
			logger,
			proxyTimeout,
			proxy.WithHTTPClient(httpClient),
		)
		if err != nil {
			return nil, fmt.Errorf("create manager storage upstream router: %w", err)
		}
	}

	publicAuthEnabled := authModeEnabled(cfg.AuthMode, authModePublic)
	oidcConfigured := publicAuthEnabled && config.HasEnabledOIDCProviders(cfg.OIDCProviders)
	publicEntitlements, sandboxAuditEntitlements, err := resolveClusterGatewayEntitlements(cfg, oidcConfigured)
	if err != nil {
		return nil, err
	}

	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load internal JWT private key: %w", err)
	}

	// Control-plane requests and data-plane ingest use different key pairs in a
	// regional deployment. The private key mounted here belongs to the data
	// plane and is also the trust root for node-local ingest callers.
	dataPlanePublicKey := privateKey.Public().(ed25519.PublicKey)
	var controlPlanePublicKey ed25519.PublicKey
	if authModeEnabled(cfg.AuthMode, authModeInternal) {
		var err error
		controlPlanePublicKey, err = internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultInternalJWTPublicKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load internal JWT public key: %w", err)
		}
	}
	var auditNetdPublicKey ed25519.PublicKey
	var auditSigningPrivateKey ed25519.PrivateKey
	var auditSigningPublicKey ed25519.PublicKey
	if cfg.SandboxObservability.AuditEnabled {
		if strings.TrimSpace(cfg.RegionID) == "" || strings.TrimSpace(cfg.ClusterID) == "" {
			return nil, fmt.Errorf("sandbox audit requires region_id and cluster_id")
		}
		auditNetdPublicKey, err = internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultAuditJWTPublicKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load dedicated network audit producer JWT public key: %w", err)
		}
		auditSigningPrivateKey, err = internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultAuditSigningPrivateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load dedicated audit signing private key: %w", err)
		}
		auditSigningPublicKey, err = internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultAuditSigningPublicKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load dedicated audit signing public key: %w", err)
		}
		if !auditSigningPrivateKey.Public().(ed25519.PublicKey).Equal(auditSigningPublicKey) {
			return nil, fmt.Errorf("audit signing key pair does not match")
		}
	}

	// Create internal auth validator (for validating tokens from regional-gateway and optionally scheduler)
	allowedCallers := cfg.AllowedCallers
	if len(allowedCallers) == 0 {
		allowedCallers = []string{
			internalauth.ServiceRegionalGateway,
			internalauth.ServiceScheduler,
			internalauth.ServiceClusterGateway,
		}
	}
	validator, sandboxObservabilityIngestValidator, sandboxAuditIngestValidator := newInternalAuthValidators(
		cfg.AuthMode,
		allowedCallers,
		controlPlanePublicKey,
		dataPlanePublicKey,
		auditNetdPublicKey,
	)

	// Create middleware
	authMiddleware := middleware.NewInternalAuthMiddleware(validator, logger)
	sandboxObservabilityIngestAuthMiddleware := middleware.NewInternalAuthMiddleware(sandboxObservabilityIngestValidator, logger)
	sandboxAuditIngestAuthMiddleware := middleware.NewInternalAuthMiddleware(sandboxAuditIngestValidator, logger)
	requestLogger := middleware.NewRequestLogger(logger)

	// Initialize internal auth generator (for downstream services)
	internalAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     internalauth.ServiceClusterGateway,
		PrivateKey: privateKey,
		TTL:        10 * time.Second,
	})

	// Create manager client
	var managerClient *client.ManagerClient
	if strings.TrimSpace(cfg.ManagerURL) != "" {
		managerClient = client.NewManagerClient(cfg.ManagerURL, internalAuthGen, logger, proxyTimeout)
		managerClient.SetHTTPClient(httpClient)
	}

	var publicIdentityRepo *gatewayidentity.Repository
	var publicAPIKeyRepo *gatewayapikey.Repository
	publicAPIKeyRepositoryReady := false
	var publicAuth *gatewaymiddleware.AuthMiddleware
	var compositeAuth *middleware.CompositeAuthMiddleware
	var publicBuiltin *gatewaybuiltin.Provider
	var publicOIDC *gatewayoidc.Manager
	var publicJWT *gatewayauthn.Issuer

	if pool != nil {
		identityOptions := []gatewayidentity.RepositoryOption(nil)
		if publicAuthEnabled {
			identityOptions = append(
				identityOptions,
				gatewayidentity.WithIdentityResourceGuard(public.IdentityResourceGuardLimits(cfg.IdentityResourceGuard)),
			)
		}
		publicIdentityRepo = gatewayidentity.NewRepository(pool, identityOptions...)
		apiKeyRepositoryFactory := options.apiKeyRepositoryFactory
		if apiKeyRepositoryFactory == nil {
			apiKeyRepositoryFactory = func(pool *pgxpool.Pool) *gatewayapikey.Repository {
				return gatewayapikey.NewRepository(pool)
			}
		}
		publicAPIKeyRepo = apiKeyRepositoryFactory(pool)
		defer func() {
			if !publicAPIKeyRepositoryReady && publicAPIKeyRepo != nil {
				_ = publicAPIKeyRepo.Close()
			}
		}()
	}

	if publicAuthEnabled {
		if publicIdentityRepo == nil || publicAPIKeyRepo == nil {
			return nil, fmt.Errorf("public auth requires database connection")
		}

		edgeCfg := &config.GatewayConfig{
			DefaultTeamName:       cfg.DefaultTeamName,
			IdentityResourceGuard: cfg.IdentityResourceGuard,
			OIDCProviders:         cfg.OIDCProviders,
			OIDCStateTTL:          cfg.OIDCStateTTL,
			BaseURL:               cfg.BaseURL,
			RegionID:              cfg.RegionID,
			PublicExposureEnabled: cfg.PublicExposureEnabled,
			PublicRootDomain:      cfg.PublicRootDomain,
			PublicRegionID:        cfg.PublicRegionID,
		}

		builtinProvider := gatewaybuiltin.NewProvider(publicIdentityRepo, &cfg.BuiltInAuth, cfg.DefaultTeamName)
		var oidcManager *gatewayoidc.Manager
		if oidcConfigured {
			oidcManager, err = gatewayoidc.NewManager(context.Background(), edgeCfg, publicIdentityRepo, logger)
			if err != nil {
				logger.Warn("Failed to initialize OIDC manager", zap.Error(err))
			}
		}
		if cfg.BuiltInAuth.InitUser != nil && (cfg.BuiltInAuth.Enabled || oidcConfigured) {
			if err := builtinProvider.EnsureInitUser(context.Background()); err != nil {
				logger.Warn("Failed to ensure init user", zap.Error(err))
			}
		}

		jwtIssuer := gatewayauthn.NewIssuer(cfg.JWTIssuer, cfg.JWTSecret, cfg.JWTAccessTokenTTL.Duration, cfg.JWTRefreshTokenTTL.Duration)

		publicAuth = gatewaymiddleware.NewAuthMiddleware(publicAPIKeyRepo, cfg.JWTSecret, jwtIssuer, logger)
		compositeAuth = middleware.NewCompositeAuthMiddleware(authMiddleware, publicAuth, logger)
		publicBuiltin = builtinProvider
		publicOIDC = oidcManager
		publicJWT = jwtIssuer
	}

	meteringHandler := gatewayhandlers.NewMeteringHandler(options.meteringReader, cfg.RegionID, logger)
	observabilityOptions := []gatewayhandlers.SandboxObservabilityHandlerOption{
		gatewayhandlers.WithSandboxObservabilityIngestPolicy(gatewayhandlers.SandboxObservabilityIngestPolicy{
			RegionID:  cfg.RegionID,
			ClusterID: cfg.ClusterID,
		}),
	}
	if cfg.SandboxObservability.AuditEnabled {
		observabilityOptions = append(observabilityOptions, gatewayhandlers.WithAuditIntegrityPolicy(gatewayhandlers.AuditIntegrityPolicy{
			RegionID:        cfg.RegionID,
			ClusterID:       cfg.ClusterID,
			SigningKey:      auditSigningPrivateKey,
			VerificationKey: auditSigningPublicKey,
		}))
	}
	observabilityHandler := gatewayhandlers.NewSandboxObservabilityHandler(options.sandboxObservabilityRepo, logger, observabilityOptions...)
	var auditWriter auditEventInserter
	if writer, ok := options.sandboxObservabilityRepo.(auditEventInserter); ok {
		auditWriter = writer
	}
	var delivery *auditDelivery
	if cfg.SandboxObservability.AuditEnabled {
		if strings.TrimSpace(cfg.SandboxObservability.AuditSpoolDir) == "" {
			return nil, fmt.Errorf("sandbox audit requires audit_spool_dir")
		}
		delivery, err = newAuditDeliveryWithLimits(
			cfg.SandboxObservability.AuditSpoolDir,
			auditWriter,
			logger,
			auditSigningPublicKey,
			auditDeliveryLimitsFromConfig(cfg.SandboxObservability.AuditSpoolLimits),
		)
		if err != nil {
			return nil, fmt.Errorf("initialize sandbox audit delivery: %w", err)
		}
	}
	teamQuotaController := options.teamQuotaController
	ownsTeamQuotaController := false
	if teamQuotaController == nil {
		if pool == nil {
			return nil, fmt.Errorf("distributed Team Quota requires region database connection")
		}
		policyManager := options.teamQuotaPolicyManager
		var policyReader coreteamquota.PolicyReader = coreteamquota.NewRepository(pool)
		if policyManager != nil {
			policyReader = policyManager
		}
		distributedStore, ok := policyReader.(interface {
			coreteamquota.PolicyReader
			coreteamquota.TeamAdmissionStateResolver
		})
		if !ok {
			return nil, fmt.Errorf("team quota policy reader does not support admission state")
		}
		teamQuotaController, err = gatewayteamquota.NewDistributedController(
			context.Background(),
			distributedStore,
			publicIdentityRepo,
			cfg.RegionID,
			cfg.TeamQuota.DistributedEnforcement,
			logger,
			gatewayteamquota.WithPolicyManager(policyManager),
		)
		if err != nil {
			return nil, fmt.Errorf("create distributed Team Quota controller: %w", err)
		}
		ownsTeamQuotaController = true
	}
	publicOverloadGuard := options.publicOverloadGuard
	ownsPublicOverloadGuard := false
	if publicOverloadGuard == nil {
		publicOverloadGuard, err = gatewaymiddleware.NewSharedOverloadGuard(
			context.Background(),
			gatewaymiddleware.SharedOverloadGuardConfig(
				cfg.GatewayConfig,
				rediscache.JoinKeyPrefix("cluster-gateway", cfg.RegionID),
			),
			logger,
		)
		if err != nil {
			if ownsTeamQuotaController && teamQuotaController != nil {
				_ = teamQuotaController.Close()
			}
			return nil, fmt.Errorf("create public overload guard: %w", err)
		}
		ownsPublicOverloadGuard = true
	}
	sandboxServiceAbuseGuard := options.sandboxServiceAbuseGuard
	ownsSandboxServiceAbuseGuard := false
	if sandboxServiceAbuseGuard == nil {
		sandboxServiceAbuseGuard, err = ratelimit.New(context.Background(), sandboxServiceAbuseGuardConfig(cfg.GatewayConfig))
		if err != nil {
			if ownsPublicOverloadGuard {
				_ = publicOverloadGuard.Close()
			}
			if ownsTeamQuotaController && teamQuotaController != nil {
				_ = teamQuotaController.Close()
			}
			return nil, fmt.Errorf("create sandbox service abuse guard: %w", err)
		}
		ownsSandboxServiceAbuseGuard = true
	}
	sandboxInternalCache, err := newSandboxInternalCache(context.Background(), cfg.GatewayConfig)
	if err != nil {
		logger.Warn("Failed to initialize sandbox internal cache", zap.Error(err))
	}

	server := &Server{
		router:                                   router,
		cfg:                                      cfg,
		proxy2Mgr:                                proxy2Mgr,
		proxy2ManagerStorage:                     proxy2ManagerStorage,
		managerClient:                            managerClient,
		authMiddleware:                           authMiddleware,
		sandboxAuditIngestAuthMiddleware:         sandboxAuditIngestAuthMiddleware,
		sandboxObservabilityIngestAuthMiddleware: sandboxObservabilityIngestAuthMiddleware,
		publicAuth:                               publicAuth,
		compositeAuth:                            compositeAuth,
		publicIdentityRepo:                       publicIdentityRepo,
		publicAPIKeyRepo:                         publicAPIKeyRepo,
		ownsPublicAPIKeyRepo:                     publicAPIKeyRepo != nil,
		teamQuotaController:                      teamQuotaController,
		ownsTeamQuotaController:                  ownsTeamQuotaController,
		publicOverloadGuard:                      publicOverloadGuard,
		ownsPublicOverloadGuard:                  ownsPublicOverloadGuard,
		ownsSandboxServiceAbuseGuard:             ownsSandboxServiceAbuseGuard,
		publicBuiltin:                            publicBuiltin,
		publicOIDC:                               publicOIDC,
		publicJWT:                                publicJWT,
		requestLogger:                            requestLogger,
		logger:                                   logger,
		meteringHandler:                          meteringHandler,
		observabilityHandler:                     observabilityHandler,
		auditSigningKey:                          auditSigningPrivateKey,
		auditDelivery:                            delivery,
		internalAuthGen:                          internalAuthGen,
		entitlements:                             publicEntitlements,
		sandboxAuditEntitlements:                 sandboxAuditEntitlements,
		obsProvider:                              obsProvider,
		httpClient:                               httpClient,
		sandboxServiceAbuseGuard:                 sandboxServiceAbuseGuard,
		sandboxInternalCache:                     sandboxInternalCache,
	}

	server.setupRoutes()

	publicAPIKeyRepositoryReady = true
	return server, nil
}

func resolveClusterGatewayEntitlements(cfg *config.ClusterGatewayConfig, oidcConfigured bool) (licensing.Entitlements, licensing.Entitlements, error) {
	return resolveClusterGatewayEntitlementsWithLoader(cfg, oidcConfigured, licensing.LoadFileEntitlements)
}

func resolveClusterGatewayEntitlementsWithLoader(
	cfg *config.ClusterGatewayConfig,
	oidcConfigured bool,
	load func(string) licensing.Entitlements,
) (licensing.Entitlements, licensing.Entitlements, error) {
	publicEntitlements := licensing.NewStaticEntitlements(licensing.FeatureSSO)
	auditEntitlements := licensing.NewStaticEntitlements()
	auditConfigured := cfg != nil && cfg.SandboxObservability.AuditEnabled
	if !oidcConfigured && !auditConfigured {
		return publicEntitlements, auditEntitlements, nil
	}
	if cfg == nil {
		return nil, nil, fmt.Errorf("cluster-gateway config is required")
	}
	if err := licensing.RequireLicenseFile(cfg.LicenseFile); err != nil {
		return nil, nil, fmt.Errorf("license_file is required when enterprise features are configured: %w", err)
	}
	if load == nil {
		return nil, nil, fmt.Errorf("enterprise entitlement loader is required")
	}

	licenseEntitlements := load(cfg.LicenseFile)
	if oidcConfigured {
		if err := licenseEntitlements.Require(licensing.FeatureSSO); err != nil {
			return nil, nil, fmt.Errorf("enterprise SSO feature is required when OIDC providers are configured: %w", err)
		}
		publicEntitlements = licenseEntitlements
	}
	if auditConfigured {
		if err := licenseEntitlements.Require(licensing.FeatureSandboxAudit); err != nil {
			return nil, nil, fmt.Errorf("enterprise sandbox audit feature is required when sandbox audit is enabled: %w", err)
		}
		auditEntitlements = licenseEntitlements
	}
	return publicEntitlements, auditEntitlements, nil
}

// Handler exposes the HTTP handler for tests.
func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) sandboxObservabilityHandler() *gatewayhandlers.SandboxObservabilityHandler {
	if s.observabilityHandler == nil {
		logger := zap.NewNop()
		if s.logger != nil {
			logger = s.logger
		}
		s.observabilityHandler = gatewayhandlers.NewSandboxObservabilityHandler(nil, logger)
	}
	return s.observabilityHandler
}

func (s *Server) outboundHTTPClient() *http.Client {
	if s != nil {
		timeout := httpclient.DefaultTimeout
		if s.cfg != nil && s.cfg.ProxyTimeout.Duration > 0 {
			timeout = s.cfg.ProxyTimeout.Duration
		}
		return httpclient.Resolve(s.httpClient, timeout)
	}
	return httpclient.Resolve(nil, 0)
}

// setupRoutes configures all HTTP routes
func (s *Server) setupRoutes() {
	// Global middleware (order matters)
	s.router.Use(httpobs.GinMiddleware(s.obsProvider.HTTPServerConfig(nil)))
	s.router.Use(middleware.Recovery(s.logger))
	s.router.Use(s.requestLogger.Logger())
	s.router.Use(gatewaymiddleware.MarkLongLivedRequests())
	s.router.Use(gatewaymiddleware.UpstreamTimeoutWhitelist())

	// Health check endpoints (no auth required)
	s.router.GET("/healthz", s.healthCheck)
	s.router.GET("/readyz", s.readinessCheck)
	s.router.GET("/metadata", gatewayhandlers.GatewayMetadata("cluster-gateway", gatewayhandlers.GatewayModeDirect))

	// Metrics endpoint
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	s.setupPublicRoutes()

	// API v1 routes
	v1 := s.router.Group("/api/v1")
	s.attachPublicAPIOverloadGuard(v1)
	{
		if authModeEnabled(s.cfg.AuthMode, authModePublic) {
			v1.Use(s.compositeAuth.Authenticate())
			v1.Use(s.teamQuotaController.ConsumeForwardedAdmissionProof())
			// Actual system administrators bypass these controls only on the
			// policy-repair routes. Rejected non-admin attempts remain charged.
			v1.Use(s.teamQuotaController.AdmitLongLivedConnections(true))
			v1.Use(s.teamQuotaController.LimitNetworkTraffic(true))
			v1.Use(s.teamQuotaController.AdmitActiveRequests(true))
			v1.Use(s.teamQuotaController.RateLimitAPIRequests(true))
			if s.cfg.TeamQuota.PolicyOwner {
				adminQuotas := v1.Group("/teams")
				adminQuotas.Use(s.publicAuth.RequireSystemAdmin())
				{
					adminQuotas.GET("/:team_id/quotas", s.teamQuotaController.ListTeam)
					adminQuotas.PUT("/:team_id/quotas/:key", s.teamQuotaController.PutTeamPolicy)
					adminQuotas.DELETE("/:team_id/quotas/:key", s.teamQuotaController.DeleteTeamPolicy)
				}
			}
			v1.GET(
				"/quotas",
				s.authMiddleware.RequirePermission(gatewayauthn.PermQuotaRead),
				s.teamQuotaController.ListCurrent,
			)
		} else {
			// Apply internal auth to all v1 routes (requests come from regional-gateway)
			v1.Use(s.authMiddleware.Authenticate())
			v1.Use(s.teamQuotaController.ConsumeForwardedAdmissionProof())
			v1.Use(s.teamQuotaController.AdmitLongLivedConnections(true))
			v1.Use(s.teamQuotaController.LimitNetworkTraffic(true))
			v1.Use(s.teamQuotaController.AdmitActiveRequests(true))
			v1.Use(s.teamQuotaController.RateLimitAPIRequests(true))
		}

		// === Sandbox Management (→ Manager) ===
		sandboxes := v1.Group("/sandboxes")
		{
			sandboxes.Use(s.auditSandboxRequests())
			sandboxes.GET("/:id/observability/events", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxAuditRead), licensinghttp.RequireFeature(s.sandboxAuditEntitlements, licensing.FeatureSandboxAudit, s.logger), s.sandboxObservabilityHandler().ListEvents)
			sandboxes.GET("/:id/observability/logs", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.sandboxObservabilityHandler().ListLogs)
			sandboxes.GET("/:id/metrics", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.sandboxObservabilityHandler().GetRuntimeMetrics)
			sandboxes.GET("/:id/metrics/catalog", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.sandboxObservabilityHandler().GetRuntimeMetricsCatalog)

			sandboxes.Use(s.managerUpstreamMiddleware())
			sandboxes.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.listSandboxes)
			sandboxes.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxCreate), s.createSandbox)
			sandboxes.GET("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.getSandbox)
			sandboxes.GET("/:id/status", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.getSandboxStatus)
			sandboxes.PUT("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.updateSandbox)
			sandboxes.DELETE("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxDelete), s.deleteSandbox)
			sandboxes.POST("/:id/pause", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.pauseSandbox)
			sandboxes.POST("/:id/resume", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.resumeSandbox)
			sandboxes.POST("/:id/snapshots", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySandboxManagerSubresource("snapshots"))
			sandboxes.GET("/:id/snapshots", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.proxySandboxManagerSubresource("snapshots"))
			sandboxes.POST("/:id/rootfs/restore", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySandboxManagerSubresource("rootfs/restore"))
			sandboxes.POST("/:id/fork", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxCreate), s.proxySandboxManagerSubresource("fork"))
			sandboxes.POST("/:id/refresh", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.refreshSandbox)

			// === Network Policy (→ Manager) ===
			sandboxes.GET("/:id/network", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.proxySandboxManagerSubresource("network"))
			sandboxes.PUT("/:id/network", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySandboxManagerSubresource("network"))

			sandboxes.GET("/:id/services", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.proxySandboxManagerSubresource("services"))
			sandboxes.PUT("/:id/services", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySandboxManagerSubresource("services"))

			s.registerSandboxProcdRoutes(sandboxes)
		}

		// === Template Management (→ Manager) ===
		templates := v1.Group("/templates")
		templates.Use(s.managerUpstreamMiddleware())
		{
			templates.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermTemplateRead), s.proxyManagerPath("/api/v1/templates"))
			templates.GET("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermTemplateRead), s.proxyManagerPathParam("/api/v1/templates/", "id", "template_id"))
			templates.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermTemplateCreate), s.proxyManagerPath("/api/v1/templates"))
			templates.POST(
				"/from-sandbox",
				s.authMiddleware.RequirePermission(gatewayauthn.PermTemplateCreate),
				s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead),
				s.proxyTemplateFromSandbox,
			)
			templates.PUT("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermTemplateWrite), s.proxyManagerPathParam("/api/v1/templates/", "id", "template_id"))
			templates.DELETE("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermTemplateDelete), s.proxyManagerPathParam("/api/v1/templates/", "id", "template_id"))
		}

		// === Registry Credentials (→ Manager) ===
		registry := v1.Group("/registry")
		registry.Use(s.managerUpstreamMiddleware())
		{
			registry.POST("/credentials", s.authMiddleware.RequirePermission(gatewayauthn.PermRegistryWrite), s.proxyToManager)
		}

		credentialSources := v1.Group("/credential-sources")
		credentialSources.Use(s.managerUpstreamMiddleware())
		{
			credentialSources.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermCredentialSourceRead), s.proxyToManager)
			credentialSources.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermCredentialSourceWrite), s.proxyToManager)
			credentialSources.GET("/:name", s.authMiddleware.RequirePermission(gatewayauthn.PermCredentialSourceRead), s.proxyToManager)
			credentialSources.PUT("/:name", s.authMiddleware.RequirePermission(gatewayauthn.PermCredentialSourceWrite), s.proxyToManager)
			credentialSources.DELETE("/:name", s.authMiddleware.RequirePermission(gatewayauthn.PermCredentialSourceDelete), s.proxyToManager)
		}

		rootFSSnapshots := v1.Group("/sandbox-rootfs-snapshots")
		rootFSSnapshots.Use(s.managerUpstreamMiddleware())
		{
			rootFSSnapshots.GET("/:snapshot_id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.proxyManagerPathParam("/api/v1/sandbox-rootfs-snapshots/", "snapshot_id", "snapshot_id"))
			rootFSSnapshots.DELETE("/:snapshot_id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxyManagerPathParam("/api/v1/sandbox-rootfs-snapshots/", "snapshot_id", "snapshot_id"))
		}

		// === SandboxVolume Management (→ Manager Storage) ===
		sandboxvolumes := v1.Group("/sandboxvolumes")
		sandboxvolumes.Use(s.managerStorageUpstreamMiddleware())
		{
			sandboxvolumes.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeCreate), s.createSandboxVolume)
			sandboxvolumes.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeRead), s.listSandboxVolumes)
			sandboxvolumes.GET("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeRead), s.getSandboxVolume)
			sandboxvolumes.DELETE("/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeDelete), s.deleteSandboxVolume)
			sandboxvolumes.POST("/:id/fork", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeWrite), s.forkSandboxVolume)
			files := sandboxvolumes.Group("/:id/files")
			{
				files.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileRead), s.handleVolumeFileOperation)
				files.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileWrite), s.handleVolumeFileOperation)
				files.DELETE("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileWrite), s.handleVolumeFileOperation)
				files.PUT("/archive", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileWrite), s.handleVolumeFileArchiveImport)
				files.GET("/watch", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileRead), s.handleVolumeFileWatch)
				files.POST("/move", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileWrite), s.handleVolumeFileMove)
				files.GET("/stat", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileRead), s.handleVolumeFileStat)
				files.GET("/list", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileRead), s.handleVolumeFileList)
			}
			// Snapshot/Restore (→ Manager Storage)
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

	// Internal API routes are only mounted when control-plane callers are
	// enabled for this deployment mode.
	if authModeEnabled(s.cfg.AuthMode, authModeInternal) {
		s.setupInternalControlPlaneRoutes()
	}
	s.setupSandboxObservabilityIngestRoutes()

	// Metering export is region-scoped and must remain available when
	// cluster-gateway serves as the single-cluster public API entrypoint.
	s.setupMeteringRoutes()

	// Host-based public exposure fallback (for non-/api paths)
	s.setupPublicExposureNoRoute()
}

func (s *Server) attachPublicAPIOverloadGuard(api *gin.RouterGroup) {
	if s.publicOverloadGuard != nil {
		api.Use(s.publicOverloadGuard.Admit())
	}
}

func (s *Server) setupPublicExposureNoRoute() {
	if s.publicOverloadGuard != nil {
		s.router.NoRoute(
			s.publicOverloadGuard.Admit(),
			s.handlePublicExposureNoRoute,
		)
		return
	}
	s.router.NoRoute(s.handlePublicExposureNoRoute)
}

func (s *Server) setupPublicRoutes() {
	if s == nil || s.cfg == nil || !authModeEnabled(s.cfg.AuthMode, authModePublic) {
		return
	}

	var teamDeletePreflight gatewayhandlers.TeamDeletePreflight
	var teamDeletionLifecycle gatewayhandlers.TeamDeletionLifecycle
	if s.publicIdentityRepo != nil {
		pool := s.publicIdentityRepo.Pool()
		teamDeletePreflight = teamresources.NewRepository(pool)
		teamDeletionLifecycle = coreteamquota.NewRepository(pool)
	}
	deps := public.Deps{
		IdentityRepo:          s.publicIdentityRepo,
		APIKeyRepo:            s.publicAPIKeyRepo,
		AuthMiddleware:        s.publicAuth,
		TeamDeletePreflight:   teamDeletePreflight,
		TeamDeletionLifecycle: teamDeletionLifecycle,
		BuiltinProvider:       s.publicBuiltin,
		OIDCManager:           s.publicOIDC,
		Entitlements:          s.entitlements,
		JWTIssuer:             s.publicJWT,
		RegionID:              s.cfg.RegionID,
		Logger:                s.logger,
	}
	if s.teamQuotaController != nil {
		deps.TeamDistributedAdmissionDisabler = s.teamQuotaController
		deps.TeamTrafficAdmission = s.teamQuotaController.AdmitEdgeTraffic(false)
		deps.TeamActiveRequestAdmission = s.teamQuotaController.AdmitActiveRequests(false)
		deps.TeamRequestAdmission = s.teamQuotaController.RateLimitAPIRequests(false)
	}
	if s.publicOverloadGuard != nil {
		deps.IdentityRequestAdmission = s.publicOverloadGuard.Admit()
	}
	public.RegisterRoutes(s.router, deps)
}

func (s *Server) registerSandboxProcdRoutes(sandboxes *gin.RouterGroup) {
	// === Durable execution sessions (→ Procd) ===
	sessions := sandboxes.Group("/:id/sessions")
	{
		sessions.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.proxySessionCollection)
		sessions.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySessionCollection)
		sessions.GET("/:session_id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.proxySessionItem)
		sessions.PUT("/:session_id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySessionItem)
		sessions.DELETE("/:session_id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySessionItem)
		sessions.PUT("/:session_id/desired-state", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySessionDesiredState)
		sessions.POST("/:session_id/attempts", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySessionAttempts)
		sessions.POST("/:session_id/inputs", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySessionInputs)
		sessions.POST("/:session_id/signals", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySessionSignals)
		sessions.PUT("/:session_id/terminal", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySessionTerminal)
		sessions.GET("/:session_id/events", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.proxySessionEvents)
		sessions.GET("/:session_id/events/stream", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.proxySessionEventStream)
		sessions.GET("/:session_id/ws", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.proxySessionWebSocket)
	}

	// === Process/Context Management (→ Procd) ===
	contexts := sandboxes.Group("/:id/contexts")
	{
		contexts.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.createContext)
		contexts.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.listContexts)
		contexts.GET("/:ctx_id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.getContext)
		contexts.DELETE("/:ctx_id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.deleteContext)
		contexts.POST("/:ctx_id/restart", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.restartContext)
		contexts.POST("/:ctx_id/input", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.contextInput)
		contexts.POST("/:ctx_id/exec", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.contextExec)
		contexts.POST("/:ctx_id/resize", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.contextResize)
		contexts.POST("/:ctx_id/signal", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.contextSignal)
		contexts.GET("/:ctx_id/ws", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.contextWebSocket)
	}

	// === File System (→ Procd) ===
	files := sandboxes.Group("/:id/files")
	{
		files.GET("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.handleFileOperation)
		files.POST("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.handleFileOperation)
		files.DELETE("", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.handleFileOperation)
		files.GET("/watch", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.handleFileWatch)
		files.POST("/move", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxWrite), s.handleFileMove)
		files.GET("/stat", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.handleFileStat)
		files.GET("/list", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.handleFileList)
	}
}

func (s *Server) setupInternalControlPlaneRoutes() {
	internal := s.router.Group("/internal/v1")
	internal.Use(s.managerUpstreamMiddleware())
	internal.Use(s.authMiddleware.Authenticate())
	{
		// Cluster information (→ Manager)
		internal.GET("/cluster/summary", s.getClusterSummary)

		// Sandbox metadata and power control (→ Manager)
		internal.GET("/sandboxes/:id", s.getInternalSandbox)
		internal.GET("/sandboxes/:id/template-source", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), s.proxyInternalManagerRequest)
		internal.DELETE("/sandboxes/:id", s.authMiddleware.RequirePermission(gatewayauthn.PermSandboxDelete), s.deleteInternalSandbox)
		internal.POST("/sandboxes/:id/resume", s.resumeInternalSandbox)
		// Template management (→ Manager)
		internal.GET("/templates", s.proxyInternalTemplateRequest)
		internal.GET("/templates/:id", s.proxyInternalTemplateRequest)
		internal.POST("/templates", s.proxyInternalTemplateRequest)
		internal.PUT("/templates/:id", s.proxyInternalTemplateRequest)
		internal.DELETE("/templates/:id", s.proxyInternalTemplateRequest)

		// Template statistics (→ Manager)
		internal.GET("/templates/stats", s.getTemplateStats)

	}
}

func (s *Server) setupSandboxObservabilityIngestRoutes() {
	overloadAdmission := s.sandboxObservabilityIngestOverloadAdmission()
	activeRequestAdmission := s.sandboxObservabilityIngestActiveRequestAdmission()
	quotaAdmission := s.sandboxObservabilityIngestQuotaAdmission()

	audit := s.router.Group("/internal/v1")
	audit.Use(overloadAdmission)
	audit.Use(s.sandboxAuditIngestAuthMiddleware.Authenticate())
	audit.Use(activeRequestAdmission)
	{
		audit.POST("/sandbox-observability/events", s.sandboxAuditIngestAuthMiddleware.RequirePermission(gatewayauthn.PermSandboxObservabilityWrite), licensinghttp.RequireFeature(s.sandboxAuditEntitlements, licensing.FeatureSandboxAudit, s.logger), quotaAdmission, s.sandboxObservabilityHandler().IngestEvents)
	}

	internal := s.router.Group("/internal/v1")
	internal.Use(overloadAdmission)
	internal.Use(s.sandboxObservabilityIngestAuthMiddleware.Authenticate())
	internal.Use(activeRequestAdmission)
	{
		internal.POST("/sandbox-observability/logs", s.sandboxObservabilityIngestAuthMiddleware.RequirePermission(gatewayauthn.PermSandboxObservabilityWrite), quotaAdmission, s.sandboxObservabilityHandler().IngestLogs)
		internal.POST("/sandbox-observability/runtime-samples", s.sandboxObservabilityIngestAuthMiddleware.RequirePermission(gatewayauthn.PermSandboxObservabilityWrite), quotaAdmission, s.sandboxObservabilityHandler().IngestRuntimeSamples)
	}
}

func (s *Server) sandboxObservabilityIngestQuotaAdmission() gin.HandlerFunc {
	if s.teamQuotaController != nil {
		return s.teamQuotaController.RateLimitBodyBytes(
			coreteamquota.KeyObservabilityIngestBytes,
			gatewayhandlers.MaxSandboxObservabilityIngestBytes,
		)
	}
	return func(c *gin.Context) {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "team quota is unavailable")
		c.Abort()
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
	switch normalizeAuthMode(s.cfg.AuthMode) {
	case authModePublic:
		internal.Use(s.publicAuth.Authenticate())
		internal.Use(s.publicAuth.RequireSystemAdmin())
	case authModeBoth:
		internal.Use(s.compositeAuth.Authenticate())
		internal.Use(requireMeteringAccess())
	default:
		internal.Use(s.authMiddleware.Authenticate())
	}
	{
		internal.GET("/metering/status", s.meteringHandler.GetStatus)
		internal.GET("/metering/events", s.meteringHandler.ListEvents)
		internal.GET("/metering/windows", s.meteringHandler.ListWindows)
	}
}

func requireMeteringAccess() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := gatewaymiddleware.GetAuthContext(c)
		if authCtx == nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "not authenticated",
			})
			return
		}
		if authCtx.AuthMethod == gatewayauthn.AuthMethodInternal || authCtx.IsSystemAdmin {
			c.Next()
			return
		}
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"error": "system admin access required",
		})
	}
}

// Start starts the HTTP server
func (s *Server) Start(ctx context.Context) error {
	if s.ownsPublicAPIKeyRepo && s.publicAPIKeyRepo != nil {
		defer func() {
			if err := s.closeAPIKeyRepository(); err != nil {
				s.logger.Warn("Failed to close API key repository", zap.Error(err))
			}
		}()
	}
	if s.shouldRunTeamQuotaTombstoneCleanup() {
		if err := s.teamQuotaController.StartDeletedTeamTombstoneCleanup(
			ctx,
			gatewayteamquota.DeletedTeamTombstoneRetention(s.cfg.JWTAccessTokenTTL.Duration),
		); err != nil {
			return fmt.Errorf("start deleted Team Quota tombstone cleanup: %w", err)
		}
	}
	if authModeEnabled(s.cfg.AuthMode, authModePublic) && s.publicIdentityRepo != nil {
		go s.publicIdentityRepo.RunIdentitySessionCleanup(
			ctx,
			s.cfg.IdentityResourceGuard.SessionCleanupInterval.Duration,
			s.cfg.IdentityResourceGuard.SessionCleanupBatchSize,
			func(err error) {
				s.logger.Warn("Failed to clean up identity sessions", zap.Error(err))
			},
		)
	}
	if s.ownsTeamQuotaController && s.teamQuotaController != nil {
		defer func() {
			if err := s.teamQuotaController.Close(); err != nil {
				s.logger.Warn("Failed to close Team Quota controller", zap.Error(err))
			}
		}()
	}
	if s.ownsPublicOverloadGuard && s.publicOverloadGuard != nil {
		defer func() {
			if err := s.publicOverloadGuard.Close(); err != nil {
				s.logger.Warn("Failed to close public overload guard", zap.Error(err))
			}
		}()
	}
	if s.ownsSandboxServiceAbuseGuard && s.sandboxServiceAbuseGuard != nil {
		defer func() {
			if err := s.sandboxServiceAbuseGuard.Close(); err != nil {
				s.logger.Warn("Failed to close sandbox service abuse guard", zap.Error(err))
			}
		}()
	}
	if s.auditDelivery != nil {
		s.auditDelivery.Start(ctx)
	}
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
	server.ConnState = httpobs.NewConnStateTracker(s.obsProvider.HTTPServerConfig(nil)).Wrap(server.ConnState)

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

func (s *Server) closeAPIKeyRepository() error {
	if s == nil || !s.ownsPublicAPIKeyRepo || s.publicAPIKeyRepo == nil {
		return nil
	}
	return s.publicAPIKeyRepo.Close()
}

func (s *Server) shouldRunTeamQuotaTombstoneCleanup() bool {
	return s != nil &&
		s.ownsTeamQuotaController &&
		s.teamQuotaController != nil &&
		s.cfg != nil &&
		s.cfg.TeamQuota.PolicyOwner
}

func sandboxServiceAbuseGuardConfig(cfg config.GatewayConfig) ratelimit.Config {
	return ratelimit.Config{
		Backend:        ratelimit.BackendRedis,
		RedisURL:       cfg.RedisURL,
		RedisKeyPrefix: rediscache.JoinKeyPrefix(cfg.RedisKeyPrefix, "sandbox-service-abuse-guard"),
		RedisTimeout:   cfg.RedisTimeout.Duration,
		FailOpen:       false,
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

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
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

func (s *Server) managerStorageUpstreamMiddleware() gin.HandlerFunc {
	return s.requireUpstream(
		func() bool {
			return strings.TrimSpace(s.cfg.ManagerStorageURL) != "" && s.proxy2ManagerStorage != nil
		},
		func() []zap.Field {
			return []zap.Field{zap.String("manager_storage_url", s.cfg.ManagerStorageURL)}
		},
		"Manager storage upstream not configured",
		"manager storage upstream not configured",
		"manager_storage_url is empty",
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

func newInternalAuthValidators(
	authMode string,
	allowedControlPlaneCallers []string,
	controlPlanePublicKey ed25519.PublicKey,
	dataPlanePublicKey ed25519.PublicKey,
	auditNetdPublicKey ed25519.PublicKey,
) (*internalauth.Validator, *internalauth.Validator, *internalauth.Validator) {
	var controlPlaneValidator *internalauth.Validator
	if authModeEnabled(authMode, authModeInternal) {
		controlPlaneValidator = internalauth.NewValidator(internalauth.ValidatorConfig{
			Target:             internalauth.ServiceClusterGateway,
			PublicKey:          controlPlanePublicKey,
			AllowedCallers:     allowedControlPlaneCallers,
			ClockSkewTolerance: 10 * time.Second,
		})
	}
	dataPlaneIngestValidator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    internalauth.ServiceClusterGateway,
		PublicKey: dataPlanePublicKey,
		AllowedCallers: []string{
			internalauth.ServiceCtld,
			internalauth.ServiceManager,
		},
		ClockSkewTolerance: 10 * time.Second,
	})
	var auditIngestValidator *internalauth.Validator
	if len(auditNetdPublicKey) == ed25519.PublicKeySize {
		auditIngestValidator = internalauth.NewValidator(internalauth.ValidatorConfig{
			Target:             internalauth.ServiceClusterGateway,
			PublicKey:          auditNetdPublicKey,
			AllowedCallers:     []string{internalauth.ServiceNetd},
			ClockSkewTolerance: 10 * time.Second,
		})
	}
	return controlPlaneValidator, dataPlaneIngestValidator, auditIngestValidator
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
