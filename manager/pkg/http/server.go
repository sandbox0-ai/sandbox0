package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/clusterservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialsource"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/registryservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templateservice"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templatehttp "github.com/sandbox0-ai/sandbox0/pkg/template/http"
	"github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Server represents the HTTP server
type Server struct {
	router                  *gin.Engine
	sandboxService          *service.SandboxService
	sandboxClaimer          service.SandboxClaimer
	sandboxTerminator       service.SandboxTerminator
	sandboxPauser           service.SandboxPauser
	sandboxResumer          service.SandboxResumer
	sandboxForker           service.SandboxForker
	egressAuthService       *egressauthservice.EgressAuthService
	credentialSourceService *credentialsource.CredentialSourceService
	templateService         *templateservice.TemplateService
	registryService         *registryservice.RegistryService
	templateStore           store.TemplateStore
	templateReconciler      TemplateReconciler
	templateStoreEnabled    bool
	templateHandler         *templatehttp.Handler
	clusterService          *clusterservice.ClusterService
	quotaRepo               *quota.Repository
	authValidator           *internalauth.Validator
	logger                  *zap.Logger
	port                    int
	obsProvider             *observability.Provider
	// Public exposure config
	publicRootDomain string
	publicRegionID   string
}

type claimSandboxCapabilityRequest struct {
	Config *struct {
		Network json.RawMessage `json:"network"`
	} `json:"config"`
}

type updateSandboxCapabilityRequest struct {
	Config *struct {
		Network json.RawMessage `json:"network"`
	} `json:"config"`
}

type capabilityBodyInspector func(target any) bool

// TemplateReconciler exposes minimal reconcile controls for template syncing.
type TemplateReconciler interface {
	TriggerReconcile(ctx context.Context)
}

// ServerDependencies names the manager capabilities exposed over HTTP. Using
// this struct keeps composition changes local and avoids order-dependent
// constructor calls as features are added or removed.
type ServerDependencies struct {
	SandboxService          *service.SandboxService
	SandboxClaimer          service.SandboxClaimer
	SandboxTerminator       service.SandboxTerminator
	SandboxPauser           service.SandboxPauser
	SandboxResumer          service.SandboxResumer
	SandboxForker           service.SandboxForker
	EgressAuthService       *egressauthservice.EgressAuthService
	CredentialSourceService *credentialsource.CredentialSourceService
	TemplateService         *templateservice.TemplateService
	RegistryService         *registryservice.RegistryService
	TemplateStore           store.TemplateStore
	TemplateReconciler      TemplateReconciler
	TemplateStoreEnabled    bool
	TemplateResourcePolicy  template.ResourcePolicy
	ClusterService          *clusterservice.ClusterService
	QuotaRepository         *quota.Repository
	AuthValidator           *internalauth.Validator
	Logger                  *zap.Logger
	Port                    int
	ObservabilityProvider   *observability.Provider
	PublicRootDomain        string
	PublicRegionID          string
}

// NewServerWithDependencies creates a manager HTTP server from named
// dependencies.
func NewServerWithDependencies(deps ServerDependencies) *Server {
	// Set gin mode based on log level
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(httpobs.GinMiddleware(deps.ObservabilityProvider.HTTPServerConfig(nil)))
	router.Use(gin.Recovery())
	router.Use(requestLogger(deps.Logger))

	if deps.SandboxClaimer == nil {
		deps.SandboxClaimer = deps.SandboxService
	}
	if deps.SandboxTerminator == nil {
		deps.SandboxTerminator, _ = deps.SandboxClaimer.(service.SandboxTerminator)
	}
	if deps.SandboxPauser == nil {
		deps.SandboxPauser, _ = deps.SandboxClaimer.(service.SandboxPauser)
	}
	if deps.SandboxResumer == nil {
		deps.SandboxResumer, _ = deps.SandboxClaimer.(service.SandboxResumer)
	}
	if deps.SandboxForker == nil {
		deps.SandboxForker, _ = deps.SandboxClaimer.(service.SandboxForker)
	}
	server := &Server{
		router:                  router,
		sandboxService:          deps.SandboxService,
		sandboxClaimer:          deps.SandboxClaimer,
		sandboxTerminator:       deps.SandboxTerminator,
		sandboxPauser:           deps.SandboxPauser,
		sandboxResumer:          deps.SandboxResumer,
		sandboxForker:           deps.SandboxForker,
		egressAuthService:       deps.EgressAuthService,
		credentialSourceService: deps.CredentialSourceService,
		templateService:         deps.TemplateService,
		registryService:         deps.RegistryService,
		templateStore:           deps.TemplateStore,
		templateReconciler:      deps.TemplateReconciler,
		templateStoreEnabled:    deps.TemplateStoreEnabled,
		clusterService:          deps.ClusterService,
		quotaRepo:               deps.QuotaRepository,
		authValidator:           deps.AuthValidator,
		logger:                  deps.Logger,
		port:                    deps.Port,
		obsProvider:             deps.ObservabilityProvider,
		publicRootDomain:        deps.PublicRootDomain,
		publicRegionID:          deps.PublicRegionID,
	}
	if deps.TemplateStoreEnabled {
		registryHosts := []string(nil)
		if deps.TemplateService != nil {
			registryHosts = deps.TemplateService.RegistryHosts()
		}
		buildStore, _ := deps.TemplateStore.(store.TemplateBuildStore)
		server.templateHandler = &templatehttp.Handler{
			Store:                deps.TemplateStore,
			BuildStore:           buildStore,
			SourceResolver:       deps.SandboxService,
			Reconciler:           deps.TemplateReconciler,
			StatsProvider:        &clusterTemplateStatsProvider{clusterService: deps.ClusterService},
			ResourcePolicy:       deps.TemplateResourcePolicy,
			PrivateRegistryHosts: registryHosts,
			Logger:               deps.Logger,
		}
	}

	server.setupRoutes()

	return server
}

// setupRoutes sets up the HTTP routes
func (s *Server) setupRoutes() {
	// Health check (no auth required)
	s.router.GET("/healthz", s.healthCheck)
	s.router.GET("/readyz", s.readinessCheck)

	// API v1 (requires auth)
	v1 := s.router.Group("/api/v1")
	v1.Use(s.authMiddleware())
	{
		// Sandbox management
		sandboxes := v1.Group("/sandboxes")
		{
			sandboxes.GET("", s.listSandboxes)
			sandboxes.POST("", s.requireNetworkPolicyInBody(func() any { return &claimSandboxCapabilityRequest{} }), s.claimSandbox)
			sandboxes.GET("/:id", s.getSandbox)
			sandboxes.PUT("/:id", s.requireNetworkPolicyInBody(func() any { return &updateSandboxCapabilityRequest{} }), s.updateSandbox)
			sandboxes.GET("/:id/status", s.getSandboxStatus)
			sandboxes.GET("/:id/network", s.requireNetworkPolicyCapability(), s.getNetworkPolicy)
			sandboxes.PUT("/:id/network", s.requireNetworkPolicyCapability(), s.updateNetworkPolicy)
			sandboxes.GET("/:id/services", s.listSandboxServices)
			sandboxes.PUT("/:id/services", s.updateSandboxServices)
			sandboxes.POST("/:id/pause", s.pauseSandbox)
			sandboxes.POST("/:id/resume", s.resumeSandbox)
			sandboxes.POST("/:id/snapshots", s.createSandboxRootFSSnapshot)
			sandboxes.GET("/:id/snapshots", s.listSandboxRootFSSnapshots)
			sandboxes.POST("/:id/rootfs/restore", s.restoreSandboxRootFS)
			sandboxes.POST("/:id/fork", s.forkSandbox)
			sandboxes.POST("/:id/refresh", s.refreshSandbox)
			sandboxes.DELETE("/:id", s.terminateSandbox)
		}

		v1.GET("/sandbox-rootfs-snapshots/:snapshot_id", s.getSandboxRootFSSnapshot)
		v1.DELETE("/sandbox-rootfs-snapshots/:snapshot_id", s.deleteSandboxRootFSSnapshot)

		// Template management (public API)
		templates := v1.Group("/templates")
		templates.Use(s.requireTemplateStoreCapability())
		{
			templates.GET("", s.listTemplates)
			templates.GET("/:id", s.getTemplate)
			templates.POST("", s.createTemplate)
			templates.POST("/from-sandbox", s.createTemplateFromSandbox)
			templates.PUT("/:id", s.updateTemplate)
			templates.DELETE("/:id", s.deleteTemplate)
		}

		registry := v1.Group("/registry")
		registry.Use(s.requireRegistryCapability())
		{
			registry.POST("/credentials", s.getRegistryCredentials)
		}

		credentialSources := v1.Group("/credential-sources")
		credentialSources.Use(s.requireCredentialSourceCapability())
		{
			credentialSources.GET("", s.listCredentialSources)
			credentialSources.POST("", s.createCredentialSource)
			credentialSources.GET("/:name", s.getCredentialSource)
			credentialSources.PUT("/:name", s.updateCredentialSource)
			credentialSources.DELETE("/:name", s.deleteCredentialSource)
		}

		quotas := v1.Group("/quotas")
		{
			quotas.GET("", s.listTeamQuotas)
			quotas.GET("/:dimension", s.getTeamQuota)
		}
	}

	// Internal API v1 (for scheduler)
	internal := s.router.Group("/internal/v1")
	internal.Use(s.authMiddleware())
	{
		internalSandboxes := internal.Group("/sandboxes")
		{
			internalSandboxes.GET("/:id", s.getSandboxInternal)
			internalSandboxes.GET("/:id/template-source", s.getSandboxTemplateSourceInternal)
		}

		// Template management (scheduler sync)
		internalTemplates := internal.Group("/templates")
		{
			internalTemplates.GET("", s.listTemplatesLegacy)
			internalTemplates.GET("/stats", s.getTemplateStats)
			internalTemplates.GET("/:id", s.getTemplateLegacy)
			internalTemplates.POST("", s.createTemplateLegacy)
			internalTemplates.PUT("/:id", s.updateTemplateLegacy)
			internalTemplates.DELETE("/:id", s.deleteTemplateLegacy)
		}

		// Cluster management
		internalCluster := internal.Group("/cluster")
		{
			internalCluster.GET("/summary", s.getClusterSummary)
		}

		internalEgressAuth := internal.Group("/egress-auth")
		{
			internalEgressAuth.POST("/resolve", s.resolveEgressAuth)
		}

		internalTeamQuotas := internal.Group("/teams/:team_id/quotas")
		{
			internalTeamQuotas.PUT("/:dimension", s.putTeamQuotaInternal)
			internalTeamQuotas.DELETE("/:dimension", s.deleteTeamQuotaInternal)
		}
	}
}

// Start starts the HTTP server
func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.port)
	s.logger.Info("Starting HTTP server", zap.String("addr", addr))

	server := &http.Server{
		Addr:    addr,
		Handler: s.router,
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}

// Handler functions

func (s *Server) healthCheck(c *gin.Context) {
	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"status": "healthy",
	})
}

func (s *Server) readinessCheck(c *gin.Context) {
	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"status": "ready",
	})
}

// Middleware

func requestLogger(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Skip logging for health check and readiness check
		path := c.Request.URL.Path
		if path == "/healthz" || path == "/readyz" {
			c.Next()
			return
		}

		// Process request
		c.Next()

		// Log request
		fields := []zap.Field{
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.String("client_ip", c.ClientIP()),
		}

		spanCtx := trace.SpanFromContext(c.Request.Context()).SpanContext()
		if spanCtx.IsValid() {
			fields = append(fields,
				zap.String("trace_id", spanCtx.TraceID().String()),
				zap.String("span_id", spanCtx.SpanID().String()),
			)
		}

		logger.Info("HTTP request", fields...)
	}
}

// authMiddleware validates internal authentication tokens
func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Extract token from multiple possible headers
		token := s.extractAuthToken(c.Request)
		if token == "" {
			s.logger.Warn("Missing authentication token",
				zap.String("path", c.Request.URL.Path),
				zap.String("method", c.Request.Method),
			)
			spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication token")
			c.Abort()
			return
		}

		// Validate token
		claims, err := s.authValidator.Validate(token)
		if err != nil {
			s.logger.Warn("Authentication failed",
				zap.String("path", c.Request.URL.Path),
				zap.String("method", c.Request.Method),
				zap.Error(err),
			)
			spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, fmt.Sprintf("unauthorized: %v", err))
			c.Abort()
			return
		}

		// Add claims to request context for handlers
		ctx := internalauth.WithClaims(c.Request.Context(), claims)
		c.Request = c.Request.WithContext(ctx)

		s.logger.Debug("Request authenticated",
			zap.String("path", c.Request.URL.Path),
			zap.String("team_id", claims.TeamID),
			zap.String("caller", claims.Caller),
		)

		c.Next()
	}
}

// extractAuthToken extracts authentication token from request headers
// Supports both X-Internal-Token and Authorization: Bearer <token>
func (s *Server) extractAuthToken(r *http.Request) string {
	// Try X-Internal-Token header first
	if token := r.Header.Get("X-Internal-Token"); token != "" {
		return token
	}

	// Try Authorization header with Bearer prefix
	if auth := r.Header.Get("Authorization"); auth != "" {
		if strings.HasPrefix(auth, "Bearer ") {
			return strings.TrimPrefix(auth, "Bearer ")
		}
	}

	return ""
}

func (s *Server) requireNetworkPolicyCapability() gin.HandlerFunc {
	return s.requireCapability(func() bool {
		return s.sandboxService != nil && s.sandboxService.SupportsNetworkPolicy()
	}, "network policy is unavailable in this deployment")
}

func (s *Server) requireNetworkPolicyInBody(newRequest func() any) gin.HandlerFunc {
	return s.requireCapabilityInBody(
		newRequest,
		func(target any) bool { return requestContainsNetworkPolicy(target, nil) },
		func() bool { return s.sandboxService != nil && s.sandboxService.SupportsNetworkPolicy() },
		"network policy is unavailable in this deployment",
	)
}

func (s *Server) requireTemplateStoreCapability() gin.HandlerFunc {
	return s.requireCapability(func() bool {
		return s.templateHandler != nil
	}, "template store is disabled")
}

func (s *Server) requireRegistryCapability() gin.HandlerFunc {
	return s.requireCapability(func() bool {
		return s.registryService != nil
	}, "registry provider is not configured")
}

func (s *Server) requireCredentialSourceCapability() gin.HandlerFunc {
	return s.requireCapability(func() bool {
		return s.credentialSourceService != nil
	}, "credential source store is not configured")
}

func (s *Server) requireCapability(enabled func() bool, message string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if enabled != nil && enabled() {
			c.Next()
			return
		}
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, message)
		c.Abort()
	}
}

func (s *Server) requireCapabilityInBody(
	newRequest func() any,
	inspector capabilityBodyInspector,
	enabled func() bool,
	message string,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		bodyBytes, err := io.ReadAll(c.Request.Body)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "failed to read request body")
			c.Abort()
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		target := newRequest()
		if !requestBodyRequiresCapability(target, bodyBytes, inspector) {
			c.Next()
			return
		}
		if enabled != nil && enabled() {
			c.Next()
			return
		}
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, message)
		c.Abort()
	}
}

func requestBodyRequiresCapability(target any, body []byte, inspector capabilityBodyInspector) bool {
	if len(bytes.TrimSpace(body)) == 0 {
		return false
	}
	if err := json.Unmarshal(body, target); err != nil {
		return false
	}
	if inspector == nil {
		return false
	}
	return inspector(target)
}

func requestContainsNetworkPolicy(target any, _ []byte) bool {
	switch req := target.(type) {
	case *claimSandboxCapabilityRequest:
		if req == nil || req.Config == nil {
			return false
		}
		return rawMessagePresent(req.Config.Network)
	case *updateSandboxCapabilityRequest:
		if req == nil || req.Config == nil {
			return false
		}
		return rawMessagePresent(req.Config.Network)
	default:
		return false
	}
}

func rawMessagePresent(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null"))
}
