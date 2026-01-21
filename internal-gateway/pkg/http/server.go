package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/client"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/middleware"
	"github.com/sandbox0-ai/infra/pkg/auth"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/proxy"
	"go.uber.org/zap"
)

// Server represents the HTTP server for internal-gateway
type Server struct {
	router          *gin.Engine
	cfg             *config.InternalGatewayConfig
	proxy2Mgr       *proxy.Router
	proxy2sp        *proxy.Router
	managerClient   *client.ManagerClient
	authMiddleware  *middleware.InternalAuthMiddleware
	requestLogger   *middleware.RequestLogger
	logger          *zap.Logger
	internalAuthGen *internalauth.Generator
	procdAuthGen    *internalauth.Generator
}

// NewServer creates a new HTTP server
func NewServer(
	cfg *config.InternalGatewayConfig,
	logger *zap.Logger,
) (*Server, error) {
	// Set gin mode
	gin.SetMode(gin.ReleaseMode)

	// Create router
	router := gin.New()

	// Create proxy router
	proxy2Mgr, err := proxy.NewRouter(
		cfg.ManagerURL,
		logger,
		time.Second*10,
	)
	if err != nil {
		return nil, fmt.Errorf("create manager proxy router: %w", err)
	}

	proxy2sp, err := proxy.NewRouter(
		cfg.StorageProxyURL,
		logger,
		time.Second*10,
	)
	if err != nil {
		return nil, fmt.Errorf("create storage-proxy proxy router: %w", err)
	}

	// Initialize internal auth keys
	publicKey, err := internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultInternalJWTPublicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load internal JWT public key: %w", err)
	}

	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load internal JWT private key: %w", err)
	}

	// Create internal auth validator (for validating tokens from edge-gateway and optionally scheduler)
	allowedCallers := cfg.AllowedCallers
	if len(allowedCallers) == 0 {
		allowedCallers = []string{"edge-gateway"}
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "internal-gateway",
		PublicKey:          publicKey,
		AllowedCallers:     allowedCallers,
		ClockSkewTolerance: 10 * time.Second,
	})

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
	managerClient := client.NewManagerClient(cfg.ManagerURL, internalAuthGen, logger)

	server := &Server{
		router:          router,
		cfg:             cfg,
		proxy2Mgr:       proxy2Mgr,
		proxy2sp:        proxy2sp,
		managerClient:   managerClient,
		authMiddleware:  authMiddleware,
		requestLogger:   requestLogger,
		logger:          logger,
		internalAuthGen: internalAuthGen,
		procdAuthGen:    procdAuthGen,
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

	// API v1 routes
	v1 := s.router.Group("/api/v1")
	{
		// Apply internal auth to all v1 routes (requests come from edge-gateway)
		v1.Use(s.authMiddleware.Authenticate())

		// === Sandbox Management (→ Manager) ===
		sandboxes := v1.Group("/sandboxes")
		{
			sandboxes.POST("", s.authMiddleware.RequirePermission(auth.PermSandboxCreate), s.createSandbox)
			sandboxes.GET("/:id", s.authMiddleware.RequirePermission(auth.PermSandboxRead), s.getSandbox)
			sandboxes.GET("/:id/status", s.authMiddleware.RequirePermission(auth.PermSandboxRead), s.getSandboxStatus)
			sandboxes.PATCH("/:id", s.authMiddleware.RequirePermission(auth.PermSandboxWrite), s.updateSandbox)
			sandboxes.DELETE("/:id", s.authMiddleware.RequirePermission(auth.PermSandboxDelete), s.deleteSandbox)
			sandboxes.POST("/:id/pause", s.authMiddleware.RequirePermission(auth.PermSandboxWrite), s.pauseSandbox)
			sandboxes.POST("/:id/resume", s.authMiddleware.RequirePermission(auth.PermSandboxWrite), s.resumeSandbox)
			sandboxes.POST("/:id/refresh", s.authMiddleware.RequirePermission(auth.PermSandboxWrite), s.refreshSandbox)

			// === Network/Bandwidth Policy (→ Manager) ===
			sandboxes.GET("/:id/network", s.authMiddleware.RequirePermission(auth.PermSandboxRead), s.getNetworkPolicy)
			sandboxes.PATCH("/:id/network", s.authMiddleware.RequirePermission(auth.PermSandboxWrite), s.updateNetworkPolicy)
			sandboxes.GET("/:id/bandwidth", s.authMiddleware.RequirePermission(auth.PermSandboxRead), s.getBandwidthPolicy)
			sandboxes.PATCH("/:id/bandwidth", s.authMiddleware.RequirePermission(auth.PermSandboxWrite), s.updateBandwidthPolicy)

			// === Process Execution (→ Procd) ===
			sandboxes.POST("/:id/exec", s.authMiddleware.RequirePermission(auth.PermSandboxWrite), s.exec) // sync mode
			sandboxes.POST("/:id/exec/stream", s.authMiddleware.RequirePermission(auth.PermSandboxWrite), s.execStream)

			// === Process/Context Management (→ Procd) ===
			contexts := sandboxes.Group("/:id/contexts")
			{
				contexts.POST("", s.createContext) // async mode
				contexts.GET("", s.listContexts)
				contexts.GET("/:ctx_id", s.getContext)
				contexts.DELETE("/:ctx_id", s.deleteContext)
				contexts.POST("/:ctx_id/restart", s.restartContext)
				contexts.POST("/:ctx_id/execute", s.executeInContext)
				contexts.GET("/:ctx_id/ws", s.contextWebSocket)
			}

			// === SandboxVolume Management (→ Procd) ===
			sandboxvolumes := sandboxes.Group("/:id/sandboxvolumes")
			{
				sandboxvolumes.POST("/mount", s.authMiddleware.RequirePermission(auth.PermSandboxWrite), s.mountSandboxVolume)
				sandboxvolumes.POST("/unmount", s.authMiddleware.RequirePermission(auth.PermSandboxWrite), s.unmountSandboxVolume)
				sandboxvolumes.GET("", s.authMiddleware.RequirePermission(auth.PermSandboxRead), s.getSandboxVolumeStatus)
			}

			// === File System (→ Procd) ===
			files := sandboxes.Group("/:id/files")
			{
				files.GET("/*path", s.handleFileOperation)
				files.POST("/*path", s.handleFileOperation)
				files.DELETE("/*path", s.handleFileOperation)
			}
		}

		// === Template Management (→ Manager) ===
		templates := v1.Group("/templates")
		{
			templates.GET("", s.authMiddleware.RequirePermission(auth.PermTemplateRead), s.listTemplates)
			templates.GET("/:id", s.authMiddleware.RequirePermission(auth.PermTemplateRead), s.getTemplate)
			templates.POST("", s.authMiddleware.RequirePermission(auth.PermTemplateCreate), s.createTemplate)
			templates.PUT("/:id", s.authMiddleware.RequirePermission(auth.PermTemplateWrite), s.updateTemplate)
			templates.DELETE("/:id", s.authMiddleware.RequirePermission(auth.PermTemplateDelete), s.deleteTemplate)
			templates.POST("/:id/pool/warm", s.authMiddleware.RequirePermission(auth.PermTemplateWrite), s.warmPool)
		}

		// === SandboxVolume Management (→ Storage Proxy) ===
		sandboxvolumes := v1.Group("/sandboxvolumes")
		{
			sandboxvolumes.POST("", s.authMiddleware.RequirePermission(auth.PermSandboxVolumeCreate), s.createSandboxVolume)
			sandboxvolumes.GET("", s.authMiddleware.RequirePermission(auth.PermSandboxVolumeRead), s.listSandboxVolumes)
			sandboxvolumes.GET("/:id", s.authMiddleware.RequirePermission(auth.PermSandboxVolumeRead), s.getSandboxVolume)
			sandboxvolumes.DELETE("/:id", s.authMiddleware.RequirePermission(auth.PermSandboxVolumeDelete), s.deleteSandboxVolume)
			// Snapshot/Restore (→ Storage Proxy)
			snapshots := sandboxvolumes.Group("/:id/snapshots")
			{
				snapshots.POST("", s.authMiddleware.RequirePermission(auth.PermSandboxVolumeWrite), s.createSandboxVolumeSnapshot)
				snapshots.GET("", s.authMiddleware.RequirePermission(auth.PermSandboxVolumeRead), s.listSandboxVolumeSnapshots)
				snapshots.GET("/:snapshot_id", s.authMiddleware.RequirePermission(auth.PermSandboxVolumeRead), s.getSandboxVolumeSnapshot)
				snapshots.POST("/:snapshot_id/restore", s.authMiddleware.RequirePermission(auth.PermSandboxVolumeWrite), s.restoreSandboxVolumeSnapshot)
				snapshots.DELETE("/:snapshot_id", s.authMiddleware.RequirePermission(auth.PermSandboxVolumeDelete), s.deleteSandboxVolumeSnapshot)
			}
		}
	}

	// Internal API routes (for scheduler to call)
	// These routes are authenticated but don't require specific permissions
	// (scheduler uses *:* permissions)
	internal := s.router.Group("/internal/v1")
	{
		internal.Use(s.authMiddleware.Authenticate())

		// Cluster information (→ Manager)
		internal.GET("/cluster/summary", s.getClusterSummary)

		// Template statistics (→ Manager)
		internal.GET("/templates/stats", s.getTemplateStats)
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout.Duration)
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
	// Internal-gateway is ready if it can process requests
	// No database dependency anymore
	c.JSON(http.StatusOK, gin.H{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
	})
}
