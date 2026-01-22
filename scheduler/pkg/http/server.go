package http

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/proxy"
	"github.com/sandbox0-ai/infra/scheduler/pkg/db"
	"go.uber.org/zap"
)

// Server represents the HTTP server for scheduler
type Server struct {
	router          *gin.Engine
	cfg             *config.SchedulerConfig
	repo            *db.Repository
	authValidator   *internalauth.Validator
	internalAuthGen *internalauth.Generator
	reconciler      Reconciler
	logger          *zap.Logger

	internalGatewayProxies   map[string]*proxy.Router
	internalGatewayProxiesMu sync.RWMutex

	clusterCache   map[string]*db.Cluster
	clusterCacheAt time.Time
	clusterCacheMu sync.RWMutex
}

// Reconciler interface for triggering reconciliation
type Reconciler interface {
	TriggerReconcile(ctx context.Context)
	GetTemplateIdleCount(clusterID, templateID string) (int32, bool)
	GetTemplateStatsAge(clusterID string) (time.Duration, bool)
}

// NewServer creates a new HTTP server
func NewServer(
	cfg *config.SchedulerConfig,
	repo *db.Repository,
	authValidator *internalauth.Validator,
	internalAuthGen *internalauth.Generator,
	reconciler Reconciler,
	logger *zap.Logger,
) *Server {
	// Set gin mode
	gin.SetMode(gin.ReleaseMode)

	// Create router
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(requestLogger(logger))

	server := &Server{
		router:                 router,
		cfg:                    cfg,
		repo:                   repo,
		authValidator:          authValidator,
		internalAuthGen:        internalAuthGen,
		reconciler:             reconciler,
		logger:                 logger,
		internalGatewayProxies: make(map[string]*proxy.Router),
		clusterCache:           make(map[string]*db.Cluster),
	}

	server.setupRoutes()

	return server
}

// setupRoutes configures all HTTP routes
func (s *Server) setupRoutes() {
	// Health check endpoints (no auth required)
	s.router.GET("/healthz", s.healthCheck)
	s.router.GET("/readyz", s.readinessCheck)

	// Metrics endpoint
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// API v1 routes
	v1 := s.router.Group("/api/v1")
	{
		// Apply internal auth to all v1 routes (requests come from edge-gateway)
		v1.Use(s.authMiddleware())

		// Template Management (source of truth for multi-cluster)
		templates := v1.Group("/templates")
		{
			templates.GET("", s.listTemplates)
			templates.GET("/:id", s.getTemplate)
			templates.POST("", s.createTemplate)
			templates.PUT("/:id", s.updateTemplate)
			templates.DELETE("/:id", s.deleteTemplate)
			templates.GET("/:id/allocations", s.getTemplateAllocations)
		}

		// Cluster Management (admin API)
		clusters := v1.Group("/clusters")
		{
			clusters.GET("", s.listClusters)
			clusters.GET("/:id", s.getCluster)
			clusters.POST("", s.createCluster)
			clusters.PUT("/:id", s.updateCluster)
			clusters.DELETE("/:id", s.deleteCluster)
		}

		// Sandbox routing (edge-gateway)
		sandboxes := v1.Group("/sandboxes")
		{
			sandboxes.POST("", s.createSandbox)
			sandboxes.Any("/:id", s.proxySandbox)
			sandboxes.Any("/:id/*path", s.proxySandbox)
		}
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
		ReadTimeout:  s.cfg.ReadTimeout.Duration,
		WriteTimeout: s.cfg.WriteTimeout.Duration,
		IdleTimeout:  s.cfg.IdleTimeout.Duration,
	}

	// Apply defaults if not set
	if server.ReadTimeout == 0 {
		server.ReadTimeout = 30 * time.Second
	}
	if server.WriteTimeout == 0 {
		server.WriteTimeout = 60 * time.Second
	}
	if server.IdleTimeout == 0 {
		server.IdleTimeout = 120 * time.Second
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
	// Check database connectivity
	if err := s.repo.Ping(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "not ready",
			"error":  "database unavailable",
		})
		return
	}

	response := gin.H{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
	}

	// Include reconciler status if available
	if s.reconciler != nil {
		if statusGetter, ok := s.reconciler.(interface {
			GetStatus() (time.Time, error)
		}); ok {
			lastReconcile, lastErr := statusGetter.GetStatus()
			response["last_reconcile"] = lastReconcile.Unix()
			if lastErr != nil {
				response["last_reconcile_error"] = lastErr.Error()
			}

			// Warn if reconcile hasn't run in a long time (e.g., 10x interval)
			reconcileInterval := s.cfg.ReconcileInterval.Duration
			if reconcileInterval == 0 {
				reconcileInterval = 30 * time.Second
			}
			warningThreshold := reconcileInterval * 10
			if time.Since(lastReconcile) > warningThreshold && !lastReconcile.IsZero() {
				response["warning"] = "reconcile hasn't run recently"
			}
		}
	}

	c.JSON(http.StatusOK, response)
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

		start := time.Now()

		// Process request
		c.Next()

		// Log request
		logger.Info("HTTP request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.String("client_ip", c.ClientIP()),
			zap.Duration("latency", time.Since(start)),
		)
	}
}

// authMiddleware validates internal authentication tokens
func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Extract internal token from header
		token := c.GetHeader(internalauth.DefaultTokenHeader)
		if token == "" {
			// Try Authorization header as fallback
			authHeader := c.GetHeader("Authorization")
			if authHeader != "" && len(authHeader) > 7 && authHeader[:7] == "Bearer " {
				token = authHeader[7:]
			}
		}

		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing internal authentication token",
			})
			return
		}

		// Validate token
		claims, err := s.authValidator.Validate(token)
		if err != nil {
			s.logger.Warn("Internal auth validation failed",
				zap.String("error", err.Error()),
				zap.String("client_ip", c.ClientIP()),
			)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "unauthorized: " + err.Error(),
			})
			return
		}

		// Store claims in context
		ctx := internalauth.WithClaims(c.Request.Context(), claims)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

func (s *Server) getInternalGatewayProxy(targetURL string) (*proxy.Router, error) {
	s.internalGatewayProxiesMu.RLock()
	p := s.internalGatewayProxies[targetURL]
	s.internalGatewayProxiesMu.RUnlock()
	if p != nil {
		return p, nil
	}

	s.internalGatewayProxiesMu.Lock()
	defer s.internalGatewayProxiesMu.Unlock()
	p = s.internalGatewayProxies[targetURL]
	if p != nil {
		return p, nil
	}

	proxyTimeout := s.cfg.ProxyTimeout.Duration
	if proxyTimeout == 0 {
		proxyTimeout = 10 * time.Second
	}

	p, err := proxy.NewRouter(targetURL, s.logger, proxyTimeout)
	if err != nil {
		return nil, err
	}
	s.internalGatewayProxies[targetURL] = p
	return p, nil
}

func (s *Server) getClusterByID(ctx context.Context, clusterID string) (*db.Cluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("cluster_id is required")
	}
	if cluster := s.getClusterFromCache(clusterID); cluster != nil {
		return cluster, nil
	}
	if err := s.refreshClusterCache(ctx); err != nil {
		return nil, err
	}
	return s.getClusterFromCache(clusterID), nil
}

func (s *Server) getClusterFromCache(clusterID string) *db.Cluster {
	s.clusterCacheMu.RLock()
	defer s.clusterCacheMu.RUnlock()
	return s.clusterCache[clusterID]
}

func (s *Server) refreshClusterCache(ctx context.Context) error {
	cacheTTL := s.cfg.ReconcileInterval.Duration
	if cacheTTL <= 0 {
		cacheTTL = 30 * time.Second
	}

	s.clusterCacheMu.RLock()
	cacheAge := time.Since(s.clusterCacheAt)
	s.clusterCacheMu.RUnlock()
	if cacheAge <= cacheTTL {
		return nil
	}

	clusters, err := s.repo.ListEnabledClusters(ctx)
	if err != nil {
		return fmt.Errorf("list enabled clusters: %w", err)
	}

	cache := make(map[string]*db.Cluster, len(clusters))
	for _, cluster := range clusters {
		cache[cluster.ClusterID] = cluster
	}

	s.clusterCacheMu.Lock()
	s.clusterCache = cache
	s.clusterCacheAt = time.Now()
	s.clusterCacheMu.Unlock()
	return nil
}
