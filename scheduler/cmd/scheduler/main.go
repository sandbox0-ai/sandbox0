package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"github.com/sandbox0-ai/sandbox0/pkg/pubsub"
	templmigrations "github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	templstorepg "github.com/sandbox0-ai/sandbox0/pkg/template/store/pg"
	"github.com/sandbox0-ai/sandbox0/scheduler/pkg/client"
	"github.com/sandbox0-ai/sandbox0/scheduler/pkg/db"
	httpserver "github.com/sandbox0-ai/sandbox0/scheduler/pkg/http"
	obsmetrics "github.com/sandbox0-ai/sandbox0/scheduler/pkg/metrics"
	schedpubsub "github.com/sandbox0-ai/sandbox0/scheduler/pkg/pubsub"
	"github.com/sandbox0-ai/sandbox0/scheduler/pkg/reconciler"
	"go.uber.org/zap"
)

func main() {
	// Load configuration
	cfg := config.LoadSchedulerConfig()

	// Initialize logger
	logger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "scheduler",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting Scheduler",
		zap.String("version", "v0.1.0"),
		zap.Int("httpPort", cfg.HTTPPort),
	)

	// Create context that cancels on signal
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Initialize observability provider
	obsProvider, err := observability.New(observability.ConfigFromEnv("scheduler", logger))
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(ctx)

	schedulerMetrics := obsmetrics.NewScheduler(obsProvider.MetricsRegistryOrNil())

	// Initialize database pool
	pool, err := initDatabase(ctx, cfg, logger, obsProvider)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer pool.Close()

	// Run database migrations
	if err := runMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run database migrations", zap.Error(err))
	}

	// Initialize clock for cross-cluster time synchronization
	clk, err := clock.NewPGX(ctx, pool,
		clock.WithSyncInterval(30*time.Second),
		clock.WithZapLogger(logger),
	)
	if err != nil {
		logger.Fatal("Failed to initialize clock", zap.Error(err))
	}
	defer clk.Close()

	logger.Info("Clock initialized for cross-cluster time synchronization",
		zap.Int64("offset_ms", clk.Offset().Milliseconds()),
		zap.Int64("rtt_ms", clk.LastRTT().Milliseconds()),
	)

	// Create repository
	repo := db.NewRepository(pool)
	templateStore := templstorepg.NewStore(pool)

	// Initialize internal auth
	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		logger.Fatal("Failed to load internal auth private key",
			zap.String("path", internalauth.DefaultInternalJWTPrivateKeyPath),
			zap.Error(err),
		)
	}

	publicKey, err := internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultInternalJWTPublicKeyPath)
	if err != nil {
		logger.Fatal("Failed to load internal auth public key",
			zap.String("path", internalauth.DefaultInternalJWTPublicKeyPath),
			zap.Error(err),
		)
	}

	// Create internal auth generator (for calling cluster-gateway)
	internalAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "scheduler",
		PrivateKey: privateKey,
		TTL:        30 * time.Second,
	})

	// Create internal auth validator (for validating requests from regional-gateway)
	validatorConfig := internalauth.DefaultValidatorConfig("scheduler", publicKey)
	validatorConfig.AllowedCallers = []string{"regional-gateway"}
	authValidator := internalauth.NewValidator(validatorConfig)

	logger.Info("Internal authentication enabled",
		zap.String("target", "scheduler"),
		zap.Strings("allowed_callers", validatorConfig.AllowedCallers),
	)

	// Create cluster-gateway client
	clusterGatewayClient := client.NewClusterGatewayClient(internalAuthGen, logger, obsProvider)
	templateSourceResolver := httpserver.NewSchedulerSandboxTemplateSourceResolver(repo, clusterGatewayClient)

	// Create reconciler
	rec := reconciler.NewReconciler(templateStore, templateStore, repo, clusterGatewayClient, cfg.ReconcileInterval.Duration, clk, cfg.PodsPerNode, logger, schedulerMetrics)

	// Create HTTP server
	httpServer, err := httpserver.NewServerWithDependencies(httpserver.ServerDependencies{
		Config:         cfg,
		Clusters:       repo,
		Templates:      templateStore,
		Allocations:    templateStore,
		SourceResolver: templateSourceResolver,
		AuthValidator:  authValidator,
		InternalAuth:   internalAuthGen,
		Reconciler:     rec,
		Logger:         logger,
		Observability:  obsProvider,
		Metrics:        schedulerMetrics,
	})
	if err != nil {
		logger.Fatal("Failed to create scheduler HTTP server", zap.Error(err))
	}

	// Start template idle listener
	if cfg.DatabaseURL != "" {
		schedpubsub.StartTemplateIdleListener(ctx, cfg.DatabaseURL, logger, func(event pubsub.TemplateIdleEvent) {
			logger.Info("Received template idle event",
				zap.String("cluster_id", event.ClusterID),
				zap.String("template_id", event.TemplateID),
				zap.Int32("idle_count", event.IdleCount),
				zap.Int32("active_count", event.ActiveCount),
			)
			rec.UpdateTemplateStats(event.ClusterID, event.TemplateID, event.IdleCount, event.ActiveCount, event.Timestamp)
		})
	}

	// Start reconciler in background
	go rec.Start(ctx)

	// Start HTTP server (blocks until context is cancelled)
	if err := httpServer.Start(ctx); err != nil {
		logger.Fatal("HTTP server error", zap.Error(err))
	}

	logger.Info("Scheduler shutdown complete")
}

// runMigrations runs database migrations on startup.
func runMigrations(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	logger.Info("Running database migrations")

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(templmigrations.FS),
		migrate.WithLogger(observability.NewMigrateLogger(logger)),
		migrate.WithSchema("scheduler"),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}

	logger.Info("Database migrations completed successfully")
	return nil
}

func initDatabase(ctx context.Context, cfg *config.SchedulerConfig, logger *zap.Logger, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
	maxConnLifetime := cfg.DatabasePool.MaxConnLifetime.Duration
	if maxConnLifetime == 0 {
		maxConnLifetime = 30 * time.Minute
	}
	maxConnIdleTime := cfg.DatabasePool.MaxConnIdleTime.Duration
	if maxConnIdleTime == 0 {
		maxConnIdleTime = 5 * time.Minute
	}

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     cfg.DatabaseURL,
		MaxConns:        cfg.DatabasePool.MaxConns,
		MinConns:        cfg.DatabasePool.MinConns,
		DefaultMaxConns: 10,
		DefaultMinConns: 2,
		MaxConnLifetime: maxConnLifetime,
		MaxConnIdleTime: maxConnIdleTime,
		Schema:          "scheduler",
		ConfigModifier:  obsProvider.Pgx.ConfigModifier(),
	})
	if err != nil {
		return nil, err
	}

	logger.Info("Connected to database",
		zap.Int32("max_conns", pool.Config().MaxConns),
		zap.Int32("min_conns", pool.Config().MinConns),
	)

	return pool, nil
}
