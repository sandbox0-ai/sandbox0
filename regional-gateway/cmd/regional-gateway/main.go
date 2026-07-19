package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	gatewaymigrations "github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/regional-gateway/pkg/http"
	"go.uber.org/zap"
)

func main() {
	// Load configuration
	cfg := config.LoadRegionalGatewayConfig()

	// Initialize logger
	logger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "regional-gateway",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting regional-gateway",
		zap.Int("port", cfg.HTTPPort),
		zap.String("cluster_gateway_url", cfg.DefaultClusterGatewayURL),
	)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize observability provider
	obsProvider, err := observability.New(observability.ConfigFromEnv("regional-gateway", logger))
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(ctx)

	// Initialize database connection pool
	pool, err := initDatabase(ctx, cfg, logger, obsProvider)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer pool.Close()

	// Run database migrations
	if err := runMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run database migrations", zap.Error(err))
	}
	cfg.TeamQuota.DistributedEnforcement.RedisKeyPrefix =
		coreteamquota.NormalizeTeamQuotaRedisKeyPrefix(
			cfg.TeamQuota.DistributedEnforcement.RedisKeyPrefix,
		)
	policyCoordinator, err := coreteamquota.NewPolicyCoordinator(
		ctx,
		pool,
		coreteamquota.PolicyCoordinatorConfig{
			RegionID:        cfg.RegionID,
			ExpectedStateID: cfg.TeamQuota.DistributedEnforcement.StateID,
			RedisURL:        cfg.TeamQuota.DistributedEnforcement.RedisURL,
			RedisKeyPrefix:  cfg.TeamQuota.DistributedEnforcement.RedisKeyPrefix,
			RedisTimeout:    cfg.TeamQuota.DistributedEnforcement.RedisTimeout.Duration,
			LeaseTTL:        cfg.TeamQuota.DistributedEnforcement.LeaseTTL.Duration,
		},
	)
	if err != nil {
		logger.Fatal("Failed to initialize Team Quota policy coordinator", zap.Error(err))
	}
	defer policyCoordinator.Close()
	if err := gatewayteamquota.ReplaceConfiguredDefaults(
		ctx,
		policyCoordinator,
		cfg.TeamQuota,
	); err != nil {
		logger.Fatal("Failed to reconcile Team Quota defaults", zap.Error(err))
	}
	go policyCoordinator.RunRepair(ctx, func(err error) {
		logger.Error("Failed to repair Team Quota policy guard", zap.Error(err))
	})
	meteringDB, meteringRepo, err := initMetering(ctx, cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize metering backend", zap.Error(err))
	}
	if meteringDB != nil {
		defer meteringDB.Close()
	}

	// Create HTTP server
	server, err := http.NewServer(
		cfg,
		pool,
		logger,
		obsProvider,
		http.WithMeteringReader(meteringRepo),
		http.WithTeamQuotaPolicyManager(policyCoordinator),
	)
	if err != nil {
		logger.Fatal("Failed to create HTTP server", zap.Error(err))
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Start(ctx)
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		cancel()
	case err := <-errChan:
		if err != nil {
			logger.Error("Server error", zap.Error(err))
		}
	}

	logger.Info("Edge gateway shutdown complete")
}

// initDatabase initializes the database connection pool
func initDatabase(ctx context.Context, cfg *config.RegionalGatewayConfig, logger *zap.Logger, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:    cfg.DatabaseURL,
		MaxConns:       int32(cfg.DatabaseMaxConns),
		MinConns:       int32(cfg.DatabaseMinConns),
		Schema:         "shared_gateway",
		ConfigModifier: obsProvider.Pgx.ConfigModifier(),
	})
	if err != nil {
		return nil, err
	}

	logger.Info("Database connection established",
		zap.Int32("max_conns", pool.Config().MaxConns),
		zap.Int32("min_conns", pool.Config().MinConns),
	)

	return pool, nil
}

// runMigrations runs database migrations on startup
func runMigrations(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	logger.Info("Running database migrations")

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(gatewaymigrations.FS),
		migrate.WithLogger(observability.NewMigrateLogger(logger)),
		migrate.WithSchema("shared_gateway"),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	if err := coreteamquota.RunMigrations(
		ctx,
		pool,
		observability.NewMigrateLogger(logger),
	); err != nil {
		return err
	}

	logger.Info("Database migrations completed successfully")
	return nil
}
