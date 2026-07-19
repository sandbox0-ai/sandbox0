package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/http"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	gatewaymigrations "github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap"
)

func main() {
	// Load configuration
	cfg := config.LoadClusterGatewayConfig()

	// Initialize logger
	logger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "cluster-gateway",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting cluster-gateway",
		zap.Int("port", cfg.HTTPPort),
		zap.String("manager_url", cfg.ManagerURL),
		zap.String("manager_storage_url", cfg.ManagerStorageURL),
	)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize observability provider
	obsProvider, err := observability.New(observability.ConfigFromEnv("cluster-gateway", logger))
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(ctx)

	var pool *pgxpool.Pool
	var policyCoordinator *coreteamquota.PolicyCoordinator
	if strings.TrimSpace(cfg.DatabaseURL) != "" {
		pool = initDatabase(ctx, cfg, logger, obsProvider)
		defer pool.Close()

		if err := runMigrations(ctx, pool, logger); err != nil {
			logger.Fatal("Failed to run database migrations", zap.Error(err))
		}
		if cfg.TeamQuota.PolicyOwner {
			cfg.TeamQuota.DistributedEnforcement.RedisKeyPrefix =
				coreteamquota.NormalizeTeamQuotaRedisKeyPrefix(
					cfg.TeamQuota.DistributedEnforcement.RedisKeyPrefix,
				)
			policyCoordinator, err = coreteamquota.NewPolicyCoordinator(
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
		} else {
			stateIdentity, err := coreteamquota.ClaimRegionStateIdentity(
				ctx,
				pool,
				coreteamquota.RegionStateIdentityConfig{
					RegionID:        cfg.RegionID,
					ExpectedStateID: cfg.TeamQuota.DistributedEnforcement.StateID,
					RedisURL:        cfg.TeamQuota.DistributedEnforcement.RedisURL,
					RedisKeyPrefix:  cfg.TeamQuota.DistributedEnforcement.RedisKeyPrefix,
					RedisTimeout:    cfg.TeamQuota.DistributedEnforcement.RedisTimeout.Duration,
				},
			)
			if err != nil {
				logger.Fatal("Failed to validate Team Quota region state identity", zap.Error(err))
			}
			cfg.TeamQuota.DistributedEnforcement.RedisKeyPrefix = stateIdentity.KeyPrefix
		}
	}
	sandboxObservabilityDB, sandboxObservabilityRepo, err := initSandboxObservability(ctx, cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize sandbox observability backend", zap.Error(err))
	}
	if sandboxObservabilityDB != nil {
		defer sandboxObservabilityDB.Close()
	}
	meteringDB, meteringRepo, err := initMetering(ctx, cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize metering backend", zap.Error(err))
	}
	if meteringDB != nil {
		defer meteringDB.Close()
	}

	// Create HTTP server
	serverOptions := []http.ServerOption{
		http.WithSandboxObservabilityRepository(sandboxObservabilityRepo),
		http.WithMeteringReader(meteringRepo),
	}
	if policyCoordinator != nil {
		serverOptions = append(serverOptions, http.WithTeamQuotaPolicyManager(policyCoordinator))
	}
	server, err := http.NewServer(
		cfg,
		pool,
		logger,
		obsProvider,
		serverOptions...,
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

	logger.Info("Internal gateway shutdown complete")
}

func initDatabase(ctx context.Context, cfg *config.ClusterGatewayConfig, logger *zap.Logger, obsProvider *observability.Provider) *pgxpool.Pool {
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:    cfg.DatabaseURL,
		MaxConns:       int32(cfg.DatabaseMaxConns),
		MinConns:       int32(cfg.DatabaseMinConns),
		Schema:         "shared_gateway",
		ConfigModifier: obsProvider.Pgx.ConfigModifier(),
	})
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}

	logger.Info("Database connection established",
		zap.Int32("max_conns", pool.Config().MaxConns),
		zap.Int32("min_conns", pool.Config().MinConns),
	)

	return pool
}

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
