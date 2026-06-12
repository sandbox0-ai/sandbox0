package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	globalhttp "github.com/sandbox0-ai/sandbox0/global-gateway/pkg/http"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	gatewaymigrations "github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"go.uber.org/zap"
)

func main() {
	cfg := config.LoadGlobalGatewayConfig()

	logger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "global-gateway",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting global-gateway", zap.Int("port", cfg.HTTPPort))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obsProvider, err := observability.New(observability.Config{
		ServiceName: "global-gateway",
		Logger:      logger,
		TraceExporter: observability.TraceExporterConfig{
			Type:     os.Getenv("OTEL_EXPORTER_TYPE"),
			Endpoint: os.Getenv("OTEL_EXPORTER_ENDPOINT"),
		},
	})
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(ctx)

	pool, err := initDatabase(ctx, cfg, logger, obsProvider)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer pool.Close()

	if err := runMigrations(ctx, pool, cfg, logger); err != nil {
		logger.Fatal("Failed to run database migrations", zap.Error(err))
	}

	server, err := globalhttp.NewServer(cfg, pool, logger, obsProvider)
	if err != nil {
		logger.Fatal("Failed to create HTTP server", zap.Error(err))
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Start(ctx)
	}()

	select {
	case sig := <-sigChan:
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		cancel()
	case err := <-errChan:
		if err != nil {
			logger.Error("Server error", zap.Error(err))
		}
	}

	logger.Info("Global gateway shutdown complete")
}

func initDatabase(
	ctx context.Context,
	cfg *config.GlobalGatewayConfig,
	logger *zap.Logger,
	obsProvider *observability.Provider,
) (*pgxpool.Pool, error) {
	schema := cfg.DatabaseSchema
	if schema == "" {
		schema = "global_gateway"
	}

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:    cfg.DatabaseURL,
		MaxConns:       int32(cfg.DatabaseMaxConns),
		MinConns:       int32(cfg.DatabaseMinConns),
		Schema:         schema,
		ConfigModifier: obsProvider.Pgx.ConfigModifier(),
	})
	if err != nil {
		return nil, err
	}

	logger.Info("Database connection established",
		zap.String("schema", schema),
		zap.Int32("max_conns", pool.Config().MaxConns),
		zap.Int32("min_conns", pool.Config().MinConns),
	)
	return pool, nil
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool, cfg *config.GlobalGatewayConfig, logger *zap.Logger) error {
	schema := cfg.DatabaseSchema
	if schema == "" {
		schema = "global_gateway"
	}

	logger.Info("Running database migrations", zap.String("schema", schema))
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(gatewaymigrations.FS),
		migrate.WithLogger(observability.NewMigrateLogger(logger)),
		migrate.WithSchema(schema),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}
