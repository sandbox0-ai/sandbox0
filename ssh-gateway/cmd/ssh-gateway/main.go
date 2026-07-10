package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	gatewaymigrations "github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	sshserver "github.com/sandbox0-ai/sandbox0/ssh-gateway/pkg/server"
	"go.uber.org/zap"
)

func main() {
	cfg := config.LoadSSHGatewayConfig()

	logger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "ssh-gateway",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obsProvider, err := observability.New(observability.ConfigFromEnv("ssh-gateway", logger))
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(ctx)
	sshGatewayMetrics := obsmetrics.NewSSHGateway(obsProvider.MetricsRegistryOrNil())

	pool, err := initDatabase(ctx, cfg, logger, obsProvider)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer pool.Close()

	if err := runMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run database migrations", zap.Error(err))
	}

	controlPlaneKeyPath := cfg.ControlPlanePrivateKeyPath
	if controlPlaneKeyPath == "" {
		controlPlaneKeyPath = internalauth.DefaultInternalJWTPrivateKeyPath
	}
	controlPlanePrivateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(controlPlaneKeyPath)
	if err != nil {
		logger.Fatal("Failed to load control-plane internal auth private key", zap.Error(err))
	}
	controlPlaneAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     cfg.InternalAuthCaller,
		PrivateKey: controlPlanePrivateKey,
		TTL:        cfg.InternalAuthTTL.Duration,
	})

	dataPlaneKeyPath := cfg.DataPlanePrivateKeyPath
	if dataPlaneKeyPath == "" {
		dataPlaneKeyPath = internalauth.DefaultInternalJWTPrivateKeyPath
	}
	dataPlanePrivateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(dataPlaneKeyPath)
	if err != nil {
		logger.Fatal("Failed to load data-plane internal auth private key", zap.Error(err))
	}
	dataPlaneAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     cfg.InternalAuthCaller,
		PrivateKey: dataPlanePrivateKey,
		TTL:        cfg.InternalAuthTTL.Duration,
	})

	repo := identity.NewRepository(pool)
	resolver := sshserver.NewRegionalSandboxResolver(cfg.RegionalGatewayURL, controlPlaneAuthGen, logger, cfg.ResumeTimeout.Duration)
	resolver.SetHTTPClient(obsProvider.HTTP.NewClient(httpobs.Config{Timeout: cfg.ResumeTimeout.Duration}))
	authorizer := sshserver.NewAuthenticator(repo, resolver, cfg.ResumeTimeout.Duration, cfg.ResumePollInterval.Duration, logger)
	server, err := sshserver.NewServer(cfg, authorizer, dataPlaneAuthGen, logger, sshserver.WithMetrics(sshGatewayMetrics))
	if err != nil {
		logger.Fatal("Failed to create ssh-gateway server", zap.Error(err))
	}
	server.SetHTTPClient(obsProvider.HTTP.NewClient(httpobs.Config{Timeout: 10 * time.Second}))

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start(ctx)
	}()
	metricsServer := newMetricsServer()
	metricsErrCh := make(chan error, 1)
	go func() {
		logger.Info("Starting metrics server", zap.String("addr", metricsServer.Addr))
		metricsErrCh <- metricsServer.ListenAndServe()
	}()

	var runErr error
	select {
	case sig := <-sigChan:
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
	case err := <-errCh:
		if err != nil {
			runErr = fmt.Errorf("ssh-gateway: %w", err)
		}
	case err := <-metricsErrCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("metrics server: %w", err)
		}
	}
	cancel()

	shutdownTimeout := cfg.ShutdownTimeout.Duration
	if shutdownTimeout <= 0 {
		shutdownTimeout = 30 * time.Second
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil && runErr == nil {
		runErr = fmt.Errorf("shutdown metrics server: %w", err)
	}
	if runErr != nil {
		logger.Fatal("ssh-gateway exited with error", zap.Error(runErr))
	}
}

func newMetricsServer() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return &http.Server{
		Addr:              fmt.Sprintf(":%d", config.DefaultSSHGatewayMetricsPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func initDatabase(ctx context.Context, cfg *config.SSHGatewayConfig, logger *zap.Logger, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
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

func runMigrations(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	logger.Info("Running database migrations")
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(gatewaymigrations.FS),
		migrate.WithLogger(observability.NewMigrateLogger(logger)),
		migrate.WithSchema("shared_gateway"),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	logger.Info("Database migrations completed successfully")
	return nil
}
