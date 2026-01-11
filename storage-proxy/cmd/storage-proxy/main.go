package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/infra/pkg/env"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/k8s"
	"github.com/sandbox0-ai/infra/pkg/migrate"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/auth"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/config"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/db"
	grpcserver "github.com/sandbox0-ai/infra/storage-proxy/pkg/grpc"
	httpserver "github.com/sandbox0-ai/infra/storage-proxy/pkg/http"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/volume"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/watcher"
	pb "github.com/sandbox0-ai/infra/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Load environment variables from .env file
	env.Load()

	// Setup logger (logrus for compatibility)
	logrusLogger := logrus.New()
	logrusLogger.SetFormatter(&logrus.JSONFormatter{})
	logrusLogger.SetOutput(os.Stdout)

	// Load configuration
	cfg := config.LoadFromEnv()
	if err := cfg.Validate(); err != nil {
		logrusLogger.WithError(err).Fatal("Invalid configuration")
	}

	// Set log level
	level, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		logrusLogger.WithError(err).Warn("Invalid log level, using info")
		level = logrus.InfoLevel
	}
	logrusLogger.SetLevel(level)

	// Setup zap logger for new components
	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	if cfg.LogLevel == "debug" {
		zapConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	}
	zapLogger, err := zapConfig.Build()
	if err != nil {
		logrusLogger.WithError(err).Fatal("Failed to create zap logger")
	}
	defer zapLogger.Sync()

	zapLogger.Info("Starting storage-proxy",
		zap.Int("grpc_port", cfg.GRPCPort),
		zap.Int("http_port", cfg.HTTPPort),
		zap.String("log_level", cfg.LogLevel),
		zap.String("cache_dir", cfg.DefaultCacheDir),
	)

	// Initialize database connection pool
	var repo *db.Repository
	if cfg.DatabaseURL != "" {
		pool, err := initDatabase(context.Background(), cfg.DatabaseURL, zapLogger)
		if err != nil {
			zapLogger.Fatal("Failed to connect to database", zap.Error(err))
		}
		defer pool.Close()

		// Run database migrations
		if err := runMigrations(context.Background(), pool, zapLogger); err != nil {
			zapLogger.Fatal("Failed to run database migrations", zap.Error(err))
		}

		repo = db.NewRepository(pool)
	} else {
		zapLogger.Warn("DATABASE_URL not set, running without database persistence")
	}

	// Create volume manager
	volMgr := volume.NewManager(logrusLogger, cfg, repo)

	// Create Kubernetes client for pod watching
	k8sClient, err := k8s.NewClient(cfg.KubeconfigPath)
	if err != nil {
		zapLogger.Warn("Failed to create Kubernetes client, pod watcher disabled",
			zap.Error(err),
		)
	} else {
		// Create and start sandbox watcher
		podWatcher := watcher.NewWatcher(
			k8sClient,
			"", // Watch all namespaces
			10*time.Minute,
			zapLogger,
		)

		// Set up delete handler to auto-unmount volumes
		podWatcher.SetPodDeleteHandler(func(info *watcher.SandboxInfo) {
			zapLogger.Info("Sandbox pod deleted, unmounting volumes",
				zap.String("sandbox_id", info.SandboxID),
			)
			if errs := volMgr.UnmountSandboxVolumes(context.Background(), info.SandboxID); errs != nil {
				zapLogger.Error("Errors unmounting sandbox volumes",
					zap.String("sandbox_id", info.SandboxID),
					zap.Int("error_count", len(errs)),
				)
			}
		})

		// Start watcher in background
		go func() {
			if err := podWatcher.Start(context.Background()); err != nil {
				zapLogger.Error("Watcher failed", zap.Error(err))
			}
		}()

		zapLogger.Info("Sandbox watcher started")
	}

	// Create authenticator based on config
	var grpcInterceptor grpc.UnaryServerInterceptor
	var httpAuthenticator *auth.HTTPAuthenticator

	if cfg.InternalAuthPublicKey != "" {
		// Use new internalauth validator
		publicKeyBytes, err := base64.StdEncoding.DecodeString(cfg.InternalAuthPublicKey)
		if err != nil {
			zapLogger.Fatal("Failed to decode internal auth public key",
				zap.Error(err),
			)
		}

		if len(publicKeyBytes) != ed25519.PublicKeySize {
			zapLogger.Fatal("Invalid internal auth public key size",
				zap.Int("expected", ed25519.PublicKeySize),
				zap.Int("actual", len(publicKeyBytes)),
			)
		}

		publicKey := ed25519.PublicKey(publicKeyBytes)
		validator := internalauth.NewValidator(internalauth.ValidatorConfig{
			Target:                 "storage-proxy",
			PublicKey:              publicKey,
			AllowedCallers:         []string{"internal-gateway", "manager", "procd"},
			ClockSkewTolerance:     5 * time.Second,
			ReplayDetectionEnabled: false, // Disable for high-throughput scenarios
		})

		authenticator := auth.NewGRPCAuthenticator(validator, zapLogger)
		grpcInterceptor = authenticator.UnaryInterceptor()

		httpAuthenticator = auth.NewHTTPAuthenticator(validator, zapLogger)

		zapLogger.Info("Using internalauth validator for gRPC and HTTP authentication")
	} else {
		zapLogger.Fatal("No authentication method configured")
	}

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpcInterceptor),
	)

	// Register FileSystem service
	fsServer := grpcserver.NewFileSystemServer(volMgr, logrusLogger)
	pb.RegisterFileSystemServer(grpcServer, fsServer)

	// Register health service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Enable reflection for grpcurl
	reflection.Register(grpcServer)

	// Start gRPC server
	grpcAddr := fmt.Sprintf("%s:%d", cfg.GRPCAddr, cfg.GRPCPort)
	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logrusLogger.WithError(err).Fatal("Failed to listen for gRPC")
	}

	go func() {
		logrusLogger.WithField("address", grpcAddr).Info("Starting gRPC server")
		if err := grpcServer.Serve(grpcListener); err != nil {
			logrusLogger.WithError(err).Fatal("Failed to serve gRPC")
		}
	}()

	// Create HTTP server
	httpSrv := httpserver.NewServer(logrusLogger, repo, httpAuthenticator)
	httpAddr := fmt.Sprintf("%s:%d", cfg.HTTPAddr, cfg.HTTPPort)
	httpServer := &http.Server{
		Addr:         httpAddr,
		Handler:      httpSrv,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logrusLogger.WithField("address", httpAddr).Info("Starting HTTP server")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrusLogger.WithError(err).Fatal("Failed to serve HTTP")
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	zapLogger.Info("Shutting down gracefully...")

	// Shutdown HTTP server
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		zapLogger.Error("HTTP server shutdown error", zap.Error(err))
	}

	// Stop gRPC server
	grpcServer.GracefulStop()

	// Unmount all volumes
	for _, volumeID := range volMgr.ListVolumes() {
		zapLogger.Info("Unmounting volume", zap.String("volume_id", volumeID))
		if err := volMgr.UnmountVolume(context.Background(), volumeID); err != nil {
			zapLogger.Error("Failed to unmount volume",
				zap.String("volume_id", volumeID),
				zap.Error(err),
			)
		}
	}

	zapLogger.Info("Shutdown complete")
}

// initDatabase initializes the database connection pool
func initDatabase(ctx context.Context, databaseURL string, logger *zap.Logger) (*pgxpool.Pool, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("database URL is empty")
	}

	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}

	// Configure pool
	poolConfig.MaxConns = 30
	poolConfig.MinConns = 5

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	logger.Info("Database connection established",
		zap.Int32("max_conns", poolConfig.MaxConns),
		zap.Int32("min_conns", poolConfig.MinConns),
	)

	return pool, nil
}

// runMigrations runs database migrations on startup
func runMigrations(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	logger.Info("Running database migrations")

	// Create a migration logger that writes to zap
	migrateLogger := &zapLogger{logger: logger}

	if err := migrate.Up(ctx, pool, "migrations",
		migrate.WithLogger(migrateLogger),
		migrate.WithSchema("sp"),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}

	logger.Info("Database migrations completed successfully")
	return nil
}

// zapLogger adapts zap.Logger to migrate.Logger interface
type zapLogger struct {
	logger *zap.Logger
}

func (z *zapLogger) Printf(format string, args ...interface{}) {
	z.logger.Info(fmt.Sprintf(format, args...))
}

func (z *zapLogger) Fatalf(format string, args ...interface{}) {
	z.logger.Fatal(fmt.Sprintf(format, args...))
}
