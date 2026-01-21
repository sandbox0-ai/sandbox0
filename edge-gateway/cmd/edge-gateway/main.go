package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	egmigrations "github.com/sandbox0-ai/infra/edge-gateway/migrations"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/edge-gateway/pkg/http"
	"github.com/sandbox0-ai/infra/pkg/migrate"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Load configuration
	cfg := config.LoadEdgeGatewayConfig()

	// Initialize logger
	logger, err := initLogger(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting edge-gateway",
		zap.Int("port", cfg.HTTPPort),
		zap.String("internal_gateway_url", cfg.DefaultInternalGatewayURL),
	)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize database connection pool
	pool, err := initDatabase(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer pool.Close()

	// Run database migrations
	if err := runMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run database migrations", zap.Error(err))
	}

	// Create HTTP server
	server, err := http.NewServer(cfg, pool, logger)
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

// initLogger initializes the zap logger
func initLogger(level string) (*zap.Logger, error) {
	var logLevel zapcore.Level
	switch level {
	case "debug":
		logLevel = zapcore.DebugLevel
	case "info":
		logLevel = zapcore.InfoLevel
	case "warn":
		logLevel = zapcore.WarnLevel
	case "error":
		logLevel = zapcore.ErrorLevel
	default:
		logLevel = zapcore.InfoLevel
	}

	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(logLevel),
		Development: false,
		Encoding:    "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	return config.Build()
}

// initDatabase initializes the database connection pool
func initDatabase(ctx context.Context, databaseURL string, logger *zap.Logger) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}

	// Configure pool
	poolConfig.MaxConns = 30
	poolConfig.MinConns = 8

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

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(egmigrations.FS),
		migrate.WithLogger(migrateLogger),
		migrate.WithSchema("eg"),
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

func (z *zapLogger) Printf(format string, args ...any) {
	z.logger.Info(fmt.Sprintf(format, args...))
}

func (z *zapLogger) Fatalf(format string, args ...any) {
	z.logger.Fatal(fmt.Sprintf(format, args...))
}
