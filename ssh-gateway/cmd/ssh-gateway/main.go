package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	managerclient "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	gatewaymigrations "github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	sshserver "github.com/sandbox0-ai/sandbox0/ssh-gateway/pkg/server"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	cfg := config.LoadSSHGatewayConfig()

	logger, err := initLogger(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obsProvider, err := observability.New(observability.Config{
		ServiceName: "ssh-gateway",
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

	if err := runMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run database migrations", zap.Error(err))
	}

	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		logger.Fatal("Failed to load internal auth private key", zap.Error(err))
	}
	internalAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     cfg.InternalAuthCaller,
		PrivateKey: privateKey,
		TTL:        cfg.InternalAuthTTL.Duration,
	})

	repo := identity.NewRepository(pool)
	manager := managerclient.NewManagerClient(cfg.ManagerURL, internalAuthGen, logger, cfg.ResumeTimeout.Duration)
	authorizer := sshserver.NewAuthenticator(repo, manager, cfg.ResumeTimeout.Duration, cfg.ResumePollInterval.Duration, logger)
	server, err := sshserver.NewServer(cfg, authorizer, internalAuthGen, logger)
	if err != nil {
		logger.Fatal("Failed to create ssh-gateway server", zap.Error(err))
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start(ctx)
	}()

	select {
	case sig := <-sigChan:
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		cancel()
	case err := <-errCh:
		if err != nil {
			logger.Fatal("ssh-gateway exited with error", zap.Error(err))
		}
	}
}

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

func initDatabase(ctx context.Context, cfg *config.SSHGatewayConfig, logger *zap.Logger, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: cfg.DatabaseURL,
		MaxConns:    int32(cfg.DatabaseMaxConns),
		MinConns:    int32(cfg.DatabaseMinConns),
		Schema:      "shared_gateway",
	})
	if err != nil {
		return nil, err
	}
	obsProvider.Pgx.WrapPool(pool)
	logger.Info("Database connection established",
		zap.Int32("max_conns", pool.Config().MaxConns),
		zap.Int32("min_conns", pool.Config().MinConns),
	)
	return pool, nil
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	logger.Info("Running database migrations")
	migrateLogger := &zapLogger{logger: logger}
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(gatewaymigrations.FS),
		migrate.WithLogger(migrateLogger),
		migrate.WithSchema("shared_gateway"),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	logger.Info("Database migrations completed successfully")
	return nil
}

type zapLogger struct {
	logger *zap.Logger
}

func (z *zapLogger) Printf(format string, args ...any) {
	z.logger.Info(fmt.Sprintf(format, args...))
}

func (z *zapLogger) Fatalf(format string, args ...any) {
	z.logger.Fatal(fmt.Sprintf(format, args...))
}
