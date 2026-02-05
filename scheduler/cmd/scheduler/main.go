package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/pkg/clock"
	"github.com/sandbox0-ai/infra/pkg/dbpool"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/migrate"
	"github.com/sandbox0-ai/infra/pkg/observability"
	obsmetrics "github.com/sandbox0-ai/infra/pkg/observability/metrics"
	"github.com/sandbox0-ai/infra/pkg/pubsub"
	schedmigrations "github.com/sandbox0-ai/infra/scheduler/migrations"
	"github.com/sandbox0-ai/infra/scheduler/pkg/client"
	"github.com/sandbox0-ai/infra/scheduler/pkg/db"
	httpserver "github.com/sandbox0-ai/infra/scheduler/pkg/http"
	schedpubsub "github.com/sandbox0-ai/infra/scheduler/pkg/pubsub"
	"github.com/sandbox0-ai/infra/scheduler/pkg/reconciler"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Load configuration
	cfg := config.LoadSchedulerConfig()

	// Initialize logger
	logger := initLogger(cfg.LogLevel)
	defer logger.Sync()

	logger.Info("Starting Scheduler",
		zap.String("version", "v0.1.0"),
		zap.Int("httpPort", cfg.HTTPPort),
	)

	// Create context that cancels on signal
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Initialize observability provider
	obsProvider, err := observability.New(observability.Config{
		ServiceName: "scheduler",
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
	clk, err := clock.New(ctx, &pgxPoolAdapter{pool: pool},
		clock.WithSyncInterval(30*time.Second),
		clock.WithLogger(&zapClockLogger{logger: logger}),
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

	// Create internal auth generator (for calling internal-gateway)
	internalAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "scheduler",
		PrivateKey: privateKey,
		TTL:        30 * time.Second,
	})

	// Create internal auth validator (for validating requests from edge-gateway)
	validatorConfig := internalauth.DefaultValidatorConfig("scheduler", publicKey)
	validatorConfig.AllowedCallers = []string{"edge-gateway"}
	authValidator := internalauth.NewValidator(validatorConfig)

	logger.Info("Internal authentication enabled",
		zap.String("target", "scheduler"),
		zap.Strings("allowed_callers", validatorConfig.AllowedCallers),
	)

	// Create internal-gateway client
	igClient := client.NewInternalGatewayClient(internalAuthGen, logger, obsProvider)

	// Create reconciler
	rec := reconciler.NewReconciler(repo, igClient, cfg.ReconcileInterval.Duration, clk, cfg.PodsPerNode, logger, schedulerMetrics)

	// Create HTTP server
	httpServer := httpserver.NewServer(cfg, repo, authValidator, internalAuthGen, rec, logger, obsProvider)

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

	migrateLogger := &zapLogger{logger: logger}
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(schedmigrations.FS),
		migrate.WithLogger(migrateLogger),
		migrate.WithSchema("sched"),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}

	logger.Info("Database migrations completed successfully")
	return nil
}

func initLogger(level string) *zap.Logger {
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}

	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(zapLevel),
		Development: false,
		Encoding:    "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
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

	logger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("failed to create logger: %v", err))
	}

	return logger
}

// zapLogger adapts zap.Logger to migrate.Logger interface.
type zapLogger struct {
	logger *zap.Logger
}

func (z *zapLogger) Printf(format string, args ...any) {
	z.logger.Info(fmt.Sprintf(format, args...))
}

func (z *zapLogger) Fatalf(format string, args ...any) {
	z.logger.Fatal(fmt.Sprintf(format, args...))
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
		Schema:          "sched",
	})
	if err != nil {
		return nil, err
	}

	// Wrap pool with observability
	obsProvider.Pgx.WrapPool(pool)

	logger.Info("Connected to database",
		zap.Int32("max_conns", pool.Config().MaxConns),
		zap.Int32("min_conns", pool.Config().MinConns),
	)

	return pool, nil
}

// pgxPoolAdapter adapts pgxpool.Pool to clock.DB interface
type pgxPoolAdapter struct {
	pool *pgxpool.Pool
}

type pgxRowAdapter struct {
	row interface {
		Scan(dest ...any) error
	}
}

func (r *pgxRowAdapter) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

func (a *pgxPoolAdapter) QueryRow(ctx context.Context, sql string, args ...any) clock.Row {
	return &pgxRowAdapter{row: a.pool.QueryRow(ctx, sql, args...)}
}

// zapClockLogger adapts zap.Logger to clock.Logger interface
type zapClockLogger struct {
	logger *zap.Logger
}

func (z *zapClockLogger) Info(msg string, keysAndValues ...any) {
	z.logger.Info(msg, toZapFields(keysAndValues)...)
}

func (z *zapClockLogger) Warn(msg string, keysAndValues ...any) {
	z.logger.Warn(msg, toZapFields(keysAndValues)...)
}

func (z *zapClockLogger) Error(msg string, keysAndValues ...any) {
	z.logger.Error(msg, toZapFields(keysAndValues)...)
}

// toZapFields converts key-value pairs to zap fields
func toZapFields(keysAndValues []any) []zap.Field {
	if len(keysAndValues)%2 != 0 {
		return []zap.Field{zap.Any("args", keysAndValues)}
	}

	fields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			continue
		}
		fields = append(fields, zap.Any(key, keysAndValues[i+1]))
	}

	return fields
}
