package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	egressbrokerhttp "github.com/sandbox0-ai/sandbox0/egress-broker/pkg/http"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	cfg := config.LoadEgressBrokerConfig()

	logger, err := initLogger(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger.Info("Starting egress-broker",
		zap.Int("http_port", resolvedHTTPPort(cfg)),
		zap.String("region_id", cfg.RegionID),
		zap.String("cluster_id", cfg.ClusterID),
	)

	var bindingStore egressauth.BindingStore
	if cfg.DatabaseURL != "" {
		pool, err := dbpool.New(ctx, dbpool.Options{
			DatabaseURL: cfg.DatabaseURL,
			MaxConns:    cfg.DatabaseMaxConns,
			MinConns:    cfg.DatabaseMinConns,
			Schema:      "sched",
		})
		if err != nil {
			logger.Fatal("Failed to connect to database", zap.Error(err))
		}
		defer pool.Close()

		if err := egressauth.RunMigrations(ctx, pool, &zapMigrateLogger{logger: logger}); err != nil {
			logger.Fatal("Failed to run egress auth migrations", zap.Error(err))
		}
		bindingStore = egressauth.NewRepository(pool)
	}

	server := egressbrokerhttp.NewServer(cfg, logger, bindingStore)
	if err := server.Start(ctx); err != nil {
		logger.Fatal("egress-broker exited with error", zap.Error(err))
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

	cfg := zap.Config{
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
	return cfg.Build()
}

func resolvedHTTPPort(cfg *config.EgressBrokerConfig) int {
	if cfg == nil || cfg.HTTPPort == 0 {
		return 8082
	}
	return cfg.HTTPPort
}

type zapMigrateLogger struct {
	logger *zap.Logger
}

func (z *zapMigrateLogger) Printf(format string, args ...any) {
	z.logger.Info(fmt.Sprintf(format, args...))
}

func (z *zapMigrateLogger) Fatalf(format string, args ...any) {
	z.logger.Fatal(fmt.Sprintf(format, args...))
}
