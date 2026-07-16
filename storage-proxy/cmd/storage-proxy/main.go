package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	storageproxyruntime "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/runtime"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

func main() {
	cfg := config.LoadStorageProxyConfig()
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid storage-proxy configuration: %v\n", err)
		os.Exit(1)
	}

	logrusLogger := logrus.New()
	logrusLogger.SetFormatter(&logrus.JSONFormatter{})
	logrusLogger.SetOutput(os.Stdout)
	level, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = logrus.InfoLevel
	}
	logrusLogger.SetLevel(level)

	logger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "storage-proxy",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		logrusLogger.WithError(err).Fatal("Failed to create storage-proxy logger")
	}
	defer logger.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	obsProvider, err := observability.New(observability.ConfigFromEnv("storage-proxy", logger))
	if err != nil {
		logger.Fatal("Failed to initialize storage-proxy observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(context.Background())

	runtime, err := storageproxyruntime.New(ctx, storageproxyruntime.Options{
		Config:        cfg,
		Logger:        logger,
		LogrusLogger:  logrusLogger,
		Observability: obsProvider,
	})
	if err != nil {
		logger.Fatal("Failed to initialize storage-proxy runtime", zap.Error(err))
	}
	if err := runtime.Start(ctx); err != nil {
		logger.Fatal("Failed to start storage-proxy runtime", zap.Error(err))
	}

	logger.Info("Storage-proxy is running", zap.String("address", runtime.Address()))
	select {
	case <-ctx.Done():
	case err := <-runtime.Errors():
		logger.Error("Storage-proxy runtime failed", zap.Error(err))
		cancel()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		logger.Error("Storage-proxy shutdown reported errors", zap.Error(err))
	}
	logger.Info("Storage-proxy stopped")
}
