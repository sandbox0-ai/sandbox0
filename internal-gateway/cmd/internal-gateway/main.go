package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sandbox0-ai/infra/internal-gateway/pkg/config"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/http"
	"github.com/sandbox0-ai/infra/pkg/env"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Load environment variables from .env file
	env.Load()

	// Load configuration
	cfg := config.Load()

	// Initialize logger
	logger, err := initLogger(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting internal-gateway",
		zap.Int("port", cfg.HTTPPort),
		zap.String("manager_url", cfg.ManagerURL),
		zap.String("storage_proxy_url", cfg.StorageProxyURL),
	)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create HTTP server
	server, err := http.NewServer(cfg, logger)
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
