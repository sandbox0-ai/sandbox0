// Package main is the entry point for the Procd service.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sandbox0-ai/infra/pkg/env"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/config"
	ctxpkg "github.com/sandbox0-ai/infra/manager/procd/pkg/context"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/file"
	procdhttp "github.com/sandbox0-ai/infra/manager/procd/pkg/http"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/volume"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Load environment variables from .env file
	env.Load()

	// Initialize logger
	logger := initLogger()
	defer logger.Sync()

	logger.Info("Starting Procd",
		zap.String("version", "1.0.0"),
	)

	// Load configuration
	cfg := config.DefaultConfig()
	if err := cfg.Validate(); err != nil {
		logger.Fatal("Invalid configuration", zap.Error(err))
	}

	logger.Info("Configuration loaded",
		zap.String("sandbox_id", cfg.SandboxID),
		zap.String("template_id", cfg.TemplateID),
		zap.Int("http_port", cfg.HTTPPort),
		zap.String("root_path", cfg.RootPath),
	)

	// Initialize managers
	contextManager := ctxpkg.NewManager(cfg.MaxContexts)

	// Create shared token provider for storage-proxy communication
	tokenProvider := procdhttp.NewTokenProvider()

	volumeCfg := &volume.Config{
		ProxyBaseURL:  cfg.StorageProxyBaseURL,
		ProxyReplicas: cfg.StorageProxyReplicas,
		NodeName:      cfg.NodeName,
		CacheMaxBytes: cfg.CacheMaxBytes,
		CacheTTL:      cfg.CacheTTL,
	}
	volumeManager := volume.NewManager(volumeCfg, tokenProvider, logger)

	fileManager, err := file.NewManager(cfg.RootPath)
	if err != nil {
		logger.Fatal("Failed to create file manager", zap.Error(err))
	}

	// Initialize internal auth validator
	publicKey, err := internalauth.LoadEd25519PublicKeyFromFile(cfg.InternalAuthPublicKeyPath)
	if err != nil {
		logger.Fatal("Failed to load internal auth public key",
			zap.String("path", cfg.InternalAuthPublicKeyPath),
			zap.Error(err),
		)
	}

	validatorConfig := internalauth.DefaultValidatorConfig("procd", publicKey)
	validatorConfig.AllowedCallers = []string{"internal-gateway", "manager"}
	authValidator := internalauth.NewValidator(validatorConfig)

	logger.Info("Internal authentication enabled",
		zap.String("target", "procd"),
		zap.Strings("allowed_callers", validatorConfig.AllowedCallers),
	)

	// Note: Network isolation is now handled by the netd service (DaemonSet).
	// procd no longer manages network policies.

	// Create and start HTTP server
	server := procdhttp.NewServer(
		cfg,
		contextManager,
		volumeManager,
		fileManager,
		authValidator,
		tokenProvider,
		logger,
	)

	// Handle shutdown signals
	done := make(chan bool, 1)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		logger.Info("Received shutdown signal")

		// Graceful shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Shutdown HTTP server
		if err := server.Shutdown(ctx); err != nil {
			logger.Error("HTTP server shutdown error", zap.Error(err))
		}

		// Cleanup managers
		contextManager.Cleanup()
		volumeManager.Cleanup()
		fileManager.Close()

		done <- true
	}()

	// Start HTTP server
	logger.Info("Procd is ready")
	if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal("HTTP server error", zap.Error(err))
	}

	<-done
	logger.Info("Procd shutdown complete")
}

func initLogger() *zap.Logger {
	logLevel := os.Getenv("PROCD_LOG_LEVEL")
	level := zapcore.InfoLevel

	switch logLevel {
	case "debug":
		level = zapcore.DebugLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	}

	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(level),
		Development: false,
		Encoding:    "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "timestamp",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "message",
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
		panic(err)
	}

	return logger
}
