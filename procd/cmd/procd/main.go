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

	"github.com/sandbox0-ai/infra/procd/pkg/config"
	ctxpkg "github.com/sandbox0-ai/infra/procd/pkg/context"
	"github.com/sandbox0-ai/infra/procd/pkg/file"
	procdhttp "github.com/sandbox0-ai/infra/procd/pkg/http"
	"github.com/sandbox0-ai/infra/procd/pkg/network"
	"github.com/sandbox0-ai/infra/procd/pkg/volume"
	"github.com/sandbox0-ai/infra/pkg/env"
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

	networkCfg := &network.Config{
		SandboxID:        cfg.SandboxID,
		TCPProxyPort:     cfg.Network.TCPProxyPort,
		EnableTCPProxy:   cfg.Network.EnableTCPProxy,
		DNSServers:       cfg.Network.DNSServers,
		DefaultDenyCIDRs: cfg.Network.DefaultDenyCIDRs,
	}
	networkManager, err := network.NewManager(networkCfg, logger)
	if err != nil {
		logger.Fatal("Failed to create network manager", zap.Error(err))
	}

	volumeCfg := &volume.Config{
		ProxyBaseURL:  cfg.StorageProxyBaseURL,
		ProxyReplicas: cfg.StorageProxyReplicas,
		NodeName:      cfg.NodeName,
		CacheMaxBytes: cfg.CacheMaxBytes,
		CacheTTL:      cfg.CacheTTL,
	}
	volumeManager := volume.NewManager(volumeCfg, logger)

	fileManager, err := file.NewManager(cfg.RootPath)
	if err != nil {
		logger.Fatal("Failed to create file manager", zap.Error(err))
	}

	// Setup network
	if err := networkManager.Setup(); err != nil {
		logger.Fatal("Failed to setup network", zap.Error(err))
	}

	// Start TCP proxy if enabled
	if cfg.Network.EnableTCPProxy {
		if err := networkManager.StartTCPProxy(); err != nil {
			logger.Fatal("Failed to start TCP proxy", zap.Error(err))
		}
	}

	// Create and start HTTP server
	server := procdhttp.NewServer(
		cfg,
		contextManager,
		networkManager,
		volumeManager,
		fileManager,
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
		networkManager.Shutdown()
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

