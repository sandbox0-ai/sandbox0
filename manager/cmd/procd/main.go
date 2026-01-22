// Package main is the entry point for the Procd service.
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

	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	ctxpkg "github.com/sandbox0-ai/infra/manager/procd/pkg/context"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/file"
	procdhttp "github.com/sandbox0-ai/infra/manager/procd/pkg/http"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/volume"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Load configuration
	cfg := config.LoadProcdConfig()

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	logger := initLogger(cfg.LogLevel)
	defer logger.Sync()

	logger.Info("Starting Procd",
		zap.String("version", "1.0.0"),
	)

	logger.Info("Configuration loaded",
		zap.Int("http_port", cfg.HTTPPort),
		zap.String("root_path", cfg.RootPath),
	)

	// Initialize managers
	contextManager := ctxpkg.NewManager()

	webhookDispatcher := webhook.NewDispatcher(webhook.Options{
		QueueSize:      cfg.WebhookQueueSize,
		MaxRetries:     cfg.WebhookMaxRetries,
		BaseBackoff:    cfg.WebhookBaseBackoff.Duration,
		RequestTimeout: cfg.WebhookRequestTimeout.Duration,
	}, logger)

	contextManager.SetExitHandler(func(event process.ExitEvent) {
		eventType := webhook.EventTypeProcessExited
		if event.State == process.ProcessStateCrashed || event.State == process.ProcessStateKilled {
			eventType = webhook.EventTypeProcessCrashed
		}
		payload := map[string]any{
			"process_id":     event.ProcessID,
			"process_type":   event.ProcessType,
			"pid":            event.PID,
			"exit_code":      event.ExitCode,
			"duration_ms":    event.Duration.Milliseconds(),
			"stdout_preview": event.StdoutPreview,
			"stderr_preview": event.StderrPreview,
			"state":          event.State,
		}
		webhookDispatcher.Enqueue(webhook.Event{
			EventType: eventType,
			Payload:   payload,
		})
	})

	contextManager.SetStartHandler(func(event process.StartEvent) {
		payload := map[string]any{
			"process_id":   event.ProcessID,
			"process_type": event.ProcessType,
			"pid":          event.PID,
			"command":      event.Config.Command,
			"env_vars":     event.Config.EnvVars,
			"cwd":          event.Config.CWD,
			"language":     event.Config.Language,
		}
		if event.Config.PTYSize != nil {
			payload["pty_size"] = event.Config.PTYSize
		}
		if event.Config.Term != "" {
			payload["term"] = event.Config.Term
		}
		webhookDispatcher.Enqueue(webhook.Event{
			EventType: webhook.EventTypeProcessStarted,
			Payload:   payload,
		})
	})

	// Create shared token provider for storage-proxy communication
	tokenProvider := procdhttp.NewTokenProvider()

	volumeCfg := &volume.Config{
		ProxyBaseURL:      cfg.StorageProxyBaseURL,
		ProxyPort:         cfg.StorageProxyPort,
		ProxyReplicas:     cfg.StorageProxyReplicas,
		NodeName:          cfg.NodeName,
		CacheMaxBytes:     cfg.CacheMaxBytes,
		CacheTTL:          cfg.CacheTTL.Duration,
		JuiceFSCacheSize:  cfg.JuiceFSCacheSize,
		JuiceFSPrefetch:   cfg.JuiceFSPrefetch,
		JuiceFSBufferSize: cfg.JuiceFSBufferSize,
		JuiceFSWriteback:  cfg.JuiceFSWriteback,
		GRPCMaxMsgSize:    100 * 1024 * 1024, // could be made configurable if added to ProcdConfig
		SOMark:            0x2,               // could be made configurable if added to ProcdConfig
	}
	volumeManager := volume.NewManager(volumeCfg, tokenProvider, logger)

	fileManager, err := file.NewManager(cfg.RootPath)
	if err != nil {
		logger.Fatal("Failed to create file manager", zap.Error(err))
	}

	// Initialize internal auth validator
	publicKey, err := internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultInternalJWTPublicKeyPath)
	if err != nil {
		logger.Fatal("Failed to load internal auth public key",
			zap.String("path", internalauth.DefaultInternalJWTPublicKeyPath),
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
		webhookDispatcher,
		logger,
	)

	// Handle shutdown signals
	done := make(chan bool, 1)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-quit
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))

		webhookDispatcher.Enqueue(webhook.Event{
			EventType: webhook.EventTypeSandboxKilled,
			Payload: map[string]any{
				"signal": sig.String(),
				"reason": "shutdown",
			},
		})

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

func initLogger(logLevel string) *zap.Logger {
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
