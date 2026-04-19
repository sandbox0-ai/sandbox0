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

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
	procdhttp "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/http"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/volume"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// // Start the reaper to clean up zombie processes
	// go reaper.Reap()

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

	// Initialize observability provider
	obsProvider, err := observability.New(observability.Config{
		ServiceName: "procd",
		Logger:      logger,
		TraceExporter: observability.TraceExporterConfig{
			Type:     os.Getenv("OTEL_EXPORTER_TYPE"),
			Endpoint: os.Getenv("OTEL_EXPORTER_ENDPOINT"),
		},
	})
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(context.Background())

	// Initialize managers
	configureProcessOutputForwarding(logger)

	contextManager := ctxpkg.NewManager()
	contextManager.SetDefaultCleanupPolicy(ctxpkg.CleanupPolicy{
		IdleTimeout: cfg.ContextIdleTimeout.Duration,
		MaxLifetime: cfg.ContextMaxLifetime.Duration,
		FinishedTTL: cfg.ContextFinishedTTL.Duration,
	})

	webhookDispatcher := webhook.NewDispatcher(webhook.Options{
		QueueSize:      cfg.WebhookQueueSize,
		MaxRetries:     cfg.WebhookMaxRetries,
		BaseBackoff:    cfg.WebhookBaseBackoff.Duration,
		RequestTimeout: cfg.WebhookRequestTimeout.Duration,
		OutboxDir:      cfg.WebhookOutboxDir,
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
		if _, err := webhookDispatcher.Enqueue(webhook.Event{
			EventType: eventType,
			Payload:   payload,
		}); err != nil {
			logger.Warn("Failed to enqueue process exit webhook", zap.Error(err))
		}
	})

	contextManager.SetStartHandler(func(event process.StartEvent) {
		payload := map[string]any{
			"process_id":   event.ProcessID,
			"process_type": event.ProcessType,
			"pid":          event.PID,
			"command":      event.Config.Command,
			"env_vars":     event.Config.EnvVars,
			"cwd":          event.Config.CWD,
			"alias":        event.Config.Alias,
		}
		if event.Config.PTYSize != nil {
			payload["pty_size"] = event.Config.PTYSize
		}
		if event.Config.Term != "" {
			payload["term"] = event.Config.Term
		}
		if _, err := webhookDispatcher.Enqueue(webhook.Event{
			EventType: webhook.EventTypeProcessStarted,
			Payload:   payload,
		}); err != nil {
			logger.Warn("Failed to enqueue process start webhook", zap.Error(err))
		}
	})

	// Create shared token provider for storage-proxy communication
	tokenProvider := procdhttp.NewTokenProvider()

	volumeCfg := &volume.Config{
		ProxyBaseURL:     cfg.StorageProxyBaseURL,
		ProxyPort:        cfg.StorageProxyPort,
		CacheMaxBytes:    cfg.CacheMaxBytes,
		CacheTTL:         cfg.CacheTTL.Duration,
		VolumeCacheSize:  cfg.VolumeCacheSize,
		VolumePrefetch:   cfg.VolumePrefetch,
		VolumeBufferSize: cfg.VolumeBufferSize,
		VolumeWriteback:  cfg.VolumeWriteback,
		GRPCMaxMsgSize:   100 * 1024 * 1024, // could be made configurable if added to ProcdConfig
	}
	volumeManager := volume.NewManager(volumeCfg, tokenProvider, logger)

	fileManager, err := file.NewManager(cfg.RootPath)
	if err != nil {
		logger.Fatal("Failed to create file manager", zap.Error(err))
	}
	volumeManager.SetEventSink(fileManager)

	// Initialize internal auth validator
	publicKey, err := internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultInternalJWTPublicKeyPath)
	if err != nil {
		logger.Fatal("Failed to load internal auth public key",
			zap.String("path", internalauth.DefaultInternalJWTPublicKeyPath),
			zap.Error(err),
		)
	}

	validatorConfig := internalauth.DefaultValidatorConfig(internalauth.ServiceProcd, publicKey)
	validatorConfig.AllowedCallers = internalauth.ProcdAllowedCallers()
	authValidator := internalauth.NewValidator(validatorConfig)

	logger.Info("Internal authentication enabled",
		zap.String("target", internalauth.ServiceProcd),
		zap.Strings("allowed_callers", validatorConfig.AllowedCallers),
	)

	// Note: Network isolation is handled by netd via pod annotations.
	// Procd no longer manages network policies.

	warmProcesses, err := startWarmProcesses(contextManager, logger)
	if err != nil {
		logger.Fatal("Failed to start warm processes", zap.Error(err))
	}
	probeRunner := &warmProcessProber{
		manager:              contextManager,
		processes:            warmProcesses,
		logger:               logger,
		exitOnFailedLiveness: len(warmProcesses) > 0,
	}

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
		obsProvider,
		probeRunner.Probe,
	)

	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	contextManager.StartCleanup(cleanupCtx, cfg.ContextCleanupInterval.Duration)

	// Handle shutdown signals
	done := make(chan bool, 1)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-quit
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))

		cleanupCancel()

		if _, err := webhookDispatcher.Enqueue(webhook.Event{
			EventType: webhook.EventTypeSandboxKilled,
			Payload: map[string]any{
				"signal": sig.String(),
				"reason": "shutdown",
			},
		}); err != nil {
			logger.Warn("Failed to enqueue sandbox killed webhook", zap.Error(err))
		}

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

		if err := webhookDispatcher.Shutdown(context.Background()); err != nil {
			logger.Warn("Webhook dispatcher shutdown error", zap.Error(err))
		}
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
