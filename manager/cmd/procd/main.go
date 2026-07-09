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
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/trust"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"github.com/sandbox0-ai/sandbox0/pkg/procdstate"
	"go.uber.org/zap"
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
	logger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "procd",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting Procd",
		zap.String("version", "1.0.0"),
	)

	logger.Info("Configuration loaded",
		zap.Int("http_port", cfg.HTTPPort),
		zap.String("root_path", cfg.RootPath),
	)
	if bundlePath, err := trust.ConfigureNetdMITMCATrust(); err != nil {
		logger.Warn("Failed to configure netd MITM CA trust", zap.Error(err))
	} else if bundlePath != "" {
		logger.Info("Configured netd MITM CA trust", zap.String("bundle_path", bundlePath))
	}

	// Initialize observability provider
	obsProvider, err := observability.New(observability.ConfigFromEnv("procd", logger))
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(context.Background())

	// Initialize managers
	configureProcessOutputForwarding(logger)

	contextManager := ctxpkg.NewManager()
	contextStateStore, err := ctxpkg.NewFileStateStore(procdstate.ContextStateDir)
	if err != nil {
		logger.Fatal("Failed to initialize context state store", zap.Error(err))
	}
	contextManager.SetStateStore(contextStateStore)
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

	fileManager, err := file.NewManager(cfg.RootPath)
	if err != nil {
		logger.Fatal("Failed to create file manager", zap.Error(err))
	}

	if err := initializeContextRecovery(contextManager, contextStateStore, logger); err != nil {
		contextManager.CleanupPreservingState()
		logger.Fatal("Failed to recover contexts", zap.Error(err))
	}

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

	// Create and start HTTP server
	server := procdhttp.NewServer(
		cfg,
		contextManager,
		fileManager,
		authValidator,
		webhookDispatcher,
		logger,
		obsProvider,
		nil,
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
		contextManager.CleanupPreservingState()
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

func initializeContextRecovery(manager *ctxpkg.Manager, store *ctxpkg.FileStateStore, logger *zap.Logger) error {
	requested, err := store.RecoveryRequested()
	if err != nil {
		return err
	}
	if !requested {
		return store.Clear()
	}
	restored, err := manager.RestoreContexts()
	if err != nil {
		return err
	}
	consumed, err := store.ConsumeRecoveryRequest()
	if err != nil {
		return err
	}
	if !consumed {
		return errors.New("context recovery request disappeared before completion")
	}
	if logger != nil {
		logger.Info("Recovered procd contexts", zap.Int("contexts", len(restored)))
	}
	return nil
}
