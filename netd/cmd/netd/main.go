package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/activeguard"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/daemon"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"go.uber.org/zap"
)

const standbyReacquireYield = time.Second

type activeLock interface {
	Close() error
}

type netdRunner interface {
	Run(context.Context) error
}

type activeLockAcquire func(context.Context, string) (activeLock, error)
type netdRunnerFactory func() netdRunner

func main() {
	cfg := config.LoadNetdConfig()

	logger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "netd",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting netd",
		zap.String("node", cfg.NodeName),
		zap.Int("health_port", cfg.HealthPort),
		zap.Int("metrics_port", cfg.MetricsPort),
		zap.Int("proxy_http_port", cfg.ProxyHTTPPort),
		zap.Int("proxy_https_port", cfg.ProxyHTTPSPort),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	obsProvider, err := observability.New(observability.ConfigFromEnv("netd", logger))
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(ctx)

	runnerFactory := func() netdRunner { return daemon.New(cfg, logger, obsProvider) }
	lockPath := strings.TrimSpace(os.Getenv(activeguard.EnvPath))
	if lockPath != "" {
		initialDelay, err := parseActiveLockInitialDelay(os.Getenv(activeguard.EnvInitialDelay))
		if err != nil {
			logger.Fatal("Invalid node-local netd active lock initial delay", zap.Error(err))
		}
		maxHold, err := parseActiveLockDuration(activeguard.EnvMaxHold, os.Getenv(activeguard.EnvMaxHold))
		if err != nil {
			logger.Fatal("Invalid node-local netd active lock maximum hold", zap.Error(err))
		}
		if maxHold > 0 {
			logger.Info("Configured bounded node-local netd active lock hold", zap.Duration("max_hold", maxHold))
		}
		err = runGuardedNetd(
			ctx,
			lockPath,
			initialDelay,
			maxHold,
			standbyReacquireYield,
			func(ctx context.Context, path string) (activeLock, error) {
				logger.Info("Waiting for node-local netd active lock", zap.String("path", path))
				guard, acquireErr := activeguard.Acquire(ctx, path)
				if acquireErr == nil {
					logger.Info("Acquired node-local netd active lock", zap.String("path", path))
				}
				return guard, acquireErr
			},
			runnerFactory,
			func(runErr error) {
				logger.Info("Yielding node-local netd active lock for embedded ctld retry", zap.Error(runErr))
			},
		)
	} else {
		err = runnerFactory().Run(ctx)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatal("netd exited with error", zap.Error(err))
	}
}

func runGuardedNetd(
	ctx context.Context,
	lockPath string,
	initialDelay, maxHold, reacquireYield time.Duration,
	acquire activeLockAcquire,
	newRunner netdRunnerFactory,
	onYield func(error),
) error {
	if acquire == nil || newRunner == nil {
		return fmt.Errorf("active lock acquire and netd runner factory are required")
	}
	if err := waitForDuration(ctx, initialDelay); err != nil {
		return err
	}
	for {
		guard, err := acquire(ctx, lockPath)
		if err != nil {
			return err
		}

		runCtx := ctx
		cancel := func() {}
		if maxHold > 0 {
			runCtx, cancel = context.WithTimeout(ctx, maxHold)
		}
		runErr := newRunner().Run(runCtx)
		cycleExpired := errors.Is(runCtx.Err(), context.DeadlineExceeded)
		cancel()
		closeErr := guard.Close()
		if closeErr != nil {
			return errors.Join(runErr, fmt.Errorf("release node-local netd active lock: %w", closeErr))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if maxHold <= 0 {
			if runErr == nil {
				return fmt.Errorf("netd stopped unexpectedly")
			}
			return runErr
		}
		if runErr == nil && !cycleExpired {
			runErr = fmt.Errorf("netd stopped unexpectedly")
		}
		if onYield != nil {
			onYield(runErr)
		}
		if err := waitForDuration(ctx, reacquireYield); err != nil {
			return err
		}
	}
}

func waitForDuration(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func parseActiveLockInitialDelay(raw string) (time.Duration, error) {
	return parseActiveLockDuration(activeguard.EnvInitialDelay, raw)
}

func parseActiveLockDuration(envName, raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	delay, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", envName, err)
	}
	if delay < 0 {
		return 0, fmt.Errorf("%s must not be negative", envName)
	}
	return delay, nil
}
