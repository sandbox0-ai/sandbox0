package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/daemon"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"go.uber.org/zap"
)

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

	daemon := daemon.New(cfg, logger, obsProvider)
	if err := daemon.Run(ctx); err != nil {
		logger.Fatal("netd exited with error", zap.Error(err))
	}
}
