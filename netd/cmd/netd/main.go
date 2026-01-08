package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/netd/pkg/config"
	"github.com/sandbox0-ai/infra/netd/pkg/dataplane"
	"github.com/sandbox0-ai/infra/netd/pkg/metrics"
	"github.com/sandbox0-ai/infra/netd/pkg/proxy"
	"github.com/sandbox0-ai/infra/netd/pkg/watcher"
	"github.com/sandbox0-ai/infra/pkg/env"
	"github.com/sandbox0-ai/infra/pkg/k8s"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
)

func main() {
	// Load environment variables from .env file
	env.Load()

	// Load configuration
	cfg := config.LoadConfig()

	// Initialize logger
	logger := initLogger(cfg.LogLevel)
	defer logger.Sync()

	logger.Info("Starting netd",
		zap.String("version", "v0.1.0"),
		zap.String("nodeName", cfg.NodeName),
		zap.Int("metricsPort", cfg.MetricsPort),
		zap.Int("healthPort", cfg.HealthPort),
		zap.Bool("failClosed", cfg.FailClosed),
	)

	// Validate required configuration
	if cfg.NodeName == "" {
		logger.Fatal("NODE_NAME environment variable is required")
	}

	// Create Kubernetes client
	k8sConfig, err := k8s.BuildRestConfig(cfg.KubeConfig)
	if err != nil {
		logger.Fatal("Failed to build Kubernetes config", zap.Error(err))
	}

	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		logger.Fatal("Failed to create Kubernetes client", zap.Error(err))
	}

	// Add CRD types to scheme
	if err := v1alpha1.AddToScheme(scheme.Scheme); err != nil {
		logger.Fatal("Failed to add CRD types to scheme", zap.Error(err))
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create watcher with REST config for CRD access
	w, err := watcher.NewWatcherWithConfig(
		k8sConfig,
		k8sClient,
		cfg.NodeName,
		cfg.Namespace,
		cfg.ResyncPeriod,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to create watcher", zap.Error(err))
	}

	// Create dataplane with eBPF support
	dpConfig := &dataplane.Config{
		ProxyHTTPPort:       cfg.ProxyHTTPPort,
		ProxyHTTPSPort:      cfg.ProxyHTTPSPort,
		ProcdPort:           cfg.ProcdPort,
		FailClosed:          cfg.FailClosed,
		StorageProxyCIDR:    cfg.StorageProxyCIDR,
		ClusterDNSCIDR:      cfg.ClusterDNSCIDR,
		InternalGatewayCIDR: cfg.InternalGatewayCIDR,
		UseEBPF:             cfg.UseEBPF,
		BPFFSPath:           cfg.BPFFSPath,
		UseEDT:              cfg.UseEDT,
	}

	dp, err := dataplane.NewDataPlaneWithEBPF(logger, dpConfig)
	if err != nil {
		logger.Fatal("Failed to create dataplane", zap.Error(err))
	}

	logger.Info("Dataplane configured",
		zap.Bool("useEBPF", dp.UseEBPF()),
	)

	// Create proxy
	p := proxy.NewProxy(
		logger,
		w,
		cfg.ProxyListenAddr,
		cfg.ProxyHTTPPort,
		cfg.ProxyHTTPSPort,
		cfg.DNSResolvers,
	)

	// Set up event handlers
	w.SetPodEventHandlers(
		// On pod add
		func(info *watcher.SandboxInfo) {
			logger.Info("Pod added, applying rules",
				zap.String("sandboxID", info.SandboxID),
				zap.String("podIP", info.PodIP),
			)

			networkPolicy := w.GetNetworkPolicy(info.SandboxID)
			bandwidthPolicy := w.GetBandwidthPolicy(info.SandboxID)

			if err := dp.ApplyPodRules(ctx, info, networkPolicy, bandwidthPolicy); err != nil {
				logger.Error("Failed to apply pod rules",
					zap.String("sandboxID", info.SandboxID),
					zap.Error(err),
				)
				metrics.RecordRuleError(info.SandboxID, "apply")
			}

			metrics.SetActiveSandboxes(len(w.ListActiveSandboxes()))
		},
		// On pod update
		func(oldInfo, newInfo *watcher.SandboxInfo) {
			// If pod became active, apply rules
			if newInfo.IsActive && (oldInfo == nil || !oldInfo.IsActive) {
				logger.Info("Pod became active, applying rules",
					zap.String("sandboxID", newInfo.SandboxID),
				)

				networkPolicy := w.GetNetworkPolicy(newInfo.SandboxID)
				bandwidthPolicy := w.GetBandwidthPolicy(newInfo.SandboxID)

				if err := dp.ApplyPodRules(ctx, newInfo, networkPolicy, bandwidthPolicy); err != nil {
					logger.Error("Failed to apply pod rules",
						zap.String("sandboxID", newInfo.SandboxID),
						zap.Error(err),
					)
					metrics.RecordRuleError(newInfo.SandboxID, "apply")
				}
			}

			// If pod became inactive, remove rules
			if !newInfo.IsActive && oldInfo != nil && oldInfo.IsActive {
				logger.Info("Pod became inactive, removing rules",
					zap.String("sandboxID", newInfo.SandboxID),
				)

				if err := dp.RemovePodRules(ctx, newInfo.SandboxID); err != nil {
					logger.Error("Failed to remove pod rules",
						zap.String("sandboxID", newInfo.SandboxID),
						zap.Error(err),
					)
					metrics.RecordRuleError(newInfo.SandboxID, "remove")
				}
			}

			metrics.SetActiveSandboxes(len(w.ListActiveSandboxes()))
		},
		// On pod delete
		func(info *watcher.SandboxInfo) {
			logger.Info("Pod deleted, removing rules",
				zap.String("sandboxID", info.SandboxID),
			)

			if err := dp.RemovePodRules(ctx, info.SandboxID); err != nil {
				logger.Error("Failed to remove pod rules",
					zap.String("sandboxID", info.SandboxID),
					zap.Error(err),
				)
				metrics.RecordRuleError(info.SandboxID, "remove")
			}

			metrics.SetActiveSandboxes(len(w.ListActiveSandboxes()))
		},
	)

	// Set up policy change handlers
	w.SetNetworkPolicyHandler(func(sandboxID string, policy *v1alpha1.SandboxNetworkPolicy) {
		logger.Info("Network policy changed",
			zap.String("sandboxID", sandboxID),
			zap.Bool("hasPolicy", policy != nil),
		)

		// Find the sandbox info and reapply rules
		for _, info := range w.ListActiveSandboxes() {
			if info.SandboxID == sandboxID {
				bandwidthPolicy := w.GetBandwidthPolicy(sandboxID)

				// Remove old rules first
				dp.RemovePodRules(ctx, sandboxID)

				// Apply new rules
				if err := dp.ApplyPodRules(ctx, info, policy, bandwidthPolicy); err != nil {
					logger.Error("Failed to reapply pod rules after policy change",
						zap.String("sandboxID", sandboxID),
						zap.Error(err),
					)
					metrics.RecordRuleError(sandboxID, "policy_update")
				}
				break
			}
		}
	})

	w.SetBandwidthPolicyHandler(func(sandboxID string, policy *v1alpha1.SandboxBandwidthPolicy) {
		logger.Info("Bandwidth policy changed",
			zap.String("sandboxID", sandboxID),
			zap.Bool("hasPolicy", policy != nil),
		)

		// Find the sandbox info and reapply rules
		for _, info := range w.ListActiveSandboxes() {
			if info.SandboxID == sandboxID {
				networkPolicy := w.GetNetworkPolicy(sandboxID)

				// Remove old rules first
				dp.RemovePodRules(ctx, sandboxID)

				// Apply new rules
				if err := dp.ApplyPodRules(ctx, info, networkPolicy, policy); err != nil {
					logger.Error("Failed to reapply pod rules after policy change",
						zap.String("sandboxID", sandboxID),
						zap.Error(err),
					)
					metrics.RecordRuleError(sandboxID, "policy_update")
				}
				break
			}
		}
	})

	// Initialize dataplane
	if err := dp.Initialize(ctx); err != nil {
		logger.Fatal("Failed to initialize dataplane", zap.Error(err))
	}

	// Start watcher
	if err := w.Start(ctx); err != nil {
		logger.Fatal("Failed to start watcher", zap.Error(err))
	}

	// Start proxy
	if err := p.Start(ctx); err != nil {
		logger.Fatal("Failed to start proxy", zap.Error(err))
	}

	// Start health server
	go startHealthServer(cfg.HealthPort, logger)

	// Start metrics server
	go startMetricsServer(cfg.MetricsPort, logger)

	// Start metrics reporter
	go startMetricsReporter(ctx, w, cfg.MetricsReportInterval, logger)

	logger.Info("netd is running")

	// Wait for termination signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down gracefully")

	// Stop proxy
	p.Stop()

	// Cleanup dataplane
	if err := dp.Cleanup(ctx); err != nil {
		logger.Error("Failed to cleanup dataplane", zap.Error(err))
	}

	// Cancel context
	cancel()

	// Give components time to shut down
	time.Sleep(2 * time.Second)

	logger.Info("netd stopped")
}

// initLogger initializes the logger
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

	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(level)
	cfg.EncoderConfig.TimeKey = "timestamp"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := cfg.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}

	return logger
}

// startHealthServer starts the health check server
func startHealthServer(port int, logger *zap.Logger) {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		// TODO: Add actual readiness checks
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf(":%d", port)
	logger.Info("Starting health server", zap.String("addr", addr))

	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("Health server failed", zap.Error(err))
	}
}

// startMetricsServer starts the Prometheus metrics server
func startMetricsServer(port int, logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	addr := fmt.Sprintf(":%d", port)
	logger.Info("Starting metrics server", zap.String("addr", addr))

	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("Metrics server failed", zap.Error(err))
	}
}

// startMetricsReporter periodically reports metrics
func startMetricsReporter(ctx context.Context, w *watcher.Watcher, interval time.Duration, logger *zap.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Update cache size metrics
			metrics.SetWatcherCacheSize("pods", len(w.ListActiveSandboxes()))

			// Log summary
			sandboxes := w.ListActiveSandboxes()
			logger.Debug("Metrics report",
				zap.Int("activeSandboxes", len(sandboxes)),
			)
		}
	}
}
