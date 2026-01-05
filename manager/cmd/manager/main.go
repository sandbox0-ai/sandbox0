package main

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0ai/infra/manager/pkg/config"
	"github.com/sandbox0ai/infra/manager/pkg/controller"
	httpserver "github.com/sandbox0ai/infra/manager/pkg/http"
	"github.com/sandbox0ai/infra/manager/pkg/service"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Initialize logger
	logger := initLogger(cfg.LogLevel)
	defer logger.Sync()

	logger.Info("Starting Manager",
		zap.String("version", "v0.1.0"),
		zap.Int("httpPort", cfg.HTTPPort),
		zap.Int("metricsPort", cfg.MetricsPort),
	)

	// Create Kubernetes client
	k8sConfig, err := buildKubeConfig(cfg.KubeConfig)
	if err != nil {
		logger.Fatal("Failed to build Kubernetes config", zap.Error(err))
	}

	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		logger.Fatal("Failed to create Kubernetes client", zap.Error(err))
	}

	// Add SandboxTemplate to scheme
	if err := v1alpha1.AddToScheme(scheme.Scheme); err != nil {
		logger.Fatal("Failed to add SandboxTemplate to scheme", zap.Error(err))
	}

	// Create informers
	informerFactory := informers.NewSharedInformerFactory(k8sClient, cfg.ResyncPeriod)
	podInformer := informerFactory.Core().V1().Pods().Informer()

	// Create SandboxTemplate informer manually
	templateInformer := createTemplateInformer(k8sConfig, cfg.Namespace, cfg.ResyncPeriod)

	// Create event recorder
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: k8sClient.CoreV1().Events(""),
	})
	eventSource := corev1.EventSource{Component: "manager"}
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, eventSource)

	// Create operator
	operator := controller.NewOperator(
		k8sClient,
		podInformer,
		templateInformer,
		recorder,
		logger,
	)

	// Create pod lister
	podLister := informerFactory.Core().V1().Pods().Lister()

	// Create services
	sandboxService := service.NewSandboxService(
		k8sClient,
		podLister,
		operator.GetTemplateLister(),
		logger,
	)

	// Create cleanup controller
	cleanupController := controller.NewCleanupController(
		k8sClient,
		podLister,
		operator.GetTemplateLister(),
		recorder,
		logger,
		cfg.CleanupInterval,
	)

	// Create HTTP server
	httpServer := httpserver.NewServer(
		sandboxService,
		logger,
		cfg.HTTPPort,
	)

	// Start metrics server
	go startMetricsServer(cfg.MetricsPort, logger)

	// Create context that cancels on signal
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Start informers
	logger.Info("Starting informers")
	informerFactory.Start(ctx.Done())
	go templateInformer.Run(ctx.Done())

	// Start operator
	go func() {
		if err := operator.Run(ctx, 2); err != nil {
			logger.Fatal("Operator failed", zap.Error(err))
		}
	}()

	// Start cleanup controller
	go func() {
		if err := cleanupController.Start(ctx); err != nil && err != context.Canceled {
			logger.Error("Cleanup controller failed", zap.Error(err))
		}
	}()

	// Start HTTP server
	go func() {
		if err := httpServer.Start(ctx); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	logger.Info("Manager is running")

	// Wait for termination signal
	<-ctx.Done()
	logger.Info("Shutting down gracefully")

	// Give components time to shut down
	time.Sleep(2 * time.Second)

	logger.Info("Manager stopped")
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

// buildKubeConfig builds Kubernetes config
func buildKubeConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

// createTemplateInformer creates an informer for SandboxTemplate
func createTemplateInformer(cfg *rest.Config, namespace string, resyncPeriod time.Duration) cache.SharedIndexInformer {
	// Create a simple informer that watches for SandboxTemplate
	// In a real implementation, we should use generated clientset
	// For now, we'll create a mock informer

	listWatch := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			// Mock list function
			return &v1alpha1.SandboxTemplateList{
				Items: []v1alpha1.SandboxTemplate{},
			}, nil
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			// Mock watch function
			return watch.NewFake(), nil
		},
	}

	informer := cache.NewSharedIndexInformer(
		listWatch,
		&v1alpha1.SandboxTemplate{},
		resyncPeriod,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)

	return informer
}

// startMetricsServer starts the Prometheus metrics server
func startMetricsServer(port int, logger *zap.Logger) {
	http.Handle("/metrics", promhttp.Handler())

	addr := fmt.Sprintf(":%d", port)
	logger.Info("Starting metrics server", zap.String("addr", addr))

	if err := http.ListenAndServe(addr, nil); err != nil {
		logger.Error("Metrics server failed", zap.Error(err))
	}
}
