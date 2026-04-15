package k8s

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/pkg/observability/internal/promutil"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Adapter provides observable Kubernetes clients
type Adapter struct {
	config  AdapterConfig
	metrics *metrics
}

// AdapterConfig configures the K8s adapter
type AdapterConfig struct {
	ServiceName    string
	Tracer         trace.Tracer
	Logger         *zap.Logger
	Registry       prometheus.Registerer
	DisableMetrics bool
	DisableLogging bool
	Disabled       bool
}

// Config holds configuration for creating an observable K8s client
type Config struct {
	// Kubeconfig path (empty for in-cluster config)
	Kubeconfig string

	// Optional: custom rest.Config
	RestConfig *rest.Config

	// Optional: QPS and Burst for rate limiting
	QPS   float32
	Burst int
}

// NewAdapter creates a new K8s adapter
func NewAdapter(cfg AdapterConfig) Adapter {
	var m *metrics
	if !cfg.Disabled && !cfg.DisableMetrics && cfg.Registry != nil {
		m = newMetrics(cfg.ServiceName, cfg.Registry)
	}

	return Adapter{
		config:  cfg,
		metrics: m,
	}
}

// NewClient creates a fully observable Kubernetes clientset
func (a Adapter) NewClient(cfg Config) (kubernetes.Interface, error) {
	restConfig, err := a.buildRestConfig(cfg)
	if err != nil {
		return nil, err
	}

	// Wrap the transport with observability
	restConfig.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &observableTransport{
			base:    rt,
			config:  a.config,
			metrics: a.metrics,
		}
	})

	return kubernetes.NewForConfig(restConfig)
}

// NewRestConfig creates an observable rest.Config
func (a Adapter) NewRestConfig(cfg Config) (*rest.Config, error) {
	restConfig, err := a.buildRestConfig(cfg)
	if err != nil {
		return nil, err
	}

	// Wrap the transport with observability
	restConfig.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &observableTransport{
			base:    rt,
			config:  a.config,
			metrics: a.metrics,
		}
	})

	return restConfig, nil
}

// WrapConfig wraps an existing rest.Config with observability instrumentation
func (a Adapter) WrapConfig(restConfig *rest.Config) {
	if restConfig == nil || a.config.Disabled {
		return
	}

	// Wrap the transport with observability
	restConfig.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &observableTransport{
			base:    rt,
			config:  a.config,
			metrics: a.metrics,
		}
	})

	a.config.Logger.Debug("Kubernetes rest.Config wrapped with observability")
}

// buildRestConfig builds a rest.Config from the given config
func (a Adapter) buildRestConfig(cfg Config) (*rest.Config, error) {
	var restConfig *rest.Config
	var err error

	if cfg.RestConfig != nil {
		restConfig = cfg.RestConfig
	} else {
		// Use the existing k8s package to build config
		restConfig, err = buildRestConfig(cfg.Kubeconfig)
		if err != nil {
			return nil, err
		}
	}

	// Set rate limiting if specified
	if cfg.QPS > 0 {
		restConfig.QPS = cfg.QPS
	}
	if cfg.Burst > 0 {
		restConfig.Burst = cfg.Burst
	}

	return restConfig, nil
}

// metrics holds Prometheus metrics for K8s client
type metrics struct {
	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	activeRequests  *prometheus.GaugeVec
}

func newMetrics(serviceName string, registry prometheus.Registerer) *metrics {
	prefix := promutil.MetricPrefix(serviceName)

	return &metrics{
		requestsTotal: promutil.RegisterCounterVec(
			registry,
			prometheus.CounterOpts{
				Name: prefix + "_k8s_client_requests_total",
				Help: "Total number of Kubernetes API requests",
			},
			[]string{"verb", "resource", "status"},
		),
		requestDuration: promutil.RegisterHistogramVec(
			registry,
			prometheus.HistogramOpts{
				Name:    prefix + "_k8s_client_request_duration_seconds",
				Help:    "Kubernetes API request duration in seconds",
				Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
			},
			[]string{"verb", "resource"},
		),
		activeRequests: promutil.RegisterGaugeVec(
			registry,
			prometheus.GaugeOpts{
				Name: prefix + "_k8s_client_active_requests",
				Help: "Number of active Kubernetes API requests",
			},
			[]string{"verb", "resource"},
		),
	}
}

// buildRestConfig is copied from infra/pkg/k8s/client.go to avoid circular dependency
func buildRestConfig(kubeconfigPath string) (*rest.Config, error) {
	// If kubeconfigPath is empty, try in-cluster config first
	if kubeconfigPath == "" {
		config, err := rest.InClusterConfig()
		if err == nil {
			return config, nil
		}
	}

	if kubeconfigPath == "" {
		home, err := os.UserHomeDir()
		if err == nil {
			kubeconfigPath = filepath.Join(home, ".kube", "config")
		}
	}

	if kubeconfigPath != "" {
		if _, err := os.Stat(kubeconfigPath); err == nil {
			config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
			if err != nil {
				return nil, fmt.Errorf("build kubeconfig from %s: %w", kubeconfigPath, err)
			}
			return config, nil
		}
	}

	return nil, fmt.Errorf("no Kubernetes config found")
}
