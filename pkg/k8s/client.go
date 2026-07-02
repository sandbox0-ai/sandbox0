package k8s

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// DefaultClientQPS is the Sandbox0 Kubernetes client token bucket rate.
	DefaultClientQPS float32 = 50
	// DefaultClientBurst is the Sandbox0 Kubernetes client burst.
	DefaultClientBurst = 100
)

// BuildRestConfig creates a Kubernetes rest config using in-cluster config or kubeconfig
func BuildRestConfig(kubeconfigPath string) (*rest.Config, error) {
	// If kubeconfigPath is empty, try in-cluster config first
	if kubeconfigPath == "" {
		config, err := rest.InClusterConfig()
		if err == nil {
			ApplyDefaultRateLimit(config)
			return config, nil
		}
	}

	// If kubeconfigPath is still empty, try default kubeconfig locations
	if kubeconfigPath == "" {
		home, err := os.UserHomeDir()
		if err == nil {
			kubeconfigPath = filepath.Join(home, ".kube", "config")
		}
	}

	// If we have a path (either provided or default), use it
	if kubeconfigPath != "" {
		if _, err := os.Stat(kubeconfigPath); err == nil {
			config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
			if err != nil {
				return nil, fmt.Errorf("build kubeconfig from %s: %w", kubeconfigPath, err)
			}
			ApplyDefaultRateLimit(config)
			return config, nil
		}
	}

	return nil, fmt.Errorf("no Kubernetes config found")
}

// ApplyDefaultRateLimit sets Sandbox0 Kubernetes client defaults when QPS or burst are unset.
func ApplyDefaultRateLimit(config *rest.Config) {
	if config == nil {
		return
	}
	if config.QPS <= 0 {
		config.QPS = DefaultClientQPS
	}
	if config.Burst <= 0 {
		config.Burst = DefaultClientBurst
	}
}

// NewClient creates a new Kubernetes clientset using in-cluster config or kubeconfig
func NewClient(kubeconfigPath string) (kubernetes.Interface, error) {
	config, err := BuildRestConfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}

// NewClientWithObservability creates a new Kubernetes clientset with observability instrumentation
func NewClientWithObservability(kubeconfigPath string, obsProvider *observability.Provider) (kubernetes.Interface, error) {
	config, err := BuildRestConfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}

	if obsProvider != nil {
		obsProvider.K8s.WrapConfig(config)
	}

	return kubernetes.NewForConfig(config)
}
