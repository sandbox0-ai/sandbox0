package registry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Credential contains registry login credentials.
type Credential struct {
	Provider  string    `json:"provider"`
	Registry  string    `json:"registry"`
	Username  string    `json:"username"`
	Password  string    `json:"password"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// Provider returns short-lived registry credentials.
type Provider interface {
	GetPushCredentials(ctx context.Context, teamID string) (*Credential, error)
}

// NewProvider creates a registry provider based on config.
func NewProvider(cfg config.RegistryConfig, k8sClient kubernetes.Interface, logger *zap.Logger) (Provider, error) {
	provider := strings.TrimSpace(strings.ToLower(cfg.Provider))
	if provider == "" {
		return nil, nil
	}
	if logger != nil {
		logger.Info("Registry provider configured", zap.String("provider", provider))
	}
	namespace := resolveNamespace(cfg.Namespace)
	secretReader := secretReader{
		client:    k8sClient,
		namespace: namespace,
	}

	switch provider {
	case "aws":
		if cfg.AWS == nil {
			return nil, fmt.Errorf("registry aws config is required")
		}
		return &awsProvider{cfg: *cfg.AWS, secrets: secretReader}, nil
	case "gcp":
		if cfg.GCP == nil {
			return nil, fmt.Errorf("registry gcp config is required")
		}
		return &gcpProvider{cfg: *cfg.GCP, secrets: secretReader}, nil
	case "azure":
		if cfg.Azure == nil {
			return nil, fmt.Errorf("registry azure config is required")
		}
		return &azureProvider{cfg: *cfg.Azure, secrets: secretReader}, nil
	case "aliyun":
		if cfg.Aliyun == nil {
			return nil, fmt.Errorf("registry aliyun config is required")
		}
		return &aliyunProvider{cfg: *cfg.Aliyun, secrets: secretReader}, nil
	case "builtin":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported registry provider: %s", provider)
	}
}

type secretReader struct {
	client    kubernetes.Interface
	namespace string
}

func (s secretReader) read(ctx context.Context, name, key string) (string, error) {
	if s.client == nil {
		return "", fmt.Errorf("k8s client is required for registry credentials")
	}
	if name == "" {
		return "", fmt.Errorf("secret name is required")
	}
	secret, err := s.client.CoreV1().Secrets(s.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if secret.Data == nil {
		return "", fmt.Errorf("secret %q has no data", name)
	}
	value, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("secret %q missing key %q", name, key)
	}
	return string(value), nil
}

func resolveNamespace(explicit string) string {
	if strings.TrimSpace(explicit) != "" {
		return explicit
	}
	data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err == nil {
		if ns := strings.TrimSpace(string(data)); ns != "" {
			return ns
		}
	}
	return "default"
}

func normalizeRegistryHost(raw string) string {
	value := strings.TrimSpace(raw)
	value = strings.TrimPrefix(value, "https://")
	value = strings.TrimPrefix(value, "http://")
	return value
}
