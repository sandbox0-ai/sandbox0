package registry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// Credential contains registry login credentials.
type Credential struct {
	Provider     string     `json:"provider"`
	PushRegistry string     `json:"pushRegistry"`
	PullRegistry string     `json:"pullRegistry,omitempty"`
	Username     string     `json:"username"`
	Password     string     `json:"password"`
	ExpiresAt    *time.Time `json:"expiresAt,omitempty"`
}

// PushCredentialsRequest describes the image push the caller is preparing.
type PushCredentialsRequest struct {
	TeamID      string
	TargetImage string
}

// ErrInvalidTargetImage indicates the requested image cannot be served by the
// current team-scoped registry credential flow.
var ErrInvalidTargetImage = errors.New("invalid target image")

// Provider returns short-lived registry credentials.
type Provider interface {
	GetPushCredentials(ctx context.Context, req PushCredentialsRequest) (*Credential, error)
}

// NewProvider creates a registry provider based on config.
func NewProvider(cfg config.RegistryConfig, secretLister corelisters.SecretLister, logger *zap.Logger) (Provider, error) {
	provider := strings.TrimSpace(strings.ToLower(cfg.Provider))
	if provider == "" {
		return nil, nil
	}
	if logger != nil {
		logger.Info("Registry provider configured", zap.String("provider", provider))
	}
	namespace := resolveNamespace(cfg.Namespace)
	secretReader := secretReader{
		secretLister: secretLister,
		namespace:    namespace,
	}
	pullRegistry := normalizeRegistryHost(cfg.PullRegistry)

	var base Provider
	switch provider {
	case "aws":
		if cfg.AWS == nil {
			return nil, fmt.Errorf("registry aws config is required")
		}
		base = &awsProvider{cfg: *cfg.AWS, secrets: secretReader}
	case "gcp":
		if cfg.GCP == nil {
			return nil, fmt.Errorf("registry gcp config is required")
		}
		base = &gcpProvider{cfg: *cfg.GCP, secrets: secretReader}
	case "azure":
		if cfg.Azure == nil {
			return nil, fmt.Errorf("registry azure config is required")
		}
		base = &azureProvider{cfg: *cfg.Azure, secrets: secretReader}
	case "aliyun":
		if cfg.Aliyun == nil {
			return nil, fmt.Errorf("registry aliyun config is required")
		}
		base = &aliyunProvider{cfg: *cfg.Aliyun, secrets: secretReader}
	case "harbor":
		if cfg.Harbor == nil {
			return nil, fmt.Errorf("registry harbor config is required")
		}
		base = &harborProvider{cfg: *cfg.Harbor, secrets: secretReader}
	case "builtin":
		if cfg.Builtin == nil {
			return nil, fmt.Errorf("registry builtin config is required")
		}
		base = &builtinProvider{
			cfg:      *cfg.Builtin,
			registry: normalizeRegistryHost(cfg.PushRegistry),
			secrets:  secretReader,
		}
	default:
		return nil, fmt.Errorf("unsupported registry provider: %s", provider)
	}

	return &providerWithPullRegistry{
		base:         base,
		pullRegistry: pullRegistry,
	}, nil
}

type providerWithPullRegistry struct {
	base         Provider
	pullRegistry string
}

func (p *providerWithPullRegistry) GetPushCredentials(ctx context.Context, req PushCredentialsRequest) (*Credential, error) {
	creds, err := p.base.GetPushCredentials(ctx, req)
	if err != nil {
		return nil, err
	}
	if creds == nil {
		return nil, nil
	}

	if p.pullRegistry != "" {
		creds.PullRegistry = p.pullRegistry
	} else if creds.PullRegistry == "" {
		// Default to push endpoint when no dedicated pull endpoint is configured.
		creds.PullRegistry = creds.PushRegistry
	}
	if strings.TrimSpace(req.TeamID) != "" {
		creds.PushRegistry = naming.TeamScopedImageRegistry(creds.PushRegistry, req.TeamID)
		creds.PullRegistry = naming.TeamScopedImageRegistry(creds.PullRegistry, req.TeamID)
	}
	return creds, nil
}

type secretReader struct {
	secretLister corelisters.SecretLister
	namespace    string
}

func (s secretReader) read(ctx context.Context, name, key string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("secret name is required")
	}
	var secretData map[string][]byte
	if s.secretLister != nil {
		secret, err := s.secretLister.Secrets(s.namespace).Get(name)
		if err != nil {
			return "", err
		}
		secretData = secret.Data
	} else {
		return "", fmt.Errorf("secret lister is required for registry credentials")
	}
	if secretData == nil {
		return "", fmt.Errorf("secret %q has no data", name)
	}
	value, ok := secretData[key]
	if !ok {
		return "", fmt.Errorf("secret %q missing key %q", name, key)
	}
	return string(value), nil
}

func (s secretReader) readRequired(ctx context.Context, secretName, configuredKey, defaultKey, description string) (string, error) {
	key := strings.TrimSpace(configuredKey)
	if key == "" {
		key = defaultKey
	}
	value, err := s.read(ctx, secretName, key)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", description, err)
	}
	return value, nil
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

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// builtinProvider provides credentials for the builtin registry.
type builtinProvider struct {
	cfg      config.RegistryBuiltinConfig
	registry string
	secrets  secretReader
}

func (p *builtinProvider) GetPushCredentials(ctx context.Context, req PushCredentialsRequest) (*Credential, error) {
	if strings.TrimSpace(p.registry) == "" {
		return nil, fmt.Errorf("builtin push registry is required")
	}
	if err := validateBuiltinTargetImage(req.TeamID, req.TargetImage, p.registry); err != nil {
		return nil, err
	}
	username := strings.TrimSpace(p.cfg.Username)
	password := strings.TrimSpace(p.cfg.Password)
	if username == "" || password == "" {
		var err error
		username, err = p.secrets.readRequired(ctx, p.cfg.AuthSecretName, p.cfg.UsernameKey, "username", "username")
		if err != nil {
			return nil, err
		}
		password, err = p.secrets.readRequired(ctx, p.cfg.AuthSecretName, p.cfg.PasswordKey, "password", "password")
		if err != nil {
			return nil, err
		}
	}
	return &Credential{
		Provider:     "builtin",
		PushRegistry: p.registry,
		Username:     username,
		Password:     password,
	}, nil
}

func validateBuiltinTargetImage(teamID, targetImage, registryHost string) error {
	trimmedTarget := strings.TrimSpace(targetImage)
	if trimmedTarget == "" {
		return fmt.Errorf("%w: targetImage is required for builtin registry credentials", ErrInvalidTargetImage)
	}

	prefix := naming.TeamImageRepositoryPrefix(teamID)
	if prefix == "" {
		return fmt.Errorf("%w: team id is required for builtin registry credentials", ErrInvalidTargetImage)
	}

	registryHostFromTarget, repository, err := naming.SplitImageReference(trimmedTarget)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTargetImage, err)
	}
	if registryHostFromTarget != "" && naming.NormalizeRegistryHost(registryHostFromTarget) != naming.NormalizeRegistryHost(registryHost) {
		return fmt.Errorf("%w: target image %q is outside builtin registry %q", ErrInvalidTargetImage, targetImage, naming.NormalizeRegistryHost(registryHost))
	}
	if repository == prefix || strings.HasPrefix(repository, prefix+"/") {
		return nil
	}
	firstSegment, _, _ := strings.Cut(repository, "/")
	if strings.HasPrefix(firstSegment, "t-") {
		return fmt.Errorf("%w: target image %q is outside team registry prefix %q", ErrInvalidTargetImage, targetImage, prefix)
	}
	return nil
}
