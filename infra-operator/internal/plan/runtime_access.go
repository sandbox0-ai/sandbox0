package plan

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	controllerinternalauth "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/internalauth"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/registry"
)

func (p *InfraPlan) BuiltinTemplates() []infrav1alpha1.BuiltinTemplateConfig {
	if p == nil || p.infra == nil || len(p.infra.Spec.BuiltinTemplates) == 0 {
		return nil
	}
	out := make([]infrav1alpha1.BuiltinTemplateConfig, len(p.infra.Spec.BuiltinTemplates))
	for i := range p.infra.Spec.BuiltinTemplates {
		p.infra.Spec.BuiltinTemplates[i].DeepCopyInto(&out[i])
	}
	return out
}

func (p *InfraPlan) DatabaseDSN(ctx context.Context, kubeClient client.Client) (string, error) {
	if p == nil || p.infra == nil {
		return "", fmt.Errorf("compiled plan source infra is required")
	}
	if p.infra.Spec.Database == nil {
		return "", fmt.Errorf("database is not configured")
	}
	return database.GetDatabaseDSN(ctx, kubeClient, p.infra)
}

func (p *InfraPlan) InitUser() *infrav1alpha1.InitUserConfig {
	if p == nil || p.infra == nil || p.infra.Spec.InitUser == nil {
		return nil
	}
	return p.infra.Spec.InitUser.DeepCopy()
}

func (p *InfraPlan) ControlPlaneKeyRefs() (string, string, string) {
	if p == nil || p.infra == nil {
		return "", "", ""
	}
	return controllerinternalauth.GetControlPlaneKeyRefs(p.infra)
}

func (p *InfraPlan) DataPlaneKeyRefs() (string, string, string) {
	if p == nil || p.infra == nil {
		return "", "", ""
	}
	return controllerinternalauth.GetDataPlaneKeyRefs(p.infra)
}

func (p *InfraPlan) ControlPlanePublicKeyRef() (string, string) {
	if p == nil || p.infra == nil {
		return "", ""
	}
	return controllerinternalauth.GetControlPlanePublicKeyRef(p.infra)
}

func (p *InfraPlan) EnterpriseLicenseSecretRef() infrav1alpha1.SecretKeyRef {
	if p == nil || p.infra == nil {
		return infrav1alpha1.SecretKeyRef{}
	}
	return common.ResolveEnterpriseLicenseSecretRef(p.infra)
}

func (p *InfraPlan) ManagerRegistryCredentialsSource() (string, string) {
	resolved := p.registryConfig()
	if resolved == nil {
		return "", ""
	}
	return resolved.SourceSecretName, resolved.SourceSecretKey
}

func (p *InfraPlan) ConfigureRegionalGatewayRegistry(cfg *apiconfig.RegionalGatewayConfig) ([]corev1.EnvVar, error) {
	if cfg == nil || p == nil || p.infra == nil || !infrav1alpha1.IsRegistryEnabled(p.infra) {
		return nil, nil
	}

	resolved := p.registryConfig()
	if resolved == nil {
		return nil, nil
	}

	cfg.Registry.Provider = string(resolved.Provider)
	cfg.Registry.PushRegistry = resolved.PushRegistry
	cfg.Registry.PullRegistry = resolved.PullRegistry
	cfg.Registry.Namespace = p.Scope.Namespace

	switch resolved.Provider {
	case infrav1alpha1.RegistryProviderBuiltin:
		cfg.Registry.Builtin = &apiconfig.RegistryBuiltinConfig{
			Username: "${S0_REGISTRY_BUILTIN_USERNAME}",
			Password: "${S0_REGISTRY_BUILTIN_PASSWORD}",
		}
		secretName := fmt.Sprintf("%s-registry-auth", p.Scope.Name)
		return []corev1.EnvVar{
			secretEnvVar("S0_REGISTRY_BUILTIN_USERNAME", secretName, "username"),
			secretEnvVar("S0_REGISTRY_BUILTIN_PASSWORD", secretName, "password"),
		}, nil
	case infrav1alpha1.RegistryProviderAWS:
		if p.infra.Spec.Registry == nil || p.infra.Spec.Registry.AWS == nil {
			return nil, fmt.Errorf("registry.aws configuration is required")
		}
		cred := p.infra.Spec.Registry.AWS.CredentialsSecret
		cfg.Registry.AWS = &apiconfig.RegistryAWSConfig{
			Region:           p.infra.Spec.Registry.AWS.Region,
			RegistryID:       p.infra.Spec.Registry.AWS.RegistryID,
			AssumeRoleARN:    p.infra.Spec.Registry.AWS.AssumeRoleARN,
			ExternalID:       p.infra.Spec.Registry.AWS.ExternalID,
			RegistryOverride: p.infra.Spec.Registry.AWS.Registry,
			AccessKeyID:      "${S0_REGISTRY_AWS_ACCESS_KEY_ID}",
			SecretAccessKey:  "${S0_REGISTRY_AWS_SECRET_ACCESS_KEY}",
		}
		envVars := []corev1.EnvVar{
			secretEnvVar("S0_REGISTRY_AWS_ACCESS_KEY_ID", cred.Name, defaultString(cred.AccessKeyKey, "accessKeyId")),
			secretEnvVar("S0_REGISTRY_AWS_SECRET_ACCESS_KEY", cred.Name, defaultString(cred.SecretKeyKey, "secretAccessKey")),
		}
		if strings.TrimSpace(cred.SessionTokenKey) != "" {
			cfg.Registry.AWS.SessionToken = "${S0_REGISTRY_AWS_SESSION_TOKEN}"
			envVars = append(envVars, secretEnvVar("S0_REGISTRY_AWS_SESSION_TOKEN", cred.Name, cred.SessionTokenKey))
		}
		return envVars, nil
	case infrav1alpha1.RegistryProviderGCP:
		if p.infra.Spec.Registry == nil || p.infra.Spec.Registry.GCP == nil {
			return nil, fmt.Errorf("registry.gcp configuration is required")
		}
		cfg.Registry.GCP = &apiconfig.RegistryGCPConfig{Registry: p.infra.Spec.Registry.GCP.Registry}
		if p.infra.Spec.Registry.GCP.ServiceAccountSecret == nil {
			return nil, nil
		}
		sa := p.infra.Spec.Registry.GCP.ServiceAccountSecret
		cfg.Registry.GCP.ServiceAccountJSON = "${S0_REGISTRY_GCP_SERVICE_ACCOUNT_JSON}"
		return []corev1.EnvVar{
			secretEnvVar("S0_REGISTRY_GCP_SERVICE_ACCOUNT_JSON", sa.Name, defaultString(sa.Key, "serviceAccount.json")),
		}, nil
	case infrav1alpha1.RegistryProviderAzure:
		if p.infra.Spec.Registry == nil || p.infra.Spec.Registry.Azure == nil {
			return nil, fmt.Errorf("registry.azure configuration is required")
		}
		cred := p.infra.Spec.Registry.Azure.CredentialsSecret
		cfg.Registry.Azure = &apiconfig.RegistryAzureConfig{
			Registry:     p.infra.Spec.Registry.Azure.Registry,
			TenantID:     "${S0_REGISTRY_AZURE_TENANT_ID}",
			ClientID:     "${S0_REGISTRY_AZURE_CLIENT_ID}",
			ClientSecret: "${S0_REGISTRY_AZURE_CLIENT_SECRET}",
		}
		return []corev1.EnvVar{
			secretEnvVar("S0_REGISTRY_AZURE_TENANT_ID", cred.Name, defaultString(cred.TenantIDKey, "tenantId")),
			secretEnvVar("S0_REGISTRY_AZURE_CLIENT_ID", cred.Name, defaultString(cred.ClientIDKey, "clientId")),
			secretEnvVar("S0_REGISTRY_AZURE_CLIENT_SECRET", cred.Name, defaultString(cred.ClientSecretKey, "clientSecret")),
		}, nil
	case infrav1alpha1.RegistryProviderAliyun:
		if p.infra.Spec.Registry == nil || p.infra.Spec.Registry.Aliyun == nil {
			return nil, fmt.Errorf("registry.aliyun configuration is required")
		}
		cred := p.infra.Spec.Registry.Aliyun.CredentialsSecret
		cfg.Registry.Aliyun = &apiconfig.RegistryAliyunConfig{
			Registry:        p.infra.Spec.Registry.Aliyun.Registry,
			Region:          p.infra.Spec.Registry.Aliyun.Region,
			InstanceID:      p.infra.Spec.Registry.Aliyun.InstanceID,
			AccessKeyID:     "${S0_REGISTRY_ALIYUN_ACCESS_KEY_ID}",
			AccessKeySecret: "${S0_REGISTRY_ALIYUN_ACCESS_KEY_SECRET}",
		}
		return []corev1.EnvVar{
			secretEnvVar("S0_REGISTRY_ALIYUN_ACCESS_KEY_ID", cred.Name, defaultString(cred.AccessKeyKey, "accessKeyId")),
			secretEnvVar("S0_REGISTRY_ALIYUN_ACCESS_KEY_SECRET", cred.Name, defaultString(cred.SecretKeyKey, "accessKeySecret")),
		}, nil
	case infrav1alpha1.RegistryProviderHarbor:
		if p.infra.Spec.Registry == nil || p.infra.Spec.Registry.Harbor == nil {
			return nil, fmt.Errorf("registry.harbor configuration is required")
		}
		cred := p.infra.Spec.Registry.Harbor.CredentialsSecret
		cfg.Registry.Harbor = &apiconfig.RegistryHarborConfig{
			Registry: p.infra.Spec.Registry.Harbor.Registry,
			Username: "${S0_REGISTRY_HARBOR_USERNAME}",
			Password: "${S0_REGISTRY_HARBOR_PASSWORD}",
		}
		return []corev1.EnvVar{
			secretEnvVar("S0_REGISTRY_HARBOR_USERNAME", cred.Name, defaultString(cred.UsernameKey, "username")),
			secretEnvVar("S0_REGISTRY_HARBOR_PASSWORD", cred.Name, defaultString(cred.PasswordKey, "password")),
		}, nil
	default:
		return nil, nil
	}
}

func (p *InfraPlan) ObservabilityEnvVars() []corev1.EnvVar {
	if p == nil || !p.Observability.Enabled || !p.Observability.CollectorEnabled || strings.TrimSpace(p.Observability.CollectorServiceURL) == "" {
		return nil
	}
	endpoint := strings.TrimPrefix(strings.TrimSpace(p.Observability.CollectorServiceURL), "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")
	return []corev1.EnvVar{
		{Name: "OTEL_EXPORTER_TYPE", Value: "otlp"},
		{Name: "OTEL_EXPORTER_ENDPOINT", Value: endpoint},
	}
}

func (p *InfraPlan) DefaultFederatedGlobalJWTIssuer() string {
	if p != nil && p.infra != nil && p.infra.Spec.Services != nil && p.infra.Spec.Services.GlobalGateway != nil && p.infra.Spec.Services.GlobalGateway.Config != nil {
		if issuer := strings.TrimSpace(p.infra.Spec.Services.GlobalGateway.Config.JWTIssuer); issuer != "" {
			return issuer
		}
	}
	return "global-gateway"
}

func (p *InfraPlan) SharedUserJWTSecretName() string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%s-user-jwt", p.Scope.Name)
}

func (p *InfraPlan) RegionalGatewayJWTSecretName() string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%s-regional-gateway-jwt", p.Scope.Name)
}

func (p *InfraPlan) ClusterGatewayJWTSecretName() string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%s-cluster-gateway-jwt", p.Scope.Name)
}

func (p *InfraPlan) ResolveNetdMITMCASecretName() string {
	if p == nil || p.infra == nil {
		return ""
	}
	if p.infra.Spec.Services != nil && p.infra.Spec.Services.Netd != nil {
		if secretName := strings.TrimSpace(p.infra.Spec.Services.Netd.MITMCASecretName); secretName != "" {
			return secretName
		}
	}
	if p.Scope.Name == "" {
		return ""
	}
	return fmt.Sprintf("%s-netd-mitm-ca", p.Scope.Name)
}

func (p *InfraPlan) registryConfig() *registry.ResolvedRegistryConfig {
	if p == nil || p.infra == nil {
		return nil
	}
	return registry.ResolveRegistryConfig(p.infra)
}

func secretEnvVar(name, secretName, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  key,
			},
		},
	}
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
