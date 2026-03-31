/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package regionalgateway

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/internalauth"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/registry"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/runtimeconfig"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the regional-gateway deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}

	// Skip if not enabled
	if infra.Spec.Services != nil && infra.Spec.Services.RegionalGateway != nil && !infra.Spec.Services.RegionalGateway.Enabled {
		logger.Info("Edge gateway is disabled, skipping")
		return nil
	}

	deploymentName := fmt.Sprintf("%s-regional-gateway", infra.Name)
	serviceName := deploymentName

	replicas := int32(1)
	if infra.Spec.Services != nil && infra.Spec.Services.RegionalGateway != nil {
		replicas = infra.Spec.Services.RegionalGateway.Replicas
	}

	labels := common.GetServiceLabels(infra.Name, "regional-gateway")
	keySecretName, privateKeyKey, _ := internalauth.GetControlPlaneKeyRefs(infra)

	config, registryEnvVars, err := r.buildConfig(ctx, infra, compiledPlan)
	if err != nil {
		return err
	}
	needEnterpriseLicense := compiledPlan.Enterprise.RegionalGateway
	common.NormalizeEnterpriseLicenseFile(&config.LicenseFile, needEnterpriseLicense)
	podAnnotations, err := common.ConfigHashAnnotation(config)
	if err != nil {
		return err
	}
	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, deploymentName, labels, config); err != nil {
		return err
	}

	var resources *corev1.ResourceRequirements
	serviceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.RegionalGateway != nil {
		resources = infra.Spec.Services.RegionalGateway.Resources
		serviceConfig = infra.Spec.Services.RegionalGateway.Service
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "config",
			MountPath: "/config/config.yaml",
			SubPath:   "config.yaml",
			ReadOnly:  true,
		},
		{
			Name:      "internal-jwt-private-key",
			MountPath: pkginternalauth.DefaultInternalJWTPrivateKeyPath,
			SubPath:   "internal_jwt_private.key",
			ReadOnly:  true,
		},
	}
	volumes := []corev1.Volume{
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: deploymentName},
				},
			},
		},
		{
			Name: "internal-jwt-private-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: keySecretName,
					Items: []corev1.KeyToPath{
						{
							Key:  privateKeyKey,
							Path: "internal_jwt_private.key",
						},
					},
				},
			},
		},
	}
	if needEnterpriseLicense {
		volumeMounts, volumes = common.AppendEnterpriseLicenseVolume(infra, config.LicenseFile, volumeMounts, volumes)
	}

	envVars := []corev1.EnvVar{
		{
			Name:  "SERVICE",
			Value: "regional-gateway",
		},
		{
			Name:  "CONFIG_PATH",
			Value: "/config/config.yaml",
		},
	}
	envVars = append(envVars, registryEnvVars...)

	// Create deployment
	httpPort := int32(config.HTTPPort)
	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:       "regional-gateway",
		Port:       httpPort,
		TargetPort: httpPort,
		Ports: []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: httpPort,
			},
		},
		Image:          fmt.Sprintf("%s:%s", imageRepo, imageTag),
		EnvVars:        envVars,
		VolumeMounts:   volumeMounts,
		Volumes:        volumes,
		PodAnnotations: podAnnotations,
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/healthz",
					Port: intstr.FromString("http"),
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       10,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/readyz",
					Port: intstr.FromString("http"),
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
		},
		Resources: resources,
	}); err != nil {
		return err
	}

	// Create service
	serviceType := common.ResolveServiceType(serviceConfig)
	servicePort := common.ResolveServicePort(serviceConfig, httpPort)
	serviceAnnotations := common.ResolveServiceAnnotations(serviceConfig)
	if err := r.Resources.ReconcileService(ctx, infra, serviceName, labels, serviceType, serviceAnnotations, servicePort, httpPort); err != nil {
		return err
	}

	// Create ingress if enabled
	if infra.Spec.Services != nil && infra.Spec.Services.RegionalGateway != nil &&
		infra.Spec.Services.RegionalGateway.Ingress != nil && infra.Spec.Services.RegionalGateway.Ingress.Enabled {
		if err := r.Resources.ReconcileIngress(ctx, infra, serviceName, servicePort, infra.Spec.Services.RegionalGateway.Ingress); err != nil {
			return err
		}
	} else {
		if err := r.deleteIngressIfExists(ctx, infra, serviceName); err != nil {
			return err
		}
	}

	if err := r.Resources.EnsureDeploymentReady(ctx, infra, deploymentName, replicas); err != nil {
		return err
	}

	logger.Info("Edge gateway reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, compiledPlan *infraplan.InfraPlan) (*apiconfig.RegionalGatewayConfig, []corev1.EnvVar, error) {
	cfg := &apiconfig.RegionalGatewayConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.RegionalGateway != nil {
		cfg = runtimeconfig.ToRegionalGateway(infra.Spec.Services.RegionalGateway.Config)
	}
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}

	if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		cfg.DatabaseURL = dsn
	}
	cfg.DefaultClusterGatewayURL = compiledPlan.RegionalGateway.DefaultClusterGatewayURL

	authMode := strings.TrimSpace(strings.ToLower(cfg.AuthMode))
	if authMode == "" {
		authMode = "self_hosted"
	}
	if infra.Spec.InitUser != nil && authMode != "federated_global" {
		password := ""
		if cfg.BuiltInAuth.Enabled || !apiconfig.HasEnabledOIDCProviders(cfg.OIDCProviders) {
			secretRef := common.ResolveSecretKeyRef(infra.Spec.InitUser.PasswordSecret, "admin-password", "password")
			var err error
			password, err = common.GetSecretValue(ctx, r.Resources.Client, infra.Namespace, secretRef)
			if err != nil {
				return nil, nil, err
			}
		}

		cfg.BuiltInAuth.InitUser = &apiconfig.InitUserConfig{
			Email:    infra.Spec.InitUser.Email,
			Password: password,
			Name:     infra.Spec.InitUser.Name,
		}
	}

	if strings.TrimSpace(cfg.JWTIssuer) == "" {
		cfg.JWTIssuer = "regional-gateway"
	}

	if strings.TrimSpace(cfg.JWTSecret) == "" {
		jwtSecret, err := common.EnsureSecretValue(
			ctx,
			r.Resources.Client,
			r.Resources.Scheme,
			infra,
			fmt.Sprintf("%s-regional-gateway-jwt", infra.Name),
			"jwt_secret",
			32,
		)
		if err != nil {
			return nil, nil, err
		}
		cfg.JWTSecret = jwtSecret
	}

	if strings.TrimSpace(infra.Spec.Region) != "" {
		cfg.RegionID = infra.Spec.Region
	}

	// Copy public exposure config from CRD top-level spec
	if infra.Spec.PublicExposure != nil {
		cfg.PublicExposureEnabled = infra.Spec.PublicExposure.Enabled
		cfg.PublicRootDomain = infra.Spec.PublicExposure.RootDomain
		cfg.PublicRegionID = infra.Spec.PublicExposure.RegionID
	}

	registryEnvVars, err := r.applyRegistryConfig(infra, cfg)
	if err != nil {
		return nil, nil, err
	}

	return cfg, registryEnvVars, nil
}

func (r *Reconciler) applyRegistryConfig(infra *infrav1alpha1.Sandbox0Infra, cfg *apiconfig.RegionalGatewayConfig) ([]corev1.EnvVar, error) {
	if !infrav1alpha1.IsRegistryEnabled(infra) {
		return nil, nil
	}

	resolved := registry.ResolveRegistryConfig(infra)
	if resolved == nil {
		return nil, nil
	}

	cfg.Registry.Provider = string(resolved.Provider)
	cfg.Registry.PushRegistry = resolved.PushRegistry
	cfg.Registry.PullRegistry = resolved.PullRegistry
	cfg.Registry.Namespace = infra.Namespace

	switch resolved.Provider {
	case infrav1alpha1.RegistryProviderBuiltin:
		cfg.Registry.Builtin = &apiconfig.RegistryBuiltinConfig{
			Username: "${S0_REGISTRY_BUILTIN_USERNAME}",
			Password: "${S0_REGISTRY_BUILTIN_PASSWORD}",
		}
		secretName := fmt.Sprintf("%s-registry-auth", infra.Name)
		return []corev1.EnvVar{
			secretEnvVar("S0_REGISTRY_BUILTIN_USERNAME", secretName, "username"),
			secretEnvVar("S0_REGISTRY_BUILTIN_PASSWORD", secretName, "password"),
		}, nil
	case infrav1alpha1.RegistryProviderAWS:
		if infra.Spec.Registry == nil || infra.Spec.Registry.AWS == nil {
			return nil, fmt.Errorf("registry.aws configuration is required")
		}
		cred := infra.Spec.Registry.AWS.CredentialsSecret
		cfg.Registry.AWS = &apiconfig.RegistryAWSConfig{
			Region:           infra.Spec.Registry.AWS.Region,
			RegistryID:       infra.Spec.Registry.AWS.RegistryID,
			RegistryOverride: infra.Spec.Registry.AWS.Registry,
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
		if infra.Spec.Registry == nil || infra.Spec.Registry.GCP == nil {
			return nil, fmt.Errorf("registry.gcp configuration is required")
		}
		sa := infra.Spec.Registry.GCP.ServiceAccountSecret
		cfg.Registry.GCP = &apiconfig.RegistryGCPConfig{
			Registry:           infra.Spec.Registry.GCP.Registry,
			ServiceAccountJSON: "${S0_REGISTRY_GCP_SERVICE_ACCOUNT_JSON}",
		}
		return []corev1.EnvVar{
			secretEnvVar("S0_REGISTRY_GCP_SERVICE_ACCOUNT_JSON", sa.Name, defaultString(sa.Key, "serviceAccount.json")),
		}, nil
	case infrav1alpha1.RegistryProviderAzure:
		if infra.Spec.Registry == nil || infra.Spec.Registry.Azure == nil {
			return nil, fmt.Errorf("registry.azure configuration is required")
		}
		cred := infra.Spec.Registry.Azure.CredentialsSecret
		cfg.Registry.Azure = &apiconfig.RegistryAzureConfig{
			Registry:     infra.Spec.Registry.Azure.Registry,
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
		if infra.Spec.Registry == nil || infra.Spec.Registry.Aliyun == nil {
			return nil, fmt.Errorf("registry.aliyun configuration is required")
		}
		cred := infra.Spec.Registry.Aliyun.CredentialsSecret
		cfg.Registry.Aliyun = &apiconfig.RegistryAliyunConfig{
			Registry:        infra.Spec.Registry.Aliyun.Registry,
			Region:          infra.Spec.Registry.Aliyun.Region,
			InstanceID:      infra.Spec.Registry.Aliyun.InstanceID,
			AccessKeyID:     "${S0_REGISTRY_ALIYUN_ACCESS_KEY_ID}",
			AccessKeySecret: "${S0_REGISTRY_ALIYUN_ACCESS_KEY_SECRET}",
		}
		return []corev1.EnvVar{
			secretEnvVar("S0_REGISTRY_ALIYUN_ACCESS_KEY_ID", cred.Name, defaultString(cred.AccessKeyKey, "accessKeyId")),
			secretEnvVar("S0_REGISTRY_ALIYUN_ACCESS_KEY_SECRET", cred.Name, defaultString(cred.SecretKeyKey, "accessKeySecret")),
		}, nil
	case infrav1alpha1.RegistryProviderHarbor:
		if infra.Spec.Registry == nil || infra.Spec.Registry.Harbor == nil {
			return nil, fmt.Errorf("registry.harbor configuration is required")
		}
		cred := infra.Spec.Registry.Harbor.CredentialsSecret
		cfg.Registry.Harbor = &apiconfig.RegistryHarborConfig{
			Registry: infra.Spec.Registry.Harbor.Registry,
			Username: "${S0_REGISTRY_HARBOR_USERNAME}",
			Password: "${S0_REGISTRY_HARBOR_PASSWORD}",
		}
		return []corev1.EnvVar{
			secretEnvVar("S0_REGISTRY_HARBOR_USERNAME", cred.Name, defaultString(cred.UsernameKey, "username")),
			secretEnvVar("S0_REGISTRY_HARBOR_PASSWORD", cred.Name, defaultString(cred.PasswordKey, "password")),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported registry provider for regional-gateway: %s", resolved.Provider)
	}
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
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

func (r *Reconciler) deleteIngressIfExists(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string) error {
	obj := &networkingv1.Ingress{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, obj)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err := r.Resources.Client.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
