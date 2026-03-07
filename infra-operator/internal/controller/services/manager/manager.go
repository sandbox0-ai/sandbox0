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

package manager

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/internalauth"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/registry"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

const registryCredentialsPath = "/etc/sandbox0/registry/.dockerconfigjson"

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the manager deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string) error {
	logger := log.FromContext(ctx)

	// Skip if not enabled
	if infra.Spec.Services != nil && infra.Spec.Services.Manager != nil && !infra.Spec.Services.Manager.Enabled {
		logger.Info("Manager is disabled, skipping")
		return nil
	}

	deploymentName := fmt.Sprintf("%s-manager", infra.Name)

	replicas := int32(1)
	if infra.Spec.Services != nil && infra.Spec.Services.Manager != nil {
		replicas = infra.Spec.Services.Manager.Replicas
	}

	labels := common.GetServiceLabels(infra.Name, "manager")
	keySecretName, privateKeyKey, publicKeyKey := internalauth.GetDataPlaneKeyRefs(infra)

	config, err := r.buildConfig(ctx, infra, imageRepo, imageTag)
	if err != nil {
		return err
	}

	if err := common.EnsureBuiltinTemplates(ctx, infra, common.BuiltinTemplateOptions{
		DatabaseURL:          config.DatabaseURL,
		DatabaseMaxConns:     config.DatabaseMaxConns,
		DatabaseMinConns:     config.DatabaseMinConns,
		TemplateStoreEnabled: config.TemplateStoreEnabled,
		Owner:                "manager",
	}); err != nil {
		return err
	}

	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, deploymentName, labels, config); err != nil {
		return err
	}

	httpPort := int32(config.HTTPPort)
	metricsPort := int32(config.MetricsPort)
	webhookPort := int32(config.WebhookPort)

	var resources *corev1.ResourceRequirements
	serviceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.Manager != nil {
		resources = infra.Spec.Services.Manager.Resources
		serviceConfig = infra.Spec.Services.Manager.Service
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
		{
			Name:      "internal-jwt-public-key",
			MountPath: pkginternalauth.DefaultInternalJWTPublicKeyPath,
			SubPath:   "internal_jwt_public.key",
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
		{
			Name: "internal-jwt-public-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: keySecretName,
					Items: []corev1.KeyToPath{
						{
							Key:  publicKeyKey,
							Path: "internal_jwt_public.key",
						},
					},
				},
			},
		},
	}

	registryConfig := registry.ResolveRegistryConfig(infra)
	if registryConfig != nil && registryConfig.SourceSecretName != "" && registryConfig.SourceSecretKey != "" {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "registry-credentials",
			MountPath: registryCredentialsPath,
			SubPath:   registryConfig.SourceSecretKey,
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "registry-credentials",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: registryConfig.SourceSecretName,
					Items: []corev1.KeyToPath{
						{
							Key:  registryConfig.SourceSecretKey,
							Path: registryConfig.SourceSecretKey,
						},
					},
				},
			},
		})
	}

	// Create deployment
	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:               "manager",
		Port:               httpPort,
		TargetPort:         httpPort,
		ServiceAccountName: fmt.Sprintf("%s-manager", infra.Name),
		Ports: []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: httpPort,
			},
			{
				Name:          "metrics",
				ContainerPort: metricsPort,
			},
			{
				Name:          "webhook",
				ContainerPort: webhookPort,
			},
		},
		Image: fmt.Sprintf("%s:%s", imageRepo, imageTag),
		EnvVars: []corev1.EnvVar{
			{
				Name:  "SERVICE",
				Value: "manager",
			},
			{
				Name:  "CONFIG_PATH",
				Value: "/config/config.yaml",
			},
		},
		VolumeMounts: volumeMounts,
		Volumes:      volumes,
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
	if err := r.Resources.ReconcileServicePorts(ctx, infra, deploymentName, labels, serviceType, []corev1.ServicePort{
		common.BuildServicePort("http", servicePort, httpPort, serviceType),
		common.BuildServicePort("metrics", metricsPort, metricsPort, serviceType),
		common.BuildServicePort("webhook", webhookPort, webhookPort, serviceType),
	}); err != nil {
		return err
	}

	if err := r.Resources.EnsureDeploymentReady(ctx, infra, deploymentName, replicas); err != nil {
		return err
	}

	logger.Info("Manager reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string) (*apiconfig.ManagerConfig, error) {
	cfg := &apiconfig.ManagerConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.Manager != nil && infra.Spec.Services.Manager.Config != nil {
		cfg = infra.Spec.Services.Manager.Config
	}

	if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		cfg.DatabaseURL = dsn
	}

	cfg.TemplateStoreEnabled = templateStoreEnabledByInternalGateway(infra)
	cfg.NetworkPolicyProvider = resolveNetworkPolicyProvider(infra)
	cfg.SandboxPodPlacement = resolveSandboxPodPlacement(infra)

	if infra.Spec.Cluster != nil && infra.Spec.Cluster.ID != "" {
		cfg.DefaultClusterId = infra.Spec.Cluster.ID
	}

	cfg.ManagerImage = fmt.Sprintf("%s:%s", imageRepo, imageTag)

	registryConfig := registry.ResolveRegistryConfig(infra)
	if registryConfig != nil {
		cfg.Registry.Provider = string(registryConfig.Provider)
		cfg.Registry.PushRegistry = registryConfig.PushRegistry
		cfg.Registry.PullRegistry = registryConfig.PullRegistry
		cfg.Registry.PullSecretName = registryConfig.TargetSecretName
		cfg.Registry.Namespace = infra.Namespace
		if registryConfig.SourceSecretName != "" {
			cfg.Registry.PullCredentialsFile = registryCredentialsPath
		}
		// For builtin provider, configure auth secret for push credentials
		if registryConfig.Provider == infrav1alpha1.RegistryProviderBuiltin {
			cfg.Registry.Builtin = &apiconfig.RegistryBuiltinConfig{
				AuthSecretName: fmt.Sprintf("%s-registry-auth", infra.Name),
				UsernameKey:    "username",
				PasswordKey:    "password",
			}
		}
	}
	if infra.Spec.Registry != nil {
		switch infra.Spec.Registry.Provider {
		case infrav1alpha1.RegistryProviderAWS:
			if infra.Spec.Registry.AWS != nil {
				cfg.Registry.AWS = &apiconfig.RegistryAWSConfig{
					Region:           infra.Spec.Registry.AWS.Region,
					RegistryID:       infra.Spec.Registry.AWS.RegistryID,
					AccessKeySecret:  infra.Spec.Registry.AWS.CredentialsSecret.Name,
					AccessKeyKey:     infra.Spec.Registry.AWS.CredentialsSecret.AccessKeyKey,
					SecretKeyKey:     infra.Spec.Registry.AWS.CredentialsSecret.SecretKeyKey,
					SessionTokenKey:  infra.Spec.Registry.AWS.CredentialsSecret.SessionTokenKey,
					RegistryOverride: infra.Spec.Registry.AWS.Registry,
				}
			}
		case infrav1alpha1.RegistryProviderGCP:
			if infra.Spec.Registry.GCP != nil {
				cfg.Registry.GCP = &apiconfig.RegistryGCPConfig{
					Registry:             infra.Spec.Registry.GCP.Registry,
					ServiceAccountSecret: infra.Spec.Registry.GCP.ServiceAccountSecret.Name,
					ServiceAccountKey:    infra.Spec.Registry.GCP.ServiceAccountSecret.Key,
				}
			}
		case infrav1alpha1.RegistryProviderAzure:
			if infra.Spec.Registry.Azure != nil {
				cfg.Registry.Azure = &apiconfig.RegistryAzureConfig{
					Registry:          infra.Spec.Registry.Azure.Registry,
					CredentialsSecret: infra.Spec.Registry.Azure.CredentialsSecret.Name,
					TenantIDKey:       infra.Spec.Registry.Azure.CredentialsSecret.TenantIDKey,
					ClientIDKey:       infra.Spec.Registry.Azure.CredentialsSecret.ClientIDKey,
					ClientSecretKey:   infra.Spec.Registry.Azure.CredentialsSecret.ClientSecretKey,
				}
			}
		case infrav1alpha1.RegistryProviderAliyun:
			if infra.Spec.Registry.Aliyun != nil {
				cfg.Registry.Aliyun = &apiconfig.RegistryAliyunConfig{
					Registry:          infra.Spec.Registry.Aliyun.Registry,
					Region:            infra.Spec.Registry.Aliyun.Region,
					InstanceID:        infra.Spec.Registry.Aliyun.InstanceID,
					CredentialsSecret: infra.Spec.Registry.Aliyun.CredentialsSecret.Name,
					AccessKeyKey:      infra.Spec.Registry.Aliyun.CredentialsSecret.AccessKeyKey,
					SecretKeyKey:      infra.Spec.Registry.Aliyun.CredentialsSecret.SecretKeyKey,
				}
			}
		case infrav1alpha1.RegistryProviderHarbor:
			if infra.Spec.Registry.Harbor != nil {
				cfg.Registry.Harbor = &apiconfig.RegistryHarborConfig{
					Registry:          infra.Spec.Registry.Harbor.Registry,
					CredentialsSecret: infra.Spec.Registry.Harbor.CredentialsSecret.Name,
					UsernameKey:       infra.Spec.Registry.Harbor.CredentialsSecret.UsernameKey,
					PasswordKey:       infra.Spec.Registry.Harbor.CredentialsSecret.PasswordKey,
				}
			}
		}
	}

	storageProxyConfig := &apiconfig.StorageProxyConfig{}
	storageProxyServiceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil && infra.Spec.Services.StorageProxy.Config != nil {
		storageProxyConfig = infra.Spec.Services.StorageProxy.Config
	}
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		storageProxyServiceConfig = infra.Spec.Services.StorageProxy.Service
	}

	if infrav1alpha1.IsStorageProxyEnabled(infra) {
		cfg.ProcdConfig.StorageProxyBaseURL = fmt.Sprintf("%s-storage-proxy.%s.svc.cluster.local", infra.Name, infra.Namespace)
		cfg.ProcdConfig.StorageProxyPort = int(common.ResolveServicePort(storageProxyServiceConfig, int32(storageProxyConfig.GRPCPort)))
	} else {
		cfg.ProcdConfig.StorageProxyBaseURL = ""
		cfg.ProcdConfig.StorageProxyPort = 0
	}

	// Copy public exposure config from CRD top-level spec for generating public URLs
	if infra.Spec.PublicExposure != nil {
		cfg.PublicRootDomain = infra.Spec.PublicExposure.RootDomain
		cfg.PublicRegionID = infra.Spec.PublicExposure.RegionID
	}

	return cfg, nil
}

// Enable template store if internal-gateway is not in multi-cluster mode.
func templateStoreEnabledByInternalGateway(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.InternalGateway == nil {
		return false
	}
	cfg := infra.Spec.Services.InternalGateway.Config
	mode := ""
	if cfg != nil {
		mode = cfg.AuthMode
	}
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		mode = "internal"
	}
	return mode != "internal"
}

func resolveNetworkPolicyProvider(infra *infrav1alpha1.Sandbox0Infra) string {
	if infrav1alpha1.IsNetdEnabled(infra) {
		return "netd"
	}
	return "noop"
}

func resolveSandboxPodPlacement(infra *infrav1alpha1.Sandbox0Infra) apiconfig.SandboxPodPlacementConfig {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Netd == nil {
		return apiconfig.SandboxPodPlacementConfig{}
	}

	placement := apiconfig.SandboxPodPlacementConfig{}
	if len(infra.Spec.Services.Netd.NodeSelector) > 0 {
		placement.NodeSelector = make(map[string]string, len(infra.Spec.Services.Netd.NodeSelector))
		for key, value := range infra.Spec.Services.Netd.NodeSelector {
			placement.NodeSelector[key] = value
		}
	}
	if len(infra.Spec.Services.Netd.Tolerations) > 0 {
		placement.Tolerations = make([]corev1.Toleration, len(infra.Spec.Services.Netd.Tolerations))
		copy(placement.Tolerations, infra.Spec.Services.Netd.Tolerations)
	}
	return placement
}
