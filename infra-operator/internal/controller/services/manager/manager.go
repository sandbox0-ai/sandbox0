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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	credentialstoresvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/credentialstore"
	meteringsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/metering"
	netdservice "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/netd"
	redissvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/redis"
	sandboxobssvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/sandboxobservability"
	storageruntimesvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/storageruntime"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

const (
	registryCredentialsPath     = "/etc/sandbox0/registry/.dockerconfigjson"
	storageRuntimeConfigPath    = "/config/storage-runtime.yaml"
	managerConfigHashAnnotation = "infra.sandbox0.ai/manager-config-hash"
	storageConfigHashAnnotation = "infra.sandbox0.ai/storage-config-hash"
	storageRuntimeFallbackPort  = int32(18081)
)

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the manager deployment.
func (r *Reconciler) Reconcile(ctx context.Context, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		return fmt.Errorf("compiled plan is required")
	}

	if !compiledPlan.Components.EnableManager {
		logger.Info("Manager is disabled, skipping")
		return nil
	}

	scope := compiledPlan.Scope
	deploymentName := fmt.Sprintf("%s-manager", scope.Name)
	replicas := compiledPlan.Manager.Replicas
	labels := common.GetServiceLabels(scope.Name, "manager")
	keySecretName, privateKeyKey, publicKeyKey := compiledPlan.DataPlaneKeyRefs()

	config, err := r.buildConfig(ctx, imageRepo, imageTag, compiledPlan)
	if err != nil {
		return err
	}
	httpPort := int32(config.HTTPPort)
	metricsPort := int32(config.MetricsPort)
	webhookPort := int32(config.WebhookPort)
	configRef, err := r.Resources.ReconcileHashedServiceConfigMapWithScope(ctx, scope, deploymentName, labels, config)
	if err != nil {
		return err
	}
	podAnnotations := configRef.PodAnnotations()
	var storageConfig *apiconfig.StorageProxyConfig
	var storageConfigRef common.ServiceConfigRef
	var storageServiceHTTPPort int32
	if compiledPlan.Components.EnableStorageRuntime {
		if scope.Owner() == nil {
			return fmt.Errorf("infra owner is required for manager storage")
		}
		storageConfig, err = storageruntimesvc.BuildRuntimeConfig(ctx, r.Resources, scope.Owner())
		if err != nil {
			return fmt.Errorf("build manager storage config: %w", err)
		}
		storageServiceHTTPPort = int32(storageConfig.HTTPPort)
		storageConfig.HTTPPort, err = resolveStorageRuntimeHTTPPort(storageServiceHTTPPort, httpPort, metricsPort, webhookPort)
		if err != nil {
			return err
		}
		storageConfigRef, err = r.Resources.ReconcileHashedServiceConfigMapWithScope(
			ctx,
			scope,
			deploymentName+"-storage",
			common.GetServiceLabels(scope.Name, "manager-storage"),
			storageConfig,
		)
		if err != nil {
			return err
		}
		podAnnotations = map[string]string{
			common.PodTemplateConfigHashAnnotation: configRef.Hash + "." + storageConfigRef.Hash,
			managerConfigHashAnnotation:            configRef.Hash,
			storageConfigHashAnnotation:            storageConfigRef.Hash,
		}
	}

	resources := compiledPlan.Manager.Resources
	serviceConfig := compiledPlan.Manager.ServiceConfig

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
					LocalObjectReference: corev1.LocalObjectReference{Name: configRef.ConfigMapName},
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
	if storageConfig != nil {
		storageMounts, storageVolumes, err := storageruntimesvc.BuildRuntimeVolumes(scope, storageConfig, storageruntimesvc.RuntimeVolumeOptions{
			ConfigMapName:    storageConfigRef.ConfigMapName,
			ConfigVolumeName: "storage-config",
			ConfigMountPath:  storageRuntimeConfigPath,
			CacheVolumeName:  "storage-cache",
			LogVolumeName:    "storage-logs",
		})
		if err != nil {
			return err
		}
		volumeMounts = append(volumeMounts, storageMounts...)
		volumes = append(volumes, storageVolumes...)
	}

	registrySecretName, registrySecretKey := compiledPlan.ManagerRegistryCredentialsSource()
	if registrySecretName != "" && registrySecretKey != "" {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "registry-credentials",
			MountPath: registryCredentialsPath,
			SubPath:   registrySecretKey,
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "registry-credentials",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: registrySecretName,
					Items: []corev1.KeyToPath{
						{
							Key:  registrySecretKey,
							Path: registrySecretKey,
						},
					},
				},
			},
		})
	}

	credentialStoreMounts, credentialStoreVolumes := credentialstoresvc.ManagerCredentialStoreVolumes(scope, config)
	volumeMounts = append(volumeMounts, credentialStoreMounts...)
	volumes = append(volumes, credentialStoreVolumes...)

	containerPorts := []corev1.ContainerPort{
		{Name: "http", ContainerPort: httpPort},
		{Name: "metrics", ContainerPort: metricsPort},
		{Name: "webhook", ContainerPort: webhookPort},
	}
	envVars := []corev1.EnvVar{
		{Name: "SERVICE", Value: "manager"},
		{Name: "CONFIG_PATH", Value: "/config/config.yaml"},
	}
	if storageConfig != nil {
		storageHTTPPort := int32(storageConfig.HTTPPort)
		containerPorts = append(containerPorts,
			corev1.ContainerPort{Name: "storage-http", ContainerPort: storageHTTPPort},
		)
		envVars = append(envVars, corev1.EnvVar{Name: "STORAGE_RUNTIME_CONFIG_PATH", Value: storageRuntimeConfigPath})
	}

	// Create deployment
	if err := r.Resources.ReconcileDeploymentWithScope(ctx, scope, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:               "manager",
		Port:               httpPort,
		TargetPort:         httpPort,
		ServiceAccountName: fmt.Sprintf("%s-manager", scope.Name),
		Ports:              containerPorts,
		Image:              fmt.Sprintf("%s:%s", imageRepo, imageTag),
		EnvVars: common.AppendObservabilityEnvVars(envVars, scope.Owner(), common.ObservabilityEnvConfig{
			ServiceName: "manager",
			RegionID:    compiledPlan.Manager.RegionID,
			ClusterID:   compiledPlan.Manager.DefaultClusterID,
		}),
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
	servicePorts := []corev1.ServicePort{
		common.BuildServicePort("http", servicePort, httpPort, serviceType),
		common.BuildServicePort("metrics", metricsPort, metricsPort, serviceType),
		common.BuildServicePort("webhook", webhookPort, webhookPort, serviceType),
	}
	if storageConfig != nil {
		servicePorts = append(servicePorts, common.BuildServicePort("storage-http", storageServiceHTTPPort, int32(storageConfig.HTTPPort), serviceType))
	}
	if err := validateManagerServicePorts(servicePorts); err != nil {
		return err
	}
	if err := r.Resources.ReconcileServicePortsWithScope(ctx, scope, deploymentName, labels, serviceType, serviceAnnotations, servicePorts); err != nil {
		return err
	}

	if err := r.Resources.EnsureDeploymentReadyWithScope(ctx, scope, deploymentName, replicas); err != nil {
		return err
	}
	if err := r.Resources.EnsureDeploymentRolloutComplete(ctx, scope, deploymentName, replicas); err != nil {
		return err
	}

	// Reconcile runtime resources first so dependent services do not observe a
	// new manager port before the manager service/config have converged.
	if err := common.EnsureBuiltinTemplates(ctx, compiledPlan.BuiltinTemplates(), common.BuiltinTemplateOptions{
		DatabaseURL:          config.DatabaseURL,
		DatabaseMaxConns:     config.DatabaseMaxConns,
		DatabaseMinConns:     config.DatabaseMinConns,
		TemplateStoreEnabled: config.TemplateStoreEnabled,
		Owner:                "manager",
		MemoryPerCPU:         common.TemplateMemoryPerCPUFromManagerConfig(config),
	}); err != nil {
		return err
	}

	logger.Info("Manager reconciled successfully")
	return nil
}

func validateManagerServicePorts(ports []corev1.ServicePort) error {
	seen := make(map[int32]string, len(ports))
	for _, servicePort := range ports {
		if previous, exists := seen[servicePort.Port]; exists {
			return fmt.Errorf("manager Service port %d is used by both %s and %s", servicePort.Port, previous, servicePort.Name)
		}
		seen[servicePort.Port] = servicePort.Name
	}
	return nil
}

func resolveStorageRuntimeHTTPPort(requested int32, reserved ...int32) (int, error) {
	if requested < 1 || requested > 65535 {
		return 0, fmt.Errorf("storage runtime HTTP port %d is outside 1-65535", requested)
	}
	available := func(candidate int32) bool {
		for _, port := range reserved {
			if candidate == port {
				return false
			}
		}
		return true
	}
	if available(requested) {
		return int(requested), nil
	}
	for candidate := storageRuntimeFallbackPort; candidate <= 65535; candidate++ {
		if available(candidate) {
			return int(candidate), nil
		}
	}
	for candidate := int32(1024); candidate < storageRuntimeFallbackPort; candidate++ {
		if available(candidate) {
			return int(candidate), nil
		}
	}
	return 0, fmt.Errorf("no available HTTP port for manager storage runtime")
}

func (r *Reconciler) buildConfig(ctx context.Context, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) (*apiconfig.ManagerConfig, error) {
	cfg := &apiconfig.ManagerConfig{}
	if compiledPlan == nil {
		return nil, fmt.Errorf("compiled plan is required")
	}
	if compiledPlan.Manager.Config != nil {
		cfg = compiledPlan.Manager.Config.DeepCopy()
	}

	if dsn, err := compiledPlan.DatabaseDSN(ctx, r.Resources.Client); err == nil {
		cfg.DatabaseURL = dsn
	}
	if rootFSObjectStorage, err := compiledPlan.RootFSObjectStorage(ctx, r.Resources.Client); err != nil {
		return nil, fmt.Errorf("resolve rootfs object storage config: %w", err)
	} else if rootFSObjectStorage != nil {
		cfg.RootFSObjectStorage = *rootFSObjectStorage
	}

	if cfg.NetworkPolicyProvider == "netd" {
		secretName, err := netdservice.EnsureMITMCASecretWithScope(ctx, r.Resources, compiledPlan.Scope, compiledPlan, common.GetServiceLabels(compiledPlan.Scope.Name, "netd"))
		if err != nil {
			return nil, fmt.Errorf("ensure netd MITM CA secret: %w", err)
		}
		cfg.NetdMITMCASecretName = secretName
		cfg.NetdMITMCASecretNamespace = compiledPlan.Scope.Namespace
	}
	if err := credentialstoresvc.ApplyManagerCredentialStoreConfig(ctx, r.Resources, compiledPlan.Scope, cfg); err != nil {
		return nil, fmt.Errorf("apply credential store config: %w", err)
	}
	if owner := compiledPlan.Scope.Owner(); owner != nil {
		if err := meteringsvc.ApplyManagerConfig(ctx, r.Resources.Client, owner, cfg); err != nil {
			return nil, fmt.Errorf("apply metering config: %w", err)
		}
		if err := sandboxobssvc.ApplyManagerConfig(ctx, r.Resources.Client, owner, compiledPlan.Services.ClusterGateway.URL, cfg); err != nil {
			return nil, fmt.Errorf("apply sandbox observability config: %w", err)
		}
		redisCfg, ok, err := redissvc.GetGatewayRedisConfig(ctx, r.Resources.Client, owner)
		if err != nil {
			return nil, fmt.Errorf("resolve redis config: %w", err)
		}
		if ok {
			cfg.RedisURL = redisCfg.URL
			cfg.RedisKeyPrefix = redisCfg.KeyPrefix
			cfg.RedisTimeout = redisCfg.Timeout
		} else {
			cfg.RedisURL = ""
			cfg.RedisKeyPrefix = ""
			cfg.RedisTimeout = metav1.Duration{}
		}
	}

	cfg.ManagerImage = fmt.Sprintf("%s:%s", imageRepo, imageTag)
	if cfg.ProcdBinImageRef == "" {
		cfg.ProcdBinImageRef = fmt.Sprintf("%s:%s-procd-bin", imageRepo, imageTag)
	}

	return cfg, nil
}
