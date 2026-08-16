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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	ctldnetworkingassets "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/ctldnetworking"
	credentialstoresvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/credentialstore"
	meteringsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/metering"
	redissvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/redis"
	sandboxobssvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/sandboxobservability"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

const (
	registryCredentialsPath     = "/etc/sandbox0/registry/.dockerconfigjson"
	managerConfigHashAnnotation = "infra.sandbox0.ai/manager-config-hash"
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
	podAnnotations[managerConfigHashAnnotation] = configRef.Hash

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
	if config.RootFSObjectStorage.ObjectEncryptionEnabled {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "object-encryption-key",
			MountPath: common.ObjectEncryptionMountDir,
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "object-encryption-key",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: common.ObjectEncryptionSecretName(scope.Name),
				Items:      []corev1.KeyToPath{{Key: common.ObjectEncryptionSecretKey, Path: common.ObjectEncryptionKeyFilename}},
			}},
		})
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
		{Name: apiconfig.ManagerLeaderElectionNameEnv, Value: deploymentName},
	}
	maxSurge := intstr.FromInt32(0)
	maxUnavailable := intstr.FromInt32(1)

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
		// A no-surge rollout also protects the first upgrade from a manager
		// version that does not participate in leader election. Once all
		// replicas are leader-aware, the Lease remains the single-writer guard.
		DeploymentStrategy: &appsv1.DeploymentStrategy{
			Type: appsv1.RollingUpdateDeploymentStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDeployment{
				MaxSurge:       &maxSurge,
				MaxUnavailable: &maxUnavailable,
			},
		},
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
		ResourcePolicy:       common.TemplateResourcePolicyFromManagerConfig(config),
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
		if rootFSObjectStorage.ObjectEncryptionEnabled {
			owner := compiledPlan.Scope.Owner()
			if owner == nil {
				return nil, fmt.Errorf("infra owner is required for rootfs object encryption")
			}
			if err := common.EnsureObjectEncryptionKeySecret(ctx, r.Resources, owner); err != nil {
				return nil, err
			}
		}
	}

	if cfg.NetworkPolicyProvider == "ctld" {
		secretName, err := ctldnetworkingassets.EnsureMITMCASecretWithScope(ctx, r.Resources, compiledPlan.Scope, compiledPlan, common.GetServiceLabels(compiledPlan.Scope.Name, "ctld"))
		if err != nil {
			return nil, fmt.Errorf("ensure network-runtime MITM CA secret: %w", err)
		}
		cfg.NetworkMITMCASecretName = secretName
		cfg.NetworkMITMCASecretNamespace = compiledPlan.Scope.Namespace
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
