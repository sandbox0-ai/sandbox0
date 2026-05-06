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
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	netdservice "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/netd"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
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
func (r *Reconciler) Reconcile(ctx context.Context, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		return fmt.Errorf("compiled plan is required")
	}

	// Skip if not enabled
	if !compiledPlan.Manager.Enabled {
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
	podAnnotations, err := common.ConfigHashAnnotation(config)
	if err != nil {
		return err
	}

	if err := r.Resources.ReconcileServiceConfigMapWithScope(ctx, scope, deploymentName, labels, config); err != nil {
		return err
	}

	httpPort := int32(config.HTTPPort)
	metricsPort := int32(config.MetricsPort)
	webhookPort := int32(config.WebhookPort)

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
	envVars := []corev1.EnvVar{
		{
			Name:  "SERVICE",
			Value: "manager",
		},
		{
			Name:  "CONFIG_PATH",
			Value: "/config/config.yaml",
		},
	}
	envVars = append(envVars, compiledPlan.ObservabilityEnvVars()...)

	// Create deployment
	if err := r.Resources.ReconcileDeploymentWithScope(ctx, scope, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:               "manager",
		Port:               httpPort,
		TargetPort:         httpPort,
		ServiceAccountName: fmt.Sprintf("%s-manager", scope.Name),
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
	if err := r.Resources.ReconcileServicePortsWithScope(ctx, scope, deploymentName, labels, serviceType, serviceAnnotations, []corev1.ServicePort{
		common.BuildServicePort("http", servicePort, httpPort, serviceType),
		common.BuildServicePort("metrics", metricsPort, metricsPort, serviceType),
		common.BuildServicePort("webhook", webhookPort, webhookPort, serviceType),
	}); err != nil {
		return err
	}

	if err := r.Resources.EnsureDeploymentReadyWithScope(ctx, scope, deploymentName, replicas); err != nil {
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
	}); err != nil {
		return err
	}

	logger.Info("Manager reconciled successfully")
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

	if cfg.NetworkPolicyProvider == "netd" {
		secretName, err := netdservice.EnsureMITMCASecretWithScope(ctx, r.Resources, compiledPlan.Scope, compiledPlan, common.GetServiceLabels(compiledPlan.Scope.Name, "netd"))
		if err != nil {
			return nil, fmt.Errorf("ensure netd MITM CA secret: %w", err)
		}
		cfg.NetdMITMCASecretName = secretName
		cfg.NetdMITMCASecretNamespace = compiledPlan.Scope.Namespace
	}

	cfg.ManagerImage = fmt.Sprintf("%s:%s", imageRepo, imageTag)

	return cfg, nil
}
