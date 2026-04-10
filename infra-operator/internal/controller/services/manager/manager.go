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
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/internalauth"
	netdservice "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/netd"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/registry"
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
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}

	// Skip if not enabled
	if !compiledPlan.Manager.Enabled {
		logger.Info("Manager is disabled, skipping")
		return nil
	}

	deploymentName := fmt.Sprintf("%s-manager", infra.Name)
	replicas := compiledPlan.Manager.Replicas

	labels := common.GetServiceLabels(infra.Name, "manager")
	keySecretName, privateKeyKey, publicKeyKey := internalauth.GetDataPlaneKeyRefs(infra)

	config, err := r.buildConfig(ctx, infra, imageRepo, imageTag, compiledPlan)
	if err != nil {
		return err
	}
	podAnnotations, err := common.ConfigHashAnnotation(config)
	if err != nil {
		return err
	}

	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, deploymentName, labels, config); err != nil {
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
	if err := r.Resources.ReconcileServicePorts(ctx, infra, deploymentName, labels, serviceType, serviceAnnotations, []corev1.ServicePort{
		common.BuildServicePort("http", servicePort, httpPort, serviceType),
		common.BuildServicePort("metrics", metricsPort, metricsPort, serviceType),
		common.BuildServicePort("webhook", webhookPort, webhookPort, serviceType),
	}); err != nil {
		return err
	}

	if err := r.Resources.EnsureDeploymentReady(ctx, infra, deploymentName, replicas); err != nil {
		return err
	}

	// Reconcile runtime resources first so dependent services do not observe a
	// new manager port before the manager service/config have converged.
	if err := common.EnsureBuiltinTemplates(ctx, infra, common.BuiltinTemplateOptions{
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

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) (*apiconfig.ManagerConfig, error) {
	cfg := &apiconfig.ManagerConfig{}
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}
	if compiledPlan.Manager.Config != nil {
		cfg = compiledPlan.Manager.Config.DeepCopy()
	}

	if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		cfg.DatabaseURL = dsn
	}

	if cfg.NetworkPolicyProvider == "netd" {
		secretName, err := netdservice.EnsureMITMCASecret(ctx, r.Resources, infra, common.GetServiceLabels(infra.Name, "netd"))
		if err != nil {
			return nil, fmt.Errorf("ensure netd MITM CA secret: %w", err)
		}
		cfg.NetdMITMCASecretName = secretName
		cfg.NetdMITMCASecretNamespace = infra.Namespace
	}

	cfg.ManagerImage = fmt.Sprintf("%s:%s", imageRepo, imageTag)

	return cfg, nil
}
