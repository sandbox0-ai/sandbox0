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

	apiconfig "github.com/sandbox0-ai/infra/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/infra/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/internalauth"
	pkginternalauth "github.com/sandbox0-ai/infra/pkg/internalauth"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the manager deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo string) error {
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

	config, err := r.buildConfig(ctx, infra, imageRepo)
	if err != nil {
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
		Image: fmt.Sprintf("%s:%s", imageRepo, infra.Spec.Version),
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
		VolumeMounts: []corev1.VolumeMount{
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
		},
		Volumes: []corev1.Volume{
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
	if err := r.Resources.ReconcileService(ctx, infra, deploymentName, labels, serviceType, servicePort, httpPort); err != nil {
		return err
	}
	if err := r.Resources.ReconcileService(ctx, infra, fmt.Sprintf("%s-metrics", deploymentName), labels, corev1.ServiceTypeClusterIP, metricsPort, metricsPort); err != nil {
		return err
	}
	if err := r.Resources.ReconcileService(ctx, infra, fmt.Sprintf("%s-webhook", deploymentName), labels, corev1.ServiceTypeClusterIP, webhookPort, webhookPort); err != nil {
		return err
	}

	logger.Info("Manager reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo string) (*apiconfig.ManagerConfig, error) {
	cfg := &apiconfig.ManagerConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.Manager != nil && infra.Spec.Services.Manager.Config != nil {
		cfg = infra.Spec.Services.Manager.Config
	}

	if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		if cfg.DatabaseURL == "" {
			cfg.DatabaseURL = dsn
		}
	}

	if cfg.DefaultTemplateNamespace == "" {
		cfg.DefaultTemplateNamespace = infra.Namespace
	}

	if infra.Spec.Cluster != nil && infra.Spec.Cluster.ID != "" {
		if cfg.DefaultClusterId == "" {
			cfg.DefaultClusterId = infra.Spec.Cluster.ID
		}
	}

	managerImage := fmt.Sprintf("%s:%s", imageRepo, infra.Spec.Version)
	if cfg.ManagerImage == "" {
		cfg.ManagerImage = managerImage
	}

	storageProxyConfig := &apiconfig.StorageProxyConfig{}
	storageProxyServiceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil && infra.Spec.Services.StorageProxy.Config != nil {
		storageProxyConfig = infra.Spec.Services.StorageProxy.Config
	}
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		storageProxyServiceConfig = infra.Spec.Services.StorageProxy.Service
	}

	if cfg.ProcdConfig.StorageProxyBaseURL == "" {
		cfg.ProcdConfig.StorageProxyBaseURL = fmt.Sprintf("%s-storage-proxy.%s.svc.cluster.local", infra.Name, infra.Namespace)
	}
	if cfg.ProcdConfig.StorageProxyPort == 0 {
		cfg.ProcdConfig.StorageProxyPort = int(common.ResolveServicePort(storageProxyServiceConfig, int32(storageProxyConfig.GRPCPort)))
	}

	return cfg, nil
}
