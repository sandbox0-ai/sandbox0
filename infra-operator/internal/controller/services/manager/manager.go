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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/infra/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/infra/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/internalauth"
	templatev1alpha1 "github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	pkginternalauth "github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"github.com/sandbox0-ai/infra/pkg/template"
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

	// Ensure namespace for the default template name.
	defaultTemplateNamespace, err := naming.TemplateNamespaceFromName(config.DefaultTemplate.Name)
	if err != nil {
		return fmt.Errorf("resolve default template namespace: %w", err)
	}
	if err := r.Resources.ReconcileNamespace(ctx, defaultTemplateNamespace); err != nil {
		return fmt.Errorf("reconcile namespace %s: %w", defaultTemplateNamespace, err)
	}

	if err := r.ensureDefaultTemplate(ctx, infra, config); err != nil {
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
	if err := r.Resources.ReconcileServicePorts(ctx, infra, deploymentName, labels, serviceType, []corev1.ServicePort{
		common.BuildServicePort("http", servicePort, httpPort),
		common.BuildServicePort("metrics", metricsPort, metricsPort),
		common.BuildServicePort("webhook", webhookPort, webhookPort),
	}); err != nil {
		return err
	}

	if err := r.Resources.EnsureDeploymentReady(ctx, infra, deploymentName, replicas); err != nil {
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
		cfg.DatabaseURL = dsn
	}

	cfg.TemplateStoreEnabled = !infrav1alpha1.IsSchedulerEnabled(infra)

	if cfg.DefaultTemplate == nil {
		cfg.DefaultTemplate = &apiconfig.DefaultTemplateConfig{}
	}
	if cfg.DefaultTemplate.Name == "" {
		cfg.DefaultTemplate.Name = template.DefaultTemplateName
	}
	if cfg.DefaultTemplate.Image == "" {
		cfg.DefaultTemplate.Image = template.DefaultTemplateImage
	}
	cfg.DefaultTemplate.Pool = applyDefaultTemplatePool(cfg.DefaultTemplate.Pool)

	if infra.Spec.Cluster != nil && infra.Spec.Cluster.ID != "" {
		cfg.DefaultClusterId = infra.Spec.Cluster.ID
	}

	cfg.ManagerImage = fmt.Sprintf("%s:%s", imageRepo, infra.Spec.Version)

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

	return cfg, nil
}

func (r *Reconciler) ensureDefaultTemplate(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, config *apiconfig.ManagerConfig) error {
	logger := log.FromContext(ctx)
	name := ""
	namespace := ""
	image := ""
	pool := apiconfig.DefaultTemplatePoolConfig{}
	if config.DefaultTemplate != nil {
		name = config.DefaultTemplate.Name
		image = config.DefaultTemplate.Image
		pool = config.DefaultTemplate.Pool
	}
	if name == "" {
		name = template.DefaultTemplateName
	}
	namespace, err := naming.TemplateNamespaceFromName(name)
	if err != nil {
		return fmt.Errorf("resolve template namespace: %w", err)
	}
	if image == "" {
		image = template.DefaultTemplateImage
	}
	pool = applyDefaultTemplatePool(pool)

	existing := &templatev1alpha1.SandboxTemplate{}
	err = r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, existing)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	template := &templatev1alpha1.SandboxTemplate{
		TypeMeta: metav1.TypeMeta{
			APIVersion: templatev1alpha1.SchemeGroupVersion.String(),
			Kind:       "SandboxTemplate",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "sandbox0infra-operator",
				"sandbox0.ai/template-role":    "default",
			},
			Annotations: map[string]string{
				"sandbox0.ai/infra-name":      infra.Name,
				"sandbox0.ai/infra-namespace": infra.Namespace,
			},
		},
		Spec: templatev1alpha1.SandboxTemplateSpec{
			DisplayName: template.DefaultTemplateDisplayName,
			Description: "Default template installed by infra-operator.",
			MainContainer: templatev1alpha1.ContainerSpec{
				Image: image,
				Resources: templatev1alpha1.ResourceQuota{
					CPU:    resource.MustParse(template.DefaultTemplateCPU),
					Memory: resource.MustParse(template.DefaultTemplateMemory),
				},
			},
			Pool: templatev1alpha1.PoolStrategy{
				MinIdle:   pool.MinIdle,
				MaxIdle:   pool.MaxIdle,
				AutoScale: pool.AutoScale,
			},
			Network: &templatev1alpha1.TplSandboxNetworkPolicy{
				Mode: templatev1alpha1.NetworkModeAllowAll,
			},
			Public: true,
		},
	}

	if config.DefaultClusterId != "" {
		template.Spec.ClusterId = &config.DefaultClusterId
	}

	if namespace == infra.Namespace {
		if err := controllerutil.SetControllerReference(infra, template, r.Resources.Scheme); err != nil {
			return err
		}
	}

	if err := r.Resources.Client.Create(ctx, template); err != nil {
		return err
	}

	logger.Info("Default template created", "name", name, "namespace", namespace)
	return nil
}

func applyDefaultTemplatePool(pool apiconfig.DefaultTemplatePoolConfig) apiconfig.DefaultTemplatePoolConfig {
	minIdle, maxIdle, autoScale := template.ApplyDefaultPool(pool.MinIdle, pool.MaxIdle, pool.AutoScale)
	pool.MinIdle = minIdle
	pool.MaxIdle = maxIdle
	pool.AutoScale = autoScale
	return pool
}
