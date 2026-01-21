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

package storageproxy

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/infra/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/infra/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/internalauth"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/storage"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the storage-proxy deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo string) error {
	logger := log.FromContext(ctx)

	// Skip if not enabled
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil && !infra.Spec.Services.StorageProxy.Enabled {
		logger.Info("Storage proxy is disabled, skipping")
		return nil
	}

	deploymentName := fmt.Sprintf("%s-storage-proxy", infra.Name)
	serviceName := deploymentName

	replicas := int32(1)
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		replicas = infra.Spec.Services.StorageProxy.Replicas
	}

	labels := common.GetServiceLabels(infra.Name, "storage-proxy")
	keySecretName, _, publicKeyKey := internalauth.GetDataPlaneKeyRefs(infra)

	config, err := r.buildConfig(ctx, infra)
	if err != nil {
		return err
	}
	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, deploymentName, labels, config); err != nil {
		return err
	}

	// Create deployment
	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:               "storage-proxy",
		Port:               8080,
		TargetPort:         8080,
		ServiceAccountName: fmt.Sprintf("%s-storage-proxy", infra.Name),
		Ports: []corev1.ContainerPort{
			{
				Name:          "grpc",
				ContainerPort: 8080,
			},
			{
				Name:          "http",
				ContainerPort: 8081,
			},
			{
				Name:          "metrics",
				ContainerPort: 9090,
			},
		},
		Image: fmt.Sprintf("%s:%s", imageRepo, infra.Spec.Version),
		EnvVars: []corev1.EnvVar{
			{
				Name:  "SERVICE",
				Value: "storage-proxy",
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
				Name:      "internal-jwt-public-key",
				MountPath: "/config/internal_jwt_public.key",
				SubPath:   "internal_jwt_public.key",
				ReadOnly:  true,
			},
			{
				Name:      "cache",
				MountPath: "/var/lib/storage-proxy/cache",
			},
			{
				Name:      "logs",
				MountPath: "/var/log/storage-proxy",
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
			{
				Name: "cache",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
			{
				Name: "logs",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
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
	}); err != nil {
		return err
	}

	// Create service (gRPC)
	if err := r.Resources.ReconcileService(ctx, infra, serviceName, labels, corev1.ServiceTypeClusterIP, 8080, 8080); err != nil {
		return err
	}
	if err := r.Resources.ReconcileService(ctx, infra, fmt.Sprintf("%s-http", serviceName), labels, corev1.ServiceTypeClusterIP, 8081, 8081); err != nil {
		return err
	}
	if err := r.Resources.ReconcileService(ctx, infra, fmt.Sprintf("%s-metrics", serviceName), labels, corev1.ServiceTypeClusterIP, 9090, 9090); err != nil {
		return err
	}

	logger.Info("Storage proxy reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.StorageProxyConfig, error) {
	var raw *runtime.RawExtension
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		raw = infra.Spec.Services.StorageProxy.Config
	}

	cfg := apiconfig.DefaultStorageProxyConfig()
	if err := common.DecodeServiceConfig(raw, cfg); err != nil {
		return nil, err
	}

	if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		if cfg.DatabaseURL == "" {
			cfg.DatabaseURL = dsn
		}
	}

	metaURL, err := database.GetJuicefsMetaURL(ctx, r.Resources.Client, infra)
	if err != nil {
		return nil, err
	}
	if cfg.MetaURL == "" {
		cfg.MetaURL = metaURL
	}

	storageConfig, err := storage.GetStorageConfig(ctx, r.Resources.Client, infra)
	if err != nil {
		return nil, err
	}

	if cfg.S3Bucket == "" {
		cfg.S3Bucket = storageConfig.Bucket
	}
	if cfg.S3Region == "" {
		cfg.S3Region = storageConfig.Region
	}
	if cfg.S3Endpoint == "" {
		cfg.S3Endpoint = storageConfig.Endpoint
	}
	if cfg.S3AccessKey == "" {
		cfg.S3AccessKey = storageConfig.AccessKey
	}
	if cfg.S3SecretKey == "" {
		cfg.S3SecretKey = storageConfig.SecretKey
	}
	if storageConfig.SessionToken != "" {
		if cfg.S3SessionToken == "" {
			cfg.S3SessionToken = storageConfig.SessionToken
		}
	}

	if infra.Spec.Cluster != nil && infra.Spec.Cluster.ID != "" {
		if cfg.DefaultClusterId == "" {
			cfg.DefaultClusterId = infra.Spec.Cluster.ID
		}
	}

	return cfg, nil
}
