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
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/internalauth"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/storage"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/runtimeconfig"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

const (
	objectStorageTypeS3Compatible = "s3"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the storage-proxy deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string) error {
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
	if config.ObjectEncryptionEnabled {
		if err := common.EnsureObjectEncryptionKeySecret(ctx, r.Resources, infra); err != nil {
			return err
		}
		config.ObjectEncryptionKeyPath = common.ObjectEncryptionKeyPath
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

	var resources *corev1.ResourceRequirements
	serviceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		resources = infra.Spec.Services.StorageProxy.Resources
		serviceConfig = infra.Spec.Services.StorageProxy.Service
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "config",
			MountPath: "/config/config.yaml",
			SubPath:   "config.yaml",
			ReadOnly:  true,
		},
		{
			Name:      "internal-jwt-public-key",
			MountPath: pkginternalauth.DefaultInternalJWTPublicKeyPath,
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
	}
	if config.ObjectEncryptionEnabled {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "object-encryption-key",
			MountPath: common.ObjectEncryptionMountDir,
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "object-encryption-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: common.ObjectEncryptionSecretName(infra.Name),
					Items: []corev1.KeyToPath{
						{
							Key:  common.ObjectEncryptionSecretKey,
							Path: common.ObjectEncryptionKeyFilename,
						},
					},
				},
			},
		})
	}

	// Create deployment
	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:               "storage-proxy",
		Port:               httpPort,
		TargetPort:         httpPort,
		ServiceAccountName: fmt.Sprintf("%s-storage-proxy", infra.Name),
		Ports: []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: httpPort,
			},
			{
				Name:          "metrics",
				ContainerPort: metricsPort,
			},
		},
		Image: fmt.Sprintf("%s:%s", imageRepo, imageTag),
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

	// Create service.
	serviceType := common.ResolveServiceType(serviceConfig)
	httpServicePort := common.ResolveServicePort(serviceConfig, httpPort)
	serviceAnnotations := common.ResolveServiceAnnotations(serviceConfig)
	if err := r.Resources.ReconcileServicePorts(ctx, infra, serviceName, labels, serviceType, serviceAnnotations, []corev1.ServicePort{
		common.BuildServicePort("http", httpServicePort, httpPort, serviceType),
		common.BuildServicePort("metrics", metricsPort, metricsPort, serviceType),
	}); err != nil {
		return err
	}

	if err := r.Resources.EnsureDeploymentReady(ctx, infra, deploymentName, replicas); err != nil {
		return err
	}

	logger.Info("Storage proxy reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.StorageProxyConfig, error) {
	cfg := &apiconfig.StorageProxyConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		cfg = runtimeconfig.ToStorageProxy(infra.Spec.Services.StorageProxy.Config)
	}

	if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		cfg.DatabaseURL = dsn
	}

	metaURL, err := database.GetStorageMetadataDSN(ctx, r.Resources.Client, infra)
	if err != nil {
		return nil, err
	}
	cfg.MetaURL = metaURL
	if infra.Spec.Region != "" {
		cfg.RegionID = infra.Spec.Region
	}

	storageConfig, err := storage.GetStorageConfig(ctx, r.Resources.Client, infra)
	if err != nil {
		return nil, err
	}

	cfg.ObjectStorageType = normalizeObjectStorageType(storageConfig.Type)
	cfg.S3Bucket = storageConfig.Bucket
	cfg.S3Region = storageConfig.Region
	cfg.S3Endpoint = storageConfig.Endpoint
	cfg.S3AccessKey = storageConfig.AccessKey
	cfg.S3SecretKey = storageConfig.SecretKey
	cfg.S3SessionToken = storageConfig.SessionToken

	if infra.Spec.Cluster != nil && infra.Spec.Cluster.ID != "" {
		cfg.DefaultClusterId = infra.Spec.Cluster.ID
	}

	return cfg, nil
}

func normalizeObjectStorageType(storageType infrav1alpha1.StorageType) string {
	if storageType == infrav1alpha1.StorageTypeBuiltin {
		return objectStorageTypeS3Compatible
	}
	return string(storageType)
}
