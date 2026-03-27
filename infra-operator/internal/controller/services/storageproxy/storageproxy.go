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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
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
	juicefsEncryptionSecretSuffix = "juicefs-encryption-key"
	juicefsEncryptionKeyFilename  = "juicefs_rsa_private.pem"
	juicefsEncryptionMountDir     = "/etc/storage-proxy/juicefs"
	juicefsEncryptionKeyPath      = "/etc/storage-proxy/juicefs/juicefs_rsa_private.pem"
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
	if config.JuiceFSEncryptionEnabled {
		secretName := fmt.Sprintf("%s-%s", infra.Name, juicefsEncryptionSecretSuffix)
		if err := r.ensureEncryptionKeySecret(ctx, infra, secretName); err != nil {
			return err
		}
		config.JuiceFSEncryptionKeyPath = juicefsEncryptionKeyPath
	}
	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, deploymentName, labels, config); err != nil {
		return err
	}

	grpcPort := int32(config.GRPCPort)
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
	if config.JuiceFSEncryptionEnabled {
		secretName := fmt.Sprintf("%s-%s", infra.Name, juicefsEncryptionSecretSuffix)
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "juicefs-encryption-key",
			MountPath: juicefsEncryptionMountDir,
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "juicefs-encryption-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: secretName,
					Items: []corev1.KeyToPath{
						{
							Key:  "private.key",
							Path: juicefsEncryptionKeyFilename,
						},
					},
				},
			},
		})
	}

	// Create deployment
	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:               "storage-proxy",
		Port:               grpcPort,
		TargetPort:         grpcPort,
		ServiceAccountName: fmt.Sprintf("%s-storage-proxy", infra.Name),
		Ports: []corev1.ContainerPort{
			{
				Name:          "grpc",
				ContainerPort: grpcPort,
			},
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

	// Create service (gRPC)
	serviceType := common.ResolveServiceType(serviceConfig)
	servicePort := common.ResolveServicePort(serviceConfig, grpcPort)
	if err := r.Resources.ReconcileServicePorts(ctx, infra, serviceName, labels, serviceType, []corev1.ServicePort{
		common.BuildServicePort("grpc", servicePort, grpcPort, serviceType),
		common.BuildServicePort("http", httpPort, httpPort, serviceType),
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

func (r *Reconciler) ensureEncryptionKeySecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, secretName string) error {
	logger := log.FromContext(ctx)
	secret := &corev1.Secret{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: infra.Namespace}, secret)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("generate RSA private key: %w", err)
	}
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: infra.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "storage-proxy",
				"app.kubernetes.io/instance":   infra.Name,
				"app.kubernetes.io/managed-by": "sandbox0infra-operator",
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"private.key": privateKeyPEM,
		},
	}
	if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
		return err
	}
	logger.Info("Creating JuiceFS encryption key secret", "secretName", secretName)
	return r.Resources.Client.Create(ctx, secret)
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.StorageProxyConfig, error) {
	cfg := &apiconfig.StorageProxyConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		cfg = runtimeconfig.ToStorageProxy(infra.Spec.Services.StorageProxy.Config)
	}

	if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		cfg.DatabaseURL = dsn
	}

	metaURL, err := database.GetJuicefsMetaURL(ctx, r.Resources.Client, infra)
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
