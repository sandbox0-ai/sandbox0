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
	"k8s.io/apimachinery/pkg/api/resource"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	credentialstoresvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/credentialstore"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	meteringsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/metering"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/storage"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/runtimeconfig"
)

const (
	objectStorageTypeS3Compatible = "s3"
	defaultCacheSizeLimit         = "20Gi"
	defaultLogSizeLimit           = "1Gi"
)

// RuntimeVolumeOptions customizes storage runtime volume names when the
// runtime is embedded in another workload.
type RuntimeVolumeOptions struct {
	ConfigMapName          string
	ConfigVolumeName       string
	ConfigMountPath        string
	CacheVolumeName        string
	LogVolumeName          string
	IncludeCredentialStore bool
}

// BuildRuntimeConfig resolves the storage-proxy config without creating a
// workload. Manager embedding uses it while preserving the logical
// storage-proxy configuration contract.
func BuildRuntimeConfig(ctx context.Context, resources *common.ResourceManager, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.StorageProxyConfig, error) {
	if resources == nil {
		return nil, fmt.Errorf("resource manager is required")
	}
	if infra == nil {
		return nil, fmt.Errorf("infra is required")
	}
	cfg := &apiconfig.StorageProxyConfig{}
	if runtimeConfig := infrav1alpha1.ResolveStorageRuntimeConfig(infra); runtimeConfig != nil {
		cfg = runtimeconfig.ToStorageProxy(runtimeConfig)
	}

	if dsn, err := database.GetDatabaseDSN(ctx, resources.Client, infra); err == nil {
		cfg.DatabaseURL = dsn
	}

	metaURL, err := database.GetStorageMetadataDSN(ctx, resources.Client, infra)
	if err != nil {
		return nil, err
	}
	cfg.MetaURL = metaURL
	cfg.RegionID = common.ResolveRegionID(infra)
	if err := meteringsvc.ApplyStorageProxyConfig(ctx, resources.Client, infra, cfg); err != nil {
		return nil, fmt.Errorf("apply metering config: %w", err)
	}

	storageConfig, err := storage.GetStorageConfig(ctx, resources.Client, infra)
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

	cfg.DefaultClusterId = common.ResolveClusterID(infra)
	if cfg.ObjectEncryptionEnabled {
		if err := common.EnsureObjectEncryptionKeySecret(ctx, resources, infra); err != nil {
			return nil, err
		}
		cfg.ObjectEncryptionKeyPath = common.ObjectEncryptionKeyPath
	}
	if err := credentialstoresvc.ApplyEncryptedPGCredentialStoreConfig(ctx, resources, common.NewObjectScope(infra), &cfg.CredentialStore); err != nil {
		return nil, err
	}

	return cfg, nil
}

// BuildRuntimeVolumes returns the storage runtime's config, cache, log,
// credential, and encryption mounts without creating a workload.
func BuildRuntimeVolumes(scope common.ObjectScope, cfg *apiconfig.StorageProxyConfig, opts RuntimeVolumeOptions) ([]corev1.VolumeMount, []corev1.Volume, error) {
	if cfg == nil {
		return nil, nil, fmt.Errorf("storage-proxy config is required")
	}
	if opts.ConfigMapName == "" {
		return nil, nil, fmt.Errorf("storage-proxy config map name is required")
	}
	configVolumeName := valueOrDefault(opts.ConfigVolumeName, "config")
	configMountPath := valueOrDefault(opts.ConfigMountPath, "/config/config.yaml")
	cacheVolumeName := valueOrDefault(opts.CacheVolumeName, "cache")
	logVolumeName := valueOrDefault(opts.LogVolumeName, "logs")
	cacheSizeLimit, err := parseSizeLimit(cfg.CacheSizeLimit, defaultCacheSizeLimit, "storage-proxy cache size limit")
	if err != nil {
		return nil, nil, err
	}
	logSizeLimit, err := parseSizeLimit(cfg.LogSizeLimit, defaultLogSizeLimit, "storage-proxy log size limit")
	if err != nil {
		return nil, nil, err
	}

	mounts := []corev1.VolumeMount{
		{Name: configVolumeName, MountPath: configMountPath, SubPath: "config.yaml", ReadOnly: true},
		{Name: cacheVolumeName, MountPath: "/var/lib/storage-proxy/cache"},
		{Name: logVolumeName, MountPath: "/var/log/storage-proxy"},
	}
	volumes := []corev1.Volume{
		{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: opts.ConfigMapName},
			}},
		},
		{Name: cacheVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: cacheSizeLimit}}},
		{Name: logVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: logSizeLimit}}},
	}
	if opts.IncludeCredentialStore {
		credentialMounts, credentialVolumes := credentialstoresvc.CredentialStoreVolumes(scope, &cfg.CredentialStore)
		mounts = append(mounts, credentialMounts...)
		volumes = append(volumes, credentialVolumes...)
	}
	if cfg.ObjectEncryptionEnabled {
		mounts = append(mounts, corev1.VolumeMount{Name: "object-encryption-key", MountPath: common.ObjectEncryptionMountDir, ReadOnly: true})
		volumes = append(volumes, corev1.Volume{
			Name: "object-encryption-key",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: common.ObjectEncryptionSecretName(scope.Name),
				Items:      []corev1.KeyToPath{{Key: common.ObjectEncryptionSecretKey, Path: common.ObjectEncryptionKeyFilename}},
			}},
		})
	}
	return mounts, volumes, nil
}

func valueOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func normalizeObjectStorageType(storageType infrav1alpha1.StorageType) string {
	if storageType == infrav1alpha1.StorageTypeBuiltin {
		return objectStorageTypeS3Compatible
	}
	return string(storageType)
}

func parseSizeLimit(value, fallback, field string) (*resource.Quantity, error) {
	if value == "" {
		value = fallback
	}
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", field, err)
	}
	if quantity.Sign() <= 0 {
		return nil, fmt.Errorf("%s must be > 0", field)
	}
	return &quantity, nil
}
