package storageproxy

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

func TestBuildRuntimeConfigMapsBuiltinStorageAndDefaultsIdentity(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "fullmode", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			PublicExposure: &infrav1alpha1.PublicExposureConfig{RegionID: "aws-us-east-1"},
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					Enabled: true, Port: 5432, Username: "sandbox0", Database: "sandbox0", SSLMode: "disable",
				},
			},
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinStorageConfig{
					Enabled: true, Bucket: "sandbox0", Region: "us-east-1",
				},
			},
		},
	}
	resources := newStorageRuntimeTestResources(t,
		infra.DeepCopy(),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "fullmode-sandbox0-database-credentials", Namespace: infra.Namespace},
			Data: map[string][]byte{
				"username": []byte("sandbox0"),
				"password": []byte("db-password"),
				"database": []byte("sandbox0"),
				"port":     []byte("5432"),
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "fullmode-sandbox0-rustfs-credentials", Namespace: infra.Namespace},
			Data: map[string][]byte{
				"endpoint":          []byte("http://fullmode-rustfs.sandbox0-system.svc:9000"),
				"RUSTFS_ACCESS_KEY": []byte("access-key"),
				"RUSTFS_SECRET_KEY": []byte("secret-key"),
			},
		},
	)

	cfg, err := BuildRuntimeConfig(context.Background(), resources, infra)
	if err != nil {
		t.Fatalf("BuildRuntimeConfig() error = %v", err)
	}
	if cfg.ObjectStorageType != "s3" {
		t.Fatalf("object storage type = %q, want s3", cfg.ObjectStorageType)
	}
	if cfg.S3Endpoint != "http://fullmode-rustfs.sandbox0-system.svc:9000" {
		t.Fatalf("S3 endpoint = %q", cfg.S3Endpoint)
	}
	if cfg.RegionID != "aws-us-east-1" {
		t.Fatalf("region ID = %q, want aws-us-east-1", cfg.RegionID)
	}
	if cfg.DefaultClusterId != naming.DefaultClusterID {
		t.Fatalf("cluster ID = %q, want %q", cfg.DefaultClusterId, naming.DefaultClusterID)
	}
}

func TestBuildRuntimeVolumesSetsLimitsAndEncryptionMount(t *testing.T) {
	cfg := &apiconfig.StorageProxyConfig{
		CacheSizeLimit:          "512Mi",
		LogSizeLimit:            "64Mi",
		ObjectEncryptionEnabled: true,
	}
	scope := common.ObjectScope{Name: "demo", Namespace: "sandbox0-system"}
	mounts, volumes, err := BuildRuntimeVolumes(scope, cfg, RuntimeVolumeOptions{
		ConfigMapName:    "demo-manager-storage-config",
		ConfigVolumeName: "storage-config",
		ConfigMountPath:  "/config/storage-proxy.yaml",
		CacheVolumeName:  "storage-cache",
		LogVolumeName:    "storage-logs",
	})
	if err != nil {
		t.Fatalf("BuildRuntimeVolumes() error = %v", err)
	}
	assertVolumeMount(t, mounts, "storage-config", "/config/storage-proxy.yaml")
	assertVolumeMount(t, mounts, "storage-cache", "/var/lib/storage-proxy/cache")
	assertVolumeMount(t, mounts, "storage-logs", "/var/log/storage-proxy")
	assertVolumeMount(t, mounts, "object-encryption-key", common.ObjectEncryptionMountDir)
	assertEmptyDirLimit(t, volumes, "storage-cache", "512Mi")
	assertEmptyDirLimit(t, volumes, "storage-logs", "64Mi")
	assertVolume(t, volumes, "object-encryption-key")
}

func newStorageRuntimeTestResources(t *testing.T, objects ...runtime.Object) *common.ResourceManager {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	return common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{})
}

func assertVolumeMount(t *testing.T, mounts []corev1.VolumeMount, name, path string) {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name == name {
			if mount.MountPath != path {
				t.Fatalf("mount %q path = %q, want %q", name, mount.MountPath, path)
			}
			return
		}
	}
	t.Fatalf("mount %q not found", name)
}

func assertVolume(t *testing.T, volumes []corev1.Volume, name string) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			return
		}
	}
	t.Fatalf("volume %q not found", name)
}

func assertEmptyDirLimit(t *testing.T, volumes []corev1.Volume, name, want string) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name != name {
			continue
		}
		if volume.EmptyDir == nil || volume.EmptyDir.SizeLimit == nil {
			t.Fatalf("volume %q has no emptyDir size limit", name)
		}
		if got := volume.EmptyDir.SizeLimit.String(); got != want {
			t.Fatalf("volume %q size limit = %q, want %q", name, got, want)
		}
		return
	}
	t.Fatalf("volume %q not found", name)
}
