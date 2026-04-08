package storage

import (
	"context"
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestValidateExternalStorageAllowsGCSWithoutSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeGCS,
				GCS: &infrav1alpha1.GCSStorageConfig{
					Bucket: "sandbox0-bucket",
				},
			},
		},
	}

	if err := ValidateExternalStorage(context.Background(), client, infra); err != nil {
		t.Fatalf("ValidateExternalStorage returned error: %v", err)
	}
}

func TestGetStorageConfigReturnsGCSWithoutSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeGCS,
				GCS: &infrav1alpha1.GCSStorageConfig{
					Bucket: "sandbox0-bucket",
				},
			},
		},
	}

	cfg, err := GetStorageConfig(context.Background(), client, infra)
	if err != nil {
		t.Fatalf("GetStorageConfig returned error: %v", err)
	}
	if cfg.Type != infrav1alpha1.StorageTypeGCS {
		t.Fatalf("unexpected storage type: %s", cfg.Type)
	}
	if cfg.Bucket != "sandbox0-bucket" {
		t.Fatalf("unexpected bucket: %q", cfg.Bucket)
	}
	if cfg.SecretName != "" || cfg.AccessKey != "" || cfg.SecretKey != "" {
		t.Fatalf("expected no static credentials, got %#v", cfg)
	}
}
