package storage

import (
	"context"
	"errors"
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

func TestIsBucketAlreadyOwnedError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "gcs already own",
			err:  errors.New("googleapi: Error 409: Your previous request to create the named bucket succeeded and you already own it., conflict"),
			want: true,
		},
		{
			name: "s3 already owned",
			err:  errors.New("BucketAlreadyOwnedByYou: The bucket you tried to create already exists, and you own it"),
			want: true,
		},
		{
			name: "permission denied",
			err:  errors.New("googleapi: Error 403: forbidden"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBucketAlreadyOwnedError(tt.err); got != tt.want {
				t.Fatalf("isBucketAlreadyOwnedError() = %v, want %v", got, tt.want)
			}
		})
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
