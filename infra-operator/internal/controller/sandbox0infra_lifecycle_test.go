package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestCollectRetainedResources(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
				},
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeS3,
				Builtin: &infrav1alpha1.BuiltinStorageConfig{
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
				},
				S3: &infrav1alpha1.S3StorageConfig{
					Endpoint: "https://s3.example.com",
					Bucket:   "sandbox0",
					Region:   "us-east-1",
					CredentialsSecret: infrav1alpha1.S3CredentialsSecret{
						Name: "s3-credentials",
					},
				},
			},
			Registry: &infrav1alpha1.RegistryConfig{
				Provider: infrav1alpha1.RegistryProviderHarbor,
				Builtin: &infrav1alpha1.BuiltinRegistryConfig{
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
				},
				Harbor: &infrav1alpha1.HarborRegistryConfig{
					Registry:   "harbor.example.com",
					PullSecret: infrav1alpha1.DockerConfigSecretRef{Name: "harbor-pull"},
					CredentialsSecret: infrav1alpha1.HarborRegistryCredentialsSecret{
						Name: "harbor-credentials",
					},
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-sandbox0-database-credentials", Namespace: "sandbox0-system"}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-postgres-data", Namespace: "sandbox0-system"}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-sandbox0-rustfs-credentials", Namespace: "sandbox0-system"}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-rustfs-data", Namespace: "sandbox0-system"}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry-data", Namespace: "sandbox0-system"}},
		).
		Build()

	retained, err := collectRetainedResources(context.Background(), client, infra.Namespace, infraplan.Compile(infra).Status.RetainedResources)
	if err != nil {
		t.Fatalf("collect retained resources: %v", err)
	}
	if len(retained) != 5 {
		t.Fatalf("expected 5 retained resources, got %d: %#v", len(retained), retained)
	}
}

func TestCollectRetainedResourcesSkipsDeletePolicy(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyDelete,
				},
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
		},
	}

	client := newValidationTestClient(t,
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-sandbox0-database-credentials", Namespace: "sandbox0-system"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-postgres-data", Namespace: "sandbox0-system"}},
	)

	retained, err := collectRetainedResources(context.Background(), client, infra.Namespace, infraplan.Compile(infra).Status.RetainedResources)
	if err != nil {
		t.Fatalf("collect retained resources: %v", err)
	}
	if len(retained) != 0 {
		t.Fatalf("expected no retained resources, got %#v", retained)
	}
}

func TestRegisterClusterFailsFastUntilImplemented(t *testing.T) {
	reconciler := &Sandbox0InfraReconciler{}
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ControlPlane: &infrav1alpha1.ControlPlaneConfig{
				URL: "https://control-plane.example.com",
			},
			Cluster: &infrav1alpha1.ClusterConfig{
				ID: "cluster-a",
			},
		},
	}

	err := reconciler.registerCluster(context.Background(), infra)
	if err == nil {
		t.Fatal("expected cluster registration to fail fast")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("expected not implemented error, got %v", err)
	}
	if infra.Status.Cluster == nil {
		t.Fatal("expected cluster status to be initialized")
	}
	if infra.Status.Cluster.Registered {
		t.Fatal("expected cluster status to remain unregistered")
	}
	if infra.Status.Cluster.ID != "cluster-a" {
		t.Fatalf("expected cluster id to be preserved, got %q", infra.Status.Cluster.ID)
	}
}
