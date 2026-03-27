package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/registry"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/storage"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestCleanupDisabledServiceResourcesCleansBuiltinDependencies(t *testing.T) {
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
					Port:     5432,
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

	reconciler, client, scheme := newCleanupTestReconciler(t,
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-postgres", Namespace: "sandbox0-system"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "demo-postgres", Namespace: "sandbox0-system"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-sandbox0-database-credentials", Namespace: "sandbox0-system"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-postgres-data", Namespace: "sandbox0-system"}},
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-rustfs", Namespace: "sandbox0-system"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "demo-rustfs", Namespace: "sandbox0-system"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-sandbox0-rustfs-credentials", Namespace: "sandbox0-system"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-rustfs-data", Namespace: "sandbox0-system"}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry", Namespace: "sandbox0-system"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry", Namespace: "sandbox0-system"}},
		&networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry", Namespace: "sandbox0-system"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry-auth", Namespace: "sandbox0-system"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry-pull", Namespace: "sandbox0-system"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry-data", Namespace: "sandbox0-system"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "harbor-pull", Namespace: "sandbox0-system"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "harbor-credentials", Namespace: "sandbox0-system"}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "demo-manager", Namespace: "sandbox0-system"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "demo-manager", Namespace: "sandbox0-system"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "demo-manager", Namespace: "sandbox0-system"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "demo-manager", Namespace: "sandbox0-system"}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "demo-manager"}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "demo-manager"}},
	)

	resources := common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{})
	err := reconciler.cleanupDisabledServiceResources(
		context.Background(),
		infra,
		infraplan.ComponentPlan{},
		database.NewReconciler(resources),
		storage.NewReconciler(resources),
		registry.NewReconciler(resources),
	)
	if err != nil {
		t.Fatalf("cleanup disabled service resources: %v", err)
	}

	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-postgres"}, &appsv1.StatefulSet{})
	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-postgres"}, &corev1.Service{})
	assertClientObjectPresent(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-sandbox0-database-credentials"}, &corev1.Secret{})
	assertClientObjectPresent(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-postgres-data"}, &corev1.PersistentVolumeClaim{})

	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-rustfs"}, &appsv1.StatefulSet{})
	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-rustfs"}, &corev1.Service{})
	assertClientObjectPresent(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-sandbox0-rustfs-credentials"}, &corev1.Secret{})
	assertClientObjectPresent(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-rustfs-data"}, &corev1.PersistentVolumeClaim{})

	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-registry"}, &appsv1.Deployment{})
	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-registry"}, &corev1.Service{})
	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-registry"}, &networkingv1.Ingress{})
	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-registry-auth"}, &corev1.Secret{})
	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-registry-pull"}, &corev1.Secret{})
	assertClientObjectPresent(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-registry-data"}, &corev1.PersistentVolumeClaim{})
	assertClientObjectPresent(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "harbor-pull"}, &corev1.Secret{})
	assertClientObjectPresent(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "harbor-credentials"}, &corev1.Secret{})

	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-manager"}, &appsv1.Deployment{})
	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-manager"}, &corev1.Service{})
	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-manager"}, &corev1.ConfigMap{})
	assertClientObjectMissing(t, client, types.NamespacedName{Namespace: "sandbox0-system", Name: "demo-manager"}, &corev1.ServiceAccount{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: "demo-manager"}, &rbacv1.ClusterRole{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: "demo-manager"}, &rbacv1.ClusterRoleBinding{})
}

func newCleanupTestReconciler(t *testing.T, objects ...ctrlclient.Object) (*Sandbox0InfraReconciler, ctrlclient.Client, *runtime.Scheme) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add networking scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rbac scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()

	return &Sandbox0InfraReconciler{
		Client: client,
		Scheme: scheme,
	}, client, scheme
}

func assertClientObjectPresent(t *testing.T, client ctrlclient.Client, key types.NamespacedName, obj ctrlclient.Object) {
	t.Helper()
	if err := client.Get(context.Background(), key, obj); err != nil {
		t.Fatalf("expected %T %s to exist: %v", obj, key.String(), err)
	}
}

func assertClientObjectMissing(t *testing.T, client ctrlclient.Client, key types.NamespacedName, obj ctrlclient.Object) {
	t.Helper()
	if err := client.Get(context.Background(), key, obj); err == nil {
		t.Fatalf("expected %T %s to be deleted", obj, key.String())
	}
}
