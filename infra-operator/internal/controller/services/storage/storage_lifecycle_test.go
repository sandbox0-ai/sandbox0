package storage

import (
	"context"
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCleanupBuiltinResourcesRespectsStatefulResourcePolicy(t *testing.T) {
	t.Run("retain keeps pvc and secret", func(t *testing.T) {
		reconciler, client := newStorageLifecycleTestReconciler(t,
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-rustfs", Namespace: "sandbox0-system"}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "demo-rustfs", Namespace: "sandbox0-system"}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-sandbox0-rustfs-credentials", Namespace: "sandbox0-system"}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-rustfs-data", Namespace: "sandbox0-system"}},
		)

		err := reconciler.CleanupBuiltinResources(context.Background(), &infrav1alpha1.Sandbox0Infra{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Storage: &infrav1alpha1.StorageConfig{
					Type: infrav1alpha1.StorageTypeS3,
					Builtin: &infrav1alpha1.BuiltinStorageConfig{
						StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
					},
					S3: &infrav1alpha1.S3StorageConfig{
						Endpoint:          "https://s3.example.com",
						Bucket:            "sandbox0",
						Region:            "us-east-1",
						CredentialsSecret: infrav1alpha1.S3CredentialsSecret{Name: "s3-credentials"},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("cleanup builtin resources: %v", err)
		}

		assertStorageMissingObject(t, client, &appsv1.StatefulSet{}, "sandbox0-system", "demo-rustfs")
		assertStorageMissingObject(t, client, &corev1.Service{}, "sandbox0-system", "demo-rustfs")
		assertStoragePresentObject(t, client, &corev1.Secret{}, "sandbox0-system", "demo-sandbox0-rustfs-credentials")
		assertStoragePresentObject(t, client, &corev1.PersistentVolumeClaim{}, "sandbox0-system", "demo-rustfs-data")
	})

	t.Run("delete removes pvc and secret", func(t *testing.T) {
		reconciler, client := newStorageLifecycleTestReconciler(t,
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-rustfs", Namespace: "sandbox0-system"}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "demo-rustfs", Namespace: "sandbox0-system"}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-sandbox0-rustfs-credentials", Namespace: "sandbox0-system"}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-rustfs-data", Namespace: "sandbox0-system"}},
		)

		err := reconciler.CleanupBuiltinResources(context.Background(), &infrav1alpha1.Sandbox0Infra{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Storage: &infrav1alpha1.StorageConfig{
					Type: infrav1alpha1.StorageTypeS3,
					Builtin: &infrav1alpha1.BuiltinStorageConfig{
						StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyDelete,
					},
					S3: &infrav1alpha1.S3StorageConfig{
						Endpoint:          "https://s3.example.com",
						Bucket:            "sandbox0",
						Region:            "us-east-1",
						CredentialsSecret: infrav1alpha1.S3CredentialsSecret{Name: "s3-credentials"},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("cleanup builtin resources: %v", err)
		}

		assertStorageMissingObject(t, client, &appsv1.StatefulSet{}, "sandbox0-system", "demo-rustfs")
		assertStorageMissingObject(t, client, &corev1.Service{}, "sandbox0-system", "demo-rustfs")
		assertStorageMissingObject(t, client, &corev1.Secret{}, "sandbox0-system", "demo-sandbox0-rustfs-credentials")
		assertStorageMissingObject(t, client, &corev1.PersistentVolumeClaim{}, "sandbox0-system", "demo-rustfs-data")
	})
}

func newStorageLifecycleTestReconciler(t *testing.T, objects ...runtime.Object) (*Reconciler, ctrlclient.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	return NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{})), client
}

func assertStoragePresentObject(t *testing.T, client ctrlclient.Client, obj runtime.Object, namespace, name string) {
	t.Helper()
	switch typed := obj.(type) {
	case *corev1.Secret:
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, typed); err != nil {
			t.Fatalf("expected %T %s/%s to exist: %v", typed, namespace, name, err)
		}
	case *corev1.PersistentVolumeClaim:
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, typed); err != nil {
			t.Fatalf("expected %T %s/%s to exist: %v", typed, namespace, name, err)
		}
	default:
		t.Fatalf("unsupported object type %T", obj)
	}
}

func assertStorageMissingObject(t *testing.T, client ctrlclient.Client, obj runtime.Object, namespace, name string) {
	t.Helper()
	switch typed := obj.(type) {
	case *appsv1.StatefulSet:
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, typed); err == nil {
			t.Fatalf("expected %T %s/%s to be deleted", typed, namespace, name)
		}
	case *corev1.Service:
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, typed); err == nil {
			t.Fatalf("expected %T %s/%s to be deleted", typed, namespace, name)
		}
	case *corev1.Secret:
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, typed); err == nil {
			t.Fatalf("expected %T %s/%s to be deleted", typed, namespace, name)
		}
	case *corev1.PersistentVolumeClaim:
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, typed); err == nil {
			t.Fatalf("expected %T %s/%s to be deleted", typed, namespace, name)
		}
	default:
		t.Fatalf("unsupported object type %T", obj)
	}
}
