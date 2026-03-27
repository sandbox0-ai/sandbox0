package registry

import (
	"context"
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCleanupBuiltinResourcesRespectsStatefulResourcePolicy(t *testing.T) {
	t.Run("retain keeps pvc and removes runtime secrets", func(t *testing.T) {
		reconciler, client := newRegistryLifecycleTestReconciler(t,
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry", Namespace: "sandbox0-system"}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry", Namespace: "sandbox0-system"}},
			&networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry", Namespace: "sandbox0-system"}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry-auth", Namespace: "sandbox0-system"}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry-pull", Namespace: "sandbox0-system"}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry-data", Namespace: "sandbox0-system"}},
		)

		err := reconciler.CleanupBuiltinResources(context.Background(), &infrav1alpha1.Sandbox0Infra{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
			Spec: infrav1alpha1.Sandbox0InfraSpec{
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
		})
		if err != nil {
			t.Fatalf("cleanup builtin resources: %v", err)
		}

		assertRegistryMissingObject(t, client, &appsv1.Deployment{}, "sandbox0-system", "demo-registry")
		assertRegistryMissingObject(t, client, &corev1.Service{}, "sandbox0-system", "demo-registry")
		assertRegistryMissingObject(t, client, &networkingv1.Ingress{}, "sandbox0-system", "demo-registry")
		assertRegistryMissingObject(t, client, &corev1.Secret{}, "sandbox0-system", "demo-registry-auth")
		assertRegistryMissingObject(t, client, &corev1.Secret{}, "sandbox0-system", "demo-registry-pull")
		assertRegistryPresentObject(t, client, &corev1.PersistentVolumeClaim{}, "sandbox0-system", "demo-registry-data")
	})

	t.Run("delete removes pvc too", func(t *testing.T) {
		reconciler, client := newRegistryLifecycleTestReconciler(t,
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry", Namespace: "sandbox0-system"}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry", Namespace: "sandbox0-system"}},
			&networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry", Namespace: "sandbox0-system"}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry-auth", Namespace: "sandbox0-system"}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry-pull", Namespace: "sandbox0-system"}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-registry-data", Namespace: "sandbox0-system"}},
		)

		err := reconciler.CleanupBuiltinResources(context.Background(), &infrav1alpha1.Sandbox0Infra{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Registry: &infrav1alpha1.RegistryConfig{
					Provider: infrav1alpha1.RegistryProviderHarbor,
					Builtin: &infrav1alpha1.BuiltinRegistryConfig{
						StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyDelete,
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
		})
		if err != nil {
			t.Fatalf("cleanup builtin resources: %v", err)
		}

		assertRegistryMissingObject(t, client, &appsv1.Deployment{}, "sandbox0-system", "demo-registry")
		assertRegistryMissingObject(t, client, &corev1.Service{}, "sandbox0-system", "demo-registry")
		assertRegistryMissingObject(t, client, &networkingv1.Ingress{}, "sandbox0-system", "demo-registry")
		assertRegistryMissingObject(t, client, &corev1.Secret{}, "sandbox0-system", "demo-registry-auth")
		assertRegistryMissingObject(t, client, &corev1.Secret{}, "sandbox0-system", "demo-registry-pull")
		assertRegistryMissingObject(t, client, &corev1.PersistentVolumeClaim{}, "sandbox0-system", "demo-registry-data")
	})
}

func newRegistryLifecycleTestReconciler(t *testing.T, objects ...runtime.Object) (*Reconciler, ctrlclient.Client) {
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
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	return NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{})), client
}

func assertRegistryPresentObject(t *testing.T, client ctrlclient.Client, obj runtime.Object, namespace, name string) {
	t.Helper()
	switch typed := obj.(type) {
	case *corev1.PersistentVolumeClaim:
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, typed); err != nil {
			t.Fatalf("expected %T %s/%s to exist: %v", typed, namespace, name, err)
		}
	default:
		t.Fatalf("unsupported object type %T", obj)
	}
}

func assertRegistryMissingObject(t *testing.T, client ctrlclient.Client, obj runtime.Object, namespace, name string) {
	t.Helper()
	switch typed := obj.(type) {
	case *appsv1.Deployment:
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, typed); err == nil {
			t.Fatalf("expected %T %s/%s to be deleted", typed, namespace, name)
		}
	case *corev1.Service:
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, typed); err == nil {
			t.Fatalf("expected %T %s/%s to be deleted", typed, namespace, name)
		}
	case *networkingv1.Ingress:
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
