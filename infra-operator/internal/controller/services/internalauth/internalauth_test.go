package internalauth

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

func TestReconcileCreatesSeparatedAuditProducerAndSigningKeys(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
				WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true}},
			}},
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Type:  infrav1alpha1.SandboxObservabilityTypeExternal,
				Audit: &infrav1alpha1.SandboxObservabilityAuditConfig{Enabled: true},
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infra.DeepCopy()).Build()
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), infra); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	producerName, _, _ := GetNetworkAuditKeyRefs(infra)
	signingName, _, _ := GetAuditSigningKeyRefs(infra)
	if producerName == signingName {
		t.Fatal("producer and signing key secrets must be distinct")
	}
	for _, name := range []string{producerName, signingName} {
		secret := &corev1.Secret{}
		if err := client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: infra.Namespace}, secret); err != nil {
			t.Fatalf("get secret %q: %v", name, err)
		}
		if len(secret.Data["private.key"]) == 0 || len(secret.Data["public.key"]) == 0 {
			t.Fatalf("secret %q does not contain a complete key pair", name)
		}
	}
}

func TestReconcileMigratesLegacyNetworkAuditKey(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
				WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true}},
			}},
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Type:  infrav1alpha1.SandboxObservabilityTypeExternal,
				Audit: &infrav1alpha1.SandboxObservabilityAuditConfig{Enabled: true},
			},
		},
	}
	legacyData := map[string][]byte{
		"private.key": []byte("legacy-private"),
		"public.key":  []byte("legacy-public"),
	}
	legacy := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-" + legacyNetworkAuditKeySecretName,
			Namespace: infra.Namespace,
		},
		Data: legacyData,
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infra.DeepCopy(), legacy).Build()
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), infra); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	name, _, _ := GetNetworkAuditKeyRefs(infra)
	migrated := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: infra.Namespace}, migrated); err != nil {
		t.Fatalf("get migrated network audit key: %v", err)
	}
	if string(migrated.Data["private.key"]) != string(legacyData["private.key"]) || string(migrated.Data["public.key"]) != string(legacyData["public.key"]) {
		t.Fatal("migrated network audit key did not preserve the legacy key pair")
	}
}
