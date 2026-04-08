package webhook

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

func TestReconcileCertSecretCreatesTLSSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(infra.DeepCopy()).Build()
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.ReconcileCertSecret(context.Background(), infra, "demo-cluster-gateway-tls", map[string]string{"app": "demo"}, []string{"gcp-ue4.sandbox0.ai"}); err != nil {
		t.Fatalf("reconcile cert secret: %v", err)
	}

	secret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-cluster-gateway-tls", Namespace: infra.Namespace}, secret); err != nil {
		t.Fatalf("get tls secret: %v", err)
	}
	if secret.Type != corev1.SecretTypeTLS {
		t.Fatalf("expected tls secret type, got %q", secret.Type)
	}
}
