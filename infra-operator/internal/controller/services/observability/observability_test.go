package observability

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

func TestGatewayCollectorConfigMergesExternalSecretHeaders(t *testing.T) {
	reconciler, _ := newTestReconciler(t,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "otel-headers", Namespace: "sandbox0-system"},
			Data: map[string][]byte{
				"headers": []byte("authorization=Bearer+secret,x-scope=tenant"),
			},
		},
	)
	infra := observabilityTestInfra(infrav1alpha1.Sandbox0InfraSpec{
		Observability: &infrav1alpha1.ObservabilityConfig{
			Backend: &infrav1alpha1.ObservabilityBackendConfig{
				Type: infrav1alpha1.ObservabilityBackendTypeExternal,
				External: &infrav1alpha1.ExternalObservabilityBackendConfig{
					Mode: infrav1alpha1.ObservabilityExternalModeManagedCollector,
					OTLP: &infrav1alpha1.ObservabilityOTLPConfig{
						Endpoint:      "otel.example.com:4317",
						Headers:       map[string]string{"x-static": "platform"},
						HeadersSecret: &infrav1alpha1.ObservabilityHeadersSecretRef{Name: "otel-headers"},
					},
				},
			},
		},
	})

	config, _, err := reconciler.gatewayCollectorConfig(context.Background(), infra)
	if err != nil {
		t.Fatalf("build gateway collector config: %v", err)
	}
	exporters := config["exporters"].(map[string]any)
	otlp := exporters["otlp"].(map[string]any)
	headers := otlp["headers"].(map[string]string)

	if got := headers["authorization"]; got != "Bearer secret" {
		t.Fatalf("authorization header = %q", got)
	}
	if got := headers["x-scope"]; got != "tenant" {
		t.Fatalf("x-scope header = %q", got)
	}
	if got := headers["x-static"]; got != "platform" {
		t.Fatalf("x-static header = %q", got)
	}
}

func newTestReconciler(t *testing.T, objects ...runtime.Object) (*Reconciler, ctrlclient.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rbac scheme: %v", err)
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	return NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{})), client
}

func observabilityTestInfra(spec infrav1alpha1.Sandbox0InfraSpec) *infrav1alpha1.Sandbox0Infra {
	return &infrav1alpha1.Sandbox0Infra{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "infra.sandbox0.ai/v1alpha1",
			Kind:       "Sandbox0Infra",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec:       spec,
	}
}
