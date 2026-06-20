package observability

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

func TestReconcileBuiltinCreatesClickHouseResourcesBeforeReady(t *testing.T) {
	reconciler, client := newTestReconciler(t)
	infra := observabilityTestInfra(infrav1alpha1.Sandbox0InfraSpec{
		Observability: &infrav1alpha1.ObservabilityConfig{
			Backend: &infrav1alpha1.ObservabilityBackendConfig{
				Type: infrav1alpha1.ObservabilityBackendTypeBuiltin,
			},
		},
	})

	err := reconciler.Reconcile(context.Background(), infra)
	if err == nil || !strings.Contains(err.Error(), "clickhouse statefulset") {
		t.Fatalf("expected clickhouse readiness error, got %v", err)
	}

	assertPresentObject(t, client, &corev1.Secret{}, "sandbox0-system", "demo-clickhouse-credentials")
	assertPresentObject(t, client, &corev1.PersistentVolumeClaim{}, "sandbox0-system", "demo-clickhouse-data")
	assertPresentObject(t, client, &appsv1.StatefulSet{}, "sandbox0-system", "demo-clickhouse")
	assertPresentObject(t, client, &corev1.Service{}, "sandbox0-system", "demo-clickhouse")
}

func TestCleanupBuiltinResourcesRespectsStatefulResourcePolicy(t *testing.T) {
	t.Run("retain keeps pvc and secret", func(t *testing.T) {
		reconciler, client := newTestReconciler(t,
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-clickhouse", Namespace: "sandbox0-system"}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "demo-clickhouse", Namespace: "sandbox0-system"}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-clickhouse-credentials", Namespace: "sandbox0-system"}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-clickhouse-data", Namespace: "sandbox0-system"}},
		)

		err := reconciler.CleanupBuiltinResources(context.Background(), observabilityTestInfra(infrav1alpha1.Sandbox0InfraSpec{
			Observability: &infrav1alpha1.ObservabilityConfig{
				Backend: &infrav1alpha1.ObservabilityBackendConfig{
					Builtin: &infrav1alpha1.BuiltinObservabilityBackendConfig{
						ClickHouse: &infrav1alpha1.BuiltinObservabilityClickHouseConfig{
							StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
						},
					},
				},
			},
		}))
		if err != nil {
			t.Fatalf("cleanup builtin resources: %v", err)
		}

		assertMissingObject(t, client, &appsv1.StatefulSet{}, "sandbox0-system", "demo-clickhouse")
		assertMissingObject(t, client, &corev1.Service{}, "sandbox0-system", "demo-clickhouse")
		assertPresentObject(t, client, &corev1.Secret{}, "sandbox0-system", "demo-clickhouse-credentials")
		assertPresentObject(t, client, &corev1.PersistentVolumeClaim{}, "sandbox0-system", "demo-clickhouse-data")
	})

	t.Run("delete removes pvc and secret", func(t *testing.T) {
		reconciler, client := newTestReconciler(t,
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-clickhouse", Namespace: "sandbox0-system"}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "demo-clickhouse", Namespace: "sandbox0-system"}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-clickhouse-credentials", Namespace: "sandbox0-system"}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-clickhouse-data", Namespace: "sandbox0-system"}},
		)

		err := reconciler.CleanupBuiltinResources(context.Background(), observabilityTestInfra(infrav1alpha1.Sandbox0InfraSpec{
			Observability: &infrav1alpha1.ObservabilityConfig{
				Backend: &infrav1alpha1.ObservabilityBackendConfig{
					Builtin: &infrav1alpha1.BuiltinObservabilityBackendConfig{
						ClickHouse: &infrav1alpha1.BuiltinObservabilityClickHouseConfig{
							StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyDelete,
						},
					},
				},
			},
		}))
		if err != nil {
			t.Fatalf("cleanup builtin resources: %v", err)
		}

		assertMissingObject(t, client, &appsv1.StatefulSet{}, "sandbox0-system", "demo-clickhouse")
		assertMissingObject(t, client, &corev1.Service{}, "sandbox0-system", "demo-clickhouse")
		assertMissingObject(t, client, &corev1.Secret{}, "sandbox0-system", "demo-clickhouse-credentials")
		assertMissingObject(t, client, &corev1.PersistentVolumeClaim{}, "sandbox0-system", "demo-clickhouse-data")
	})
}

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

func assertPresentObject(t *testing.T, client ctrlclient.Client, obj ctrlclient.Object, namespace, name string) {
	t.Helper()
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, obj); err != nil {
		t.Fatalf("expected %T %s/%s to exist: %v", obj, namespace, name, err)
	}
}

func assertMissingObject(t *testing.T, client ctrlclient.Client, obj ctrlclient.Object, namespace, name string) {
	t.Helper()
	err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, obj)
	if err == nil {
		t.Fatalf("expected %T %s/%s to be deleted", obj, namespace, name)
	}
}
