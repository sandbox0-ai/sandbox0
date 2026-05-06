package observability

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

func TestReconcileCreatesCollectorAndClickHouse(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Region:        "aws-us-east-1",
			Cluster:       &infrav1alpha1.ClusterConfig{ID: "cluster-a"},
			Observability: &infrav1alpha1.ObservabilityConfig{Enabled: true},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infra).Build()
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(ctx, infra); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("first reconcile error = %v, want not ready", err)
	}

	clickhouse := &appsv1.StatefulSet{}
	if err := client.Get(ctx, types.NamespacedName{Name: "demo-clickhouse", Namespace: infra.Namespace}, clickhouse); err != nil {
		t.Fatalf("get clickhouse statefulset: %v", err)
	}
	clickhouse.Status.ReadyReplicas = 1
	if err := client.Status().Update(ctx, clickhouse); err != nil {
		t.Fatalf("mark clickhouse ready: %v", err)
	}

	if err := reconciler.Reconcile(ctx, infra); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	collectorConfig := &corev1.ConfigMap{}
	if err := client.Get(ctx, types.NamespacedName{Name: "demo-otel-collector", Namespace: infra.Namespace}, collectorConfig); err != nil {
		t.Fatalf("get collector configmap: %v", err)
	}
	config := collectorConfig.Data["config.yaml"]
	for _, want := range []string{"receivers:", "clickhouse:", "sandbox0.region_id", "aws-us-east-1", "cluster-a"} {
		if !strings.Contains(config, want) {
			t.Fatalf("collector config missing %q:\n%s", want, config)
		}
	}

	if err := client.Get(ctx, types.NamespacedName{Name: "demo-clickhouse", Namespace: infra.Namespace}, clickhouse); err != nil {
		t.Fatalf("get clickhouse statefulset: %v", err)
	}
	if got := clickhouse.Spec.Template.Spec.Containers[0].Image; got != defaultClickHouseImage {
		t.Fatalf("clickhouse image = %q, want %q", got, defaultClickHouseImage)
	}

	collector := &appsv1.Deployment{}
	if err := client.Get(ctx, types.NamespacedName{Name: "demo-otel-collector", Namespace: infra.Namespace}, collector); err != nil {
		t.Fatalf("get collector deployment: %v", err)
	}
	if got := collector.Spec.Template.Spec.Containers[0].Image; got != defaultCollectorImage {
		t.Fatalf("collector image = %q, want %q", got, defaultCollectorImage)
	}
}
