package fuseplugin

import (
	"context"
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

func TestReconcileUsesSharedSandboxNodePlacement(t *testing.T) {
	infra := newFusePluginTestInfra()
	infra.Spec.SandboxNodePlacement = &infrav1alpha1.SandboxNodePlacementConfig{
		NodeSelector: map[string]string{
			"sandbox0.ai/node-role": "shared",
		},
		Tolerations: []corev1.Toleration{
			{
				Key:      "sandbox0.ai/sandbox",
				Operator: corev1.TolerationOpEqual,
				Value:    "true",
				Effect:   corev1.TaintEffectNoSchedule,
			},
		},
	}
	infra.Spec.Services.Netd.NodeSelector = map[string]string{
		"sandbox0.ai/node-role": "legacy",
	}

	ds := reconcileFusePluginDaemonSet(t, infra)
	if got := ds.Spec.Template.Spec.NodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared node selector, got %q", got)
	}
	if len(ds.Spec.Template.Spec.Tolerations) != 1 || ds.Spec.Template.Spec.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared toleration, got %#v", ds.Spec.Template.Spec.Tolerations)
	}
}

func TestReconcileFallsBackToLegacyNetdPlacement(t *testing.T) {
	infra := newFusePluginTestInfra()
	infra.Spec.Services.Netd.NodeSelector = map[string]string{
		"sandbox0.ai/node-role": "legacy",
	}
	infra.Spec.Services.Netd.Tolerations = []corev1.Toleration{
		{
			Key:      "sandbox.gke.io/runtime",
			Operator: corev1.TolerationOpEqual,
			Value:    "gvisor",
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}

	ds := reconcileFusePluginDaemonSet(t, infra)
	if got := ds.Spec.Template.Spec.NodeSelector["sandbox0.ai/node-role"]; got != "legacy" {
		t.Fatalf("expected legacy node selector fallback, got %q", got)
	}
	if len(ds.Spec.Template.Spec.Tolerations) != 1 || ds.Spec.Template.Spec.Tolerations[0].Value != "gvisor" {
		t.Fatalf("expected legacy toleration fallback, got %#v", ds.Spec.Template.Spec.Tolerations)
	}
}

func reconcileFusePluginDaemonSet(t *testing.T, infra *infrav1alpha1.Sandbox0Infra) *appsv1.DaemonSet {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1 scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(infra.DeepCopy()).
		Build()

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest"); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	ds := &appsv1.DaemonSet{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      infra.Name + "-k8s-plugin",
		Namespace: infra.Namespace,
	}, ds); err != nil {
		t.Fatalf("expected daemonset to be created: %v", err)
	}

	return ds
}

func newFusePluginTestInfra() *infrav1alpha1.Sandbox0Infra {
	return &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					BaseServiceConfig: infrav1alpha1.BaseServiceConfig{
						Enabled: true,
					},
				},
				Netd: &infrav1alpha1.NetdServiceConfig{},
			},
		},
	}
}
