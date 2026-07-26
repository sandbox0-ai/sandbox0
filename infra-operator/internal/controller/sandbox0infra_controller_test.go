package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestExpectedConditionTypesIncludesGlobalGateway(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	conditions := infraplan.Compile(infra).Status.ExpectedConditions
	if len(conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d: %#v", len(conditions), conditions)
	}
	if conditions[0] != infrav1alpha1.ConditionTypeDatabaseReady {
		t.Fatalf("expected database condition first, got %q", conditions[0])
	}
	if conditions[1] != infrav1alpha1.ConditionTypeGlobalGatewayReady {
		t.Fatalf("expected global-gateway condition second, got %q", conditions[1])
	}
}

func TestSandbox0InfraUpdateRequiresReconcile(t *testing.T) {
	base := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "sandbox0",
			Namespace:  "sandbox0-system",
			Generation: 3,
		},
	}

	statusOnly := base.DeepCopy()
	statusOnly.Status.Phase = infrav1alpha1.PhaseReady
	if sandbox0InfraUpdateRequiresReconcile(event.UpdateEvent{ObjectOld: base, ObjectNew: statusOnly}) {
		t.Fatal("status-only update should not trigger reconciliation")
	}

	specUpdate := base.DeepCopy()
	specUpdate.Generation++
	if !sandbox0InfraUpdateRequiresReconcile(event.UpdateEvent{ObjectOld: base, ObjectNew: specUpdate}) {
		t.Fatal("generation change should trigger reconciliation")
	}

	deleting := base.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	if !sandbox0InfraUpdateRequiresReconcile(event.UpdateEvent{ObjectOld: base, ObjectNew: deleting}) {
		t.Fatal("deletion timestamp change should trigger reconciliation")
	}
}

func TestRequestsForManagedDataPlanePodIgnoresRuntimeSandboxes(t *testing.T) {
	reconciler := &Sandbox0InfraReconciler{}
	sandboxPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tpl-default",
			Name:      "default-idle",
			Labels: map[string]string{
				"sandbox0.ai/template-id": "default",
				"sandbox0.ai/pool-type":   "idle",
			},
		},
	}
	if requests := reconciler.requestsForManagedDataPlanePod(context.Background(), sandboxPod); len(requests) != 0 {
		t.Fatalf("runtime sandbox Pod mapped to reconcile requests: %#v", requests)
	}

	ctldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "sandbox0-system",
			Name:      "fullmode-ctld-a-node",
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "sandbox0infra-operator",
				"app.kubernetes.io/component":  "ctld",
				"app.kubernetes.io/instance":   "fullmode",
			},
		},
	}
	requests := reconciler.requestsForManagedDataPlanePod(context.Background(), ctldPod)
	if len(requests) != 1 || requests[0].NamespacedName != (types.NamespacedName{Namespace: "sandbox0-system", Name: "fullmode"}) {
		t.Fatalf("ctld Pod mapped to unexpected reconcile requests: %#v", requests)
	}
}
