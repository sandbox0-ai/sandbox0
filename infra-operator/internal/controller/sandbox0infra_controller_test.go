package controller

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

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

func TestPodReadinessSummaryIncludesWaitingReason(t *testing.T) {
	pod := &corev1.Pod{}
	pod.Name = "default-idle"
	pod.Status.Phase = corev1.PodPending
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "procd",
		State: corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"},
		},
	}}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:   corev1.PodReady,
		Status: corev1.ConditionFalse,
		Reason: "ContainersNotReady",
	}}

	summary := podReadinessSummary(pod)
	for _, want := range []string{"default-idle phase=Pending", "procd=waiting(ImagePullBackOff)", "Ready=ContainersNotReady"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary %q does not contain %q", summary, want)
		}
	}
}
