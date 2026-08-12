package service

import (
	"context"
	"testing"
	"time"

	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/carrier"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPersistUpdatedCarrierPodPreservesS0FSRuntimeVersion(t *testing.T) {
	template := newSandboxResourceTestTemplate(t)
	pod := newSandboxResourceTestActivePod(t, template, "sandbox-1")
	pod.Annotations[carrier.AnnotationSlot] = "s0-test-slot"
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "1"
	record := &sandboxstore.SandboxRecord{
		ID: "sandbox-1", TeamID: "team-a", UserID: "user-a", TemplateID: template.Name,
		TemplateName: template.Name, TemplateNamespace: template.Namespace,
		DesiredState: sandboxstore.SandboxDesiredStateActive, RuntimeGeneration: 1,
		CurrentPodName: pod.Name, CurrentPodNamespace: pod.Namespace,
		RootFSRuntimeVersion: sandboxstore.RootFSRuntimeS0FSV2,
	}
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{record.ID: record}}
	svc := &SandboxService{
		sandboxStore: store, templateLister: staticTemplateLister{templates: []*api.SandboxTemplate{template}},
		clock: systemTime{},
	}

	if err := svc.persistUpdatedSandboxPod(context.Background(), pod); err != nil {
		t.Fatalf("persistUpdatedSandboxPod() error = %v", err)
	}
	if got := store.records[record.ID].RootFSRuntimeVersion; got != sandboxstore.RootFSRuntimeS0FSV2 {
		t.Fatalf("rootfs runtime version = %q, want %q", got, sandboxstore.RootFSRuntimeS0FSV2)
	}
}

func TestWaitForCarrierRuntimeStartedDoesNotRequirePodReadiness(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "carrier-1", Namespace: "sandbox0-system"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.10",
			Conditions: []corev1.PodCondition{{
				Type: "sandbox0.ai/ready", Status: corev1.ConditionFalse,
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "procd", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod),
		config:    SandboxServiceConfig{RuntimeReadyTimeout: time.Second},
	}

	started, err := svc.waitForCarrierRuntimeStarted(context.Background(), pod.Namespace, pod.Name)
	if err != nil {
		t.Fatalf("waitForCarrierRuntimeStarted() error = %v", err)
	}
	if started == nil || started.Name != pod.Name {
		t.Fatalf("waitForCarrierRuntimeStarted() = %#v, want %s", started, pod.Name)
	}
}

func TestWaitForCarrierRuntimeStartedRejectsTerminatedRuntime(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "carrier-1", Namespace: "sandbox0-system"},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "procd", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error"}},
			}},
		},
	}
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod),
		config:    SandboxServiceConfig{RuntimeReadyTimeout: time.Second},
	}

	if _, err := svc.waitForCarrierRuntimeStarted(context.Background(), pod.Namespace, pod.Name); err == nil {
		t.Fatal("waitForCarrierRuntimeStarted() error = nil, want terminal Pod error")
	}
}
