package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/carrier"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		ObjectMeta: metav1.ObjectMeta{Name: "carrier-1", Namespace: "sandbox0-system", UID: "pod-uid"},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type: "sandbox0.ai/ready", Status: corev1.ConditionFalse,
			}},
		},
	}
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := spec.WriteSuccess(w, http.StatusOK, procdapi.StartupResponse{
			Status: "started", Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID),
		}); err != nil {
			t.Fatalf("write startup response: %v", err)
		}
	}))
	defer procd.Close()
	procdURL, procdPort := parsedTestServer(t, procd.URL)
	svc := &SandboxService{
		podLister:   newTestPodLister(t, pod),
		procdClient: procdapi.NewProcdClient(procdapi.ProcdClientConfig{Timeout: time.Second}),
		config:      SandboxServiceConfig{ProcdPort: procdPort, RuntimeReadyTimeout: time.Second},
	}

	started, err := svc.waitForCarrierRuntimeStarted(context.Background(), pod, procdURL.Hostname())
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
		podLister:   newTestPodLister(t, pod),
		procdClient: procdapi.NewProcdClient(procdapi.ProcdClientConfig{Timeout: 100 * time.Millisecond}),
		config:      SandboxServiceConfig{ProcdPort: 1, RuntimeReadyTimeout: time.Second},
	}

	if _, err := svc.waitForCarrierRuntimeStarted(context.Background(), pod, "127.0.0.1"); err == nil {
		t.Fatal("waitForCarrierRuntimeStarted() error = nil, want terminal Pod error")
	}
}

func TestWaitForS0FSCarrierReadyOverlapsProcdAndRootFSPreparation(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "carrier-1", Namespace: "sandbox0-system", UID: "pod-uid"},
	}
	procdStarted := make(chan struct{})
	allowProcd := make(chan struct{})
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(procdStarted)
		<-allowProcd
		_ = spec.WriteSuccess(w, http.StatusOK, procdapi.StartupResponse{
			Status: "started", Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID),
		})
	}))
	defer procd.Close()
	procdURL, procdPort := parsedTestServer(t, procd.URL)
	pod.Status.PodIP = procdURL.Hostname()
	svc := &SandboxService{
		podLister:   newTestPodLister(t, pod),
		procdClient: procdapi.NewProcdClient(procdapi.ProcdClientConfig{Timeout: time.Second}),
		config:      SandboxServiceConfig{ProcdPort: procdPort, RuntimeReadyTimeout: time.Second},
	}

	rootFSStarted := make(chan struct{})
	allowRootFS := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := svc.waitForS0FSCarrierReady(context.Background(), pod, "template-a", "shared", func(context.Context, *corev1.Pod) error {
			close(rootFSStarted)
			<-allowRootFS
			return nil
		})
		result <- err
	}()

	select {
	case <-procdStarted:
	case <-time.After(time.Second):
		t.Fatal("procd readiness did not start while rootfs preparation was blocked")
	}
	select {
	case <-rootFSStarted:
	case <-time.After(time.Second):
		t.Fatal("rootfs preparation did not start while procd readiness was blocked")
	}
	close(allowRootFS)
	select {
	case err := <-result:
		t.Fatalf("waitForS0FSCarrierReady() returned before procd was ready: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(allowProcd)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("waitForS0FSCarrierReady() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitForS0FSCarrierReady() did not return after both readiness paths completed")
	}
}
