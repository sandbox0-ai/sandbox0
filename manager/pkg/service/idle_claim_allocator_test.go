package service

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestReserveIdleClaimCandidateAssignsDistinctPodsConcurrently(t *testing.T) {
	const count = 64
	candidates := make([]*corev1.Pod, 0, count)
	for index := range count {
		pod := newClaimTestPod("ns-a", fmt.Sprintf("idle-%d", index), "template-a", true)
		pod.UID = types.UID(fmt.Sprintf("uid-%d", index))
		candidates = append(candidates, pod)
	}
	service := &SandboxService{}
	start := make(chan struct{})
	results := make(chan *corev1.Pod, count)

	var group sync.WaitGroup
	for range count {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			results <- service.reserveIdleClaimCandidate(candidates)
		}()
	}
	close(start)
	group.Wait()
	close(results)

	selected := make(map[string]struct{}, count)
	for pod := range results {
		if pod == nil {
			t.Fatal("reserveIdleClaimCandidate() = nil, want one candidate per caller")
		}
		if _, duplicate := selected[pod.Name]; duplicate {
			t.Fatalf("candidate %q was assigned more than once", pod.Name)
		}
		selected[pod.Name] = struct{}{}
	}
	if len(selected) != count {
		t.Fatalf("selected candidates = %d, want %d", len(selected), count)
	}
}

func TestClaimIdlePodAvoidsDuplicateCandidatesWithStaleInformer(t *testing.T) {
	const count = 32
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a", Namespace: "ns-a"},
	}
	pods := make([]*corev1.Pod, 0, count)
	objects := make([]runtime.Object, 0, count)
	for index := range count {
		pod := newClaimTestPod("ns-a", fmt.Sprintf("idle-%d", index), "template-a", true)
		pod.UID = types.UID(fmt.Sprintf("uid-%d", index))
		pods = append(pods, pod)
		objects = append(objects, pod.DeepCopy())
	}
	client := fake.NewSimpleClientset(objects...)
	service := &SandboxService{
		k8sClient: client,
		podLister: newClaimTestPodLister(t, pods...),
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}
	start := make(chan struct{})
	results := make(chan *corev1.Pod, count)
	errs := make(chan error, count)

	var group sync.WaitGroup
	for index := range count {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			pod, err := service.claimIdlePod(context.Background(), template, &ClaimRequest{
				SandboxID: fmt.Sprintf("sandbox-%d", index),
				TeamID:    "team-a",
				UserID:    "user-a",
			})
			results <- pod
			errs <- err
		}(index)
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("claimIdlePod() error = %v", err)
		}
	}
	selected := make(map[string]struct{}, count)
	for pod := range results {
		if pod == nil {
			t.Fatal("claimIdlePod() = nil, want a hot claim")
		}
		if _, duplicate := selected[pod.Name]; duplicate {
			t.Fatalf("Pod %q was claimed more than once", pod.Name)
		}
		selected[pod.Name] = struct{}{}
		if got := pod.Annotations[controller.AnnotationHotClaimCompletionProtocol]; got != controller.HotClaimCompletionProtocolRecordV1 {
			t.Fatalf("completion protocol = %q, want %q", got, controller.HotClaimCompletionProtocolRecordV1)
		}
	}
	if len(selected) != count {
		t.Fatalf("claimed Pods = %d, want %d", len(selected), count)
	}
}

func TestPodEventHandlerReleasesObservedIdleClaimReservation(t *testing.T) {
	pod := newClaimTestPod("ns-a", "idle-a", "template-a", true)
	service := &SandboxService{}
	if selected := service.reserveIdleClaimCandidate([]*corev1.Pod{pod}); selected == nil {
		t.Fatal("initial candidate reservation failed")
	}
	if selected := service.reserveIdleClaimCandidate([]*corev1.Pod{pod}); selected != nil {
		t.Fatalf("reserved candidate was assigned twice: %s", selected.Name)
	}

	reserved := pod.DeepCopy()
	reserved.Annotations[controller.AnnotationHotClaimReservation] = "token"
	service.PodEventHandler().UpdateFunc(pod, reserved)

	if selected := service.reserveIdleClaimCandidate([]*corev1.Pod{pod}); selected == nil {
		t.Fatal("informer-confirmed reservation did not release the local candidate")
	}
}
