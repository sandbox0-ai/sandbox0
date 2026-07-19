package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestObservedPodOverheadCommitsExactThenDeletesWithoutAbort(t *testing.T) {
	admittedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "sandbox-a",
			Namespace:  "default",
			UID:        "runtime-uid",
			Finalizers: []string{sandboxCleanupFinalizer},
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-a",
			},
			Annotations: map[string]string{
				controller.AnnotationRuntimeGeneration: "3",
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "procd",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			}},
		}}},
	}
	observedPod := admittedPod.DeepCopy()
	observedPod.Spec.Overhead = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("125m"),
		corev1.ResourceMemory: resource.MustParse("96Mi"),
	}

	store := newObservedRuntimeQuotaStore()
	client := fake.NewSimpleClientset(observedPod.DeepCopy())
	client.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		store.record("delete")
		return false, nil, nil
	})
	svc := &SandboxService{
		k8sClient:      client,
		teamQuotaStore: store,
	}
	reservation := testQuotaReservation(
		teamquota.Owner{
			TeamID: "team-a",
			Kind:   "sandbox",
			ID:     "sandbox-a",
		},
		teamquota.Operation{
			ID:         "claim-op",
			Kind:       "claim",
			Generation: 3,
		},
		activeSandboxQuotaTarget(admittedPod),
	)
	admission := &sandboxTeamQuotaAdmission{Reservation: reservation}

	err := svc.finalizeSandboxTeamQuotaAdmission(context.Background(), admission, observedPod)
	if err == nil {
		t.Fatal("finalizeSandboxTeamQuotaAdmission() error = nil, want observed overage")
	}
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("finalizeSandboxTeamQuotaAdmission() error = %v, want ErrQuotaExceeded", err)
	}
	var overage *TeamQuotaObservedRuntimeOverage
	if !errors.As(err, &overage) {
		t.Fatalf("finalizeSandboxTeamQuotaAdmission() error = %T, want TeamQuotaObservedRuntimeOverage", err)
	}
	if !admission.Committed {
		t.Fatal("admission committed = false after observed exact commit")
	}
	wantObserved := activeSandboxQuotaTarget(observedPod)
	if !quotaValuesEqual(store.observedExactTarget(), wantObserved) {
		t.Fatalf("observed exact target = %v, want %v", store.observedExactTarget(), wantObserved)
	}
	if !quotaValuesEqual(reservation.Target, wantObserved) {
		t.Fatalf("reservation target = %v, want observed %v", reservation.Target, wantObserved)
	}

	svc.releaseFailedSandboxTeamQuotaAdmission(
		admission,
		observedPod,
		"observed runtime exceeds admitted quota",
	)
	select {
	case <-store.confirmed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for physical runtime release confirmation")
	}

	events := store.eventsSnapshot()
	assertEventOrder(
		t,
		events,
		"attach",
		"commit_observed_exact",
		"begin_release",
		"delete",
		"confirm_release",
	)
	if containsEvent(events, "commit") {
		t.Fatalf("events = %v, normal Commit must not run after CommitObservedExact", events)
	}
	if containsEvent(events, "abort") {
		t.Fatalf("events = %v, Abort must not run after CommitObservedExact", events)
	}
	if _, err := client.CoreV1().Pods(observedPod.Namespace).Get(
		context.Background(),
		observedPod.Name,
		metav1.GetOptions{},
	); !apierrors.IsNotFound(err) {
		t.Fatalf("runtime Pod Get() error = %v, want not found after overage cleanup", err)
	}
}

type observedRuntimeQuotaStore struct {
	permissiveTeamQuotaCapacityStore

	mu            sync.Mutex
	events        []string
	observedExact teamquota.Values
	confirmed     chan struct{}
	confirmOnce   sync.Once
}

func newObservedRuntimeQuotaStore() *observedRuntimeQuotaStore {
	return &observedRuntimeQuotaStore{confirmed: make(chan struct{})}
}

func (s *observedRuntimeQuotaStore) record(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *observedRuntimeQuotaStore) AttachRuntime(
	context.Context,
	teamquota.OperationRef,
	teamquota.RuntimeRef,
) error {
	s.record("attach")
	return nil
}

func (s *observedRuntimeQuotaStore) Commit(
	context.Context,
	teamquota.OperationRef,
) error {
	s.record("commit")
	return nil
}

func (s *observedRuntimeQuotaStore) CommitObservedExact(
	_ context.Context,
	_ teamquota.OperationRef,
	exact teamquota.Values,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "commit_observed_exact")
	s.observedExact = exact.Clone()
	return nil
}

func (s *observedRuntimeQuotaStore) Abort(
	context.Context,
	teamquota.OperationRef,
	string,
) error {
	s.record("abort")
	return nil
}

func (s *observedRuntimeQuotaStore) BeginRelease(
	_ context.Context,
	request teamquota.ReleaseRequest,
) (*teamquota.Reservation, error) {
	s.record("begin_release")
	return testQuotaReservation(request.Owner, request.Operation, request.Target), nil
}

func (s *observedRuntimeQuotaStore) ConfirmRelease(
	context.Context,
	teamquota.OperationRef,
	teamquota.RuntimeRef,
) error {
	s.record("confirm_release")
	s.confirmOnce.Do(func() { close(s.confirmed) })
	return nil
}

func (s *observedRuntimeQuotaStore) observedExactTarget() teamquota.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observedExact.Clone()
}

func (s *observedRuntimeQuotaStore) eventsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

func assertEventOrder(t *testing.T, events []string, want ...string) {
	t.Helper()
	index := -1
	for _, event := range want {
		found := -1
		for i := index + 1; i < len(events); i++ {
			if events[i] == event {
				found = i
				break
			}
		}
		if found < 0 {
			t.Fatalf("events = %v, missing %q after index %d", events, event, index)
		}
		index = found
	}
}

func containsEvent(events []string, want string) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}
