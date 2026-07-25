package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestMarkHotClaimReservationReadyKeepsWarmPoolAttachment(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateInitializing)
	client := fake.NewSimpleClientset(pod.DeepCopy())
	service := &SandboxService{k8sClient: client}

	ready, err := service.markHotClaimReservationReady(context.Background(), pod)
	if err != nil {
		t.Fatalf("markHotClaimReservationReady() error = %v", err)
	}
	if got := ready.Annotations[controller.AnnotationHotClaimReservationState]; got != controller.HotClaimReservationStateReady {
		t.Fatalf("reservation state = %q, want %q", got, controller.HotClaimReservationStateReady)
	}
	if got := ready.Annotations[controller.AnnotationHotClaimReadyAt]; got == "" {
		t.Fatal("hot claim ready timestamp is empty")
	}
	if got := ready.Labels[controller.LabelPoolType]; got != controller.PoolTypeIdle {
		t.Fatalf("pool type = %q, want idle before controller detachment", got)
	}
	if len(ready.OwnerReferences) != 2 {
		t.Fatalf("owner references = %#v, want both warm-pool and external owners", ready.OwnerReferences)
	}
}

func TestMarkHotClaimReservationReadyAllowsResourceVersionAdvance(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	stalePod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateInitializing)
	stalePod.ResourceVersion = "10"
	livePod := stalePod.DeepCopy()
	livePod.ResourceVersion = "11"
	client := fake.NewSimpleClientset(livePod)
	service := &SandboxService{k8sClient: client}

	ready, err := service.markHotClaimReservationReady(context.Background(), stalePod)
	if err != nil {
		t.Fatalf("markHotClaimReservationReady() error = %v", err)
	}
	if got := ready.Annotations[controller.AnnotationHotClaimReservationState]; got != controller.HotClaimReservationStateReady {
		t.Fatalf("reservation state = %q, want %q", got, controller.HotClaimReservationStateReady)
	}
}

func TestHotClaimReservationControllerFinalizesDurableReadyClaim(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateReady)
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": hotClaimReservationTestRecord(pod),
	}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	reconciler := NewHotClaimReservationController(
		client,
		newClaimTestPodLister(t, pod),
		store,
		nil,
	)
	reconciler.clock = fixedClock{now: now}
	pacer := &recordingHotClaimDetachmentPacer{}
	reconciler.detachPacer = pacer

	requeueAfter, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name)
	if err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	if requeueAfter != 0 {
		t.Fatalf("reconcile() requeueAfter = %s, want 0", requeueAfter)
	}
	if pacer.calls != 1 {
		t.Fatalf("detachment pacer calls = %d, want 1", pacer.calls)
	}

	got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get finalized pod: %v", err)
	}
	if got.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		t.Fatalf("pool type = %q, want active", got.Labels[controller.LabelPoolType])
	}
	if controller.IsHotClaimReservedPod(got) {
		t.Fatalf("finalized pod still has reservation: %#v", got.Annotations)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].UID != types.UID("external-owner-uid") {
		t.Fatalf("owner references = %#v, want only external owner", got.OwnerReferences)
	}
	if !hasSandboxCleanupFinalizer(got) {
		t.Fatal("sandbox cleanup finalizer was removed")
	}
}

func TestHotClaimReservationControllerExpandsRequestsBeforeDetachment(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateReady)
	quota := v1alpha1.ResourceQuota{
		CPU:    resource.MustParse("250m"),
		Memory: resource.MustParse("1Gi"),
	}
	setHotClaimReservationResources(t, pod, quota)
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": hotClaimReservationTestRecord(pod),
	}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	reconciler := NewHotClaimReservationController(
		client,
		newClaimTestPodLister(t, pod),
		store,
		nil,
	)
	reconciler.clock = fixedClock{now: now}
	reconciler.detachPacer = &recordingHotClaimDetachmentPacer{}

	if _, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name); err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}

	resizeIndex := -1
	detachIndex := -1
	for index, action := range client.Actions() {
		if !action.Matches("patch", "pods") {
			continue
		}
		if action.GetSubresource() == "resize" {
			resizeIndex = index
		} else {
			detachIndex = index
		}
	}
	if resizeIndex < 0 || detachIndex < 0 || resizeIndex >= detachIndex {
		t.Fatalf("actions = %#v, want pods/resize before metadata detachment", client.Actions())
	}

	got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get finalized pod: %v", err)
	}
	resources := sandboxRuntimeContainer(t, got).Resources
	assertQuantity(t, resources.Limits[corev1.ResourceMemory], "1Gi")
	assertQuantity(t, resources.Limits[corev1.ResourceCPU], "250m")
	assertQuantity(t, resources.Requests[corev1.ResourceMemory], "256Mi")
	assertQuantity(t, resources.Requests[corev1.ResourceCPU], "25m")
	if got.Annotations[controller.AnnotationHotClaimActiveResources] != "" {
		t.Fatalf("active resources annotation was not removed: %#v", got.Annotations)
	}
	if got.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		t.Fatalf("pool type = %q, want active", got.Labels[controller.LabelPoolType])
	}
}

func TestHotClaimReservationControllerKeepsWarmPoolAttachmentWhenRequestExpansionFails(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateReady)
	setHotClaimReservationResources(t, pod, v1alpha1.ResourceQuota{
		CPU:    resource.MustParse("250m"),
		Memory: resource.MustParse("1Gi"),
	})
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": hotClaimReservationTestRecord(pod),
	}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	client.PrependReactor("patch", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "resize" {
			return false, nil, nil
		}
		return true, nil, errors.New("resize rejected")
	})
	reconciler := NewHotClaimReservationController(
		client,
		newClaimTestPodLister(t, pod),
		store,
		nil,
	)
	reconciler.clock = fixedClock{now: now}
	reconciler.detachPacer = &recordingHotClaimDetachmentPacer{}

	if _, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name); err == nil {
		t.Fatal("reconcile() error = nil, want request expansion failure")
	}
	got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get reserved pod: %v", err)
	}
	if got.Labels[controller.LabelPoolType] != controller.PoolTypeIdle {
		t.Fatalf("pool type = %q, want idle after failed request expansion", got.Labels[controller.LabelPoolType])
	}
	if !controller.IsHotClaimReservedPod(got) {
		t.Fatal("reservation was removed after failed request expansion")
	}
	if len(got.OwnerReferences) != 2 {
		t.Fatalf("owner references = %#v, want warm-pool attachment preserved", got.OwnerReferences)
	}
	for _, action := range client.Actions() {
		if action.Matches("patch", "pods") && action.GetSubresource() == "" {
			t.Fatalf("unexpected metadata detachment after failed request expansion: %#v", client.Actions())
		}
	}
}

func TestHotClaimReservationControllerWaitsForSettleWindowWithHealthyCapacity(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateReady)
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": hotClaimReservationTestRecord(pod),
	}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	pacer := &recordingHotClaimDetachmentPacer{}
	reconciler := NewHotClaimReservationController(
		client,
		newClaimTestPodLister(t, hotClaimReservationCapacityPods(pod, hotClaimDetachmentLowWatermark)...),
		store,
		nil,
	)
	reconciler.clock = fixedClock{now: now}
	reconciler.detachPacer = pacer

	requeueAfter, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name)
	if err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	if requeueAfter != hotClaimDetachmentSettleWindow {
		t.Fatalf("reconcile() requeueAfter = %s, want %s", requeueAfter, hotClaimDetachmentSettleWindow)
	}
	if pacer.calls != 0 {
		t.Fatalf("detachment pacer calls = %d, want 0 before settle window", pacer.calls)
	}
	got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get reserved pod: %v", err)
	}
	if got.Labels[controller.LabelPoolType] != controller.PoolTypeIdle {
		t.Fatalf("pool type = %q, want idle during settle window", got.Labels[controller.LabelPoolType])
	}
}

func TestHotClaimReservationControllerMaxDelayBypassesPacer(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(
		now.Add(-hotClaimDetachmentMaxDelay-time.Second),
		controller.HotClaimReservationStateReady,
	)
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": hotClaimReservationTestRecord(pod),
	}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	pacer := &recordingHotClaimDetachmentPacer{err: errors.New("pacer must be bypassed")}
	reconciler := NewHotClaimReservationController(
		client,
		newClaimTestPodLister(t, hotClaimReservationCapacityPods(pod, hotClaimDetachmentLowWatermark)...),
		store,
		nil,
	)
	reconciler.clock = fixedClock{now: now}
	reconciler.detachPacer = pacer

	if _, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name); err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	if pacer.calls != 0 {
		t.Fatalf("detachment pacer calls = %d, want 0 after max delay", pacer.calls)
	}
	got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get finalized pod: %v", err)
	}
	if got.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		t.Fatalf("pool type = %q, want active after max delay", got.Labels[controller.LabelPoolType])
	}
}

func TestHotClaimReservationControllerWaitsForDurableIdentity(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateReady)
	client := fake.NewSimpleClientset(pod.DeepCopy())
	reconciler := NewHotClaimReservationController(
		client,
		newClaimTestPodLister(t, pod),
		&memorySandboxStore{records: map[string]*SandboxRecord{}},
		nil,
	)
	reconciler.clock = fixedClock{now: now}

	requeueAfter, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name)
	if err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	if requeueAfter != hotClaimReservationRecoveryGracePeriod {
		t.Fatalf("reconcile() requeueAfter = %s, want %s", requeueAfter, hotClaimReservationRecoveryGracePeriod)
	}
	if _, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("reservation was removed before durable identity existed: %v", err)
	}
}

func TestHotClaimReservationControllerDeletesAbandonedPartialClaim(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(
		now.Add(-hotClaimReservationRecoveryGracePeriod-time.Second),
		controller.HotClaimReservationStateInitializing,
	)
	record := hotClaimReservationTestRecord(pod)
	store := &memorySandboxStore{records: map[string]*SandboxRecord{"sandbox-a": record}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	reconciler := NewHotClaimReservationController(
		client,
		newClaimTestPodLister(t, pod),
		store,
		nil,
	)
	reconciler.clock = fixedClock{now: now}

	if _, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name); err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	if _, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Fatalf("get abandoned pod error = %v, want not found", err)
	}
	gotRecord, err := store.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if gotRecord.Status != SandboxStatusDeleted || gotRecord.DeletedAt.IsZero() {
		t.Fatalf("sandbox record = %#v, want deleted", gotRecord)
	}
}

func TestHotClaimReservationControllerPreservesPausedIdentityOnAbandonedResume(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(
		now.Add(-hotClaimReservationRecoveryGracePeriod-time.Second),
		controller.HotClaimReservationStateInitializing,
	)
	record := hotClaimReservationTestRecord(pod)
	record.Status = SandboxStatusPaused
	record.CurrentPodName = ""
	record.CurrentPodNamespace = ""
	store := &memorySandboxStore{records: map[string]*SandboxRecord{"sandbox-a": record}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	reconciler := NewHotClaimReservationController(
		client,
		newClaimTestPodLister(t, pod),
		store,
		nil,
	)
	reconciler.clock = fixedClock{now: now}

	if _, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name); err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	gotRecord, err := store.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if gotRecord.Status != SandboxStatusPaused || !gotRecord.DeletedAt.IsZero() {
		t.Fatalf("paused sandbox record = %#v, want preserved", gotRecord)
	}
}

func TestHotClaimReservationControllerStartupScanEnqueuesReservations(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateReady)
	reconciler := NewHotClaimReservationController(
		fake.NewSimpleClientset(pod.DeepCopy()),
		newClaimTestPodLister(t, pod),
		nil,
		nil,
	)

	reconciler.enqueueReservations()
	if got := reconciler.queue.Len(); got != 1 {
		t.Fatalf("startup queue length = %d, want 1", got)
	}
}

func newHotClaimReservationTestPod(reservedAt time.Time, state string) *corev1.Pod {
	controllerOwner := true
	pod := newClaimTestPod("sandbox-a", "idle-a", "default", true)
	pod.UID = types.UID("pod-uid")
	pod.CreationTimestamp = metav1.NewTime(reservedAt.Add(-time.Minute))
	pod.Labels[controller.LabelSandboxID] = "sandbox-a"
	pod.Annotations[controller.AnnotationSandboxID] = "sandbox-a"
	pod.Annotations[controller.AnnotationTeamID] = "team-a"
	pod.Annotations[controller.AnnotationUserID] = "user-a"
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "1"
	pod.Annotations[controller.AnnotationHotClaimReservation] = "reservation-token"
	pod.Annotations[controller.AnnotationHotClaimReservationState] = state
	pod.Annotations[controller.AnnotationHotClaimReservedAt] = reservedAt.Format(time.RFC3339Nano)
	if state == controller.HotClaimReservationStateReady {
		pod.Annotations[controller.AnnotationHotClaimReadyAt] = reservedAt.Format(time.RFC3339Nano)
	}
	pod.Finalizers = []string{"example.com/unrelated", sandboxCleanupFinalizer}
	pod.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "ReplicaSet",
			Name:       "warm-pool",
			UID:        types.UID("replicaset-uid"),
			Controller: &controllerOwner,
		},
		{
			APIVersion: "example.com/v1",
			Kind:       "ExternalOwner",
			Name:       "external-owner",
			UID:        types.UID("external-owner-uid"),
		},
	}
	return pod
}

func setHotClaimReservationResources(t *testing.T, pod *corev1.Pod, quota v1alpha1.ResourceQuota) {
	t.Helper()
	for index := range pod.Spec.Containers {
		if pod.Spec.Containers[index].Name == "procd" {
			pod.Spec.Containers[index].Resources = v1alpha1.BuildIdleResourceRequirements(quota)
			break
		}
	}
	activeResources, err := json.Marshal(v1alpha1.BuildResourceRequirements(quota))
	if err != nil {
		t.Fatalf("marshal active resources: %v", err)
	}
	pod.Annotations[controller.AnnotationHotClaimActiveResources] = string(activeResources)
}

func hotClaimReservationCapacityPods(reserved *corev1.Pod, claimable int) []*corev1.Pod {
	pods := make([]*corev1.Pod, 0, claimable+1)
	pods = append(pods, reserved)
	for index := range claimable {
		pods = append(pods, newClaimTestPod(
			reserved.Namespace,
			fmt.Sprintf("idle-ready-%d", index),
			reserved.Labels[controller.LabelTemplateID],
			true,
		))
	}
	return pods
}

type recordingHotClaimDetachmentPacer struct {
	calls int
	err   error
}

func (p *recordingHotClaimDetachmentPacer) Wait(context.Context) error {
	p.calls++
	return p.err
}

func hotClaimReservationTestRecord(pod *corev1.Pod) *SandboxRecord {
	return &SandboxRecord{
		ID:                  sandboxIDFromPod(pod),
		Status:              SandboxStatusRunning,
		CurrentPodNamespace: pod.Namespace,
		CurrentPodName:      pod.Name,
		RuntimeGeneration:   runtimeGenerationFromPod(pod),
		CreatedAt:           pod.CreationTimestamp.Time,
	}
}
