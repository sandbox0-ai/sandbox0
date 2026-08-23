package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCompleteHotClaimReservationPersistsCompletionMarkerWithoutPodPatch(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateInitializing)
	pod.Annotations[controller.AnnotationHotClaimCompletionProtocol] = controller.HotClaimCompletionProtocolRecordV2
	client := fake.NewSimpleClientset(pod.DeepCopy())
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{}}
	service := &SandboxService{
		k8sClient:    client,
		sandboxStore: store,
		clock:        fixedClock{now: now},
	}
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: pod.Namespace},
	}
	req := &ClaimRequest{
		SandboxID:         "sandbox-a",
		Template:          "default",
		TeamID:            "team-a",
		UserID:            "user-a",
		RuntimeGeneration: 1,
	}

	if err := service.completeHotClaimReservation(context.Background(), pod, template, req); err != nil {
		t.Fatalf("completeHotClaimReservation() error = %v", err)
	}
	record, err := store.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if record == nil || record.DesiredState != sandboxstore.SandboxDesiredStateActive || !record.HotClaimCompletedAt.Equal(now) {
		t.Fatalf("sandbox record = %#v, want active with a completion marker", record)
	}
	if actions := client.Actions(); len(actions) != 0 {
		t.Fatalf("Kubernetes actions = %v, want none on completion path", actions)
	}
	if got := pod.Annotations[controller.AnnotationHotClaimReservationState]; got != controller.HotClaimReservationStateInitializing {
		t.Fatalf("reservation state = %q, want unchanged initializing", got)
	}
}

func TestSandboxRecordForHotClaimStartsActiveWithoutCompletionMarker(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateInitializing)
	pod.Annotations[controller.AnnotationHotClaimCompletionProtocol] = controller.HotClaimCompletionProtocolRecordV2
	service := &SandboxService{clock: fixedClock{now: now}}
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: pod.Namespace},
	}

	record := sandboxRecordForClaimedPod(service, pod, template, &ClaimRequest{
		SandboxID:         "sandbox-a",
		Template:          "default",
		TeamID:            "team-a",
		UserID:            "user-a",
		RuntimeGeneration: 1,
	})
	if record.DesiredState != sandboxstore.SandboxDesiredStateActive || !record.HotClaimCompletedAt.IsZero() {
		t.Fatalf("sandbox record = %#v, want active without a completion marker", record)
	}
}

func TestHotClaimReservationControllerFinalizesRecordCompletedClaim(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateInitializing)
	pod.Annotations[controller.AnnotationHotClaimCompletionProtocol] = controller.HotClaimCompletionProtocolRecordV2
	record := hotClaimReservationTestRecord(pod)
	record.HotClaimCompletedAt = now
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{"sandbox-a": record}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	reconciler := NewHotClaimReservationController(client, newClaimTestPodLister(t, pod), store, nil)
	reconciler.clock = fixedClock{now: now}
	reconciler.settleWindow = 0
	reconciler.detachPacer = &recordingHotClaimDetachmentPacer{}

	if _, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name); err != nil {
		t.Fatalf("reconcile() error = %v", err)
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
	if _, exists := got.Annotations[controller.AnnotationHotClaimCompletionProtocol]; exists {
		t.Fatalf("completion protocol remains after finalization: %#v", got.Annotations)
	}
}

func TestCommittedHotClaimResumeSurvivesReservationRecoveryGrace(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(
		now.Add(-hotClaimReservationRecoveryGracePeriod-time.Second),
		controller.HotClaimReservationStateInitializing,
	)
	pod.Annotations[controller.AnnotationHotClaimCompletionProtocol] = controller.HotClaimCompletionProtocolRecordV2
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "2"
	record := hotClaimReservationTestRecord(pod)
	record.DesiredState = sandboxstore.SandboxDesiredStatePaused
	record.RuntimeID = ""
	record.RuntimeNamespace = ""
	record.RuntimeGeneration = 1
	txn := &sandboxstore.SandboxLifecycleTxn{
		ID:                 "resume-txn-sandbox-a",
		SandboxID:          record.ID,
		Kind:               sandboxstore.SandboxLifecycleKindResume,
		Phase:              sandboxstore.SandboxLifecyclePhasePreparing,
		FromGeneration:     1,
		ToGeneration:       2,
		ToRuntimeNamespace: pod.Namespace,
		ToRuntimeID:        pod.Name,
	}
	store := &memorySandboxStore{
		records:       map[string]*sandboxstore.SandboxRecord{record.ID: record},
		lifecycleTxns: map[string]*sandboxstore.SandboxLifecycleTxn{txn.ID: txn},
	}
	service := &SandboxService{
		sandboxStore: store,
		clock:        fixedClock{now: now},
	}

	if err := service.commitResumedSandboxRuntime(context.Background(), pod, record, txn); err != nil {
		t.Fatalf("commitResumedSandboxRuntime() error = %v", err)
	}
	gotRecord, err := store.GetSandbox(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if gotRecord.DesiredState != sandboxstore.SandboxDesiredStateActive ||
		gotRecord.RuntimeNamespace != pod.Namespace ||
		gotRecord.RuntimeID != pod.Name ||
		gotRecord.RuntimeGeneration != 2 ||
		!gotRecord.HotClaimCompletedAt.Equal(now) {
		t.Fatalf("sandbox record = %#v, want committed hot resume with completion marker", gotRecord)
	}
	if gotTxn := store.lifecycleTxns[txn.ID]; gotTxn == nil || gotTxn.Phase != sandboxstore.SandboxLifecyclePhaseCommitted {
		t.Fatalf("lifecycle transaction = %#v, want committed", gotTxn)
	}

	client := fake.NewSimpleClientset(pod.DeepCopy())
	reconciler := NewHotClaimReservationController(client, newClaimTestPodLister(t, pod), store, nil)
	reconciler.clock = fixedClock{now: now}
	reconciler.settleWindow = 0
	reconciler.detachPacer = &recordingHotClaimDetachmentPacer{}

	if _, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name); err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	gotPod, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get finalized pod: %v", err)
	}
	if gotPod.Labels[controller.LabelPoolType] != controller.PoolTypeActive || controller.IsHotClaimReservedPod(gotPod) {
		t.Fatalf("finalized pod = %#v, want active without a hot claim reservation", gotPod)
	}
	gotRecord, err = store.GetSandbox(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("GetSandbox() after reconcile error = %v", err)
	}
	if gotRecord.DesiredState != sandboxstore.SandboxDesiredStateActive || !gotRecord.DeletedAt.IsZero() {
		t.Fatalf("sandbox record after recovery grace = %#v, want active and not deleted", gotRecord)
	}
}

func TestHotClaimReservationControllerDoesNotCompleteUnsafeInitializingClaim(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		protocol  string
		completed bool
	}{
		{
			name:      "completion marker lacks completion protocol",
			completed: true,
		},
		{
			name:     "record protocol lacks completion marker",
			protocol: controller.HotClaimCompletionProtocolRecordV2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateInitializing)
			if test.protocol != "" {
				pod.Annotations[controller.AnnotationHotClaimCompletionProtocol] = test.protocol
			}
			record := hotClaimReservationTestRecord(pod)
			if test.completed {
				record.HotClaimCompletedAt = now
			}
			store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{"sandbox-a": record}}
			client := fake.NewSimpleClientset(pod.DeepCopy())
			reconciler := NewHotClaimReservationController(client, newClaimTestPodLister(t, pod), store, nil)
			reconciler.clock = fixedClock{now: now}

			requeueAfter, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name)
			if err != nil {
				t.Fatalf("reconcile() error = %v", err)
			}
			if requeueAfter != hotClaimReservationRecoveryGracePeriod {
				t.Fatalf("requeueAfter = %s, want %s", requeueAfter, hotClaimReservationRecoveryGracePeriod)
			}
			got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get reserved pod: %v", err)
			}
			if got.Labels[controller.LabelPoolType] != controller.PoolTypeIdle {
				t.Fatalf("pool type = %q, want idle", got.Labels[controller.LabelPoolType])
			}
		})
	}
}

func TestHotClaimReservationControllerFinalizesDurableReadyClaim(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateReady)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
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
	reconciler.settleWindow = 0
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

func TestHotClaimReservationControllerWaitsForSettleWindowWithHealthyCapacity(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateReady)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
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

func TestHotClaimReservationControllerBypassesSettleWindowBelowLowWatermark(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(now, controller.HotClaimReservationStateReady)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": hotClaimReservationTestRecord(pod),
	}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	reconciler := NewHotClaimReservationController(
		client,
		newClaimTestPodLister(t, hotClaimReservationCapacityPods(pod, hotClaimDetachmentLowWatermark-1)...),
		store,
		nil,
	)
	reconciler.clock = fixedClock{now: now}
	reconciler.detachPacer = &recordingHotClaimDetachmentPacer{}

	requeueAfter, err := reconciler.reconcile(context.Background(), pod.Namespace+"/"+pod.Name)
	if err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	if requeueAfter != 0 {
		t.Fatalf("reconcile() requeueAfter = %s, want immediate low-water detachment", requeueAfter)
	}
	got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get finalized pod: %v", err)
	}
	if got.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		t.Fatalf("pool type = %q, want active below low watermark", got.Labels[controller.LabelPoolType])
	}
}

func TestHotClaimReservationControllerMaxDelayBypassesPacer(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pod := newHotClaimReservationTestPod(
		now.Add(-hotClaimDetachmentMaxDelay-time.Second),
		controller.HotClaimReservationStateReady,
	)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
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
		&memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{}},
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
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{"sandbox-a": record}}
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
	if gotRecord.DesiredState != sandboxstore.SandboxDesiredStateDeleted || gotRecord.DeletedAt.IsZero() {
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
	record.DesiredState = sandboxstore.SandboxDesiredStatePaused
	record.RuntimeID = ""
	record.RuntimeNamespace = ""
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{"sandbox-a": record}}
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
	if gotRecord.DesiredState != sandboxstore.SandboxDesiredStatePaused || !gotRecord.DeletedAt.IsZero() {
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

func hotClaimReservationTestRecord(pod *corev1.Pod) *sandboxstore.SandboxRecord {
	return &sandboxstore.SandboxRecord{
		ID:                sandboxPodID(pod),
		DesiredState:      sandboxstore.SandboxDesiredStateActive,
		RuntimeNamespace:  pod.Namespace,
		RuntimeID:         pod.Name,
		RuntimeGeneration: runtimeGenerationFromPod(pod),
		CreatedAt:         pod.CreationTimestamp.Time,
	}
}
