package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

type fakeHotClaimReservationStore struct {
	*memorySandboxStore
	reservationMu sync.Mutex
	reservations  map[string]*HotClaimReservation
}

func newFakeHotClaimReservationStore() *fakeHotClaimReservationStore {
	return &fakeHotClaimReservationStore{
		memorySandboxStore: &memorySandboxStore{},
	}
}

func (s *fakeHotClaimReservationStore) TryReserveHotClaim(_ context.Context, reservation *HotClaimReservation) (bool, error) {
	s.reservationMu.Lock()
	defer s.reservationMu.Unlock()
	if s.reservations == nil {
		s.reservations = make(map[string]*HotClaimReservation)
	}
	if _, exists := s.reservations[reservation.SandboxID]; exists {
		return false, nil
	}
	for _, existing := range s.reservations {
		if existing.ClusterID == reservation.ClusterID &&
			existing.Namespace == reservation.Namespace &&
			existing.PodName == reservation.PodName {
			return false, nil
		}
	}
	s.reservations[reservation.SandboxID] = cloneHotClaimReservation(reservation)
	return true, nil
}

func (s *fakeHotClaimReservationStore) CommitHotClaim(_ context.Context, record *SandboxRecord, podUID types.UID, metadata HotClaimPodMetadata) error {
	s.reservationMu.Lock()
	reservation := s.reservations[record.ID]
	if reservation == nil || reservation.PodUID != podUID {
		s.reservationMu.Unlock()
		return ErrSandboxRecordNotFound
	}
	reservation.Metadata = cloneHotClaimPodMetadata(metadata)
	reservation.CommittedAt = time.Now().UTC()
	s.reservationMu.Unlock()
	return s.UpsertSandbox(context.Background(), record)
}

func (s *fakeHotClaimReservationStore) GetHotClaimReservation(_ context.Context, sandboxID string) (*HotClaimReservation, error) {
	s.reservationMu.Lock()
	defer s.reservationMu.Unlock()
	return cloneHotClaimReservation(s.reservations[sandboxID]), nil
}

func (s *fakeHotClaimReservationStore) ListHotClaimReservations(_ context.Context, clusterID string, createdBefore time.Time, limit int) ([]*HotClaimReservation, error) {
	s.reservationMu.Lock()
	defer s.reservationMu.Unlock()
	reservations := make([]*HotClaimReservation, 0)
	for _, reservation := range s.reservations {
		if reservation.ClusterID != clusterID {
			continue
		}
		if reservation.CreatedAt.After(createdBefore) {
			continue
		}
		reservations = append(reservations, cloneHotClaimReservation(reservation))
		if len(reservations) == limit {
			break
		}
	}
	return reservations, nil
}

func (s *fakeHotClaimReservationStore) ReleaseHotClaimReservation(_ context.Context, sandboxID string, podUID types.UID) error {
	s.reservationMu.Lock()
	defer s.reservationMu.Unlock()
	reservation := s.reservations[sandboxID]
	if reservation != nil && reservation.PodUID == podUID {
		delete(s.reservations, sandboxID)
	}
	return nil
}

func cloneHotClaimReservation(reservation *HotClaimReservation) *HotClaimReservation {
	if reservation == nil {
		return nil
	}
	cloned := *reservation
	cloned.Metadata = cloneHotClaimPodMetadata(reservation.Metadata)
	return &cloned
}

func cloneHotClaimPodMetadata(metadata HotClaimPodMetadata) HotClaimPodMetadata {
	return HotClaimPodMetadata{
		Labels:      cloneHotClaimStringMap(metadata.Labels),
		Annotations: cloneHotClaimStringMap(metadata.Annotations),
		Finalizers:  append([]string(nil), metadata.Finalizers...),
	}
}

func TestClaimIdlePodUsesDurableReservationBeforeKubernetesMetadata(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "ns-a",
		},
	}
	readyPod := newClaimTestPod("ns-a", "idle-ready", "template-a", true)
	readyPod.UID = types.UID("pod-uid")
	readyPod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "ReplicaSet",
		Name:       "pool-template-a",
		UID:        types.UID("pool-uid"),
	}}
	store := newFakeHotClaimReservationStore()
	client := fake.NewSimpleClientset(readyPod.DeepCopy())
	svc := &SandboxService{
		k8sClient:                client,
		podLister:                newClaimTestPodLister(t, readyPod),
		sandboxStore:             store,
		NetworkPolicyService:     NewNetworkPolicyService(zap.NewNop()),
		hotClaimReservationStore: store,
		clock:                    systemTime{},
		logger:                   zap.NewNop(),
	}
	req := &ClaimRequest{
		TeamID:                     "team-a",
		UserID:                     "user-a",
		SandboxID:                  "sandbox-a",
		RuntimeGeneration:          1,
		deferHotRuntimePreparation: true,
	}

	claimed, err := svc.claimIdlePod(context.Background(), template, req)
	if err != nil {
		t.Fatalf("claimIdlePod() error = %v", err)
	}
	if claimed == nil {
		t.Fatal("claimIdlePod() = nil, want claimed pod")
	}
	if req.pendingHotClaimFinalization == nil || !req.pendingHotClaimFinalization.durable {
		t.Fatal("durable hot claim finalization was not recorded")
	}
	if claimed.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		t.Fatalf("returned pool type = %q, want active", claimed.Labels[controller.LabelPoolType])
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "pods" && (action.GetVerb() == "patch" || action.GetVerb() == "update") {
			t.Fatalf("unexpected Kubernetes mutation on durable reservation path: %s", action.GetVerb())
		}
	}

	live, err := client.CoreV1().Pods(readyPod.Namespace).Get(context.Background(), readyPod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get live pod: %v", err)
	}
	if live.Labels[controller.LabelPoolType] != controller.PoolTypeIdle {
		t.Fatalf("live pool type = %q, want idle before commit", live.Labels[controller.LabelPoolType])
	}
	if sandboxIDFromPod(live) == req.SandboxID {
		t.Fatal("live pod exposed sandbox identity before reservation commit")
	}

	record := &SandboxRecord{
		ID:                  req.SandboxID,
		TeamID:              req.TeamID,
		TemplateID:          template.Name,
		TemplateName:        template.Name,
		TemplateNamespace:   template.Namespace,
		Status:              SandboxStatusStarting,
		CurrentPodName:      claimed.Name,
		CurrentPodNamespace: claimed.Namespace,
	}
	if err := store.CommitHotClaim(context.Background(), record, req.pendingHotClaimFinalization.podUID, req.pendingHotClaimFinalization.metadata); err != nil {
		t.Fatalf("CommitHotClaim() error = %v", err)
	}
	materialized, err := svc.getSandboxPod(context.Background(), req.SandboxID)
	if err != nil {
		t.Fatalf("getSandboxPod() error = %v", err)
	}
	if sandboxIDFromPod(materialized) != req.SandboxID {
		t.Fatalf("materialized sandbox ID = %q, want %q", sandboxIDFromPod(materialized), req.SandboxID)
	}

	if err := svc.finalizePendingHotClaim(context.Background(), *req.pendingHotClaimFinalization); err != nil {
		t.Fatalf("finalizePendingHotClaim() error = %v", err)
	}
	live, err = client.CoreV1().Pods(readyPod.Namespace).Get(context.Background(), readyPod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get finalized pod: %v", err)
	}
	if live.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		t.Fatalf("finalized pool type = %q, want active", live.Labels[controller.LabelPoolType])
	}
	if sandboxIDFromPod(live) != req.SandboxID {
		t.Fatalf("finalized sandbox ID = %q, want %q", sandboxIDFromPod(live), req.SandboxID)
	}
	if len(live.OwnerReferences) != 0 {
		t.Fatalf("finalized owner references = %v, want none", live.OwnerReferences)
	}
	reservation, err := store.GetHotClaimReservation(context.Background(), req.SandboxID)
	if err != nil {
		t.Fatalf("GetHotClaimReservation() error = %v", err)
	}
	if reservation != nil {
		t.Fatal("hot claim reservation was not released after finalization")
	}
	liveThroughStaleCache, err := svc.getSandboxPod(context.Background(), req.SandboxID)
	if err != nil {
		t.Fatalf("getSandboxPod() after reservation release error = %v", err)
	}
	if sandboxIDFromPod(liveThroughStaleCache) != req.SandboxID {
		t.Fatalf("stale-cache sandbox ID = %q, want %q", sandboxIDFromPod(liveThroughStaleCache), req.SandboxID)
	}
}

func TestDurableHotClaimFastPathRejectsRestrictedNetworkPolicy(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "ns-a",
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			Network: &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeBlockAll},
		},
	}
	readyPod := newClaimTestPod("ns-a", "idle-ready", "template-a", true)
	readyPod.UID = types.UID("pod-uid")
	templateHash, err := controller.TemplateSpecHash(template)
	if err != nil {
		t.Fatalf("template hash: %v", err)
	}
	readyPod.Annotations[controller.AnnotationTemplateSpecHash] = templateHash
	store := newFakeHotClaimReservationStore()
	client := fake.NewSimpleClientset(readyPod.DeepCopy())
	svc := &SandboxService{
		k8sClient:                client,
		podLister:                newClaimTestPodLister(t, readyPod),
		NetworkPolicyService:     NewNetworkPolicyService(zap.NewNop()),
		hotClaimReservationStore: store,
		clock:                    systemTime{},
		logger:                   zap.NewNop(),
	}
	req := &ClaimRequest{
		TeamID:                     "team-a",
		UserID:                     "user-a",
		SandboxID:                  "sandbox-a",
		RuntimeGeneration:          1,
		deferHotRuntimePreparation: true,
	}

	claimed, err := svc.claimIdlePod(context.Background(), template, req)
	if err != nil {
		t.Fatalf("claimIdlePod() error = %v", err)
	}
	if claimed == nil {
		t.Fatal("claimIdlePod() = nil, want claimed pod")
	}
	if req.pendingHotClaimFinalization == nil || req.pendingHotClaimFinalization.durable {
		t.Fatal("restricted policy unexpectedly used durable deferred metadata")
	}
	patches := 0
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "pods" && action.GetVerb() == "patch" {
			patches++
		}
	}
	if patches != 1 {
		t.Fatalf("Kubernetes metadata patches = %d, want 1 for restricted policy", patches)
	}
}

func TestReconcileHotClaimReservationsDeletesAbandonedPod(t *testing.T) {
	pod := newClaimTestPod("ns-a", "idle-ready", "template-a", true)
	pod.UID = types.UID("pod-uid")
	store := newFakeHotClaimReservationStore()
	store.reservations = map[string]*HotClaimReservation{
		"sandbox-a": {
			SandboxID: "sandbox-a",
			TeamID:    "team-a",
			ClusterID: "default",
			Namespace: pod.Namespace,
			PodName:   pod.Name,
			PodUID:    pod.UID,
			CreatedAt: time.Now().Add(-hotClaimReservationAbandonAfter - time.Second),
		},
	}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	svc := &SandboxService{
		k8sClient:                client,
		hotClaimReservationStore: store,
		logger:                   zap.NewNop(),
	}

	svc.reconcileHotClaimReservations(context.Background())
	if _, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{}); err == nil {
		t.Fatal("abandoned reserved pod was not deleted")
	}
	reservation, err := store.GetHotClaimReservation(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetHotClaimReservation() error = %v", err)
	}
	if reservation != nil {
		t.Fatal("abandoned reservation was not released")
	}
}
