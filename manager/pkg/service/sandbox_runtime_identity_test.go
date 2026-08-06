package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

type memorySandboxStore struct {
	mu                sync.Mutex
	records           map[string]*sandboxstore.SandboxRecord
	lifecycleTxns     map[string]*sandboxstore.SandboxLifecycleTxn
	rootFSStates      map[string]*sandboxstore.SandboxRootFSState
	rootFSFilesystems map[string]*sandboxstore.RootFSFilesystem
	rootFSSnapshots   map[string]*sandboxstore.RootFSSnapshot
	deletes           []string
	saves             int
	pauses            int
	lockCalls         int
	activeTxnGets     int
	lockStarted       chan struct{}
	blockLock         chan struct{}
}

type memorySandboxStoreTx struct {
	store *memorySandboxStore
}

func (t memorySandboxStoreTx) SaveSandbox(_ context.Context, record *sandboxstore.SandboxRecord) error {
	return t.store.UpsertSandbox(context.Background(), record)
}

func (s *memorySandboxStore) UpsertSandbox(_ context.Context, record *sandboxstore.SandboxRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[string]*sandboxstore.SandboxRecord)
	}
	if existing := s.records[record.ID]; existing != nil &&
		(existing.DesiredState == sandboxstore.SandboxDesiredStateTerminating || existing.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !existing.DeletedAt.IsZero()) {
		return nil
	}
	clone := cloneSandboxRecord(record)
	if existing := s.records[record.ID]; existing != nil && clone.HotClaimCompletedAt.IsZero() {
		clone.HotClaimCompletedAt = existing.HotClaimCompletedAt
	}
	s.records[record.ID] = clone
	return nil
}

func (s *memorySandboxStore) GetSandbox(_ context.Context, sandboxID string) (*sandboxstore.SandboxRecord, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		return nil, nil
	}
	return cloneSandboxRecord(s.records[sandboxID]), nil
}

func (s *memorySandboxStore) ListSandboxes(_ context.Context, req *sandboxstore.ListSandboxesRequest) ([]*sandboxstore.SandboxRecord, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		return nil, nil
	}
	var records []*sandboxstore.SandboxRecord
	for _, record := range s.records {
		if record == nil {
			continue
		}
		if req != nil {
			if req.TeamID != "" && record.TeamID != req.TeamID {
				continue
			}
			if req.TemplateID != "" && record.TemplateID != req.TemplateID {
				continue
			}
		}
		records = append(records, cloneSandboxRecord(record))
	}
	return records, nil
}

func (s *memorySandboxStore) ListHardExpiredSandboxes(context.Context, time.Time, int) ([]*sandboxstore.SandboxRecord, error) {
	return nil, nil
}

func (s *memorySandboxStore) ListActiveLifecycleTxns(_ context.Context, kind string, limit int) ([]*sandboxstore.SandboxLifecycleTxn, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lifecycleTxns == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = len(s.lifecycleTxns)
	}
	txns := make([]*sandboxstore.SandboxLifecycleTxn, 0, len(s.lifecycleTxns))
	for _, txn := range s.lifecycleTxns {
		if txn == nil || txn.Kind != kind || !sandboxLifecyclePhaseActive(txn.Phase) {
			continue
		}
		txns = append(txns, sandboxstore.CloneSandboxLifecycleTxn(txn))
		if len(txns) >= limit {
			break
		}
	}
	return txns, nil
}

func (s *memorySandboxStore) ListPendingRuntimeRecoverySandboxIDs(_ context.Context, limit int) ([]string, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = len(s.records)
	}
	var sandboxIDs []string
	for sandboxID, record := range s.records {
		if record == nil || record.DesiredState != sandboxstore.SandboxDesiredStatePaused || !record.DeletedAt.IsZero() {
			continue
		}
		var latest *sandboxstore.SandboxLifecycleTxn
		for _, txn := range s.lifecycleTxns {
			if txn == nil || txn.SandboxID != sandboxID || txn.Phase != sandboxstore.SandboxLifecyclePhaseCommitted {
				continue
			}
			if latest == nil || txn.Epoch > latest.Epoch {
				latest = txn
			}
		}
		if latest == nil || latest.Kind != sandboxstore.SandboxLifecycleKindPause ||
			!sandboxLifecycleSourceReconstructsRuntime(latest.Source) {
			continue
		}
		sandboxIDs = append(sandboxIDs, sandboxID)
		if len(sandboxIDs) >= limit {
			break
		}
	}
	return sandboxIDs, nil
}

func (s *memorySandboxStore) GetActiveLifecycleTxn(_ context.Context, sandboxID string) (*sandboxstore.SandboxLifecycleTxn, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeTxnGets++
	for _, txn := range s.lifecycleTxns {
		if txn != nil && txn.SandboxID == sandboxID && sandboxLifecyclePhaseActive(txn.Phase) {
			return sandboxstore.CloneSandboxLifecycleTxn(txn), nil
		}
	}
	return nil, nil
}

func (s *memorySandboxStore) MarkSandboxDeleted(_ context.Context, sandboxID string, deletedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[string]*sandboxstore.SandboxRecord)
	}
	record := s.records[sandboxID]
	if record == nil {
		record = &sandboxstore.SandboxRecord{ID: sandboxID}
		s.records[sandboxID] = record
	}
	record.DesiredState = sandboxstore.SandboxDesiredStateDeleted
	record.DeletedAt = deletedAt
	record.CurrentPodName = ""
	record.CurrentPodNamespace = ""
	for _, txn := range s.lifecycleTxns {
		if txn != nil && txn.SandboxID == sandboxID && sandboxLifecyclePhaseActive(txn.Phase) {
			txn.Phase = sandboxstore.SandboxLifecyclePhaseAborted
			txn.Error = "sandbox deleted"
			txn.AbortedAt = deletedAt
		}
	}
	delete(s.rootFSStates, sandboxID)
	s.deletes = append(s.deletes, sandboxID)
	return nil
}

func (s *memorySandboxStore) SaveRootFSState(_ context.Context, state *sandboxstore.SandboxRootFSState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rootFSStates == nil {
		s.rootFSStates = make(map[string]*sandboxstore.SandboxRootFSState)
	}
	s.rootFSStates[state.SandboxID] = cloneSandboxRootFSState(state)
	return nil
}

func (s *memorySandboxStore) GetLatestRootFSState(_ context.Context, sandboxID string) (*sandboxstore.SandboxRootFSState, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rootFSStates == nil {
		return nil, nil
	}
	return cloneSandboxRootFSState(s.rootFSStates[sandboxID]), nil
}

func (s *memorySandboxStore) WithSandboxLock(ctx context.Context, sandboxID string, fn func(context.Context, sandboxstore.SandboxStoreTx, *sandboxstore.SandboxRecord) error) error {
	if s == nil {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	s.mu.Lock()
	s.lockCalls++
	if s.lockStarted != nil {
		select {
		case s.lockStarted <- struct{}{}:
		default:
		}
	}
	blockLock := s.blockLock
	record := cloneSandboxRecord(s.records[sandboxID])
	s.mu.Unlock()
	if blockLock != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-blockLock:
		}
	}
	if record == nil {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	snapshot := s.snapshot()
	if err := fn(ctx, memorySandboxStoreTx{store: s}, record); err != nil {
		s.restore(snapshot)
		return err
	}
	return nil
}

type memorySandboxStoreSnapshot struct {
	records           map[string]*sandboxstore.SandboxRecord
	lifecycleTxns     map[string]*sandboxstore.SandboxLifecycleTxn
	rootFSStates      map[string]*sandboxstore.SandboxRootFSState
	rootFSFilesystems map[string]*sandboxstore.RootFSFilesystem
}

func (s *memorySandboxStore) snapshot() memorySandboxStoreSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return memorySandboxStoreSnapshot{
		records:           cloneSandboxRecordMap(s.records),
		lifecycleTxns:     cloneSandboxLifecycleTxnMap(s.lifecycleTxns),
		rootFSStates:      cloneSandboxRootFSStateMap(s.rootFSStates),
		rootFSFilesystems: cloneRootFSFilesystemMap(s.rootFSFilesystems),
	}
}

func (s *memorySandboxStore) restore(snapshot memorySandboxStoreSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = snapshot.records
	s.lifecycleTxns = snapshot.lifecycleTxns
	s.rootFSStates = snapshot.rootFSStates
	s.rootFSFilesystems = snapshot.rootFSFilesystems
}

func (s *memorySandboxStore) setSandboxDesiredState(sandboxID, desiredState string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record := s.records[sandboxID]; record != nil {
		record.DesiredState = desiredState
	}
}

func (t memorySandboxStoreTx) SaveRuntime(_ context.Context, sandboxID, namespace, podName string, generation int64, expiresAt, hardExpiresAt time.Time, metadata sandboxstore.SandboxRuntimeMetadata) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	record := t.store.records[sandboxID]
	if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateTerminating || !record.DeletedAt.IsZero() {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	record.CurrentPodNamespace = namespace
	record.CurrentPodName = podName
	record.DesiredState = sandboxstore.SandboxDesiredStateActive
	record.RuntimeGeneration = generation
	record.ExpiresAt = expiresAt
	record.HardExpiresAt = hardExpiresAt
	if metadata.WebhookStateVolumeID != "" {
		record.WebhookStateVolumeID = metadata.WebhookStateVolumeID
	}
	if metadata.OwnerKind != "" {
		record.OwnerKind = metadata.OwnerKind
	}
	t.store.saves++
	return nil
}

func (t memorySandboxStoreTx) MarkHotClaimCompleted(_ context.Context, sandboxID string, completedAt time.Time) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	record := t.store.records[sandboxID]
	if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateTerminating || record.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	record.HotClaimCompletedAt = completedAt
	return nil
}

func (t memorySandboxStoreTx) MarkRuntimePaused(_ context.Context, sandboxID string, generation int64, _ time.Time) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	record := t.store.records[sandboxID]
	if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateTerminating || !record.DeletedAt.IsZero() {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	record.CurrentPodNamespace = ""
	record.CurrentPodName = ""
	record.DesiredState = sandboxstore.SandboxDesiredStatePaused
	if record.RuntimeGeneration < generation {
		record.RuntimeGeneration = generation
	}
	t.store.pauses++
	return nil
}

func (t memorySandboxStoreTx) MarkRuntimeTerminating(_ context.Context, sandboxID string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	record := t.store.records[sandboxID]
	if record == nil || !record.DeletedAt.IsZero() {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	record.DesiredState = sandboxstore.SandboxDesiredStateTerminating
	return nil
}

func (t memorySandboxStoreTx) SaveRootFSState(_ context.Context, state *sandboxstore.SandboxRootFSState) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.store.rootFSStates == nil {
		t.store.rootFSStates = make(map[string]*sandboxstore.SandboxRootFSState)
	}
	t.store.rootFSStates[state.SandboxID] = cloneSandboxRootFSState(state)
	return nil
}

func (t memorySandboxStoreTx) GetActiveLifecycleTxn(_ context.Context, sandboxID string) (*sandboxstore.SandboxLifecycleTxn, error) {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	for _, txn := range t.store.lifecycleTxns {
		if txn != nil && txn.SandboxID == sandboxID && sandboxLifecyclePhaseActive(txn.Phase) {
			return sandboxstore.CloneSandboxLifecycleTxn(txn), nil
		}
	}
	return nil, nil
}

func (t memorySandboxStoreTx) BeginLifecycleTxn(_ context.Context, txn *sandboxstore.SandboxLifecycleTxn) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.store.lifecycleTxns == nil {
		t.store.lifecycleTxns = make(map[string]*sandboxstore.SandboxLifecycleTxn)
	}
	record := t.store.records[txn.SandboxID]
	record.LifecycleEpoch++
	txn.Epoch = record.LifecycleEpoch
	if txn.Phase == "" {
		txn.Phase = sandboxstore.SandboxLifecyclePhasePreparing
	}
	if txn.Source == "" {
		txn.Source = sandboxstore.SandboxLifecycleSourceManual
	}
	t.store.lifecycleTxns[txn.ID] = sandboxstore.CloneSandboxLifecycleTxn(txn)
	return nil
}

func (t memorySandboxStoreTx) SetLifecycleTxnRuntime(_ context.Context, txnID, namespace, podName string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if txn := t.store.lifecycleTxns[txnID]; txn != nil && sandboxLifecyclePhaseActive(txn.Phase) {
		txn.ToPodNamespace = namespace
		txn.ToPodName = podName
	}
	return nil
}

func (t memorySandboxStoreTx) UpdateLifecycleTxnPhase(_ context.Context, txnID, phase string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if txn := t.store.lifecycleTxns[txnID]; txn != nil && sandboxLifecyclePhaseActive(txn.Phase) {
		if sandboxLifecycleTxnCancelRequested(txn) {
			return fmt.Errorf("active lifecycle txn %s not found", txnID)
		}
		txn.Phase = phase
	}
	return nil
}

func (t memorySandboxStoreTx) SetLifecycleTxnPreparedHead(_ context.Context, txnID, preparedHeadLayerID string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if txn := t.store.lifecycleTxns[txnID]; txn != nil && sandboxLifecyclePhaseActive(txn.Phase) {
		if sandboxLifecycleTxnCancelRequested(txn) {
			return fmt.Errorf("active lifecycle txn %s not found", txnID)
		}
		txn.PreparedHeadLayerID = preparedHeadLayerID
	}
	return nil
}

func (t memorySandboxStoreTx) RequestLifecycleTxnCancel(_ context.Context, txnID, reason string) (bool, error) {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	txn := t.store.lifecycleTxns[txnID]
	if !sandboxLifecycleTxnCancelableAutoPause(txn) {
		return false, nil
	}
	if txn.CancelRequestedAt.IsZero() {
		txn.CancelRequestedAt = time.Now()
	}
	if txn.CancelReason == "" {
		txn.CancelReason = reason
	}
	return true, nil
}

func (t memorySandboxStoreTx) CommitLifecycleTxn(_ context.Context, txnID, preparedHeadLayerID string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if txn := t.store.lifecycleTxns[txnID]; txn != nil && sandboxLifecyclePhaseActive(txn.Phase) {
		if sandboxLifecycleTxnCancelRequested(txn) {
			return fmt.Errorf("active lifecycle txn %s not found", txnID)
		}
		txn.Phase = sandboxstore.SandboxLifecyclePhaseCommitted
		txn.PreparedHeadLayerID = preparedHeadLayerID
	}
	return nil
}

func (t memorySandboxStoreTx) AbortLifecycleTxn(_ context.Context, txnID, reason string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if txn := t.store.lifecycleTxns[txnID]; txn != nil && sandboxLifecyclePhaseActive(txn.Phase) {
		txn.Phase = sandboxstore.SandboxLifecyclePhaseAborted
		txn.Error = reason
	}
	return nil
}

func cloneSandboxRecord(record *sandboxstore.SandboxRecord) *sandboxstore.SandboxRecord {
	if record == nil {
		return nil
	}
	clone := *record
	if record.Mounts != nil {
		clone.Mounts = append([]managerapi.ClaimMount(nil), record.Mounts...)
	}
	if record.Config.Services != nil {
		clone.Config.Services = append([]managerapi.SandboxAppService(nil), record.Config.Services...)
	}
	clone.Config.Resources = cloneSandboxResourceConfig(record.Config.Resources)
	return &clone
}

func cloneSandboxRecordMap(records map[string]*sandboxstore.SandboxRecord) map[string]*sandboxstore.SandboxRecord {
	if records == nil {
		return nil
	}
	cloned := make(map[string]*sandboxstore.SandboxRecord, len(records))
	for key, record := range records {
		cloned[key] = cloneSandboxRecord(record)
	}
	return cloned
}

func cloneSandboxLifecycleTxnMap(txns map[string]*sandboxstore.SandboxLifecycleTxn) map[string]*sandboxstore.SandboxLifecycleTxn {
	if txns == nil {
		return nil
	}
	cloned := make(map[string]*sandboxstore.SandboxLifecycleTxn, len(txns))
	for key, txn := range txns {
		cloned[key] = sandboxstore.CloneSandboxLifecycleTxn(txn)
	}
	return cloned
}

func sandboxLifecyclePhaseActive(phase string) bool {
	return phase != sandboxstore.SandboxLifecyclePhaseCommitted && phase != sandboxstore.SandboxLifecyclePhaseAborted
}

func cloneSandboxRootFSLayers(layers []*sandboxstore.SandboxRootFSLayer) []*sandboxstore.SandboxRootFSLayer {
	if len(layers) == 0 {
		return nil
	}
	cloned := make([]*sandboxstore.SandboxRootFSLayer, 0, len(layers))
	for _, layer := range layers {
		if layer == nil {
			cloned = append(cloned, nil)
			continue
		}
		copy := *layer
		copy.SnapshotParentChain = append([]string(nil), layer.SnapshotParentChain...)
		cloned = append(cloned, &copy)
	}
	return cloned
}

func cloneSandboxRootFSState(state *sandboxstore.SandboxRootFSState) *sandboxstore.SandboxRootFSState {
	if state == nil {
		return nil
	}
	clone := *state
	if state.SnapshotParentChain != nil {
		clone.SnapshotParentChain = append([]string(nil), state.SnapshotParentChain...)
	}
	clone.LayerChain = cloneSandboxRootFSLayers(state.LayerChain)
	return &clone
}

func cloneSandboxRootFSStateMap(states map[string]*sandboxstore.SandboxRootFSState) map[string]*sandboxstore.SandboxRootFSState {
	if states == nil {
		return nil
	}
	cloned := make(map[string]*sandboxstore.SandboxRootFSState, len(states))
	for key, state := range states {
		cloned[key] = cloneSandboxRootFSState(state)
	}
	return cloned
}

func cloneRootFSFilesystemMap(filesystems map[string]*sandboxstore.RootFSFilesystem) map[string]*sandboxstore.RootFSFilesystem {
	if filesystems == nil {
		return nil
	}
	cloned := make(map[string]*sandboxstore.RootFSFilesystem, len(filesystems))
	for key, filesystem := range filesystems {
		cloned[key] = cloneRootFSFilesystemForTest(filesystem)
	}
	return cloned
}

func TestGetSandboxPodRejectsMultipleActiveRuntimePods(t *testing.T) {
	first := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	second := runtimeIdentityPod("ns-a", "pod-b", "sandbox-a")
	svc := &SandboxService{
		podLister: runtimeIdentityPodLister(t, first, second),
		logger:    zap.NewNop(),
	}

	_, err := svc.getSandboxPod(context.Background(), "sandbox-a")
	if !k8serrors.IsConflict(err) {
		t.Fatalf("getSandboxPod() error = %v, want conflict", err)
	}
}

func TestResumePausedSandboxRuntimeWaitsWhileRuntimeDeleting(t *testing.T) {
	deletionTime := metav1.NewTime(time.Now().UTC())
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.DeletionTimestamp = &deletionTime
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			TeamID:            "team-a",
			UserID:            "user-a",
			TemplateID:        "default",
			TemplateName:      "default",
			TemplateNamespace: "tpl-default",
			DesiredState:      sandboxstore.SandboxDesiredStatePaused,
			TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		},
	}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    runtimeIdentityPodLister(t, pod),
		sandboxStore: store,
		logger:       zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*sandboxLifecycleWaitInterval)
	defer cancel()
	_, err := svc.ResumePausedSandboxRuntime(ctx, "sandbox-a")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want context deadline", err)
	}
	if k8serrors.IsConflict(err) {
		t.Fatalf("ResumePausedSandboxRuntime() returned conflict while old runtime is deleting")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "pods" {
			t.Fatalf("unexpected pod create while old runtime is deleting: %#v", action)
		}
	}
	if store.saves != 0 {
		t.Fatalf("store saves = %d, want 0", store.saves)
	}
}

func TestResumePausedSandboxRuntimeDeletesStaleUnhealthyRuntimeBeforeIdentityCheck(t *testing.T) {
	now := time.Date(2026, time.July, 28, 4, 0, 0, 0, time.UTC)
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.UID = "stale-runtime"
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:               v1alpha1.SandboxPodLivenessConditionType,
		Status:             corev1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(now.Add(-2 * time.Minute)),
	}}
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			TeamID:            "team-a",
			UserID:            "user-a",
			TemplateID:        "default",
			TemplateName:      "default",
			TemplateNamespace: "tpl-default",
			DesiredState:      sandboxstore.SandboxDesiredStatePaused,
			RuntimeGeneration: 1,
			TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		},
	}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		deletionTime := metav1.NewTime(now)
		pod.DeletionTimestamp = &deletionTime
		return true, nil, nil
	})
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    runtimeIdentityPodLister(t, pod),
		sandboxStore: store,
		clock:        fixedClock{now: now},
		logger:       zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*sandboxLifecycleWaitInterval)
	defer cancel()
	_, err := svc.ResumePausedSandboxRuntime(ctx, "sandbox-a")

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want context deadline while stale pod deletion is observed", err)
	}
	if strings.Contains(err.Error(), "runtime identity changed") {
		t.Fatalf("ResumePausedSandboxRuntime() checked stale runtime identity before deletion: %v", err)
	}
	var deleted bool
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "pods" {
			deleted = true
		}
	}
	if !deleted {
		t.Fatal("stale unhealthy runtime pod was not deleted")
	}
	if len(store.lifecycleTxns) != 0 {
		t.Fatalf("lifecycle transactions = %#v, want none", store.lifecycleTxns)
	}
}

func TestResumePausedSandboxRuntimeReplacesFailedRuntime(t *testing.T) {
	failedPod := runtimeIdentityPod("tpl-default", "failed-pod", "sandbox-a")
	failedPod.Annotations[controller.AnnotationRuntimeGeneration] = "3"
	failedPod.Status.Phase = corev1.PodFailed
	idlePod := newClaimTestPod("tpl-default", "idle-pod", "default", true)
	indexer := newClaimTestPodIndexer(t, failedPod, idlePod)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": {
			ID:                  "sandbox-a",
			TeamID:              "team-a",
			UserID:              "user-a",
			TemplateID:          "default",
			TemplateName:        "default",
			TemplateNamespace:   "tpl-default",
			DesiredState:        sandboxstore.SandboxDesiredStateActive,
			CurrentPodName:      failedPod.Name,
			CurrentPodNamespace: failedPod.Namespace,
			RuntimeGeneration:   3,
			TemplateSpec:        v1alpha1.SandboxTemplateSpec{},
		},
	}}
	client := fake.NewSimpleClientset(failedPod.DeepCopy(), idlePod.DeepCopy())
	deletedFailedPod := false
	client.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		deleteAction, ok := action.(ktesting.DeleteAction)
		if !ok || deleteAction.GetName() != failedPod.Name {
			return false, nil, nil
		}
		record, err := store.GetSandbox(context.Background(), "sandbox-a")
		if err != nil {
			t.Errorf("GetSandbox() before failed pod delete error = %v", err)
		} else if record.DesiredState != sandboxstore.SandboxDesiredStatePaused {
			t.Errorf("desired state before failed pod delete = %q, want paused", record.DesiredState)
		}
		deletedFailedPod = true
		if err := indexer.Delete(failedPod); err != nil {
			return true, nil, err
		}
		return false, nil, nil
	})
	observedTxn := make(chan *sandboxstore.SandboxLifecycleTxn, 1)
	client.PrependReactor("patch", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		patchAction, ok := action.(ktesting.PatchAction)
		if !ok || patchAction.GetName() != idlePod.Name {
			return false, nil, nil
		}
		txn, err := store.GetActiveLifecycleTxn(context.Background(), "sandbox-a")
		if err != nil {
			t.Errorf("GetActiveLifecycleTxn() error = %v", err)
		}
		observedTxn <- txn
		return true, nil, errors.New("stop replacement claim")
	})
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    corelisters.NewPodLister(indexer),
		sandboxStore: store,
		config:       SandboxServiceConfig{ProcdPort: 49983},
		clock:        fixedClock{now: time.Date(2026, time.March, 7, 12, 0, 0, 0, time.UTC)},
		logger:       zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := svc.ResumePausedSandboxRuntime(ctx, "sandbox-a")
	if err == nil || !strings.Contains(err.Error(), "stop replacement claim") {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want replacement claim failure", err)
	}
	if !deletedFailedPod {
		t.Fatal("failed runtime pod was not deleted")
	}
	record, err := store.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if record.DesiredState != sandboxstore.SandboxDesiredStatePaused {
		t.Fatalf("desired state = %q, want paused before replacement claim", record.DesiredState)
	}
	if record.CurrentPodName != "" || record.CurrentPodNamespace != "" {
		t.Fatalf("current runtime = %s/%s, want empty", record.CurrentPodNamespace, record.CurrentPodName)
	}
	if record.RuntimeGeneration != 3 {
		t.Fatalf("runtime generation = %d, want 3", record.RuntimeGeneration)
	}
	var txn *sandboxstore.SandboxLifecycleTxn
	select {
	case txn = <-observedTxn:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replacement claim")
	}
	if txn == nil || txn.Kind != sandboxstore.SandboxLifecycleKindResume {
		t.Fatalf("replacement lifecycle txn = %+v, want resume txn", txn)
	}
	if txn.FromGeneration != 3 || txn.ToGeneration != 4 {
		t.Fatalf("replacement generations = %d -> %d, want 3 -> 4", txn.FromGeneration, txn.ToGeneration)
	}
}

func TestResumePausedSandboxRuntimeJoinsResumingRuntime(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	markRuntimeIdentityPodReady(t, pod)
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-a": {
				ID:                "sandbox-a",
				TeamID:            "team-a",
				UserID:            "user-a",
				TemplateID:        "default",
				TemplateName:      "default",
				TemplateNamespace: "tpl-default",
				DesiredState:      sandboxstore.SandboxDesiredStatePaused,
				RuntimeGeneration: 3,
				TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
			},
		},
		lifecycleTxns: map[string]*sandboxstore.SandboxLifecycleTxn{
			"txn-a": {
				ID:             "txn-a",
				SandboxID:      "sandbox-a",
				Kind:           sandboxstore.SandboxLifecycleKindResume,
				Phase:          sandboxstore.SandboxLifecyclePhasePreparing,
				FromGeneration: 3,
				ToGeneration:   4,
				ToPodNamespace: "ns-a",
				ToPodName:      "pod-a",
			},
		},
	}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    runtimeIdentityPodLister(t, pod),
		sandboxStore: store,
		config:       SandboxServiceConfig{ProcdPort: 49983},
		logger:       zap.NewNop(),
	}

	go func() {
		time.Sleep(2 * sandboxLifecycleWaitInterval)
		store.mu.Lock()
		store.lifecycleTxns["txn-a"].Phase = sandboxstore.SandboxLifecyclePhaseCommitted
		store.records["sandbox-a"].CurrentPodName = "pod-a"
		store.records["sandbox-a"].CurrentPodNamespace = "ns-a"
		store.records["sandbox-a"].RuntimeGeneration = 4
		store.mu.Unlock()
		store.setSandboxDesiredState("sandbox-a", sandboxstore.SandboxDesiredStateActive)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sandbox, err := svc.ResumePausedSandboxRuntime(ctx, "sandbox-a")
	if err != nil {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want nil", err)
	}
	if sandbox.Status != managerapi.SandboxStatusRunning {
		t.Fatalf("status = %q, want running", sandbox.Status)
	}
	if sandbox.RuntimeGeneration != 4 {
		t.Fatalf("runtime generation = %d, want 4", sandbox.RuntimeGeneration)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "pods" {
			t.Fatalf("unexpected pod create while joining active resume: %#v", action)
		}
	}
}

func TestResumePausedSandboxRuntimeReconcilesStaleStartingRecord(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	markRuntimeIdentityPodReady(t, pod)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": {
			ID:                  "sandbox-a",
			TeamID:              "team-a",
			UserID:              "user-a",
			TemplateID:          "default",
			TemplateName:        "default",
			TemplateNamespace:   "tpl-default",
			DesiredState:        sandboxstore.SandboxDesiredStateActive,
			CurrentPodName:      "pod-a",
			CurrentPodNamespace: "ns-a",
			RuntimeGeneration:   4,
			TemplateSpec:        v1alpha1.SandboxTemplateSpec{},
		},
	}}
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(pod.DeepCopy()),
		podLister:    runtimeIdentityPodLister(t, pod),
		sandboxStore: store,
		config:       SandboxServiceConfig{ProcdPort: 49983},
		logger:       zap.NewNop(),
	}

	sandbox, err := svc.ResumePausedSandboxRuntime(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want nil", err)
	}
	if sandbox.Status != managerapi.SandboxStatusRunning {
		t.Fatalf("status = %q, want running", sandbox.Status)
	}
	if got := store.records["sandbox-a"].DesiredState; got != sandboxstore.SandboxDesiredStateActive {
		t.Fatalf("stored desired state = %q, want active", got)
	}
	if store.saves != 1 {
		t.Fatalf("store saves = %d, want 1", store.saves)
	}
}

func TestResumePausedSandboxRuntimeReconcilesStaleStartingRecordWithoutPod(t *testing.T) {
	idlePod := newClaimTestPod("tpl-default", "idle-a", "default", true)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			TeamID:            "team-a",
			UserID:            "user-a",
			TemplateID:        "default",
			TemplateName:      "default",
			TemplateNamespace: "tpl-default",
			DesiredState:      sandboxstore.SandboxDesiredStateActive,
			RuntimeGeneration: 3,
			TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		},
	}}
	client := fake.NewSimpleClientset(idlePod.DeepCopy())
	client.PrependReactor("patch", "pods", func(_ ktesting.Action) (bool, runtime.Object, error) {
		if store.pauses != 1 {
			t.Errorf("runtime pause reconciliations = %d, want 1", store.pauses)
		}
		txn, err := store.GetActiveLifecycleTxn(context.Background(), "sandbox-a")
		if err != nil {
			t.Errorf("GetActiveLifecycleTxn() error = %v", err)
		} else if txn == nil || txn.Kind != sandboxstore.SandboxLifecycleKindResume {
			t.Errorf("active lifecycle transaction = %+v, want resume", txn)
		}
		return true, nil, errors.New("stop reconciled hot claim")
	})
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    runtimeIdentityPodLister(t, idlePod),
		sandboxStore: store,
		config:       SandboxServiceConfig{ProcdPort: 49983},
		clock:        fixedClock{now: time.Date(2026, time.March, 7, 12, 0, 0, 0, time.UTC)},
		logger:       zap.NewNop(),
	}

	_, err := svc.ResumePausedSandboxRuntime(context.Background(), "sandbox-a")
	if err == nil || !strings.Contains(err.Error(), "stop reconciled hot claim") {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want hot claim failure", err)
	}
	if store.pauses != 1 {
		t.Fatalf("runtime pause reconciliations = %d, want 1", store.pauses)
	}
	if got := store.records["sandbox-a"].DesiredState; got != sandboxstore.SandboxDesiredStatePaused {
		t.Fatalf("stored desired state after failed claim = %q, want paused", got)
	}
}

func TestRequestPauseSandboxRuntimeReconcilesStaleStartingRecord(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	markRuntimeIdentityPodReady(t, pod)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": {
			ID:                  "sandbox-a",
			TeamID:              "team-a",
			UserID:              "user-a",
			TemplateID:          "default",
			TemplateName:        "default",
			TemplateNamespace:   "tpl-default",
			DesiredState:        sandboxstore.SandboxDesiredStateActive,
			CurrentPodName:      "pod-a",
			CurrentPodNamespace: "ns-a",
			RuntimeGeneration:   4,
			TemplateSpec:        v1alpha1.SandboxTemplateSpec{},
		},
	}}
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		k8sClient:     fake.NewSimpleClientset(pod.DeepCopy()),
		podLister:     runtimeIdentityPodLister(t, pod),
		sandboxStore:  store,
		ctldClient:    ctldapi.NewClientWithTimeout(0),
		pauseEnqueuer: enqueuer,
		config: SandboxServiceConfig{
			CtldEnabled: true,
			ProcdPort:   49983,
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}

	status, err := svc.RequestPauseSandboxRuntime(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("RequestPauseSandboxRuntime() error = %v, want nil", err)
	}
	if status != managerapi.SandboxStatusRunning {
		t.Fatalf("status = %q, want running", status)
	}
	if got := store.records["sandbox-a"].DesiredState; got != sandboxstore.SandboxDesiredStateActive {
		t.Fatalf("stored desired state = %q, want active", got)
	}
	active, err := store.GetActiveLifecycleTxn(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetActiveLifecycleTxn() error = %v", err)
	}
	if active == nil || active.Kind != sandboxstore.SandboxLifecycleKindPause {
		t.Fatalf("active txn = %+v, want pause", active)
	}
	if len(enqueuer.calls) != 1 || enqueuer.calls[0] != "sandbox-a" {
		t.Fatalf("pause queue calls = %#v, want sandbox-a", enqueuer.calls)
	}
}

func TestResumeSandboxSingleflightPreventsConcurrentSandboxLocks(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	markRuntimeIdentityPodReady(t, pod)
	lockStarted := make(chan struct{}, 1)
	blockLock := make(chan struct{})
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-a": {
				ID:                  "sandbox-a",
				TeamID:              "team-a",
				UserID:              "user-a",
				TemplateID:          "default",
				TemplateName:        "default",
				TemplateNamespace:   "tpl-default",
				DesiredState:        sandboxstore.SandboxDesiredStateActive,
				CurrentPodName:      "pod-a",
				CurrentPodNamespace: "ns-a",
				RuntimeGeneration:   4,
				TemplateSpec:        v1alpha1.SandboxTemplateSpec{},
			},
		},
		lockStarted: lockStarted,
		blockLock:   blockLock,
	}
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(pod.DeepCopy()),
		podLister:    runtimeIdentityPodLister(t, pod),
		sandboxStore: store,
		config:       SandboxServiceConfig{ProcdPort: 49983},
		logger:       zap.NewNop(),
	}

	const callers = 16
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			_, err := svc.ResumeSandbox(context.Background(), "sandbox-a")
			errs <- err
		}()
	}

	select {
	case <-lockStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first resume executor")
	}
	time.Sleep(2 * sandboxLifecycleWaitInterval)
	store.mu.Lock()
	lockCalls := store.lockCalls
	store.mu.Unlock()
	if lockCalls != 1 {
		t.Fatalf("sandbox lock calls while first resume is in flight = %d, want 1", lockCalls)
	}

	close(blockLock)
	for i := 0; i < callers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("ResumeSandbox() error = %v", err)
		}
	}
	store.mu.Lock()
	lockCalls = store.lockCalls
	store.mu.Unlock()
	if lockCalls != 1 {
		t.Fatalf("sandbox lock calls after joined resumes = %d, want 1", lockCalls)
	}
}

func TestResumePausedSandboxRuntimeBeginsTransactionBeforeClaimingPod(t *testing.T) {
	idlePod := newClaimTestPod("tpl-default", "idle-a", "default", true)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			TeamID:            "team-a",
			UserID:            "user-a",
			TemplateID:        "default",
			TemplateName:      "default",
			TemplateNamespace: "tpl-default",
			DesiredState:      sandboxstore.SandboxDesiredStatePaused,
			RuntimeGeneration: 3,
			TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		},
	}}
	client := fake.NewSimpleClientset(idlePod.DeepCopy())
	observedTxn := make(chan *sandboxstore.SandboxLifecycleTxn, 1)
	client.PrependReactor("patch", "pods", func(_ ktesting.Action) (bool, runtime.Object, error) {
		txn, err := store.GetActiveLifecycleTxn(context.Background(), "sandbox-a")
		if err != nil {
			t.Errorf("GetActiveLifecycleTxn() error = %v", err)
		}
		observedTxn <- txn
		return true, nil, errors.New("stop hot claim")
	})
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    runtimeIdentityPodLister(t, idlePod),
		sandboxStore: store,
		config:       SandboxServiceConfig{ProcdPort: 49983},
		clock:        fixedClock{now: time.Date(2026, time.March, 7, 12, 0, 0, 0, time.UTC)},
		logger:       zap.NewNop(),
	}

	_, err := svc.ResumePausedSandboxRuntime(context.Background(), "sandbox-a")
	if err == nil || !strings.Contains(err.Error(), "stop hot claim") {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want hot claim failure", err)
	}
	var txn *sandboxstore.SandboxLifecycleTxn
	select {
	case txn = <-observedTxn:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for pod patch")
	}
	if txn == nil {
		t.Fatal("active resume txn was not visible before pod claim")
	}
	if txn.Kind != sandboxstore.SandboxLifecycleKindResume || txn.Phase != sandboxstore.SandboxLifecyclePhasePreparing {
		t.Fatalf("observed txn = %+v, want active resume preparing txn", txn)
	}
	if txn.FromGeneration != 3 || txn.ToGeneration != 4 {
		t.Fatalf("txn generations = %d -> %d, want 3 -> 4", txn.FromGeneration, txn.ToGeneration)
	}

	active, err := store.GetActiveLifecycleTxn(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetActiveLifecycleTxn() error = %v", err)
	}
	if active != nil {
		t.Fatalf("active txn after failed claim = %+v, want nil", active)
	}
	var aborted *sandboxstore.SandboxLifecycleTxn
	for _, candidate := range store.lifecycleTxns {
		if candidate != nil && candidate.Kind == sandboxstore.SandboxLifecycleKindResume {
			aborted = candidate
			break
		}
	}
	if aborted == nil || aborted.Phase != sandboxstore.SandboxLifecyclePhaseAborted {
		t.Fatalf("stored resume txn = %+v, want aborted", aborted)
	}
}

func TestResumePausedSandboxRuntimeWaitsForActivePauseTransaction(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-a": {
				ID:                  "sandbox-a",
				TeamID:              "team-a",
				UserID:              "user-a",
				TemplateID:          "default",
				TemplateName:        "default",
				TemplateNamespace:   "tpl-default",
				DesiredState:        sandboxstore.SandboxDesiredStateActive,
				CurrentPodName:      "pod-a",
				CurrentPodNamespace: "ns-a",
				RuntimeGeneration:   4,
				TemplateSpec:        v1alpha1.SandboxTemplateSpec{},
			},
		},
		lifecycleTxns: map[string]*sandboxstore.SandboxLifecycleTxn{
			"txn-a": {
				ID:               "txn-a",
				SandboxID:        "sandbox-a",
				Kind:             sandboxstore.SandboxLifecycleKindPause,
				Phase:            sandboxstore.SandboxLifecyclePhasePreparing,
				FromGeneration:   4,
				FromPodNamespace: "ns-a",
				FromPodName:      "pod-a",
			},
		},
	}
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(pod.DeepCopy()),
		podLister:    runtimeIdentityPodLister(t, pod),
		sandboxStore: store,
		logger:       zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*sandboxLifecycleWaitInterval)
	defer cancel()
	_, err := svc.ResumePausedSandboxRuntime(ctx, "sandbox-a")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want context deadline", err)
	}
	if k8serrors.IsConflict(err) {
		t.Fatalf("ResumePausedSandboxRuntime() returned conflict for active pause transaction")
	}
	if txn := store.lifecycleTxns["txn-a"]; txn == nil || txn.Phase != sandboxstore.SandboxLifecyclePhasePreparing {
		t.Fatalf("pause txn = %+v, want request path to leave it for the pause controller", txn)
	}
}

func TestResumePausedSandboxRuntimeCancelsAutoPauseTransaction(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	markRuntimeIdentityPodReady(t, pod)
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-a": {
				ID:                  "sandbox-a",
				TeamID:              "team-a",
				UserID:              "user-a",
				TemplateID:          "default",
				TemplateName:        "default",
				TemplateNamespace:   "tpl-default",
				DesiredState:        sandboxstore.SandboxDesiredStateActive,
				CurrentPodName:      "pod-a",
				CurrentPodNamespace: "ns-a",
				RuntimeGeneration:   4,
				TemplateSpec:        v1alpha1.SandboxTemplateSpec{},
			},
		},
		lifecycleTxns: map[string]*sandboxstore.SandboxLifecycleTxn{
			"txn-a": {
				ID:               "txn-a",
				SandboxID:        "sandbox-a",
				Kind:             sandboxstore.SandboxLifecycleKindPause,
				Phase:            sandboxstore.SandboxLifecyclePhasePreparing,
				Source:           sandboxstore.SandboxLifecycleSourceAuto,
				Cancelable:       true,
				FromGeneration:   4,
				FromPodNamespace: "ns-a",
				FromPodName:      "pod-a",
			},
		},
	}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    runtimeIdentityPodLister(t, pod),
		sandboxStore: store,
		config:       SandboxServiceConfig{ProcdPort: 49983},
		logger:       zap.NewNop(),
	}

	cancelObserved := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.After(time.Second)
		for {
			select {
			case <-deadline:
				return
			case <-ticker.C:
			}
			store.mu.Lock()
			txn := store.lifecycleTxns["txn-a"]
			if txn != nil && !txn.CancelRequestedAt.IsZero() {
				txn.Phase = sandboxstore.SandboxLifecyclePhaseAborted
				txn.Error = txn.CancelReason
				store.mu.Unlock()
				close(cancelObserved)
				return
			}
			store.mu.Unlock()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sandbox, err := svc.ResumePausedSandboxRuntime(ctx, "sandbox-a")
	if err != nil {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want nil", err)
	}
	select {
	case <-cancelObserved:
	default:
		t.Fatal("resume path did not request auto pause cancellation")
	}
	if sandbox.Status != managerapi.SandboxStatusRunning {
		t.Fatalf("status = %q, want running", sandbox.Status)
	}
	if sandbox.InternalAddr == "" {
		t.Fatal("runtime address is empty, want existing runtime")
	}
	for _, action := range client.Actions() {
		switch action.GetVerb() {
		case "create", "delete":
			if action.GetResource().Resource == "pods" {
				t.Fatalf("unexpected pod %s while canceling auto pause: %#v", action.GetVerb(), action)
			}
		}
	}
	txn := store.lifecycleTxns["txn-a"]
	if txn == nil || txn.Phase != sandboxstore.SandboxLifecyclePhaseAborted || txn.CancelReason == "" {
		t.Fatalf("pause txn = %+v, want aborted with cancel reason", txn)
	}
}

func TestCompletePausingSandboxRuntimeAbortsCanceledAutoPause(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-a": {
				ID:                  "sandbox-a",
				TeamID:              "team-a",
				UserID:              "user-a",
				TemplateID:          "default",
				TemplateName:        "default",
				TemplateNamespace:   "tpl-default",
				DesiredState:        sandboxstore.SandboxDesiredStateActive,
				CurrentPodName:      "pod-a",
				CurrentPodNamespace: "ns-a",
				RuntimeGeneration:   4,
				TemplateSpec:        v1alpha1.SandboxTemplateSpec{},
			},
		},
		lifecycleTxns: map[string]*sandboxstore.SandboxLifecycleTxn{
			"txn-a": {
				ID:                "txn-a",
				SandboxID:         "sandbox-a",
				Kind:              sandboxstore.SandboxLifecycleKindPause,
				Phase:             sandboxstore.SandboxLifecyclePhasePreparing,
				Source:            sandboxstore.SandboxLifecycleSourceAuto,
				Cancelable:        true,
				FromGeneration:    4,
				FromPodNamespace:  "ns-a",
				FromPodName:       "pod-a",
				CancelRequestedAt: time.Now(),
				CancelReason:      "runtime access arrived during auto pause",
			},
		},
	}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    runtimeIdentityPodLister(t, pod),
		sandboxStore: store,
		logger:       zap.NewNop(),
	}

	if err := svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-a"); err != nil {
		t.Fatalf("CompletePausingSandboxRuntime() error = %v, want nil", err)
	}
	txn := store.lifecycleTxns["txn-a"]
	if txn == nil || txn.Phase != sandboxstore.SandboxLifecyclePhaseAborted {
		t.Fatalf("pause txn = %+v, want aborted", txn)
	}
	if store.records["sandbox-a"].DesiredState != sandboxstore.SandboxDesiredStateActive {
		t.Fatalf("record desired state = %q, want active", store.records["sandbox-a"].DesiredState)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "pods" {
			t.Fatalf("unexpected pod delete after canceled auto pause: %#v", action)
		}
	}
}

func TestResumePausedSandboxRuntimeStartsRecoveryForRunningRecordWithoutPod(t *testing.T) {
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			TeamID:            "team-a",
			UserID:            "user-a",
			TemplateID:        "default",
			TemplateName:      "default",
			TemplateNamespace: "tpl-default",
			DesiredState:      sandboxstore.SandboxDesiredStateActive,
			RuntimeGeneration: 4,
			TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		},
	}}
	client := fake.NewSimpleClientset()
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		k8sClient:     client,
		podLister:     runtimeIdentityPodLister(t),
		sandboxStore:  store,
		pauseEnqueuer: enqueuer,
		logger:        zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := svc.ResumePausedSandboxRuntime(ctx, "sandbox-a")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want deadline while recovery remains queued", err)
	}
	txn, getErr := store.GetActiveLifecycleTxn(context.Background(), "sandbox-a")
	if getErr != nil {
		t.Fatalf("GetActiveLifecycleTxn() error = %v", getErr)
	}
	if txn == nil || txn.Kind != sandboxstore.SandboxLifecycleKindPause || txn.Source != sandboxstore.SandboxLifecycleSourceLost {
		t.Fatalf("lifecycle txn = %+v, want lost-runtime pause", txn)
	}
	if len(enqueuer.recoveryCalls) != 1 || enqueuer.recoveryCalls[0] != "sandbox-a" {
		t.Fatalf("recovery calls = %#v, want sandbox-a", enqueuer.recoveryCalls)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "pods" {
			t.Fatalf("unexpected pod create for running record without runtime: %#v", action)
		}
	}
}

func TestResumePausedSandboxRuntimeRejectsHardExpiredRecord(t *testing.T) {
	now := time.Date(2026, time.March, 7, 12, 0, 0, 0, time.UTC)
	hardExpiresAt := now.Add(-time.Second)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			TeamID:            "team-a",
			UserID:            "user-a",
			TemplateID:        "default",
			TemplateName:      "default",
			TemplateNamespace: "tpl-default",
			DesiredState:      sandboxstore.SandboxDesiredStatePaused,
			HardExpiresAt:     hardExpiresAt,
			TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		},
	}}
	svc := &SandboxService{
		sandboxStore: store,
		clock:        fixedClock{now: now},
		logger:       zap.NewNop(),
	}

	_, err := svc.ResumePausedSandboxRuntime(context.Background(), "sandbox-a")
	if !k8serrors.IsNotFound(err) {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want not found", err)
	}
	if store.saves != 0 {
		t.Fatalf("store saves = %d, want 0", store.saves)
	}
	if got := store.records["sandbox-a"].HardExpiresAt; !got.Equal(hardExpiresAt) {
		t.Fatalf("hard expires at = %s, want %s", got, hardExpiresAt)
	}
}

func TestTerminatePausedSandboxCompletesPersistentCleanupWithoutWebhookDelivery(t *testing.T) {
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": {
			ID:                   "sandbox-a",
			TeamID:               "team-a",
			UserID:               "user-a",
			DesiredState:         sandboxstore.SandboxDesiredStatePaused,
			WebhookStateVolumeID: "volume-a",
			Config: sandboxstore.SandboxConfig{Webhook: &sandboxstore.WebhookConfig{
				URL:    "https://example.test/webhook",
				Secret: "secret",
			}},
		},
	}}
	bindings := &deleteRecordingBindingStore{}
	volumes := &recordingSystemVolumeClient{}
	svc := &SandboxService{
		k8sClient:           fake.NewSimpleClientset(),
		podLister:           runtimeIdentityPodLister(t),
		credentialStore:     bindings,
		webhookStateVolumes: volumes,
		sandboxStore:        store,
		clock:               systemTime{},
		logger:              zap.NewNop(),
	}

	if err := svc.TerminateSandbox(context.Background(), "sandbox-a"); err != nil {
		t.Fatalf("TerminateSandbox() error = %v", err)
	}
	if store.records["sandbox-a"].DesiredState != sandboxstore.SandboxDesiredStateDeleted {
		t.Fatalf("desired state = %q, want deleted", store.records["sandbox-a"].DesiredState)
	}
	if bindings.deleteCalls != 1 {
		t.Fatalf("DeleteBindings calls = %d, want 1", bindings.deleteCalls)
	}
	if len(volumes.marked) != 1 || volumes.marked[0] != "sandbox-a:sandbox_deleted" {
		t.Fatalf("marked volumes = %#v, want sandbox-a:sandbox_deleted", volumes.marked)
	}
}

func TestTerminateSandboxAbortsActivePauseTransaction(t *testing.T) {
	now := time.Date(2026, time.March, 7, 12, 0, 0, 0, time.UTC)
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-a": {
				ID:                "sandbox-a",
				TeamID:            "team-a",
				UserID:            "user-a",
				TemplateID:        "default",
				TemplateName:      "default",
				TemplateNamespace: "tpl-default",
				DesiredState:      sandboxstore.SandboxDesiredStateActive,
				RuntimeGeneration: 3,
			},
		},
		lifecycleTxns: map[string]*sandboxstore.SandboxLifecycleTxn{
			"txn-a": {
				ID:             "txn-a",
				SandboxID:      "sandbox-a",
				Kind:           sandboxstore.SandboxLifecycleKindPause,
				Phase:          sandboxstore.SandboxLifecyclePhaseBarriered,
				Epoch:          1,
				FromGeneration: 3,
			},
		},
	}
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(),
		podLister:    runtimeIdentityPodLister(t),
		sandboxStore: store,
		clock:        fixedClock{now: now},
		logger:       zap.NewNop(),
	}

	if err := svc.TerminateSandbox(context.Background(), "sandbox-a"); err != nil {
		t.Fatalf("TerminateSandbox() error = %v", err)
	}
	if err := svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-a"); err != nil {
		t.Fatalf("CompletePausingSandboxRuntime() error = %v", err)
	}
	if got := store.records["sandbox-a"].DesiredState; got != sandboxstore.SandboxDesiredStateDeleted {
		t.Fatalf("desired state = %q, want deleted", got)
	}
	if txn, err := store.GetActiveLifecycleTxn(context.Background(), "sandbox-a"); err != nil || txn != nil {
		t.Fatalf("active txn = %+v, err = %v, want nil", txn, err)
	}
	if store.pauses != 0 {
		t.Fatalf("store pauses = %d, want 0", store.pauses)
	}
}

func runtimeIdentityPod(namespace, name, sandboxID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				controller.LabelPoolType:  controller.PoolTypeActive,
				controller.LabelSandboxID: sandboxID,
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:    "team-a",
				controller.AnnotationSandboxID: sandboxID,
			},
		},
	}
}

func markRuntimeIdentityPodReady(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	assignment, revision, err := runtimecontrol.AssignmentFromPod(pod)
	if err != nil {
		t.Fatalf("derive runtime assignment: %v", err)
	}
	if assignment == nil {
		t.Fatal("runtime assignment is nil")
	}
	pod.Annotations[runtimecontrol.AnnotationAssignmentRevision] = revision
	pod.Annotations[runtimecontrol.AnnotationAssignmentReady] = revision
	pod.Annotations[runtimecontrol.AnnotationObservedRevision] = revision
	pod.Annotations[runtimecontrol.AnnotationObservedGeneration] = fmt.Sprintf("%d", assignment.RuntimeGeneration)
	pod.Annotations[runtimecontrol.AnnotationObservedState] = string(runtimecontrol.ObservedReady)
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:   v1alpha1.SandboxPodReadinessConditionType,
		Status: corev1.ConditionTrue,
	})
}

func runtimeIdentityPodLister(t *testing.T, pods ...*corev1.Pod) corelisters.PodLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, pod := range pods {
		if err := indexer.Add(pod); err != nil {
			t.Fatalf("add pod to indexer: %v", err)
		}
	}
	return corelisters.NewPodLister(indexer)
}
