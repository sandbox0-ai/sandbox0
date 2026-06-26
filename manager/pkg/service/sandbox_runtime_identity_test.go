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
	records           map[string]*SandboxRecord
	lifecycleTxns     map[string]*SandboxLifecycleTxn
	rootFSStates      map[string]*SandboxRootFSState
	rootFSFilesystems map[string]*RootFSFilesystem
	rootFSSnapshots   map[string]*RootFSSnapshot
	deletes           []string
	saves             int
	pauses            int
	lockCalls         int
	lockStarted       chan struct{}
	blockLock         chan struct{}
}

type memorySandboxStoreTx struct {
	store *memorySandboxStore
}

func (s *memorySandboxStore) UpsertSandbox(_ context.Context, record *SandboxRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[string]*SandboxRecord)
	}
	s.records[record.ID] = cloneSandboxRecord(record)
	return nil
}

func (s *memorySandboxStore) GetSandbox(_ context.Context, sandboxID string) (*SandboxRecord, error) {
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

func (s *memorySandboxStore) ListSandboxes(_ context.Context, req *ListSandboxesRequest) ([]*SandboxRecord, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		return nil, nil
	}
	var records []*SandboxRecord
	for _, record := range s.records {
		if record == nil {
			continue
		}
		if req != nil {
			if req.TeamID != "" && record.TeamID != req.TeamID {
				continue
			}
			if req.Status != "" && record.Status != req.Status {
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

func (s *memorySandboxStore) ListHardExpiredSandboxes(context.Context, time.Time, int) ([]*SandboxRecord, error) {
	return nil, nil
}

func (s *memorySandboxStore) ListActiveLifecycleTxns(_ context.Context, kind string, limit int) ([]*SandboxLifecycleTxn, error) {
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
	txns := make([]*SandboxLifecycleTxn, 0, len(s.lifecycleTxns))
	for _, txn := range s.lifecycleTxns {
		if txn == nil || txn.Kind != kind || !sandboxLifecyclePhaseActive(txn.Phase) {
			continue
		}
		txns = append(txns, cloneSandboxLifecycleTxn(txn))
		if len(txns) >= limit {
			break
		}
	}
	return txns, nil
}

func (s *memorySandboxStore) GetActiveLifecycleTxn(_ context.Context, sandboxID string) (*SandboxLifecycleTxn, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, txn := range s.lifecycleTxns {
		if txn != nil && txn.SandboxID == sandboxID && sandboxLifecyclePhaseActive(txn.Phase) {
			return cloneSandboxLifecycleTxn(txn), nil
		}
	}
	return nil, nil
}

func (s *memorySandboxStore) MarkSandboxDeleted(_ context.Context, sandboxID string, deletedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[string]*SandboxRecord)
	}
	record := s.records[sandboxID]
	if record == nil {
		record = &SandboxRecord{ID: sandboxID}
		s.records[sandboxID] = record
	}
	record.Status = SandboxStatusDeleted
	record.DeletedAt = deletedAt
	record.CurrentPodName = ""
	record.CurrentPodNamespace = ""
	for _, txn := range s.lifecycleTxns {
		if txn != nil && txn.SandboxID == sandboxID && sandboxLifecyclePhaseActive(txn.Phase) {
			txn.Phase = SandboxLifecyclePhaseAborted
			txn.Error = "sandbox deleted"
			txn.AbortedAt = deletedAt
		}
	}
	delete(s.rootFSStates, sandboxID)
	s.deletes = append(s.deletes, sandboxID)
	return nil
}

func (s *memorySandboxStore) SaveRootFSState(_ context.Context, state *SandboxRootFSState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rootFSStates == nil {
		s.rootFSStates = make(map[string]*SandboxRootFSState)
	}
	s.rootFSStates[state.SandboxID] = cloneSandboxRootFSState(state)
	return nil
}

func (s *memorySandboxStore) GetLatestRootFSState(_ context.Context, sandboxID string) (*SandboxRootFSState, error) {
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

func (s *memorySandboxStore) WithSandboxLock(ctx context.Context, sandboxID string, fn func(context.Context, SandboxStoreTx, *SandboxRecord) error) error {
	if s == nil {
		return ErrSandboxRecordNotFound
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
		return ErrSandboxRecordNotFound
	}
	snapshot := s.snapshot()
	if err := fn(ctx, memorySandboxStoreTx{store: s}, record); err != nil {
		s.restore(snapshot)
		return err
	}
	return nil
}

type memorySandboxStoreSnapshot struct {
	records           map[string]*SandboxRecord
	lifecycleTxns     map[string]*SandboxLifecycleTxn
	rootFSStates      map[string]*SandboxRootFSState
	rootFSFilesystems map[string]*RootFSFilesystem
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

func (s *memorySandboxStore) setSandboxStatus(sandboxID, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record := s.records[sandboxID]; record != nil {
		record.Status = status
	}
}

func (t memorySandboxStoreTx) SaveRuntime(_ context.Context, sandboxID, namespace, podName, status string, generation int64, expiresAt, hardExpiresAt time.Time) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	record := t.store.records[sandboxID]
	if record == nil || !record.DeletedAt.IsZero() {
		return ErrSandboxRecordNotFound
	}
	record.CurrentPodNamespace = namespace
	record.CurrentPodName = podName
	record.Status = status
	record.RuntimeGeneration = generation
	record.ExpiresAt = expiresAt
	record.HardExpiresAt = hardExpiresAt
	t.store.saves++
	return nil
}

func (t memorySandboxStoreTx) MarkRuntimePaused(_ context.Context, sandboxID string, generation int64, _ time.Time) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	record := t.store.records[sandboxID]
	if record == nil || !record.DeletedAt.IsZero() {
		return ErrSandboxRecordNotFound
	}
	record.CurrentPodNamespace = ""
	record.CurrentPodName = ""
	record.Status = SandboxStatusPaused
	if record.RuntimeGeneration < generation {
		record.RuntimeGeneration = generation
	}
	t.store.pauses++
	return nil
}

func (t memorySandboxStoreTx) SaveRootFSState(_ context.Context, state *SandboxRootFSState) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.store.rootFSStates == nil {
		t.store.rootFSStates = make(map[string]*SandboxRootFSState)
	}
	t.store.rootFSStates[state.SandboxID] = cloneSandboxRootFSState(state)
	return nil
}

func (t memorySandboxStoreTx) GetActiveLifecycleTxn(_ context.Context, sandboxID string) (*SandboxLifecycleTxn, error) {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	for _, txn := range t.store.lifecycleTxns {
		if txn != nil && txn.SandboxID == sandboxID && sandboxLifecyclePhaseActive(txn.Phase) {
			return cloneSandboxLifecycleTxn(txn), nil
		}
	}
	return nil, nil
}

func (t memorySandboxStoreTx) BeginLifecycleTxn(_ context.Context, txn *SandboxLifecycleTxn) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.store.lifecycleTxns == nil {
		t.store.lifecycleTxns = make(map[string]*SandboxLifecycleTxn)
	}
	record := t.store.records[txn.SandboxID]
	record.LifecycleEpoch++
	txn.Epoch = record.LifecycleEpoch
	if txn.Phase == "" {
		txn.Phase = SandboxLifecyclePhasePreparing
	}
	if txn.Source == "" {
		txn.Source = SandboxLifecycleSourceManual
	}
	t.store.lifecycleTxns[txn.ID] = cloneSandboxLifecycleTxn(txn)
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
		txn.Phase = SandboxLifecyclePhaseCommitted
		txn.PreparedHeadLayerID = preparedHeadLayerID
	}
	return nil
}

func (t memorySandboxStoreTx) AbortLifecycleTxn(_ context.Context, txnID, reason string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if txn := t.store.lifecycleTxns[txnID]; txn != nil && sandboxLifecyclePhaseActive(txn.Phase) {
		txn.Phase = SandboxLifecyclePhaseAborted
		txn.Error = reason
	}
	return nil
}

func cloneSandboxRecord(record *SandboxRecord) *SandboxRecord {
	if record == nil {
		return nil
	}
	clone := *record
	if record.Mounts != nil {
		clone.Mounts = append([]ClaimMount(nil), record.Mounts...)
	}
	if record.Config.Services != nil {
		clone.Config.Services = append([]SandboxAppService(nil), record.Config.Services...)
	}
	return &clone
}

func cloneSandboxRecordMap(records map[string]*SandboxRecord) map[string]*SandboxRecord {
	if records == nil {
		return nil
	}
	cloned := make(map[string]*SandboxRecord, len(records))
	for key, record := range records {
		cloned[key] = cloneSandboxRecord(record)
	}
	return cloned
}

func cloneSandboxLifecycleTxnMap(txns map[string]*SandboxLifecycleTxn) map[string]*SandboxLifecycleTxn {
	if txns == nil {
		return nil
	}
	cloned := make(map[string]*SandboxLifecycleTxn, len(txns))
	for key, txn := range txns {
		cloned[key] = cloneSandboxLifecycleTxn(txn)
	}
	return cloned
}

func cloneSandboxRootFSState(state *SandboxRootFSState) *SandboxRootFSState {
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

func cloneSandboxRootFSStateMap(states map[string]*SandboxRootFSState) map[string]*SandboxRootFSState {
	if states == nil {
		return nil
	}
	cloned := make(map[string]*SandboxRootFSState, len(states))
	for key, state := range states {
		cloned[key] = cloneSandboxRootFSState(state)
	}
	return cloned
}

func cloneRootFSFilesystemMap(filesystems map[string]*RootFSFilesystem) map[string]*RootFSFilesystem {
	if filesystems == nil {
		return nil
	}
	cloned := make(map[string]*RootFSFilesystem, len(filesystems))
	for key, filesystem := range filesystems {
		cloned[key] = cloneRootFSFilesystemForTest(filesystem)
	}
	return cloned
}

func TestRootFSStateFromLayerChainKeepsCurrentSandboxID(t *testing.T) {
	state := rootFSStateFromLayerChain("child-sandbox", []*SandboxRootFSLayer{
		{
			ID:              "layer-parent",
			SourceSandboxID: "parent-sandbox",
			TeamID:          "team-1",
			DiffDigest:      "sha256:parent",
			DiffObjectKey:   "rootfs/parent.tar",
		},
		{
			ID:              "layer-child",
			ParentLayerID:   "layer-parent",
			SourceSandboxID: "parent-sandbox",
			TeamID:          "team-1",
			DiffDigest:      "sha256:child",
			DiffObjectKey:   "rootfs/child.tar",
		},
	})

	if state == nil {
		t.Fatal("state is nil")
	}
	if state.SandboxID != "child-sandbox" {
		t.Fatalf("SandboxID = %q, want child-sandbox", state.SandboxID)
	}
	if state.LayerID != "layer-child" || state.ParentLayerID != "layer-parent" {
		t.Fatalf("head = %q parent = %q, want layer-child/layer-parent", state.LayerID, state.ParentLayerID)
	}
	if len(state.LayerChain) != 2 {
		t.Fatalf("LayerChain len = %d, want 2", len(state.LayerChain))
	}
}

func TestSandboxIndexTracksMultipleRuntimePodsForSameSandbox(t *testing.T) {
	index := NewSandboxIndex()
	first := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	second := runtimeIdentityPod("ns-a", "pod-b", "sandbox-a")

	index.handleAdd(first)
	index.handleAdd(second)

	refs := index.GetPodRefs("sandbox-a")
	if len(refs) != 2 {
		t.Fatalf("refs = %#v, want two runtime pod refs", refs)
	}
	ids := index.ListSandboxIDs("ns-a")
	if len(ids) != 1 || ids[0] != "sandbox-a" {
		t.Fatalf("sandbox ids = %#v, want sandbox-a", ids)
	}

	index.handleDelete(first)
	refs = index.GetPodRefs("sandbox-a")
	if len(refs) != 1 || refs[0].Name != "pod-b" {
		t.Fatalf("refs after first delete = %#v, want pod-b", refs)
	}
	ids = index.ListSandboxIDs("ns-a")
	if len(ids) != 1 || ids[0] != "sandbox-a" {
		t.Fatalf("sandbox ids after first delete = %#v, want sandbox-a", ids)
	}
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
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			TeamID:            "team-a",
			UserID:            "user-a",
			TemplateID:        "default",
			TemplateName:      "default",
			TemplateNamespace: "tpl-default",
			Status:            SandboxStatusPaused,
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

func TestResumePausedSandboxRuntimeJoinsResumingRuntime(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-a": {
				ID:                "sandbox-a",
				TeamID:            "team-a",
				UserID:            "user-a",
				TemplateID:        "default",
				TemplateName:      "default",
				TemplateNamespace: "tpl-default",
				Status:            SandboxStatusPaused,
				RuntimeGeneration: 3,
				TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
			},
		},
		lifecycleTxns: map[string]*SandboxLifecycleTxn{
			"txn-a": {
				ID:             "txn-a",
				SandboxID:      "sandbox-a",
				Kind:           SandboxLifecycleKindResume,
				Phase:          SandboxLifecyclePhasePreparing,
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
		store.lifecycleTxns["txn-a"].Phase = SandboxLifecyclePhaseCommitted
		store.records["sandbox-a"].CurrentPodName = "pod-a"
		store.records["sandbox-a"].CurrentPodNamespace = "ns-a"
		store.records["sandbox-a"].RuntimeGeneration = 4
		store.mu.Unlock()
		store.setSandboxStatus("sandbox-a", SandboxStatusRunning)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sandbox, err := svc.ResumePausedSandboxRuntime(ctx, "sandbox-a")
	if err != nil {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want nil", err)
	}
	if sandbox.Status != SandboxStatusRunning {
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

func TestResumeSandboxSingleflightPreventsConcurrentSandboxLocks(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	lockStarted := make(chan struct{}, 1)
	blockLock := make(chan struct{})
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-a": {
				ID:                  "sandbox-a",
				TeamID:              "team-a",
				UserID:              "user-a",
				TemplateID:          "default",
				TemplateName:        "default",
				TemplateNamespace:   "tpl-default",
				Status:              SandboxStatusRunning,
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
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			TeamID:            "team-a",
			UserID:            "user-a",
			TemplateID:        "default",
			TemplateName:      "default",
			TemplateNamespace: "tpl-default",
			Status:            SandboxStatusPaused,
			RuntimeGeneration: 3,
			TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		},
	}}
	client := fake.NewSimpleClientset(idlePod.DeepCopy())
	observedTxn := make(chan *SandboxLifecycleTxn, 1)
	client.PrependReactor("update", "pods", func(_ ktesting.Action) (bool, runtime.Object, error) {
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
	var txn *SandboxLifecycleTxn
	select {
	case txn = <-observedTxn:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for pod update")
	}
	if txn == nil {
		t.Fatal("active resume txn was not visible before pod claim")
	}
	if txn.Kind != SandboxLifecycleKindResume || txn.Phase != SandboxLifecyclePhasePreparing {
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
	var aborted *SandboxLifecycleTxn
	for _, candidate := range store.lifecycleTxns {
		if candidate != nil && candidate.Kind == SandboxLifecycleKindResume {
			aborted = candidate
			break
		}
	}
	if aborted == nil || aborted.Phase != SandboxLifecyclePhaseAborted {
		t.Fatalf("stored resume txn = %+v, want aborted", aborted)
	}
}

func TestResumePausedSandboxRuntimeWaitsForActivePauseTransaction(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-a": {
				ID:                  "sandbox-a",
				TeamID:              "team-a",
				UserID:              "user-a",
				TemplateID:          "default",
				TemplateName:        "default",
				TemplateNamespace:   "tpl-default",
				Status:              SandboxStatusRunning,
				CurrentPodName:      "pod-a",
				CurrentPodNamespace: "ns-a",
				RuntimeGeneration:   4,
				TemplateSpec:        v1alpha1.SandboxTemplateSpec{},
			},
		},
		lifecycleTxns: map[string]*SandboxLifecycleTxn{
			"txn-a": {
				ID:               "txn-a",
				SandboxID:        "sandbox-a",
				Kind:             SandboxLifecycleKindPause,
				Phase:            SandboxLifecyclePhasePreparing,
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
	if txn := store.lifecycleTxns["txn-a"]; txn == nil || txn.Phase != SandboxLifecyclePhasePreparing {
		t.Fatalf("pause txn = %+v, want request path to leave it for the pause controller", txn)
	}
}

func TestResumePausedSandboxRuntimeCancelsAutoPauseTransaction(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-a": {
				ID:                  "sandbox-a",
				TeamID:              "team-a",
				UserID:              "user-a",
				TemplateID:          "default",
				TemplateName:        "default",
				TemplateNamespace:   "tpl-default",
				Status:              SandboxStatusRunning,
				CurrentPodName:      "pod-a",
				CurrentPodNamespace: "ns-a",
				RuntimeGeneration:   4,
				TemplateSpec:        v1alpha1.SandboxTemplateSpec{},
			},
		},
		lifecycleTxns: map[string]*SandboxLifecycleTxn{
			"txn-a": {
				ID:               "txn-a",
				SandboxID:        "sandbox-a",
				Kind:             SandboxLifecycleKindPause,
				Phase:            SandboxLifecyclePhasePreparing,
				Source:           SandboxLifecycleSourceAuto,
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
				txn.Phase = SandboxLifecyclePhaseAborted
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
	if sandbox.Status != SandboxStatusRunning {
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
	if txn == nil || txn.Phase != SandboxLifecyclePhaseAborted || txn.CancelReason == "" {
		t.Fatalf("pause txn = %+v, want aborted with cancel reason", txn)
	}
}

func TestCompletePausingSandboxRuntimeAbortsCanceledAutoPause(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-a": {
				ID:                  "sandbox-a",
				TeamID:              "team-a",
				UserID:              "user-a",
				TemplateID:          "default",
				TemplateName:        "default",
				TemplateNamespace:   "tpl-default",
				Status:              SandboxStatusRunning,
				CurrentPodName:      "pod-a",
				CurrentPodNamespace: "ns-a",
				RuntimeGeneration:   4,
				TemplateSpec:        v1alpha1.SandboxTemplateSpec{},
			},
		},
		lifecycleTxns: map[string]*SandboxLifecycleTxn{
			"txn-a": {
				ID:                "txn-a",
				SandboxID:         "sandbox-a",
				Kind:              SandboxLifecycleKindPause,
				Phase:             SandboxLifecyclePhasePreparing,
				Source:            SandboxLifecycleSourceAuto,
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
	if txn == nil || txn.Phase != SandboxLifecyclePhaseAborted {
		t.Fatalf("pause txn = %+v, want aborted", txn)
	}
	if store.records["sandbox-a"].Status != SandboxStatusRunning {
		t.Fatalf("record status = %q, want running", store.records["sandbox-a"].Status)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "pods" {
			t.Fatalf("unexpected pod delete after canceled auto pause: %#v", action)
		}
	}
}

func TestResumePausedSandboxRuntimeDoesNotCreateRuntimeForRunningRecordWithoutPod(t *testing.T) {
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			TeamID:            "team-a",
			UserID:            "user-a",
			TemplateID:        "default",
			TemplateName:      "default",
			TemplateNamespace: "tpl-default",
			Status:            SandboxStatusRunning,
			RuntimeGeneration: 4,
			TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		},
	}}
	client := fake.NewSimpleClientset()
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    runtimeIdentityPodLister(t),
		sandboxStore: store,
		logger:       zap.NewNop(),
	}

	_, err := svc.ResumePausedSandboxRuntime(context.Background(), "sandbox-a")
	if !k8serrors.IsConflict(err) {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want conflict", err)
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
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			TeamID:            "team-a",
			UserID:            "user-a",
			TemplateID:        "default",
			TemplateName:      "default",
			TemplateNamespace: "tpl-default",
			Status:            SandboxStatusPaused,
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

func TestTerminatePausedSandboxRecordRunsPersistentCleanup(t *testing.T) {
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": {
			ID:     "sandbox-a",
			TeamID: "team-a",
			UserID: "user-a",
			Status: SandboxStatusPaused,
			Config: SandboxConfig{Webhook: &WebhookConfig{
				URL:    "https://example.test/webhook",
				Secret: "secret",
			}},
		},
	}}
	bindings := &deleteRecordingBindingStore{}
	volumes := &recordingSystemVolumeClient{}
	emitter := &recordingDeletionWebhookEmitter{}
	svc := &SandboxService{
		podLister:              runtimeIdentityPodLister(t),
		credentialStore:        bindings,
		webhookStateVolumes:    volumes,
		deletionWebhookEmitter: emitter,
		sandboxStore:           store,
		clock:                  systemTime{},
		logger:                 zap.NewNop(),
	}

	if err := svc.TerminateSandbox(context.Background(), "sandbox-a"); err != nil {
		t.Fatalf("TerminateSandbox() error = %v", err)
	}
	if store.records["sandbox-a"].Status != SandboxStatusDeleted {
		t.Fatalf("status = %q, want deleted", store.records["sandbox-a"].Status)
	}
	if bindings.deleteCalls != 1 {
		t.Fatalf("DeleteBindings calls = %d, want 1", bindings.deleteCalls)
	}
	if len(emitter.calls) != 1 {
		t.Fatalf("webhook calls = %d, want 1", len(emitter.calls))
	}
	if len(volumes.marked) != 1 || volumes.marked[0] != "sandbox-a:sandbox_deleted" {
		t.Fatalf("marked volumes = %#v, want sandbox-a:sandbox_deleted", volumes.marked)
	}
}

func TestTerminateSandboxAbortsActivePauseTransaction(t *testing.T) {
	now := time.Date(2026, time.March, 7, 12, 0, 0, 0, time.UTC)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-a": {
				ID:                "sandbox-a",
				TeamID:            "team-a",
				UserID:            "user-a",
				TemplateID:        "default",
				TemplateName:      "default",
				TemplateNamespace: "tpl-default",
				Status:            SandboxStatusRunning,
				RuntimeGeneration: 3,
			},
		},
		lifecycleTxns: map[string]*SandboxLifecycleTxn{
			"txn-a": {
				ID:             "txn-a",
				SandboxID:      "sandbox-a",
				Kind:           SandboxLifecycleKindPause,
				Phase:          SandboxLifecyclePhaseBarriered,
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
	if got := store.records["sandbox-a"].Status; got != SandboxStatusDeleted {
		t.Fatalf("status = %q, want deleted", got)
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
