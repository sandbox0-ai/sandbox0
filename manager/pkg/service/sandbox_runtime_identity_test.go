package service

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type memorySandboxStore struct {
	records           map[string]*SandboxRecord
	rootFSStates      map[string]*SandboxRootFSState
	rootFSFilesystems map[string]*RootFSFilesystem
	rootFSSnapshots   map[string]*RootFSSnapshot
	deletes           []string
	saves             int
	pauses            int
}

type memorySandboxStoreTx struct {
	store *memorySandboxStore
}

func (s *memorySandboxStore) UpsertSandbox(_ context.Context, record *SandboxRecord) error {
	if s.records == nil {
		s.records = make(map[string]*SandboxRecord)
	}
	s.records[record.ID] = cloneSandboxRecord(record)
	return nil
}

func (s *memorySandboxStore) GetSandbox(_ context.Context, sandboxID string) (*SandboxRecord, error) {
	if s == nil || s.records == nil {
		return nil, nil
	}
	return cloneSandboxRecord(s.records[sandboxID]), nil
}

func (s *memorySandboxStore) ListSandboxes(_ context.Context, req *ListSandboxesRequest) ([]*SandboxRecord, error) {
	if s == nil || s.records == nil {
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

func (s *memorySandboxStore) ListPausingSandboxes(_ context.Context, limit int) ([]*SandboxRecord, error) {
	if s == nil || s.records == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = len(s.records)
	}
	records := make([]*SandboxRecord, 0, len(s.records))
	for _, record := range s.records {
		if record == nil || record.Status != SandboxStatusPausing {
			continue
		}
		records = append(records, cloneSandboxRecord(record))
		if len(records) >= limit {
			break
		}
	}
	return records, nil
}

func (s *memorySandboxStore) MarkSandboxDeleted(_ context.Context, sandboxID string, deletedAt time.Time) error {
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
	delete(s.rootFSStates, sandboxID)
	s.deletes = append(s.deletes, sandboxID)
	return nil
}

func (s *memorySandboxStore) SaveRootFSState(_ context.Context, state *SandboxRootFSState) error {
	if s.rootFSStates == nil {
		s.rootFSStates = make(map[string]*SandboxRootFSState)
	}
	s.rootFSStates[state.SandboxID] = cloneSandboxRootFSState(state)
	return nil
}

func (s *memorySandboxStore) GetLatestRootFSState(_ context.Context, sandboxID string) (*SandboxRootFSState, error) {
	if s == nil || s.rootFSStates == nil {
		return nil, nil
	}
	return cloneSandboxRootFSState(s.rootFSStates[sandboxID]), nil
}

func (s *memorySandboxStore) WithSandboxLock(ctx context.Context, sandboxID string, fn func(context.Context, SandboxStoreTx, *SandboxRecord) error) error {
	record, err := s.GetSandbox(ctx, sandboxID)
	if err != nil {
		return err
	}
	if record == nil {
		return ErrSandboxRecordNotFound
	}
	return fn(ctx, memorySandboxStoreTx{store: s}, record)
}

func (t memorySandboxStoreTx) SaveRuntime(_ context.Context, sandboxID, namespace, podName, status string, generation int64, expiresAt, hardExpiresAt time.Time) error {
	record := t.store.records[sandboxID]
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
	record := t.store.records[sandboxID]
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
	return t.store.SaveRootFSState(context.Background(), state)
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

func TestResumePausedSandboxRuntimeDoesNotCreatePodWhileRuntimeDeleting(t *testing.T) {
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

	_, err := svc.ResumePausedSandboxRuntime(context.Background(), "sandbox-a")
	if !k8serrors.IsConflict(err) {
		t.Fatalf("ResumePausedSandboxRuntime() error = %v, want conflict", err)
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
