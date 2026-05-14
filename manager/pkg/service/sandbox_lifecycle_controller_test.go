package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

type recordingSandboxCleaner struct {
	calls []SandboxLifecycleInfo
	err   error
}

type deleteRecordingBindingStore struct {
	deleteCalls int
}

type recordingSystemVolumeClient struct {
	created []string
	deleted []string
	marked  []string
	list    []SandboxSystemVolume
}

type recordingDeletionWebhookEmitter struct {
	calls []SandboxLifecycleInfo
	err   error
}

func (c *recordingSandboxCleaner) CleanupDeletedSandbox(_ context.Context, info SandboxLifecycleInfo) error {
	c.calls = append(c.calls, info)
	return c.err
}

func (s *deleteRecordingBindingStore) GetBindings(context.Context, string, string) (*egressauth.BindingRecord, error) {
	return nil, nil
}

func (s *deleteRecordingBindingStore) UpsertBindings(context.Context, *egressauth.BindingRecord) error {
	return nil
}

func (s *deleteRecordingBindingStore) DeleteBindings(context.Context, string, string) error {
	s.deleteCalls++
	return nil
}

func (s *deleteRecordingBindingStore) GetSourceByRef(context.Context, string, string) (*egressauth.CredentialSource, error) {
	return nil, nil
}

func (s *deleteRecordingBindingStore) GetSourceVersion(context.Context, int64, int64) (*egressauth.CredentialSourceVersion, error) {
	return nil, nil
}

func (c *recordingSystemVolumeClient) Create(_ context.Context, _, _, sandboxID, kind string) (string, error) {
	id := sandboxID + "-" + kind
	c.created = append(c.created, id)
	return id, nil
}

func (c *recordingSystemVolumeClient) Delete(_ context.Context, _, _, _, volumeID string) error {
	c.deleted = append(c.deleted, volumeID)
	return nil
}

func (c *recordingSystemVolumeClient) MarkSandboxForCleanup(_ context.Context, _, _, sandboxID, reason string) error {
	c.marked = append(c.marked, sandboxID+":"+reason)
	return nil
}

func (c *recordingSystemVolumeClient) List(_ context.Context) ([]SandboxSystemVolume, error) {
	return c.list, nil
}

func (e *recordingDeletionWebhookEmitter) EmitSandboxDeleted(_ context.Context, info SandboxLifecycleInfo) error {
	e.calls = append(e.calls, info)
	return e.err
}

func TestSandboxLifecycleControllerCleansAndRemovesFinalizer(t *testing.T) {
	deletionTime := metav1.NewTime(time.Now().UTC())
	pod := newLifecycleTestPod()
	pod.Finalizers = []string{sandboxCleanupFinalizer}
	pod.DeletionTimestamp = &deletionTime
	client := fake.NewSimpleClientset(pod.DeepCopy())
	cleaner := &recordingSandboxCleaner{}
	controller := NewSandboxLifecycleController(client, nil, cleaner, zap.NewNop())

	err := controller.reconcile(context.Background(), sandboxLifecycleItemFromInfo(SandboxLifecycleInfo{
		Namespace: pod.Namespace,
		PodName:   pod.Name,
		SandboxID: pod.Name,
		TeamID:    "team-a",
	}, false))
	if err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	if len(cleaner.calls) != 1 {
		t.Fatalf("cleanup calls = %d, want 1", len(cleaner.calls))
	}
	if got := cleaner.calls[0].SandboxID; got != pod.Name {
		t.Fatalf("cleanup sandboxID = %q, want %q", got, pod.Name)
	}
	updated, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get updated pod: %v", err)
	}
	if hasSandboxCleanupFinalizer(updated) {
		t.Fatal("sandbox cleanup finalizer was not removed")
	}
}

func TestSandboxLifecycleControllerRetriesCleanupBeforeRemovingFinalizer(t *testing.T) {
	deletionTime := metav1.NewTime(time.Now().UTC())
	pod := newLifecycleTestPod()
	pod.Finalizers = []string{sandboxCleanupFinalizer}
	pod.DeletionTimestamp = &deletionTime
	client := fake.NewSimpleClientset(pod.DeepCopy())
	cleaner := &recordingSandboxCleaner{err: errors.New("cleanup failed")}
	controller := NewSandboxLifecycleController(client, nil, cleaner, zap.NewNop())

	err := controller.reconcile(context.Background(), sandboxLifecycleItemFromInfo(SandboxLifecycleInfo{
		Namespace: pod.Namespace,
		PodName:   pod.Name,
		SandboxID: pod.Name,
		TeamID:    "team-a",
	}, false))
	if err == nil {
		t.Fatal("reconcile() error = nil, want cleanup failure")
	}
	updated, getErr := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if getErr != nil {
		t.Fatalf("get updated pod: %v", getErr)
	}
	if !hasSandboxCleanupFinalizer(updated) {
		t.Fatal("sandbox cleanup finalizer was removed before cleanup succeeded")
	}
}

func TestSandboxLifecycleControllerBackfillsCleanupFinalizer(t *testing.T) {
	pod := newLifecycleTestPod()
	client := fake.NewSimpleClientset(pod.DeepCopy())
	cleaner := &recordingSandboxCleaner{}
	controller := NewSandboxLifecycleController(client, nil, cleaner, zap.NewNop())

	err := controller.reconcile(context.Background(), sandboxLifecycleItemFromInfo(SandboxLifecycleInfo{
		Namespace: pod.Namespace,
		PodName:   pod.Name,
		SandboxID: pod.Name,
		TeamID:    "team-a",
	}, false))
	if err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	if len(cleaner.calls) != 0 {
		t.Fatalf("cleanup calls = %d, want 0 for non-deleting pod", len(cleaner.calls))
	}
	updated, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get updated pod: %v", err)
	}
	if !hasSandboxCleanupFinalizer(updated) {
		t.Fatal("sandbox cleanup finalizer was not backfilled")
	}
}

func TestSandboxLifecycleControllerCleansLegacyDeleteEvent(t *testing.T) {
	cleaner := &recordingSandboxCleaner{}
	controller := NewSandboxLifecycleController(fake.NewSimpleClientset(), nil, cleaner, zap.NewNop())

	err := controller.reconcile(context.Background(), sandboxLifecycleQueueItem{
		Namespace: "ns-a",
		PodName:   "sandbox-a",
		SandboxID: "sandbox-a",
		TeamID:    "team-a",
		Deleted:   true,
	})
	if err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	if len(cleaner.calls) != 1 {
		t.Fatalf("cleanup calls = %d, want 1", len(cleaner.calls))
	}
}

func TestSandboxServiceCleanupDeletedSandboxRemovesExternalState(t *testing.T) {
	removed := make([]string, 0, 1)
	store := &deleteRecordingBindingStore{}
	svc := &SandboxService{
		networkProvider: &assertingNetworkProvider{removeFunc: func(namespace, sandboxID string) {
			removed = append(removed, namespace+"/"+sandboxID)
		}},
		credentialStore: store,
		logger:          zap.NewNop(),
	}

	err := svc.CleanupDeletedSandbox(context.Background(), SandboxLifecycleInfo{
		Namespace: "ns-a",
		PodName:   "sandbox-a",
		SandboxID: "sandbox-a",
		TeamID:    "team-a",
	})
	if err != nil {
		t.Fatalf("CleanupDeletedSandbox() error = %v", err)
	}
	if len(removed) != 1 || removed[0] != "ns-a/sandbox-a" {
		t.Fatalf("network removals = %#v, want ns-a/sandbox-a", removed)
	}
	if store.deleteCalls != 1 {
		t.Fatalf("DeleteBindings calls = %d, want 1", store.deleteCalls)
	}
}

func TestSandboxServiceCleanupDeletedSandboxEmitsWebhookAndMarksStateVolumeForCleanup(t *testing.T) {
	volumeClient := &recordingSystemVolumeClient{}
	emitter := &recordingDeletionWebhookEmitter{}
	svc := &SandboxService{
		webhookStateVolumes:    volumeClient,
		deletionWebhookEmitter: emitter,
		logger:                 zap.NewNop(),
	}

	err := svc.CleanupDeletedSandbox(context.Background(), SandboxLifecycleInfo{
		Namespace:            "ns-a",
		PodName:              "sandbox-a",
		SandboxID:            "sandbox-a",
		TeamID:               "team-a",
		UserID:               "user-a",
		WebhookURL:           "https://example.test/webhook",
		WebhookSecret:        "secret",
		WebhookStateVolumeID: "volume-a",
	})
	if err != nil {
		t.Fatalf("CleanupDeletedSandbox() error = %v", err)
	}
	if len(emitter.calls) != 1 {
		t.Fatalf("webhook calls = %d, want 1", len(emitter.calls))
	}
	if emitter.calls[0].WebhookSecret != "secret" {
		t.Fatalf("webhook secret = %q, want secret", emitter.calls[0].WebhookSecret)
	}
	if len(volumeClient.marked) != 1 || volumeClient.marked[0] != "sandbox-a:sandbox_deleted" {
		t.Fatalf("marked volumes = %#v, want sandbox-a:sandbox_deleted", volumeClient.marked)
	}
}

func TestSandboxServiceCleanupDeletedSandboxUnbindsVolumePortals(t *testing.T) {
	var got ctldapi.UnbindVolumePortalRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/volume-portals/unbind" {
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode unbind request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ctldapi.UnbindVolumePortalResponse{Unbound: true})
	}))
	defer ctld.Close()

	ctldURL, err := url.Parse(ctld.URL)
	if err != nil {
		t.Fatalf("parse ctld url: %v", err)
	}
	ctldPort, err := strconv.Atoi(ctldURL.Port())
	if err != nil {
		t.Fatalf("parse ctld port: %v", err)
	}
	svc := &SandboxService{
		ctldClient: NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config: SandboxServiceConfig{
			CtldEnabled: true,
			CtldPort:    ctldPort,
		},
		logger: zap.NewNop(),
	}

	err = svc.CleanupDeletedSandbox(context.Background(), SandboxLifecycleInfo{
		Namespace: "ns-a",
		PodName:   "sandbox-a",
		SandboxID: "sandbox-a",
		PodUID:    "pod-uid-a",
		HostIP:    ctldURL.Hostname(),
		VolumePortals: []SandboxLifecycleVolumePortal{{
			SandboxVolumeID: "vol-1",
			MountPoint:      "/workspace/data",
			PortalName:      "data",
		}},
	})
	if err != nil {
		t.Fatalf("CleanupDeletedSandbox() error = %v", err)
	}
	if got.Namespace != "ns-a" || got.PodName != "sandbox-a" || got.PodUID != "pod-uid-a" {
		t.Fatalf("unexpected pod identity in unbind request: %+v", got)
	}
	if got.SandboxVolumeID != "vol-1" || got.MountPath != "/workspace/data" || got.PortalName != "data" {
		t.Fatalf("unexpected volume portal unbind request: %+v", got)
	}
}

func TestSandboxLifecycleInfoFromPodIncludesWebhookMetadata(t *testing.T) {
	pod := newLifecycleTestPod()
	pod.Annotations[controller.AnnotationUserID] = "user-a"
	pod.Annotations[controller.AnnotationWebhookStateVolumeID] = "volume-a"
	pod.Annotations[controller.AnnotationConfig] = `{"webhook":{"url":"https://example.test/webhook","secret":"secret"}}`

	info, ok := sandboxLifecycleInfoFromPod(pod)
	if !ok {
		t.Fatal("expected lifecycle info")
	}
	if info.UserID != "user-a" || info.WebhookStateVolumeID != "volume-a" {
		t.Fatalf("unexpected lifecycle metadata: %#v", info)
	}
	if info.WebhookURL != "https://example.test/webhook" || info.WebhookSecret != "secret" {
		t.Fatalf("unexpected webhook metadata: %#v", info)
	}
}

func TestSandboxLifecycleInfoFromPodIncludesVolumePortals(t *testing.T) {
	pod := newLifecycleTestPod()
	pod.UID = types.UID("pod-uid-a")
	pod.Spec.NodeName = "node-a"
	pod.Status.HostIP = "10.0.0.8"
	mountsJSON, err := json.Marshal([]ClaimMount{{
		SandboxVolumeID: "vol-1",
		MountPoint:      "/workspace/data",
	}})
	if err != nil {
		t.Fatalf("marshal mounts: %v", err)
	}
	pod.Annotations[controller.AnnotationMounts] = string(mountsJSON)
	pod.Spec.Volumes = []corev1.Volume{{
		Name: "data",
		VolumeSource: corev1.VolumeSource{
			CSI: &corev1.CSIVolumeSource{
				Driver: volumeportal.DriverName,
				VolumeAttributes: map[string]string{
					volumeportal.AttributePortalName: "data",
					volumeportal.AttributeMountPath:  "/workspace/data",
				},
			},
		},
	}}

	info, ok := sandboxLifecycleInfoFromPod(pod)
	if !ok {
		t.Fatal("expected lifecycle info")
	}
	if info.PodUID != "pod-uid-a" || info.NodeName != "node-a" || info.HostIP != "10.0.0.8" {
		t.Fatalf("unexpected pod identity: %#v", info)
	}
	if len(info.VolumePortals) != 1 {
		t.Fatalf("volume portals = %#v, want one", info.VolumePortals)
	}
	portal := info.VolumePortals[0]
	if portal.SandboxVolumeID != "vol-1" || portal.MountPoint != "/workspace/data" || portal.PortalName != "data" {
		t.Fatalf("volume portal = %+v, want vol-1 data", portal)
	}
}

func TestSystemVolumeReconcilerMarksOrphanedOwnedVolume(t *testing.T) {
	volumeClient := &recordingSystemVolumeClient{
		list: []SandboxSystemVolume{{
			VolumeID:       "volume-a",
			TeamID:         "team-a",
			UserID:         "user-a",
			OwnerSandboxID: "sandbox-a",
			OwnerClusterID: "cluster-a",
			Purpose:        "webhook-state",
		}},
	}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	svc := &SandboxService{
		podLister:           corelisters.NewPodLister(indexer),
		webhookStateVolumes: volumeClient,
		clock:               systemTime{},
		logger:              zap.NewNop(),
	}

	if err := svc.reconcileSystemVolumes(context.Background()); err != nil {
		t.Fatalf("reconcileSystemVolumes() error = %v", err)
	}
	if len(volumeClient.marked) != 1 || volumeClient.marked[0] != "sandbox-a:orphaned_sandbox" {
		t.Fatalf("marked = %#v, want sandbox-a:orphaned_sandbox", volumeClient.marked)
	}
}

func TestSystemVolumeReconcilerKeepsActiveOwnedVolume(t *testing.T) {
	volumeClient := &recordingSystemVolumeClient{
		list: []SandboxSystemVolume{{
			VolumeID:       "volume-a",
			TeamID:         "team-a",
			UserID:         "user-a",
			OwnerSandboxID: "sandbox-a",
			OwnerClusterID: "cluster-a",
			Purpose:        "webhook-state",
		}},
	}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	if err := indexer.Add(newLifecycleTestPod()); err != nil {
		t.Fatalf("add pod to indexer: %v", err)
	}
	svc := &SandboxService{
		podLister:           corelisters.NewPodLister(indexer),
		webhookStateVolumes: volumeClient,
		clock:               systemTime{},
		logger:              zap.NewNop(),
	}

	if err := svc.reconcileSystemVolumes(context.Background()); err != nil {
		t.Fatalf("reconcileSystemVolumes() error = %v", err)
	}
	if len(volumeClient.marked) != 0 {
		t.Fatalf("marked = %#v, want none", volumeClient.marked)
	}
}

func TestSystemVolumeReconcilerDeletesCleanupRequestedVolume(t *testing.T) {
	cleanupRequestedAt := time.Now().Add(-time.Minute)
	volumeClient := &recordingSystemVolumeClient{
		list: []SandboxSystemVolume{{
			VolumeID:           "volume-a",
			TeamID:             "team-a",
			UserID:             "user-a",
			OwnerSandboxID:     "sandbox-a",
			OwnerClusterID:     "cluster-a",
			Purpose:            "webhook-state",
			CleanupRequestedAt: &cleanupRequestedAt,
		}},
	}
	svc := &SandboxService{
		webhookStateVolumes: volumeClient,
		clock:               systemTime{},
		logger:              zap.NewNop(),
	}

	if err := svc.reconcileSystemVolumes(context.Background()); err != nil {
		t.Fatalf("reconcileSystemVolumes() error = %v", err)
	}
	if len(volumeClient.deleted) != 1 || volumeClient.deleted[0] != "volume-a" {
		t.Fatalf("deleted = %#v, want volume-a", volumeClient.deleted)
	}
}

func TestTerminateSandboxRequestsPodDeleteWithoutExternalCleanup(t *testing.T) {
	pod := newLifecycleTestPod()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	if err := indexer.Add(pod.DeepCopy()); err != nil {
		t.Fatalf("add pod to indexer: %v", err)
	}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	removed := make([]string, 0, 1)
	store := &deleteRecordingBindingStore{}
	svc := &SandboxService{
		k8sClient: client,
		podLister: corelisters.NewPodLister(indexer),
		networkProvider: &assertingNetworkProvider{removeFunc: func(namespace, sandboxID string) {
			removed = append(removed, namespace+"/"+sandboxID)
		}},
		credentialStore: store,
		logger:          zap.NewNop(),
	}

	if err := svc.TerminateSandbox(context.Background(), pod.Name); err != nil {
		t.Fatalf("TerminateSandbox() error = %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("network removals = %#v, want none before lifecycle controller cleanup", removed)
	}
	if store.deleteCalls != 0 {
		t.Fatalf("DeleteBindings calls = %d, want 0 before lifecycle controller cleanup", store.deleteCalls)
	}
	if _, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Fatalf("pod get error = %v, want not found after delete", err)
	}

	sawFinalizerUpdate := false
	for _, action := range client.Actions() {
		if action.GetVerb() != "update" || action.GetResource().Resource != "pods" {
			continue
		}
		updateAction, ok := action.(k8stesting.UpdateAction)
		if !ok {
			continue
		}
		updatedPod, ok := updateAction.GetObject().(*corev1.Pod)
		if ok && hasSandboxCleanupFinalizer(updatedPod) {
			sawFinalizerUpdate = true
			break
		}
	}
	if !sawFinalizerUpdate {
		t.Fatal("TerminateSandbox did not add sandbox cleanup finalizer before deleting the pod")
	}
}

func newLifecycleTestPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-a",
			Namespace: "ns-a",
			Labels: map[string]string{
				controller.LabelPoolType:  controller.PoolTypeActive,
				controller.LabelSandboxID: "sandbox-a",
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID: "team-a",
			},
		},
	}
}
