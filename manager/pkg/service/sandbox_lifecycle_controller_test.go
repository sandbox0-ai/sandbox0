package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
