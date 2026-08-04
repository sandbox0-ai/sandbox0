package watcher

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxpod"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

func TestHandlePodDeleteRemovesCachedSandboxWhenResourceVersionIsEqual(t *testing.T) {
	w := NewWatcher(nil, 0, zap.NewNop())
	pod := testSandboxPod("sandbox-a", "uid-a", "10", "10.0.0.2", "node-a")

	w.handlePodUpsert(pod)
	w.handlePodDelete(pod.DeepCopy())

	if got := w.ListSandboxesByNode("node-a"); len(got) != 0 {
		t.Fatalf("sandboxes after equal resourceVersion delete = %#v, want empty", got)
	}
}

func TestHandlePodUpsertRemovesDeletingSandboxFromFallbackCache(t *testing.T) {
	w := NewWatcher(nil, 0, zap.NewNop())
	pod := testSandboxPod("sandbox-a", "uid-a", "10", "10.0.0.2", "node-a")
	w.handlePodUpsert(pod)

	deleting := pod.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	deleting.ResourceVersion = pod.ResourceVersion
	w.handlePodUpsert(deleting)

	if got := w.ListSandboxesByNode("node-a"); len(got) != 0 {
		t.Fatalf("sandboxes after deleting update = %#v, want empty", got)
	}
}

func TestHandlePodUpsertDoesNotNotifyForAppliedHashOnlyUpdate(t *testing.T) {
	w := NewWatcher(nil, 0, zap.NewNop())
	notifications := 0
	w.SetSandboxHandlers(func(*SandboxInfo) {
		notifications++
	}, nil)
	pod := testSandboxPod("sandbox-a", "uid-a", "10", "10.0.0.2", "node-a")
	pod.Annotations = map[string]string{
		sandboxpod.AnnotationNetworkPolicy:     `{"mode":"allow-all"}`,
		sandboxpod.AnnotationNetworkPolicyHash: "hash-a",
	}
	w.handlePodUpsert(pod)

	applied := pod.DeepCopy()
	applied.ResourceVersion = "11"
	applied.Annotations[sandboxpod.AnnotationNetworkPolicyAppliedHash] = "hash-a"
	w.handlePodUpsert(applied)

	if notifications != 1 {
		t.Fatalf("sandbox notifications = %d, want 1", notifications)
	}
	got := w.ListSandboxesByNode("node-a")
	if len(got) != 1 || got[0].NetworkAppliedHash != "hash-a" {
		t.Fatalf("cached sandbox = %#v, want updated applied hash", got)
	}
}

func TestHandlePodUpsertNotifiesWhenPolicyHashChanges(t *testing.T) {
	w := NewWatcher(nil, 0, zap.NewNop())
	notifications := 0
	w.SetSandboxHandlers(func(*SandboxInfo) {
		notifications++
	}, nil)
	pod := testSandboxPod("sandbox-a", "uid-a", "10", "10.0.0.2", "node-a")
	pod.Annotations = map[string]string{
		sandboxpod.AnnotationNetworkPolicy:     `{"mode":"allow-all"}`,
		sandboxpod.AnnotationNetworkPolicyHash: "hash-a",
	}
	w.handlePodUpsert(pod)

	changed := pod.DeepCopy()
	changed.ResourceVersion = "11"
	changed.Annotations[sandboxpod.AnnotationNetworkPolicy] = `{"mode":"block-all"}`
	changed.Annotations[sandboxpod.AnnotationNetworkPolicyHash] = "hash-b"
	w.handlePodUpsert(changed)

	if notifications != 2 {
		t.Fatalf("sandbox notifications = %d, want 2", notifications)
	}
}

func TestListSandboxesByNodeUsesInformerCacheAsAuthoritativeSource(t *testing.T) {
	w := NewWatcher(nil, 0, zap.NewNop())
	informer := cache.NewSharedIndexInformer(nil, &corev1.Pod{}, 0, cache.Indexers{podNodeIndex: indexPodByNode})
	w.podInformer = informer

	active := testSandboxPod("sandbox-a", "uid-a", "10", "10.0.0.2", "node-a")
	deleting := testSandboxPod("sandbox-b", "uid-b", "11", "10.0.0.3", "node-a")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	terminal := testSandboxPod("sandbox-c", "uid-c", "12", "10.0.0.4", "node-a")
	terminal.Status.Phase = corev1.PodSucceeded
	noIP := testSandboxPod("sandbox-d", "uid-d", "13", "", "node-a")
	otherNode := testSandboxPod("sandbox-e", "uid-e", "14", "10.0.0.5", "node-b")
	reserved := testSandboxPod("sandbox-f", "uid-f", "15", "10.0.0.6", "node-a")
	reserved.Labels[sandboxpod.LabelPoolType] = sandboxpod.PoolTypeIdle
	reserved.Annotations = map[string]string{
		sandboxpod.AnnotationHotClaimReservation: "reservation-token",
	}
	idle := testSandboxPod("sandbox-g", "uid-g", "16", "10.0.0.7", "node-a")
	idle.Labels[sandboxpod.LabelPoolType] = sandboxpod.PoolTypeIdle

	for _, pod := range []*corev1.Pod{active, deleting, terminal, noIP, otherNode, reserved, idle} {
		if err := informer.GetStore().Add(pod); err != nil {
			t.Fatalf("add pod: %v", err)
		}
	}

	got := w.ListSandboxesByNode("node-a")
	if len(got) != 2 {
		t.Fatalf("node-a sandboxes = %#v, want active and reserved sandboxes", got)
	}
	byName := map[string]*SandboxInfo{}
	for _, info := range got {
		byName[info.Name] = info
	}
	if byName[active.Name] == nil || byName[reserved.Name] == nil {
		t.Fatalf("node-a sandboxes = %#v, want %s and %s", got, active.Name, reserved.Name)
	}

	all := w.ListSandboxesByNode("")
	if len(all) != 3 {
		t.Fatalf("all-node sandboxes = %#v, want active and reserved sandboxes", all)
	}
	byName = map[string]*SandboxInfo{}
	for _, info := range all {
		byName[info.Name] = info
	}
	if byName[active.Name] == nil || byName[otherNode.Name] == nil || byName[reserved.Name] == nil {
		t.Fatalf("all-node sandboxes = %#v, want %s, %s, and %s", all, active.Name, otherNode.Name, reserved.Name)
	}
}

func testSandboxPod(name, uid, resourceVersion, podIP, nodeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            name,
			UID:             types.UID(uid),
			ResourceVersion: resourceVersion,
			Labels: map[string]string{
				sandboxpod.LabelSandboxID: "sandbox-id-" + name,
				sandboxpod.LabelPoolType:  sandboxpod.PoolTypeActive,
			},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: podIP,
		},
	}
}
