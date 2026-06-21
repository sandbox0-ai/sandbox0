package watcher

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
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

	for _, pod := range []*corev1.Pod{active, deleting, terminal, noIP, otherNode} {
		if err := informer.GetStore().Add(pod); err != nil {
			t.Fatalf("add pod: %v", err)
		}
	}

	got := w.ListSandboxesByNode("node-a")
	if len(got) != 1 {
		t.Fatalf("node-a sandboxes = %#v, want exactly active sandbox", got)
	}
	if got[0].Name != active.Name || got[0].PodIP != active.Status.PodIP {
		t.Fatalf("unexpected sandbox: %#v", got[0])
	}

	all := w.ListSandboxesByNode("")
	if len(all) != 2 {
		t.Fatalf("all-node sandboxes = %#v, want active sandboxes from both nodes", all)
	}
	byName := map[string]*SandboxInfo{}
	for _, info := range all {
		byName[info.Name] = info
	}
	if byName[active.Name] == nil || byName[otherNode.Name] == nil {
		t.Fatalf("all-node sandboxes = %#v, want %s and %s", all, active.Name, otherNode.Name)
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
				controller.LabelSandboxID: "sandbox-id-" + name,
				controller.LabelPoolType:  controller.PoolTypeActive,
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
