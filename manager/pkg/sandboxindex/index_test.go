package sandboxindex

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxpod"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func TestIndexTracksMultipleRuntimePodsForSameSandbox(t *testing.T) {
	index := NewSandboxIndex()
	handler := index.ResourceEventHandler()
	first := indexedPod("ns-a", "pod-a", "sandbox-a")
	second := indexedPod("ns-a", "pod-b", "sandbox-a")

	handler.AddFunc(first)
	handler.AddFunc(second)

	refs := index.GetPodRefs("sandbox-a")
	if len(refs) != 2 {
		t.Fatalf("refs = %#v, want two runtime pod refs", refs)
	}
	handler.DeleteFunc(cache.DeletedFinalStateUnknown{Obj: first})
	refs = index.GetPodRefs("sandbox-a")
	if len(refs) != 1 || refs[0].Name != "pod-b" {
		t.Fatalf("refs after first delete = %#v, want pod-b", refs)
	}
}

func TestIndexMovesUpdatedPodBetweenSandboxes(t *testing.T) {
	index := NewSandboxIndex()
	handler := index.ResourceEventHandler()
	oldPod := indexedPod("ns-a", "pod-a", "sandbox-a")
	newPod := indexedPod("ns-a", "pod-a", "sandbox-b")

	handler.AddFunc(oldPod)
	handler.UpdateFunc(oldPod, newPod)

	if refs := index.GetPodRefs("sandbox-a"); len(refs) != 0 {
		t.Fatalf("old sandbox refs = %#v, want none", refs)
	}
	refs := index.GetPodRefs("sandbox-b")
	if len(refs) != 1 || refs[0].Namespace != "ns-a" || refs[0].Name != "pod-a" {
		t.Fatalf("new sandbox refs = %#v", refs)
	}
}

func indexedPod(namespace, name, sandboxID string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace,
		Name:      name,
		Labels:    map[string]string{sandboxpod.LabelSandboxID: sandboxID},
	}}
}
