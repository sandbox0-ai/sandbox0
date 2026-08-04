package podmeta

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxpod"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func TestSandboxID(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:        "pod-a",
		Labels:      map[string]string{sandboxpod.LabelSandboxID: "sandbox-from-label"},
		Annotations: map[string]string{sandboxpod.AnnotationSandboxID: "sandbox-from-annotation"},
	}}
	if got := SandboxID(pod); got != "sandbox-from-label" {
		t.Fatalf("SandboxID() = %q, want label identity", got)
	}
	delete(pod.Labels, sandboxpod.LabelSandboxID)
	if got := SandboxID(pod); got != "sandbox-from-annotation" {
		t.Fatalf("SandboxID() = %q, want annotation identity", got)
	}
	delete(pod.Annotations, sandboxpod.AnnotationSandboxID)
	if got := SandboxID(pod); got != "pod-a" {
		t.Fatalf("SandboxID() = %q, want pod name fallback", got)
	}
}

func TestFromInformerEvent(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-a"}}
	if got := FromInformerEvent(cache.DeletedFinalStateUnknown{Obj: pod}); got != pod {
		t.Fatalf("FromInformerEvent() = %#v, want pod", got)
	}
	if got := FromInformerEvent("not-a-pod"); got != nil {
		t.Fatalf("FromInformerEvent() = %#v, want nil", got)
	}
}
