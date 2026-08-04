// Package podmeta contains manager-owned helpers for reading sandbox pod metadata.
package podmeta

import (
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxpod"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// SandboxID returns the explicit sandbox identity carried by a pod. Pods
// without identity metadata fall back to their name for legacy compatibility.
func SandboxID(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	if sandboxID := strings.TrimSpace(pod.Labels[sandboxpod.LabelSandboxID]); sandboxID != "" {
		return sandboxID
	}
	if sandboxID := strings.TrimSpace(pod.Annotations[sandboxpod.AnnotationSandboxID]); sandboxID != "" {
		return sandboxID
	}
	return pod.Name
}

// FromInformerEvent extracts a pod from direct and tombstone informer events.
func FromInformerEvent(obj any) *corev1.Pod {
	switch pod := obj.(type) {
	case *corev1.Pod:
		return pod
	case cache.DeletedFinalStateUnknown:
		if pod, ok := pod.Obj.(*corev1.Pod); ok {
			return pod
		}
	case *cache.DeletedFinalStateUnknown:
		if pod, ok := pod.Obj.(*corev1.Pod); ok {
			return pod
		}
	}
	return nil
}
