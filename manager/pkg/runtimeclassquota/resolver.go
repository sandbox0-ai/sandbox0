// Package runtimeclassquota resolves the Pod overhead Kubernetes admission
// will add for quota calculations without mutating the submitted PodSpec.
package runtimeclassquota

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ResolvePodSpec returns a quota-only copy of spec with RuntimeClass
// overhead.podFixed populated. An already populated spec.Overhead is treated as
// the authoritative API-server observation and is never added twice.
func ResolvePodSpec(
	ctx context.Context,
	client kubernetes.Interface,
	spec *corev1.PodSpec,
) (*corev1.PodSpec, error) {
	if spec == nil {
		return nil, fmt.Errorf("pod spec is required")
	}
	resolved := spec.DeepCopy()
	if resolved.Overhead != nil {
		return resolved, nil
	}
	if resolved.RuntimeClassName == nil {
		return resolved, nil
	}
	runtimeClassName := strings.TrimSpace(*resolved.RuntimeClassName)
	if runtimeClassName == "" {
		return nil, fmt.Errorf("runtime class name is empty")
	}
	if client == nil {
		return nil, fmt.Errorf("kubernetes client is required to resolve RuntimeClass %q", runtimeClassName)
	}
	runtimeClass, err := client.NodeV1().
		RuntimeClasses().
		Get(ctx, runtimeClassName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get RuntimeClass %q: %w", runtimeClassName, err)
	}
	if runtimeClass.Overhead != nil {
		resolved.Overhead = runtimeClass.Overhead.PodFixed.DeepCopy()
	}
	return resolved, nil
}
