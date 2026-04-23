package http

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func TestKubernetesVolumeCtldResolverPrefersLocalCtldPod(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}

	client := k8sfake.NewSimpleClientset(
		newResolverPod("sandbox0-system", "storage-proxy-a", "node-system", "10.24.0.4", map[string]string{
			ctldInstanceLabel: "fullmode",
		}, false),
		newResolverPod("sandbox0-system", "fullmode-ctld-system", "node-system", "10.24.0.4", map[string]string{
			ctldNameLabel:     ctldComponentName,
			ctldInstanceLabel: "fullmode",
		}, true),
		newResolverPod("sandbox0-system", "fullmode-ctld-sandbox", "node-sandbox", "10.24.15.221", map[string]string{
			ctldNameLabel:     ctldComponentName,
			ctldInstanceLabel: "fullmode",
		}, true),
	)

	resolver := newKubernetesVolumeCtldResolver(client, "sandbox0-system/storage-proxy-a")
	got, err := resolver.ResolveLocalCtldURL(context.Background())
	if err != nil {
		t.Fatalf("ResolveLocalCtldURL() error = %v", err)
	}
	if got != "http://10.24.0.4:8095" {
		t.Fatalf("ResolveLocalCtldURL() = %q, want %q", got, "http://10.24.0.4:8095")
	}
}

func TestKubernetesVolumeCtldResolverFallsBackToReadyCtldPod(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}

	client := k8sfake.NewSimpleClientset(
		newResolverPod("sandbox0-system", "storage-proxy-a", "node-system", "10.24.0.4", map[string]string{
			ctldInstanceLabel: "fullmode",
		}, false),
		newResolverPod("sandbox0-system", "fullmode-ctld-not-ready", "node-system", "10.24.0.4", map[string]string{
			ctldNameLabel:     ctldComponentName,
			ctldInstanceLabel: "fullmode",
		}, false),
		newResolverPod("sandbox0-system", "fullmode-ctld-sandbox", "node-sandbox", "10.24.15.221", map[string]string{
			ctldNameLabel:     ctldComponentName,
			ctldInstanceLabel: "fullmode",
		}, true),
	)

	resolver := newKubernetesVolumeCtldResolver(client, "sandbox0-system/storage-proxy-a")
	got, err := resolver.ResolveLocalCtldURL(context.Background())
	if err != nil {
		t.Fatalf("ResolveLocalCtldURL() error = %v", err)
	}
	if got != "http://10.24.15.221:8095" {
		t.Fatalf("ResolveLocalCtldURL() = %q, want %q", got, "http://10.24.15.221:8095")
	}
}

func TestKubernetesVolumeCtldResolverFiltersToMatchingInstance(t *testing.T) {
	client := k8sfake.NewSimpleClientset(
		newResolverPod("sandbox0-system", "storage-proxy-a", "node-system", "10.24.0.4", map[string]string{
			ctldInstanceLabel: "fullmode",
		}, false),
		newResolverPod("sandbox0-system", "other-ctld", "node-sandbox-a", "10.24.15.200", map[string]string{
			ctldNameLabel:     ctldComponentName,
			ctldInstanceLabel: "other",
		}, true),
		newResolverPod("sandbox0-system", "fullmode-ctld-sandbox", "node-sandbox-b", "10.24.15.221", map[string]string{
			ctldNameLabel:     ctldComponentName,
			ctldInstanceLabel: "fullmode",
		}, true),
	)

	resolver := newKubernetesVolumeCtldResolver(client, "sandbox0-system/storage-proxy-a")
	got, err := resolver.ResolveLocalCtldURL(context.Background())
	if err != nil {
		t.Fatalf("ResolveLocalCtldURL() error = %v", err)
	}
	if got != "http://10.24.15.221:8095" {
		t.Fatalf("ResolveLocalCtldURL() = %q, want %q", got, "http://10.24.15.221:8095")
	}
}

func newResolverPod(namespace, name, nodeName, podIP string, labels map[string]string, ready bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: podIP,
		},
	}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:   corev1.PodReady,
		Status: corev1.ConditionFalse,
	}}
	if ready {
		pod.Status.Conditions[0].Status = corev1.ConditionTrue
	}
	return pod
}
