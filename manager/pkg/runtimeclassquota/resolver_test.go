package runtimeclassquota

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestResolvePodSpecUsesRuntimeClassPodFixedWithoutMutatingInput(t *testing.T) {
	runtimeClassName := "sandbox-runtime"
	client := fake.NewSimpleClientset(&nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: runtimeClassName},
		Handler:    "sandbox",
		Overhead: &nodev1.Overhead{PodFixed: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("75m"),
			corev1.ResourceMemory: resource.MustParse("96Mi"),
		}},
	})
	spec := &corev1.PodSpec{RuntimeClassName: &runtimeClassName}

	resolved, err := ResolvePodSpec(context.Background(), client, spec)
	if err != nil {
		t.Fatalf("ResolvePodSpec() error = %v", err)
	}
	if spec.Overhead != nil {
		t.Fatalf("input overhead = %v, want nil", spec.Overhead)
	}
	if got := resolved.Overhead[corev1.ResourceCPU]; got.Cmp(resource.MustParse("75m")) != 0 {
		t.Fatalf("resolved CPU overhead = %s, want 75m", got.String())
	}
	if got := resolved.Overhead[corev1.ResourceMemory]; got.Cmp(resource.MustParse("96Mi")) != 0 {
		t.Fatalf("resolved memory overhead = %s, want 96Mi", got.String())
	}
}

func TestResolvePodSpecKeepsObservedPodOverheadAuthoritative(t *testing.T) {
	runtimeClassName := "sandbox-runtime"
	client := fake.NewSimpleClientset(&nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: runtimeClassName},
		Handler:    "sandbox",
		Overhead: &nodev1.Overhead{PodFixed: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("75m"),
		}},
	})
	observed := corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("125m"),
	}
	spec := &corev1.PodSpec{
		RuntimeClassName: &runtimeClassName,
		Overhead:         observed,
	}

	resolved, err := ResolvePodSpec(context.Background(), client, spec)
	if err != nil {
		t.Fatalf("ResolvePodSpec() error = %v", err)
	}
	if got := resolved.Overhead[corev1.ResourceCPU]; got.Cmp(resource.MustParse("125m")) != 0 {
		t.Fatalf("resolved CPU overhead = %s, want observed 125m", got.String())
	}
	if got := len(client.Actions()); got != 0 {
		t.Fatalf("Kubernetes actions = %d, want 0 because observed overhead is authoritative", got)
	}
}

func TestResolvePodSpecWithoutRuntimeClassDoesNotRequireClient(t *testing.T) {
	spec := &corev1.PodSpec{}

	resolved, err := ResolvePodSpec(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("ResolvePodSpec() error = %v", err)
	}
	if resolved == spec {
		t.Fatal("ResolvePodSpec() returned the input pointer, want a quota-only copy")
	}
	if resolved.Overhead != nil {
		t.Fatalf("resolved overhead = %v, want nil", resolved.Overhead)
	}
}

func TestResolvePodSpecFailsWhenRuntimeClassIsNotFound(t *testing.T) {
	runtimeClassName := "missing-runtime"

	_, err := ResolvePodSpec(
		context.Background(),
		fake.NewSimpleClientset(),
		&corev1.PodSpec{RuntimeClassName: &runtimeClassName},
	)
	if err == nil {
		t.Fatal("ResolvePodSpec() error = nil, want RuntimeClass not found")
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("ResolvePodSpec() error = %v, want Kubernetes not found", err)
	}
}
