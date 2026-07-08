package startlimiter

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestSnapshotCountsWarmReadyNodesAndStartPressure(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(
		readyNode("sandbox-a", map[string]string{"role": "sandbox"}, nil),
		readyNode("sandbox-b", map[string]string{"role": "sandbox"}, []corev1.Taint{{Key: "sandbox", Value: "true", Effect: corev1.TaintEffectNoSchedule}}),
		readyNode("default-a", map[string]string{"role": "default"}, nil),
		notReadyNode("sandbox-not-ready", map[string]string{"role": "sandbox"}),
		unschedulableNode("sandbox-cordoned", map[string]string{"role": "sandbox"}),
		readyIdlePod("default", "idle-ready", "tmpl-a"),
		startingIdlePod("default", "idle-starting", "tmpl-a"),
		startingActivePod("default", "active-starting", "tmpl-a"),
		readyActivePod("default", "active-ready", "tmpl-a"),
		replicaSet("default", "tmpl-a-rs", "tmpl-a", 3),
	)
	limiter, err := New(ctx, Config{
		K8sClient:      client,
		PerSandboxNode: 30,
		MaxLimit:       80,
		SandboxNodeSelector: map[string]string{
			"role": "sandbox",
		},
		SandboxTolerations: []corev1.Toleration{{
			Key:      "sandbox",
			Operator: corev1.TolerationOpEqual,
			Value:    "true",
			Effect:   corev1.TaintEffectNoSchedule,
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	snapshot, err := limiter.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.WarmReadySandboxNodes != 2 {
		t.Fatalf("WarmReadySandboxNodes = %d, want 2", snapshot.WarmReadySandboxNodes)
	}
	if snapshot.Limit != 60 {
		t.Fatalf("Limit = %d, want 60", snapshot.Limit)
	}
	if snapshot.InFlight != 3 {
		t.Fatalf("InFlight = %d, want 3", snapshot.InFlight)
	}
	if snapshot.Available != 57 {
		t.Fatalf("Available = %d, want 57", snapshot.Available)
	}
}

func TestAdmitThrottlesWhenBudgetIsFull(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(
		readyNode("sandbox-a", map[string]string{"role": "sandbox"}, nil),
		startingActivePod("default", "active-starting", "tmpl-a"),
	)
	limiter, err := New(ctx, Config{
		K8sClient:      client,
		PerSandboxNode: 1,
		MaxLimit:       80,
		SandboxNodeSelector: map[string]string{
			"role": "sandbox",
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	called := false
	_, err = limiter.Admit(ctx, ReasonColdCreate, 1, func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrThrottled) {
		t.Fatalf("Admit() error = %v, want ErrThrottled", err)
	}
	if called {
		t.Fatal("Admit() called mutation despite full budget")
	}
}

func TestNewUsesRedisWhenConfigured(t *testing.T) {
	ctx := context.Background()
	redisServer := miniredis.RunT(t)
	client := fake.NewSimpleClientset(readyNode("sandbox-a", nil, nil))

	limiter, err := New(ctx, Config{
		K8sClient: client,
		Redis: rediscache.Config{
			URL:       "redis://" + redisServer.Addr() + "/0",
			KeyPrefix: "test",
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got := limiter.Backend(); got != BackendRedis {
		t.Fatalf("Backend() = %q, want %q", got, BackendRedis)
	}
}

func readyNode(name string, labels map[string]string, taints []corev1.Taint) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec:       corev1.NodeSpec{Taints: taints},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:   corev1.NodeReady,
			Status: corev1.ConditionTrue,
		}}},
	}
}

func notReadyNode(name string, labels map[string]string) *corev1.Node {
	node := readyNode(name, labels, nil)
	node.Status.Conditions[0].Status = corev1.ConditionFalse
	return node
}

func unschedulableNode(name string, labels map[string]string) *corev1.Node {
	node := readyNode(name, labels, nil)
	node.Spec.Unschedulable = true
	return node
}

func readyIdlePod(namespace, name, templateID string) *corev1.Pod {
	pod := sandboxPod(namespace, name, templateID, poolTypeIdle)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	return pod
}

func startingIdlePod(namespace, name, templateID string) *corev1.Pod {
	pod := sandboxPod(namespace, name, templateID, poolTypeIdle)
	pod.Status.Phase = corev1.PodPending
	return pod
}

func readyActivePod(namespace, name, templateID string) *corev1.Pod {
	pod := sandboxPod(namespace, name, templateID, poolTypeActive)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	return pod
}

func startingActivePod(namespace, name, templateID string) *corev1.Pod {
	pod := sandboxPod(namespace, name, templateID, poolTypeActive)
	pod.Status.Phase = corev1.PodPending
	return pod
}

func sandboxPod(namespace, name, templateID, poolType string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				labelTemplateID: templateID,
				labelPoolType:   poolType,
			},
		},
	}
}

func replicaSet(namespace, name, templateID string, replicas int32) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				labelTemplateID: templateID,
			},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: &replicas},
	}
}
