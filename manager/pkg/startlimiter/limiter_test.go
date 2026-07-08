package startlimiter

import (
	"context"
	"errors"
	"testing"
	"time"

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

func TestRedisReserveHoldsBudgetUntilReleased(t *testing.T) {
	ctx := context.Background()
	redisServer := miniredis.RunT(t)
	client := fake.NewSimpleClientset(readyNode("sandbox-a", nil, nil))
	limiter, err := New(ctx, Config{
		ClusterID:      "cluster-a",
		K8sClient:      client,
		PerSandboxNode: 1,
		MaxLimit:       1,
		Redis: rediscache.Config{
			URL:       "redis://" + redisServer.Addr() + "/0",
			KeyPrefix: "test",
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	reservation, snapshot, err := limiter.Reserve(ctx, ReasonColdCreate, 1)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	defer reservation.Release()
	if reservation.Token() == "" {
		t.Fatal("Reservation token is empty")
	}
	if snapshot.Available != 1 {
		t.Fatalf("pre-reservation Available = %d, want 1", snapshot.Available)
	}

	snapshot, err = limiter.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.InFlight != 1 || snapshot.Available != 0 {
		t.Fatalf("Snapshot() inFlight/available = %d/%d, want 1/0", snapshot.InFlight, snapshot.Available)
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
		t.Fatal("Admit() called mutation while reservation held all budget")
	}

	reservation.Release()
	_, err = limiter.Admit(ctx, ReasonColdCreate, 1, func(context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("Admit() after release error = %v", err)
	}
	if !called {
		t.Fatal("Admit() after release did not call mutation")
	}
}

func TestRedisReservationAvoidsDoubleCountingAnnotatedPod(t *testing.T) {
	ctx := context.Background()
	redisServer := miniredis.RunT(t)
	client := fake.NewSimpleClientset(readyNode("sandbox-a", nil, nil))
	limiter, err := New(ctx, Config{
		ClusterID:      "cluster-a",
		K8sClient:      client,
		PerSandboxNode: 10,
		MaxLimit:       10,
		Redis: rediscache.Config{
			URL:       "redis://" + redisServer.Addr() + "/0",
			KeyPrefix: "test",
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	reservation, _, err := limiter.Reserve(ctx, ReasonColdCreate, 1)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	defer reservation.Release()

	pod := startingActivePod("default", "active-starting", "tmpl-a")
	pod.Annotations = map[string]string{
		AnnotationClaimStartReservation: reservation.Token(),
	}
	if _, err := client.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	snapshot, err := limiter.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.InFlight != 1 {
		t.Fatalf("InFlight with active reservation and annotated pod = %d, want 1", snapshot.InFlight)
	}

	reservation.Release()
	snapshot, err = limiter.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() after release error = %v", err)
	}
	if snapshot.InFlight != 1 {
		t.Fatalf("InFlight after reservation release = %d, want pod pressure 1", snapshot.InFlight)
	}
}

func TestRedisLockBusyReturnsThrottled(t *testing.T) {
	ctx := context.Background()
	redisServer := miniredis.RunT(t)
	client := fake.NewSimpleClientset(readyNode("sandbox-a", nil, nil))
	limiter, err := New(ctx, Config{
		ClusterID:      "cluster-a",
		K8sClient:      client,
		PerSandboxNode: 1,
		MaxLimit:       1,
		AcquireTimeout: time.Millisecond,
		Redis: rediscache.Config{
			URL:       "redis://" + redisServer.Addr() + "/0",
			KeyPrefix: "test",
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := limiter.redisClient.Set(ctx, limiter.lockKey, "busy", time.Minute).Err(); err != nil {
		t.Fatalf("seed redis lock: %v", err)
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
		t.Fatal("Admit() called mutation while redis lock was busy")
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
