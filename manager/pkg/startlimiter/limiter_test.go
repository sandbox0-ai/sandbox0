package startlimiter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
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

func TestSnapshotUsesCachedListersWhenConfigured(t *testing.T) {
	ctx := context.Background()
	nodeLister, podLister, replicaSetLister := cachedListers(t,
		[]*corev1.Node{readyNode("sandbox-a", map[string]string{"role": "sandbox"}, nil)},
		nil,
		[]*appsv1.ReplicaSet{replicaSet("default", "tmpl-a-rs", "tmpl-a", 2)},
	)
	limiter, err := New(ctx, Config{
		NodeLister:       nodeLister,
		PodLister:        podLister,
		ReplicaSetLister: replicaSetLister,
		PerSandboxNode:   10,
		MaxLimit:         10,
		SandboxNodeSelector: map[string]string{
			"role": "sandbox",
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	snapshot, err := limiter.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.WarmReadySandboxNodes != 1 {
		t.Fatalf("WarmReadySandboxNodes = %d, want 1", snapshot.WarmReadySandboxNodes)
	}
	if snapshot.Limit != 10 {
		t.Fatalf("Limit = %d, want 10", snapshot.Limit)
	}
	if snapshot.InFlight != 2 {
		t.Fatalf("InFlight = %d, want ReplicaSet desired gap 2", snapshot.InFlight)
	}
	if snapshot.Available != 8 {
		t.Fatalf("Available = %d, want 8", snapshot.Available)
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

func TestRedisReserveTracksMultipleUnits(t *testing.T) {
	ctx := context.Background()
	redisServer := miniredis.RunT(t)
	client := fake.NewSimpleClientset(readyNode("sandbox-a", nil, nil))
	limiter, err := New(ctx, Config{
		ClusterID:      "cluster-a",
		K8sClient:      client,
		PerSandboxNode: 5,
		MaxLimit:       5,
		Redis: rediscache.Config{
			URL:       "redis://" + redisServer.Addr() + "/0",
			KeyPrefix: "test",
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first, _, err := limiter.Reserve(ctx, ReasonPoolReconcile, 3)
	if err != nil {
		t.Fatalf("Reserve(3) error = %v", err)
	}
	defer first.Release()

	_, snapshot, err := limiter.Reserve(ctx, ReasonPoolReconcile, 3)
	if !errors.Is(err, ErrThrottled) {
		t.Fatalf("second Reserve(3) error = %v, want ErrThrottled", err)
	}
	if snapshot.InFlight != 3 || snapshot.Available != 2 {
		t.Fatalf("throttled snapshot in-flight/available = %d/%d, want 3/2", snapshot.InFlight, snapshot.Available)
	}

	second, _, err := limiter.Reserve(ctx, ReasonPoolReconcile, 2)
	if err != nil {
		t.Fatalf("Reserve(2) error = %v", err)
	}
	defer second.Release()

	snapshot, err = limiter.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.InFlight != 5 || snapshot.Available != 0 {
		t.Fatalf("Snapshot() in-flight/available = %d/%d, want 5/0", snapshot.InFlight, snapshot.Available)
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

func TestRedisReserveAtomicallyEnforcesLimitAcrossInstances(t *testing.T) {
	ctx := context.Background()
	redisServer := miniredis.RunT(t)
	client := fake.NewSimpleClientset(readyNode("sandbox-a", nil, nil))
	newLimiter := func() *Limiter {
		limiter, err := New(ctx, Config{
			ClusterID:      "cluster-a",
			K8sClient:      client,
			PerSandboxNode: 80,
			MaxLimit:       80,
			Redis: rediscache.Config{
				URL:       "redis://" + redisServer.Addr() + "/0",
				KeyPrefix: "test",
				Timeout:   2 * time.Second,
			},
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		return limiter
	}
	limiters := []*Limiter{newLimiter(), newLimiter()}

	type result struct {
		reservation *Reservation
		snapshot    *Snapshot
		err         error
	}
	const attempts = 160
	start := make(chan struct{})
	results := make(chan result, attempts)
	var wg sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			reservation, snapshot, err := limiters[index%len(limiters)].Reserve(ctx, ReasonColdCreate, 1)
			results <- result{reservation: reservation, snapshot: snapshot, err: err}
		}(index)
	}
	close(start)
	wg.Wait()
	close(results)

	var reservations []*Reservation
	throttled := 0
	for result := range results {
		if result.err == nil {
			reservations = append(reservations, result.reservation)
			continue
		}
		if !errors.Is(result.err, ErrThrottled) {
			t.Fatalf("Reserve() error = %v, want ErrThrottled", result.err)
		}
		var throttledErr *ThrottledError
		if !errors.As(result.err, &throttledErr) {
			t.Fatalf("Reserve() error type = %T, want *ThrottledError", result.err)
		}
		if result.snapshot == nil || result.snapshot.InFlight != 80 || result.snapshot.Available != 0 {
			t.Fatalf("throttled snapshot = %#v, want in-flight/available 80/0", result.snapshot)
		}
		if got := result.err.Error(); got != "claim start admission throttled: in_flight=80 limit=80 requested=1" {
			t.Fatalf("Reserve() error = %q, want capacity throttle", got)
		}
		throttled++
	}
	if len(reservations) != 80 || throttled != 80 {
		t.Fatalf("reservations/throttled = %d/%d, want 80/80", len(reservations), throttled)
	}

	for _, reservation := range reservations {
		reservation.Release()
	}
	snapshot, err := limiters[0].Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() after release error = %v", err)
	}
	if snapshot.InFlight != 0 || snapshot.Available != 80 {
		t.Fatalf("Snapshot() after release in-flight/available = %d/%d, want 0/80", snapshot.InFlight, snapshot.Available)
	}
}

func TestRedisReserveRemovesExpiredReservation(t *testing.T) {
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
	const expiredToken = "expired"
	if err := limiter.redisClient.ZAdd(ctx, limiter.reservationKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: expiredToken,
	}).Err(); err != nil {
		t.Fatalf("seed expired reservation: %v", err)
	}
	if err := limiter.redisClient.HSet(ctx, limiter.reservationUnitsKey, expiredToken, 1).Err(); err != nil {
		t.Fatalf("seed expired reservation units: %v", err)
	}

	reservation, snapshot, err := limiter.Reserve(ctx, ReasonColdCreate, 1)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	defer reservation.Release()
	if snapshot.InFlight != 0 || snapshot.Available != 1 {
		t.Fatalf("pre-reservation in-flight/available = %d/%d, want 0/1", snapshot.InFlight, snapshot.Available)
	}
	exists, err := limiter.redisClient.HExists(ctx, limiter.reservationUnitsKey, expiredToken).Result()
	if err != nil {
		t.Fatalf("check expired reservation units: %v", err)
	}
	if exists {
		t.Fatal("expired reservation units were not removed")
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

func cachedListers(t *testing.T, nodes []*corev1.Node, pods []*corev1.Pod, replicaSets []*appsv1.ReplicaSet) (corelisters.NodeLister, corelisters.PodLister, appslisters.ReplicaSetLister) {
	t.Helper()
	nodeIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, node := range nodes {
		if err := nodeIndexer.Add(node); err != nil {
			t.Fatalf("add node to indexer: %v", err)
		}
	}
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, pod := range pods {
		if err := podIndexer.Add(pod); err != nil {
			t.Fatalf("add pod to indexer: %v", err)
		}
	}
	replicaSetIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, replicaSet := range replicaSets {
		if err := replicaSetIndexer.Add(replicaSet); err != nil {
			t.Fatalf("add ReplicaSet to indexer: %v", err)
		}
	}
	return corelisters.NewNodeLister(nodeIndexer),
		corelisters.NewPodLister(podIndexer),
		appslisters.NewReplicaSetLister(replicaSetIndexer)
}
