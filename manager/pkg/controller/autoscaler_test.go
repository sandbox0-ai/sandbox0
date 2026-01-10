package controller

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func newTestPodLister(t *testing.T, pods ...*corev1.Pod) corelisters.PodLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, p := range pods {
		if p == nil {
			continue
		}
		if err := indexer.Add(p); err != nil {
			t.Fatalf("add pod: %v", err)
		}
	}
	return corelisters.NewPodLister(indexer)
}

func mustCreateRS(t *testing.T, client *fake.Clientset, rs *appsv1.ReplicaSet) {
	t.Helper()
	if rs == nil {
		t.Fatalf("nil rs")
	}
	_, err := client.AppsV1().ReplicaSets(rs.Namespace).Create(context.Background(), rs, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create rs: %v", err)
	}
}

func getRSReplicas(t *testing.T, client *fake.Clientset, ns, name string) int32 {
	t.Helper()
	rs, err := client.AppsV1().ReplicaSets(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get rs: %v", err)
	}
	if rs.Spec.Replicas == nil {
		return 0
	}
	return *rs.Spec.Replicas
}

func TestAutoScaler_SlowStartScaleUp(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tpl",
			Namespace: "sb0",
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{
				MinIdle:   0,
				MaxIdle:   50,
				AutoScale: true,
			},
		},
	}

	rsName := v1alpha1.GenReplicasetName(template)
	k8s := fake.NewSimpleClientset()
	mustCreateRS(t, k8s, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
			},
			Annotations: map[string]string{},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: ptr32(1), // below SlowStartThreshold (default 4)
		},
	})

	activePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p1",
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeActive,
			},
			Annotations: map[string]string{
				AnnotationClaimedAt: now.Add(-30 * time.Second).Format(time.RFC3339),
				AnnotationClaimType: "cold",
			},
		},
	}

	podLister := newTestPodLister(t, activePod)
	as := NewAutoScaler(k8s, podLister, zap.NewNop())

	if err := as.ReconcileAutoScale(ctx, template, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Slow start should increase replicas quickly (at least doubling from 1 -> >=2).
	replicas := getRSReplicas(t, k8s, template.Namespace, rsName)
	if replicas < 2 {
		t.Fatalf("expected replicas to scale up to >=2, got %d", replicas)
	}
}

func TestAutoScaler_ScaleUpClampedToMaxIdle(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl", Namespace: "sb0"},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 3, AutoScale: true},
		},
	}
	rsName := v1alpha1.GenReplicasetName(template)

	k8s := fake.NewSimpleClientset()
	mustCreateRS(t, k8s, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: rsName, Namespace: template.Namespace, Annotations: map[string]string{}},
		Spec:       appsv1.ReplicaSetSpec{Replicas: ptr32(3)},
	})

	activePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p1",
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeActive,
			},
			Annotations: map[string]string{
				AnnotationClaimedAt: now.Add(-20 * time.Second).Format(time.RFC3339),
				AnnotationClaimType: "cold",
			},
		},
	}
	as := NewAutoScaler(k8s, newTestPodLister(t, activePod), zap.NewNop())

	if err := as.ReconcileAutoScale(ctx, template, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	replicas := getRSReplicas(t, k8s, template.Namespace, rsName)
	if replicas != 3 {
		t.Fatalf("expected replicas to remain clamped at maxIdle=3, got %d", replicas)
	}
}

func TestAutoScaler_ScaleDownOnNoTraffic(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl", Namespace: "sb0"},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 2, MaxIdle: 50, AutoScale: true},
		},
	}
	rsName := v1alpha1.GenReplicasetName(template)

	k8s := fake.NewSimpleClientset()
	// No recent claims: we make last scale time old so scale-down cooldown passes.
	mustCreateRS(t, k8s, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: template.Namespace,
			Annotations: map[string]string{
				annotationAutoscaleLastScaleTime: now.Add(-10 * time.Minute).Format(time.RFC3339),
				annotationAutoscaleLastClaimTime: now.Add(-20 * time.Minute).Format(time.RFC3339),
			},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: ptr32(20)},
	})

	// No active pods => treated as no traffic at least window size.
	as := NewAutoScaler(k8s, newTestPodLister(t), zap.NewNop())
	if err := as.ReconcileAutoScale(ctx, template, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	replicas := getRSReplicas(t, k8s, template.Namespace, rsName)
	if replicas >= 20 {
		t.Fatalf("expected scale down to reduce replicas, got %d", replicas)
	}
	if replicas < template.Spec.Pool.MinIdle {
		t.Fatalf("expected replicas >= minIdle=%d, got %d", template.Spec.Pool.MinIdle, replicas)
	}
}

func TestAutoScaler_ScaleUpCooldownRespected(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl", Namespace: "sb0"},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 50, AutoScale: true},
		},
	}
	rsName := v1alpha1.GenReplicasetName(template)

	k8s := fake.NewSimpleClientset()
	// last scale time is very recent, so ScaleUpCooldown should prevent bump.
	mustCreateRS(t, k8s, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: template.Namespace,
			Annotations: map[string]string{
				annotationAutoscaleLastScaleTime: now.Add(-5 * time.Second).Format(time.RFC3339),
			},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: ptr32(5)},
	})

	activePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p1",
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeActive,
			},
			Annotations: map[string]string{
				AnnotationClaimedAt: now.Add(-20 * time.Second).Format(time.RFC3339),
				AnnotationClaimType: "cold",
			},
		},
	}

	as := NewAutoScaler(k8s, newTestPodLister(t, activePod), zap.NewNop())
	if err := as.ReconcileAutoScale(ctx, template, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	replicas := getRSReplicas(t, k8s, template.Namespace, rsName)
	if replicas != 5 {
		t.Fatalf("expected replicas unchanged due to cooldown, got %d", replicas)
	}
}

func TestAutoScaler_ClampToMinIdle(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl", Namespace: "sb0"},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 7, MaxIdle: 50, AutoScale: true},
		},
	}
	rsName := v1alpha1.GenReplicasetName(template)

	k8s := fake.NewSimpleClientset()
	mustCreateRS(t, k8s, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: rsName, Namespace: template.Namespace, Annotations: map[string]string{}},
		Spec:       appsv1.ReplicaSetSpec{Replicas: ptr32(2)},
	})

	as := NewAutoScaler(k8s, newTestPodLister(t), zap.NewNop())
	if err := as.ReconcileAutoScale(ctx, template, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	replicas := getRSReplicas(t, k8s, template.Namespace, rsName)
	if replicas < template.Spec.Pool.MinIdle {
		t.Fatalf("expected replicas clamped to minIdle=%d, got %d", template.Spec.Pool.MinIdle, replicas)
	}
}

func TestAutoScaler_MaxIdleLessThanMinIdle_ClampsToMinIdle(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl", Namespace: "sb0"},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 5, MaxIdle: 3, AutoScale: true},
		},
	}
	rsName := v1alpha1.GenReplicasetName(template)

	k8s := fake.NewSimpleClientset()
	mustCreateRS(t, k8s, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: rsName, Namespace: template.Namespace, Annotations: map[string]string{}},
		Spec:       appsv1.ReplicaSetSpec{Replicas: ptr32(0)},
	})

	as := NewAutoScaler(k8s, newTestPodLister(t), zap.NewNop())
	if err := as.ReconcileAutoScale(ctx, template, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	replicas := getRSReplicas(t, k8s, template.Namespace, rsName)
	if replicas != 5 {
		t.Fatalf("expected replicas clamped to minIdle=5 when maxIdle<minIdle, got %d", replicas)
	}
}

func TestAutoScaler_ReplicaSetMissing_NoError(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl", Namespace: "sb0"},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 10, AutoScale: true},
		},
	}

	k8s := fake.NewSimpleClientset()
	as := NewAutoScaler(k8s, newTestPodLister(t), zap.NewNop())
	if err := as.ReconcileAutoScale(ctx, template, now); err != nil {
		t.Fatalf("expected no error when RS missing, got %v", err)
	}
}

func TestAutoScaler_AutoScaleDisabled_NoChange(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl", Namespace: "sb0"},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 10, AutoScale: false},
		},
	}
	rsName := v1alpha1.GenReplicasetName(template)

	k8s := fake.NewSimpleClientset()
	mustCreateRS(t, k8s, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: rsName, Namespace: template.Namespace, Annotations: map[string]string{}},
		Spec:       appsv1.ReplicaSetSpec{Replicas: ptr32(3)},
	})

	activePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p1",
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeActive,
			},
			Annotations: map[string]string{
				AnnotationClaimedAt: now.Add(-20 * time.Second).Format(time.RFC3339),
				AnnotationClaimType: "cold",
			},
		},
	}

	as := NewAutoScaler(k8s, newTestPodLister(t, activePod), zap.NewNop())
	if err := as.ReconcileAutoScale(ctx, template, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	replicas := getRSReplicas(t, k8s, template.Namespace, rsName)
	if replicas != 3 {
		t.Fatalf("expected replicas unchanged when autoscale disabled, got %d", replicas)
	}
}

func TestAutoScaler_IgnoreBadOrOldClaimAnnotations(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl", Namespace: "sb0"},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 50, AutoScale: true},
		},
	}
	rsName := v1alpha1.GenReplicasetName(template)

	k8s := fake.NewSimpleClientset()
	mustCreateRS(t, k8s, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: template.Namespace,
			// Pin last-claim-time to "recent" so scale-down doesn't happen in this test.
			// This test only asserts that bad/old cold claims do not trigger scale-up.
			Annotations: map[string]string{
				annotationAutoscaleLastClaimTime: now.Add(-30 * time.Second).Format(time.RFC3339),
			},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: ptr32(5)},
	})

	// One pod has invalid claimed-at, another is outside the window; neither should trigger scale-up.
	badTimePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bad",
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeActive,
			},
			Annotations: map[string]string{
				AnnotationClaimedAt: "not-a-time",
				AnnotationClaimType: "cold",
			},
		},
	}
	oldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "old",
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeActive,
			},
			Annotations: map[string]string{
				AnnotationClaimedAt: now.Add(-10 * time.Minute).Format(time.RFC3339),
				AnnotationClaimType: "cold",
			},
		},
	}

	as := NewAutoScaler(k8s, newTestPodLister(t, badTimePod, oldPod), zap.NewNop())
	if err := as.ReconcileAutoScale(ctx, template, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	replicas := getRSReplicas(t, k8s, template.Namespace, rsName)
	if replicas != 5 {
		t.Fatalf("expected replicas unchanged when only bad/old claims exist, got %d", replicas)
	}
}

func TestAutoScaler_PersistsLastClaimTimeEvenWithoutReplicaChange(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl", Namespace: "sb0"},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 10, AutoScale: true},
		},
	}
	rsName := v1alpha1.GenReplicasetName(template)

	k8s := fake.NewSimpleClientset()
	mustCreateRS(t, k8s, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        rsName,
			Namespace:   template.Namespace,
			Annotations: map[string]string{},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: ptr32(5)},
	})

	// A hot claim should not necessarily change replicas, but should update last-claim-time annotation.
	activePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p1",
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeActive,
			},
			Annotations: map[string]string{
				AnnotationClaimedAt: now.Add(-10 * time.Second).Format(time.RFC3339),
				AnnotationClaimType: "hot",
			},
		},
	}

	as := NewAutoScaler(k8s, newTestPodLister(t, activePod), zap.NewNop())
	if err := as.ReconcileAutoScale(ctx, template, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	rs, err := k8s.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rsName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get rs: %v", err)
	}
	if rs.Annotations == nil || rs.Annotations[annotationAutoscaleLastClaimTime] == "" {
		t.Fatalf("expected %s annotation to be persisted on RS", annotationAutoscaleLastClaimTime)
	}
}
