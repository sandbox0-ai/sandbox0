package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestAutoScalerColdClaimRefillsIdleDeficitByStep(t *testing.T) {
	template := autoscalerTestTemplate("template-a", 15, 50)
	scaler, client := newAutoScalerTestHarness(t, template, 15, nil, AutoScaleConfig{
		MinScaleInterval:        time.Millisecond,
		ScaleUpFactor:           1.5,
		MaxScaleStep:            10,
		MinIdleBuffer:           2,
		TargetIdleRatio:         0.2,
		NoTrafficScaleDownAfter: time.Minute,
		ScaleDownPercent:        0.1,
	})

	decision, err := scaler.OnColdClaim(context.Background(), template)
	require.NoError(t, err)
	require.True(t, decision.ShouldScale)
	assert.Equal(t, int32(15), decision.OldReplicas)
	assert.Equal(t, int32(25), decision.NewReplicas)
	assert.Equal(t, int32(10), decision.Delta)

	rs := getAutoscalerTestReplicaSet(t, client, template)
	assert.Equal(t, int32(25), *rs.Spec.Replicas)
}

func TestAutoScalerColdClaimUsesLiveReplicaSetTarget(t *testing.T) {
	template := autoscalerTestTemplate("template-a", 15, 50)
	scaler, client := newAutoScalerTestHarness(t, template, 15, nil, AutoScaleConfig{
		MinScaleInterval:        time.Millisecond,
		ScaleUpFactor:           1.5,
		MaxScaleStep:            10,
		MinIdleBuffer:           2,
		TargetIdleRatio:         0.2,
		NoTrafficScaleDownAfter: time.Minute,
		ScaleDownPercent:        0.1,
	})
	rs := getAutoscalerTestReplicaSet(t, client, template)
	liveReplicas := int32(25)
	rs.Spec.Replicas = &liveReplicas
	_, err := client.AppsV1().ReplicaSets(template.Namespace).Update(context.Background(), rs, metav1.UpdateOptions{})
	require.NoError(t, err)

	decision, err := scaler.OnColdClaim(context.Background(), template)
	require.NoError(t, err)
	require.True(t, decision.ShouldScale)
	assert.Equal(t, int32(25), decision.OldReplicas)
	assert.Equal(t, int32(35), decision.NewReplicas)

	rs = getAutoscalerTestReplicaSet(t, client, template)
	assert.Equal(t, int32(35), *rs.Spec.Replicas)
}

func TestAutoScalerHotClaimSkipsWhenPendingIdleCoversTarget(t *testing.T) {
	template := autoscalerTestTemplate("template-a", 15, 50)
	var pods []*corev1.Pod
	for i := 0; i < 17; i++ {
		pods = append(pods, autoscalerIdlePod(template, "idle-pending-"+string(rune('a'+i)), false))
	}
	scaler, _ := newAutoScalerTestHarness(t, template, 17, pods, AutoScaleConfig{
		MinScaleInterval:        time.Millisecond,
		ScaleUpFactor:           1.5,
		MaxScaleStep:            10,
		MinIdleBuffer:           2,
		TargetIdleRatio:         0.2,
		NoTrafficScaleDownAfter: time.Minute,
		ScaleDownPercent:        0.1,
	})

	decision, err := scaler.OnHotClaim(context.Background(), template)
	require.NoError(t, err)
	require.False(t, decision.ShouldScale)
	assert.Contains(t, decision.Reason, "no scale needed")
}

func TestAutoScalerAdmitColdClaimRejectsNetworkBacklogAndRecordsMetric(t *testing.T) {
	template := autoscalerTestTemplate("template-a", 1, 10)
	pods := []*corev1.Pod{
		autoscalerActivePod(template, "active-no-ip", corev1.PodPending, ""),
	}
	scaler, _ := newAutoScalerTestHarness(t, template, 1, pods, AutoScaleConfig{
		MinScaleInterval:        time.Millisecond,
		ScaleUpFactor:           1.5,
		MaxScaleStep:            10,
		MinIdleBuffer:           2,
		TargetIdleRatio:         0.2,
		NoTrafficScaleDownAfter: time.Minute,
		ScaleDownPercent:        0.1,
		MaxColdClaimInFlight:    1,
	})
	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewManager(registry)
	scaler.SetMetrics(metrics)

	admission, err := scaler.AdmitColdClaim(context.Background(), template)
	require.NoError(t, err)
	require.False(t, admission.Admitted)
	assert.Equal(t, "pod network identity backlog", admission.Reason)
	assert.Equal(t, int32(1), admission.NetworkBacklog)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.AutoscalerDecisionsTotal.WithLabelValues(
		"template-a", "admit_cold_claim", "pod_network_identity_backlog", "rejected",
	)))
}

func TestAutoScalerReconcileScaleDownReturnsToMinIdle(t *testing.T) {
	template := autoscalerTestTemplate("template-a", 15, 50)
	scaler, client := newAutoScalerTestHarness(t, template, 30, nil, AutoScaleConfig{
		MinScaleInterval:        time.Millisecond,
		ScaleUpFactor:           1.5,
		MaxScaleStep:            10,
		MinIdleBuffer:           2,
		TargetIdleRatio:         0.2,
		NoTrafficScaleDownAfter: time.Minute,
		ScaleDownPercent:        0.1,
	})

	err := scaler.ReconcileScaleDown(context.Background(), template, time.Now().Add(2*time.Minute))
	require.NoError(t, err)

	rs := getAutoscalerTestReplicaSet(t, client, template)
	assert.Equal(t, int32(15), *rs.Spec.Replicas)
}

func TestAutoScalerPoolStatsCountsBacklog(t *testing.T) {
	template := autoscalerTestTemplate("template-a", 1, 10)
	pods := []*corev1.Pod{
		autoscalerIdlePod(template, "idle-ready", true),
		autoscalerIdlePod(template, "idle-pending", false),
		autoscalerActivePod(template, "active-running", corev1.PodRunning, "10.0.0.10"),
		autoscalerActivePod(template, "active-no-ip", corev1.PodPending, ""),
	}
	scaler, _ := newAutoScalerTestHarness(t, template, 1, pods, DefaultAutoScaleConfig())

	stats, err := scaler.getPoolStatsDetailed(template)
	require.NoError(t, err)
	assert.Equal(t, int32(1), stats.readyIdle)
	assert.Equal(t, int32(1), stats.pendingIdle)
	assert.Equal(t, int32(1), stats.runningActive)
	assert.Equal(t, int32(1), stats.pendingActive)
	assert.Equal(t, int32(1), stats.activeWithoutIP)
}

func TestToAutoScaleConfigAppliesRuntimeDefaults(t *testing.T) {
	cfg := toAutoScaleConfig(apiconfig.AutoscalerConfig{})
	defaults := DefaultAutoScaleConfig()
	assert.Equal(t, defaults.MinScaleInterval, cfg.MinScaleInterval)
	assert.Equal(t, defaults.MaxScaleStep, cfg.MaxScaleStep)
	assert.Equal(t, defaults.MinIdleBuffer, cfg.MinIdleBuffer)
	assert.Equal(t, defaults.NoTrafficScaleDownAfter, cfg.NoTrafficScaleDownAfter)
}

func TestNewAutoScalerWithConfigAppliesZeroDefaults(t *testing.T) {
	scaler := NewAutoScalerWithConfig(
		fake.NewSimpleClientset(),
		corelisters.NewPodLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})),
		appslisters.NewReplicaSetLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})),
		zap.NewNop(),
		AutoScaleConfig{},
	)

	defaults := DefaultAutoScaleConfig()
	assert.Equal(t, defaults.MinScaleInterval, scaler.config.MinScaleInterval)
	assert.Equal(t, defaults.MaxScaleStep, scaler.config.MaxScaleStep)
	assert.Equal(t, defaults.MinIdleBuffer, scaler.config.MinIdleBuffer)
	assert.Equal(t, defaults.NoTrafficScaleDownAfter, scaler.config.NoTrafficScaleDownAfter)
}

func TestScaleRateLimiter(t *testing.T) {
	limiter := newScaleRateLimiter(100 * time.Millisecond)
	key := "test-template"

	assert.True(t, limiter.TryAcquire(key))
	assert.False(t, limiter.TryAcquire(key))
	limiter.Complete(key)
	assert.False(t, limiter.TryAcquire(key))
	time.Sleep(110 * time.Millisecond)
	assert.True(t, limiter.TryAcquire(key))
}

func TestScaleRateLimiterConcurrency(t *testing.T) {
	limiter := newScaleRateLimiter(10 * time.Millisecond)
	key := "concurrent-template"
	var successCount int32
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if limiter.TryAcquire(key) {
				successCount++
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int32(1), successCount)
}

func autoscalerTestTemplate(name string, minIdle, maxIdle int32) *v1alpha1.SandboxTemplate {
	return &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{
				MinIdle: minIdle,
				MaxIdle: maxIdle,
			},
		},
	}
}

func newAutoScalerTestHarness(
	t *testing.T,
	template *v1alpha1.SandboxTemplate,
	replicas int32,
	pods []*corev1.Pod,
	config AutoScaleConfig,
) (*AutoScaler, *fake.Clientset) {
	t.Helper()

	rsName, err := replicasetNameForTest(template)
	require.NoError(t, err)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: template.Namespace,
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
		},
	}
	objects := []runtime.Object{rs}
	for _, pod := range pods {
		objects = append(objects, pod)
	}
	client := fake.NewSimpleClientset(objects...)

	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, pod := range pods {
		require.NoError(t, podIndexer.Add(pod))
	}
	rsIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, rsIndexer.Add(rs))

	return NewAutoScalerWithConfig(
		client,
		corelisters.NewPodLister(podIndexer),
		appslisters.NewReplicaSetLister(rsIndexer),
		zap.NewNop(),
		config,
	), client
}

func autoscalerIdlePod(template *v1alpha1.SandboxTemplate, name string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeIdle,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "main",
				Image: "busybox",
				Ports: []corev1.ContainerPort{{
					Name:          "http",
					ContainerPort: 8080,
					Protocol:      corev1.ProtocolTCP,
				}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/healthz",
							Port: intstr.FromInt(8080),
						},
					},
				},
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: status,
			}},
		},
	}
}

func autoscalerActivePod(template *v1alpha1.SandboxTemplate, name string, phase corev1.PodPhase, podIP string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeActive,
			},
		},
		Status: corev1.PodStatus{
			Phase: phase,
			PodIP: podIP,
		},
	}
}

func getAutoscalerTestReplicaSet(t *testing.T, client *fake.Clientset, template *v1alpha1.SandboxTemplate) *appsv1.ReplicaSet {
	t.Helper()
	name, err := replicasetNameForTest(template)
	require.NoError(t, err)
	rs, err := client.AppsV1().ReplicaSets(template.Namespace).Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)
	return rs
}

func replicasetNameForTest(template *v1alpha1.SandboxTemplate) (string, error) {
	return naming.ReplicasetName(naming.DefaultClusterID, template.Name)
}
