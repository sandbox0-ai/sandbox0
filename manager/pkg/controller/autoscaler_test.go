package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestAutoScaler_OnColdClaim(t *testing.T) {
	tests := []struct {
		name             string
		template         *v1alpha1.SandboxTemplate
		existingPods     []*corev1.Pod
		existingRS       *appsv1.ReplicaSet
		expectedShould   bool
		expectedDelta    int32
		expectedReplicas int32
		expectError      bool
		skipIfNoRS       bool // If true, skip when ReplicaSet doesn't exist
		testRateLimit    bool // If true, test rate limiting by making two calls
	}{
		{
			name: "scale up on cold claim with no idle pods",
			template: &v1alpha1.SandboxTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-template",
					Namespace: "default",
				},
				Spec: v1alpha1.SandboxTemplateSpec{
					Pool: v1alpha1.PoolStrategy{
						MinIdle: 2,
						MaxIdle: 20,
					},
				},
			},
			existingPods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "active-1",
						Namespace: "default",
						Labels: map[string]string{
							LabelTemplateID: "test-template",
							LabelPoolType:   PoolTypeActive,
						},
						Annotations: map[string]string{
							AnnotationClaimedAt: time.Now().Add(-1 * time.Minute).Format(time.RFC3339),
							AnnotationClaimType: "cold",
						},
					},
					Status: corev1.PodStatus{Phase: corev1.PodRunning},
				},
			},
			existingRS: func() *appsv1.ReplicaSet {
				rsName, _ := naming.ReplicasetName(naming.DefaultClusterID, "test-template")
				replicas := int32(2)
				return &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      rsName,
						Namespace: "default",
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: &replicas,
					},
				}
			}(),
			expectedShould:   true,
			expectedDelta:    1,
			expectedReplicas: 3,
		},
		{
			name: "no scale when rate limited",
			template: &v1alpha1.SandboxTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rate-limited-template",
					Namespace: "default",
				},
				Spec: v1alpha1.SandboxTemplateSpec{
					Pool: v1alpha1.PoolStrategy{
						MinIdle: 2,
						MaxIdle: 20,
					},
				},
			},
			existingPods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "active-1",
						Namespace: "default",
						Labels: map[string]string{
							LabelTemplateID: "rate-limited-template",
							LabelPoolType:   PoolTypeActive,
						},
						Annotations: map[string]string{
							AnnotationClaimedAt: time.Now().Format(time.RFC3339),
						},
					},
					Status: corev1.PodStatus{Phase: corev1.PodRunning},
				},
			},
			existingRS: func() *appsv1.ReplicaSet {
				rsName, _ := naming.ReplicasetName(naming.DefaultClusterID, "rate-limited-template")
				replicas := int32(2)
				return &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      rsName,
						Namespace: "default",
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: &replicas,
					},
				}
			}(),
			expectedShould: false,
			// This test makes TWO calls to verify rate limiting works
			// First call will succeed, second call will be rate limited
			testRateLimit: true,
		},
		{
			name: "respect maxIdle limit",
			template: &v1alpha1.SandboxTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "max-idle-template",
					Namespace: "default",
				},
				Spec: v1alpha1.SandboxTemplateSpec{
					Pool: v1alpha1.PoolStrategy{
						MinIdle: 5,
						MaxIdle: 5,
					},
				},
			},
			existingPods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "active-1",
						Namespace: "default",
						Labels: map[string]string{
							LabelTemplateID: "max-idle-template",
							LabelPoolType:   PoolTypeActive,
						},
					},
					Status: corev1.PodStatus{Phase: corev1.PodRunning},
				},
			},
			existingRS: func() *appsv1.ReplicaSet {
				rsName, _ := naming.ReplicasetName(naming.DefaultClusterID, "max-idle-template")
				replicas := int32(5)
				return &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      rsName,
						Namespace: "default",
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: &replicas,
					},
				}
			}(),
			expectedShould:   false,
			expectedReplicas: 5,
		},
		{
			name: "scale based on active count ratio",
			template: &v1alpha1.SandboxTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ratio-template",
					Namespace: "default",
				},
				Spec: v1alpha1.SandboxTemplateSpec{
					Pool: v1alpha1.PoolStrategy{
						MinIdle: 1,
						MaxIdle: 50,
					},
				},
			},
			existingPods: func() []*corev1.Pod {
				var pods []*corev1.Pod
				for i := 0; i < 20; i++ {
					pods = append(pods, &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "active-" + string(rune('0'+i)),
							Namespace: "default",
							Labels: map[string]string{
								LabelTemplateID: "ratio-template",
								LabelPoolType:   PoolTypeActive,
							},
							Annotations: map[string]string{
								AnnotationClaimedAt: time.Now().Add(-1 * time.Minute).Format(time.RFC3339),
							},
						},
						Status: corev1.PodStatus{Phase: corev1.PodRunning},
					})
				}
				return pods
			}(),
			existingRS: func() *appsv1.ReplicaSet {
				rsName, _ := naming.ReplicasetName(naming.DefaultClusterID, "ratio-template")
				replicas := int32(2)
				return &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      rsName,
						Namespace: "default",
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: &replicas,
					},
				}
			}(),
			expectedShould:   true,
			expectedReplicas: 4,
		},
		{
			name: "no replicaset exists",
			template: &v1alpha1.SandboxTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-rs-template",
					Namespace: "default",
				},
				Spec: v1alpha1.SandboxTemplateSpec{
					Pool: v1alpha1.PoolStrategy{
						MinIdle: 2,
						MaxIdle: 20,
					},
				},
			},
			existingPods:   nil,
			existingRS:     nil, // No ReplicaSet
			expectedShould: false,
			skipIfNoRS:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup fake client and informers
			var objects []runtime.Object
			for _, pod := range tt.existingPods {
				objects = append(objects, pod)
			}
			if tt.existingRS != nil {
				objects = append(objects, tt.existingRS)
			}
			k8sClient := fake.NewSimpleClientset(objects...)

			// Create indexer and lister for pods
			podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			for _, pod := range tt.existingPods {
				require.NoError(t, podIndexer.Add(pod))
			}
			podLister := corelisters.NewPodLister(podIndexer)

			// Create indexer and lister for replicasets
			rsIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			if tt.existingRS != nil {
				require.NoError(t, rsIndexer.Add(tt.existingRS))
			}
			rsLister := appslisters.NewReplicaSetLister(rsIndexer)

			// Create sync scaler with fast rate limit for testing
			config := DefaultAutoScaleConfig()
			config.MinScaleInterval = 10 * time.Millisecond
			config.ScaleUpFactor = 1.5
			config.MaxScaleStep = 10

			scaler := NewAutoScalerWithConfig(
				k8sClient,
				podLister,
				rsLister,
				zap.NewNop(),
				config,
			)

			ctx := context.Background()

			if tt.testRateLimit {
				// First call should succeed
				decision1, err := scaler.OnColdClaim(ctx, tt.template)
				require.NoError(t, err)
				assert.True(t, decision1.ShouldScale, "First call should trigger scale")

				// Second immediate call should be rate limited
				decision2, err := scaler.OnColdClaim(ctx, tt.template)
				require.NoError(t, err)
				assert.False(t, decision2.ShouldScale, "Second call should be rate limited")
				return
			}

			// First call
			decision, err := scaler.OnColdClaim(ctx, tt.template)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedShould, decision.ShouldScale, "ShouldScale mismatch")

			if tt.expectedShould {
				assert.GreaterOrEqual(t, decision.Delta, tt.expectedDelta, "Delta should be at least expected")
			}
		})
	}
}

func TestAutoScaler_CalculateDesiredReplicas(t *testing.T) {
	scaler := &AutoScaler{
		config: AutoScaleConfig{
			ScaleUpFactor:   1.5,
			MaxScaleStep:    10,
			MinIdleBuffer:   2,
			TargetIdleRatio: 0.2,
		},
	}

	tests := []struct {
		name            string
		minIdle         int32
		maxIdle         int32
		currentReplicas int32
		idleCount       int32
		activeCount     int32
		expectedMin     int32
		expectedMax     int32
	}{
		{
			name:            "scale up from zero",
			minIdle:         2,
			maxIdle:         20,
			currentReplicas: 0,
			idleCount:       0,
			activeCount:     1,
			expectedMin:     2,
			expectedMax:     20,
		},
		{
			name:            "scale based on active count",
			minIdle:         1,
			maxIdle:         50,
			currentReplicas: 2,
			idleCount:       0,
			activeCount:     20,
			expectedMin:     4,
			expectedMax:     12,
		},
		{
			name:            "respect maxIdle",
			minIdle:         5,
			maxIdle:         5,
			currentReplicas: 5,
			idleCount:       0,
			activeCount:     100,
			expectedMin:     5,
			expectedMax:     5,
		},
		{
			name:            "scale with buffer",
			minIdle:         2,
			maxIdle:         20,
			currentReplicas: 2,
			idleCount:       0,
			activeCount:     0,
			expectedMin:     4,
			expectedMax:     20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			template := &v1alpha1.SandboxTemplate{
				Spec: v1alpha1.SandboxTemplateSpec{
					Pool: v1alpha1.PoolStrategy{
						MinIdle: tt.minIdle,
						MaxIdle: tt.maxIdle,
					},
				},
			}

			result := scaler.calculateDesiredReplicas(template, tt.currentReplicas, tt.idleCount, tt.activeCount)

			assert.GreaterOrEqual(t, result, tt.expectedMin, "Result should be >= expectedMin")
			assert.LessOrEqual(t, result, tt.expectedMax, "Result should be <= expectedMax")
		})
	}
}

func TestScaleRateLimiter(t *testing.T) {
	limiter := newScaleRateLimiter(100 * time.Millisecond)

	key := "test-template"

	// First call should succeed (acquire the lock)
	assert.True(t, limiter.TryAcquire(key), "First TryAcquire should succeed")

	// Immediate second call should be blocked (in progress)
	assert.False(t, limiter.TryAcquire(key), "Second TryAcquire while in progress should be blocked")

	// Complete the first operation
	limiter.Complete(key)

	// Immediate third call should still be blocked (interval not passed)
	assert.False(t, limiter.TryAcquire(key), "TryAcquire immediately after complete should be blocked by interval")

	// Wait for interval
	time.Sleep(110 * time.Millisecond)

	// After interval, should be allowed again
	assert.True(t, limiter.TryAcquire(key), "TryAcquire after interval should succeed")
}

func TestScaleRateLimiter_Concurrency(t *testing.T) {
	limiter := newScaleRateLimiter(10 * time.Millisecond) // Short interval for testing

	key := "concurrent-template"
	var successCount int32
	var wg sync.WaitGroup

	// Launch 100 concurrent goroutines trying to acquire the rate limiter
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

	// Only ONE goroutine should have succeeded
	assert.Equal(t, int32(1), successCount, "Only one goroutine should acquire the rate limiter")

	// Complete the operation
	limiter.Complete(key)

	// Wait for interval to pass
	time.Sleep(20 * time.Millisecond)

	// Now another should succeed
	assert.True(t, limiter.TryAcquire(key), "TryAcquire after complete+interval should succeed")
}

func TestAutoScaler_GetPoolStats(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stats-template",
			Namespace: "default",
		},
	}

	pods := []*corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "idle-running",
				Namespace: "default",
				Labels: map[string]string{
					LabelTemplateID: "stats-template",
					LabelPoolType:   PoolTypeIdle,
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "idle-not-ready",
				Namespace: "default",
				Labels: map[string]string{
					LabelTemplateID: "stats-template",
					LabelPoolType:   PoolTypeIdle,
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "idle-failed",
				Namespace: "default",
				Labels: map[string]string{
					LabelTemplateID: "stats-template",
					LabelPoolType:   PoolTypeIdle,
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodFailed},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "active-running",
				Namespace: "default",
				Labels: map[string]string{
					LabelTemplateID: "stats-template",
					LabelPoolType:   PoolTypeActive,
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "active-pending",
				Namespace: "default",
				Labels: map[string]string{
					LabelTemplateID: "stats-template",
					LabelPoolType:   PoolTypeActive,
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		},
	}

	// Create indexer and lister
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, pod := range pods {
		require.NoError(t, podIndexer.Add(pod))
	}
	podLister := corelisters.NewPodLister(podIndexer)

	scaler := &AutoScaler{
		podLister: podLister,
		logger:    zap.NewNop(),
		config:    DefaultAutoScaleConfig(),
	}

	idle, active, err := scaler.getPoolStats(template)

	require.NoError(t, err)
	assert.Equal(t, int32(1), idle, "Idle count mismatch")
	assert.Equal(t, int32(1), active, "Active count mismatch")
}

func TestAutoScaler_GetLastClaimTime(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim-time-template",
			Namespace: "default",
		},
	}

	now := time.Now()
	pods := []*corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "active-1",
				Namespace: "default",
				Labels: map[string]string{
					LabelTemplateID: "claim-time-template",
					LabelPoolType:   PoolTypeActive,
				},
				Annotations: map[string]string{
					AnnotationClaimedAt: now.Add(-5 * time.Minute).Format(time.RFC3339),
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "active-2",
				Namespace: "default",
				Labels: map[string]string{
					LabelTemplateID: "claim-time-template",
					LabelPoolType:   PoolTypeActive,
				},
				Annotations: map[string]string{
					AnnotationClaimedAt: now.Add(-1 * time.Minute).Format(time.RFC3339),
				},
			},
		},
	}

	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, pod := range pods {
		require.NoError(t, podIndexer.Add(pod))
	}
	podLister := corelisters.NewPodLister(podIndexer)

	scaler := &AutoScaler{
		podLister: podLister,
		logger:    zap.NewNop(),
		config:    DefaultAutoScaleConfig(),
	}

	lastClaim := scaler.getLastClaimTime(template)

	expectedTime := now.Add(-1 * time.Minute)
	diff := lastClaim.Sub(expectedTime)
	assert.Less(t, diff.Abs(), time.Second, "Last claim time should be within 1 second of expected")
}
