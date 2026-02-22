package controller

// AutoScaler Algorithm
//
// This autoscaler is a fast, event-driven approach that responds in milliseconds.
//
// # Architecture Overview
//
//	Cold Claim Request
//	        │
//	        ▼
//	┌───────────────────┐
//	│ Start goroutine   │  ← Non-blocking, returns immediately
//	└─────────┬─────────┘
//	          │
//	          ▼
//	┌───────────────────┐     Rate Limited
//	│ TryAcquire()      │─────────────────► return
//	└─────────┬─────────┘
//	          │ Acquired
//	          ▼
//	┌───────────────────┐
//	│ Calculate desired │
//	│ replicas          │
//	└─────────┬─────────┘
//	          │
//	          ▼
//	┌───────────────────┐
//	│ Update ReplicaSet │
//	└─────────┬─────────┘
//	          │
//	          ▼
//	┌───────────────────┐
//	│ Complete()        │  ← Release rate limiter
//	└───────────────────┘
//
// # Rate Limiter Design
//
// The rate limiter ensures safe concurrent scaling with two conditions:
//
//  1. In-Progress Check: Only ONE scale operation can run at a time
//     - Prevents thundering herd when multiple cold claims arrive simultaneously
//     - Uses atomic check-and-set via TryAcquire()
//
//  2. Interval Check: After a scale COMPLETES, wait minInterval before next scale
//     - Interval is measured from completion time, not start time
//     - Default: 100ms
//
// Timeline:
//
//	T0: TryAcquire() → true (inProgress=true)
//	T1: Scaling in progress...
//	    TryAcquire() → false (in progress)
//	T2: Complete() (inProgress=false, lastCompleteAt=T2)
//	    TryAcquire() → false (interval not passed)
//	T2+100ms: TryAcquire() → true
//
// # Scaling Strategy
//
// The desired replica count is calculated using multiple strategies:
//
//  1. MinIdle Guarantee: Ensure at least minIdle replicas
//
//  2. Active Ratio: Scale based on active pod count
//     desiredIdle = activeCount × TargetIdleRatio (default 0.2)
//
//  3. Scale Factor: On cold claim, scale up by factor (default 1.5x)
//     newReplicas = currentReplicas × ScaleUpFactor
//     Capped by MaxScaleStep (default 10)
//
//  4. Bounds: Always clamp to [minIdle, maxIdle]
//
// # Scale Down
//
// Scale down is handled asynchronously by the background reconcile loop:
// - Triggered after NoTrafficScaleDownAfter (default 10min) of no claims
// - Reduces replicas by ScaleDownPercent (default 10%)
// - Never goes below minIdle

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
)

// AutoScaler provides synchronous scaling decisions during cold claims.
// Unlike the previous async autoscaler with 30s cooldown, this scaler
// responds in milliseconds by making scaling decisions directly in the
// claim path.
type AutoScaler struct {
	k8sClient        kubernetes.Interface
	podLister        corelisters.PodLister
	replicaSetLister appslisters.ReplicaSetLister
	logger           *zap.Logger

	// Rate limiter to prevent over-scaling during concurrent cold claims
	rateLimiter *scaleRateLimiter

	// Configuration
	config AutoScaleConfig
}

// AutoScaleConfig holds configuration for the sync scaler.
type AutoScaleConfig struct {
	// MinScaleInterval is the minimum time between scale operations for a template.
	// This prevents thundering herd when multiple cold claims arrive simultaneously.
	// Default: 100ms (much faster than the previous 30s async cooldown)
	MinScaleInterval time.Duration

	// ScaleUpFactor determines how aggressively to scale up.
	// When cold claim occurs, newReplicas = current * ScaleUpFactor.
	// Default: 1.5 (50% increase per cold claim, capped by MaxScaleStep)
	ScaleUpFactor float64

	// MaxScaleStep caps the maximum pods to add in a single scale operation.
	// Default: 10
	MaxScaleStep int32

	// MinIdleBuffer is the minimum number of idle pods to maintain above minIdle.
	// When idle count drops to minIdle + MinIdleBuffer, proactive scaling kicks in.
	// Default: 2
	MinIdleBuffer int32

	// TargetIdleRatio is the target ratio of idle pods to active pods.
	// Formula: desiredIdle = active * TargetIdleRatio
	// Default: 0.2 (1 idle for every 5 active)
	TargetIdleRatio float64

	// NoTrafficScaleDownAfter is the duration without any claims before scaling down.
	// Scale down is still async and happens in the background reconcile loop.
	// Default: 10 minutes
	NoTrafficScaleDownAfter time.Duration

	// ScaleDownPercent is the percentage to reduce replicas during scale down.
	// Default: 10%
	ScaleDownPercent float64
}

// DefaultAutoScaleConfig returns the default configuration.
func DefaultAutoScaleConfig() AutoScaleConfig {
	return AutoScaleConfig{
		MinScaleInterval:        100 * time.Millisecond,
		ScaleUpFactor:           1.5,
		MaxScaleStep:            10,
		MinIdleBuffer:           2,
		TargetIdleRatio:         0.2,
		NoTrafficScaleDownAfter: 10 * time.Minute,
		ScaleDownPercent:        0.10,
	}
}

// NewAutoScaler creates a new AutoScaler.
func NewAutoScaler(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	replicaSetLister appslisters.ReplicaSetLister,
	logger *zap.Logger,
) *AutoScaler {
	return NewAutoScalerWithConfig(k8sClient, podLister, replicaSetLister, logger, DefaultAutoScaleConfig())
}

// NewAutoScalerWithConfig creates a new AutoScaler with custom configuration.
func NewAutoScalerWithConfig(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	replicaSetLister appslisters.ReplicaSetLister,
	logger *zap.Logger,
	config AutoScaleConfig,
) *AutoScaler {
	return &AutoScaler{
		k8sClient:        k8sClient,
		podLister:        podLister,
		replicaSetLister: replicaSetLister,
		logger:           logger,
		rateLimiter:      newScaleRateLimiter(config.MinScaleInterval),
		config:           config,
	}
}

// ScaleDecision represents the result of a scaling decision.
type ScaleDecision struct {
	ShouldScale bool   // Whether scaling was performed
	OldReplicas int32  // Previous replica count
	NewReplicas int32  // New replica count (same as OldReplicas if ShouldScale is false)
	Delta       int32  // Change in replicas (positive = scale up)
	Reason      string // Human-readable reason for the decision
}

// OnColdClaim is called when a cold claim occurs.
// It makes an immediate scaling decision to replenish the idle pool.
// This method is designed to be fast and can be called in a goroutine.
func (s *AutoScaler) OnColdClaim(ctx context.Context, template *v1alpha1.SandboxTemplate) (decision *ScaleDecision, err error) {
	if template == nil {
		return nil, fmt.Errorf("nil template")
	}

	templateKey := template.Namespace + "/" + template.Name

	// Atomic check-and-record: if rate limited, return immediately
	// This prevents race conditions when multiple goroutines call this method
	if !s.rateLimiter.TryAcquire(templateKey) {
		s.logger.Debug("Scale rate limited",
			zap.String("template", template.Name),
		)
		return &ScaleDecision{
			ShouldScale: false,
			Reason:      "rate limited",
		}, nil
	}

	// Always mark complete when done (success, failure, or no scale needed)
	// This ensures the rate limiter is released and interval starts from completion
	defer s.rateLimiter.Complete(templateKey)

	// Get current state
	idleCount, activeCount, err := s.getPoolStats(template)
	if err != nil {
		return nil, fmt.Errorf("get pool stats: %w", err)
	}

	currentReplicas, err := s.getCurrentReplicas(template)
	if err != nil {
		if errors.IsNotFound(err) {
			// ReplicaSet doesn't exist yet, PoolManager will create it
			return &ScaleDecision{
				ShouldScale: false,
				Reason:      "replicaset not found",
			}, nil
		}
		return nil, fmt.Errorf("get current replicas: %w", err)
	}

	// Calculate desired replicas
	desiredReplicas := s.calculateDesiredReplicas(template, currentReplicas, idleCount, activeCount)

	// Check if we need to scale
	if desiredReplicas <= currentReplicas {
		return &ScaleDecision{
			ShouldScale: false,
			OldReplicas: currentReplicas,
			NewReplicas: currentReplicas,
			Reason: fmt.Sprintf("no scale needed: current=%d, desired=%d, idle=%d, active=%d",
				currentReplicas, desiredReplicas, idleCount, activeCount),
		}, nil
	}

	// Execute the scale operation
	if err := s.executeScaleUp(ctx, template, currentReplicas, desiredReplicas); err != nil {
		return nil, fmt.Errorf("execute scale up: %w", err)
	}

	s.logger.Info("Auto scale up completed",
		zap.String("template", template.Name),
		zap.String("namespace", template.Namespace),
		zap.Int32("oldReplicas", currentReplicas),
		zap.Int32("newReplicas", desiredReplicas),
		zap.Int32("idle", idleCount),
		zap.Int32("active", activeCount),
	)

	return &ScaleDecision{
		ShouldScale: true,
		OldReplicas: currentReplicas,
		NewReplicas: desiredReplicas,
		Delta:       desiredReplicas - currentReplicas,
		Reason:      fmt.Sprintf("cold claim triggered scale: idle=%d, active=%d", idleCount, activeCount),
	}, nil
}

// calculateDesiredReplicas determines the target replica count.
func (s *AutoScaler) calculateDesiredReplicas(
	template *v1alpha1.SandboxTemplate,
	currentReplicas, _ /* idleCount */, activeCount int32,
) int32 {
	// Note: idleCount is tracked but not used in current strategy.
	// Future enhancements may use it for predictive scaling.
	cfg := s.config
	minIdle := template.Spec.Pool.MinIdle
	maxIdle := template.Spec.Pool.MaxIdle

	// Ensure maxIdle is at least minIdle
	if maxIdle < minIdle {
		maxIdle = minIdle
	}

	var desired int32

	// Strategy 1: Ensure at least minIdle
	if currentReplicas < minIdle {
		desired = minIdle
	} else {
		desired = currentReplicas
	}

	// Strategy 2: Scale based on active count (target idle ratio)
	// desiredIdle = activeCount * TargetIdleRatio
	// But we need at least minIdle + MinIdleBuffer to handle burst
	targetIdleFromActive := int32(float64(activeCount) * cfg.TargetIdleRatio)
	minRecommended := minIdle + cfg.MinIdleBuffer

	if targetIdleFromActive > minRecommended {
		minRecommended = targetIdleFromActive
	}

	// Strategy 3: Apply scale factor on cold claim
	// This ensures we over-provision slightly to handle burst traffic
	scaledDesired := int32(float64(desired) * cfg.ScaleUpFactor)
	if scaledDesired > desired {
		// Cap by MaxScaleStep
		delta := scaledDesired - desired
		if delta > cfg.MaxScaleStep {
			delta = cfg.MaxScaleStep
		}
		desired = desired + delta
	}

	// Ensure we have at least the minimum recommended
	if desired < minRecommended {
		desired = minRecommended
	}

	// Clamp to [minIdle, maxIdle]
	if desired < minIdle {
		desired = minIdle
	}
	if desired > maxIdle {
		desired = maxIdle
	}

	return desired
}

// getPoolStats returns the current idle and active pod counts.
func (s *AutoScaler) getPoolStats(template *v1alpha1.SandboxTemplate) (idle, active int32, err error) {
	// Count idle pods
	idlePods, err := s.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeIdle,
	}))
	if err != nil {
		return 0, 0, fmt.Errorf("list idle pods: %w", err)
	}

	for _, pod := range idlePods {
		if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
			idle++
		}
	}

	// Count active pods
	activePods, err := s.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeActive,
	}))
	if err != nil {
		return 0, 0, fmt.Errorf("list active pods: %w", err)
	}

	for _, pod := range activePods {
		if pod.Status.Phase == corev1.PodRunning {
			active++
		}
	}

	return idle, active, nil
}

// getCurrentReplicas returns the current replica count from the ReplicaSet.
func (s *AutoScaler) getCurrentReplicas(template *v1alpha1.SandboxTemplate) (int32, error) {
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	rsName, err := naming.ReplicasetName(clusterID, template.Name)
	if err != nil {
		return 0, fmt.Errorf("generate replicaset name: %w", err)
	}

	rs, err := s.replicaSetLister.ReplicaSets(template.Namespace).Get(rsName)
	if err != nil {
		return 0, err
	}

	if rs.Spec.Replicas == nil {
		return 0, nil
	}
	return *rs.Spec.Replicas, nil
}

// executeScaleUp updates the ReplicaSet with the new replica count.
func (s *AutoScaler) executeScaleUp(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	_ /* oldReplicas */, newReplicas int32,
) error {
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	rsName, err := naming.ReplicasetName(clusterID, template.Name)
	if err != nil {
		return fmt.Errorf("generate replicaset name: %w", err)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rs, err := s.replicaSetLister.ReplicaSets(template.Namespace).Get(rsName)
		if err != nil {
			if errors.IsNotFound(err) {
				return nil // PoolManager will create it
			}
			return err
		}

		rs = rs.DeepCopy()
		rs.Spec.Replicas = ptrToInt32(newReplicas)

		_, err = s.k8sClient.AppsV1().ReplicaSets(rs.Namespace).Update(ctx, rs, metav1.UpdateOptions{})
		return err
	})
}

// ReconcileScaleDown performs async scale-down for templates with no traffic.
// This is called from the background reconcile loop, not from the claim path.
func (s *AutoScaler) ReconcileScaleDown(ctx context.Context, template *v1alpha1.SandboxTemplate, now time.Time) error {
	if template == nil || !template.Spec.Pool.AutoScale {
		return nil
	}

	// Check if we should scale down based on last claim time
	lastClaimTime := s.getLastClaimTime(template)
	if lastClaimTime.IsZero() {
		return nil
	}

	timeSinceLastClaim := now.Sub(lastClaimTime)
	if timeSinceLastClaim < s.config.NoTrafficScaleDownAfter {
		return nil
	}

	currentReplicas, err := s.getCurrentReplicas(template)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	minIdle := template.Spec.Pool.MinIdle
	if currentReplicas <= minIdle {
		return nil // Already at minimum
	}

	// Calculate new replica count (reduce by ScaleDownPercent)
	delta := int32(float64(currentReplicas) * s.config.ScaleDownPercent)
	if delta < 1 {
		delta = 1
	}
	newReplicas := currentReplicas - delta
	if newReplicas < minIdle {
		newReplicas = minIdle
	}

	if newReplicas == currentReplicas {
		return nil
	}

	s.logger.Info("Scale down due to no traffic",
		zap.String("template", template.Name),
		zap.Int32("oldReplicas", currentReplicas),
		zap.Int32("newReplicas", newReplicas),
		zap.Duration("idle", timeSinceLastClaim),
	)

	return s.executeScaleUp(ctx, template, currentReplicas, newReplicas)
}

// getLastClaimTime returns the most recent claim time from active pods.
func (s *AutoScaler) getLastClaimTime(template *v1alpha1.SandboxTemplate) time.Time {
	activePods, err := s.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeActive,
	}))
	if err != nil {
		return time.Time{}
	}

	var lastClaim time.Time
	for _, pod := range activePods {
		if pod.Annotations == nil {
			continue
		}
		claimedAtStr := pod.Annotations[AnnotationClaimedAt]
		if claimedAtStr == "" {
			continue
		}
		claimedAt, err := time.Parse(time.RFC3339, claimedAtStr)
		if err != nil {
			continue
		}
		if claimedAt.After(lastClaim) {
			lastClaim = claimedAt
		}
	}

	return lastClaim
}

// Helper functions

func ptrToInt32(v int32) *int32 {
	return &v
}
