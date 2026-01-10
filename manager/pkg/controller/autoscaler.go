package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
)

const (
	annotationAutoscaleLastScaleTime = "sandbox0.ai/autoscale-last-scale-time"
	annotationAutoscaleLastColdTime  = "sandbox0.ai/autoscale-last-cold-time"
	annotationAutoscaleLastClaimTime = "sandbox0.ai/autoscale-last-claim-time"
)

type AutoScaler struct {
	k8sClient kubernetes.Interface
	podLister corelisters.PodLister
	logger    *zap.Logger
}

// NewAutoScaler creates a new AutoScaler.
func NewAutoScaler(k8sClient kubernetes.Interface, podLister corelisters.PodLister, logger *zap.Logger) *AutoScaler {
	return &AutoScaler{
		k8sClient: k8sClient,
		podLister: podLister,
		logger:    logger,
	}
}

type AutoScaleConfig struct {
	Window             time.Duration
	ScaleUpCooldown    time.Duration
	ScaleDownCooldown  time.Duration
	NoTrafficScaleDown time.Duration

	// TargetColdRate is the desired maximum ratio of cold claims in the window.
	// Example: 0.05 means "keep cold claims <= 5%".
	TargetColdRate float64

	// SlowStartThreshold is a TCP-inspired threshold for switching from exponential growth
	// to congestion-avoidance-like growth. When MinIdle < threshold and we see cold claims,
	// we will scale up aggressively (roughly doubling).
	SlowStartThreshold int32

	// ScaleUpAggressiveness controls how strongly we react to coldRate above target.
	// Larger means faster scale-up; keep conservative to avoid waste.
	ScaleUpAggressiveness float64

	// ScaleDownPercent is the max percent to scale down (of current minIdle) when conditions allow.
	ScaleDownPercent float64

	// MaxScaleUpPercent caps scale-up step as a percent of current minIdle per decision.
	MaxScaleUpPercent float64

	MinStep int32
	MaxStep int32
}

func defaultAutoScaleConfig() AutoScaleConfig {
	return AutoScaleConfig{
		Window:             2 * time.Minute,
		ScaleUpCooldown:    30 * time.Second,
		ScaleDownCooldown:  5 * time.Minute,
		NoTrafficScaleDown: 10 * time.Minute,

		TargetColdRate:        0.05, // 5%
		SlowStartThreshold:    4,
		ScaleUpAggressiveness: 1.2,
		ScaleDownPercent:      0.10, // 10% per decision (slow)
		MaxScaleUpPercent:     0.50, // up to +50% per decision (fast but bounded)

		MinStep: 1,
		MaxStep: 50,
	}
}

// ReconcileAutoScale computes desired ReplicaSet replicas for the template.
// Algorithm (pragmatic + safe):
// - Use recent claim events as demand signal, derived from active pods' annotations:
//   - AnnotationClaimedAt: RFC3339 timestamp
//   - AnnotationClaimType: "hot" or "cold"
//
// - If we see cold claims in the recent window, scale up quickly (TCP-inspired).
// - If there is sustained no traffic, scale down slowly.
// - Always clamp replicas within [MinIdle, MaxIdle] (user-provided bounds).
func (as *AutoScaler) ReconcileAutoScale(ctx context.Context, template *v1alpha1.SandboxTemplate, now time.Time) error {
	if template == nil {
		return fmt.Errorf("nil template")
	}
	if !template.Spec.Pool.AutoScale {
		return nil
	}
	cfg := defaultAutoScaleConfig()

	// Load ReplicaSet and use it as the actuator + cooldown state holder.
	rsName := v1alpha1.GenReplicasetName(template)
	rs, err := as.k8sClient.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rsName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// PoolManager will create it; we'll reconcile next cycle.
			return nil
		}
		return fmt.Errorf("get replicaset %s: %w", rsName, err)
	}

	currentReplicas := int32(0)
	if rs.Spec.Replicas != nil {
		currentReplicas = *rs.Spec.Replicas
	}

	// Gather recent claim stats from active pods.
	activePods, err := as.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeActive,
	}))
	if err != nil {
		return fmt.Errorf("list active pods: %w", err)
	}

	windowStart := now.Add(-cfg.Window)
	var claimsTotal int32
	var coldClaims int32
	var lastClaimTime time.Time
	var lastClaimTimeAny time.Time
	var lastColdTime time.Time

	for _, p := range activePods {
		if p == nil || p.Annotations == nil {
			continue
		}
		claimedAtStr, ok := p.Annotations[AnnotationClaimedAt]
		if !ok || claimedAtStr == "" {
			continue
		}
		claimedAt, err := time.Parse(time.RFC3339, claimedAtStr)
		if err != nil {
			continue
		}

		// Track latest claim time regardless of window for long no-traffic detection.
		if claimedAt.After(lastClaimTimeAny) {
			lastClaimTimeAny = claimedAt
		}

		if claimedAt.Before(windowStart) {
			continue
		}

		claimsTotal++
		if claimedAt.After(lastClaimTime) {
			lastClaimTime = claimedAt
		}

		if p.Annotations[AnnotationClaimType] == "cold" {
			coldClaims++
			if claimedAt.After(lastColdTime) {
				lastColdTime = claimedAt
			}
		}
	}

	// Cooldowns stored on ReplicaSet annotations (survive restarts; avoids mutating user template).
	lastScaleAt := parseRFC3339(rs.Annotations, annotationAutoscaleLastScaleTime)
	lastColdAtAnn := parseRFC3339(rs.Annotations, annotationAutoscaleLastColdTime)
	lastClaimAtAnn := parseRFC3339(rs.Annotations, annotationAutoscaleLastClaimTime)
	if lastColdTime.After(lastColdAtAnn) {
		lastColdAtAnn = lastColdTime
	}
	if lastClaimTimeAny.After(lastClaimAtAnn) {
		lastClaimAtAnn = lastClaimTimeAny
	}

	minIdle := template.Spec.Pool.MinIdle
	maxIdle := template.Spec.Pool.MaxIdle
	if maxIdle < 0 {
		maxIdle = 0
	}
	if minIdle < 0 {
		minIdle = 0
	}
	if maxIdle < minIdle {
		maxIdle = minIdle
	}

	desired := clamp32(currentReplicas, minIdle, maxIdle)

	// Scale up decision (TCP-inspired):
	// Treat cold claims as "loss" / insufficient warm capacity.
	// - When MinIdle is small (< SlowStartThreshold): slow-start-like exponential growth (double)
	// - Otherwise: congestion-avoidance-like growth using percent step based on how much coldRate exceeds target
	if coldClaims > 0 && now.Sub(lastScaleAt) >= cfg.ScaleUpCooldown {
		coldRate := float64(coldClaims) / float64(max32(1, claimsTotal))
		target := cfg.TargetColdRate
		if target <= 0 {
			target = 0.05
		}
		errRatio := (coldRate - target) / target // e.g. 1.0 means 2x the target
		if errRatio < 0 {
			errRatio = 0
		}

		// Base step: at least 1, and also react to absolute coldClaims (burst sensitivity).
		base := max32(cfg.MinStep, min32(cfg.MaxStep, coldClaims))

		cur := desired
		if cur < 0 {
			cur = 0
		}

		var step int32
		if cur < max32(1, cfg.SlowStartThreshold) {
			// Slow start: step ~= cur (doubling). If cur==0, still bump at least base.
			step = max32(base, max32(1, cur))
		} else {
			// Congestion avoidance: proportional to current size and error ratio.
			percentStep := int32(float64(max32(1, cur)) * cfg.ScaleUpAggressiveness * errRatio)

			// Cap by MaxScaleUpPercent.
			maxPercentStep := int32(float64(max32(1, cur)) * cfg.MaxScaleUpPercent)
			if maxPercentStep < cfg.MinStep {
				maxPercentStep = cfg.MinStep
			}
			step = clamp32(max32(base, percentStep), cfg.MinStep, min32(cfg.MaxStep, maxPercentStep))
		}

		desired = clamp32(desired+step, 0, maxIdle)
	}

	// Scale down decision: no recent traffic for a while -> slowly decrease.
	// We use persisted last-claim-time for long no-traffic windows.
	noTrafficDuration := time.Duration(1<<63 - 1) // effectively infinity
	if !lastClaimAtAnn.IsZero() {
		noTrafficDuration = now.Sub(lastClaimAtAnn)
	} else if !lastClaimTime.IsZero() {
		noTrafficDuration = now.Sub(lastClaimTime)
	} else if claimsTotal == 0 {
		// If we didn't observe a claim at all, treat as no traffic at least window size.
		noTrafficDuration = cfg.Window
	}
	if noTrafficDuration >= cfg.NoTrafficScaleDown && now.Sub(lastScaleAt) >= cfg.ScaleDownCooldown {
		if desired > 0 {
			step := int32(float64(desired) * cfg.ScaleDownPercent)
			step = clamp32(step, cfg.MinStep, cfg.MaxStep)
			desired = clamp32(desired-step, 0, maxIdle)
		}
	}

	desired = clamp32(desired, minIdle, maxIdle)
	needUpdate := desired != currentReplicas

	needPersist := !lastClaimAtAnn.IsZero() || !lastColdAtAnn.IsZero()
	// If no replica change and no timestamps to persist, do nothing.
	if !needUpdate && !needPersist {
		return nil
	}

	// Only write if replicas changed or we have annotation changes to persist.
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := as.k8sClient.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rsName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}

		current = current.DeepCopy()
		if current.Annotations == nil {
			current.Annotations = map[string]string{}
		}
		if !lastClaimAtAnn.IsZero() {
			current.Annotations[annotationAutoscaleLastClaimTime] = lastClaimAtAnn.UTC().Format(time.RFC3339)
		}
		if !lastColdAtAnn.IsZero() {
			current.Annotations[annotationAutoscaleLastColdTime] = lastColdAtAnn.UTC().Format(time.RFC3339)
		}
		if needUpdate {
			current.Annotations[annotationAutoscaleLastScaleTime] = now.UTC().Format(time.RFC3339)
			current.Spec.Replicas = ptr32(desired)
			as.logger.Info("Autoscale updated ReplicaSet replicas",
				zap.String("template", template.Name),
				zap.String("namespace", template.Namespace),
				zap.String("replicaset", rsName),
				zap.Int32("replicas", desired),
				zap.Int32("minIdle", minIdle),
				zap.Int32("maxIdle", maxIdle),
				zap.Int32("coldClaimsWindow", coldClaims),
				zap.Int32("claimsWindow", claimsTotal),
			)
		}

		_, err = as.k8sClient.AppsV1().ReplicaSets(current.Namespace).Update(ctx, current, metav1.UpdateOptions{})
		return err
	})
}

func parseRFC3339(ann map[string]string, key string) time.Time {
	if ann == nil {
		return time.Time{}
	}
	v := ann[key]
	if v == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}
	}
	return t
}

func clamp32(v, lo, hi int32) int32 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func max32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func ptr32(v int32) *int32 { return &v }
