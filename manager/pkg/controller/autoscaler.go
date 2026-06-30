package controller

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
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

// AutoScaler owns the idle pool replica target for a SandboxTemplate.
//
// The external contract is still template pool minIdle/maxIdle plus the
// OnColdClaim hook. Internally the scaler is deliberately small:
//   - claim events update the recent-traffic timestamp,
//   - hot/cold claims ask for a bounded idle-pool refill,
//   - cold claims are admitted only while the pod-network backlog is healthy,
//   - scale-down returns the pool target to minIdle after no traffic.
type AutoScaler struct {
	k8sClient kubernetes.Interface
	podLister corelisters.PodLister
	logger    *zap.Logger

	rateLimiter *scaleRateLimiter
	coldClaims  *coldClaimLimiter

	stateMu sync.Mutex
	state   map[string]*autoScalerTemplateState

	metrics *obsmetrics.ManagerMetrics
	config  AutoScaleConfig
}

// AutoScaleConfig holds configuration for pool scaling behavior.
type AutoScaleConfig struct {
	// MinScaleInterval is the minimum time between scale writes for a template.
	MinScaleInterval time.Duration

	// ScaleUpFactor adds headroom to observed idle-pool deficits.
	ScaleUpFactor float64

	// MaxScaleStep caps the maximum replicas added in one scale-up write.
	MaxScaleStep int32

	// MinIdleBuffer is the extra idle target used during urgent refills.
	MinIdleBuffer int32

	// TargetIdleRatio keeps idle capacity proportional to active sandboxes.
	TargetIdleRatio float64

	// NoTrafficScaleDownAfter is the idle period before returning to minIdle.
	NoTrafficScaleDownAfter time.Duration

	// ScaleDownPercent is kept for config compatibility. Scale-down now returns
	// directly to minIdle after the idle window.
	ScaleDownPercent float64

	// MaxColdClaimInFlight caps per-template cold pods that have not reached
	// network identity yet. Zero derives a cap from MaxScaleStep and pool bounds.
	MaxColdClaimInFlight int32
}

type autoScalerTemplateState struct {
	lastClaimAt time.Time
}

type autoScalerPoolStats struct {
	readyIdle       int32
	pendingIdle     int32
	runningActive   int32
	pendingActive   int32
	activeWithoutIP int32
}

func (s autoScalerPoolStats) activeTotal() int32 {
	return s.runningActive + s.pendingActive
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
	config = normalizeAutoScaleConfig(config)
	if logger == nil {
		logger = zap.NewNop()
	}
	return &AutoScaler{
		k8sClient:   k8sClient,
		podLister:   podLister,
		logger:      logger,
		rateLimiter: newScaleRateLimiter(config.MinScaleInterval),
		coldClaims:  newColdClaimLimiter(),
		state:       make(map[string]*autoScalerTemplateState),
		config:      config,
	}
}

// SetMetrics attaches manager metrics. Nil disables autoscaler metrics.
func (s *AutoScaler) SetMetrics(metrics *obsmetrics.ManagerMetrics) {
	s.metrics = metrics
}

// ScaleDecision represents the result of a scaling decision.
type ScaleDecision struct {
	ShouldScale bool
	OldReplicas int32
	NewReplicas int32
	Delta       int32
	Reason      string
}

// ColdClaimAdmission is the autoscaler's synchronous decision before the
// service creates a cold runtime pod.
type ColdClaimAdmission struct {
	Admitted       bool
	Reason         string
	RetryAfter     time.Duration
	InFlight       int32
	Limit          int32
	NetworkBacklog int32
	ScaleDecision  *ScaleDecision
}

// OnColdClaim preserves the existing autoscaler hook for cold claims.
func (s *AutoScaler) OnColdClaim(ctx context.Context, template *v1alpha1.SandboxTemplate) (*ScaleDecision, error) {
	s.recordClaim(template)
	return s.scaleForClaim(ctx, template, "cold")
}

// OnHotClaim is called after a hot claim removes one ready pod from the pool.
func (s *AutoScaler) OnHotClaim(ctx context.Context, template *v1alpha1.SandboxTemplate) (*ScaleDecision, error) {
	s.recordClaim(template)
	return s.scaleForClaim(ctx, template, "hot")
}

// AdmitColdClaim applies backpressure before the service creates a cold pod.
func (s *AutoScaler) AdmitColdClaim(ctx context.Context, template *v1alpha1.SandboxTemplate) (*ColdClaimAdmission, error) {
	if template == nil {
		return nil, fmt.Errorf("nil template")
	}
	s.recordClaim(template)

	stats, err := s.getPoolStatsDetailed(template)
	if err != nil {
		return nil, fmt.Errorf("get pool stats: %w", err)
	}
	templateName := autoscalerMetricTemplate(template)
	limit := s.maxColdClaimInFlight(template)
	s.observePool(templateName, stats, 0, 0)

	if stats.activeWithoutIP >= limit {
		s.observeDecision(templateName, "admit_cold_claim", "pod_network_identity_backlog", "rejected")
		return &ColdClaimAdmission{
			Admitted:       false,
			Reason:         "pod network identity backlog",
			RetryAfter:     time.Second,
			Limit:          limit,
			NetworkBacklog: stats.activeWithoutIP,
		}, nil
	}

	key := autoscalerTemplateKey(template)
	inFlight, ok := s.coldClaims.TryAcquire(key, limit)
	s.observeColdClaimsInFlight(templateName, inFlight)
	if !ok {
		s.observeDecision(templateName, "admit_cold_claim", "in_flight_limit", "rejected")
		return &ColdClaimAdmission{
			Admitted:       false,
			Reason:         "cold claim in-flight limit reached",
			RetryAfter:     time.Second,
			InFlight:       inFlight,
			Limit:          limit,
			NetworkBacklog: stats.activeWithoutIP,
		}, nil
	}

	decision, err := s.scaleForClaim(ctx, template, "cold")
	if err != nil {
		remaining := s.coldClaims.Release(key)
		s.observeColdClaimsInFlight(templateName, remaining)
		return nil, err
	}

	s.observeDecision(templateName, "admit_cold_claim", "admitted", "admitted")
	return &ColdClaimAdmission{
		Admitted:       true,
		Reason:         "admitted",
		InFlight:       inFlight,
		Limit:          limit,
		NetworkBacklog: stats.activeWithoutIP,
		ScaleDecision:  decision,
	}, nil
}

// CompleteColdClaim releases a cold admission slot.
func (s *AutoScaler) CompleteColdClaim(template *v1alpha1.SandboxTemplate) {
	if s == nil || template == nil || s.coldClaims == nil {
		return
	}
	remaining := s.coldClaims.Release(autoscalerTemplateKey(template))
	s.observeColdClaimsInFlight(autoscalerMetricTemplate(template), remaining)
}

func (s *AutoScaler) scaleForClaim(ctx context.Context, template *v1alpha1.SandboxTemplate, claimType string) (*ScaleDecision, error) {
	if template == nil {
		return nil, fmt.Errorf("nil template")
	}

	key := autoscalerTemplateKey(template)
	templateName := autoscalerMetricTemplate(template)
	if !s.rateLimiter.TryAcquire(key) {
		s.observeDecision(templateName, "scale_up", "rate_limited", "skipped")
		return &ScaleDecision{ShouldScale: false, Reason: "rate limited"}, nil
	}
	defer s.rateLimiter.Complete(key)

	stats, err := s.getPoolStatsDetailed(template)
	if err != nil {
		return nil, fmt.Errorf("get pool stats: %w", err)
	}

	currentReplicas, err := s.getCurrentReplicas(ctx, template)
	if err != nil {
		if errors.IsNotFound(err) {
			s.observeDecision(templateName, "scale_up", "replicaset_not_found", "skipped")
			return &ScaleDecision{ShouldScale: false, Reason: "replicaset not found"}, nil
		}
		return nil, fmt.Errorf("get current replicas: %w", err)
	}

	desiredReplicas, reason := s.calculateScaleUpTarget(template, currentReplicas, stats, claimType)
	s.observePool(templateName, stats, currentReplicas, desiredReplicas)
	if desiredReplicas <= currentReplicas {
		s.observeDecision(templateName, "scale_up", reason, "skipped")
		return &ScaleDecision{
			ShouldScale: false,
			OldReplicas: currentReplicas,
			NewReplicas: currentReplicas,
			Reason: fmt.Sprintf("no scale needed: current=%d desired=%d ready_idle=%d pending_idle=%d active=%d",
				currentReplicas, desiredReplicas, stats.readyIdle, stats.pendingIdle, stats.activeTotal()),
		}, nil
	}

	if err := s.executeScale(ctx, template, desiredReplicas); err != nil {
		return nil, fmt.Errorf("execute scale: %w", err)
	}

	s.logger.Info("Auto scale up completed",
		zap.String("template", template.Name),
		zap.String("namespace", template.Namespace),
		zap.Int32("oldReplicas", currentReplicas),
		zap.Int32("newReplicas", desiredReplicas),
		zap.Int32("readyIdle", stats.readyIdle),
		zap.Int32("pendingIdle", stats.pendingIdle),
		zap.Int32("active", stats.activeTotal()),
		zap.String("reason", reason),
	)
	s.observeDecision(templateName, "scale_up", reason, "scaled")
	s.observeScaleDelta(templateName, desiredReplicas-currentReplicas)

	return &ScaleDecision{
		ShouldScale: true,
		OldReplicas: currentReplicas,
		NewReplicas: desiredReplicas,
		Delta:       desiredReplicas - currentReplicas,
		Reason:      reason,
	}, nil
}

func (s *AutoScaler) calculateScaleUpTarget(
	template *v1alpha1.SandboxTemplate,
	currentReplicas int32,
	stats autoScalerPoolStats,
	claimType string,
) (int32, string) {
	minIdle, maxIdle := normalizedPoolBounds(template)
	desired := currentReplicas
	reason := "within_target"
	if desired < minIdle {
		desired = minIdle
		reason = "min_idle"
	}

	activeTarget := int32(math.Ceil(float64(stats.activeTotal()) * s.config.TargetIdleRatio))
	if activeTarget < minIdle {
		activeTarget = minIdle
	}
	if activeTarget > desired {
		desired = activeTarget
		reason = "active_ratio"
	}

	availableIdle := stats.readyIdle + stats.pendingIdle
	urgentIdleTarget := minIdle + s.config.MinIdleBuffer
	if urgentIdleTarget < minIdle {
		urgentIdleTarget = minIdle
	}
	switch claimType {
	case "cold":
		if availableIdle < urgentIdleTarget {
			deficit := urgentIdleTarget - availableIdle
			target := currentReplicas + s.scaleUpStep(deficit)
			if target > desired {
				desired = target
				reason = "cold_idle_deficit"
			}
		}
	case "hot":
		lowWatermark := minIdle / 2
		if lowWatermark < 1 && minIdle > 0 {
			lowWatermark = 1
		}
		if stats.readyIdle <= lowWatermark && availableIdle < urgentIdleTarget {
			deficit := urgentIdleTarget - availableIdle
			target := currentReplicas + s.scaleUpStep(deficit)
			if target > desired {
				desired = target
				reason = "hot_low_watermark"
			}
		}
	}

	if desired < minIdle {
		desired = minIdle
	}
	if desired > maxIdle {
		desired = maxIdle
	}
	return desired, reason
}

func (s *AutoScaler) scaleUpStep(deficit int32) int32 {
	if deficit < 1 {
		deficit = 1
	}
	factor := s.config.ScaleUpFactor
	if factor < 1 {
		factor = 1
	}
	step := int32(math.Ceil(float64(deficit) * factor))
	if step < 1 {
		step = 1
	}
	if s.config.MaxScaleStep > 0 && step > s.config.MaxScaleStep {
		step = s.config.MaxScaleStep
	}
	return step
}

func (s *AutoScaler) getPoolStatsDetailed(template *v1alpha1.SandboxTemplate) (autoScalerPoolStats, error) {
	var stats autoScalerPoolStats

	idlePods, err := s.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeIdle,
	}))
	if err != nil {
		return stats, fmt.Errorf("list idle pods: %w", err)
	}
	for _, pod := range idlePods {
		if pod.DeletionTimestamp != nil || isTerminalPodPhase(pod.Status.Phase) {
			continue
		}
		if IsPodReady(pod) {
			stats.readyIdle++
		} else {
			stats.pendingIdle++
		}
	}

	activePods, err := s.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeActive,
	}))
	if err != nil {
		return stats, fmt.Errorf("list active pods: %w", err)
	}
	for _, pod := range activePods {
		if pod.DeletionTimestamp != nil || isTerminalPodPhase(pod.Status.Phase) {
			continue
		}
		if pod.Status.PodIP == "" {
			stats.activeWithoutIP++
		}
		if pod.Status.Phase == corev1.PodRunning {
			stats.runningActive++
		} else {
			stats.pendingActive++
		}
	}

	return stats, nil
}

func isTerminalPodPhase(phase corev1.PodPhase) bool {
	return phase == corev1.PodSucceeded || phase == corev1.PodFailed
}

func (s *AutoScaler) getCurrentReplicas(ctx context.Context, template *v1alpha1.SandboxTemplate) (int32, error) {
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	rsName, err := naming.ReplicasetName(clusterID, template.Name)
	if err != nil {
		return 0, fmt.Errorf("generate replicaset name: %w", err)
	}

	rs, err := s.k8sClient.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rsName, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	if rs.Spec.Replicas == nil {
		return 0, nil
	}
	return *rs.Spec.Replicas, nil
}

func (s *AutoScaler) executeScale(ctx context.Context, template *v1alpha1.SandboxTemplate, replicas int32) error {
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	rsName, err := naming.ReplicasetName(clusterID, template.Name)
	if err != nil {
		return fmt.Errorf("generate replicaset name: %w", err)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rs, err := s.k8sClient.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rsName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}
		updated := rs.DeepCopy()
		updated.Spec.Replicas = ptrToInt32(replicas)
		_, err = s.k8sClient.AppsV1().ReplicaSets(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
}

// ReconcileScaleDown returns the idle pool target to minIdle after no traffic.
func (s *AutoScaler) ReconcileScaleDown(ctx context.Context, template *v1alpha1.SandboxTemplate, now time.Time) error {
	if template == nil {
		return nil
	}

	currentReplicas, err := s.getCurrentReplicas(ctx, template)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	minIdle, _ := normalizedPoolBounds(template)
	if currentReplicas <= minIdle {
		return nil
	}

	stats, err := s.getPoolStatsDetailed(template)
	if err != nil {
		return fmt.Errorf("get pool stats: %w", err)
	}
	templateName := autoscalerMetricTemplate(template)
	s.observePool(templateName, stats, currentReplicas, minIdle)
	if stats.activeTotal() > 0 {
		s.observeDecision(templateName, "scale_down", "active_pods", "skipped")
		return nil
	}

	lastClaimTime := s.lastClaimTime(template)
	if observed := s.getLastClaimTime(template); observed.After(lastClaimTime) {
		lastClaimTime = observed
	}
	if !lastClaimTime.IsZero() && now.Sub(lastClaimTime) < s.config.NoTrafficScaleDownAfter {
		s.observeDecision(templateName, "scale_down", "recent_claim", "skipped")
		return nil
	}

	s.logger.Info("Scale down due to no traffic",
		zap.String("template", template.Name),
		zap.Int32("oldReplicas", currentReplicas),
		zap.Int32("newReplicas", minIdle),
	)
	if err := s.executeScale(ctx, template, minIdle); err != nil {
		return err
	}
	s.observeDecision(templateName, "scale_down", "no_recent_claims", "scaled")
	s.observeScaleDelta(templateName, minIdle-currentReplicas)
	return nil
}

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

func normalizeAutoScaleConfig(cfg AutoScaleConfig) AutoScaleConfig {
	defaultCfg := DefaultAutoScaleConfig()
	if cfg.MinScaleInterval <= 0 {
		cfg.MinScaleInterval = defaultCfg.MinScaleInterval
	}
	if cfg.ScaleUpFactor <= 0 {
		cfg.ScaleUpFactor = defaultCfg.ScaleUpFactor
	}
	if cfg.MaxScaleStep <= 0 {
		cfg.MaxScaleStep = defaultCfg.MaxScaleStep
	}
	if cfg.MinIdleBuffer <= 0 {
		cfg.MinIdleBuffer = defaultCfg.MinIdleBuffer
	}
	if cfg.TargetIdleRatio <= 0 {
		cfg.TargetIdleRatio = defaultCfg.TargetIdleRatio
	}
	if cfg.NoTrafficScaleDownAfter <= 0 {
		cfg.NoTrafficScaleDownAfter = defaultCfg.NoTrafficScaleDownAfter
	}
	if cfg.ScaleDownPercent <= 0 {
		cfg.ScaleDownPercent = defaultCfg.ScaleDownPercent
	}
	return cfg
}

func normalizedPoolBounds(template *v1alpha1.SandboxTemplate) (minIdle, maxIdle int32) {
	if template == nil {
		return 0, 0
	}
	minIdle = template.Spec.Pool.MinIdle
	maxIdle = template.Spec.Pool.MaxIdle
	if minIdle < 0 {
		minIdle = 0
	}
	if maxIdle < minIdle {
		maxIdle = minIdle
	}
	return minIdle, maxIdle
}

func (s *AutoScaler) maxColdClaimInFlight(template *v1alpha1.SandboxTemplate) int32 {
	if s.config.MaxColdClaimInFlight > 0 {
		return s.config.MaxColdClaimInFlight
	}
	minIdle, maxIdle := normalizedPoolBounds(template)
	limit := s.config.MaxScaleStep * 3
	if limit < minIdle+s.config.MinIdleBuffer {
		limit = minIdle + s.config.MinIdleBuffer
	}
	if limit <= 0 {
		limit = 1
	}
	if maxIdle > 0 && limit > maxIdle {
		limit = maxIdle
	}
	return limit
}

func autoscalerTemplateKey(template *v1alpha1.SandboxTemplate) string {
	if template == nil {
		return ""
	}
	return template.Namespace + "/" + template.Name
}

func autoscalerMetricTemplate(template *v1alpha1.SandboxTemplate) string {
	if template == nil {
		return ""
	}
	return template.Name
}

func (s *AutoScaler) recordClaim(template *v1alpha1.SandboxTemplate) {
	if s == nil || template == nil {
		return
	}
	key := autoscalerTemplateKey(template)
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	state := s.state[key]
	if state == nil {
		state = &autoScalerTemplateState{}
		s.state[key] = state
	}
	state.lastClaimAt = time.Now()
}

func (s *AutoScaler) lastClaimTime(template *v1alpha1.SandboxTemplate) time.Time {
	if s == nil || template == nil {
		return time.Time{}
	}
	key := autoscalerTemplateKey(template)
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if state := s.state[key]; state != nil {
		return state.lastClaimAt
	}
	return time.Time{}
}

func (s *AutoScaler) observeDecision(template, action, reason, result string) {
	if s == nil || s.metrics == nil || s.metrics.AutoscalerDecisionsTotal == nil {
		return
	}
	s.metrics.AutoscalerDecisionsTotal.WithLabelValues(template, action, reason, result).Inc()
}

func (s *AutoScaler) observePool(template string, stats autoScalerPoolStats, currentReplicas, desiredReplicas int32) {
	if s == nil || s.metrics == nil {
		return
	}
	if s.metrics.AutoscalerPoolReplicas != nil {
		s.metrics.AutoscalerPoolReplicas.WithLabelValues(template, "current").Set(float64(currentReplicas))
		s.metrics.AutoscalerPoolReplicas.WithLabelValues(template, "desired").Set(float64(desiredReplicas))
	}
	if s.metrics.AutoscalerPoolPods != nil {
		s.metrics.AutoscalerPoolPods.WithLabelValues(template, "ready_idle").Set(float64(stats.readyIdle))
		s.metrics.AutoscalerPoolPods.WithLabelValues(template, "pending_idle").Set(float64(stats.pendingIdle))
		s.metrics.AutoscalerPoolPods.WithLabelValues(template, "active").Set(float64(stats.activeTotal()))
		s.metrics.AutoscalerPoolPods.WithLabelValues(template, "active_without_ip").Set(float64(stats.activeWithoutIP))
	}
}

func (s *AutoScaler) observeColdClaimsInFlight(template string, count int32) {
	if s == nil || s.metrics == nil || s.metrics.AutoscalerColdClaimsInFlight == nil {
		return
	}
	s.metrics.AutoscalerColdClaimsInFlight.WithLabelValues(template).Set(float64(count))
}

func (s *AutoScaler) observeScaleDelta(template string, delta int32) {
	if s == nil || s.metrics == nil || s.metrics.AutoscalerScaleDelta == nil || delta == 0 {
		return
	}
	direction := "up"
	if delta < 0 {
		direction = "down"
		delta = -delta
	}
	s.metrics.AutoscalerScaleDelta.WithLabelValues(template, direction).Observe(float64(delta))
}

func ptrToInt32(v int32) *int32 {
	return &v
}
