package controller

import (
	"context"
	"fmt"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

const autoscalerDisabledReason = "template autoscaler disabled"

// AutoScaler is currently a compatibility shim.
//
// Template autoscaling is intentionally disabled. Burst cold claims already
// create active pods; expanding the idle pool in parallel compounds Kubernetes
// and runtime pressure, makes cold starts slower, and can leave unused warm pods
// after the burst ends. Until the replacement autoscaler has explicit
// backpressure semantics, PoolManager reconciles warm-pool ReplicaSets directly
// to spec.pool.minIdle.
type AutoScaler struct {
	config AutoScaleConfig
}

// AutoScaleConfig is preserved for manager config compatibility.
type AutoScaleConfig struct {
	MinScaleInterval        time.Duration
	ScaleUpFactor           float64
	MaxScaleStep            int32
	MinIdleBuffer           int32
	TargetIdleRatio         float64
	NoTrafficScaleDownAfter time.Duration
	ScaleDownPercent        float64
	MaxColdClaimInFlight    int32
}

// DefaultAutoScaleConfig returns the legacy autoscaler defaults. The values are
// still normalized so existing config can round-trip while the scaler is off.
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

// NewAutoScaler creates a disabled autoscaler compatibility shim.
func NewAutoScaler(
	_ kubernetes.Interface,
	_ corelisters.PodLister,
	logger *zap.Logger,
) *AutoScaler {
	return NewAutoScalerWithConfig(nil, nil, logger, DefaultAutoScaleConfig())
}

// NewAutoScalerWithConfig creates a disabled autoscaler compatibility shim.
func NewAutoScalerWithConfig(
	_ kubernetes.Interface,
	_ corelisters.PodLister,
	_ *zap.Logger,
	config AutoScaleConfig,
) *AutoScaler {
	return &AutoScaler{
		config: normalizeAutoScaleConfig(config),
	}
}

// SetMetrics preserves the legacy injection point. The disabled scaler does not
// emit autoscaler metrics.
func (s *AutoScaler) SetMetrics(_ *obsmetrics.ManagerMetrics) {}

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

// OnColdClaim preserves the legacy hook but never scales.
func (s *AutoScaler) OnColdClaim(_ context.Context, template *v1alpha1.SandboxTemplate) (*ScaleDecision, error) {
	if template == nil {
		return nil, fmt.Errorf("nil template")
	}
	return disabledScaleDecision(), nil
}

// OnHotClaim preserves the legacy hook but never scales.
func (s *AutoScaler) OnHotClaim(_ context.Context, template *v1alpha1.SandboxTemplate) (*ScaleDecision, error) {
	if template == nil {
		return nil, fmt.Errorf("nil template")
	}
	return disabledScaleDecision(), nil
}

// AdmitColdClaim preserves the legacy admission hook and always admits. Cold
// claim backpressure must live in a replacement design that does not couple
// admission to idle-pool scale-up writes.
func (s *AutoScaler) AdmitColdClaim(_ context.Context, template *v1alpha1.SandboxTemplate) (*ColdClaimAdmission, error) {
	if template == nil {
		return nil, fmt.Errorf("nil template")
	}
	return &ColdClaimAdmission{
		Admitted:      true,
		Reason:        autoscalerDisabledReason,
		ScaleDecision: disabledScaleDecision(),
	}, nil
}

// CompleteColdClaim preserves the legacy release hook. There are no tracked
// cold-claim slots while autoscaling is disabled.
func (s *AutoScaler) CompleteColdClaim(_ *v1alpha1.SandboxTemplate) {}

// ReconcileScaleDown is a no-op while PoolManager keeps ReplicaSet replicas at
// minIdle on every template reconcile.
func (s *AutoScaler) ReconcileScaleDown(_ context.Context, _ *v1alpha1.SandboxTemplate, _ time.Time) (time.Duration, error) {
	return 0, nil
}

func disabledScaleDecision() *ScaleDecision {
	return &ScaleDecision{
		ShouldScale: false,
		Reason:      autoscalerDisabledReason,
	}
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

// toAutoScaleConfig converts config.AutoscalerConfig to AutoScaleConfig without
// changing the external config surface.
func toAutoScaleConfig(cfg apiconfig.AutoscalerConfig) AutoScaleConfig {
	defaultCfg := DefaultAutoScaleConfig()
	minScaleInterval := cfg.MinScaleInterval.Duration
	if minScaleInterval <= 0 {
		minScaleInterval = defaultCfg.MinScaleInterval
	}
	maxScaleStep := cfg.MaxScaleStep
	if maxScaleStep <= 0 {
		maxScaleStep = defaultCfg.MaxScaleStep
	}
	minIdleBuffer := cfg.MinIdleBuffer
	if minIdleBuffer <= 0 {
		minIdleBuffer = defaultCfg.MinIdleBuffer
	}
	noTrafficScaleDownAfter := cfg.NoTrafficScaleDownAfter.Duration
	if noTrafficScaleDownAfter <= 0 {
		noTrafficScaleDownAfter = defaultCfg.NoTrafficScaleDownAfter
	}
	return AutoScaleConfig{
		MinScaleInterval:        minScaleInterval,
		ScaleUpFactor:           cfg.ParsedScaleUpFactor(defaultCfg.ScaleUpFactor),
		MaxScaleStep:            maxScaleStep,
		MinIdleBuffer:           minIdleBuffer,
		TargetIdleRatio:         cfg.ParsedTargetIdleRatio(defaultCfg.TargetIdleRatio),
		NoTrafficScaleDownAfter: noTrafficScaleDownAfter,
		ScaleDownPercent:        cfg.ParsedScaleDownPercent(defaultCfg.ScaleDownPercent),
	}
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
