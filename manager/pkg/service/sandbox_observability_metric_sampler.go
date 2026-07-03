package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/ingest"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type SandboxRuntimeMetricSamplerConfig struct {
	RegionID  string
	ClusterID string
	Interval  time.Duration
}

type SandboxRuntimeMetricSampler struct {
	sandboxService *SandboxService
	podLister      corelisters.PodLister
	worker         *ingest.MetricWorker
	cfg            SandboxRuntimeMetricSamplerConfig
	logger         *zap.Logger
	clock          TimeProvider
}

func NewSandboxRuntimeMetricSampler(sandboxService *SandboxService, podLister corelisters.PodLister, worker *ingest.MetricWorker, cfg SandboxRuntimeMetricSamplerConfig, logger *zap.Logger, clock TimeProvider) *SandboxRuntimeMetricSampler {
	if cfg.Interval <= 0 {
		cfg.Interval = 15 * time.Second
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if clock == nil {
		clock = systemTime{}
	}
	return &SandboxRuntimeMetricSampler{
		sandboxService: sandboxService,
		podLister:      podLister,
		worker:         worker,
		cfg:            cfg,
		logger:         logger,
		clock:          clock,
	}
}

func (s *SandboxRuntimeMetricSampler) Run(ctx context.Context) {
	if s == nil || s.sandboxService == nil || s.podLister == nil || s.worker == nil {
		return
	}
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	s.sample(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sample(ctx)
		}
	}
}

func (s *SandboxRuntimeMetricSampler) sample(ctx context.Context) {
	pods, err := s.podLister.List(labels.Everything())
	if err != nil {
		s.logger.Warn("Failed to list sandbox pods for runtime metric sampling", zap.Error(err))
		return
	}
	for _, pod := range pods {
		if !sandboxObservabilityPodEligible(pod) {
			continue
		}
		s.samplePod(ctx, pod)
	}
}

func (s *SandboxRuntimeMetricSampler) samplePod(ctx context.Context, pod *corev1.Pod) {
	sandboxID := strings.TrimSpace(sandboxIDFromPod(pod))
	teamID := strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID])
	if sandboxID == "" || teamID == "" {
		return
	}
	usage, err := s.sandboxService.GetSandboxResourceUsage(ctx, sandboxID)
	if err != nil {
		s.logger.Debug("Failed to sample sandbox runtime metrics",
			zap.String("sandbox_id", sandboxID),
			zap.String("pod", pod.Name),
			zap.Error(err),
		)
		return
	}
	now := s.clock.Now().UTC()
	samples := BuildSandboxRuntimeMetricSamples(SandboxRuntimeMetricSampleInput{
		TeamID:     teamID,
		SandboxID:  sandboxID,
		RegionID:   s.cfg.RegionID,
		ClusterID:  s.cfg.ClusterID,
		PodName:    pod.Name,
		Namespace:  pod.Namespace,
		OccurredAt: now,
		Usage:      usage,
	})
	for _, sample := range samples {
		if !s.worker.TryEnqueue(sample) {
			s.logger.Debug("Dropped sandbox runtime metric sample",
				zap.String("sandbox_id", sandboxID),
				zap.String("name", sample.Name),
			)
		}
	}
}

type SandboxRuntimeMetricSampleInput struct {
	TeamID     string
	SandboxID  string
	RegionID   string
	ClusterID  string
	PodName    string
	Namespace  string
	OccurredAt time.Time
	Usage      *SandboxResourceUsage
}

func BuildSandboxRuntimeMetricSamples(input SandboxRuntimeMetricSampleInput) []sandboxobservability.MetricSample {
	if input.Usage == nil || input.TeamID == "" || input.SandboxID == "" || input.OccurredAt.IsZero() {
		return nil
	}
	attrs := map[string]any{
		"pod_name":  input.PodName,
		"namespace": input.Namespace,
		"source":    "procd",
	}
	samples := []sandboxobservability.MetricSample{}
	add := func(contextID, name, unit string, value float64, extra map[string]any) {
		if name == "" {
			return
		}
		attributes := cloneMetricAttributes(attrs)
		for key, val := range extra {
			attributes[key] = val
		}
		cursor := fmt.Sprintf("manager-runtime:%s:%s:%s:%d", input.SandboxID, contextID, name, input.OccurredAt.UnixNano())
		samples = append(samples, sandboxobservability.MetricSample{
			TeamID:     input.TeamID,
			SandboxID:  input.SandboxID,
			RegionID:   input.RegionID,
			ClusterID:  input.ClusterID,
			ContextID:  contextID,
			OccurredAt: input.OccurredAt,
			Name:       name,
			Unit:       unit,
			Value:      value,
			Cursor:     cursor,
			Attributes: attributes,
		})
	}

	usage := input.Usage
	add("", "container.memory.usage_bytes", "bytes", float64(usage.ContainerMemoryUsage), nil)
	add("", "container.memory.working_set_bytes", "bytes", float64(usage.ContainerMemoryWorkingSet), nil)
	add("", "container.memory.limit_bytes", "bytes", float64(usage.ContainerMemoryLimit), nil)
	add("", "process.memory.rss_bytes", "bytes", float64(usage.TotalMemoryRSS), nil)
	add("", "process.memory.vms_bytes", "bytes", float64(usage.TotalMemoryVMS), nil)
	add("", "process.open_files", "count", float64(usage.TotalOpenFiles), nil)
	add("", "process.threads", "count", float64(usage.TotalThreadCount), nil)
	add("", "process.io.read_bytes", "bytes", float64(usage.TotalIOReadBytes), nil)
	add("", "process.io.write_bytes", "bytes", float64(usage.TotalIOWriteBytes), nil)
	add("", "context.count", "count", float64(usage.ContextCount), nil)
	add("", "context.running_count", "count", float64(usage.RunningContextCount), nil)
	add("", "context.paused_count", "count", float64(usage.PausedContextCount), nil)

	for _, ctxUsage := range usage.Contexts {
		extra := map[string]any{
			"context_type": ctxUsage.Type,
			"language":     ctxUsage.Language,
			"running":      ctxUsage.Running,
			"paused":       ctxUsage.Paused,
		}
		contextID := ctxUsage.ContextID
		if ctxUsage.Usage.CPUPercent >= 0 {
			add(contextID, "process.cpu.percent", "percent", ctxUsage.Usage.CPUPercent, extra)
		}
		add(contextID, "process.memory.rss_bytes", "bytes", float64(ctxUsage.Usage.MemoryRSS), extra)
		add(contextID, "process.memory.vms_bytes", "bytes", float64(ctxUsage.Usage.MemoryVMS), extra)
		add(contextID, "process.open_files", "count", float64(ctxUsage.Usage.OpenFiles), extra)
		add(contextID, "process.threads", "count", float64(ctxUsage.Usage.ThreadCount), extra)
		add(contextID, "process.io.read_bytes", "bytes", float64(ctxUsage.Usage.IOReadBytes), extra)
		add(contextID, "process.io.write_bytes", "bytes", float64(ctxUsage.Usage.IOWriteBytes), extra)
	}
	return samples
}

func cloneMetricAttributes(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, val := range in {
		out[key] = val
	}
	return out
}
