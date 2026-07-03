package service

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/ingest"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

const defaultSandboxObservabilityLogTailLines int64 = 500

type SandboxLogProducerConfig struct {
	RegionID     string
	ClusterID    string
	PollInterval time.Duration
}

type SandboxLogProducer struct {
	k8sClient kubernetes.Interface
	podLister corelisters.PodLister
	worker    *ingest.LogWorker
	cfg       SandboxLogProducerConfig
	logger    *zap.Logger
	clock     TimeProvider

	mu   sync.Mutex
	seen map[string]time.Time
}

func NewSandboxLogProducer(k8sClient kubernetes.Interface, podLister corelisters.PodLister, worker *ingest.LogWorker, cfg SandboxLogProducerConfig, logger *zap.Logger, clock TimeProvider) *SandboxLogProducer {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 10 * time.Second
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if clock == nil {
		clock = systemTime{}
	}
	return &SandboxLogProducer{
		k8sClient: k8sClient,
		podLister: podLister,
		worker:    worker,
		cfg:       cfg,
		logger:    logger,
		clock:     clock,
		seen:      make(map[string]time.Time),
	}
}

func (p *SandboxLogProducer) Run(ctx context.Context) {
	if p == nil || p.k8sClient == nil || p.podLister == nil || p.worker == nil {
		return
	}
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()
	p.collect(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.collect(ctx)
		}
	}
}

func (p *SandboxLogProducer) collect(ctx context.Context) {
	pods, err := p.podLister.List(labels.Everything())
	if err != nil {
		p.logger.Warn("Failed to list sandbox pods for log observability", zap.Error(err))
		return
	}
	for _, pod := range pods {
		if !sandboxObservabilityPodEligible(pod) {
			continue
		}
		if err := p.collectPod(ctx, pod); err != nil {
			p.logger.Debug("Failed to collect sandbox process logs",
				zap.String("namespace", pod.Namespace),
				zap.String("pod", pod.Name),
				zap.Error(err),
			)
		}
	}
	p.pruneSeen(p.clock.Now().Add(-10 * p.cfg.PollInterval))
}

func (p *SandboxLogProducer) collectPod(ctx context.Context, pod *corev1.Pod) error {
	if pod == nil {
		return nil
	}
	sandboxID := strings.TrimSpace(sandboxIDFromPod(pod))
	teamID := strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID])
	if sandboxID == "" || teamID == "" {
		return nil
	}
	sinceSeconds := int64(p.cfg.PollInterval.Seconds()*2 + 1)
	if sinceSeconds < 1 {
		sinceSeconds = 1
	}
	tailLines := defaultSandboxObservabilityLogTailLines
	stream, err := p.k8sClient.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
		Container:    DefaultSandboxLogContainer,
		Timestamps:   true,
		SinceSeconds: &sinceSeconds,
		TailLines:    &tailLines,
	}).Stream(ctx)
	if err != nil {
		return fmt.Errorf("stream pod logs: %w", err)
	}
	defer stream.Close()
	return p.projectLogStream(ctx, pod, sandboxID, teamID, stream)
}

func (p *SandboxLogProducer) projectLogStream(ctx context.Context, pod *corev1.Pod, sandboxID, teamID string, reader io.Reader) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, processLogScannerInitialBuffer()), maxSandboxProcessLogScanBytes)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		event, occurredAt, ok := ParseSandboxProcessLogLine(scanner.Bytes())
		if !ok {
			continue
		}
		if occurredAt.IsZero() {
			occurredAt = p.clock.Now().UTC()
		}
		cursor := sandboxProcessLogCursor(pod, event, occurredAt, lineNo)
		if !p.markSeen(cursor, occurredAt) {
			continue
		}
		entry := sandboxobservability.LogEntry{
			TeamID:     teamID,
			SandboxID:  sandboxID,
			RegionID:   p.cfg.RegionID,
			ClusterID:  p.cfg.ClusterID,
			ContextID:  event.ProcessID,
			ProcessID:  event.ProcessID,
			OccurredAt: occurredAt,
			Stream:     sandboxobservability.LogStream(event.Source),
			Message:    event.Data,
			Cursor:     cursor,
			Attributes: map[string]any{
				"pod_name":     pod.Name,
				"namespace":    pod.Namespace,
				"process_type": event.ProcessType,
				"pid":          event.PID,
				"alias":        event.Alias,
				"truncated":    event.Truncated,
			},
		}
		if !p.worker.TryEnqueue(entry) {
			p.logger.Debug("Dropped sandbox process log observability entry",
				zap.String("sandbox_id", sandboxID),
				zap.String("cursor", cursor),
			)
		}
	}
	return scanner.Err()
}

func sandboxObservabilityPodEligible(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if pod.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		return false
	}
	if sandboxIDFromPod(pod) == "" {
		return false
	}
	return strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID]) != ""
}

func sandboxProcessLogCursor(pod *corev1.Pod, event SandboxProcessLogEvent, occurredAt time.Time, lineNo int) string {
	podUID := ""
	podName := ""
	if pod != nil {
		podUID = string(pod.UID)
		podName = pod.Name
	}
	if podUID == "" {
		podUID = podName
	}
	return fmt.Sprintf("procd-log:%s:%s:%s:%d:%d", podUID, event.ProcessID, event.Source, occurredAt.UnixNano(), lineNo)
}

func processLogScannerInitialBuffer() int {
	if process.DefaultContainerLogMaxLineBytes > 0 {
		return process.DefaultContainerLogMaxLineBytes
	}
	return 4096
}

func (p *SandboxLogProducer) markSeen(cursor string, occurredAt time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.seen[cursor]; ok {
		return false
	}
	p.seen[cursor] = occurredAt
	return true
}

func (p *SandboxLogProducer) pruneSeen(before time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for cursor, seenAt := range p.seen {
		if seenAt.Before(before) {
			delete(p.seen, cursor)
		}
	}
}
