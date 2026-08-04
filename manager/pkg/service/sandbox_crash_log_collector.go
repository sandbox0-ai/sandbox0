package service

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	defaultProcdCrashLogTailLines      int64 = 2048
	defaultProcdCrashLogReadLimitBytes int64 = 16 << 20
	defaultProcdCrashLogRetainBytes          = 512 << 10
	defaultProcdCrashLogChunkBytes           = 32 << 10
	defaultProcdCrashLogRequestTimeout       = 3 * time.Second
	maxProcdCrashLogCaptureRetries           = 4
)

type containerLogReader interface {
	ReadLogs(ctx context.Context, namespace, podName string, previous bool) ([]byte, error)
}

type kubernetesContainerLogReader struct {
	client kubernetes.Interface
}

func (r *kubernetesContainerLogReader) ReadLogs(ctx context.Context, namespace, podName string, previous bool) ([]byte, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("kubernetes client is not configured")
	}
	tailLines := defaultProcdCrashLogTailLines
	limitBytes := defaultProcdCrashLogReadLimitBytes
	stream, err := r.client.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container:  DefaultSandboxLogContainer,
		Previous:   previous,
		Timestamps: true,
		TailLines:  &tailLines,
		LimitBytes: &limitBytes,
	}).Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("stream procd container logs: %w", err)
	}
	defer stream.Close()

	logs, err := io.ReadAll(io.LimitReader(stream, limitBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read procd container logs: %w", err)
	}
	if int64(len(logs)) > limitBytes {
		return nil, fmt.Errorf("procd container logs exceeded %d bytes", limitBytes)
	}
	return logs, nil
}

type procdCrashLogItem struct {
	Namespace         string
	PodName           string
	PodUID            string
	SandboxID         string
	TeamID            string
	RuntimeGeneration int64
	RestartCount      int32
	ContainerID       string
	Previous          bool
	ExitCode          int32
	Signal            int32
	Reason            string
	Message           string
	StartedAt         time.Time
	FinishedAt        time.Time
}

func (i procdCrashLogItem) crashID() string {
	podIdentity := i.PodUID
	if podIdentity == "" {
		podIdentity = i.Namespace + "/" + i.PodName
	}
	if !i.Previous && strings.TrimSpace(i.ContainerID) != "" {
		return podIdentity + ":" + strings.TrimSpace(i.ContainerID)
	}
	return fmt.Sprintf("%s:%d", podIdentity, i.RestartCount)
}

// SandboxCrashLogCollector captures procd logs after an unexpected termination
// and emits them through the manager's platform logger.
type SandboxCrashLogCollector struct {
	reader  containerLogReader
	logger  *zap.Logger
	metrics *obsmetrics.ManagerMetrics
	queue   workqueue.TypedRateLimitingInterface[procdCrashLogItem]
}

// NewSandboxCrashLogCollector creates an event-driven collector for failed
// procd container instances.
func NewSandboxCrashLogCollector(k8sClient kubernetes.Interface, logger *zap.Logger, metrics *obsmetrics.ManagerMetrics) *SandboxCrashLogCollector {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxCrashLogCollector{
		reader:  &kubernetesContainerLogReader{client: k8sClient},
		logger:  logger,
		metrics: metrics,
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.NewTypedItemExponentialFailureRateLimiter[procdCrashLogItem](100*time.Millisecond, time.Second),
		),
	}
}

// ResourceEventHandler detects terminated procd attempts from shared Pod informer events.
func (c *SandboxCrashLogCollector) ResourceEventHandler() cache.ResourceEventHandlerFuncs {
	if c == nil {
		return cache.ResourceEventHandlerFuncs{}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			c.enqueueTermination(nil, sandboxPodFromInformerEvent(obj))
		},
		UpdateFunc: func(oldObj, newObj any) {
			c.enqueueTermination(sandboxPodFromInformerEvent(oldObj), sandboxPodFromInformerEvent(newObj))
		},
	}
}

// Run processes crash log capture work until the context is canceled.
func (c *SandboxCrashLogCollector) Run(ctx context.Context, workers int) error {
	if c == nil {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if c.queue == nil {
		c.queue = workqueue.NewTypedRateLimitingQueue(
			workqueue.NewTypedItemExponentialFailureRateLimiter[procdCrashLogItem](100*time.Millisecond, time.Second),
		)
	}

	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting sandbox crash log collector", zap.Int("workers", workers))
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	<-ctx.Done()
	c.logger.Info("Sandbox crash log collector stopped")
	return ctx.Err()
}

func (c *SandboxCrashLogCollector) enqueueTermination(oldPod, newPod *corev1.Pod) {
	if c == nil || c.queue == nil || !sandboxCrashLogPodEligible(newPod) {
		return
	}
	newStatus := procdContainerStatus(newPod)
	if newStatus == nil {
		return
	}

	if terminated := newStatus.State.Terminated; terminated != nil {
		if sameCurrentProcdTermination(oldPod, newPod, newStatus, terminated) {
			return
		}
		c.queue.Add(crashLogItem(newPod, newStatus, terminated, false))
		return
	}

	if newStatus.RestartCount == 0 || newStatus.LastTerminationState.Terminated == nil {
		return
	}
	oldRestartCount := int32(0)
	if oldPod != nil && oldPod.UID == newPod.UID {
		if oldStatus := procdContainerStatus(oldPod); oldStatus != nil {
			oldRestartCount = oldStatus.RestartCount
		}
	}
	if newStatus.RestartCount <= oldRestartCount {
		return
	}

	terminated := newStatus.LastTerminationState.Terminated
	if terminated.ExitCode == 0 && terminated.Signal == 0 {
		return
	}
	c.queue.Add(crashLogItem(newPod, newStatus, terminated, true))
}

func sameCurrentProcdTermination(
	oldPod, newPod *corev1.Pod,
	newStatus *corev1.ContainerStatus,
	newTerminated *corev1.ContainerStateTerminated,
) bool {
	if oldPod == nil || newPod == nil || oldPod.UID != newPod.UID {
		return false
	}
	oldStatus := procdContainerStatus(oldPod)
	if oldStatus == nil || oldStatus.State.Terminated == nil {
		return false
	}
	return oldStatus.ContainerID == newStatus.ContainerID &&
		oldStatus.State.Terminated.FinishedAt.Equal(&newTerminated.FinishedAt)
}

func crashLogItem(
	pod *corev1.Pod,
	status *corev1.ContainerStatus,
	terminated *corev1.ContainerStateTerminated,
	previous bool,
) procdCrashLogItem {
	return procdCrashLogItem{
		Namespace:         pod.Namespace,
		PodName:           pod.Name,
		PodUID:            string(pod.UID),
		SandboxID:         sandboxPodID(pod),
		TeamID:            strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID]),
		RuntimeGeneration: runtimeGenerationFromPod(pod),
		RestartCount:      status.RestartCount,
		ContainerID:       status.ContainerID,
		Previous:          previous,
		ExitCode:          terminated.ExitCode,
		Signal:            terminated.Signal,
		Reason:            terminated.Reason,
		Message:           terminated.Message,
		StartedAt:         terminated.StartedAt.Time,
		FinishedAt:        terminated.FinishedAt.Time,
	}
}

func sandboxCrashLogPodEligible(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || !controller.IsClaimedSandboxPod(pod) {
		return false
	}
	if strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID]) == "" {
		return false
	}
	return strings.TrimSpace(sandboxPodID(pod)) != ""
}

func procdContainerStatus(pod *corev1.Pod) *corev1.ContainerStatus {
	if pod == nil {
		return nil
	}
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == DefaultSandboxLogContainer {
			return &pod.Status.ContainerStatuses[i]
		}
	}
	return nil
}

func (c *SandboxCrashLogCollector) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *SandboxCrashLogCollector) processNextWorkItem(ctx context.Context) bool {
	item, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(item)

	err := c.captureLogs(ctx, item)
	if err == nil {
		c.queue.Forget(item)
		return true
	}
	if ctx.Err() != nil {
		c.queue.Forget(item)
		return true
	}
	if c.queue.NumRequeues(item) < maxProcdCrashLogCaptureRetries {
		c.logger.Debug("Procd crash log capture failed, retrying",
			append(procdCrashLogFields(item), zap.Error(err))...,
		)
		c.queue.AddRateLimited(item)
		return true
	}

	c.queue.Forget(item)
	c.observeCapture("error", 0)
	c.logger.Warn("Failed to capture terminated procd container logs",
		append(procdCrashLogFields(item), zap.Error(err))...,
	)
	return true
}

func (c *SandboxCrashLogCollector) captureLogs(ctx context.Context, item procdCrashLogItem) error {
	if c == nil || c.reader == nil {
		return fmt.Errorf("previous container log reader is not configured")
	}
	requestCtx, cancel := context.WithTimeout(ctx, defaultProcdCrashLogRequestTimeout)
	defer cancel()

	raw, err := c.reader.ReadLogs(requestCtx, item.Namespace, item.PodName, item.Previous)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return fmt.Errorf("terminated procd container logs were empty")
	}

	filtered, droppedProcessLines, err := filterProcdCrashLogs(raw)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(filtered)) == 0 {
		c.observeCapture("empty", 0)
		c.logger.Warn("Terminated procd container logs contained no internal log lines",
			append(procdCrashLogFields(item), zap.Int("dropped_process_log_lines", droppedProcessLines))...,
		)
		return nil
	}

	retained, truncated := retainProcdCrashLogTail(filtered, defaultProcdCrashLogRetainBytes)
	chunks := splitProcdCrashLogChunks(retained, defaultProcdCrashLogChunkBytes)
	fields := procdCrashLogFields(item)
	fields = append(fields,
		zap.Int("captured_log_bytes", len(retained)),
		zap.Bool("previous_log_truncated", truncated),
		zap.Int("dropped_process_log_lines", droppedProcessLines),
		zap.Int("chunk_count", len(chunks)),
		zap.Bool("untrusted_log_content", true),
	)
	for index, chunk := range chunks {
		chunkFields := append([]zap.Field{}, fields...)
		chunkFields = append(chunkFields,
			zap.Int("chunk_index", index),
			zap.String("log_chunk", chunk),
		)
		c.logger.Warn("Captured terminated procd container logs", chunkFields...)
	}
	c.observeCapture("success", len(retained))
	return nil
}

func filterProcdCrashLogs(raw []byte) ([]byte, int, error) {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64<<10), int(defaultProcdCrashLogReadLimitBytes))
	var filtered bytes.Buffer
	droppedProcessLines := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if _, _, isProcessLog := ParseSandboxProcessLogLine(line); isProcessLog {
			droppedProcessLines++
			continue
		}
		filtered.Write(line)
		filtered.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, fmt.Errorf("filter terminated procd container logs: %w", err)
	}
	return filtered.Bytes(), droppedProcessLines, nil
}

func retainProcdCrashLogTail(logs []byte, limit int) ([]byte, bool) {
	if limit <= 0 || len(logs) <= limit {
		return logs, false
	}
	logs = logs[len(logs)-limit:]
	if index := bytes.IndexByte(logs, '\n'); index >= 0 && index+1 < len(logs) {
		logs = logs[index+1:]
	}
	return logs, true
}

func splitProcdCrashLogChunks(logs []byte, limit int) []string {
	if len(logs) == 0 {
		return nil
	}
	if limit <= 0 {
		limit = len(logs)
	}
	chunks := make([]string, 0, (len(logs)+limit-1)/limit)
	for len(logs) > 0 {
		end := min(limit, len(logs))
		if end < len(logs) {
			if index := bytes.LastIndexByte(logs[:end], '\n'); index >= end/2 {
				end = index + 1
			}
		}
		chunks = append(chunks, strings.ToValidUTF8(string(logs[:end]), "?"))
		logs = logs[end:]
	}
	return chunks
}

func procdCrashLogFields(item procdCrashLogItem) []zap.Field {
	fields := []zap.Field{
		zap.String("event", "sandbox_procd_crash"),
		zap.String("crash_id", item.crashID()),
		zap.String("sandbox_id", item.SandboxID),
		zap.String("team_id", item.TeamID),
		zap.String("namespace", item.Namespace),
		zap.String("pod", item.PodName),
		zap.String("pod_uid", item.PodUID),
		zap.Int64("runtime_generation", item.RuntimeGeneration),
		zap.Int32("restart_count", item.RestartCount),
		zap.String("container_id", item.ContainerID),
		zap.Bool("previous_logs", item.Previous),
		zap.Int32("exit_code", item.ExitCode),
		zap.Int32("signal", item.Signal),
		zap.String("reason", item.Reason),
	}
	if item.Message != "" {
		fields = append(fields, zap.String("termination_message", item.Message))
	}
	if !item.StartedAt.IsZero() {
		fields = append(fields, zap.Time("container_started_at", item.StartedAt))
	}
	if !item.FinishedAt.IsZero() {
		fields = append(fields, zap.Time("container_finished_at", item.FinishedAt))
	}
	return fields
}

func (c *SandboxCrashLogCollector) observeCapture(result string, size int) {
	if c == nil || c.metrics == nil {
		return
	}
	if c.metrics.ProcdCrashLogCapturesTotal != nil {
		c.metrics.ProcdCrashLogCapturesTotal.WithLabelValues(result).Inc()
	}
	if result == "success" && c.metrics.ProcdCrashLogBytes != nil {
		c.metrics.ProcdCrashLogBytes.Observe(float64(size))
	}
}
