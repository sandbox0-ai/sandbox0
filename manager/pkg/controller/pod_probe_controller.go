package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
)

const (
	sandboxProbeWorkers           = 8
	sandboxProbeRetryAfter        = 5 * time.Second
	sandboxProbeHealthyAfter      = 30 * time.Second
	sandboxProbeHealthyJitter     = 0.2
	sandboxProbeControllerTimeout = 10 * time.Second
)

type SandboxProbeRunner interface {
	ProbeSandboxPod(ctx context.Context, pod *corev1.Pod, kind sandboxprobe.Kind) (*sandboxprobe.Response, error)
}

func (op *Operator) runPodProbeWorker(ctx context.Context) {
	for op.processNextPodProbe(ctx) {
	}
}

func (op *Operator) processNextPodProbe(ctx context.Context) bool {
	key, shutdown := op.podProbeQueue.Get()
	if shutdown {
		return false
	}
	op.observeSandboxProbeQueueDepth()

	err := func() error {
		defer op.podProbeQueue.Done(key)

		requeueAfter, err := op.syncPodProbe(ctx, key)
		if err != nil {
			if op.podProbeQueue.NumRequeues(key) < maxRetries {
				op.podProbeQueue.AddRateLimited(key)
				op.observeSandboxProbeQueueDepth()
				return err
			}
			op.podProbeQueue.Forget(key)
			return fmt.Errorf("dropping sandbox pod probe %q after retries: %w", key, err)
		}

		op.podProbeQueue.Forget(key)
		if requeueAfter > 0 {
			op.podProbeQueue.AddAfter(key, requeueAfter)
			op.observeSandboxProbeQueueDepth()
		}
		return nil
	}()
	if err != nil {
		op.logger.Warn("Sandbox pod probe reconciliation failed", zap.String("key", key), zap.Error(err))
	}
	return true
}

func (op *Operator) syncPodProbe(ctx context.Context, key string) (time.Duration, error) {
	started := time.Now()
	result := "success"
	defer func() {
		if op.metrics == nil {
			return
		}
		op.metrics.SandboxProbeReconcileTotal.WithLabelValues(result).Inc()
		op.metrics.SandboxProbeReconcileDuration.WithLabelValues(result).Observe(time.Since(started).Seconds())
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		result = "error"
		return 0, fmt.Errorf("split sandbox pod probe key: %w", err)
	}
	pod, err := op.podLister.Pods(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return 0, nil
		}
		result = "error"
		return 0, fmt.Errorf("get sandbox pod for probe: %w", err)
	}
	if !isSandboxProbeCandidate(pod) {
		return 0, nil
	}

	probeCtx, cancel := context.WithTimeout(ctx, sandboxProbeControllerTimeout)
	defer cancel()
	updated, err := op.ensureSandboxProbeConditions(probeCtx, pod)
	if err != nil {
		result = "error"
		return 0, fmt.Errorf("ensure sandbox pod probe conditions for %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if shouldRetrySandboxProbe(updated) {
		return sandboxProbeRetryAfter, nil
	}
	return wait.Jitter(sandboxProbeHealthyAfter, sandboxProbeHealthyJitter), nil
}

func (op *Operator) ensureSandboxProbeConditions(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	if pod == nil {
		return nil, nil
	}
	if op.probeRunner == nil {
		return EnsureSandboxPodProbeConditions(ctx, op.k8sClient, pod,
			probeFailure(sandboxprobe.KindStartup, "SandboxProbeUnavailable", "sandbox probe runner is unavailable"),
			probeFailure(sandboxprobe.KindReadiness, "SandboxProbeUnavailable", "sandbox probe runner is unavailable"),
			probeFailure(sandboxprobe.KindLiveness, "SandboxProbeUnavailable", "sandbox probe runner is unavailable"),
		)
	}
	if pod.Status.Phase != corev1.PodRunning {
		message := fmt.Sprintf("pod phase is %s", pod.Status.Phase)
		return EnsureSandboxPodProbeConditions(ctx, op.k8sClient, pod,
			probeFailure(sandboxprobe.KindStartup, "PodNotRunning", message),
			probeFailure(sandboxprobe.KindReadiness, "PodNotRunning", message),
			probeFailure(sandboxprobe.KindLiveness, "PodNotRunning", message),
		)
	}

	startup := op.runSandboxProbe(ctx, pod, sandboxprobe.KindStartup)
	readiness := op.runSandboxProbe(ctx, pod, sandboxprobe.KindReadiness)
	liveness := op.runSandboxProbe(ctx, pod, sandboxprobe.KindLiveness)
	return EnsureSandboxPodProbeConditions(ctx, op.k8sClient, pod, startup, readiness, liveness)
}

func (op *Operator) runSandboxProbe(ctx context.Context, pod *corev1.Pod, kind sandboxprobe.Kind) *sandboxprobe.Response {
	started := time.Now()
	result, err := op.probeRunner.ProbeSandboxPod(ctx, pod, kind)
	resultLabel := "success"
	if err != nil {
		resultLabel = "error"
		result = probeFailure(kind, "SandboxProbeFailed", err.Error())
	} else if result == nil {
		resultLabel = "missing"
		result = probeFailure(kind, "SandboxProbeMissing", "sandbox probe returned no result")
	} else {
		resultLabel = string(result.Status)
	}
	if op.metrics != nil {
		op.metrics.SandboxProbeRequestsTotal.WithLabelValues(string(kind), resultLabel).Inc()
		op.metrics.SandboxProbeRequestDuration.WithLabelValues(string(kind), resultLabel).Observe(time.Since(started).Seconds())
	}
	return result
}

func probeFailure(kind sandboxprobe.Kind, reason, message string) *sandboxprobe.Response {
	result := sandboxprobe.Failed(kind, reason, message, nil)
	return &result
}

func isSandboxProbeCandidate(pod *corev1.Pod) bool {
	return pod != nil &&
		pod.DeletionTimestamp == nil &&
		pod.Labels[LabelTemplateID] != "" &&
		HasSandboxPodReadinessGate(pod)
}

func shouldRetrySandboxProbe(pod *corev1.Pod) bool {
	if !isSandboxProbeCandidate(pod) {
		return false
	}
	if pod.Status.Phase != corev1.PodRunning {
		return true
	}
	if !podConditionTrue(pod.Status.Conditions, v1alpha1.SandboxPodStartupConditionType) ||
		!podConditionTrue(pod.Status.Conditions, v1alpha1.SandboxPodReadinessConditionType) {
		return true
	}
	live := findPodCondition(pod.Status.Conditions, v1alpha1.SandboxPodLivenessConditionType)
	return live != nil && live.Status == corev1.ConditionFalse
}

func podProbeInputsChanged(oldPod, newPod *corev1.Pod) bool {
	if oldPod == nil || newPod == nil {
		return oldPod != newPod
	}
	return oldPod.UID != newPod.UID ||
		oldPod.DeletionTimestamp == nil && newPod.DeletionTimestamp != nil ||
		oldPod.Spec.NodeName != newPod.Spec.NodeName ||
		oldPod.Status.Phase != newPod.Status.Phase ||
		oldPod.Status.PodIP != newPod.Status.PodIP ||
		!reflect.DeepEqual(oldPod.Spec.ReadinessGates, newPod.Spec.ReadinessGates) ||
		!reflect.DeepEqual(oldPod.Status.ContainerStatuses, newPod.Status.ContainerStatuses)
}

func (op *Operator) enqueuePodProbe(pod *corev1.Pod) {
	if op == nil || op.podProbeQueue == nil || !isSandboxProbeCandidate(pod) {
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		return
	}
	op.podProbeQueue.Add(key)
	op.observeSandboxProbeQueueDepth()
}

func (op *Operator) observeSandboxProbeQueueDepth() {
	if op != nil && op.metrics != nil && op.podProbeQueue != nil {
		op.metrics.SandboxProbeQueueDepth.Set(float64(op.podProbeQueue.Len()))
	}
}
