package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

func (s *SandboxService) prodAddress(ctx context.Context, pod *corev1.Pod) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("pod is nil")
	}
	if podIP := strings.TrimSpace(pod.Status.PodIP); podIP != "" {
		return fmt.Sprintf("http://%s:%d", podIP, s.config.ProcdPort), nil
	}

	podIP, err := s.waitForPodIP(ctx, pod.Namespace, pod.Name)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("http://%s:%d", podIP, s.config.ProcdPort), nil
}

func (s *SandboxService) waitForPodIP(ctx context.Context, namespace, name string) (string, error) {
	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()

	for {
		pod, err := s.podLister.Pods(namespace).Get(name)
		if err != nil {
			return "", fmt.Errorf("get pod for ip: %w", err)
		}
		if podIP := strings.TrimSpace(pod.Status.PodIP); podIP != "" {
			return podIP, nil
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("pod ip not assigned")
		case <-ticker.C:
		}
	}
}

func (s *SandboxService) waitForPodClaimReady(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	timeout := s.config.ProcdInitTimeout
	if timeout < defaultPodClaimReadyTimeout {
		timeout = defaultPodClaimReadyTimeout
	}

	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	lastReason := "pod is not ready"
	for {
		pod, err := s.podLister.Pods(namespace).Get(name)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				lastReason = fmt.Sprintf("pod %s/%s is not visible", namespace, name)
				select {
				case <-readyCtx.Done():
					return nil, fmt.Errorf("pod %s/%s not claim-ready after %s: %s", namespace, name, timeout, lastReason)
				case <-ticker.C:
					continue
				}
			}
			return nil, fmt.Errorf("get pod for claim readiness: %w", err)
		}

		ready, reason := s.isPodClaimReady(readyCtx, pod)
		if ready {
			return pod, nil
		}
		if reason != "" {
			lastReason = reason
		}

		select {
		case <-readyCtx.Done():
			return nil, fmt.Errorf("pod %s/%s not claim-ready after %s: %s", namespace, name, timeout, lastReason)
		case <-ticker.C:
		}
	}
}

func (s *SandboxService) isPodClaimReady(ctx context.Context, pod *corev1.Pod) (bool, string) {
	if pod == nil {
		return false, "pod is nil"
	}
	if pod.Status.Phase != corev1.PodRunning {
		return false, fmt.Sprintf("pod phase is %s", pod.Status.Phase)
	}
	if strings.TrimSpace(pod.Status.PodIP) == "" {
		return false, "pod IP is not assigned"
	}
	if !podContainerRunning(pod, "procd") {
		return false, "procd container is not running"
	}
	if !controller.HasSandboxPodReadinessGate(pod) {
		return true, ""
	}

	result, err := s.ProbeSandboxPod(ctx, pod, sandboxprobe.KindReadiness)
	if err != nil {
		return false, err.Error()
	}
	if result == nil {
		return false, "sandbox readiness probe returned no result"
	}
	if result.Status != sandboxprobe.StatusPassed {
		message := strings.TrimSpace(result.Message)
		if message != "" {
			return false, message
		}
		if result.Reason != "" {
			return false, result.Reason
		}
		return false, fmt.Sprintf("sandbox readiness probe is %s", result.Status)
	}
	return true, ""
}

func podContainerRunning(pod *corev1.Pod, name string) bool {
	if pod == nil {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == name && status.State.Running != nil {
			return true
		}
	}
	return false
}

func (s *SandboxService) refreshSandboxProbeConditionsAsync(pod *corev1.Pod) {
	if s == nil || pod == nil || !controller.HasSandboxPodReadinessGate(pod) {
		return
	}
	go func(snapshot *corev1.Pod) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := s.refreshSandboxProbeConditions(ctx, snapshot); err != nil && s.logger != nil {
			s.logger.Warn("Failed to refresh sandbox probe conditions asynchronously",
				zap.String("pod", snapshot.Name),
				zap.String("namespace", snapshot.Namespace),
				zap.Error(err),
			)
		}
	}(pod.DeepCopy())
}

func (s *SandboxService) refreshSandboxProbeConditions(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	if !controller.HasSandboxPodReadinessGate(pod) {
		return pod, nil
	}
	startup := s.probeSandboxPodOrFailure(ctx, pod, sandboxprobe.KindStartup)
	readiness := s.probeSandboxPodOrFailure(ctx, pod, sandboxprobe.KindReadiness)
	liveness := s.probeSandboxPodOrFailure(ctx, pod, sandboxprobe.KindLiveness)
	return controller.EnsureSandboxPodProbeConditions(ctx, s.k8sClient, pod, startup, readiness, liveness)
}

func (s *SandboxService) ProbeSandboxPod(ctx context.Context, pod *corev1.Pod, kind sandboxprobe.Kind) (*sandboxprobe.Response, error) {
	if pod == nil {
		return nil, fmt.Errorf("pod is nil")
	}
	if pod.Status.Phase != corev1.PodRunning {
		result := sandboxprobe.Failed(kind, "PodNotRunning", fmt.Sprintf("pod phase is %s", pod.Status.Phase), nil)
		return &result, nil
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return nil, err
	}
	result, err := s.ctldClient.ProbePod(ctx, ctldAddress, pod.Namespace, pod.Name, kind)
	if result != nil && result.Status != "" {
		return result, nil
	}
	return result, err
}

func (s *SandboxService) probeSandboxPodOrFailure(ctx context.Context, pod *corev1.Pod, kind sandboxprobe.Kind) *sandboxprobe.Response {
	result, err := s.ProbeSandboxPod(ctx, pod, kind)
	if err != nil {
		failure := sandboxprobe.Failed(kind, "SandboxProbeFailed", err.Error(), nil)
		return &failure
	}
	if result == nil {
		failure := sandboxprobe.Failed(kind, "SandboxProbeMissing", "sandbox probe returned no result", nil)
		return &failure
	}
	return result
}

// podPhaseToSandboxStatus converts pod phase to sandbox status
func (s *SandboxService) podPhaseToSandboxStatus(phase corev1.PodPhase) string {
	switch phase {
	case corev1.PodPending:
		return SandboxStatusStarting
	case corev1.PodRunning:
		return SandboxStatusRunning
	case corev1.PodSucceeded:
		return SandboxStatusCompleted
	case corev1.PodFailed:
		return SandboxStatusFailed
	default:
		return SandboxStatusPending
	}
}
