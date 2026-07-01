package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
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

func (s *SandboxService) waitForPodNetworkIdentity(ctx context.Context, template, namespace, name string) (*corev1.Pod, error) {
	timeout := s.config.ProcdInitTimeout
	if timeout < defaultPodClaimReadyTimeout {
		timeout = defaultPodClaimReadyTimeout
	}

	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	waiter := s.ensurePodNetworkIdentityWaiter()
	tracker := newPodNetworkIdentityStageTracker(s, template)
	lastReason := "pod network identity is not ready"

	evaluate := func(source string) (*corev1.Pod, bool, error) {
		if s.podLister == nil {
			return nil, false, fmt.Errorf("pod lister is not configured")
		}
		pod, err := s.podLister.Pods(namespace).Get(name)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				lastReason = fmt.Sprintf("pod %s/%s is not visible", namespace, name)
				s.observePodNetworkIdentityCheck(source, "not_ready", lastReason)
				return nil, false, nil
			}
			reason := "get pod for network identity"
			s.observePodNetworkIdentityCheck(source, "error", reason)
			tracker.observeFailure("error", reason)
			return nil, false, fmt.Errorf("get pod for network identity: %w", err)
		}

		tracker.observePod(pod)
		ready, reason := isPodNetworkIdentityReady(pod)
		if ready {
			s.observePodNetworkIdentityCheck(source, "ready", "ready")
			return pod, true, nil
		}
		if reason != "" {
			lastReason = reason
		}
		if isTerminalPodNetworkIdentityReason(lastReason) {
			s.observePodNetworkIdentityCheck(source, "error", lastReason)
			tracker.observeFailure("error", lastReason)
			return nil, false, fmt.Errorf("pod %s/%s network identity not ready: %s", namespace, name, lastReason)
		}
		s.observePodNetworkIdentityCheck(source, "not_ready", lastReason)
		return nil, false, nil
	}

	if pod, ready, err := evaluate("cache"); err != nil || ready {
		return pod, err
	}

	events, unregister := waiter.register(namespace, name)
	defer unregister()

	// Recheck after registering to avoid missing an informer event that arrives
	// between the initial cache read and waiter registration.
	if pod, ready, err := evaluate("cache_recheck"); err != nil || ready {
		return pod, err
	}

	for {
		select {
		case <-readyCtx.Done():
			if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(ctxErr, context.DeadlineExceeded) {
				lastReason = ctxErr.Error()
				s.observePodNetworkIdentityCheck("context", "canceled", lastReason)
				tracker.observeFailure("canceled", lastReason)
				return nil, fmt.Errorf("pod %s/%s network identity wait canceled: %w", namespace, name, ctxErr)
			}
			s.observePodNetworkIdentityCheck("informer", "timeout", lastReason)
			tracker.observeFailure("timeout", lastReason)
			return nil, fmt.Errorf("pod %s/%s network identity not ready after %s: %s", namespace, name, timeout, lastReason)
		case event := <-events:
			if event.deleted {
				tracker.observePod(event.pod)
				lastReason = "pod is deleting"
				s.observePodNetworkIdentityCheck("informer", "error", lastReason)
				tracker.observeFailure("error", lastReason)
				return nil, fmt.Errorf("pod %s/%s network identity not ready: %s", namespace, name, lastReason)
			}
			pod, ready, err := evaluate("informer")
			if err != nil || ready {
				return pod, err
			}
		}
	}
}

func (s *SandboxService) observePodNetworkIdentityCheck(source, result, reason string) {
	if s == nil || s.metrics == nil || s.metrics.PodNetworkIdentityChecksTotal == nil {
		return
	}
	if source == "" {
		source = "unknown"
	}
	if result == "" {
		result = "unknown"
	}
	s.metrics.PodNetworkIdentityChecksTotal.WithLabelValues(source, result, podNetworkIdentityReasonLabel(reason)).Inc()
}

func (s *SandboxService) observePodNetworkIdentityStage(template, stage, status, reason string, started time.Time) {
	s.observePodNetworkIdentityStageDuration(template, stage, status, reason, time.Since(started))
}

func (s *SandboxService) observePodNetworkIdentityStageDuration(template, stage, status, reason string, duration time.Duration) {
	if s == nil || s.metrics == nil || s.metrics.PodNetworkIdentityStageDuration == nil {
		return
	}
	if template == "" {
		template = "unknown"
	}
	if stage == "" {
		stage = "unknown"
	}
	if status == "" {
		status = "unknown"
	}
	if duration < 0 {
		duration = 0
	}
	s.metrics.PodNetworkIdentityStageDuration.WithLabelValues(template, stage, status, podNetworkIdentityReasonLabel(reason)).Observe(duration.Seconds())
}

func podNetworkIdentityReasonLabel(reason string) string {
	reason = strings.ToLower(strings.TrimSpace(reason))
	switch {
	case reason == "" || reason == "ready":
		return "ready"
	case strings.Contains(reason, "not visible") || strings.Contains(reason, "not found"):
		return "not_found"
	case strings.Contains(reason, "node is not assigned"):
		return "node_unassigned"
	case strings.Contains(reason, "ip is not assigned"):
		return "ip_unassigned"
	case strings.Contains(reason, "phase is terminal"):
		return "terminal"
	case strings.Contains(reason, "deleting"):
		return "deleting"
	case strings.Contains(reason, "get pod"):
		return "get_error"
	case strings.Contains(reason, "context canceled"):
		return "context_canceled"
	case strings.Contains(reason, "deadline exceeded"):
		return "deadline"
	default:
		return "not_ready"
	}
}

type podNetworkIdentityStageTracker struct {
	service   *SandboxService
	template  string
	started   time.Time
	visibleAt time.Time
	nodeAt    time.Time
	podIPAt   time.Time
}

func newPodNetworkIdentityStageTracker(service *SandboxService, template string) *podNetworkIdentityStageTracker {
	return &podNetworkIdentityStageTracker{
		service:  service,
		template: template,
		started:  time.Now(),
	}
}

func (t *podNetworkIdentityStageTracker) observePod(pod *corev1.Pod) {
	if t == nil || pod == nil {
		return
	}
	now := time.Now()
	if t.visibleAt.IsZero() {
		t.service.observePodNetworkIdentityStageDuration(t.template, "cache_visible", "success", "ready", now.Sub(t.started))
		t.visibleAt = now
	}
	if strings.TrimSpace(pod.Spec.NodeName) != "" && t.nodeAt.IsZero() {
		t.service.observePodNetworkIdentityStageDuration(t.template, "node_assigned", "success", "ready", now.Sub(t.visibleAt))
		t.nodeAt = now
	}
	if strings.TrimSpace(pod.Spec.NodeName) != "" && strings.TrimSpace(pod.Status.PodIP) != "" && t.podIPAt.IsZero() {
		t.service.observePodNetworkIdentityStageDuration(t.template, "pod_ip_assigned", "success", "ready", now.Sub(t.nodeAt))
		t.podIPAt = now
	}
}

func (t *podNetworkIdentityStageTracker) observeFailure(status, reason string) {
	if t == nil || !t.podIPAt.IsZero() {
		return
	}
	now := time.Now()
	stage := "cache_visible"
	started := t.started
	if !t.visibleAt.IsZero() {
		stage = "node_assigned"
		started = t.visibleAt
	}
	if !t.nodeAt.IsZero() {
		stage = "pod_ip_assigned"
		started = t.nodeAt
	}
	t.service.observePodNetworkIdentityStageDuration(t.template, stage, status, reason, now.Sub(started))
}

func isTerminalPodNetworkIdentityReason(reason string) bool {
	label := podNetworkIdentityReasonLabel(reason)
	return label == "terminal" || label == "deleting"
}

func isPodNetworkIdentityReady(pod *corev1.Pod) (bool, string) {
	if pod == nil {
		return false, "pod is nil"
	}
	if pod.DeletionTimestamp != nil {
		return false, "pod is deleting"
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false, fmt.Sprintf("pod phase is terminal: %s", pod.Status.Phase)
	}
	if strings.TrimSpace(pod.Spec.NodeName) == "" {
		return false, "pod node is not assigned"
	}
	if strings.TrimSpace(pod.Status.PodIP) == "" {
		return false, "pod IP is not assigned"
	}
	return true, ""
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
	if err == nil && kind == sandboxprobe.KindReadiness && result != nil && result.Status == sandboxprobe.StatusPassed {
		if portalErr := s.ensurePodVolumePortalsPublished(ctx, ctldAddress, pod); portalErr != nil {
			failure := sandboxprobe.Failed(kind, "VolumePortalsNotReady", portalErr.Error(), nil)
			return &failure, nil
		}
	}
	if result != nil && result.Status != "" {
		return result, nil
	}
	return result, err
}

func (s *SandboxService) ensurePodVolumePortalsPublished(ctx context.Context, ctldAddress string, pod *corev1.Pod) error {
	if s == nil || s.ctldClient == nil || pod == nil {
		return nil
	}
	portals := expectedVolumePortalsForPod(pod)
	if len(portals) == 0 {
		return nil
	}
	podUID := strings.TrimSpace(string(pod.UID))
	if podUID == "" {
		return fmt.Errorf("pod UID is not assigned")
	}
	resp, err := s.ctldClient.CheckVolumePortals(ctx, ctldAddress, ctldapi.CheckVolumePortalsRequest{
		PodUID:  podUID,
		Portals: portals,
	})
	if err != nil {
		return fmt.Errorf("check volume portals: %w", err)
	}
	if resp == nil {
		return fmt.Errorf("check volume portals returned no response")
	}
	if resp.Ready {
		return nil
	}
	if len(resp.Missing) == 0 {
		return fmt.Errorf("volume portals are not published")
	}
	return fmt.Errorf("volume portals are not published: %s", strings.Join(resp.Missing, ", "))
}

func expectedVolumePortalsForPod(pod *corev1.Pod) []ctldapi.VolumePortalRef {
	if pod == nil {
		return nil
	}
	portals := make([]ctldapi.VolumePortalRef, 0)
	seen := make(map[string]struct{})
	for _, volume := range pod.Spec.Volumes {
		if volume.CSI == nil || volume.CSI.Driver != volumeportal.DriverName {
			continue
		}
		attrs := volume.CSI.VolumeAttributes
		mountPath := strings.TrimSpace(attrs[volumeportal.AttributeMountPath])
		portalName := volumeportal.NormalizePortalName(attrs[volumeportal.AttributePortalName], mountPath)
		if portalName == "" {
			continue
		}
		if _, ok := seen[portalName]; ok {
			continue
		}
		seen[portalName] = struct{}{}
		portals = append(portals, ctldapi.VolumePortalRef{
			PortalName: portalName,
			MountPath:  mountPath,
		})
	}
	return portals
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

// podToSandboxStatus converts pod state to sandbox status.
func (s *SandboxService) podToSandboxStatus(pod *corev1.Pod) string {
	if pod == nil {
		return SandboxStatusStarting
	}
	if pod.DeletionTimestamp != nil {
		return SandboxStatusTerminating
	}
	return s.podPhaseToSandboxStatus(pod.Status.Phase)
}

// podPhaseToSandboxStatus converts pod phase to sandbox status
func (s *SandboxService) podPhaseToSandboxStatus(phase corev1.PodPhase) string {
	switch phase {
	case corev1.PodPending:
		return SandboxStatusStarting
	case corev1.PodRunning:
		return SandboxStatusRunning
	case corev1.PodSucceeded:
		return SandboxStatusFailed
	case corev1.PodFailed:
		return SandboxStatusFailed
	default:
		return SandboxStatusStarting
	}
}
