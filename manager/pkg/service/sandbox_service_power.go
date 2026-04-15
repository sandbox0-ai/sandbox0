package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
)

func sandboxPowerStateFromAnnotations(annotations map[string]string) SandboxPowerState {
	legacyObserved := SandboxPowerStateActive
	if len(annotations) > 0 && annotations[controller.AnnotationPaused] == "true" {
		legacyObserved = SandboxPowerStatePaused
	}

	state := SandboxPowerState{
		Desired:            normalizeSandboxPowerState(annotationValue(annotations, controller.AnnotationPowerStateDesired), legacyObserved),
		DesiredGeneration:  parseInt64Annotation(annotations, controller.AnnotationPowerStateDesiredGeneration),
		Observed:           normalizeSandboxPowerState(annotationValue(annotations, controller.AnnotationPowerStateObserved), legacyObserved),
		ObservedGeneration: parseInt64Annotation(annotations, controller.AnnotationPowerStateObservedGeneration),
	}
	state.Phase = normalizeSandboxPowerPhase(annotationValue(annotations, controller.AnnotationPowerStatePhase), state.Desired, state.Observed)
	return state
}

func normalizeSandboxPowerState(raw, fallback string) string {
	switch raw {
	case SandboxPowerStateActive, SandboxPowerStatePaused:
		return raw
	default:
		return fallback
	}
}

func normalizeSandboxPowerPhase(raw, desired, observed string) string {
	switch raw {
	case SandboxPowerPhaseStable, SandboxPowerPhasePausing, SandboxPowerPhaseResuming:
		return raw
	}
	if desired == observed {
		return SandboxPowerPhaseStable
	}
	if desired == SandboxPowerStatePaused {
		return SandboxPowerPhasePausing
	}
	if desired == SandboxPowerStateActive {
		return SandboxPowerPhaseResuming
	}
	return SandboxPowerPhaseStable
}

func parseInt64Annotation(annotations map[string]string, key string) int64 {
	raw := strings.TrimSpace(annotationValue(annotations, key))
	if raw == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

func annotationValue(annotations map[string]string, key string) string {
	if len(annotations) == 0 {
		return ""
	}
	return annotations[key]
}

func shouldReconcileSandboxPowerState(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	state := sandboxPowerStateFromAnnotations(pod.Annotations)
	return state.Phase != SandboxPowerPhaseStable || state.Desired != state.Observed
}

func sandboxPodMayHaveFrozenCgroup(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if pod.Annotations[controller.AnnotationPaused] == "true" {
		return true
	}
	state := sandboxPowerStateFromAnnotations(pod.Annotations)
	if state.Phase == SandboxPowerPhasePausing {
		return true
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Reason == "SandboxPaused" || strings.Contains(strings.ToLower(condition.Message), "cgroup is frozen") {
			return true
		}
	}
	return false
}

func hasExplicitSandboxPowerStateAnnotations(annotations map[string]string) bool {
	if len(annotations) == 0 {
		return false
	}
	return annotations[controller.AnnotationPowerStateDesired] != "" ||
		annotations[controller.AnnotationPowerStateDesiredGeneration] != "" ||
		annotations[controller.AnnotationPowerStateObserved] != "" ||
		annotations[controller.AnnotationPowerStateObservedGeneration] != "" ||
		annotations[controller.AnnotationPowerStatePhase] != ""
}

func nextSandboxPowerStateGeneration(current SandboxPowerState) int64 {
	base := current.DesiredGeneration
	if current.ObservedGeneration > base {
		base = current.ObservedGeneration
	}
	return base + 1
}

func requestedSandboxPowerState(annotations map[string]string, target string) SandboxPowerState {
	current := sandboxPowerStateFromAnnotations(annotations)
	generation := current.DesiredGeneration
	if generation == 0 || current.Desired != target {
		generation = nextSandboxPowerStateGeneration(current)
	}
	state := SandboxPowerState{
		Desired:            target,
		DesiredGeneration:  generation,
		Observed:           current.Observed,
		ObservedGeneration: current.ObservedGeneration,
	}
	if state.Observed == target {
		state.ObservedGeneration = generation
		state.Phase = SandboxPowerPhaseStable
		return state
	}
	state.Phase = normalizeSandboxPowerPhase("", target, state.Observed)
	return state
}

func completedSandboxPowerState(annotations map[string]string, target string) SandboxPowerState {
	current := sandboxPowerStateFromAnnotations(annotations)
	generation := current.DesiredGeneration
	if generation == 0 || current.Desired != target {
		generation = nextSandboxPowerStateGeneration(current)
	}
	return SandboxPowerState{
		Desired:            target,
		DesiredGeneration:  generation,
		Observed:           target,
		ObservedGeneration: generation,
		Phase:              SandboxPowerPhaseStable,
	}
}

func currentSandboxPowerExpectation(annotations map[string]string, target string) expectedSandboxPowerState {
	state := sandboxPowerStateFromAnnotations(annotations)
	return expectedSandboxPowerState{Desired: target, Generation: state.DesiredGeneration}
}

func staleSandboxPowerStateError(current SandboxPowerState) error {
	return fmt.Errorf("%w: desired=%s generation=%d", errSandboxPowerStateStale, current.Desired, current.DesiredGeneration)
}

func (s *SandboxService) matchSandboxPowerExpectation(pod *corev1.Pod, expected expectedSandboxPowerState) (SandboxPowerState, error) {
	if pod == nil {
		return SandboxPowerState{}, fmt.Errorf("pod is nil")
	}
	current := sandboxPowerStateFromAnnotations(pod.Annotations)
	if expected.Generation > 0 && (current.Desired != expected.Desired || current.DesiredGeneration != expected.Generation) {
		return current, staleSandboxPowerStateError(current)
	}
	return current, nil
}

func applySandboxPowerStateAnnotations(annotations map[string]string, state SandboxPowerState) {
	if annotations == nil {
		return
	}
	annotations[controller.AnnotationPowerStateDesired] = state.Desired
	annotations[controller.AnnotationPowerStateDesiredGeneration] = strconv.FormatInt(state.DesiredGeneration, 10)
	annotations[controller.AnnotationPowerStateObserved] = state.Observed
	annotations[controller.AnnotationPowerStateObservedGeneration] = strconv.FormatInt(state.ObservedGeneration, 10)
	annotations[controller.AnnotationPowerStatePhase] = state.Phase
}

func sandboxPowerStateEqual(a, b SandboxPowerState) bool {
	return a.Desired == b.Desired &&
		a.DesiredGeneration == b.DesiredGeneration &&
		a.Observed == b.Observed &&
		a.ObservedGeneration == b.ObservedGeneration &&
		a.Phase == b.Phase
}

func (s *SandboxService) sandboxPowerStateLock(sandboxID string) *sync.Mutex {
	if existing, ok := s.powerStateLocks.Load(sandboxID); ok {
		return existing.(*sync.Mutex)
	}
	mu := &sync.Mutex{}
	actual, _ := s.powerStateLocks.LoadOrStore(sandboxID, mu)
	return actual.(*sync.Mutex)
}

func (s *SandboxService) updateSandboxPowerStateAnnotations(ctx context.Context, pod *corev1.Pod, state SandboxPowerState) (*corev1.Pod, error) {
	if pod == nil {
		return nil, fmt.Errorf("pod is nil")
	}
	updated := pod.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	applySandboxPowerStateAnnotations(updated.Annotations, state)
	result, err := s.k8sClient.CoreV1().Pods(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *SandboxService) requestSandboxPowerState(ctx context.Context, sandboxID, target string) (SandboxPowerState, error) {
	lock := s.sandboxPowerStateLock(sandboxID)
	lock.Lock()
	defer lock.Unlock()

	var state SandboxPowerState
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := s.getSandboxPodForPowerState(ctx, sandboxID)
		if err != nil {
			return err
		}

		state = requestedSandboxPowerState(pod.Annotations, target)
		current := sandboxPowerStateFromAnnotations(pod.Annotations)
		if hasExplicitSandboxPowerStateAnnotations(pod.Annotations) && sandboxPowerStateEqual(current, state) {
			return nil
		}
		_, err = s.updateSandboxPowerStateAnnotations(ctx, pod, state)
		return err
	})
	if err != nil {
		return SandboxPowerState{}, fmt.Errorf("update power state annotations: %w", err)
	}

	if state.Phase != SandboxPowerPhaseStable {
		s.triggerSandboxPowerStateReconcile(sandboxID)
	}

	return state, nil
}

func (s *SandboxService) waitForSandboxPowerState(ctx context.Context, sandboxID, target string, generation int64) (SandboxPowerState, error) {
	var state SandboxPowerState
	err := wait.PollUntilContextCancel(ctx, defaultSandboxPowerPollInterval, true, func(ctx context.Context) (bool, error) {
		pod, err := s.getSandboxPodForPowerState(ctx, sandboxID)
		if err != nil {
			return false, err
		}
		state = sandboxPowerStateFromAnnotations(pod.Annotations)
		if generation > 0 && (state.Desired != target || state.DesiredGeneration != generation) {
			return false, fmt.Errorf("%w: %w", ErrSandboxPowerTransitionSuperseded, staleSandboxPowerStateError(state))
		}
		return state.Desired == target && state.Observed == target && state.Phase == SandboxPowerPhaseStable, nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return state, ctx.Err()
		}
		return state, err
	}
	return state, nil
}

func sandboxPowerTransitionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, defaultSandboxPowerTransitionTimeout)
}

func (s *SandboxService) triggerSandboxPowerStateReconcile(sandboxID string) {
	if _, loaded := s.powerStateReconcilers.LoadOrStore(sandboxID, struct{}{}); loaded {
		return
	}
	go func() {
		defer s.finishSandboxPowerStateReconcile(sandboxID)
		s.reconcileSandboxPowerState(sandboxID)
	}()
}

// StartPowerStateReconciler periodically reconciles power transitions left pending by another manager replica.
func (s *SandboxService) StartPowerStateReconciler(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	s.reconcilePendingSandboxPowerStates(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcilePendingSandboxPowerStates(ctx)
		}
	}
}

func (s *SandboxService) reconcilePendingSandboxPowerStates(ctx context.Context) {
	if ctx.Err() != nil || s.podLister == nil {
		return
	}
	pods, err := s.podLister.List(labels.Everything())
	if err != nil {
		s.logger.Warn("Failed to list sandboxes for power state reconcile", zap.Error(err))
		return
	}
	for _, pod := range pods {
		if !shouldReconcileSandboxPowerState(pod) {
			continue
		}
		sandboxID := strings.TrimSpace(pod.Labels[controller.LabelSandboxID])
		if sandboxID == "" {
			continue
		}
		s.triggerSandboxPowerStateReconcile(sandboxID)
	}
}

func (s *SandboxService) finishSandboxPowerStateReconcile(sandboxID string) {
	s.powerStateReconcilers.Delete(sandboxID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pod, err := s.getSandboxPodForPowerState(ctx, sandboxID)
	if err != nil {
		return
	}
	if shouldReconcileSandboxPowerState(pod) {
		s.triggerSandboxPowerStateReconcile(sandboxID)
	}
}

func (s *SandboxService) reconcileSandboxPowerState(sandboxID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for {
		pod, err := s.getSandboxPodForPowerState(ctx, sandboxID)
		if err != nil {
			s.logger.Warn("Power state reconcile failed to load sandbox",
				zap.String("sandboxID", sandboxID),
				zap.Error(err),
			)
			return
		}
		if pod.DeletionTimestamp != nil {
			return
		}

		state := sandboxPowerStateFromAnnotations(pod.Annotations)
		if state.Phase == SandboxPowerPhaseStable && state.Desired == state.Observed {
			return
		}

		s.logger.Info("Reconciling sandbox power state",
			zap.String("sandboxID", sandboxID),
			zap.String("desired", state.Desired),
			zap.Int64("desiredGeneration", state.DesiredGeneration),
			zap.String("observed", state.Observed),
			zap.Int64("observedGeneration", state.ObservedGeneration),
			zap.String("phase", state.Phase),
		)

		switch state.Desired {
		case SandboxPowerStatePaused:
			if _, err := s.PauseSandbox(ctx, sandboxID); err != nil {
				s.logger.Error("Pause reconcile failed",
					zap.String("sandboxID", sandboxID),
					zap.Error(err),
				)
				return
			}
		case SandboxPowerStateActive:
			if _, err := s.ResumeSandbox(ctx, sandboxID); err != nil {
				s.logger.Error("Resume reconcile failed",
					zap.String("sandboxID", sandboxID),
					zap.Error(err),
				)
				return
			}
		default:
			s.logger.Warn("Skipping power state reconcile with unsupported desired state",
				zap.String("sandboxID", sandboxID),
				zap.String("desired", state.Desired),
			)
			return
		}

		nextPod, err := s.getSandboxPodForPowerState(ctx, sandboxID)
		if err != nil {
			return
		}
		nextState := sandboxPowerStateFromAnnotations(nextPod.Annotations)
		if nextState.Phase == SandboxPowerPhaseStable && nextState.Desired == nextState.Observed {
			return
		}
		if nextState.Desired == state.Desired && nextState.DesiredGeneration == state.DesiredGeneration {
			return
		}
	}
}

func (s *SandboxService) getSandboxPodForPowerState(ctx context.Context, sandboxID string) (*corev1.Pod, error) {
	if s.k8sClient == nil {
		return s.getSandboxPod(ctx, sandboxID)
	}
	if s.sandboxIndex != nil {
		if namespace, ok := s.sandboxIndex.GetNamespace(sandboxID); ok {
			return s.k8sClient.CoreV1().Pods(namespace).Get(ctx, sandboxID, metav1.GetOptions{})
		}
	}
	selector := labels.SelectorFromSet(map[string]string{controller.LabelSandboxID: sandboxID}).String()
	pods, err := s.k8sClient.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "pod"}, sandboxID)
	}
	return pods.Items[0].DeepCopy(), nil
}
