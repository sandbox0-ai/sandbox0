package service

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	managerconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/appservice"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

func (s *SandboxService) publishRuntimeAssignment(
	ctx context.Context,
	pod *corev1.Pod,
	resetCopiedSessionState bool,
) (*corev1.Pod, string, error) {
	if s == nil || s.k8sClient == nil || pod == nil {
		return pod, "", errors.New("runtime assignment dependencies are not configured")
	}
	var updated *corev1.Pod
	var revision string
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := s.k8sClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		current = current.DeepCopy()
		if current.Annotations == nil {
			current.Annotations = make(map[string]string)
		}
		appDomain := appservice.SandboxAppDomain(s.config.PublicRegionID, s.config.PublicRootDomain)
		if appDomain == "" {
			delete(current.Annotations, runtimecontrol.AnnotationAppDomain)
		} else {
			current.Annotations[runtimecontrol.AnnotationAppDomain] = appDomain
		}
		if resetCopiedSessionState {
			current.Annotations[runtimecontrol.AnnotationResetCopiedState] = "true"
		} else {
			delete(current.Annotations, runtimecontrol.AnnotationResetCopiedState)
		}
		delete(current.Annotations, runtimecontrol.AnnotationAssignmentReady)
		clearRuntimeObservationAnnotations(current.Annotations)
		_, revision, err = runtimecontrol.AssignmentFromPod(current)
		if err != nil {
			return err
		}
		if revision == "" {
			return errors.New("runtime assignment is missing")
		}
		current.Annotations[runtimecontrol.AnnotationAssignmentRevision] = revision
		updated, err = s.k8sClient.CoreV1().Pods(current.Namespace).Update(ctx, current, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return pod, "", fmt.Errorf("publish runtime assignment: %w", err)
	}
	return updated, revision, nil
}

func deactivateRuntimeAssignment(pod *corev1.Pod) (string, error) {
	if pod == nil {
		return "", errors.New("pod is required")
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	delete(pod.Annotations, runtimecontrol.AnnotationAssignmentReady)
	clearRuntimeObservationAnnotations(pod.Annotations)
	_, revision, err := runtimecontrol.AssignmentFromPod(pod)
	if err != nil {
		return "", err
	}
	if revision == "" {
		return "", errors.New("runtime assignment is missing")
	}
	pod.Annotations[runtimecontrol.AnnotationAssignmentRevision] = revision
	return revision, nil
}

func (s *SandboxService) activateRuntimeAssignment(
	ctx context.Context,
	pod *corev1.Pod,
	expectedRevision string,
) (*corev1.Pod, error) {
	if s == nil || s.k8sClient == nil || pod == nil {
		return pod, errors.New("runtime assignment dependencies are not configured")
	}
	expectedRevision = strings.TrimSpace(expectedRevision)
	if expectedRevision == "" {
		return pod, errors.New("runtime assignment revision is required")
	}

	if len(expectedVolumePortalsForPod(pod)) > 0 {
		ctldAddress, err := s.ctldAddressForPod(ctx, pod)
		if err != nil {
			return pod, fmt.Errorf("resolve CTLD address: %w", err)
		}
		if err := s.ensurePodVolumePortalsPublished(ctx, ctldAddress, pod); err != nil {
			return pod, err
		}
	}

	var updated *corev1.Pod
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := s.k8sClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		current = current.DeepCopy()
		assignment, revision, err := runtimecontrol.AssignmentFromPod(current)
		if err != nil {
			return err
		}
		if assignment == nil || revision != expectedRevision {
			return fmt.Errorf("runtime assignment changed before activation")
		}
		if strings.TrimSpace(current.Annotations[runtimecontrol.AnnotationAssignmentRevision]) != expectedRevision {
			return fmt.Errorf("runtime assignment was not published before activation")
		}
		if current.Annotations == nil {
			current.Annotations = make(map[string]string)
		}
		clearRuntimeObservationAnnotations(current.Annotations)
		current.Annotations[runtimecontrol.AnnotationAssignmentReady] = expectedRevision
		updated, err = s.k8sClient.CoreV1().Pods(current.Namespace).Update(ctx, current, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return pod, fmt.Errorf("activate runtime assignment: %w", err)
	}
	return s.waitForRuntimeAssignmentReady(ctx, updated.Namespace, updated.Name, expectedRevision)
}

func (s *SandboxService) waitForRuntimeAssignmentReady(
	ctx context.Context,
	namespace, name, expectedRevision string,
) (*corev1.Pod, error) {
	timeout := s.config.RuntimeReadyTimeout
	if timeout <= 0 {
		timeout = managerconfig.DefaultRuntimeReadyTimeout
	}
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	waiter := s.ensurePodEventWaiter()
	lastReason := "runtime observation is pending"
	evaluate := func() (*corev1.Pod, bool, error) {
		if s.podLister == nil {
			return nil, false, errors.New("pod lister is not configured")
		}
		pod, err := s.podLister.Pods(namespace).Get(name)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				lastReason = "pod is not visible"
				return nil, false, nil
			}
			return nil, false, err
		}
		ready, failed, reason := runtimeAssignmentObservation(pod, expectedRevision)
		if failed {
			return nil, false, fmt.Errorf("runtime assignment failed: %s", reason)
		}
		if ready {
			return pod, true, nil
		}
		if reason != "" {
			lastReason = reason
		}
		return nil, false, nil
	}
	if pod, ready, err := evaluate(); err != nil || ready {
		return pod, err
	}
	events, unregister := waiter.register(namespace, name)
	defer unregister()
	if pod, ready, err := evaluate(); err != nil || ready {
		return pod, err
	}
	for {
		select {
		case <-readyCtx.Done():
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("runtime assignment not ready after %s: %s", timeout, lastReason)
		case event := <-events:
			if event.deleted {
				return nil, errors.New("runtime pod is deleting")
			}
			pod, ready, err := evaluate()
			if err != nil || ready {
				return pod, err
			}
		}
	}
}

func clearRuntimeObservationAnnotations(annotations map[string]string) {
	delete(annotations, runtimecontrol.AnnotationObservedState)
	delete(annotations, runtimecontrol.AnnotationObservedRevision)
	delete(annotations, runtimecontrol.AnnotationObservedGeneration)
}

func runtimeAssignmentObservation(pod *corev1.Pod, expectedRevision string) (ready, failed bool, reason string) {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false, false, "pod is missing or deleting"
	}
	if pod.Status.Phase != corev1.PodRunning {
		return false, false, fmt.Sprintf("pod phase is %s", pod.Status.Phase)
	}
	assignment, revision, err := runtimecontrol.AssignmentFromPod(pod)
	if err != nil {
		return false, true, err.Error()
	}
	if assignment == nil {
		return false, false, "runtime assignment is missing"
	}
	if expectedRevision != "" && revision != expectedRevision {
		return false, false, "runtime assignment revision changed"
	}
	if strings.TrimSpace(pod.Annotations[runtimecontrol.AnnotationAssignmentRevision]) != revision {
		return false, false, "runtime assignment is not published"
	}
	if strings.TrimSpace(pod.Annotations[runtimecontrol.AnnotationAssignmentReady]) != revision {
		return false, false, "runtime assignment is waiting for storage"
	}
	observedState := runtimecontrol.ObservedState(strings.TrimSpace(pod.Annotations[runtimecontrol.AnnotationObservedState]))
	observedRevision := strings.TrimSpace(pod.Annotations[runtimecontrol.AnnotationObservedRevision])
	observedGeneration, _ := strconv.ParseInt(strings.TrimSpace(pod.Annotations[runtimecontrol.AnnotationObservedGeneration]), 10, 64)
	if observedRevision != revision || observedGeneration != assignment.RuntimeGeneration {
		return false, false, "runtime observation does not match the assignment"
	}
	if observedState == runtimecontrol.ObservedFailed {
		failed, reason := runtimeProbeFailure(pod)
		return false, failed, reason
	}
	if observedState != runtimecontrol.ObservedReady {
		return false, false, fmt.Sprintf("runtime observation is %s", observedState)
	}
	if !podConditionIsTrue(pod, v1alpha1.SandboxPodReadinessConditionType) {
		return false, false, "runtime readiness condition is not true"
	}
	return true, false, ""
}

func runtimeProbeFailure(pod *corev1.Pod) (bool, string) {
	if pod != nil {
		for i := range pod.Status.Conditions {
			condition := &pod.Status.Conditions[i]
			if condition.Type != v1alpha1.SandboxPodReadinessConditionType ||
				condition.Status != corev1.ConditionFalse ||
				condition.Reason != "RuntimeFailed" {
				continue
			}
			if message := strings.TrimSpace(condition.Message); message != "" {
				return true, message
			}
			return true, "runtime activation failed"
		}
	}
	return false, "runtime failure condition is pending"
}

func runtimeProbeConditionMessage(pod *corev1.Pod) string {
	if pod != nil {
		for i := range pod.Status.Conditions {
			condition := &pod.Status.Conditions[i]
			if condition.Type == v1alpha1.SandboxPodReadinessConditionType && strings.TrimSpace(condition.Message) != "" {
				return condition.Message
			}
		}
	}
	return "runtime activation failed"
}

func podConditionIsTrue(pod *corev1.Pod, conditionType corev1.PodConditionType) bool {
	if pod == nil {
		return false
	}
	for i := range pod.Status.Conditions {
		condition := &pod.Status.Conditions[i]
		if condition.Type == conditionType {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
