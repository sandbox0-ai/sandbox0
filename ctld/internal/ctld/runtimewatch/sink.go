package runtimewatch

import (
	"context"
	"reflect"
	"strconv"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

type PodStatusSink struct {
	client kubernetes.Interface
}

func NewPodStatusSink(client kubernetes.Interface) *PodStatusSink {
	return &PodStatusSink{client: client}
}

func (s *PodStatusSink) Desired(ctx context.Context, pod *corev1.Pod, snapshot runtimecontrol.Snapshot) error {
	if s == nil || s.client == nil || pod == nil {
		return nil
	}
	var readiness sandboxprobe.Response
	switch snapshot.State {
	case runtimecontrol.DesiredStandby:
		readiness = sandboxprobe.Suspended(
			sandboxprobe.KindReadiness,
			"RuntimeStandbyPending",
			"runtime is waiting for the standby process to connect",
			nil,
		)
	case runtimecontrol.DesiredWaitingStorage:
		readiness = sandboxprobe.Suspended(
			sandboxprobe.KindReadiness,
			"RuntimeWaitingStorage",
			"runtime assignment is waiting for storage",
			nil,
		)
	case runtimecontrol.DesiredActive:
		readiness = sandboxprobe.Suspended(
			sandboxprobe.KindReadiness,
			"RuntimeActivating",
			"runtime assignment is being applied",
			nil,
		)
	case runtimecontrol.DesiredRevoked:
		readiness = sandboxprobe.Failed(
			sandboxprobe.KindReadiness,
			"RuntimeRevoked",
			"runtime assignment is revoked",
			nil,
		)
	default:
		return nil
	}
	_, err := controller.EnsureSandboxPodProbeConditions(ctx, s.client, pod, nil, &readiness, nil)
	return err
}

func (s *PodStatusSink) Observed(ctx context.Context, pod *corev1.Pod, observation runtimecontrol.Observation) error {
	if s == nil || s.client == nil || pod == nil {
		return nil
	}
	switch observation.State {
	case runtimecontrol.ObservedWaiting, runtimecontrol.ObservedLoading, runtimecontrol.ObservedRecovering:
		// Desired state already keeps the Pod unroutable. Avoid turning each
		// transient activation step into metadata and status API writes.
		return nil
	}
	updated, err := s.updateObservedAnnotations(
		ctx,
		pod,
		observation.State,
		observation.Revision,
		observation.RuntimeGeneration,
	)
	if err != nil {
		return err
	}

	startup := sandboxprobe.Passed(sandboxprobe.KindStartup, "RuntimeControlConnected", "runtime control stream is connected", nil)
	liveness := sandboxprobe.Passed(sandboxprobe.KindLiveness, "ProcdLive", "procd is live", nil)
	var readiness sandboxprobe.Response
	switch observation.State {
	case runtimecontrol.ObservedStandby:
		readiness = sandboxprobe.Passed(sandboxprobe.KindReadiness, "RuntimeStandby", "runtime is ready for assignment", nil)
	case runtimecontrol.ObservedWaiting:
		readiness = sandboxprobe.Suspended(sandboxprobe.KindReadiness, "RuntimeWaitingStorage", "runtime assignment is waiting for storage", nil)
	case runtimecontrol.ObservedLoading:
		readiness = sandboxprobe.Suspended(sandboxprobe.KindReadiness, "RuntimeLoading", "runtime state is loading", nil)
	case runtimecontrol.ObservedRecovering:
		readiness = sandboxprobe.Suspended(sandboxprobe.KindReadiness, "RuntimeRecovering", "runtime processes are recovering", nil)
	case runtimecontrol.ObservedReady:
		readiness = sandboxprobe.Passed(sandboxprobe.KindReadiness, "RuntimeReady", "runtime assignment is ready", nil)
	case runtimecontrol.ObservedFailed:
		readiness = sandboxprobe.Failed(sandboxprobe.KindReadiness, "RuntimeFailed", observation.Reason, nil)
	default:
		readiness = sandboxprobe.Failed(sandboxprobe.KindReadiness, "RuntimeObservationInvalid", "runtime observation is invalid", nil)
	}
	_, err = controller.EnsureSandboxPodProbeConditions(ctx, s.client, updated, &startup, &readiness, &liveness)
	return err
}

func (s *PodStatusSink) Disconnected(ctx context.Context, pod *corev1.Pod) error {
	if s == nil || s.client == nil || pod == nil {
		return nil
	}
	updated, err := s.updateObservedAnnotations(ctx, pod, runtimecontrol.ObservedDisconnected, "", 0)
	if err != nil {
		return err
	}
	startup := sandboxprobe.Suspended(sandboxprobe.KindStartup, "RuntimeControlDisconnected", "runtime control stream is disconnected", nil)
	readiness := sandboxprobe.Failed(sandboxprobe.KindReadiness, "RuntimeControlDisconnected", "runtime control stream is disconnected", nil)
	liveness := sandboxprobe.Suspended(sandboxprobe.KindLiveness, "RuntimeControlDisconnected", "runtime control stream is disconnected", nil)
	_, err = controller.EnsureSandboxPodProbeConditions(ctx, s.client, updated, &startup, &readiness, &liveness)
	return err
}

func (s *PodStatusSink) updateObservedAnnotations(
	ctx context.Context,
	pod *corev1.Pod,
	state runtimecontrol.ObservedState,
	revision string,
	generation int64,
) (*corev1.Pod, error) {
	var updated *corev1.Pod
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := s.client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		current = current.DeepCopy()
		before := current.DeepCopy()
		if current.Annotations == nil {
			current.Annotations = make(map[string]string)
		}
		current.Annotations[runtimecontrol.AnnotationObservedState] = string(state)
		if revision == "" {
			delete(current.Annotations, runtimecontrol.AnnotationObservedRevision)
		} else {
			current.Annotations[runtimecontrol.AnnotationObservedRevision] = revision
		}
		if generation <= 0 {
			delete(current.Annotations, runtimecontrol.AnnotationObservedGeneration)
		} else {
			current.Annotations[runtimecontrol.AnnotationObservedGeneration] = strconv.FormatInt(generation, 10)
		}
		if reflect.DeepEqual(current.Annotations, before.Annotations) {
			updated = current
			return nil
		}
		updated, err = s.client.CoreV1().Pods(current.Namespace).Update(ctx, current, metav1.UpdateOptions{})
		return err
	})
	return updated, err
}
