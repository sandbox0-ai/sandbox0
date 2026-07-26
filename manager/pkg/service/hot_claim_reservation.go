package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

var errHotClaimReservationLost = errors.New("hot claim reservation lost")

// markHotClaimReservationReady records that all sandbox-side initialization is
// complete. The detachment controller still verifies durable sandbox identity
// before removing the warm-pool owner.
func (s *SandboxService) markHotClaimReservationReady(
	ctx context.Context,
	pod *corev1.Pod,
) (*corev1.Pod, error) {
	if s == nil || pod == nil || !controller.IsHotClaimReservedPod(pod) {
		return pod, nil
	}
	expectedUID := pod.UID
	expectedToken := pod.Annotations[controller.AnnotationHotClaimReservation]
	current := pod
	var readyPod *corev1.Pod

	err := retry.OnError(retry.DefaultRetry, func(err error) bool {
		return k8serrors.IsConflict(err) || isClaimMetadataPatchPreconditionFailure(err)
	}, func() error {
		if current == nil {
			latest, err := s.k8sClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return fmt.Errorf("%w: pod %s/%s no longer exists", errHotClaimReservationLost, pod.Namespace, pod.Name)
				}
				return err
			}
			current = latest
		}
		if expectedUID != "" && current.UID != expectedUID {
			return fmt.Errorf("%w: pod %s/%s UID changed", errHotClaimReservationLost, pod.Namespace, pod.Name)
		}
		if current.Annotations[controller.AnnotationHotClaimReservation] != expectedToken {
			if current.Annotations[controller.AnnotationHotClaimReservation] == "" &&
				current.Labels[controller.LabelPoolType] == controller.PoolTypeActive &&
				sandboxIDFromPod(current) == sandboxIDFromPod(pod) {
				readyPod = current
				return nil
			}
			return fmt.Errorf("%w: pod %s/%s token changed", errHotClaimReservationLost, pod.Namespace, pod.Name)
		}
		switch current.Annotations[controller.AnnotationHotClaimReservationState] {
		case controller.HotClaimReservationStateReady:
			readyPod = current
			return nil
		case controller.HotClaimReservationStateInitializing:
		default:
			return fmt.Errorf("%w: pod %s/%s has invalid state", errHotClaimReservationLost, pod.Namespace, pod.Name)
		}

		operations := make([]claimMetadataPatchOperation, 0, 4)
		if current.UID != "" {
			operations = append(operations, claimMetadataPatchOperation{
				Operation: "test",
				Path:      "/metadata/uid",
				Value:     current.UID,
			})
		}
		// Pod status updates advance resourceVersion during initialization. UID
		// and the unique reservation token are the stable CAS inputs here.
		operations = append(operations,
			claimMetadataPatchOperation{
				Operation: "test",
				Path:      metadataMapPath("annotations", controller.AnnotationHotClaimReservation),
				Value:     expectedToken,
			},
			claimMetadataPatchOperation{
				Operation: "replace",
				Path:      metadataMapPath("annotations", controller.AnnotationHotClaimReservationState),
				Value:     controller.HotClaimReservationStateReady,
			},
		)
		patch, err := json.Marshal(operations)
		if err != nil {
			return fmt.Errorf("marshal hot claim ready patch: %w", err)
		}
		readyPod, err = s.k8sClient.CoreV1().Pods(current.Namespace).Patch(
			ctx,
			current.Name,
			types.JSONPatchType,
			patch,
			metav1.PatchOptions{},
		)
		if err != nil {
			current = nil
		}
		return err
	})
	if err != nil {
		if current != nil {
			return current, err
		}
		return pod, err
	}
	return readyPod, nil
}

func (s *SandboxService) enqueueHotClaimReservation(pod *corev1.Pod) {
	if s == nil || pod == nil || s.hotClaimReservationEnqueuer == nil ||
		!controller.IsHotClaimReservedPod(pod) {
		return
	}
	s.hotClaimReservationEnqueuer.EnqueueHotClaimReservation(pod.Namespace, pod.Name)
}
