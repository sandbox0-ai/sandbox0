package service

import (
	"context"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	corev1 "k8s.io/api/core/v1"
)

// completeHotClaimReservation durably records successful sandbox
// initialization. The reservation controller uses this record transition to
// detach the Pod, so the request path does not need a second Pod patch.
func (s *SandboxService) completeHotClaimReservation(
	ctx context.Context,
	pod *corev1.Pod,
	template *v1alpha1.SandboxTemplate,
	req *ClaimRequest,
) error {
	if s == nil || s.sandboxStore == nil || pod == nil || template == nil || req == nil ||
		!hotClaimUsesRecordCompletion(pod) {
		return nil
	}
	record := sandboxRecordForClaimedPod(s, pod, template, req)
	record.Status = SandboxStatusRunning
	return s.sandboxStore.UpsertSandbox(ctx, record)
}

func hotClaimUsesRecordCompletion(pod *corev1.Pod) bool {
	return pod != nil &&
		controller.IsHotClaimReservedPod(pod) &&
		pod.Annotations[controller.AnnotationHotClaimCompletionProtocol] ==
			controller.HotClaimCompletionProtocolRecordV1
}

func (s *SandboxService) enqueueHotClaimReservation(pod *corev1.Pod) {
	if s == nil || pod == nil || s.hotClaimReservationEnqueuer == nil ||
		!controller.IsHotClaimReservedPod(pod) {
		return
	}
	s.hotClaimReservationEnqueuer.EnqueueHotClaimReservation(pod.Namespace, pod.Name)
}
