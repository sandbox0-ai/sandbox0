// Package sandboxpod defines the Kubernetes Pod metadata and readiness
// contract shared by manager, ctld, and the embedded network runtime.
package sandboxpod

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	LabelTemplateID        = "sandbox0.ai/template-id"
	LabelTemplateLogicalID = "sandbox0.ai/template-logical-id"
	LabelTemplateScope     = "sandbox0.ai/template-scope"
	LabelPoolType          = "sandbox0.ai/pool-type"
	LabelSandboxID         = "sandbox0.ai/sandbox-id"
	LabelOwnerKind         = "sandbox0.ai/owner-kind"

	PoolTypeIdle   = "idle"
	PoolTypeActive = "active"

	AnnotationTeamID                       = "sandbox0.ai/team-id"
	AnnotationUserID                       = "sandbox0.ai/user-id"
	AnnotationClaimedAt                    = "sandbox0.ai/claimed-at"
	AnnotationClaimType                    = "sandbox0.ai/claim-type"
	AnnotationExpiresAt                    = "sandbox0.ai/expires-at"
	AnnotationHardExpiresAt                = "sandbox0.ai/hard-expires-at"
	AnnotationConfig                       = "sandbox0.ai/config"
	AnnotationMounts                       = "sandbox0.ai/mounts"
	AnnotationPaused                       = "sandbox0.ai/paused"
	AnnotationPausedAt                     = "sandbox0.ai/paused-at"
	AnnotationPausedState                  = "sandbox0.ai/paused-state"
	AnnotationPowerStateDesired            = "sandbox0.ai/power-state-desired"
	AnnotationPowerStateDesiredGeneration  = "sandbox0.ai/power-state-desired-generation"
	AnnotationPowerStateObserved           = "sandbox0.ai/power-state-observed"
	AnnotationPowerStateObservedGeneration = "sandbox0.ai/power-state-observed-generation"
	AnnotationPowerStatePhase              = "sandbox0.ai/power-state-phase"
	AnnotationNetworkPolicy                = "sandbox0.ai/network-policy"
	AnnotationNetworkPolicyHash            = "sandbox0.ai/network-policy-hash"
	AnnotationNetworkPolicyAppliedHash     = "sandbox0.ai/network-policy-applied-hash"
	AnnotationSandboxID                    = "sandbox0.ai/sandbox-id"
	AnnotationRuntimeGeneration            = "sandbox0.ai/runtime-generation"
	AnnotationRootFSSnapshotterInstance    = "sandbox0.ai/rootfs-snapshotter-instance"
	AnnotationWebhookStateVolumeID         = "sandbox0.ai/webhook-state-volume-id"
	AnnotationHotClaimReservation          = "sandbox0.ai/hot-claim-reservation"
	AnnotationHotClaimReservationState     = "sandbox0.ai/hot-claim-reservation-state"
	AnnotationHotClaimReservedAt           = "sandbox0.ai/hot-claim-reserved-at"
	AnnotationHotClaimReadyAt              = "sandbox0.ai/hot-claim-ready-at"
	AnnotationHotClaimCompletionProtocol   = "sandbox0.ai/hot-claim-completion-protocol"
	AnnotationTemplateSpecHash             = "sandbox0.ai/template-spec-hash"
	AnnotationTemplateTeamID               = "sandbox0.ai/template-team-id"
	AnnotationTemplateUserID               = "sandbox0.ai/template-user-id"
	AnnotationOwnerKind                    = "sandbox0.ai/owner-kind"
	AnnotationAppDomain                    = "sandbox0.ai/app-domain"
	AnnotationResetCopiedState             = "sandbox0.ai/runtime-reset-copied-state"
	AnnotationAssignmentRevision           = "sandbox0.ai/runtime-assignment-revision"
	AnnotationAssignmentReady              = "sandbox0.ai/runtime-assignment-ready"
	AnnotationObservedRevision             = "sandbox0.ai/runtime-observed-revision"
	AnnotationObservedGeneration           = "sandbox0.ai/runtime-observed-generation"
	AnnotationObservedState                = "sandbox0.ai/runtime-observed-state"

	OwnerKindTeamWarmPool = "team_warm_pool"

	HotClaimReservationStateInitializing = "initializing"
	HotClaimReservationStateReady        = "ready"
	HotClaimCompletionProtocolRecordV2   = "record-completion-v2"

	SandboxPodStartupConditionType   corev1.PodConditionType = "sandbox0.ai/startup"
	SandboxPodReadinessConditionType corev1.PodConditionType = "sandbox0.ai/ready"
	SandboxPodLivenessConditionType  corev1.PodConditionType = "sandbox0.ai/live"
)

// IsHotClaimReserved reports whether an idle warm-pool pod is reserved by a
// sandbox claim and must no longer be exposed as idle capacity.
func IsHotClaimReserved(pod *corev1.Pod) bool {
	return pod != nil && strings.TrimSpace(pod.Annotations[AnnotationHotClaimReservation]) != ""
}

// IsClaimed reports whether a pod is an active sandbox, including the short
// interval in which a hot claim still belongs to its warm pool.
func IsClaimed(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	return pod.Labels[LabelPoolType] == PoolTypeActive || IsHotClaimReserved(pod)
}
