package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const teamQuotaCleanupTimeout = 5 * time.Minute

// TeamQuotaRateLimitError reports a distributed token-bucket rejection.
type TeamQuotaRateLimitError struct {
	Key        teamquota.Key
	RetryAfter time.Duration
}

// TeamQuotaObservedRuntimeOverage reports that Kubernetes created or mutated a
// Pod above its admitted target. The observed physical target has already been
// committed before this error is returned.
type TeamQuotaObservedRuntimeOverage struct {
	Key      teamquota.Key
	Admitted int64
	Observed int64
}

func (e *TeamQuotaObservedRuntimeOverage) Error() string {
	if e == nil {
		return ErrQuotaExceeded.Error()
	}
	return fmt.Sprintf(
		"observed runtime exceeds admitted team quota target for %s: admitted=%d observed=%d",
		e.Key,
		e.Admitted,
		e.Observed,
	)
}

func (e *TeamQuotaObservedRuntimeOverage) Unwrap() error {
	return ErrQuotaExceeded
}

func isObservedRuntimeOverage(err error) bool {
	var overage *TeamQuotaObservedRuntimeOverage
	return errors.As(err, &overage)
}

func (e *TeamQuotaRateLimitError) Error() string {
	if e == nil {
		return ErrQuotaExceeded.Error()
	}
	return fmt.Sprintf("team quota rate exceeded for %s; retry after %s", e.Key, e.RetryAfter)
}

func (e *TeamQuotaRateLimitError) Unwrap() error {
	return ErrQuotaExceeded
}

// TeamQuotaRetryAfter extracts the server-provided retry delay.
func TeamQuotaRetryAfter(err error) time.Duration {
	var limited *TeamQuotaRateLimitError
	if errors.As(err, &limited) && limited != nil {
		return limited.RetryAfter
	}
	return 0
}

func (s *SandboxService) admitSandboxStartTeamQuota(ctx context.Context, teamID string) error {
	if s == nil || s.teamQuotaRateLimiter == nil {
		return fmt.Errorf("%w: sandbox-start rate limiter is not configured", ErrTeamQuotaUnavailable)
	}
	decision, err := s.teamQuotaRateLimiter.Take(
		ctx,
		strings.TrimSpace(teamID),
		teamquota.KeySandboxStarts,
		1,
	)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTeamQuotaUnavailable, err)
	}
	if decision.Allowed {
		return nil
	}
	retryAfter := decision.RetryAfter
	if retryAfter <= 0 {
		retryAfter = time.Second
	}
	return &TeamQuotaRateLimitError{
		Key:        teamquota.KeySandboxStarts,
		RetryAfter: retryAfter,
	}
}

// admitSandboxStartBeforePoolClaim charges public-template consumption before
// an idle Pod can be detached from its ReplicaSet. That hot claim induces a
// replacement start just like a cold claim. Team-owned warm pools are charged
// later by their scale-up reconciler, so their hot transfer is not charged
// twice.
func (s *SandboxService) admitSandboxStartBeforePoolClaim(
	ctx context.Context,
	teamID string,
	template *v1alpha1.SandboxTemplate,
) (bool, error) {
	if isTeamOwnedWarmPoolTemplate(template, teamID) {
		return false, nil
	}
	if err := s.admitSandboxStartTeamQuota(ctx, teamID); err != nil {
		return false, err
	}
	return true, nil
}

func (s *SandboxService) reserveSandboxTeamQuota(
	ctx context.Context,
	req *ClaimRequest,
	template *v1alpha1.SandboxTemplate,
	operationKind string,
) (*teamquota.Reservation, error) {
	if s == nil || s.teamQuotaStore == nil {
		return nil, fmt.Errorf("%w: capacity store is not configured", ErrTeamQuotaUnavailable)
	}
	if req == nil {
		return nil, fmt.Errorf("%w: claim request is required", ErrInvalidClaimRequest)
	}
	teamID := strings.TrimSpace(req.TeamID)
	if teamID == "" {
		return nil, fmt.Errorf("%w: team_id is required for team quota admission", ErrInvalidClaimRequest)
	}
	target, err := s.sandboxTeamQuotaTarget(ctx, template, req.Config)
	if err != nil {
		return nil, err
	}
	operationID := strings.TrimSpace(req.OperationID)
	if operationID == "" {
		operationID = uuid.NewString()
		req.OperationID = operationID
	}
	owner := sandboxTeamQuotaOwner(req, template)
	reservation, err := s.teamQuotaStore.ReserveTarget(ctx, teamquota.ReserveRequest{
		Owner: owner,
		Operation: teamquota.Operation{
			ID:         operationID,
			Kind:       operationKind,
			Generation: req.RuntimeGeneration,
		},
		Target: target,
	})
	if err != nil {
		return nil, classifyTeamQuotaAdmissionError(err)
	}
	return reservation, nil
}

func sandboxTeamQuotaOwner(req *ClaimRequest, template *v1alpha1.SandboxTemplate) teamquota.Owner {
	clusterID := ""
	if template != nil {
		clusterID = naming.ClusterIDOrDefault(template.Spec.ClusterId)
	}
	return teamquota.Owner{
		TeamID:    strings.TrimSpace(req.TeamID),
		Kind:      "sandbox",
		ID:        strings.TrimSpace(req.SandboxID),
		ClusterID: clusterID,
	}
}

func teamWarmPoolOwnerForClaim(template *v1alpha1.SandboxTemplate) (teamquota.Owner, bool) {
	return controller.TeamWarmPoolQuotaOwner(template)
}

func (s *SandboxService) prepareTeamWarmPoolTransfer(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	req *ClaimRequest,
	idlePod *corev1.Pod,
) (*teamquota.Reservation, error) {
	if s == nil || s.teamQuotaStore == nil {
		return nil, fmt.Errorf("%w: capacity store is not configured", ErrTeamQuotaUnavailable)
	}
	if req == nil || idlePod == nil {
		return nil, fmt.Errorf("%w: warm-pool transfer inputs are required", ErrTeamQuotaUnavailable)
	}
	source, ok := teamWarmPoolOwnerForClaim(template)
	if !ok || source.TeamID != strings.TrimSpace(req.TeamID) {
		return nil, fmt.Errorf("%w: team warm-pool owner does not match claim team", ErrTeamQuotaUnavailable)
	}
	if strings.TrimSpace(idlePod.Annotations[controller.AnnotationTeamID]) != source.TeamID ||
		idlePod.Annotations[controller.AnnotationOwnerKind] != controller.OwnerKindTeamWarmPool {
		return nil, fmt.Errorf("%w: idle pod is not owned by the expected team warm pool", ErrTeamQuotaUnavailable)
	}
	idlePodUID := strings.TrimSpace(string(idlePod.UID))
	if idlePodUID == "" {
		return nil, fmt.Errorf("%w: idle pod UID is required for quota transfer", ErrTeamQuotaUnavailable)
	}
	if strings.TrimSpace(req.OperationID) == "" {
		req.OperationID = uuid.NewString()
	}

	sourceDecrease := positiveTeamQuotaValues(PodSpecTeamQuotaResources(&idlePod.Spec))
	sourceDecrease[teamquota.KeySandboxRuntimeCount] = 1
	// Transfer the exact physical idle Pod before applying any requested resize.
	// This keeps a downsize charged at the old value until UpdateResize succeeds;
	// the normal quota-aware resize saga then admits growth and finalizes shrink.
	destinationTarget := activeSandboxQuotaTarget(idlePod)
	operation := teamquota.Operation{
		ID:         warmPoolTransferOperationID(req.OperationID, idlePodUID),
		Kind:       "claim_warm_pool_transfer",
		Generation: req.RuntimeGeneration,
	}
	reservation, err := s.teamQuotaStore.PrepareTransfer(ctx, teamquota.TransferRequest{
		Source:            source,
		Destination:       sandboxTeamQuotaOwner(req, template),
		Operation:         operation,
		SourceDecrease:    sourceDecrease,
		DestinationTarget: destinationTarget,
		// Detaching the selected Pod can briefly coexist with a ReplicaSet
		// replacement before the external replica commitment is reduced. Hold
		// that complete runtime capacity until the transfer commits.
		TransitionReserve: sourceDecrease.Clone(),
		Runtime: teamquota.RuntimeRef{
			Namespace:  idlePod.Namespace,
			Name:       idlePod.Name,
			UID:        string(idlePod.UID),
			Generation: req.RuntimeGeneration,
		},
	})
	if err != nil {
		return nil, classifyTeamQuotaAdmissionError(err)
	}
	return reservation, nil
}

// warmPoolTransferOperationID scopes request idempotency to the exact idle
// runtime. An aborted hot claim can then fall back to a cold reservation
// without colliding with the transfer's terminal operation history.
func warmPoolTransferOperationID(requestOperationID, idlePodUID string) string {
	name := strings.Join([]string{
		"team-quota/warm-pool-transfer/v1",
		strings.TrimSpace(requestOperationID),
		strings.TrimSpace(idlePodUID),
	}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)).String()
}

func positiveTeamQuotaValues(values teamquota.Values) teamquota.Values {
	positive := make(teamquota.Values)
	for key, value := range values {
		if value > 0 {
			positive[key] = value
		}
	}
	return positive
}

func (s *SandboxService) commitTeamWarmPoolTransfer(
	ctx context.Context,
	reservation *teamquota.Reservation,
	observedSource teamquota.Values,
) error {
	if reservation == nil {
		return nil
	}
	if s == nil || s.teamQuotaStore == nil {
		return fmt.Errorf("%w: capacity store is not configured", ErrTeamQuotaUnavailable)
	}
	store, ok := s.teamQuotaStore.(teamquota.ObservedTransferStore)
	if !ok || store == nil {
		return fmt.Errorf(
			"%w: observed transfer store is not configured",
			ErrTeamQuotaUnavailable,
		)
	}
	if err := store.CommitTransferObservedSource(
		ctx,
		teamquota.Ref(reservation.Owner, reservation.Operation),
		observedSource,
	); err != nil {
		return classifyTeamQuotaAdmissionError(err)
	}
	reservation.State = "active"
	reservation.Committed = reservation.Target.Clone()
	reservation.Reserved = make(teamquota.Values)
	return nil
}

func (s *SandboxService) abortTeamWarmPoolTransfer(
	ctx context.Context,
	reservation *teamquota.Reservation,
	reason string,
) {
	if s == nil || s.teamQuotaStore == nil || reservation == nil {
		return
	}
	if err := s.teamQuotaStore.AbortTransfer(
		ctx,
		teamquota.Ref(reservation.Owner, reservation.Operation),
		reason,
	); err != nil && s.logger != nil {
		s.logger.Error("Failed to abort warm-pool team quota transfer",
			zap.String("sandboxID", reservation.Owner.ID),
			zap.String("operationID", reservation.Operation.ID),
			zap.Error(err),
		)
	}
}

func classifyTeamQuotaAdmissionError(err error) error {
	if err == nil {
		return nil
	}
	if teamquota.IsExceeded(err) {
		return fmt.Errorf("%w: %v", ErrQuotaExceeded, err)
	}
	var conflict *teamquota.OperationConflictError
	if errors.As(err, &conflict) {
		return fmt.Errorf("%w: %v", ErrClaimConflict, err)
	}
	return fmt.Errorf("%w: %v", ErrTeamQuotaUnavailable, err)
}

func (s *SandboxService) attachAndCommitSandboxTeamQuota(
	ctx context.Context,
	reservation *teamquota.Reservation,
	pod *corev1.Pod,
) error {
	if reservation == nil {
		return nil
	}
	if s == nil || s.teamQuotaStore == nil {
		return fmt.Errorf("%w: capacity store is not configured", ErrTeamQuotaUnavailable)
	}
	if pod == nil {
		return fmt.Errorf("%w: observed runtime pod is required", ErrTeamQuotaUnavailable)
	}
	ref := teamquota.Ref(reservation.Owner, reservation.Operation)
	if err := s.teamQuotaStore.AttachRuntime(ctx, ref, sandboxTeamQuotaRuntimeRef(pod)); err != nil {
		return classifyTeamQuotaAdmissionError(err)
	}
	observed := activeSandboxQuotaTarget(pod)
	overageKey, exceeds := quotaTargetExcess(observed, reservation.Target)
	if exceeds {
		admittedValue := reservation.Target[overageKey]
		store, ok := s.teamQuotaStore.(teamquota.ObservedExactCapacityStore)
		if !ok || store == nil {
			return fmt.Errorf(
				"%w: observed exact capacity store is not configured",
				ErrTeamQuotaUnavailable,
			)
		}
		if err := store.CommitObservedExact(ctx, ref, observed); err != nil {
			return classifyTeamQuotaAdmissionError(err)
		}
		reservation.State = "active"
		reservation.Committed = observed.Clone()
		reservation.Target = observed.Clone()
		reservation.Reserved = make(teamquota.Values)
		return &TeamQuotaObservedRuntimeOverage{
			Key:      overageKey,
			Admitted: admittedValue,
			Observed: observed[overageKey],
		}
	}
	if err := s.teamQuotaStore.Commit(ctx, ref); err != nil {
		return classifyTeamQuotaAdmissionError(err)
	}
	return nil
}

func quotaTargetExcess(observed, admitted teamquota.Values) (teamquota.Key, bool) {
	for _, key := range observed.Keys() {
		if observed[key] > admitted[key] {
			return key, true
		}
	}
	return "", false
}

func (s *SandboxService) finalizeSandboxTeamQuotaAdmission(
	ctx context.Context,
	admission *sandboxTeamQuotaAdmission,
	pod *corev1.Pod,
) error {
	if admission == nil || admission.Reservation == nil || admission.Committed {
		return nil
	}
	err := s.attachAndCommitSandboxTeamQuota(ctx, admission.Reservation, pod)
	if err == nil || isObservedRuntimeOverage(err) {
		admission.Committed = true
	}
	return err
}

func (s *SandboxService) resizeSandboxPodResourcesWithTeamQuota(
	ctx context.Context,
	pod *corev1.Pod,
	template *v1alpha1.SandboxTemplate,
	limits v1alpha1.SandboxResourceLimits,
) (*corev1.Pod, error) {
	if s == nil || s.teamQuotaStore == nil {
		return nil, fmt.Errorf("%w: capacity store is not configured", ErrTeamQuotaUnavailable)
	}
	if pod == nil {
		return nil, fmt.Errorf("%w: pod is required", ErrInvalidClaimRequest)
	}
	teamID := strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID])
	sandboxID := strings.TrimSpace(sandboxIDFromPod(pod))
	if teamID == "" || sandboxID == "" {
		return nil, fmt.Errorf("%w: sandbox team and identity are required for resize admission", ErrTeamQuotaUnavailable)
	}

	next := pod.DeepCopy()
	if err := applySandboxResourceLimitsToPodSpec(&next.Spec, limits); err != nil {
		return nil, err
	}
	target := PodSpecTeamQuotaResources(&next.Spec)
	target[teamquota.KeySandboxIdentityCount] = 1
	target[teamquota.KeySandboxRuntimeCount] = 1
	clusterID := ""
	if template != nil {
		clusterID = naming.ClusterIDOrDefault(template.Spec.ClusterId)
	}
	operation := teamquota.Operation{
		ID:         uuid.NewString(),
		Kind:       "resize",
		Generation: runtimeGenerationFromPod(pod),
	}
	reservation, err := s.teamQuotaStore.ReserveTarget(ctx, teamquota.ReserveRequest{
		Owner: teamquota.Owner{
			TeamID:    teamID,
			Kind:      "sandbox",
			ID:        sandboxID,
			ClusterID: clusterID,
		},
		Operation: operation,
		Target:    target,
	})
	if err != nil {
		return nil, classifyTeamQuotaAdmissionError(err)
	}

	physicalMutationStarted := false
	defer func() {
		if reservation == nil || physicalMutationStarted {
			return
		}
		abortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.abortSandboxTeamQuota(abortCtx, reservation, "sandbox resize failed before runtime mutation")
	}()

	// Once the Kubernetes mutation is submitted, any ambiguous error must keep
	// the reservation pending for recovery instead of undercounting a changed
	// runtime.
	physicalMutationStarted = true
	resized, err := s.resizeSandboxPodResources(ctx, pod, limits)
	if err != nil {
		return nil, err
	}
	if err := s.attachAndCommitSandboxTeamQuota(ctx, reservation, resized); err != nil {
		if isObservedRuntimeOverage(err) {
			s.releaseCommittedSandboxTeamQuotaAfterFailure(
				reservation,
				resized,
				"resized runtime exceeded admitted team quota",
			)
		}
		return nil, err
	}
	return resized, nil
}

func (s *SandboxService) abortSandboxTeamQuota(
	ctx context.Context,
	reservation *teamquota.Reservation,
	reason string,
) {
	if s == nil || s.teamQuotaStore == nil || reservation == nil {
		return
	}
	if err := s.teamQuotaStore.Abort(ctx, teamquota.Ref(reservation.Owner, reservation.Operation), reason); err != nil && s.logger != nil {
		s.logger.Error("Failed to abort sandbox team quota reservation",
			zap.String("sandboxID", reservation.Owner.ID),
			zap.String("operationID", reservation.Operation.ID),
			zap.Error(err),
		)
	}
}

// releaseFailedSandboxTeamQuota keeps the allocation counted until the exact
// runtime behind a failed claim has disappeared.
func (s *SandboxService) releaseFailedSandboxTeamQuota(
	reservation *teamquota.Reservation,
	pod *corev1.Pod,
	reason string,
) {
	if s == nil || s.teamQuotaStore == nil || reservation == nil {
		return
	}
	if pod == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.abortSandboxTeamQuota(ctx, reservation, reason)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	ref := teamquota.Ref(reservation.Owner, reservation.Operation)
	runtimeRef := sandboxTeamQuotaRuntimeRef(pod)
	if err := s.teamQuotaStore.AttachRuntime(ctx, ref, runtimeRef); err != nil {
		cancel()
		s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "attach failed runtime", err)
		return
	}
	if err := s.teamQuotaStore.Commit(ctx, ref); err != nil {
		cancel()
		s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "commit failed runtime", err)
		return
	}
	releaseOperation := teamquota.Operation{
		ID:         uuid.NewString(),
		Kind:       "claim_failure_release",
		Generation: runtimeRef.Generation,
	}
	release, err := s.teamQuotaStore.BeginRelease(ctx, teamquota.ReleaseRequest{
		Owner:     reservation.Owner,
		Operation: releaseOperation,
		Target:    zeroSandboxTeamQuotaTarget(),
		Runtime:   runtimeRef,
	})
	cancel()
	if err != nil {
		s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "begin failed runtime release", err)
		return
	}

	s.requestSandboxDeletionAfterClaimFailure(pod, reason)
	go s.confirmSandboxTeamQuotaAfterRuntimeGone(
		teamquota.Ref(release.Owner, release.Operation),
		runtimeRef,
	)
}

func (s *SandboxService) releaseFailedSandboxTeamQuotaAdmission(
	admission *sandboxTeamQuotaAdmission,
	pod *corev1.Pod,
	reason string,
) {
	if admission == nil || admission.Reservation == nil {
		return
	}
	if admission.Committed {
		s.releaseCommittedSandboxTeamQuotaAfterFailure(admission.Reservation, pod, reason)
		return
	}
	if !admission.Transfer {
		s.releaseFailedSandboxTeamQuota(admission.Reservation, pod, reason)
		return
	}
	s.requestSandboxDeletionAfterClaimFailure(pod, reason)
	if !admission.WarmPoolCommitmentDrained {
		go s.abortPreparedTransferAfterRuntimeGone(
			admission.Reservation,
			admission.WarmPoolCommitment,
			pod,
			reason,
		)
		return
	}
	go s.finalizePreparedTransferAfterClaimFailure(
		admission.Reservation,
		admission.WarmPoolCommitment,
		pod,
		reason,
	)
}

func (s *SandboxService) abortPreparedTransferAfterRuntimeGone(
	reservation *teamquota.Reservation,
	commitment *warmPoolReplicaCommitment,
	pod *corev1.Pod,
	reason string,
) {
	if s == nil || s.teamQuotaStore == nil || reservation == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), teamQuotaCleanupTimeout)
	defer cancel()
	runtimeRef := sandboxTeamQuotaRuntimeRef(pod)
	if err := s.waitForTeamQuotaRuntimeGone(ctx, runtimeRef); err != nil {
		s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "wait for failed transfer runtime deletion", err)
		return
	}
	released, err := s.warmPoolReplicaCommitmentReleased(ctx, commitment)
	if err != nil {
		s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "inspect failed warm-pool transfer marker", err)
		return
	}
	if released {
		observed, err := s.waitForWarmPoolReplicaCommitment(ctx, commitment)
		if err != nil {
			s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "drain failed warm-pool transfer source", err)
			return
		}
		commitment.observedSource = observed
		if err := s.commitTeamWarmPoolTransfer(
			ctx,
			reservation,
			commitment.observedSource,
		); err != nil {
			s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "commit applied failed warm-pool transfer", err)
			return
		}
		if err := s.clearWarmPoolReplicaCommitment(ctx, commitment); err != nil {
			s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "clear committed warm-pool transfer marker", err)
		}
		s.releaseCommittedSandboxTeamQuotaAfterFailure(reservation, pod, reason)
		return
	}
	if err := s.teamQuotaStore.AbortTransfer(
		ctx,
		teamquota.Ref(reservation.Owner, reservation.Operation),
		reason,
	); err != nil {
		s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "abort failed warm-pool transfer", err)
		return
	}
	if err := s.clearWarmPoolReplicaCommitment(ctx, commitment); err != nil {
		s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "clear aborted warm-pool transfer marker", err)
	}
}

func (s *SandboxService) finalizePreparedTransferAfterClaimFailure(
	reservation *teamquota.Reservation,
	commitment *warmPoolReplicaCommitment,
	pod *corev1.Pod,
	reason string,
) {
	if s == nil || s.teamQuotaStore == nil || reservation == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), teamQuotaCleanupTimeout)
	defer cancel()
	ticker := time.NewTicker(sandboxLifecycleWaitInterval)
	defer ticker.Stop()
	for {
		observed, waitErr := s.waitForWarmPoolReplicaCommitment(ctx, commitment)
		if waitErr != nil {
			s.logTeamQuotaCleanupFailure(
				reservation.Owner.ID,
				"observe prepared warm-pool transfer source",
				waitErr,
			)
			return
		}
		commitment.observedSource = observed
		err := s.commitTeamWarmPoolTransfer(ctx, reservation, observed)
		if err == nil {
			if clearErr := s.clearWarmPoolReplicaCommitment(ctx, commitment); clearErr != nil {
				s.logTeamQuotaCleanupFailure(
					reservation.Owner.ID,
					"clear committed warm-pool transfer marker",
					clearErr,
				)
			}
			s.releaseCommittedSandboxTeamQuotaAfterFailure(reservation, pod, reason)
			return
		}
		if ctx.Err() != nil {
			s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "commit prepared warm-pool transfer", err)
			return
		}
		select {
		case <-ctx.Done():
			s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "commit prepared warm-pool transfer", ctx.Err())
			return
		case <-ticker.C:
		}
	}
}

func (s *SandboxService) releaseCommittedSandboxTeamQuotaAfterFailure(
	reservation *teamquota.Reservation,
	pod *corev1.Pod,
	reason string,
) {
	s.releaseCommittedSandboxTeamQuotaRuntimeAfterFailure(
		reservation,
		sandboxTeamQuotaRuntimeRef(pod),
		pod,
		reason,
	)
}

func (s *SandboxService) releaseCommittedSandboxTeamQuotaRuntimeAfterFailure(
	reservation *teamquota.Reservation,
	runtimeRef teamquota.RuntimeRef,
	pod *corev1.Pod,
	reason string,
) {
	if s == nil || s.teamQuotaStore == nil || reservation == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	releaseOperation := teamquota.Operation{
		ID: uuid.NewSHA1(
			uuid.NameSpaceOID,
			[]byte("sandbox-failure-release:"+reservation.Operation.ID),
		).String(),
		Kind:       "claim_failure_release",
		Generation: runtimeRef.Generation,
	}
	release, err := s.teamQuotaStore.BeginRelease(ctx, teamquota.ReleaseRequest{
		Owner:     reservation.Owner,
		Operation: releaseOperation,
		Target:    zeroSandboxTeamQuotaTarget(),
		Runtime:   runtimeRef,
	})
	if err != nil {
		s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "begin committed allocation release", err)
		s.requestSandboxDeletionAfterClaimFailure(pod, reason)
		return
	}
	if release == nil {
		s.logTeamQuotaCleanupFailure(
			reservation.Owner.ID,
			"begin committed allocation release",
			errors.New("capacity store returned no release reservation"),
		)
		s.requestSandboxDeletionAfterClaimFailure(pod, reason)
		return
	}
	ref := teamquota.Ref(release.Owner, release.Operation)
	s.requestSandboxDeletionAfterClaimFailure(pod, reason)
	if pod == nil || runtimeRef.Namespace == "" || runtimeRef.Name == "" {
		if err := s.teamQuotaStore.ConfirmRelease(ctx, ref, runtimeRef); err != nil {
			s.logTeamQuotaCleanupFailure(reservation.Owner.ID, "confirm committed allocation release", err)
		}
		return
	}
	go s.confirmSandboxTeamQuotaAfterRuntimeGone(ref, runtimeRef)
}

func (s *SandboxService) confirmSandboxTeamQuotaAfterRuntimeGone(
	operation teamquota.OperationRef,
	runtimeRef teamquota.RuntimeRef,
) {
	ctx, cancel := context.WithTimeout(context.Background(), teamQuotaCleanupTimeout)
	defer cancel()
	if s == nil || s.teamQuotaStore == nil {
		return
	}
	if err := s.waitForTeamQuotaRuntimeGone(ctx, runtimeRef); err != nil {
		s.logTeamQuotaCleanupFailure(operation.Owner.ID, "wait for runtime deletion", err)
		return
	}
	if err := s.teamQuotaStore.ConfirmRelease(ctx, operation, runtimeRef); err != nil {
		s.logTeamQuotaCleanupFailure(operation.Owner.ID, "confirm runtime release", err)
	}
}

func (s *SandboxService) waitForTeamQuotaRuntimeGone(ctx context.Context, runtimeRef teamquota.RuntimeRef) error {
	if strings.TrimSpace(runtimeRef.Namespace) == "" || strings.TrimSpace(runtimeRef.Name) == "" {
		return nil
	}
	if s == nil || s.k8sClient == nil {
		return fmt.Errorf("%w: kubernetes client is not configured", ErrTeamQuotaUnavailable)
	}
	ticker := time.NewTicker(sandboxLifecycleWaitInterval)
	defer ticker.Stop()
	for {
		pod, err := s.k8sClient.CoreV1().Pods(runtimeRef.Namespace).Get(ctx, runtimeRef.Name, metav1.GetOptions{})
		switch {
		case k8serrors.IsNotFound(err):
			return nil
		case err != nil:
			return err
		case runtimeRef.UID != "" && string(pod.UID) != runtimeRef.UID:
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func sandboxTeamQuotaRuntimeRef(pod *corev1.Pod) teamquota.RuntimeRef {
	if pod == nil {
		return teamquota.RuntimeRef{}
	}
	return teamquota.RuntimeRef{
		Namespace:  pod.Namespace,
		Name:       pod.Name,
		UID:        string(pod.UID),
		Generation: runtimeGenerationFromPod(pod),
	}
}

func zeroSandboxTeamQuotaTarget() teamquota.Values {
	return teamquota.Values{
		teamquota.KeySandboxIdentityCount:         0,
		teamquota.KeySandboxRuntimeCount:          0,
		teamquota.KeySandboxCPUMillicores:         0,
		teamquota.KeySandboxMemoryBytes:           0,
		teamquota.KeySandboxEphemeralStorageBytes: 0,
	}
}

func pausedSandboxTeamQuotaTarget() teamquota.Values {
	target := zeroSandboxTeamQuotaTarget()
	target[teamquota.KeySandboxIdentityCount] = 1
	return target
}

func (s *SandboxService) beginDeletedSandboxTeamQuotaRelease(
	ctx context.Context,
	info SandboxLifecycleInfo,
	runtimePaused bool,
) error {
	if s == nil || s.teamQuotaStore == nil {
		return fmt.Errorf("%w: capacity store is not configured", ErrTeamQuotaUnavailable)
	}
	teamID := strings.TrimSpace(info.TeamID)
	sandboxID := strings.TrimSpace(info.SandboxID)
	if sandboxID == "" {
		sandboxID = strings.TrimSpace(info.PodName)
	}
	if teamID == "" || sandboxID == "" {
		return fmt.Errorf("%w: deleted sandbox team and identity are required", ErrTeamQuotaUnavailable)
	}
	target := zeroSandboxTeamQuotaTarget()
	if runtimePaused {
		target = pausedSandboxTeamQuotaTarget()
	}
	runtimeRef := teamquota.RuntimeRef{
		Namespace:  strings.TrimSpace(info.Namespace),
		Name:       strings.TrimSpace(info.PodName),
		UID:        strings.TrimSpace(info.PodUID),
		Generation: info.RuntimeGeneration,
	}
	runtimeIdentity := runtimeRef.UID
	if runtimeIdentity == "" {
		runtimeIdentity = runtimeRef.Namespace + "/" + runtimeRef.Name
	}
	operationKind := "delete_runtime"
	if runtimePaused {
		operationKind = "pause_runtime"
	}
	owner := teamquota.Owner{
		TeamID:    teamID,
		Kind:      "sandbox",
		ID:        sandboxID,
		ClusterID: naming.ClusterIDOrDefault(&info.ClusterID),
	}
	operation := teamquota.Operation{
		ID:         fmt.Sprintf("%s:%s:%d", operationKind, runtimeIdentity, runtimeRef.Generation),
		Kind:       operationKind,
		Generation: runtimeRef.Generation,
	}
	release, err := s.teamQuotaStore.BeginRelease(ctx, teamquota.ReleaseRequest{
		Owner:     owner,
		Operation: operation,
		Target:    target,
		Runtime:   runtimeRef,
	})
	if err != nil {
		if teamquota.IsTeamAdmissionDisabled(err) && !runtimePaused {
			released, verifyErr := s.deletedSandboxTeamQuotaAlreadyReleased(ctx, owner)
			if verifyErr != nil {
				return fmt.Errorf(
					"%w: verify deleted sandbox quota after team tombstone: %v",
					ErrTeamQuotaUnavailable,
					verifyErr,
				)
			}
			if released {
				return nil
			}
		}
		return classifyTeamQuotaAdmissionError(err)
	}
	ref := teamquota.Ref(release.Owner, release.Operation)
	if runtimeRef.Namespace == "" || runtimeRef.Name == "" {
		if err := s.teamQuotaStore.ConfirmRelease(ctx, ref, runtimeRef); err != nil {
			return classifyTeamQuotaAdmissionError(err)
		}
		return nil
	}
	go s.confirmSandboxTeamQuotaAfterRuntimeGone(ref, runtimeRef)
	return nil
}

type sandboxTeamQuotaRecoveryReader interface {
	GetRecoveryAllocation(
		ctx context.Context,
		owner teamquota.Owner,
	) (*teamquota.RecoveryAllocation, error)
}

// deletedSandboxTeamQuotaAlreadyReleased makes a deletion retry a no-op only
// after the durable ledger proves that no capacity or operation remains. It is
// intentionally narrower than normal admission: a team tombstone must never
// authorize a new allocation or a non-zero target.
func (s *SandboxService) deletedSandboxTeamQuotaAlreadyReleased(
	ctx context.Context,
	owner teamquota.Owner,
) (bool, error) {
	reader, ok := s.teamQuotaStore.(sandboxTeamQuotaRecoveryReader)
	if !ok || reader == nil {
		return false, fmt.Errorf("capacity store does not expose recovery state")
	}
	allocation, err := reader.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		return false, err
	}
	if allocation == nil {
		return true, nil
	}
	if allocation.State != "released" || allocation.Operation != nil {
		return false, nil
	}
	for _, values := range []teamquota.Values{allocation.Committed, allocation.Pending} {
		for _, value := range values {
			if value != 0 {
				return false, nil
			}
		}
	}
	return true, nil
}

func (s *SandboxService) logTeamQuotaCleanupFailure(sandboxID, operation string, err error) {
	if s == nil || s.logger == nil || err == nil {
		return
	}
	s.logger.Error("Sandbox team quota cleanup failed",
		zap.String("sandboxID", sandboxID),
		zap.String("operation", operation),
		zap.Error(err),
	)
}
