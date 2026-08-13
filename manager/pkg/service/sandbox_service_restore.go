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
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/carrier"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const sandboxLifecycleWaitInterval = 100 * time.Millisecond

// ResumePausedSandboxRuntime creates a new runtime for a paused durable sandbox
// and restores the latest writable rootfs checkpoint. A terminal runtime first
// completes crash recovery through the durable pause transaction.
func (s *SandboxService) ResumePausedSandboxRuntime(ctx context.Context, sandboxID string) (result *managerapi.Sandbox, resultErr error) {
	if s == nil || s.sandboxStore == nil {
		return nil, k8serrors.NewNotFound(corev1.Resource("pod"), sandboxID)
	}

	var pod *corev1.Pod
	var record *sandboxstore.SandboxRecord
	var template *v1alpha1.SandboxTemplate
	var txn *sandboxstore.SandboxLifecycleTxn
	var req *ClaimRequest
	var deletingPodRef *sandboxRuntimePodRef
	runtimeRecoveryRequested := false
	claimType := "hot"
	restoreNeeded := false
	started := time.Now()
	prepareStarted := started
	defer func() {
		if restoreNeeded && record != nil {
			s.observeClaimPhase(record.TemplateID, claimType, "resume_total", started, resultErr)
		}
	}()
	for {
		pod = nil
		record = nil
		template = nil
		txn = nil
		req = nil
		deletingPodRef = nil
		runtimeRecoveryRequested = false
		restoreNeeded = false
		var waitErr error
		err := s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, locked *sandboxstore.SandboxRecord) error {
			if locked.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !locked.DeletedAt.IsZero() {
				return k8serrors.NewNotFound(corev1.Resource("sandbox"), sandboxID)
			}
			if locked.DesiredState == sandboxstore.SandboxDesiredStateTerminating {
				return k8serrors.NewConflict(corev1.Resource("sandbox"), sandboxID, fmt.Errorf("sandbox termination is in progress"))
			}
			if sandboxHardExpired(locked.HardExpiresAt, s.now()) {
				return k8serrors.NewNotFound(corev1.Resource("sandbox"), sandboxID)
			}
			activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, sandboxID)
			if err != nil {
				return err
			}
			if activeTxn != nil {
				switch activeTxn.Kind {
				case sandboxstore.SandboxLifecycleKindPause:
					if sandboxLifecycleTxnCancelableAutoPause(activeTxn) {
						if _, err := tx.RequestLifecycleTxnCancel(lockCtx, activeTxn.ID, "runtime access arrived during auto pause"); err != nil {
							return err
						}
					}
					waitErr = errSandboxLifecyclePausing
					return nil
				default:
					waitErr = errSandboxLifecycleResuming
					return nil
				}
			}
			existing, getErr := s.getSandboxPod(lockCtx, sandboxID)
			if getErr == nil {
				if existing.DeletionTimestamp != nil {
					deletingPodRef = &sandboxRuntimePodRef{
						namespace: existing.Namespace,
						name:      existing.Name,
					}
					if err := s.k8sClient.CoreV1().Pods(existing.Namespace).Delete(lockCtx, existing.Name, immediatePodDeletionOptions()); err != nil && !k8serrors.IsNotFound(err) {
						return fmt.Errorf("shorten stale sandbox runtime pod deletion: %w", err)
					}
					return errSandboxRuntimeDeleting
				}
				if locked.DesiredState == sandboxstore.SandboxDesiredStatePaused {
					deletingPodRef = &sandboxRuntimePodRef{
						namespace: existing.Namespace,
						name:      existing.Name,
					}
					if err := s.k8sClient.CoreV1().Pods(existing.Namespace).Delete(lockCtx, existing.Name, immediatePodDeletionOptions()); err != nil && !k8serrors.IsNotFound(err) {
						return fmt.Errorf("delete stale sandbox runtime pod: %w", err)
					}
					return errSandboxRuntimeDeleting
				}
				if sandboxRuntimePodNeedsReplacement(existing) {
					if err := beginCrashRecoveryTxn(lockCtx, tx, locked, existing); err != nil {
						return err
					}
					runtimeRecoveryRequested = true
					waitErr = errSandboxLifecyclePausing
					return nil
				}
				if sandboxRuntimeLivenessFailureSustained(
					sandboxRuntimeLivenessCondition(existing),
					s.now(),
					defaultSandboxRuntimeUnhealthyAfter,
				) {
					if err := beginHealthRecoveryTxn(lockCtx, tx, locked, existing); err != nil {
						return err
					}
					runtimeRecoveryRequested = true
					waitErr = errSandboxLifecyclePausing
					return nil
				}
				pod = existing
				record = nil
				return tx.SaveRuntime(lockCtx, sandboxID, existing.Namespace, existing.Name, runtimeGenerationFromPod(existing), parseRFC3339AnnotationTime(existing.Annotations, controller.AnnotationExpiresAt), parseRFC3339AnnotationTime(existing.Annotations, controller.AnnotationHardExpiresAt), sandboxRuntimeMetadataFromPod(existing))
			}
			if getErr != nil && !k8serrors.IsNotFound(getErr) {
				return fmt.Errorf("get current runtime pod: %w", getErr)
			}
			if locked.DesiredState != sandboxstore.SandboxDesiredStatePaused {
				waitErr = errSandboxRuntimeReconcileRequested
				return nil
			}

			resumeTemplate, err := s.templateForSandboxRecord(locked)
			if err != nil {
				return err
			}
			template = resumeTemplate
			if err := s.enforceActiveSandboxQuota(lockCtx, locked.TeamID); err != nil {
				return err
			}
			generation := locked.RuntimeGeneration + 1
			record = cloneSandboxRecordForLifecycle(locked)
			req = &ClaimRequest{
				TeamID:               locked.TeamID,
				UserID:               locked.UserID,
				Template:             locked.TemplateID,
				Config:               &locked.Config,
				Mounts:               locked.Mounts,
				SandboxID:            locked.ID,
				RuntimeGeneration:    generation,
				HardExpiresAt:        locked.HardExpiresAt,
				WebhookStateVolumeID: locked.WebhookStateVolumeID,
			}
			if strings.TrimSpace(locked.OwnerKind) != "" {
				req.Metadata = &ClaimMetadata{OwnerKind: locked.OwnerKind}
			}
			restoreNeeded = true
			expectedHeadID, err := currentRootFSHeadID(lockCtx, tx, sandboxID)
			if err != nil {
				return err
			}
			txn = &sandboxstore.SandboxLifecycleTxn{
				ID:                   uuid.NewString(),
				SandboxID:            sandboxID,
				Kind:                 sandboxstore.SandboxLifecycleKindResume,
				Phase:                sandboxstore.SandboxLifecyclePhasePreparing,
				FromGeneration:       locked.RuntimeGeneration,
				ToGeneration:         generation,
				ExpectedHeadID:       expectedHeadID,
				RootFSRuntimeVersion: locked.RootFSRuntimeVersion,
			}
			return tx.BeginLifecycleTxn(lockCtx, txn)
		})
		if err == nil && runtimeRecoveryRequested {
			s.enqueueSandboxPause(sandboxID)
			err = waitErr
		}
		if err == nil && waitErr != nil {
			err = waitErr
		}
		if err == nil {
			break
		}
		if errors.Is(err, sandboxstore.ErrSandboxRecordNotFound) {
			return nil, k8serrors.NewNotFound(corev1.Resource("sandbox"), sandboxID)
		}
		switch {
		case errors.Is(err, errSandboxLifecyclePausing):
			if err := s.waitForSandboxLifecycleTxnExit(ctx, sandboxID); err != nil {
				return nil, err
			}
			continue
		case errors.Is(err, errSandboxLifecycleResuming):
			if err := s.waitForSandboxLifecycleTxnExit(ctx, sandboxID); err != nil {
				return nil, err
			}
			continue
		case errors.Is(err, errSandboxRuntimeDeleting):
			if deletingPodRef == nil {
				return nil, err
			}
			if err := s.waitForSandboxRuntimePodDeletion(ctx, deletingPodRef.namespace, deletingPodRef.name); err != nil {
				return nil, err
			}
			continue
		case errors.Is(err, errSandboxRuntimeReconcileRequested):
			if err := s.ReconcileSandboxRuntime(ctx, sandboxID); err != nil {
				return nil, err
			}
			if err := s.waitForSandboxLifecycleTxnExit(ctx, sandboxID); err != nil {
				return nil, err
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(sandboxLifecycleWaitInterval):
			}
			continue
		default:
			return nil, err
		}
	}
	if record != nil && restoreNeeded {
		s.observeClaimPhase(record.TemplateID, claimType, "prepare_resume_transaction", prepareStarted, nil)
	}
	if pod == nil {
		if record == nil || !restoreNeeded {
			return s.GetSandbox(ctx, sandboxID)
		}
		phaseStarted := time.Now()
		err := s.restoreResumeCredentialBindings(ctx, req)
		s.observeClaimPhase(record.TemplateID, claimType, "restore_credential_bindings", phaseStarted, err)
		if err != nil {
			_ = s.abortLifecycleTxn(context.Background(), sandboxID, txn.ID, err.Error())
			return nil, fmt.Errorf("restore credential bindings: %w", err)
		}
		phaseStarted = time.Now()
		if record.RootFSRuntimeVersion == sandboxstore.RootFSRuntimeS0FSV2 {
			if s.sharedCarrierPool == nil {
				err = fmt.Errorf("S0FS carrier runtime is not configured")
				_ = s.abortLifecycleTxn(context.Background(), sandboxID, txn.ID, err.Error())
				return nil, err
			}
			pod, claimType, err = s.allocateS0FSCarrier(ctx, template, req)
			s.observeClaimPhase(record.TemplateID, claimType, "allocate_s0fs_carrier", phaseStarted, err)
			if err != nil {
				_ = s.abortLifecycleTxn(context.Background(), sandboxID, txn.ID, err.Error())
				return nil, fmt.Errorf("allocate S0FS carrier: %w", err)
			}
		} else {
			pod, err = s.claimIdlePod(ctx, template, req)
			s.observeClaimPhase(record.TemplateID, claimType, "claim_idle_pod", phaseStarted, err)
			if err != nil {
				_ = s.abortLifecycleTxn(context.Background(), sandboxID, txn.ID, err.Error())
				return nil, fmt.Errorf("claim idle pod: %w", err)
			}
			if pod == nil {
				claimType = "cold"
				phaseStarted = time.Now()
				pod, err = s.createNewPod(ctx, template, req)
				s.observeClaimPhase(record.TemplateID, claimType, "create_new_pod", phaseStarted, err)
				if err != nil {
					_ = s.abortLifecycleTxn(context.Background(), sandboxID, txn.ID, err.Error())
					return nil, fmt.Errorf("create runtime pod: %w", err)
				}
			}
		}
		txn.ToPodNamespace = pod.Namespace
		txn.ToPodName = pod.Name
		phaseStarted = time.Now()
		err = s.recordResumeLifecycleRuntime(ctx, record.ID, txn, pod)
		s.observeClaimPhase(record.TemplateID, claimType, "record_resume_runtime", phaseStarted, err)
		if err != nil {
			s.requestSandboxDeletionAfterClaimFailure(pod, "restored runtime transaction update failed")
			_ = s.abortLifecycleTxn(context.Background(), sandboxID, txn.ID, err.Error())
			return nil, err
		}
	}
	if record == nil || !restoreNeeded {
		return s.GetSandbox(ctx, sandboxID)
	}

	restoredPod, err := s.finishRestoredSandboxRuntime(ctx, pod, record, claimType)
	if err != nil {
		if restoredPod != nil {
			pod = restoredPod
		}
		s.requestSandboxDeletionAfterClaimFailure(pod, "restored runtime initialization failed")
		if txn != nil {
			_ = s.abortLifecycleTxn(context.Background(), sandboxID, txn.ID, err.Error())
		}
		return nil, err
	}
	if txn != nil {
		if txn.ToPodNamespace != restoredPod.Namespace || txn.ToPodName != restoredPod.Name {
			if err := s.recordResumeLifecycleRuntime(ctx, record.ID, txn, restoredPod); err != nil {
				s.requestSandboxDeletionAfterClaimFailure(restoredPod, "rootfs Head runtime transaction update failed")
				_ = s.abortLifecycleTxn(context.Background(), sandboxID, txn.ID, err.Error())
				return nil, err
			}
		}
		phaseStarted := time.Now()
		err = s.commitResumedSandboxRuntime(ctx, restoredPod, record, txn)
		s.observeClaimPhase(record.TemplateID, claimType, "commit_resume_runtime", phaseStarted, err)
		if err != nil {
			s.requestSandboxDeletionAfterClaimFailure(restoredPod, "restored runtime commit failed")
			_ = s.abortLifecycleTxn(context.Background(), sandboxID, txn.ID, err.Error())
			return nil, err
		}
	}
	s.enqueueHotClaimReservation(restoredPod)
	phaseStarted := time.Now()
	result, err = s.GetSandbox(ctx, sandboxID)
	s.observeClaimPhase(record.TemplateID, claimType, "read_resumed_sandbox", phaseStarted, err)
	return result, err
}

// restoreResumeCredentialBindings rejoins bindings stored outside the sanitized
// sandbox config before a paused sandbox claims its replacement runtime pod.
func (s *SandboxService) restoreResumeCredentialBindings(ctx context.Context, req *ClaimRequest) error {
	if req == nil {
		return nil
	}
	req.mayHaveExistingCredentialBindings = true
	bindings, err := s.loadCredentialBindingsForSandbox(ctx, req.TeamID, req.SandboxID)
	if err != nil {
		return err
	}
	if len(bindings) == 0 {
		return nil
	}
	config := cloneSandboxConfig(req.Config)
	if config == nil {
		config = &sandboxstore.SandboxConfig{}
	}
	if config.Network == nil {
		config.Network = &v1alpha1.SandboxNetworkPolicy{}
	}
	config.Network.CredentialBindings = bindings
	req.Config = config
	return nil
}

func (s *SandboxService) recordResumeLifecycleRuntime(ctx context.Context, sandboxID string, txn *sandboxstore.SandboxLifecycleTxn, pod *corev1.Pod) error {
	if s == nil || s.sandboxStore == nil || txn == nil || pod == nil {
		return nil
	}
	return s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, locked *sandboxstore.SandboxRecord) error {
		activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, sandboxID)
		if err != nil {
			return err
		}
		if activeTxn == nil || activeTxn.ID != txn.ID || activeTxn.Kind != sandboxstore.SandboxLifecycleKindResume {
			return fmt.Errorf("resume lifecycle transaction is no longer active")
		}
		if locked.DesiredState != sandboxstore.SandboxDesiredStatePaused {
			return fmt.Errorf("resume lifecycle runtime update expected paused sandbox, got %s", locked.DesiredState)
		}
		podGeneration := runtimeGenerationFromPod(pod)
		if podGeneration != txn.ToGeneration {
			return fmt.Errorf("resume lifecycle generation changed: txn=%d pod=%d", txn.ToGeneration, podGeneration)
		}
		return tx.SetLifecycleTxnRuntime(lockCtx, txn.ID, pod.Namespace, pod.Name)
	})
}

func (s *SandboxService) commitResumedSandboxRuntime(ctx context.Context, pod *corev1.Pod, record *sandboxstore.SandboxRecord, txn *sandboxstore.SandboxLifecycleTxn) error {
	if s == nil || s.sandboxStore == nil || pod == nil || record == nil || txn == nil {
		return nil
	}
	return s.sandboxStore.WithSandboxLock(ctx, record.ID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, locked *sandboxstore.SandboxRecord) error {
		activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, record.ID)
		if err != nil {
			return err
		}
		if activeTxn == nil || activeTxn.ID != txn.ID || activeTxn.Kind != sandboxstore.SandboxLifecycleKindResume {
			return fmt.Errorf("resume lifecycle transaction is no longer active")
		}
		if locked.DesiredState != sandboxstore.SandboxDesiredStatePaused {
			return fmt.Errorf("resume lifecycle commit expected paused sandbox, got %s", locked.DesiredState)
		}
		podGeneration := runtimeGenerationFromPod(pod)
		if podGeneration != txn.ToGeneration {
			return fmt.Errorf("resume lifecycle generation changed: txn=%d pod=%d", txn.ToGeneration, podGeneration)
		}
		if err := tx.SaveRuntime(lockCtx, record.ID, pod.Namespace, pod.Name, txn.ToGeneration, parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt), parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt), sandboxRuntimeMetadataFromPod(pod)); err != nil {
			return err
		}
		if hotClaimUsesRecordCompletion(pod) {
			if err := tx.MarkHotClaimCompleted(lockCtx, record.ID, s.clock.Now().UTC()); err != nil {
				return err
			}
		}
		return tx.CommitLifecycleTxn(lockCtx, txn.ID, "")
	})
}

var (
	errSandboxLifecyclePausing             = errors.New("sandbox lifecycle pause is in progress")
	errSandboxLifecycleResuming            = errors.New("sandbox lifecycle resume is in progress")
	errSandboxLifecycleRootFSCheckpointing = errors.New("sandbox lifecycle rootfs checkpoint is in progress")
	errSandboxRuntimeDeleting              = errors.New("sandbox runtime pod deletion is in progress")
	errSandboxRuntimeReconcileRequested    = errors.New("sandbox runtime state requires reconciliation")
)

type sandboxRuntimePodRef struct {
	namespace string
	name      string
}

func sandboxRuntimePodNeedsReplacement(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	_, terminated := terminatedProcdContainer(pod)
	return terminated != nil || sandboxRuntimePodTerminal(pod)
}

func (s *SandboxService) waitForSandboxLifecycleTxnExit(ctx context.Context, sandboxID string) error {
	if s == nil || s.sandboxStore == nil {
		return nil
	}
	ticker := time.NewTicker(sandboxLifecycleWaitInterval)
	defer ticker.Stop()
	for {
		txn, err := s.sandboxStore.GetActiveLifecycleTxn(ctx, sandboxID)
		if err != nil {
			if errors.Is(err, sandboxstore.ErrSandboxRecordNotFound) {
				return k8serrors.NewNotFound(corev1.Resource("sandbox"), sandboxID)
			}
			return err
		}
		if txn == nil {
			return nil
		}
		if sandboxLifecycleRootFSSourceCheckpointTxnStale(txn, s.now()) {
			reason := "stale rootfs checkpoint transaction"
			switch txn.Kind {
			case sandboxstore.SandboxLifecycleKindFork:
				reason = "stale fork transaction"
			case sandboxstore.SandboxLifecycleKindSnapshot:
				reason = "stale snapshot transaction"
			}
			if err := s.abortLifecycleTxn(ctx, sandboxID, txn.ID, reason); err != nil {
				return err
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *SandboxService) waitForSandboxRuntimePodDeletion(ctx context.Context, namespace, name string) error {
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(name) == "" {
		return nil
	}
	ticker := time.NewTicker(sandboxLifecycleWaitInterval)
	defer ticker.Stop()
	for {
		if s != nil && s.podLister != nil {
			_, err := s.podLister.Pods(namespace).Get(name)
			if k8serrors.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
		} else if s != nil && s.k8sClient != nil {
			_, err := s.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
			switch {
			case k8serrors.IsNotFound(err):
				return nil
			case err != nil:
				return err
			}
		} else {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *SandboxService) finishRestoredSandboxRuntime(ctx context.Context, pod *corev1.Pod, record *sandboxstore.SandboxRecord, claimType string) (*corev1.Pod, error) {
	template, err := s.templateForSandboxRecord(record)
	if err != nil {
		return pod, err
	}
	s0fsCarrier := record.RootFSRuntimeVersion == sandboxstore.RootFSRuntimeS0FSV2 && strings.TrimSpace(pod.Annotations[carrier.AnnotationSlot]) != ""
	phaseStarted := time.Now()
	rootFSHead, err := s.latestRootFSHead(ctx, record.ID)
	s.observeClaimPhase(record.TemplateID, claimType, "load_rootfs_head", phaseStarted, err)
	if err != nil {
		return pod, fmt.Errorf("load rootfs Head: %w", err)
	}
	// A missing published Head means the lost runtime had no durable rootfs
	// checkpoint. Keep the claimed template baseline and start a new sync below.
	if s0fsCarrier && rootFSHead == nil {
		return pod, fmt.Errorf("S0FS carrier resume requires a published rootfs Head")
	}
	if claimType == "cold" && !s0fsCarrier {
		networkPod, err := s.waitForColdPodNetworkPolicy(ctx, pod, record.TeamID)
		if err != nil {
			return pod, err
		}
		pod = networkPod
		// A published Head replaces the template container below. Waiting for the
		// template container's full readiness first adds an entire probe period
		// without protecting the restored runtime. The Head container still has
		// its image identity checked before storage and assignment activation.
		if rootFSHead == nil {
			readyPod, err := s.waitForPodClaimReady(ctx, pod.Namespace, pod.Name)
			if err != nil {
				return pod, fmt.Errorf("wait for pod claim readiness: %w", err)
			}
			pod = readyPod
		}
	}
	req := &ClaimRequest{
		TeamID:               record.TeamID,
		UserID:               record.UserID,
		Template:             record.TemplateID,
		Config:               &record.Config,
		Mounts:               record.Mounts,
		SandboxID:            record.ID,
		RuntimeGeneration:    record.RuntimeGeneration + 1,
		HardExpiresAt:        record.HardExpiresAt,
		WebhookStateVolumeID: record.WebhookStateVolumeID,
	}
	if strings.TrimSpace(record.OwnerKind) != "" {
		req.Metadata = &ClaimMetadata{OwnerKind: record.OwnerKind}
	}
	resetCopiedSessionState := false
	runtimeRevision := ""
	if rootFSHead != nil {
		resetCopiedSessionState = strings.TrimSpace(rootFSHead.SourceSandboxID) != "" && strings.TrimSpace(rootFSHead.SourceSandboxID) != strings.TrimSpace(record.ID)
		var recreated bool
		if s0fsCarrier {
			phaseStarted = time.Now()
			pod, runtimeRevision, err = s.publishRuntimeAssignment(ctx, pod, resetCopiedSessionState)
			s.observeClaimPhase(record.TemplateID, claimType, "publish_runtime_assignment", phaseStarted, err)
			if err != nil {
				return pod, err
			}
			phaseStarted = time.Now()
			pod, err = s.activateS0FSCarrierHead(ctx, pod, rootFSHead, record.TemplateID, claimType)
		} else {
			phaseStarted = time.Now()
			pod, recreated, err = s.activateRuntimeWithRootFSHead(ctx, pod, template, req, rootFSHead, claimType == "cold")
		}
		s.observeClaimPhase(record.TemplateID, claimType, "materialize_rootfs_head", phaseStarted, err)
		if err != nil {
			return pod, err
		}
		if recreated {
			claimType = "cold"
			phaseStarted = time.Now()
			pod, err = s.waitForColdPodNetworkPolicy(ctx, pod, record.TeamID)
			s.observeClaimPhase(record.TemplateID, claimType, "rootfs_head_network_policy", phaseStarted, err)
			if err != nil {
				return pod, err
			}
			phaseStarted = time.Now()
			// Full claim readiness depends on the runtime assignment published below.
			pod, err = s.waitForPodRootFSHeadReady(ctx, pod.Namespace, pod.Name, rootFSHead)
			s.observeClaimPhase(record.TemplateID, claimType, "rootfs_head_runtime_ready", phaseStarted, err)
			if err != nil {
				return pod, err
			}
		}
	}
	if runtimeRevision == "" {
		phaseStarted = time.Now()
		pod, runtimeRevision, err = s.publishRuntimeAssignment(ctx, pod, resetCopiedSessionState)
		s.observeClaimPhase(record.TemplateID, claimType, "publish_runtime_assignment", phaseStarted, err)
		if err != nil {
			return pod, err
		}
	}
	prepareRootFS := func(readyCtx context.Context, readyPod *corev1.Pod) error {
		phaseStarted := time.Now()
		_, bindErr := s.bindVolumePortals(readyCtx, readyPod, req, template)
		s.observeClaimPhase(record.TemplateID, claimType, "bind_volume_portals", phaseStarted, bindErr)
		if bindErr != nil {
			return fmt.Errorf("bind volume portals: %w", bindErr)
		}
		phaseStarted = time.Now()
		bindErr = s.bindWebhookStatePortal(readyCtx, readyPod, req)
		s.observeClaimPhase(record.TemplateID, claimType, "bind_webhook_state_portal", phaseStarted, bindErr)
		if bindErr != nil {
			return fmt.Errorf("bind webhook state portal: %w", bindErr)
		}
		phaseStarted = time.Now()
		bindErr = s.bindSandboxRootFSSync(readyCtx, readyPod, record)
		s.observeClaimPhase(record.TemplateID, claimType, "bind_rootfs_sync", phaseStarted, bindErr)
		if bindErr != nil {
			return fmt.Errorf("bind rootfs sync: %w", bindErr)
		}
		return nil
	}
	if s0fsCarrier {
		pod, err = s.waitForS0FSCarrierReady(ctx, pod, record.TemplateID, claimType, prepareRootFS)
	} else {
		err = prepareRootFS(ctx, pod)
	}
	if err != nil {
		return pod, fmt.Errorf("prepare restored rootfs: %w", err)
	}
	phaseStarted = time.Now()
	pod, err = s.activateRuntimeAssignment(ctx, pod, runtimeRevision)
	s.observeClaimPhase(record.TemplateID, claimType, "activate_runtime", phaseStarted, err)
	if err != nil {
		return pod, fmt.Errorf("activate runtime: %w", err)
	}
	if s.logger != nil {
		s.logger.Info("Resumed paused sandbox runtime",
			zap.String("sandboxID", record.ID),
			zap.String("pod", pod.Name),
			zap.String("claimType", claimType),
		)
	}
	return pod, nil
}

func (s *SandboxService) templateForSandboxRecord(record *sandboxstore.SandboxRecord) (*v1alpha1.SandboxTemplate, error) {
	if record == nil {
		return nil, fmt.Errorf("sandbox record is required")
	}
	if s.templateLister != nil && record.TemplateNamespace != "" && record.TemplateName != "" {
		if template, err := s.templateLister.Get(record.TemplateNamespace, record.TemplateName); err == nil {
			return template, nil
		}
	}
	templateName := strings.TrimSpace(record.TemplateName)
	if templateName == "" {
		templateName = record.TemplateID
	}
	namespace := strings.TrimSpace(record.TemplateNamespace)
	if namespace == "" {
		var err error
		namespace, err = naming.TemplateNamespaceForBuiltin(record.TemplateID)
		if err != nil {
			return nil, err
		}
	}
	spec := record.TemplateSpec
	return &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      templateName,
			Namespace: namespace,
			Labels: map[string]string{
				controller.LabelTemplateLogicalID: record.TemplateID,
			},
		},
		Spec: spec,
	}, nil
}

func sandboxRestoreContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, defaultSandboxRestoreTimeout)
}
