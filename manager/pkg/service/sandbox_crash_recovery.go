package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var errSandboxCrashRecoveryBlocked = errors.New("sandbox crash recovery is blocked by another lifecycle transaction")

// RecoverTerminatedSandboxRuntime durably starts checkpoint recovery for an
// unexpectedly terminated claimed runtime. The pause controller checkpoints
// and reconstructs the runtime so manager restarts reuse the same transaction.
func (s *SandboxService) RecoverTerminatedSandboxRuntime(ctx context.Context, pod *corev1.Pod) error {
	if s == nil || s.sandboxStore == nil || pod == nil || pod.DeletionTimestamp != nil {
		return nil
	}
	status, terminated := terminatedProcdContainer(pod)
	if !sandboxCrashRecoveryPodEligible(pod) || (terminated == nil && !sandboxRuntimePodTerminal(pod)) {
		return nil
	}

	sandboxID := sandboxIDFromPod(pod)
	enqueue := false
	deleteStaleRuntime := false
	err := s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx SandboxStoreTx, record *SandboxRecord) error {
		if record == nil || record.DesiredState == SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() || sandboxHardExpired(record.HardExpiresAt, s.now()) {
			return nil
		}
		if !sandboxRecordReferencesPod(record, pod) {
			return nil
		}
		activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, sandboxID)
		if err != nil {
			return err
		}
		if activeTxn != nil {
			if crashRecoveryTxnReferencesPod(activeTxn, pod) {
				enqueue = true
				return nil
			}
			return fmt.Errorf("%w: active %s transaction %s", errSandboxCrashRecoveryBlocked, activeTxn.Kind, activeTxn.ID)
		}
		if record.DesiredState == SandboxDesiredStatePaused {
			deleteStaleRuntime = true
			return nil
		}
		if err := beginCrashRecoveryTxn(lockCtx, tx, record, pod); err != nil {
			return err
		}
		enqueue = true
		return nil
	})
	if errors.Is(err, ErrSandboxRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if deleteStaleRuntime {
		return s.deleteRecoveredRuntimePod(ctx, pod)
	}
	if !enqueue {
		return nil
	}
	if s.logger != nil {
		containerID := ""
		if status != nil {
			containerID = status.ContainerID
		}
		exitCode := int32(0)
		signal := int32(0)
		reason := ""
		if terminated != nil {
			exitCode = terminated.ExitCode
			signal = terminated.Signal
			reason = terminated.Reason
		}
		s.logger.Warn("Queued terminated sandbox runtime rootfs recovery",
			zap.String("sandboxID", sandboxID),
			zap.String("namespace", pod.Namespace),
			zap.String("pod", pod.Name),
			zap.String("podUID", string(pod.UID)),
			zap.String("containerID", containerID),
			zap.Int64("runtimeGeneration", runtimeGenerationFromPod(pod)),
			zap.Int32("exitCode", exitCode),
			zap.Int32("signal", signal),
			zap.String("reason", reason),
		)
	}
	s.enqueueSandboxRecovery(sandboxID)
	return nil
}

// RecoverUnhealthySandboxRuntime starts a durable pause-and-resume transition
// after the runtime liveness probe has failed continuously beyond the recovery
// threshold. Runtime failure recovery is a platform responsibility and does
// not depend on the sandbox auto-resume access policy.
func (s *SandboxService) RecoverUnhealthySandboxRuntime(ctx context.Context, pod *corev1.Pod) error {
	if s == nil || s.sandboxStore == nil || pod == nil || pod.DeletionTimestamp != nil ||
		!sandboxCrashRecoveryPodEligible(pod) || pod.Status.Phase != corev1.PodRunning {
		return nil
	}
	if _, terminated := terminatedProcdContainer(pod); terminated != nil {
		return s.RecoverTerminatedSandboxRuntime(ctx, pod)
	}
	liveness := sandboxRuntimeLivenessCondition(pod)
	if !sandboxRuntimeLivenessFailureSustained(liveness, s.now(), defaultSandboxRuntimeUnhealthyAfter) {
		return nil
	}

	sandboxID := sandboxIDFromPod(pod)
	enqueue := false
	err := s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx SandboxStoreTx, record *SandboxRecord) error {
		if record == nil || record.DesiredState == SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() ||
			sandboxHardExpired(record.HardExpiresAt, s.now()) || !sandboxRecordReferencesPod(record, pod) {
			return nil
		}
		activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, sandboxID)
		if err != nil {
			return err
		}
		if activeTxn != nil {
			if healthRecoveryTxnReferencesPod(activeTxn, pod) {
				enqueue = true
				return nil
			}
			return fmt.Errorf("%w: active %s transaction %s", errSandboxCrashRecoveryBlocked, activeTxn.Kind, activeTxn.ID)
		}
		if record.DesiredState == SandboxDesiredStatePaused {
			return nil
		}
		if err := beginHealthRecoveryTxn(lockCtx, tx, record, pod); err != nil {
			return err
		}
		enqueue = true
		return nil
	})
	if errors.Is(err, ErrSandboxRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !enqueue {
		return nil
	}
	if s.logger != nil {
		s.logger.Error("Queued unhealthy sandbox runtime reconstruction",
			zap.String("sandboxID", sandboxID),
			zap.String("namespace", pod.Namespace),
			zap.String("pod", pod.Name),
			zap.String("podUID", string(pod.UID)),
			zap.Int64("runtimeGeneration", runtimeGenerationFromPod(pod)),
			zap.String("reason", liveness.Reason),
			zap.String("message", liveness.Message),
			zap.Time("unhealthySince", liveness.LastTransitionTime.Time),
		)
	}
	s.enqueueSandboxRecovery(sandboxID)
	return nil
}

func beginCrashRecoveryTxn(ctx context.Context, tx SandboxStoreTx, record *SandboxRecord, pod *corev1.Pod) error {
	if tx == nil || record == nil || pod == nil {
		return nil
	}
	_, terminated := terminatedProcdContainer(pod)
	if terminated == nil && !sandboxRuntimePodTerminal(pod) {
		return fmt.Errorf("pod %s/%s has no terminated procd container to recover", pod.Namespace, pod.Name)
	}
	if !sandboxRecordReferencesPod(record, pod) {
		return fmt.Errorf("sandbox runtime identity changed before crash recovery")
	}
	return tx.BeginLifecycleTxn(ctx, &SandboxLifecycleTxn{
		ID:               uuid.NewString(),
		SandboxID:        record.ID,
		Kind:             SandboxLifecycleKindPause,
		Phase:            SandboxLifecyclePhasePreparing,
		Source:           SandboxLifecycleSourceCrash,
		Cancelable:       false,
		FromGeneration:   runtimeGenerationFromPod(pod),
		FromPodNamespace: pod.Namespace,
		FromPodName:      pod.Name,
	})
}

func beginHealthRecoveryTxn(ctx context.Context, tx SandboxStoreTx, record *SandboxRecord, pod *corev1.Pod) error {
	if tx == nil || record == nil || pod == nil {
		return nil
	}
	condition := sandboxRuntimeLivenessCondition(pod)
	if condition == nil || condition.Status != corev1.ConditionFalse {
		return fmt.Errorf("pod %s/%s has no failed liveness condition to recover", pod.Namespace, pod.Name)
	}
	if !sandboxRecordReferencesPod(record, pod) {
		return fmt.Errorf("sandbox runtime identity changed before health recovery")
	}
	return tx.BeginLifecycleTxn(ctx, &SandboxLifecycleTxn{
		ID:               uuid.NewString(),
		SandboxID:        record.ID,
		Kind:             SandboxLifecycleKindPause,
		Phase:            SandboxLifecyclePhasePreparing,
		Source:           SandboxLifecycleSourceHealth,
		Cancelable:       false,
		FromGeneration:   runtimeGenerationFromPod(pod),
		FromPodNamespace: pod.Namespace,
		FromPodName:      pod.Name,
	})
}

func sandboxRecordReferencesPod(record *SandboxRecord, pod *corev1.Pod) bool {
	if record == nil || pod == nil {
		return false
	}
	if strings.TrimSpace(record.CurrentPodNamespace) != strings.TrimSpace(pod.Namespace) ||
		strings.TrimSpace(record.CurrentPodName) != strings.TrimSpace(pod.Name) {
		return false
	}
	return record.RuntimeGeneration == runtimeGenerationFromPod(pod)
}

func crashRecoveryTxnReferencesPod(txn *SandboxLifecycleTxn, pod *corev1.Pod) bool {
	return runtimeRecoveryTxnReferencesPod(txn, pod, SandboxLifecycleSourceCrash)
}

func healthRecoveryTxnReferencesPod(txn *SandboxLifecycleTxn, pod *corev1.Pod) bool {
	return runtimeRecoveryTxnReferencesPod(txn, pod, SandboxLifecycleSourceHealth)
}

func runtimeRecoveryTxnReferencesPod(txn *SandboxLifecycleTxn, pod *corev1.Pod, source string) bool {
	if txn == nil || pod == nil || txn.Kind != SandboxLifecycleKindPause || txn.Source != source {
		return false
	}
	return txn.FromGeneration == runtimeGenerationFromPod(pod) &&
		strings.TrimSpace(txn.FromPodNamespace) == strings.TrimSpace(pod.Namespace) &&
		strings.TrimSpace(txn.FromPodName) == strings.TrimSpace(pod.Name)
}

func sandboxCrashRecoveryPodEligible(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || !controller.IsClaimedSandboxPod(pod) {
		return false
	}
	return strings.TrimSpace(sandboxIDFromPod(pod)) != ""
}

func terminatedProcdContainer(pod *corev1.Pod) (*corev1.ContainerStatus, *corev1.ContainerStateTerminated) {
	status := procdContainerStatus(pod)
	if status == nil || status.State.Terminated == nil {
		return status, nil
	}
	return status, status.State.Terminated
}

func sandboxRuntimePodTerminal(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	return pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
}

func (s *SandboxService) deleteRecoveredRuntimePod(ctx context.Context, pod *corev1.Pod) error {
	if s == nil || pod == nil || s.k8sClient == nil {
		return nil
	}
	current, err := s.ensureSandboxDeletionFinalizer(ctx, pod)
	if err != nil {
		return fmt.Errorf("ensure sandbox cleanup finalizer before deleting recovered runtime: %w", err)
	}
	if current == nil {
		current = pod
	}
	err = s.k8sClient.CoreV1().Pods(current.Namespace).Delete(ctx, current.Name, metav1.DeleteOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete recovered runtime pod: %w", err)
	}
	return nil
}
