package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func runtimeReconcileConflict(sandboxID, message string) error {
	return fmt.Errorf("sandbox %s runtime reconciliation conflict: %s", sandboxID, message)
}

// ReconcileSandboxRuntime repairs one durable-runtime mismatch. Absence is
// confirmed through the Kubernetes API before any lifecycle state is changed;
// an unavailable API is therefore never interpreted as a missing runtime.
func (s *SandboxService) ReconcileSandboxRuntime(ctx context.Context, sandboxID string) error {
	if s == nil || s.sandboxStore == nil {
		return nil
	}
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return nil
	}
	pods, err := s.listSandboxRuntimePodsStrong(ctx, sandboxID)
	if err != nil {
		return fmt.Errorf("confirm sandbox runtime state: %w", err)
	}

	var record *SandboxRecord
	var stalePods []*corev1.Pod
	var finishTermination bool
	var enqueuePause bool
	var enqueueRecovery bool
	var cleanupLostRuntime bool
	var resumeTxn *SandboxLifecycleTxn
	err = s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx SandboxStoreTx, locked *SandboxRecord) error {
		if locked == nil || locked.DesiredState == SandboxDesiredStateDeleted || !locked.DeletedAt.IsZero() || !s.sandboxRecordBelongsToCluster(locked) {
			return nil
		}
		record = cloneSandboxRecordForLifecycle(locked)
		activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, sandboxID)
		if err != nil {
			return err
		}

		if locked.DesiredState == SandboxDesiredStateTerminating || sandboxHardExpired(locked.HardExpiresAt, s.now()) {
			if activeTxn != nil {
				if err := tx.AbortLifecycleTxn(lockCtx, activeTxn.ID, "sandbox termination requested"); err != nil {
					return err
				}
			}
			if locked.DesiredState != SandboxDesiredStateTerminating {
				if err := tx.MarkRuntimeTerminating(lockCtx, sandboxID); err != nil {
					return err
				}
				record.DesiredState = SandboxDesiredStateTerminating
			}
			finishTermination = true
			return nil
		}

		if activeTxn != nil {
			switch activeTxn.Kind {
			case SandboxLifecycleKindPause:
				if sandboxLifecycleSourceReconstructsRuntime(activeTxn.Source) {
					if activeTxn.Source == SandboxLifecycleSourceLost {
						cleanupLostRuntime = true
						for _, pod := range pods {
							generation := runtimeGenerationFromPod(pod)
							switch {
							case generation > activeTxn.FromGeneration:
								return runtimeReconcileConflict(sandboxID, "a newer runtime exists during lost-runtime recovery")
							case generation == activeTxn.FromGeneration:
								if activeTxn.FromPodName != "" &&
									(pod.Name != activeTxn.FromPodName || pod.Namespace != activeTxn.FromPodNamespace) {
									return runtimeReconcileConflict(sandboxID, "an unexpected runtime owns the lost generation")
								}
								cleanupLostRuntime = false
							default:
								stalePods = append(stalePods, pod)
							}
						}
					}
					enqueueRecovery = true
				} else {
					enqueuePause = true
				}
				return nil
			case SandboxLifecycleKindResume:
				resumeTxn = cloneSandboxLifecycleTxn(activeTxn)
				return nil
			case SandboxLifecycleKindFork, SandboxLifecycleKindSnapshot:
				if sandboxSourceRuntimeTxnPodAvailable(activeTxn, pods) ||
					!sandboxLifecycleRootFSSourceCheckpointTxnStale(activeTxn, s.now()) {
					return nil
				}
				if err := tx.AbortLifecycleTxn(lockCtx, activeTxn.ID, "source runtime disappeared during lifecycle transaction"); err != nil {
					return err
				}
			default:
				return nil
			}
		}

		if locked.DesiredState == SandboxDesiredStatePaused {
			stalePods = append(stalePods, pods...)
			return nil
		}

		activePods, deletingPods := splitSandboxRuntimePods(pods)
		matching := matchingSandboxRuntimePods(locked, activePods)
		if len(matching) > 1 {
			return runtimeReconcileConflict(sandboxID, "multiple active pods match the durable runtime identity")
		}
		if len(matching) == 1 {
			for _, pod := range activePods {
				if pod.UID == matching[0].UID {
					continue
				}
				if runtimeGenerationFromPod(pod) >= locked.RuntimeGeneration {
					return runtimeReconcileConflict(sandboxID, "multiple unfenced active runtime pods")
				}
				stalePods = append(stalePods, pod)
			}
			return nil
		}

		if len(activePods) == 1 && runtimeGenerationFromPod(activePods[0]) == locked.RuntimeGeneration {
			pod := activePods[0]
			return tx.SaveRuntime(
				lockCtx,
				sandboxID,
				pod.Namespace,
				pod.Name,
				locked.RuntimeGeneration,
				parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt),
				parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt),
				sandboxRuntimeMetadataFromPod(pod),
			)
		}

		for _, pod := range activePods {
			generation := runtimeGenerationFromPod(pod)
			if generation >= locked.RuntimeGeneration {
				return runtimeReconcileConflict(sandboxID, fmt.Sprintf("unowned runtime generation %d is active", generation))
			}
			stalePods = append(stalePods, pod)
		}
		for _, pod := range deletingPods {
			if runtimeGenerationFromPod(pod) > locked.RuntimeGeneration {
				return runtimeReconcileConflict(sandboxID, "a newer runtime is still deleting")
			}
		}

		if err := beginLostRuntimeRecoveryTxn(lockCtx, tx, locked); err != nil {
			return err
		}
		enqueueRecovery = true
		cleanupLostRuntime = len(pods) == 0
		return nil
	})
	if errors.Is(err, ErrSandboxRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	if len(stalePods) > 0 {
		if err := s.deleteSandboxRuntimePods(ctx, stalePods); err != nil {
			return err
		}
	}
	if finishTermination && record != nil {
		return s.finishTerminatingSandbox(ctx, record, pods)
	}
	if resumeTxn != nil && record != nil {
		return s.recoverStaleResumeTransaction(ctx, record, resumeTxn, pods)
	}
	if cleanupLostRuntime && record != nil {
		if err := s.cleanupDeletedSandbox(ctx, sandboxLifecycleInfoFromRecord(record), true, false); err != nil {
			return fmt.Errorf("cleanup missing sandbox runtime: %w", err)
		}
	}
	if enqueueRecovery {
		if s.logger != nil {
			s.logger.Warn("Queued missing sandbox runtime reconstruction",
				zap.String("sandboxID", sandboxID),
				zap.Int64("runtimeGeneration", record.RuntimeGeneration),
			)
		}
		s.enqueueSandboxRecovery(sandboxID)
	} else if enqueuePause {
		s.enqueueSandboxPause(sandboxID)
	}
	return nil
}

func (s *SandboxService) recoverStaleResumeTransaction(ctx context.Context, record *SandboxRecord, txn *SandboxLifecycleTxn, pods []*corev1.Pod) error {
	if s == nil || record == nil || txn == nil || txn.Kind != SandboxLifecycleKindResume || txn.UpdatedAt.IsZero() {
		return nil
	}
	staleAfter := 2 * time.Minute
	if s.config.RuntimeReadyTimeout > 0 {
		staleAfter = max(staleAfter, s.config.RuntimeReadyTimeout+30*time.Second)
	}
	if s.now().Sub(txn.UpdatedAt) < staleAfter {
		return nil
	}

	var target *corev1.Pod
	for _, pod := range pods {
		if pod == nil || runtimeGenerationFromPod(pod) != txn.ToGeneration {
			continue
		}
		if txn.ToPodName != "" && (pod.Name != txn.ToPodName || pod.Namespace != txn.ToPodNamespace) {
			continue
		}
		if target != nil {
			return runtimeReconcileConflict(record.ID, "multiple pods own the stale resume generation")
		}
		target = pod
	}

	if target == nil || target.DeletionTimestamp != nil {
		if err := s.abortLifecycleTxn(ctx, record.ID, txn.ID, "stale resume runtime is missing"); err != nil {
			return err
		}
		_, err := s.ResumePausedSandboxRuntime(ctx, record.ID)
		return err
	}
	if txn.ToPodName == "" {
		if err := s.recordResumeLifecycleRuntime(ctx, record.ID, txn, target); err != nil {
			return err
		}
		txn.ToPodNamespace = target.Namespace
		txn.ToPodName = target.Name
	}
	restored, err := s.finishRestoredSandboxRuntime(ctx, target, record, "recovery")
	if err != nil {
		return err
	}
	if err := s.commitResumedSandboxRuntime(ctx, restored, record, txn); err != nil {
		return err
	}
	s.enqueueHotClaimReservation(restored)
	if s.logger != nil {
		s.logger.Info("Recovered stale sandbox resume transaction",
			zap.String("sandboxID", record.ID),
			zap.String("pod", restored.Name),
			zap.Int64("runtimeGeneration", txn.ToGeneration),
		)
	}
	return nil
}

func (s *SandboxService) listSandboxRuntimePodsStrong(ctx context.Context, sandboxID string) ([]*corev1.Pod, error) {
	if s == nil || s.k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client is not configured")
	}
	selector := labels.SelectorFromSet(map[string]string{controller.LabelSandboxID: sandboxID}).String()
	list, err := s.k8sClient.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	pods := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pod := &list.Items[i]
		if sandboxIDFromPod(pod) != sandboxID || !sandboxRuntimePodOwnedBySandbox(pod) {
			continue
		}
		pods = append(pods, pod.DeepCopy())
	}
	sort.Slice(pods, func(i, j int) bool {
		leftGeneration := runtimeGenerationFromPod(pods[i])
		rightGeneration := runtimeGenerationFromPod(pods[j])
		if leftGeneration != rightGeneration {
			return leftGeneration > rightGeneration
		}
		if pods[i].Namespace != pods[j].Namespace {
			return pods[i].Namespace < pods[j].Namespace
		}
		return pods[i].Name < pods[j].Name
	})
	return pods, nil
}

func (s *SandboxService) sandboxRecordBelongsToCluster(record *SandboxRecord) bool {
	if s == nil || record == nil {
		return false
	}
	clusterID := strings.TrimSpace(s.config.ClusterID)
	return clusterID == "" || strings.TrimSpace(record.ClusterID) == clusterID
}

func splitSandboxRuntimePods(pods []*corev1.Pod) (active, deleting []*corev1.Pod) {
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		if pod.DeletionTimestamp != nil {
			deleting = append(deleting, pod)
		} else {
			active = append(active, pod)
		}
	}
	return active, deleting
}

func matchingSandboxRuntimePods(record *SandboxRecord, pods []*corev1.Pod) []*corev1.Pod {
	matching := make([]*corev1.Pod, 0, 1)
	for _, pod := range pods {
		if sandboxRecordReferencesPod(record, pod) {
			matching = append(matching, pod)
		}
	}
	return matching
}

func sandboxSourceRuntimeTxnPodAvailable(txn *SandboxLifecycleTxn, pods []*corev1.Pod) bool {
	if txn == nil {
		return false
	}
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil || runtimeGenerationFromPod(pod) != txn.FromGeneration {
			continue
		}
		if txn.FromPodName != "" && (pod.Name != txn.FromPodName || pod.Namespace != txn.FromPodNamespace) {
			continue
		}
		return true
	}
	return false
}

func beginLostRuntimeRecoveryTxn(ctx context.Context, tx SandboxStoreTx, record *SandboxRecord) error {
	if tx == nil || record == nil {
		return nil
	}
	return tx.BeginLifecycleTxn(ctx, &SandboxLifecycleTxn{
		ID:               uuid.NewString(),
		SandboxID:        record.ID,
		Kind:             SandboxLifecycleKindPause,
		Phase:            SandboxLifecyclePhasePreparing,
		Source:           SandboxLifecycleSourceLost,
		Cancelable:       false,
		FromGeneration:   record.RuntimeGeneration,
		FromPodNamespace: record.CurrentPodNamespace,
		FromPodName:      record.CurrentPodName,
	})
}

func (s *SandboxService) deleteSandboxRuntimePods(ctx context.Context, pods []*corev1.Pod) error {
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		current, err := s.ensureSandboxDeletionFinalizer(ctx, pod)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("ensure runtime cleanup finalizer on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		if current == nil {
			current = pod
		}
		err = s.k8sClient.CoreV1().Pods(current.Namespace).Delete(ctx, current.Name, metav1.DeleteOptions{})
		if err != nil && !k8serrors.IsNotFound(err) {
			return fmt.Errorf("delete sandbox runtime pod %s/%s: %w", current.Namespace, current.Name, err)
		}
	}
	return nil
}

func (s *SandboxService) finishTerminatingSandbox(ctx context.Context, record *SandboxRecord, pods []*corev1.Pod) error {
	if s == nil || record == nil || s.sandboxStore == nil {
		return nil
	}
	if len(pods) == 0 {
		if err := s.cleanupDeletedSandbox(ctx, sandboxLifecycleInfoFromRecord(record), false, false); err != nil {
			return fmt.Errorf("cleanup terminating sandbox without runtime: %w", err)
		}
	} else if err := s.deleteSandboxRuntimePods(ctx, pods); err != nil {
		return err
	}
	return s.sandboxStore.MarkSandboxDeleted(ctx, record.ID, s.now())
}
