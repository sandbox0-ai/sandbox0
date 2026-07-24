package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

const (
	hotClaimReservationRecoveryGrace  = 10 * time.Second
	hotClaimReservationAbandonAfter   = 30 * time.Second
	hotClaimReservationReconcileEvery = 2 * time.Second
	hotClaimReservationReconcileLimit = 500
)

// HotClaimPodMetadata is the durable desired identity of a claimed warm-pool pod.
type HotClaimPodMetadata struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	Finalizers  []string          `json:"finalizers"`
}

// HotClaimReservation prevents two manager replicas from claiming the same
// warm-pool pod before its active identity is visible through Kubernetes.
type HotClaimReservation struct {
	SandboxID   string
	TeamID      string
	ClusterID   string
	Namespace   string
	PodName     string
	PodUID      types.UID
	Metadata    HotClaimPodMetadata
	CommittedAt time.Time
	CreatedAt   time.Time
}

// HotClaimReservationStore persists cross-replica warm-pool reservations.
type HotClaimReservationStore interface {
	TryReserveHotClaim(ctx context.Context, reservation *HotClaimReservation) (bool, error)
	CommitHotClaim(ctx context.Context, record *SandboxRecord, podUID types.UID, metadata HotClaimPodMetadata) error
	GetHotClaimReservation(ctx context.Context, sandboxID string) (*HotClaimReservation, error)
	ListHotClaimReservations(ctx context.Context, clusterID string, createdBefore time.Time, limit int) ([]*HotClaimReservation, error)
	ReleaseHotClaimReservation(ctx context.Context, sandboxID string, podUID types.UID) error
}

func hotClaimPodMetadata(pod *corev1.Pod) HotClaimPodMetadata {
	if pod == nil {
		return HotClaimPodMetadata{}
	}
	return HotClaimPodMetadata{
		Labels:      cloneHotClaimStringMap(pod.Labels),
		Annotations: cloneHotClaimStringMap(pod.Annotations),
		Finalizers:  append([]string(nil), pod.Finalizers...),
	}
}

func cloneHotClaimStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func materializeReservedHotClaimPod(pod *corev1.Pod, reservation *HotClaimReservation) (*corev1.Pod, error) {
	if pod == nil || reservation == nil {
		return nil, fmt.Errorf("pod and hot claim reservation are required")
	}
	if pod.Namespace != reservation.Namespace || pod.Name != reservation.PodName {
		return nil, fmt.Errorf("hot claim reservation pod identity does not match")
	}
	if reservation.PodUID != "" && pod.UID != reservation.PodUID {
		return nil, fmt.Errorf("hot claim reservation pod UID does not match")
	}
	if reservation.CommittedAt.IsZero() {
		return nil, fmt.Errorf("hot claim reservation is not committed")
	}

	materialized := pod.DeepCopy()
	materialized.Labels = cloneHotClaimStringMap(reservation.Metadata.Labels)
	materialized.Annotations = cloneHotClaimStringMap(reservation.Metadata.Annotations)
	materialized.Finalizers = mergeFinalizers(pod.Finalizers, reservation.Metadata.Finalizers)
	materialized.OwnerReferences = nil
	return materialized, nil
}

func mergeFinalizers(current, desired []string) []string {
	if len(current) == 0 && len(desired) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(current)+len(desired))
	merged := make([]string, 0, len(current)+len(desired))
	for _, finalizer := range append(append([]string(nil), current...), desired...) {
		if _, ok := seen[finalizer]; ok {
			continue
		}
		seen[finalizer] = struct{}{}
		merged = append(merged, finalizer)
	}
	return merged
}

func (s *SandboxService) reservedHotClaimPod(ctx context.Context, sandboxID string) (*corev1.Pod, error) {
	if s == nil || s.hotClaimReservationStore == nil || s.k8sClient == nil {
		return nil, nil
	}
	reservation, err := s.hotClaimReservationStore.GetHotClaimReservation(ctx, sandboxID)
	if err != nil || reservation == nil || reservation.CommittedAt.IsZero() {
		return nil, err
	}
	if reservation.ClusterID != s.hotClaimClusterID() {
		return nil, nil
	}
	pod, err := s.k8sClient.CoreV1().Pods(reservation.Namespace).Get(ctx, reservation.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return materializeReservedHotClaimPod(pod, reservation)
}

func (s *SandboxService) finalizeReservedHotClaim(ctx context.Context, sandboxID string) error {
	if s == nil || s.hotClaimReservationStore == nil || s.k8sClient == nil {
		return nil
	}
	reservation, err := s.hotClaimReservationStore.GetHotClaimReservation(ctx, sandboxID)
	if err != nil || reservation == nil {
		return err
	}
	if reservation.ClusterID != s.hotClaimClusterID() {
		return nil
	}
	if reservation.CommittedAt.IsZero() {
		return fmt.Errorf("hot claim reservation %s is not committed", sandboxID)
	}

	return retry.OnError(retry.DefaultBackoff, func(err error) bool {
		return k8serrors.IsConflict(err) || k8serrors.IsInvalid(err)
	}, func() error {
		pod, getErr := s.k8sClient.CoreV1().Pods(reservation.Namespace).Get(ctx, reservation.PodName, metav1.GetOptions{})
		if getErr != nil {
			if k8serrors.IsNotFound(getErr) {
				return s.hotClaimReservationStore.ReleaseHotClaimReservation(ctx, reservation.SandboxID, reservation.PodUID)
			}
			return getErr
		}
		if pod.UID != reservation.PodUID {
			return s.hotClaimReservationStore.ReleaseHotClaimReservation(ctx, reservation.SandboxID, reservation.PodUID)
		}
		if pod.Labels[controller.LabelPoolType] == controller.PoolTypeActive &&
			sandboxIDFromPod(pod) == reservation.SandboxID {
			return s.hotClaimReservationStore.ReleaseHotClaimReservation(ctx, reservation.SandboxID, reservation.PodUID)
		}
		if existing := strings.TrimSpace(pod.Labels[controller.LabelSandboxID]); existing != "" && existing != reservation.SandboxID {
			return fmt.Errorf("reserved pod is already owned by sandbox %s", existing)
		}
		if existing := strings.TrimSpace(pod.Annotations[controller.AnnotationSandboxID]); existing != "" && existing != reservation.SandboxID {
			return fmt.Errorf("reserved pod is already owned by sandbox %s", existing)
		}

		desired, materializeErr := materializeReservedHotClaimPod(pod, reservation)
		if materializeErr != nil {
			return materializeErr
		}
		if _, patchErr := s.patchClaimedPodMetadata(ctx, desired); patchErr != nil {
			return patchErr
		}
		return s.hotClaimReservationStore.ReleaseHotClaimReservation(ctx, reservation.SandboxID, reservation.PodUID)
	})
}

func (s *SandboxService) releaseReservedHotClaim(ctx context.Context, pending *pendingHotClaimFinalization) error {
	if s == nil || s.hotClaimReservationStore == nil || pending == nil {
		return nil
	}
	return s.hotClaimReservationStore.ReleaseHotClaimReservation(ctx, pending.sandboxID, pending.podUID)
}

func (s *SandboxService) activateUncommittedHotClaim(ctx context.Context, pending *pendingHotClaimFinalization) error {
	if s == nil || s.k8sClient == nil || pending == nil || !pending.durable {
		return nil
	}
	return retry.OnError(retry.DefaultBackoff, func(err error) bool {
		return k8serrors.IsConflict(err) || k8serrors.IsInvalid(err)
	}, func() error {
		pod, err := s.k8sClient.CoreV1().Pods(pending.namespace).Get(ctx, pending.podName, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if pod.UID != pending.podUID {
			return nil
		}
		reservation := &HotClaimReservation{
			SandboxID:   pending.sandboxID,
			Namespace:   pending.namespace,
			PodName:     pending.podName,
			PodUID:      pending.podUID,
			Metadata:    pending.metadata,
			CommittedAt: time.Now(),
		}
		desired, err := materializeReservedHotClaimPod(pod, reservation)
		if err != nil {
			return err
		}
		_, err = s.patchClaimedPodMetadata(ctx, desired)
		return err
	})
}

// StartHotClaimReservationReconciler recovers committed claims after manager
// restarts and discards abandoned pre-commit reservations.
func (s *SandboxService) StartHotClaimReservationReconciler(ctx context.Context) {
	if s == nil || s.hotClaimReservationStore == nil {
		return
	}
	ticker := time.NewTicker(hotClaimReservationReconcileEvery)
	defer ticker.Stop()
	for {
		s.reconcileHotClaimReservations(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *SandboxService) reconcileHotClaimReservations(ctx context.Context) {
	cutoff := time.Now().Add(-hotClaimReservationRecoveryGrace)
	reservations, err := s.hotClaimReservationStore.ListHotClaimReservations(ctx, s.hotClaimClusterID(), cutoff, hotClaimReservationReconcileLimit)
	if err != nil {
		s.hotClaimLogger().Warn("Failed to list hot claim reservations", zap.Error(err))
		return
	}
	for _, reservation := range reservations {
		if reservation == nil {
			continue
		}
		if !reservation.CommittedAt.IsZero() {
			if err := s.finalizeReservedHotClaim(ctx, reservation.SandboxID); err != nil && !errors.Is(err, context.Canceled) {
				s.hotClaimLogger().Warn("Failed to recover committed hot claim",
					zap.String("sandboxID", reservation.SandboxID),
					zap.String("pod", reservation.PodName),
					zap.Error(err),
				)
			}
			continue
		}
		if time.Since(reservation.CreatedAt) < hotClaimReservationAbandonAfter {
			continue
		}
		if err := s.discardAbandonedHotClaim(ctx, reservation); err != nil && !errors.Is(err, context.Canceled) {
			s.hotClaimLogger().Warn("Failed to discard abandoned hot claim",
				zap.String("sandboxID", reservation.SandboxID),
				zap.String("pod", reservation.PodName),
				zap.Error(err),
			)
		}
	}
}

func (s *SandboxService) discardAbandonedHotClaim(ctx context.Context, reservation *HotClaimReservation) error {
	pod, err := s.k8sClient.CoreV1().Pods(reservation.Namespace).Get(ctx, reservation.PodName, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	if err == nil && pod.UID == reservation.PodUID {
		uid := reservation.PodUID
		if err := s.k8sClient.CoreV1().Pods(reservation.Namespace).Delete(ctx, reservation.PodName, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid},
		}); err != nil && !k8serrors.IsNotFound(err) {
			return err
		}
	}
	return s.hotClaimReservationStore.ReleaseHotClaimReservation(ctx, reservation.SandboxID, reservation.PodUID)
}

func (s *SandboxService) hotClaimLogger() *zap.Logger {
	if s != nil && s.logger != nil {
		return s.logger
	}
	return zap.NewNop()
}

func (s *SandboxService) hotClaimClusterID() string {
	if s == nil || strings.TrimSpace(s.config.ClusterID) == "" {
		return naming.DefaultClusterID
	}
	return strings.TrimSpace(s.config.ClusterID)
}
