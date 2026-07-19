package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
)

var warmPoolReplicaCommitmentBackoff = wait.Backoff{
	Steps:    32,
	Duration: 2 * time.Millisecond,
	Factor:   1.3,
	Jitter:   0.2,
	Cap:      100 * time.Millisecond,
}

type warmPoolReplicaCommitment struct {
	namespace      string
	name           string
	uid            string
	operationID    string
	replicasAfter  int32
	template       *v1alpha1.SandboxTemplate
	source         teamquota.Owner
	observedSource teamquota.Values
}

func warmPoolReplicaCommitmentForIdlePod(
	template *v1alpha1.SandboxTemplate,
	pod *corev1.Pod,
	operationID string,
) (*warmPoolReplicaCommitment, error) {
	if template == nil || pod == nil {
		return nil, fmt.Errorf("warm-pool template and idle pod are required")
	}
	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil ||
		controllerRef.Kind != "ReplicaSet" ||
		!strings.HasPrefix(controllerRef.APIVersion, appsv1.SchemeGroupVersion.Group) {
		return nil, fmt.Errorf("idle pod %s/%s has no ReplicaSet controller", pod.Namespace, pod.Name)
	}
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return nil, fmt.Errorf("warm-pool transfer operation ID is required")
	}
	source, ok := teamWarmPoolOwnerForClaim(template)
	if !ok {
		return nil, fmt.Errorf("team warm-pool quota owner is required")
	}
	return &warmPoolReplicaCommitment{
		namespace:   pod.Namespace,
		name:        controllerRef.Name,
		uid:         string(controllerRef.UID),
		operationID: operationID,
		template:    template,
		source:      source,
	}, nil
}

// releaseWarmPoolReplicaCommitment atomically records and applies exactly one
// ReplicaSet decrement for a prepared hot-claim transfer. The per-operation
// marker makes overlapping claims and crash recovery idempotent.
func (s *SandboxService) releaseWarmPoolReplicaCommitment(
	ctx context.Context,
	commitment *warmPoolReplicaCommitment,
) error {
	if s == nil || s.k8sClient == nil || commitment == nil {
		return fmt.Errorf("kubernetes client and warm-pool commitment are required")
	}
	err := retry.RetryOnConflict(warmPoolReplicaCommitmentBackoff, func() error {
		rs, err := s.k8sClient.AppsV1().ReplicaSets(commitment.namespace).Get(ctx, commitment.name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if commitment.uid != "" && string(rs.UID) != "" && string(rs.UID) != commitment.uid {
			return fmt.Errorf("ReplicaSet %s/%s identity changed", commitment.namespace, commitment.name)
		}
		markers, err := warmPoolTransferMarkers(rs)
		if err != nil {
			return err
		}
		if replicasAfter, ok := markers[commitment.operationID]; ok {
			commitment.replicasAfter = replicasAfter
			return nil
		}
		current := int32(0)
		if rs.Spec.Replicas != nil {
			current = *rs.Spec.Replicas
		}
		if current < 0 {
			return fmt.Errorf("ReplicaSet %s/%s has a negative replica commitment", commitment.namespace, commitment.name)
		}
		next := max(current-1, 0)
		updated := rs.DeepCopy()
		updated.Spec.Replicas = &next
		markers[commitment.operationID] = next
		if err := setWarmPoolTransferMarkers(updated, markers); err != nil {
			return err
		}
		_, err = s.k8sClient.AppsV1().ReplicaSets(commitment.namespace).
			Update(ctx, updated, metav1.UpdateOptions{})
		if err == nil {
			commitment.replicasAfter = next
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("release warm-pool ReplicaSet commitment: %w", err)
	}
	observed, err := s.waitForWarmPoolReplicaCommitment(ctx, commitment)
	if err != nil {
		return fmt.Errorf("wait for warm-pool ReplicaSet commitment: %w", err)
	}
	commitment.observedSource = observed
	return nil
}

// clearWarmPoolReplicaCommitment removes a completed transfer marker without
// changing the ReplicaSet commitment.
func (s *SandboxService) clearWarmPoolReplicaCommitment(
	ctx context.Context,
	commitment *warmPoolReplicaCommitment,
) error {
	if s == nil || s.k8sClient == nil || commitment == nil {
		return nil
	}
	return retry.RetryOnConflict(warmPoolReplicaCommitmentBackoff, func() error {
		rs, err := s.k8sClient.AppsV1().ReplicaSets(commitment.namespace).
			Get(ctx, commitment.name, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if commitment.uid != "" && string(rs.UID) != "" && string(rs.UID) != commitment.uid {
			return nil
		}
		markers, err := warmPoolTransferMarkers(rs)
		if err != nil {
			return err
		}
		if _, ok := markers[commitment.operationID]; !ok {
			return nil
		}
		updated := rs.DeepCopy()
		delete(markers, commitment.operationID)
		if err := setWarmPoolTransferMarkers(updated, markers); err != nil {
			return err
		}
		_, err = s.k8sClient.AppsV1().ReplicaSets(commitment.namespace).
			Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
}

func (s *SandboxService) warmPoolReplicaCommitmentReleased(
	ctx context.Context,
	commitment *warmPoolReplicaCommitment,
) (bool, error) {
	if s == nil || s.k8sClient == nil || commitment == nil {
		return false, nil
	}
	rs, err := s.k8sClient.AppsV1().ReplicaSets(commitment.namespace).
		Get(ctx, commitment.name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if commitment.uid != "" && string(rs.UID) != "" && string(rs.UID) != commitment.uid {
		return false, nil
	}
	markers, err := warmPoolTransferMarkers(rs)
	if err != nil {
		return false, err
	}
	replicasAfter, ok := markers[commitment.operationID]
	if ok {
		commitment.replicasAfter = replicasAfter
	}
	return ok, nil
}

func (s *SandboxService) waitForWarmPoolReplicaCommitment(
	ctx context.Context,
	commitment *warmPoolReplicaCommitment,
) (teamquota.Values, error) {
	if commitment == nil || commitment.template == nil {
		return nil, fmt.Errorf("warm-pool commitment template is required")
	}
	ticker := time.NewTicker(sandboxLifecycleWaitInterval)
	defer ticker.Stop()
	for {
		rs, err := s.k8sClient.AppsV1().ReplicaSets(commitment.namespace).
			Get(ctx, commitment.name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if commitment.uid != "" && string(rs.UID) != "" && string(rs.UID) != commitment.uid {
			return nil, fmt.Errorf("ReplicaSet %s/%s identity changed", commitment.namespace, commitment.name)
		}
		pods, err := s.k8sClient.CoreV1().Pods(commitment.namespace).
			List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		observed, err := observedTeamWarmPoolTarget(
			ctx,
			s.k8sClient,
			commitment.template,
			rs,
			pods.Items,
			commitment.source,
		)
		if err != nil {
			return nil, err
		}
		if observed[teamquota.KeySandboxRuntimeCount] <= int64(commitment.replicasAfter) {
			return observed, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func warmPoolTransferMarkers(rs *appsv1.ReplicaSet) (map[string]int32, error) {
	markers := make(map[string]int32)
	if rs == nil || rs.Annotations == nil {
		return markers, nil
	}
	raw := strings.TrimSpace(rs.Annotations[controller.AnnotationTeamQuotaWarmPoolTransfers])
	if raw == "" {
		return markers, nil
	}
	if err := json.Unmarshal([]byte(raw), &markers); err != nil {
		return nil, fmt.Errorf(
			"decode ReplicaSet %s/%s warm-pool transfer markers: %w",
			rs.Namespace,
			rs.Name,
			err,
		)
	}
	for operationID, replicasAfter := range markers {
		if strings.TrimSpace(operationID) == "" {
			return nil, fmt.Errorf(
				"decode ReplicaSet %s/%s warm-pool transfer markers: operation ID is required",
				rs.Namespace,
				rs.Name,
			)
		}
		if replicasAfter < 0 {
			return nil, fmt.Errorf(
				"decode ReplicaSet %s/%s warm-pool transfer markers: operation %s has negative replicas",
				rs.Namespace,
				rs.Name,
				operationID,
			)
		}
	}
	return markers, nil
}

func setWarmPoolTransferMarkers(
	rs *appsv1.ReplicaSet,
	markers map[string]int32,
) error {
	if rs == nil {
		return fmt.Errorf("ReplicaSet is required")
	}
	if rs.Annotations == nil {
		rs.Annotations = make(map[string]string)
	}
	if len(markers) == 0 {
		delete(rs.Annotations, controller.AnnotationTeamQuotaWarmPoolTransfers)
		return nil
	}
	raw, err := json.Marshal(markers)
	if err != nil {
		return fmt.Errorf("encode warm-pool transfer markers: %w", err)
	}
	rs.Annotations[controller.AnnotationTeamQuotaWarmPoolTransfers] = string(raw)
	return nil
}

func (s *SandboxService) logWarmPoolCommitmentMarkerCleanupFailure(
	pod *corev1.Pod,
	err error,
) {
	if s == nil || s.logger == nil || err == nil {
		return
	}
	namespace, name := "", ""
	if pod != nil {
		namespace, name = pod.Namespace, pod.Name
	}
	s.logger.Error("Failed to clear warm-pool transfer marker",
		zap.String("namespace", namespace),
		zap.String("pod", name),
		zap.Error(err),
	)
}

func isTeamOwnedWarmPoolTemplate(template *v1alpha1.SandboxTemplate, teamID string) bool {
	if template == nil {
		return false
	}
	owner, ok := teamWarmPoolOwnerForClaim(template)
	return ok && owner.TeamID == strings.TrimSpace(teamID)
}
