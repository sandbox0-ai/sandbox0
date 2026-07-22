package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func (s *SandboxService) enforceActiveSandboxQuota(ctx context.Context, teamID string) error {
	teamID = strings.TrimSpace(teamID)
	if s == nil || s.quotaStore == nil || teamID == "" {
		return nil
	}
	limit, err := s.quotaStore.GetLimit(ctx, teamID, quota.DimensionActiveSandboxes)
	if err != nil {
		return fmt.Errorf("load active sandbox quota: %w", err)
	}
	if limit == nil {
		return nil
	}
	current, err := s.currentQuotaUsage(ctx, teamID, quota.DimensionActiveSandboxes)
	if err != nil {
		return fmt.Errorf("load active sandbox usage: %w", err)
	}
	decision := quota.Check(teamID, quota.DimensionActiveSandboxes, current, 1, limit)
	if decision.Allowed {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrQuotaExceeded, decision.Err())
}

func (s *SandboxService) currentQuotaUsage(ctx context.Context, teamID string, dimension quota.Dimension) (int64, error) {
	current, err := s.quotaStore.CurrentUsage(ctx, teamID, dimension)
	if err == nil {
		return current, nil
	}
	if !errors.Is(err, quota.ErrUsageStoreNotConfigured) {
		return 0, err
	}
	current, ok, liveErr := s.currentLiveQuotaUsage(ctx, teamID, dimension)
	if liveErr != nil {
		return 0, liveErr
	}
	if ok {
		return current, nil
	}
	return 0, err
}

func (s *SandboxService) currentLiveQuotaUsage(ctx context.Context, teamID string, dimension quota.Dimension) (int64, bool, error) {
	if dimension != quota.DimensionActiveSandboxes {
		return 0, false, nil
	}
	current, ok, err := s.currentSandboxStoreActiveQuotaUsage(ctx, teamID)
	if err != nil || ok {
		return current, ok, err
	}
	if s == nil || s.podLister == nil {
		return 0, false, nil
	}
	pods, err := s.podLister.List(labels.SelectorFromSet(map[string]string{
		controller.LabelPoolType: controller.PoolTypeActive,
	}))
	if err != nil {
		return 0, true, fmt.Errorf("list active sandbox pods: %w", err)
	}
	var total int64
	for _, pod := range pods {
		if !liveQuotaPodMatchesTeam(pod, teamID) {
			continue
		}
		total++
	}
	return total, true, nil
}

func (s *SandboxService) currentSandboxStoreActiveQuotaUsage(ctx context.Context, teamID string) (int64, bool, error) {
	if s == nil || s.sandboxStore == nil {
		return 0, false, nil
	}
	records, err := s.sandboxStore.ListSandboxes(ctx, &ListSandboxesRequest{TeamID: teamID})
	if err != nil {
		return 0, true, fmt.Errorf("list sandbox records: %w", err)
	}
	var total int64
	for _, record := range records {
		if sandboxRecordCountsForActiveQuota(record) {
			total++
		}
	}
	return total, true, nil
}

func sandboxRecordCountsForActiveQuota(record *SandboxRecord) bool {
	if record == nil || !record.DeletedAt.IsZero() {
		return false
	}
	switch record.Status {
	case SandboxStatusStarting, SandboxStatusRunning:
		return true
	default:
		return false
	}
}

func liveQuotaPodMatchesTeam(pod *corev1.Pod, teamID string) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	if pod.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		return false
	}
	if pod.Labels[controller.LabelSandboxID] == "" && pod.Annotations[controller.AnnotationSandboxID] == "" {
		return false
	}
	if strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID]) != teamID {
		return false
	}
	return pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending
}
