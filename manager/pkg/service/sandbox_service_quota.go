package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

func (s *SandboxService) enforceSandboxCPUQuota(ctx context.Context, teamID string, resourceQuota v1alpha1.ResourceQuota) error {
	requested := resourceQuota.CPU.MilliValue()
	return s.enforceQuota(ctx, teamID, quota.DimensionCPU, requested)
}

func (s *SandboxService) enforceSandboxMemoryQuota(ctx context.Context, teamID string, resourceQuota v1alpha1.ResourceQuota) error {
	requested := bytesToMiBRoundUp(resourceQuota.Memory.Value())
	return s.enforceQuota(ctx, teamID, quota.DimensionMemory, requested)
}

func (s *SandboxService) enforceQuota(ctx context.Context, teamID string, dimension quota.Dimension, requested int64) error {
	teamID = strings.TrimSpace(teamID)
	if s == nil || s.quotaStore == nil || teamID == "" {
		return nil
	}
	limit, err := s.quotaStore.GetLimit(ctx, teamID, dimension)
	if err != nil {
		return fmt.Errorf("load %s quota: %w", dimension, err)
	}
	if limit == nil {
		return nil
	}
	if requested > 0 && requested > limit.LimitValue {
		decision := quota.Check(teamID, dimension, 0, requested, limit)
		return fmt.Errorf("%w: %s", ErrQuotaExceeded, decision.Err())
	}
	current, err := s.currentQuotaUsage(ctx, teamID, dimension)
	if err != nil {
		return fmt.Errorf("load %s usage: %w", dimension, err)
	}
	decision := quota.Check(teamID, dimension, current, requested, limit)
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
	switch dimension {
	case quota.DimensionActiveSandboxes, quota.DimensionCPU, quota.DimensionMemory:
	default:
		return 0, false, nil
	}
	if dimension == quota.DimensionActiveSandboxes {
		current, ok, err := s.currentSandboxStoreActiveQuotaUsage(ctx, teamID)
		if err != nil || ok {
			return current, ok, err
		}
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
		switch dimension {
		case quota.DimensionActiveSandboxes:
			total++
		case quota.DimensionCPU, quota.DimensionMemory:
			total += liveQuotaPodResourceUsage(pod, dimension)
		}
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

func liveQuotaPodResourceUsage(pod *corev1.Pod, dimension quota.Dimension) int64 {
	if pod == nil {
		return 0
	}
	var total int64
	for _, container := range pod.Spec.Containers {
		switch dimension {
		case quota.DimensionCPU:
			total += quotaCPUUsageMilli(container.Resources.Limits.Cpu(), container.Resources.Requests.Cpu())
		case quota.DimensionMemory:
			total += quotaMemoryUsageMiB(container.Resources.Limits.Memory(), container.Resources.Requests.Memory())
		}
	}
	return total
}

func quotaCPUUsageMilli(limit, request *resource.Quantity) int64 {
	if limit != nil && limit.Sign() > 0 {
		return limit.MilliValue()
	}
	if request != nil && request.Sign() > 0 {
		return request.MilliValue()
	}
	return 0
}

func quotaMemoryUsageMiB(limit, request *resource.Quantity) int64 {
	if limit != nil && limit.Sign() > 0 {
		return bytesToMiBRoundUp(limit.Value())
	}
	if request != nil && request.Sign() > 0 {
		return bytesToMiBRoundUp(request.Value())
	}
	return 0
}

func bytesToMiBRoundUp(value int64) int64 {
	if value <= 0 {
		return 0
	}
	const mib = int64(1024 * 1024)
	return (value + mib - 1) / mib
}
