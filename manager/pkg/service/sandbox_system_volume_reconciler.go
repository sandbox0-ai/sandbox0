package service

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	defaultSandboxSystemVolumeReconcileInterval = 30 * time.Second
	defaultSandboxSystemVolumeCleanupDelay      = 15 * time.Second
)

// StartSystemVolumeReconciler garbage-collects manager-owned volumes whose
// sandbox lifecycle cleanup was skipped or interrupted.
func (s *SandboxService) StartSystemVolumeReconciler(ctx context.Context, interval time.Duration) {
	if s == nil || s.webhookStateVolumes == nil {
		return
	}
	if interval <= 0 {
		interval = defaultSandboxSystemVolumeReconcileInterval
	}
	logger := s.logger
	if logger == nil {
		logger = zap.NewNop()
	}
	logger.Info("Starting sandbox system volume reconciler", zap.Duration("interval", interval))
	if err := s.reconcileSystemVolumes(ctx); err != nil {
		logger.Warn("Sandbox system volume reconcile failed", zap.Error(err))
	}
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := s.reconcileSystemVolumes(ctx); err != nil {
			logger.Warn("Sandbox system volume reconcile failed", zap.Error(err))
		}
	}, interval)
}

func (s *SandboxService) reconcileSystemVolumes(ctx context.Context) error {
	if s == nil || s.webhookStateVolumes == nil {
		return nil
	}
	volumes, err := s.webhookStateVolumes.List(ctx)
	if err != nil {
		return fmt.Errorf("list system volumes: %w", err)
	}
	now := time.Now().UTC()
	if s.clock != nil {
		now = s.clock.Now()
	}
	for _, volume := range volumes {
		sandboxID := volume.OwnerSandboxID
		if sandboxID == "" || volume.VolumeID == "" {
			continue
		}
		if volume.CleanupRequestedAt != nil {
			if now.Sub(*volume.CleanupRequestedAt) < defaultSandboxSystemVolumeCleanupDelay {
				continue
			}
			if err := s.webhookStateVolumes.Delete(ctx, volume.TeamID, volume.UserID, sandboxID, volume.VolumeID); err != nil {
				if s.logger != nil {
					s.logger.Warn("Failed to delete system volume marked for cleanup",
						zap.String("sandboxID", sandboxID),
						zap.String("volumeID", volume.VolumeID),
						zap.String("purpose", volume.Purpose),
						zap.Error(err),
					)
				}
			}
			continue
		}
		if s.systemVolumeOwnerPodActive(sandboxID) {
			continue
		}
		if err := s.webhookStateVolumes.MarkSandboxForCleanup(ctx, volume.TeamID, volume.UserID, sandboxID, "orphaned_sandbox"); err != nil {
			if s.logger != nil {
				s.logger.Warn("Failed to mark orphaned system volume for cleanup",
					zap.String("sandboxID", sandboxID),
					zap.String("volumeID", volume.VolumeID),
					zap.String("purpose", volume.Purpose),
					zap.Error(err),
				)
			}
		}
	}
	return nil
}

func (s *SandboxService) systemVolumeOwnerPodActive(sandboxID string) bool {
	if s == nil || s.podLister == nil || sandboxID == "" {
		return true
	}
	pods, err := s.podLister.List(labels.SelectorFromSet(map[string]string{
		controller.LabelSandboxID: sandboxID,
	}))
	if err != nil {
		return true
	}
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil || pod.Labels == nil {
			continue
		}
		if pod.Labels[controller.LabelPoolType] == controller.PoolTypeActive {
			return true
		}
	}
	return false
}
