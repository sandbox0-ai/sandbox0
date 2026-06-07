package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ResumePausedSandboxRuntime creates a new runtime for a paused durable sandbox
// and restores the latest writable rootfs checkpoint.
func (s *SandboxService) ResumePausedSandboxRuntime(ctx context.Context, sandboxID string) (*Sandbox, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, k8serrors.NewNotFound(corev1.Resource("pod"), sandboxID)
	}

	var pod *corev1.Pod
	var record *SandboxRecord
	claimType := "hot"
	err := s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx SandboxStoreTx, locked *SandboxRecord) error {
		if locked.Status == SandboxStatusDeleted || !locked.DeletedAt.IsZero() {
			return k8serrors.NewNotFound(corev1.Resource("sandbox"), sandboxID)
		}
		switch locked.Status {
		case SandboxStatusStarting, SandboxStatusPausing, SandboxStatusResuming:
			return k8serrors.NewConflict(corev1.Resource("sandbox"), sandboxID, fmt.Errorf("sandbox lifecycle operation %q is in progress", locked.Status))
		}
		record = locked

		existing, getErr := s.getSandboxPod(lockCtx, sandboxID)
		if getErr == nil {
			if existing.DeletionTimestamp != nil {
				return k8serrors.NewConflict(corev1.Resource("pod"), existing.Name, fmt.Errorf("sandbox runtime deletion is still in progress"))
			}
			pod = existing
			return tx.SaveRuntime(lockCtx, sandboxID, existing.Namespace, existing.Name, s.podToSandboxStatus(existing), runtimeGenerationFromPod(existing), parseRFC3339AnnotationTime(existing.Annotations, controller.AnnotationExpiresAt), parseRFC3339AnnotationTime(existing.Annotations, controller.AnnotationHardExpiresAt))
		}
		if getErr != nil && !k8serrors.IsNotFound(getErr) {
			return fmt.Errorf("get current runtime pod: %w", getErr)
		}

		template, err := s.templateForSandboxRecord(locked)
		if err != nil {
			return err
		}
		if err := s.enforceActiveSandboxQuota(lockCtx, locked.TeamID); err != nil {
			return err
		}
		if err := s.enforceSandboxCPUQuota(lockCtx, locked.TeamID, template); err != nil {
			return err
		}
		if err := s.enforceSandboxMemoryQuota(lockCtx, locked.TeamID, template); err != nil {
			return err
		}
		generation := locked.RuntimeGeneration + 1
		req := &ClaimRequest{
			TeamID:            locked.TeamID,
			UserID:            locked.UserID,
			Template:          locked.TemplateID,
			Config:            &locked.Config,
			Mounts:            locked.Mounts,
			SandboxID:         locked.ID,
			RuntimeGeneration: generation,
		}
		pod, err = s.claimIdlePod(lockCtx, template, req)
		if err != nil {
			return fmt.Errorf("claim idle pod: %w", err)
		}
		if pod == nil {
			claimType = "cold"
			pod, err = s.createNewPod(lockCtx, template, req)
			if err != nil {
				return fmt.Errorf("create runtime pod: %w", err)
			}
		}
		return tx.SaveRuntime(lockCtx, sandboxID, pod.Namespace, pod.Name, SandboxStatusResuming, generation, parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt), parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt))
	})
	if err != nil {
		if errors.Is(err, ErrSandboxRecordNotFound) {
			return nil, k8serrors.NewNotFound(corev1.Resource("sandbox"), sandboxID)
		}
		return nil, err
	}
	if pod == nil {
		return nil, fmt.Errorf("restore sandbox runtime did not create or find a pod")
	}
	if record == nil {
		return s.GetSandbox(ctx, sandboxID)
	}

	if err := s.finishRestoredSandboxRuntime(ctx, pod, record, claimType); err != nil {
		s.requestSandboxDeletionAfterClaimFailure(pod, "restored runtime initialization failed")
		_ = s.pauseSandboxRuntime(context.Background(), sandboxID, false)
		return nil, err
	}
	return s.GetSandbox(ctx, sandboxID)
}

func (s *SandboxService) finishRestoredSandboxRuntime(ctx context.Context, pod *corev1.Pod, record *SandboxRecord, claimType string) error {
	template, err := s.templateForSandboxRecord(record)
	if err != nil {
		return err
	}
	if claimType == "cold" {
		readyPod, err := s.waitForPodClaimReady(ctx, pod.Namespace, pod.Name)
		if err != nil {
			return fmt.Errorf("wait for pod claim readiness: %w", err)
		}
		pod = readyPod
		s.refreshSandboxProbeConditionsAsync(pod)
	}
	req := &ClaimRequest{
		TeamID:            record.TeamID,
		UserID:            record.UserID,
		Template:          record.TemplateID,
		Config:            &record.Config,
		Mounts:            record.Mounts,
		SandboxID:         record.ID,
		RuntimeGeneration: record.RuntimeGeneration + 1,
	}
	rootFSState, err := s.latestRootFSState(ctx, record.ID)
	if err != nil {
		return fmt.Errorf("load rootfs checkpoint: %w", err)
	}
	if err := s.applySandboxRootFSCheckpoint(ctx, pod, rootFSState); err != nil {
		return err
	}
	if _, err := s.bindVolumePortals(ctx, pod, req, template); err != nil {
		return fmt.Errorf("bind volume portals: %w", err)
	}
	if err := s.bindWebhookStatePortal(ctx, pod, req); err != nil {
		return fmt.Errorf("bind webhook state portal: %w", err)
	}
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return fmt.Errorf("get procd address: %w", err)
	}
	if _, err := s.initializeProcd(ctx, pod, req, procdAddress); err != nil {
		return fmt.Errorf("initialize procd: %w", err)
	}
	if err := s.persistUpdatedSandboxPod(ctx, pod); err != nil {
		return fmt.Errorf("persist restored sandbox: %w", err)
	}
	if s.logger != nil {
		s.logger.Info("Resumed paused sandbox runtime",
			zap.String("sandboxID", record.ID),
			zap.String("pod", pod.Name),
			zap.String("claimType", claimType),
		)
	}
	return nil
}

func (s *SandboxService) templateForSandboxRecord(record *SandboxRecord) (*v1alpha1.SandboxTemplate, error) {
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
