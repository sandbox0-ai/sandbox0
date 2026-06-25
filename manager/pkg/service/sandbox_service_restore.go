package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const sandboxLifecycleWaitInterval = 100 * time.Millisecond

// ResumePausedSandboxRuntime creates a new runtime for a paused durable sandbox
// and restores the latest writable rootfs checkpoint.
func (s *SandboxService) ResumePausedSandboxRuntime(ctx context.Context, sandboxID string) (*Sandbox, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, k8serrors.NewNotFound(corev1.Resource("pod"), sandboxID)
	}

	var pod *corev1.Pod
	var record *SandboxRecord
	var deletingPodRef *sandboxRuntimePodRef
	claimType := "hot"
	restoreNeeded := false
	for {
		pod = nil
		record = nil
		deletingPodRef = nil
		restoreNeeded = false
		err := s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx SandboxStoreTx, locked *SandboxRecord) error {
			if locked.Status == SandboxStatusDeleted || !locked.DeletedAt.IsZero() {
				return k8serrors.NewNotFound(corev1.Resource("sandbox"), sandboxID)
			}
			if sandboxHardExpired(locked.HardExpiresAt, s.now()) {
				return k8serrors.NewNotFound(corev1.Resource("sandbox"), sandboxID)
			}
			switch locked.Status {
			case SandboxStatusPausing:
				return errSandboxLifecyclePausing
			case SandboxStatusStarting, SandboxStatusResuming:
				return errSandboxLifecycleResuming
			}
			record = locked

			existing, getErr := s.getSandboxPod(lockCtx, sandboxID)
			if getErr == nil {
				if existing.DeletionTimestamp != nil {
					deletingPodRef = &sandboxRuntimePodRef{
						namespace: existing.Namespace,
						name:      existing.Name,
					}
					return errSandboxRuntimeDeleting
				}
				pod = existing
				record = nil
				return tx.SaveRuntime(lockCtx, sandboxID, existing.Namespace, existing.Name, s.podToSandboxStatus(existing), runtimeGenerationFromPod(existing), parseRFC3339AnnotationTime(existing.Annotations, controller.AnnotationExpiresAt), parseRFC3339AnnotationTime(existing.Annotations, controller.AnnotationHardExpiresAt))
			}
			if getErr != nil && !k8serrors.IsNotFound(getErr) {
				return fmt.Errorf("get current runtime pod: %w", getErr)
			}
			if locked.Status != SandboxStatusPaused {
				return k8serrors.NewConflict(corev1.Resource("sandbox"), sandboxID, fmt.Errorf("sandbox runtime for status %q is not available", locked.Status))
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
				HardExpiresAt:     locked.HardExpiresAt,
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
			restoreNeeded = true
			return tx.SaveRuntime(lockCtx, sandboxID, pod.Namespace, pod.Name, SandboxStatusResuming, generation, parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt), parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt))
		})
		if err == nil {
			break
		}
		if errors.Is(err, ErrSandboxRecordNotFound) {
			return nil, k8serrors.NewNotFound(corev1.Resource("sandbox"), sandboxID)
		}
		switch {
		case errors.Is(err, errSandboxLifecyclePausing):
			if err := s.CompletePausingSandboxRuntime(ctx, sandboxID); err != nil {
				return nil, err
			}
			continue
		case errors.Is(err, errSandboxLifecycleResuming):
			if err := s.waitForSandboxLifecycleStatusExit(ctx, sandboxID, SandboxStatusStarting, SandboxStatusResuming); err != nil {
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
		default:
			return nil, err
		}
	}
	if pod == nil {
		return nil, fmt.Errorf("restore sandbox runtime did not create or find a pod")
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
		_ = s.pauseSandboxRuntime(context.Background(), sandboxID, false)
		return nil, err
	}
	return s.GetSandbox(ctx, sandboxID)
}

var (
	errSandboxLifecyclePausing  = errors.New("sandbox lifecycle pause is in progress")
	errSandboxLifecycleResuming = errors.New("sandbox lifecycle resume is in progress")
	errSandboxRuntimeDeleting   = errors.New("sandbox runtime pod deletion is in progress")
)

type sandboxRuntimePodRef struct {
	namespace string
	name      string
}

func (s *SandboxService) waitForSandboxLifecycleStatusExit(ctx context.Context, sandboxID string, statuses ...string) error {
	if s == nil || s.sandboxStore == nil {
		return nil
	}
	waiting := make(map[string]struct{}, len(statuses))
	for _, status := range statuses {
		waiting[status] = struct{}{}
	}
	ticker := time.NewTicker(sandboxLifecycleWaitInterval)
	defer ticker.Stop()
	for {
		record, err := s.sandboxStore.GetSandbox(ctx, sandboxID)
		if err != nil {
			if errors.Is(err, ErrSandboxRecordNotFound) {
				return k8serrors.NewNotFound(corev1.Resource("sandbox"), sandboxID)
			}
			return err
		}
		if record == nil || record.Status == SandboxStatusDeleted || !record.DeletedAt.IsZero() {
			return k8serrors.NewNotFound(corev1.Resource("sandbox"), sandboxID)
		}
		if _, ok := waiting[record.Status]; !ok {
			return nil
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
		apiDeleting := false
		if s != nil && s.k8sClient != nil {
			pod, err := s.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
			switch {
			case k8serrors.IsNotFound(err):
			case err != nil:
				return err
			case pod != nil && pod.DeletionTimestamp != nil:
				apiDeleting = true
			default:
				return nil
			}
		}
		if s != nil && s.podLister != nil {
			pod, err := s.podLister.Pods(namespace).Get(name)
			if k8serrors.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if pod != nil && pod.DeletionTimestamp == nil && !apiDeleting {
				return nil
			}
		} else if !apiDeleting {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *SandboxService) finishRestoredSandboxRuntime(ctx context.Context, pod *corev1.Pod, record *SandboxRecord, claimType string) (*corev1.Pod, error) {
	template, err := s.templateForSandboxRecord(record)
	if err != nil {
		return pod, err
	}
	if claimType == "cold" {
		readyPod, err := s.waitForPodClaimReady(ctx, pod.Namespace, pod.Name)
		if err != nil {
			return pod, fmt.Errorf("wait for pod claim readiness: %w", err)
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
		return pod, fmt.Errorf("load rootfs checkpoint: %w", err)
	}
	pod, err = s.applySandboxRootFSCheckpointWithFallback(ctx, pod, record, template, req, rootFSState, SandboxStatusResuming)
	if err != nil {
		return pod, err
	}
	if _, err := s.bindVolumePortals(ctx, pod, req, template); err != nil {
		return pod, fmt.Errorf("bind volume portals: %w", err)
	}
	if err := s.bindWebhookStatePortal(ctx, pod, req); err != nil {
		return pod, fmt.Errorf("bind webhook state portal: %w", err)
	}
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return pod, fmt.Errorf("get procd address: %w", err)
	}
	if _, err := s.initializeProcd(ctx, pod, template, req, procdAddress); err != nil {
		return pod, fmt.Errorf("initialize procd: %w", err)
	}
	if err := s.persistUpdatedSandboxPod(ctx, pod); err != nil {
		return pod, fmt.Errorf("persist restored sandbox: %w", err)
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
