package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
)

// TerminateSandbox terminates a sandbox
func (s *SandboxService) TerminateSandbox(ctx context.Context, sandboxID string) error {
	s.logger.Info("Terminating sandbox", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			s.logger.Info("Sandbox already terminated", zap.String("sandboxID", sandboxID))
			if s.sandboxStore != nil {
				record, getErr := s.sandboxStore.GetSandbox(ctx, sandboxID)
				if getErr != nil {
					return fmt.Errorf("get sandbox record: %w", getErr)
				}
				if record != nil && record.Status != SandboxStatusDeleted {
					if err := s.CleanupDeletedSandbox(ctx, sandboxLifecycleInfoFromRecord(record)); err != nil {
						return fmt.Errorf("cleanup deleted sandbox record: %w", err)
					}
				}
				return s.sandboxStore.MarkSandboxDeleted(ctx, sandboxID, s.clock.Now())
			}
			return nil
		}
		return fmt.Errorf("get pod: %w", err)
	}
	s.thawSandboxBeforeTermination(ctx, pod, sandboxID)
	if err := s.unbindRootfsBeforeRuntimeDeletion(ctx, pod, true); err != nil {
		return fmt.Errorf("unbind rootfs before termination: %w", err)
	}

	pod, err = s.ensureSandboxDeletionFinalizer(ctx, pod)
	if err != nil {
		return fmt.Errorf("ensure sandbox cleanup finalizer: %w", err)
	}
	pod, err = s.markRuntimeDeletionReason(ctx, pod, runtimeDeletionReasonDeleted)
	if err != nil {
		return fmt.Errorf("mark sandbox deletion reason: %w", err)
	}

	err = s.k8sClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		s.logger.Info("Sandbox already terminated", zap.String("sandboxID", sandboxID))
		if s.sandboxStore != nil {
			return s.sandboxStore.MarkSandboxDeleted(ctx, sandboxID, s.clock.Now())
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete pod: %w", err)
	}

	s.logger.Info("Sandbox termination requested", zap.String("sandboxID", sandboxID), zap.String("pod", pod.Name))
	if s.sandboxStore != nil {
		if err := s.sandboxStore.MarkSandboxDeleted(ctx, sandboxID, s.clock.Now()); err != nil {
			return err
		}
	}

	return nil
}

// CleanSandboxRuntime deletes the runtime pod while preserving durable sandbox state and public services.
func (s *SandboxService) CleanSandboxRuntime(ctx context.Context, sandboxID string) error {
	s.logger.Info("Cleaning sandbox runtime", zap.String("sandboxID", sandboxID))
	var deletedPodNamespace string
	var deletedPodName string
	clean := func(ctx context.Context, tx SandboxStoreTx, record *SandboxRecord) error {
		pod, err := s.getSandboxPod(ctx, sandboxID)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				if tx != nil {
					return tx.MarkRuntimeCleaned(ctx, sandboxID, 0, s.clock.Now())
				}
				return nil
			}
			return fmt.Errorf("get pod: %w", err)
		}
		generation := runtimeGenerationFromPod(pod)
		if err := s.unbindRootfsBeforeRuntimeDeletion(ctx, pod, false); err != nil {
			return fmt.Errorf("unbind rootfs before clean: %w", err)
		}
		pod, err = s.ensureSandboxDeletionFinalizer(ctx, pod)
		if err != nil {
			return fmt.Errorf("ensure sandbox cleanup finalizer: %w", err)
		}
		pod, err = s.markRuntimeDeletionReason(ctx, pod, runtimeDeletionReasonCleaned)
		if err != nil {
			return fmt.Errorf("mark runtime deletion reason: %w", err)
		}
		if err := s.k8sClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
			return fmt.Errorf("delete runtime pod: %w", err)
		}
		deletedPodNamespace = pod.Namespace
		deletedPodName = pod.Name
		if tx != nil {
			return tx.MarkRuntimeCleaned(ctx, sandboxID, generation, s.clock.Now())
		}
		return nil
	}
	if s.sandboxStore != nil {
		if err := s.sandboxStore.WithSandboxLock(ctx, sandboxID, clean); err != nil {
			if errors.Is(err, ErrSandboxRecordNotFound) {
				if err := clean(ctx, nil, nil); err != nil {
					return err
				}
				return s.waitForRuntimePodDeleted(ctx, deletedPodNamespace, deletedPodName)
			}
			return err
		}
		return s.waitForRuntimePodDeleted(ctx, deletedPodNamespace, deletedPodName)
	}
	if err := clean(ctx, nil, nil); err != nil {
		return err
	}
	return s.waitForRuntimePodDeleted(ctx, deletedPodNamespace, deletedPodName)
}

// CleanSandboxRuntimeByID implements controller.SandboxRuntimeCleaner.
func (s *SandboxService) CleanSandboxRuntimeByID(ctx context.Context, sandboxID string) error {
	return s.CleanSandboxRuntime(ctx, sandboxID)
}

const (
	runtimeDeletionReasonCleaned = "cleaned"
	runtimeDeletionReasonDeleted = "deleted"
	runtimeDeletionWaitTimeout   = 30 * time.Second
)

func (s *SandboxService) waitForRuntimePodDeleted(ctx context.Context, namespace, name string) error {
	if s == nil || s.k8sClient == nil || strings.TrimSpace(namespace) == "" || strings.TrimSpace(name) == "" {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, runtimeDeletionWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := s.k8sClient.CoreV1().Pods(namespace).Get(waitCtx, name, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("wait for runtime pod deletion: %w", err)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("runtime pod %s/%s was not deleted after %s", namespace, name, runtimeDeletionWaitTimeout)
		case <-ticker.C:
		}
	}
}

func (s *SandboxService) markRuntimeDeletionReason(ctx context.Context, pod *corev1.Pod, reason string) (*corev1.Pod, error) {
	if s == nil || pod == nil || s.k8sClient == nil || strings.TrimSpace(reason) == "" {
		return pod, nil
	}
	var updated *corev1.Pod
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := s.k8sClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		updated = current.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = make(map[string]string)
		}
		updated.Annotations[controller.AnnotationRuntimeDeletionReason] = reason
		updated, err = s.k8sClient.CoreV1().Pods(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (s *SandboxService) thawSandboxBeforeTermination(ctx context.Context, pod *corev1.Pod, sandboxID string) {
	if s == nil || !s.config.CtldEnabled || !sandboxPodMayHaveFrozenCgroup(pod) {
		return
	}
	if _, err := s.RequestResumeSandbox(ctx, sandboxID); err != nil {
		s.logger.Warn("Failed to request sandbox thaw before termination",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
	}
}

func (s *SandboxService) unbindRootfsBeforeRuntimeDeletion(ctx context.Context, pod *corev1.Pod, tolerateAlreadyUnbound bool) error {
	if s == nil || pod == nil || pod.Annotations == nil {
		return nil
	}
	filesystemID := strings.TrimSpace(pod.Annotations[controller.AnnotationFilesystemID])
	if filesystemID == "" {
		return nil
	}
	if !s.config.CtldEnabled {
		return fmt.Errorf("ctld is required for sandbox filesystem rootfs")
	}
	if s.ctldClient == nil {
		return fmt.Errorf("ctld client is not configured")
	}
	if strings.TrimSpace(string(pod.UID)) == "" {
		return fmt.Errorf("pod uid is required for rootfs unbind")
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return err
	}
	resp, err := s.ctldClient.UnbindRootfs(ctx, ctldAddress, ctldapi.UnbindRootfsRequest{
		PodUID:              string(pod.UID),
		PortalName:          volumeportal.RootfsPortalName,
		MountPath:           volumeportal.RootfsMountPath,
		SandboxFilesystemID: filesystemID,
	})
	if err != nil {
		if tolerateAlreadyUnbound && isRootfsAlreadyUnboundError(err) {
			if s.logger != nil {
				s.logger.Warn("Skipping sandbox rootfs unbind after ctld reported it already unbound",
					zap.String("sandboxID", sandboxIDFromPod(pod)),
					zap.String("filesystemID", filesystemID),
					zap.Error(err),
				)
			}
			return nil
		}
		return err
	}
	if s.logger != nil && resp != nil && strings.TrimSpace(resp.S0FSHead) != "" {
		s.logger.Info("Committed sandbox filesystem rootfs",
			zap.String("sandboxID", sandboxIDFromPod(pod)),
			zap.String("filesystemID", filesystemID),
			zap.String("s0fsHead", resp.S0FSHead),
		)
	}
	return nil
}

func isRootfsAlreadyUnboundError(err error) bool {
	var reqErr *ctldapi.RequestError
	if !errors.As(err, &reqErr) || reqErr == nil {
		return false
	}
	if reqErr.StatusCode != http.StatusBadRequest && reqErr.StatusCode != http.StatusNotFound {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(reqErr.Message))
	return strings.Contains(message, "not found") ||
		strings.Contains(message, "is not bound") ||
		strings.Contains(message, "not bound")
}

// GetSandbox gets a sandbox by ID
func (s *SandboxService) GetSandbox(ctx context.Context, sandboxID string) (*Sandbox, error) {
	var record *SandboxRecord
	if s.sandboxStore != nil {
		var storeErr error
		record, storeErr = s.sandboxStore.GetSandbox(ctx, sandboxID)
		if storeErr != nil {
			return nil, fmt.Errorf("get sandbox record: %w", storeErr)
		}
		if record != nil && record.Status == SandboxStatusDeleted {
			return nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
		}
		if record != nil && record.Status == SandboxStatusCleaned {
			return s.recordToSandbox(record), nil
		}
	}
	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		if k8serrors.IsNotFound(err) && record != nil {
			return s.recordToSandbox(record), nil
		}
		return nil, fmt.Errorf("get pod: %w", err)
	}

	return s.podToSandbox(ctx, pod, sandboxID), nil
}

// UpdateSandbox updates mutable sandbox configuration fields.
func (s *SandboxService) UpdateSandbox(ctx context.Context, sandboxID string, cfg *SandboxUpdateConfig) (*Sandbox, error) {
	if cfg == nil {
		return nil, fmt.Errorf("sandbox config is required")
	}

	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		if k8serrors.IsNotFound(err) && s.sandboxStore != nil {
			record, getErr := s.sandboxStore.GetSandbox(ctx, sandboxID)
			if getErr != nil {
				return nil, fmt.Errorf("get sandbox record: %w", getErr)
			}
			if record != nil && record.Status != SandboxStatusDeleted {
				return s.updateCleanedSandboxRecord(ctx, record, cfg)
			}
		}
		return nil, fmt.Errorf("get pod: %w", err)
	}

	var networkState *BuildNetworkPolicyResult
	var updatedPod *corev1.Pod
	var rollbackBindings func(context.Context) error

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Get the latest version of the pod
		current, err := s.getSandboxPod(ctx, sandboxID)
		if err != nil {
			return err
		}

		updatedPod = current.DeepCopy()
		if updatedPod.Annotations == nil {
			updatedPod.Annotations = make(map[string]string)
		}

		merged := SandboxConfig{}
		if configJSON := updatedPod.Annotations[controller.AnnotationConfig]; configJSON != "" {
			if err := json.Unmarshal([]byte(configJSON), &merged); err != nil {
				s.logger.Warn("Failed to parse existing sandbox config annotation",
					zap.String("sandboxID", sandboxID),
					zap.Error(err),
				)
			}
		}

		if cfg.TTL != nil {
			merged.TTL = cfg.TTL
			setExpirationAnnotation(updatedPod.Annotations, s.clock.Now(), cfg.TTL)
		}
		if cfg.HardTTL != nil {
			merged.HardTTL = cfg.HardTTL
			setHardExpirationAnnotation(updatedPod.Annotations, s.clock.Now(), cfg.HardTTL)
		}
		if cfg.AutoResume != nil {
			merged.AutoResume = cfg.AutoResume
		}
		if cfg.Services != nil {
			services, err := NormalizeSandboxAppServices(cfg.Services)
			if err != nil {
				return err
			}
			merged.Services = services
		}

		if cfg.Network != nil {
			if s.NetworkPolicyService == nil {
				return fmt.Errorf("network policy service not configured")
			}

			teamID := updatedPod.Annotations[controller.AnnotationTeamID]
			templateSpec, templateBindings := s.templateNetworkDefaults(updatedPod)
			requestSpec := merged.Network
			if cfg.Network != nil {
				requestSpec = cfg.Network
				merged.Network = sanitizedNetworkPolicyForPersistence(cfg.Network)
			}
			requestBindings := append([]v1alpha1.CredentialBinding(nil), cfg.Network.CredentialBindings...)
			if cfg.Network.CredentialBindings == nil {
				requestBindings, err = s.loadCredentialBindings(ctx, updatedPod)
				if err != nil {
					return fmt.Errorf("load credential bindings: %w", err)
				}
			}
			networkState = s.NetworkPolicyService.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
				SandboxID:        sandboxIDFromPod(updatedPod),
				TeamID:           teamID,
				TemplateSpec:     templateSpec,
				RequestSpec:      requestSpec,
				TemplateBindings: templateBindings,
				RequestBindings:  requestBindings,
			})
			rollbackBindings, err = s.syncCredentialBindings(ctx, updatedPod, teamID, networkState)
			if err != nil {
				return fmt.Errorf("stage credential bindings: %w", err)
			}
			if _, err := s.setNetworkPolicyAnnotations(updatedPod, policySpecFromState(networkState)); err != nil {
				return err
			}
		}

		if merged.AutoResume != nil && !*merged.AutoResume && SandboxAppServicesHaveResumeRoute(merged.Services) {
			return fmt.Errorf("cannot set resume=true on public routes when sandbox auto_resume is disabled")
		}

		updatedConfigJSON, err := json.Marshal(merged)
		if err != nil {
			return fmt.Errorf("marshal sandbox config: %w", err)
		}
		updatedPod.Annotations[controller.AnnotationConfig] = string(updatedConfigJSON)

		updatedPod, err = s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, updatedPod, metav1.UpdateOptions{})
		if err != nil && rollbackBindings != nil {
			if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
				s.logger.Warn("Failed to roll back credential bindings after sandbox update failure",
					zap.String("sandboxID", sandboxID),
					zap.Error(rollbackErr),
				)
			}
		}
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("update pod: %w", err)
	}

	if networkState != nil {
		teamID := updatedPod.Annotations[controller.AnnotationTeamID]
		if err := s.applyNetworkProvider(ctx, updatedPod, teamID, policySpecFromState(networkState)); err != nil {
			return nil, fmt.Errorf("apply network policy: %w", err)
		}
	}
	if err := s.persistUpdatedSandboxPod(ctx, updatedPod); err != nil {
		return nil, err
	}

	return s.podToSandbox(ctx, updatedPod, sandboxID), nil
}

func (s *SandboxService) updateCleanedSandboxRecord(ctx context.Context, record *SandboxRecord, cfg *SandboxUpdateConfig) (*Sandbox, error) {
	if record == nil {
		return nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, "")
	}
	merged := record.Config
	now := s.clock.Now()
	if cfg.TTL != nil {
		merged.TTL = cfg.TTL
		record.ExpiresAt = expirationFromTTL(now, cfg.TTL)
	}
	if cfg.HardTTL != nil {
		merged.HardTTL = cfg.HardTTL
		record.HardExpiresAt = expirationFromTTL(now, cfg.HardTTL)
	}
	if cfg.AutoResume != nil {
		merged.AutoResume = cfg.AutoResume
	}
	if cfg.Services != nil {
		services, err := NormalizeSandboxAppServices(cfg.Services)
		if err != nil {
			return nil, err
		}
		merged.Services = services
	}
	if cfg.Network != nil {
		merged.Network = sanitizedNetworkPolicyForPersistence(cfg.Network)
	}
	if merged.AutoResume != nil && !*merged.AutoResume && SandboxAppServicesHaveResumeRoute(merged.Services) {
		return nil, fmt.Errorf("cannot set resume=true on public routes when sandbox auto_resume is disabled")
	}
	record.Config = merged
	record.UpdatedAt = now
	if err := s.sandboxStore.UpsertSandbox(ctx, record); err != nil {
		return nil, err
	}
	return s.recordToSandbox(record), nil
}

func expirationFromTTL(now time.Time, ttl *int32) time.Time {
	if ttl == nil || *ttl <= 0 {
		return time.Time{}
	}
	return now.Add(time.Duration(*ttl) * time.Second)
}

func (s *SandboxService) persistUpdatedSandboxPod(ctx context.Context, pod *corev1.Pod) error {
	if s == nil || s.sandboxStore == nil || pod == nil {
		return nil
	}
	template := s.templateForPod(pod)
	if template == nil {
		return nil
	}
	record := &SandboxRecord{
		ID:                  sandboxIDFromPod(pod),
		TeamID:              pod.Annotations[controller.AnnotationTeamID],
		UserID:              pod.Annotations[controller.AnnotationUserID],
		FilesystemID:        pod.Annotations[controller.AnnotationFilesystemID],
		TemplateID:          sandboxTemplateIDFromLabels(pod.Labels),
		TemplateName:        template.Name,
		TemplateNamespace:   template.Namespace,
		ClusterID:           naming.ClusterIDOrDefault(template.Spec.ClusterId),
		Status:              s.podToSandboxStatus(pod),
		Config:              parseSandboxConfig(pod.Annotations[controller.AnnotationConfig]),
		Mounts:              parseClaimMounts(pod.Annotations[controller.AnnotationMounts]),
		TemplateSpec:        template.Spec,
		CurrentPodName:      pod.Name,
		CurrentPodNamespace: pod.Namespace,
		RuntimeGeneration:   runtimeGenerationFromPod(pod),
		ClaimedAt:           parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationClaimedAt),
		ExpiresAt:           parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt),
		HardExpiresAt:       parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt),
		CreatedAt:           pod.CreationTimestamp.Time,
	}
	if record.ID == "" {
		record.ID = pod.Name
	}
	return s.sandboxStore.UpsertSandbox(ctx, record)
}

func (s *SandboxService) getSandboxPod(ctx context.Context, sandboxID string) (*corev1.Pod, error) {
	if s.sandboxIndex != nil && s.podLister != nil {
		refs := s.sandboxIndex.GetPodRefs(sandboxID)
		if len(refs) > 0 {
			pods := make([]*corev1.Pod, 0, len(refs))
			for _, ref := range refs {
				pod, err := s.podLister.Pods(ref.Namespace).Get(ref.Name)
				if err != nil {
					if k8serrors.IsNotFound(err) {
						continue
					}
					return nil, err
				}
				pods = append(pods, pod)
			}
			pod, err := selectSandboxRuntimePod(sandboxID, pods)
			if err == nil || !k8serrors.IsNotFound(err) {
				return pod, err
			}
		}
	}

	pods, err := s.podLister.List(labels.SelectorFromSet(map[string]string{
		controller.LabelSandboxID: sandboxID,
	}))
	if err != nil {
		return nil, err
	}
	return selectSandboxRuntimePod(sandboxID, pods)
}

func selectSandboxRuntimePod(sandboxID string, pods []*corev1.Pod) (*corev1.Pod, error) {
	var active []*corev1.Pod
	var deleting []*corev1.Pod
	for _, pod := range pods {
		if pod == nil || sandboxIDFromPod(pod) != sandboxID {
			continue
		}
		if pod.DeletionTimestamp != nil {
			deleting = append(deleting, pod)
			continue
		}
		active = append(active, pod)
	}
	if len(active) == 1 {
		return active[0], nil
	}
	if len(active) > 1 {
		return nil, k8serrors.NewConflict(schema.GroupResource{Resource: "pod"}, sandboxID, fmt.Errorf("multiple active runtime pods found for sandbox"))
	}
	if len(deleting) == 1 {
		return deleting[0], nil
	}
	if len(deleting) > 1 {
		return nil, k8serrors.NewConflict(schema.GroupResource{Resource: "pod"}, sandboxID, fmt.Errorf("multiple deleting runtime pods found for sandbox"))
	}
	return nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "pod"}, sandboxID)
}

// podToSandbox converts a pod to a sandbox object
func (s *SandboxService) podToSandbox(ctx context.Context, pod *corev1.Pod, sandboxID string) *Sandbox {
	if sandboxID == "" {
		sandboxID = sandboxIDFromPod(pod)
	}
	status := s.podToSandboxStatus(pod)

	// Parse timestamps
	claimedAt := parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationClaimedAt)
	expiresAt := parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt)
	hardExpiresAt := parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt)
	createdAt := pod.CreationTimestamp.Time

	internalAddr, err := s.prodAddress(ctx, pod)
	if err != nil {
		s.logger.Error("Failed to get procd address", zap.String("sandboxID", sandboxID), zap.Error(err))
	}

	cfg := parseSandboxConfig(pod.Annotations[controller.AnnotationConfig])
	autoResume := true
	if cfg.AutoResume != nil {
		autoResume = *cfg.AutoResume
	}
	powerState := sandboxPowerStateFromAnnotations(pod.Annotations)

	return &Sandbox{
		ID:            sandboxID,
		TemplateID:    sandboxTemplateIDFromLabels(pod.Labels),
		TeamID:        pod.Annotations[controller.AnnotationTeamID],
		UserID:        pod.Annotations[controller.AnnotationUserID],
		FilesystemID:  pod.Annotations[controller.AnnotationFilesystemID],
		InternalAddr:  internalAddr,
		Status:        status,
		Paused:        powerState.Observed == SandboxPowerStatePaused,
		PowerState:    powerState,
		AutoResume:    autoResume,
		Services:      cfg.Services,
		Mounts:        parseClaimMounts(pod.Annotations[controller.AnnotationMounts]),
		PodName:       pod.Name,
		ExpiresAt:     expiresAt,
		HardExpiresAt: hardExpiresAt,
		ClaimedAt:     claimedAt,
		CreatedAt:     createdAt,
	}
}

func (s *SandboxService) recordToSandbox(record *SandboxRecord) *Sandbox {
	if record == nil {
		return nil
	}
	autoResume := true
	if record.Config.AutoResume != nil {
		autoResume = *record.Config.AutoResume
	}
	powerState := SandboxPowerState{
		Desired:            SandboxPowerStateActive,
		DesiredGeneration:  record.RuntimeGeneration,
		Observed:           SandboxPowerStateActive,
		ObservedGeneration: record.RuntimeGeneration,
		Phase:              SandboxPowerPhaseStable,
	}
	return &Sandbox{
		ID:            record.ID,
		TemplateID:    record.TemplateID,
		TeamID:        record.TeamID,
		UserID:        record.UserID,
		FilesystemID:  record.FilesystemID,
		Status:        record.Status,
		Paused:        false,
		PowerState:    powerState,
		AutoResume:    autoResume,
		Services:      record.Config.Services,
		Mounts:        record.Mounts,
		PodName:       record.CurrentPodName,
		ExpiresAt:     record.ExpiresAt,
		HardExpiresAt: record.HardExpiresAt,
		ClaimedAt:     record.ClaimedAt,
		CreatedAt:     record.CreatedAt,
	}
}

func sandboxLifecycleInfoFromRecord(record *SandboxRecord) SandboxLifecycleInfo {
	if record == nil {
		return SandboxLifecycleInfo{}
	}
	info := SandboxLifecycleInfo{
		Namespace:    record.CurrentPodNamespace,
		PodName:      record.CurrentPodName,
		SandboxID:    record.ID,
		TeamID:       record.TeamID,
		UserID:       record.UserID,
		FilesystemID: record.FilesystemID,
	}
	if record.Config.Webhook != nil {
		info.WebhookURL = strings.TrimSpace(record.Config.Webhook.URL)
		info.WebhookSecret = strings.TrimSpace(record.Config.Webhook.Secret)
	}
	return info
}

func sandboxTemplateIDFromLabels(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	if logicalID := strings.TrimSpace(labels[controller.LabelTemplateLogicalID]); logicalID != "" {
		return logicalID
	}
	return labels[controller.LabelTemplateID]
}

func parseRFC3339AnnotationTime(annotations map[string]string, key string) time.Time {
	if len(annotations) == 0 {
		return time.Time{}
	}
	raw := annotations[key]
	if raw == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func parseSandboxConfig(configJSON string) SandboxConfig {
	if configJSON == "" {
		return SandboxConfig{}
	}
	var cfg SandboxConfig
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return SandboxConfig{}
	}
	return cfg
}

func parseClaimMounts(mountsJSON string) []ClaimMount {
	if mountsJSON == "" {
		return nil
	}
	var mounts []ClaimMount
	if err := json.Unmarshal([]byte(mountsJSON), &mounts); err != nil {
		return nil
	}
	normalized, err := normalizeClaimMounts(mounts)
	if err != nil {
		return nil
	}
	return normalized
}

// GetSandboxStatus gets the status of a sandbox
func (s *SandboxService) GetSandboxStatus(ctx context.Context, sandboxID string) (map[string]any, error) {
	sandbox, err := s.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}

	status := map[string]any{
		"sandbox_id":      sandbox.ID,
		"template_id":     sandbox.TemplateID,
		"team_id":         sandbox.TeamID,
		"user_id":         sandbox.UserID,
		"filesystem_id":   sandbox.FilesystemID,
		"pod_name":        sandbox.PodName,
		"status":          sandbox.Status,
		"claimed_at":      sandbox.ClaimedAt.Format(time.RFC3339),
		"expires_at":      sandbox.ExpiresAt.Format(time.RFC3339),
		"hard_expires_at": sandbox.HardExpiresAt.Format(time.RFC3339),
		"created_at":      sandbox.CreatedAt.Format(time.RFC3339),
	}

	return status, nil
}

// RefreshRequest represents a sandbox refresh request
type RefreshRequest struct {
	Duration int32 `json:"duration,omitempty"` // Duration to extend in seconds (optional, defaults to original TTL)
}

// RefreshResponse represents a sandbox refresh response
type RefreshResponse struct {
	SandboxID     string    `json:"sandbox_id"`
	ExpiresAt     time.Time `json:"expires_at"`
	HardExpiresAt time.Time `json:"hard_expires_at"`
}

// RefreshSandbox refreshes the TTL and HardTTL of a sandbox
func (s *SandboxService) RefreshSandbox(ctx context.Context, sandboxID string, req *RefreshRequest) (*RefreshResponse, error) {
	s.logger.Info("Refreshing sandbox TTL", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	// Parse original config to get TTL and HardTTL values
	var originalConfig SandboxConfig
	if configJSON := pod.Annotations[controller.AnnotationConfig]; configJSON != "" {
		if err := json.Unmarshal([]byte(configJSON), &originalConfig); err != nil {
			s.logger.Warn("Failed to parse original config, using defaults", zap.Error(err))
		}
	}

	// Update pod annotation
	podCopy := pod.DeepCopy()
	if podCopy.Annotations == nil {
		podCopy.Annotations = make(map[string]string)
	}
	now := s.clock.Now()
	var ttlDuration time.Duration

	// Determine the TTL to apply. Explicit duration enables TTL for that duration; otherwise use original config/default.
	var ttlToApply *int32
	if req != nil && req.Duration > 0 {
		ttlToApply = int32Ptr(req.Duration)
	} else if originalConfig.TTL != nil {
		ttlToApply = originalConfig.TTL
	} else if s.config.DefaultTTL > 0 {
		ttlToApply = int32Ptr(int32(s.config.DefaultTTL.Seconds()))
	}
	if ttlToApply != nil && *ttlToApply > 0 {
		ttlDuration = time.Duration(*ttlToApply) * time.Second
	}
	setExpirationAnnotation(podCopy.Annotations, now, ttlToApply)

	newExpiresAt := parseRFC3339AnnotationTime(podCopy.Annotations, controller.AnnotationExpiresAt)

	// Also refresh HardTTL if configured.
	var newHardExpiresAt time.Time
	if originalConfig.HardTTL != nil && *originalConfig.HardTTL > 0 {
		setHardExpirationAnnotation(podCopy.Annotations, now, originalConfig.HardTTL)
		newHardExpiresAt = parseRFC3339AnnotationTime(podCopy.Annotations, controller.AnnotationHardExpiresAt)
		s.logger.Info("Refreshing hard TTL",
			zap.String("sandboxID", sandboxID),
			zap.Time("newHardExpiresAt", newHardExpiresAt),
			zap.Duration("hardTTLDuration", time.Duration(*originalConfig.HardTTL)*time.Second),
		)
	} else {
		delete(podCopy.Annotations, controller.AnnotationHardExpiresAt)
	}

	// Apply the update
	_, err = s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, podCopy, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("update pod: %w", err)
	}

	s.logger.Info("Sandbox TTL refreshed successfully",
		zap.String("sandboxID", sandboxID),
		zap.Time("newExpiresAt", newExpiresAt),
		zap.Duration("ttlDuration", ttlDuration),
	)

	return &RefreshResponse{
		SandboxID:     sandboxID,
		ExpiresAt:     newExpiresAt,
		HardExpiresAt: newHardExpiresAt,
	}, nil
}
