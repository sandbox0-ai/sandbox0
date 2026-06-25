package service

import (
	"context"
	"encoding/json"
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
					if err := s.cleanupDeletedSandbox(ctx, sandboxLifecycleInfoFromRecord(record), false); err != nil {
						return fmt.Errorf("cleanup deleted sandbox record: %w", err)
					}
				}
				return s.sandboxStore.MarkSandboxDeleted(ctx, sandboxID, s.clock.Now())
			}
			return nil
		}
		return fmt.Errorf("get pod: %w", err)
	}

	pod, err = s.ensureSandboxDeletionFinalizer(ctx, pod)
	if err != nil {
		return fmt.Errorf("ensure sandbox cleanup finalizer: %w", err)
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

// PauseSandboxRuntime accepts a pause request and schedules checkpoint work.
func (s *SandboxService) PauseSandboxRuntime(ctx context.Context, sandboxID string) error {
	_, err := s.RequestPauseSandboxRuntime(ctx, sandboxID)
	return err
}

// RequestPauseSandboxRuntime records a durable pausing state and returns without
// waiting for rootfs checkpoint upload.
func (s *SandboxService) RequestPauseSandboxRuntime(ctx context.Context, sandboxID string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("sandbox service is nil")
	}
	if s.sandboxStore == nil {
		if err := s.pauseSandboxRuntime(ctx, sandboxID, true); err != nil {
			return "", err
		}
		return SandboxStatusPaused, nil
	}

	status := SandboxStatusPausing
	err := s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx SandboxStoreTx, record *SandboxRecord) error {
		switch record.Status {
		case SandboxStatusDeleted:
			return k8serrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
		case SandboxStatusPaused:
			status = SandboxStatusPaused
			return nil
		case SandboxStatusPausing:
			status = SandboxStatusPausing
			return nil
		case SandboxStatusStarting, SandboxStatusResuming:
			return k8serrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID, fmt.Errorf("sandbox lifecycle operation %q is in progress", record.Status))
		}

		pod, err := s.getSandboxPod(lockCtx, sandboxID)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				status = SandboxStatusPaused
				return tx.MarkRuntimePaused(lockCtx, sandboxID, 0, s.clock.Now())
			}
			return fmt.Errorf("get pod: %w", err)
		}
		if !s.config.CtldEnabled || s.ctldClient == nil {
			return ErrSandboxCheckpointRequiresCtld
		}
		generation := runtimeGenerationFromPod(pod)
		status = SandboxStatusPausing
		return tx.SaveRuntime(lockCtx, sandboxID, pod.Namespace, pod.Name, SandboxStatusPausing, generation, parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt), parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt))
	})
	if err != nil {
		if errors.Is(err, ErrSandboxRecordNotFound) {
			if fallbackErr := s.pauseSandboxRuntime(ctx, sandboxID, true); fallbackErr != nil {
				return "", fallbackErr
			}
			return SandboxStatusPaused, nil
		}
		return "", err
	}
	if status == SandboxStatusPausing {
		s.enqueueSandboxPause(sandboxID)
	}
	return status, nil
}

func (s *SandboxService) enqueueSandboxPause(sandboxID string) {
	if s == nil || strings.TrimSpace(sandboxID) == "" {
		return
	}
	if s.pauseEnqueuer != nil {
		s.pauseEnqueuer.EnqueueSandboxPause(sandboxID)
		return
	}
	go func() {
		if err := s.CompletePausingSandboxRuntime(context.Background(), sandboxID); err != nil && s.logger != nil {
			s.logger.Warn("Async sandbox pause completion failed",
				zap.String("sandboxID", sandboxID),
				zap.Error(err),
			)
		}
	}()
}

func (s *SandboxService) pauseSandboxRuntime(ctx context.Context, sandboxID string, saveRootFS bool) error {
	if s.logger != nil {
		s.logger.Info("Pausing sandbox runtime", zap.String("sandboxID", sandboxID))
	}
	pause := func(ctx context.Context, tx SandboxStoreTx, record *SandboxRecord) error {
		if record != nil {
			switch record.Status {
			case SandboxStatusDeleted:
				return k8serrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
			case SandboxStatusPaused:
				return nil
			case SandboxStatusStarting, SandboxStatusPausing, SandboxStatusResuming:
				if saveRootFS {
					return k8serrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID, fmt.Errorf("sandbox lifecycle operation %q is in progress", record.Status))
				}
			}
		}
		pod, err := s.getSandboxPod(ctx, sandboxID)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				if tx != nil {
					return tx.MarkRuntimePaused(ctx, sandboxID, 0, s.clock.Now())
				}
				return nil
			}
			return fmt.Errorf("get pod: %w", err)
		}
		generation := runtimeGenerationFromPod(pod)
		if saveRootFS {
			if s == nil || !s.config.CtldEnabled || s.ctldClient == nil {
				return ErrSandboxCheckpointRequiresCtld
			}
			if err := s.saveSandboxRootFSCheckpoint(ctx, pod, record, tx); err != nil {
				return err
			}
		}
		pod, err = s.ensureSandboxDeletionFinalizer(ctx, pod)
		if err != nil {
			return fmt.Errorf("ensure sandbox cleanup finalizer: %w", err)
		}
		if err := s.k8sClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
			return fmt.Errorf("delete runtime pod: %w", err)
		}
		if tx != nil {
			return tx.MarkRuntimePaused(ctx, sandboxID, generation, s.clock.Now())
		}
		return nil
	}
	if s.sandboxStore != nil {
		if err := s.sandboxStore.WithSandboxLock(ctx, sandboxID, pause); err != nil {
			if errors.Is(err, ErrSandboxRecordNotFound) {
				return pause(ctx, nil, nil)
			}
			return err
		}
		return nil
	}
	return pause(ctx, nil, nil)
}

// CompletePausingSandboxRuntime finishes a previously accepted durable pause.
func (s *SandboxService) CompletePausingSandboxRuntime(ctx context.Context, sandboxID string) error {
	if s == nil {
		return nil
	}
	if s.sandboxStore == nil {
		return s.pauseSandboxRuntime(ctx, sandboxID, true)
	}

	var record *SandboxRecord
	if err := s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(_ context.Context, _ SandboxStoreTx, locked *SandboxRecord) error {
		if locked.Status != SandboxStatusPausing {
			return nil
		}
		record = cloneSandboxRecordForLifecycle(locked)
		return nil
	}); err != nil {
		if errors.Is(err, ErrSandboxRecordNotFound) {
			return nil
		}
		return err
	}
	if record == nil {
		return nil
	}

	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return s.markPausingRuntimePaused(ctx, sandboxID, record.RuntimeGeneration, nil)
		}
		return fmt.Errorf("get pod: %w", err)
	}
	generation := runtimeGenerationFromPod(pod)
	if generation != record.RuntimeGeneration {
		return fmt.Errorf("sandbox runtime generation changed during pause: record=%d pod=%d", record.RuntimeGeneration, generation)
	}
	if record.CurrentPodName != "" && pod.Name != record.CurrentPodName {
		return k8serrors.NewConflict(schema.GroupResource{Resource: "pod"}, pod.Name, fmt.Errorf("pausing sandbox points at runtime pod %s", record.CurrentPodName))
	}
	if record.CurrentPodNamespace != "" && pod.Namespace != record.CurrentPodNamespace {
		return k8serrors.NewConflict(schema.GroupResource{Resource: "pod"}, pod.Name, fmt.Errorf("pausing sandbox points at runtime namespace %s", record.CurrentPodNamespace))
	}
	if !s.config.CtldEnabled || s.ctldClient == nil {
		return ErrSandboxCheckpointRequiresCtld
	}
	rootFSState, err := s.prepareSandboxRootFSCheckpoint(ctx, pod, record)
	if err != nil {
		return err
	}
	stillPausing, err := s.sandboxStillPausing(ctx, sandboxID, generation)
	if err != nil || !stillPausing {
		return err
	}
	pod, err = s.ensureSandboxDeletionFinalizer(ctx, pod)
	if err != nil {
		return fmt.Errorf("ensure sandbox cleanup finalizer: %w", err)
	}
	if err := s.k8sClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete runtime pod: %w", err)
	}
	return s.markPausingRuntimePaused(ctx, sandboxID, generation, rootFSState)
}

func (s *SandboxService) sandboxStillPausing(ctx context.Context, sandboxID string, generation int64) (bool, error) {
	if s == nil || s.sandboxStore == nil {
		return true, nil
	}
	record, err := s.sandboxStore.GetSandbox(ctx, sandboxID)
	if err != nil {
		return false, err
	}
	return record != nil && record.Status == SandboxStatusPausing && record.RuntimeGeneration == generation, nil
}

func (s *SandboxService) markPausingRuntimePaused(ctx context.Context, sandboxID string, generation int64, rootFSState *SandboxRootFSState) error {
	if s == nil || s.sandboxStore == nil {
		return nil
	}
	return s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx SandboxStoreTx, record *SandboxRecord) error {
		if record.Status != SandboxStatusPausing || record.RuntimeGeneration != generation {
			return nil
		}
		if rootFSState != nil {
			if err := tx.SaveRootFSState(lockCtx, rootFSState); err != nil {
				return err
			}
		}
		return tx.MarkRuntimePaused(lockCtx, sandboxID, generation, s.clock.Now())
	})
}

func cloneSandboxRecordForLifecycle(record *SandboxRecord) *SandboxRecord {
	if record == nil {
		return nil
	}
	clone := *record
	if record.Mounts != nil {
		clone.Mounts = append([]ClaimMount(nil), record.Mounts...)
	}
	if record.Config.Services != nil {
		clone.Config.Services = append([]SandboxAppService(nil), record.Config.Services...)
	}
	return &clone
}

// PauseSandboxByID implements controller.SandboxRuntimePauser.
func (s *SandboxService) PauseSandboxByID(ctx context.Context, sandboxID string) error {
	return s.PauseSandboxRuntime(ctx, sandboxID)
}

// ListHardExpiredSandboxIDs returns durable sandboxes whose hard TTL has expired.
func (s *SandboxService) ListHardExpiredSandboxIDs(ctx context.Context, now time.Time, limit int) ([]string, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, nil
	}
	records, err := s.sandboxStore.ListHardExpiredSandboxes(ctx, now, limit)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(records))
	for _, record := range records {
		if record != nil && strings.TrimSpace(record.ID) != "" {
			ids = append(ids, record.ID)
		}
	}
	return ids, nil
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
		if record != nil {
			if record.Status == SandboxStatusDeleted {
				return nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
			}
			if recordLifecycleStatusOverridesPod(record.Status) {
				return s.recordToSandbox(record), nil
			}
		}
	}
	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			if record != nil {
				return s.recordToSandbox(record), nil
			}
			return nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
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
				return s.updatePausedSandboxRecord(ctx, record, cfg)
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
		if err := validateSandboxConfigLifecycle(merged.TTL, merged.HardTTL); err != nil {
			return err
		}
		if cfg.AutoResume != nil {
			merged.AutoResume = cfg.AutoResume
		}
		if cfg.EnvVars != nil {
			merged.EnvVars = cloneEnvVars(cfg.EnvVars)
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

	if cfg.EnvVars != nil {
		if err := s.updateActiveSandboxEnvVars(ctx, updatedPod, sandboxID, sandboxEnvVarsFromPod(updatedPod)); err != nil {
			return nil, fmt.Errorf("update sandbox env vars: %w", err)
		}
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

func (s *SandboxService) updatePausedSandboxRecord(ctx context.Context, record *SandboxRecord, cfg *SandboxUpdateConfig) (*Sandbox, error) {
	if record == nil {
		return nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, "")
	}
	merged := record.Config
	now := s.clock.Now()
	nextExpiresAt := record.ExpiresAt
	nextHardExpiresAt := record.HardExpiresAt
	if cfg.TTL != nil {
		merged.TTL = cfg.TTL
		nextExpiresAt = expirationFromTTL(now, cfg.TTL)
	}
	if cfg.HardTTL != nil {
		merged.HardTTL = cfg.HardTTL
		nextHardExpiresAt = expirationFromTTL(now, cfg.HardTTL)
	}
	if err := validateSandboxConfigLifecycle(merged.TTL, merged.HardTTL); err != nil {
		return nil, err
	}
	record.ExpiresAt = nextExpiresAt
	record.HardExpiresAt = nextHardExpiresAt
	if cfg.AutoResume != nil {
		merged.AutoResume = cfg.AutoResume
	}
	if cfg.EnvVars != nil {
		merged.EnvVars = cloneEnvVars(cfg.EnvVars)
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

func (s *SandboxService) updateActiveSandboxEnvVars(ctx context.Context, pod *corev1.Pod, sandboxID string, envVars map[string]string) error {
	if s.procdClient == nil {
		return fmt.Errorf("procd client is not configured")
	}
	if s.internalTokenGenerator == nil {
		return fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return fmt.Errorf("procd address: %w", err)
	}
	currentSandboxID := sandboxIDFromPod(pod)
	if currentSandboxID == "" {
		currentSandboxID = sandboxID
	}
	teamID, userID := "", ""
	if pod != nil && pod.Annotations != nil {
		teamID = pod.Annotations[controller.AnnotationTeamID]
		userID = pod.Annotations[controller.AnnotationUserID]
	}
	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, currentSandboxID)
	if err != nil {
		return fmt.Errorf("generate internal token: %w", err)
	}
	_, err = s.procdClient.UpdateSandboxEnvVars(ctx, procdAddress, UpdateSandboxEnvVarsRequest{
		EnvVars: cloneEnvVars(envVars),
	}, internalToken)
	return err
}

func sandboxEnvVarsFromPod(pod *corev1.Pod) map[string]string {
	if pod == nil || pod.Annotations == nil {
		return nil
	}
	cfg := parseSandboxConfig(pod.Annotations[controller.AnnotationConfig])
	return cloneEnvVars(cfg.EnvVars)
}

func expirationFromTTL(now time.Time, ttl *int32) time.Time {
	if ttl == nil || *ttl <= 0 {
		return time.Time{}
	}
	return now.Add(time.Duration(*ttl) * time.Second)
}

func sandboxHardExpired(hardExpiresAt time.Time, now time.Time) bool {
	return !hardExpiresAt.IsZero() && !hardExpiresAt.After(now)
}

func (s *SandboxService) now() time.Time {
	if s != nil && s.clock != nil {
		return s.clock.Now()
	}
	return time.Now()
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
	return &Sandbox{
		ID:                sandboxID,
		TemplateID:        sandboxTemplateIDFromLabels(pod.Labels),
		TeamID:            pod.Annotations[controller.AnnotationTeamID],
		UserID:            pod.Annotations[controller.AnnotationUserID],
		InternalAddr:      internalAddr,
		Status:            status,
		Paused:            status == SandboxStatusPaused,
		AutoResume:        autoResume,
		Services:          cfg.Services,
		Mounts:            parseClaimMounts(pod.Annotations[controller.AnnotationMounts]),
		PodName:           pod.Name,
		RuntimeGeneration: runtimeGenerationFromPod(pod),
		ExpiresAt:         expiresAt,
		HardExpiresAt:     hardExpiresAt,
		ClaimedAt:         claimedAt,
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
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
	return &Sandbox{
		ID:                record.ID,
		TemplateID:        record.TemplateID,
		TeamID:            record.TeamID,
		UserID:            record.UserID,
		Status:            record.Status,
		Paused:            record.Status == SandboxStatusPaused,
		AutoResume:        autoResume,
		Services:          record.Config.Services,
		Mounts:            record.Mounts,
		PodName:           record.CurrentPodName,
		RuntimeGeneration: record.RuntimeGeneration,
		ExpiresAt:         record.ExpiresAt,
		HardExpiresAt:     record.HardExpiresAt,
		ClaimedAt:         record.ClaimedAt,
		CreatedAt:         record.CreatedAt,
		UpdatedAt:         record.UpdatedAt,
	}
}

func recordLifecycleStatusOverridesPod(status string) bool {
	switch status {
	case SandboxStatusPausing, SandboxStatusResuming:
		return true
	default:
		return false
	}
}

func sandboxLifecycleInfoFromRecord(record *SandboxRecord) SandboxLifecycleInfo {
	if record == nil {
		return SandboxLifecycleInfo{}
	}
	info := SandboxLifecycleInfo{
		Namespace:         record.CurrentPodNamespace,
		PodName:           record.CurrentPodName,
		SandboxID:         record.ID,
		TeamID:            record.TeamID,
		UserID:            record.UserID,
		RuntimeGeneration: record.RuntimeGeneration,
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
	if req != nil {
		if req.Duration < 0 {
			return nil, fmt.Errorf("%w: duration must be >= 0", ErrInvalidClaimRequest)
		}
		if req.Duration > 0 {
			ttlToApply = int32Ptr(req.Duration)
		}
	}
	if ttlToApply == nil && originalConfig.TTL != nil {
		ttlToApply = originalConfig.TTL
	} else if ttlToApply == nil && s.config.DefaultTTL > 0 {
		ttlToApply = int32Ptr(int32(s.config.DefaultTTL.Seconds()))
	}
	if ttlToApply != nil && *ttlToApply > 0 {
		ttlDuration = time.Duration(*ttlToApply) * time.Second
	}
	if err := validateSandboxConfigLifecycle(ttlToApply, originalConfig.HardTTL); err != nil {
		return nil, err
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
