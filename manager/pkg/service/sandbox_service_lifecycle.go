package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
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
			return nil
		}
		return fmt.Errorf("get pod: %w", err)
	}
	s.thawSandboxBeforeTermination(ctx, pod, sandboxID)

	pod, err = s.ensureSandboxDeletionFinalizer(ctx, pod)
	if err != nil {
		return fmt.Errorf("ensure sandbox cleanup finalizer: %w", err)
	}

	err = s.k8sClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		s.logger.Info("Sandbox already terminated", zap.String("sandboxID", sandboxID))
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete pod: %w", err)
	}

	s.logger.Info("Sandbox termination requested", zap.String("sandboxID", sandboxID), zap.String("pod", pod.Name))

	return nil
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

// GetSandbox gets a sandbox by ID
func (s *SandboxService) GetSandbox(ctx context.Context, sandboxID string) (*Sandbox, error) {
	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
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
				SandboxID:        updatedPod.Name,
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

	return s.podToSandbox(ctx, updatedPod, sandboxID), nil
}

func (s *SandboxService) getSandboxPod(ctx context.Context, sandboxID string) (*corev1.Pod, error) {
	if s.sandboxIndex != nil {
		if namespace, ok := s.sandboxIndex.GetNamespace(sandboxID); ok {
			return s.podLister.Pods(namespace).Get(sandboxID)
		}
	}

	pods, err := s.podLister.List(labels.SelectorFromSet(map[string]string{
		controller.LabelSandboxID: sandboxID,
	}))
	if err != nil {
		return nil, err
	}
	if len(pods) == 0 {
		return nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "pod"}, sandboxID)
	}
	return pods[0], nil
}

// podToSandbox converts a pod to a sandbox object
func (s *SandboxService) podToSandbox(ctx context.Context, pod *corev1.Pod, sandboxID string) *Sandbox {
	status := s.podPhaseToSandboxStatus(pod.Status.Phase)

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
