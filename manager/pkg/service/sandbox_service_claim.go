package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/util/retry"
)

const (
	volumePortalBindRetryWindow   = 5 * time.Second
	volumePortalBindRetryInterval = 100 * time.Millisecond
)

// ClaimRequest represents a sandbox claim request
type ClaimRequest struct {
	TeamID   string
	UserID   string
	Template string         `json:"template"`
	Config   *SandboxConfig `json:"config,omitempty"`
	Mounts   []ClaimMount   `json:"mounts,omitempty"`
}

type ClaimMount struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
	MountPoint      string `json:"mount_point"`
}

type BootstrapMountStatus struct {
	SandboxVolumeID     string `json:"sandboxvolume_id"`
	MountPoint          string `json:"mount_point"`
	State               string `json:"state"`
	MountedAt           string `json:"mounted_at,omitempty"`
	MountedDurationSecs int64  `json:"mounted_duration_sec,omitempty"`
	ErrorCode           string `json:"error_code,omitempty"`
	ErrorMessage        string `json:"error_message,omitempty"`
}

// SandboxConfig represents sandbox configuration
type SandboxConfig struct {
	EnvVars      map[string]string              `json:"env_vars,omitempty"`
	TTL          *int32                         `json:"ttl,omitempty"`      // Time-to-live in seconds (0 disables)
	HardTTL      *int32                         `json:"hard_ttl,omitempty"` // Hard time-to-live in seconds (0 disables)
	Network      *v1alpha1.SandboxNetworkPolicy `json:"network,omitempty"`
	Webhook      *WebhookConfig                 `json:"webhook,omitempty"`
	AutoResume   *bool                          `json:"auto_resume,omitempty"`
	ExposedPorts []ExposedPortConfig            `json:"exposed_ports,omitempty"`
}

// SandboxUpdateConfig represents sandbox configuration fields that can be updated at runtime.
// Unlike SandboxConfig, env_vars and webhook are excluded as they only affect new processes
// or require restart to take effect.
type SandboxUpdateConfig struct {
	TTL          *int32                         `json:"ttl,omitempty"`
	HardTTL      *int32                         `json:"hard_ttl,omitempty"`
	Network      *v1alpha1.SandboxNetworkPolicy `json:"network,omitempty"`
	AutoResume   *bool                          `json:"auto_resume,omitempty"`
	ExposedPorts []ExposedPortConfig            `json:"exposed_ports,omitempty"`
}

type ExposedPortConfig struct {
	Port   int  `json:"port"`
	Resume bool `json:"resume"`
}

func int32Ptr(v int32) *int32 {
	return &v
}

func cloneSandboxConfig(cfg *SandboxConfig) *SandboxConfig {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	if cloned.Network != nil {
		cloned.Network = sanitizedNetworkPolicyForPersistence(cloned.Network)
	}
	return &cloned
}

func (s *SandboxService) claimConfigForPersistence(cfg *SandboxConfig) *SandboxConfig {
	persisted := cloneSandboxConfig(cfg)
	if persisted == nil {
		if s.config.DefaultTTL <= 0 {
			return nil
		}
		persisted = &SandboxConfig{}
	}
	if persisted.TTL == nil && s.config.DefaultTTL > 0 {
		persisted.TTL = int32Ptr(int32(s.config.DefaultTTL.Seconds()))
	}
	return persisted
}

func setExpirationAnnotation(annotations map[string]string, now time.Time, ttl *int32) {
	if annotations == nil {
		return
	}
	if ttl == nil || *ttl <= 0 {
		delete(annotations, controller.AnnotationExpiresAt)
		return
	}
	expiresAt := now.Add(time.Duration(*ttl) * time.Second)
	annotations[controller.AnnotationExpiresAt] = expiresAt.Format(time.RFC3339)
}

func setHardExpirationAnnotation(annotations map[string]string, now time.Time, hardTTL *int32) {
	if annotations == nil {
		return
	}
	if hardTTL == nil || *hardTTL <= 0 {
		delete(annotations, controller.AnnotationHardExpiresAt)
		return
	}
	hardExpiresAt := now.Add(time.Duration(*hardTTL) * time.Second)
	annotations[controller.AnnotationHardExpiresAt] = hardExpiresAt.Format(time.RFC3339)
}

func validateClaimMounts(req *ClaimRequest) error {
	if req == nil {
		return nil
	}
	normalized, err := normalizeClaimMounts(req.Mounts)
	if err != nil {
		return err
	}
	req.Mounts = normalized
	return nil
}

func normalizeClaimMounts(mounts []ClaimMount) ([]ClaimMount, error) {
	if len(mounts) == 0 {
		return nil, nil
	}
	normalized := append([]ClaimMount(nil), mounts...)
	seenVolumes := make(map[string]struct{}, len(normalized))
	seenMountPoints := make(map[string]string, len(normalized))
	for i := range normalized {
		mount := &normalized[i]
		mount.SandboxVolumeID = strings.TrimSpace(mount.SandboxVolumeID)
		if mount.SandboxVolumeID == "" {
			return nil, fmt.Errorf("%w: mounts[%d].sandboxvolume_id is required", ErrInvalidClaimRequest, i)
		}
		cleanMountPoint := filepath.Clean(strings.TrimSpace(mount.MountPoint))
		if !filepath.IsAbs(cleanMountPoint) || cleanMountPoint == string(filepath.Separator) || strings.Contains(cleanMountPoint, "..") {
			return nil, fmt.Errorf("%w: mounts[%d].mount_point is invalid", ErrInvalidClaimRequest, i)
		}
		if cleanMountPoint == webhookStateMountPoint || strings.HasPrefix(cleanMountPoint, webhookStateMountPoint+string(filepath.Separator)) {
			return nil, fmt.Errorf("%w: mounts[%d].mount_point uses a sandbox0 reserved path", ErrInvalidClaimRequest, i)
		}
		if _, exists := seenVolumes[mount.SandboxVolumeID]; exists {
			return nil, fmt.Errorf("%w: duplicate sandboxvolume_id %q in claim mounts", ErrInvalidClaimRequest, mount.SandboxVolumeID)
		}
		if existing, exists := seenMountPoints[cleanMountPoint]; exists && existing != mount.SandboxVolumeID {
			return nil, fmt.Errorf("%w: duplicate mount_point %q in claim mounts", ErrInvalidClaimRequest, cleanMountPoint)
		}
		mount.MountPoint = cleanMountPoint
		seenVolumes[mount.SandboxVolumeID] = struct{}{}
		seenMountPoints[cleanMountPoint] = mount.SandboxVolumeID
	}
	return normalized, nil
}

// WebhookConfig represents outbound webhook configuration.
type WebhookConfig struct {
	URL      string `json:"url"`
	Secret   string `json:"secret,omitempty"`
	WatchDir string `json:"watch_dir,omitempty"`
}

// ClaimResponse represents a sandbox claim response
type ClaimResponse struct {
	SandboxID       string                 `json:"sandbox_id"`
	Status          string                 `json:"status"`
	ProcdAddress    string                 `json:"procd_address"`
	PodName         string                 `json:"pod_name"`
	Template        string                 `json:"template"`
	ClusterId       *string                `json:"cluster_id,omitempty"`
	BootstrapMounts []BootstrapMountStatus `json:"bootstrap_mounts,omitempty"`
}

// ClaimSandbox claims a sandbox from the idle pool or creates a new one
func (s *SandboxService) ClaimSandbox(ctx context.Context, req *ClaimRequest) (*ClaimResponse, error) {
	start := time.Now()
	metrics := s.metrics
	phaseStarted := time.Now()
	canonicalTemplateID, err := naming.CanonicalTemplateID(req.Template)
	s.observeClaimPhase(req.Template, "unknown", "canonicalize_template", phaseStarted, err)
	if err != nil {
		return nil, err
	}
	req.Template = canonicalTemplateID
	phaseStarted = time.Now()
	if err := validateClaimMounts(req); err != nil {
		s.observeClaimPhase(req.Template, "unknown", "validate_claim_mounts", phaseStarted, err)
		return nil, err
	}
	s.observeClaimPhase(req.Template, "unknown", "validate_claim_mounts", phaseStarted, nil)
	s.logger.Info("Claiming sandbox",
		zap.String("template", req.Template),
		zap.String("teamID", req.TeamID),
	)

	// Resolve tenant template name:
	// prefer team-scoped template, fall back to public, and always enforce ownership checks.
	phaseStarted = time.Now()
	resolvedName := req.Template
	var template *v1alpha1.SandboxTemplate

	if req.TeamID != "" {
		privateName := naming.TemplateNameForCluster(naming.ScopeTeam, req.TeamID, req.Template)
		privateNamespace, nsErr := naming.TemplateNamespaceForTeam(req.TeamID)
		if nsErr != nil {
			s.observeClaimPhase(req.Template, "unknown", "resolve_template", phaseStarted, nsErr)
			return nil, fmt.Errorf("resolve template namespace for %s: %w", privateName, nsErr)
		}
		t, getErr := s.templateLister.Get(privateNamespace, privateName)
		if getErr == nil {
			template = t
			resolvedName = privateName
		}
	}

	if template == nil {
		publicNamespace, nsErr := naming.TemplateNamespaceForBuiltin(req.Template)
		if nsErr != nil {
			s.observeClaimPhase(req.Template, "unknown", "resolve_template", phaseStarted, nsErr)
			return nil, fmt.Errorf("resolve template namespace for %s: %w", req.Template, nsErr)
		}
		template, err = s.templateLister.Get(publicNamespace, req.Template)
		if err != nil {
			s.observeClaimPhase(req.Template, "unknown", "resolve_template", phaseStarted, err)
			if k8serrors.IsNotFound(err) {
				return nil, fmt.Errorf("template %s not found in namespace %s", req.Template, publicNamespace)
			}
			return nil, fmt.Errorf("get template: %w", err)
		}
	}

	// Enforce tenant isolation (best-effort based on scheduler-projected metadata).
	if template.Labels != nil && template.Labels["sandbox0.ai/template-scope"] == naming.ScopeTeam {
		teamID := ""
		if template.Annotations != nil {
			teamID = template.Annotations["sandbox0.ai/template-team-id"]
		}
		if teamID != "" && teamID != req.TeamID {
			err := fmt.Errorf("forbidden: template belongs to a different team")
			s.observeClaimPhase(req.Template, "unknown", "resolve_template", phaseStarted, err)
			return nil, err
		}
	}
	s.observeClaimPhase(req.Template, "unknown", "resolve_template", phaseStarted, nil)

	phaseStarted = time.Now()
	if err := validateClaimMountsForTemplate(req, template); err != nil {
		s.observeClaimPhase(req.Template, "unknown", "validate_template_mounts", phaseStarted, err)
		return nil, err
	}
	s.observeClaimPhase(req.Template, "unknown", "validate_template_mounts", phaseStarted, nil)

	_ = resolvedName // reserved for audit/debugging (name used is template.ObjectMeta.Name)

	// Try to claim an idle pod first
	phaseStarted = time.Now()
	pod, err := s.claimIdlePod(ctx, template, req)
	claimIdleType := "hot"
	if pod == nil {
		claimIdleType = "cold"
	}
	if err != nil {
		claimIdleType = "unknown"
	}
	s.observeClaimPhase(req.Template, claimIdleType, "claim_idle_pod", phaseStarted, err)
	if err != nil {
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("claim idle pod: %w", err)
	}
	claimType := "hot"

	// If no idle pod available, create a new one (cold start)
	if pod == nil {
		claimType = "cold"
		s.logger.Info("No idle pod available, creating new pod",
			zap.String("template", req.Template),
		)

		// Trigger async scale-up to replenish the idle pool
		// This runs in a goroutine to not block the cold claim response
		if s.autoScaler != nil {
			go func() {
				scaleCtx := context.Background()
				scaleDecision, scaleErr := s.autoScaler.OnColdClaim(scaleCtx, template)
				if scaleErr != nil {
					s.logger.Warn("Auto scale failed during cold claim",
						zap.String("template", req.Template),
						zap.Error(scaleErr),
					)
				} else if scaleDecision != nil && scaleDecision.ShouldScale {
					s.logger.Info("Auto scale triggered",
						zap.String("template", req.Template),
						zap.Int32("delta", scaleDecision.Delta),
						zap.String("reason", scaleDecision.Reason),
					)
				}
			}()
		}

		phaseStarted = time.Now()
		pod, err = s.createNewPod(ctx, template, req)
		s.observeClaimPhase(req.Template, claimType, "create_new_pod", phaseStarted, err)
		if err != nil {
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, fmt.Errorf("create new pod: %w", err)
		}
	}

	// Note: Network policies are stored in pod annotations.
	// They are set in claimIdlePod() and createNewPod() methods. Hot claims have
	// already selected a Kubernetes-ready idle pod; cold claims use the faster
	// claim-ready path below and let Kubernetes PodReady catch up asynchronously.
	if claimType == "cold" {
		phaseStarted = time.Now()
		readyPod, err := s.waitForPodClaimReady(ctx, pod.Namespace, pod.Name)
		s.observeClaimPhase(req.Template, claimType, "wait_for_pod_claim_ready", phaseStarted, err)
		if err != nil {
			s.requestSandboxDeletionAfterClaimFailure(pod, "claim readiness failed")
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, fmt.Errorf("wait for pod claim readiness: %w", err)
		}
		pod = readyPod
		s.refreshSandboxProbeConditionsAsync(pod)
	}

	phaseStarted = time.Now()
	portalMounts, err := s.bindVolumePortals(ctx, pod, req, template)
	s.observeClaimPhase(req.Template, claimType, "bind_volume_portals", phaseStarted, err)
	if err != nil {
		s.requestSandboxDeletionAfterClaimFailure(pod, "volume portal bind failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("bind volume portals: %w", err)
	}
	phaseStarted = time.Now()
	if err := s.bindWebhookStatePortal(ctx, pod, req); err != nil {
		s.observeClaimPhase(req.Template, claimType, "bind_webhook_state_portal", phaseStarted, err)
		s.requestSandboxDeletionAfterClaimFailure(pod, "webhook state portal bind failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("bind webhook state portal: %w", err)
	}
	s.observeClaimPhase(req.Template, claimType, "bind_webhook_state_portal", phaseStarted, nil)

	phaseStarted = time.Now()
	procdAddress, err := s.prodAddress(ctx, pod)
	s.observeClaimPhase(req.Template, claimType, "resolve_procd_address", phaseStarted, err)
	if err != nil {
		s.requestSandboxDeletionAfterClaimFailure(pod, "procd address resolution failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	phaseStarted = time.Now()
	if _, err := s.initializeProcd(ctx, pod, req, procdAddress); err != nil {
		s.observeClaimPhase(req.Template, claimType, "initialize_procd", phaseStarted, err)
		s.requestSandboxDeletionAfterClaimFailure(pod, "procd initialization failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("initialize procd: %w", err)
	}
	s.observeClaimPhase(req.Template, claimType, "initialize_procd", phaseStarted, nil)

	if metrics != nil {
		metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "success").Inc()
		metrics.SandboxClaimDuration.WithLabelValues(req.Template, claimType).Observe(time.Since(start).Seconds())
	}

	return &ClaimResponse{
		SandboxID:       pod.Name,
		Status:          "starting",
		ProcdAddress:    procdAddress,
		PodName:         pod.Name,
		Template:        req.Template,
		ClusterId:       template.Spec.ClusterId,
		BootstrapMounts: portalMounts,
	}, nil
}

func (s *SandboxService) observeClaimPhase(template, claimType, phase string, started time.Time, err error) {
	if s == nil || s.metrics == nil || s.metrics.SandboxClaimPhaseDuration == nil {
		return
	}
	if claimType == "" {
		claimType = "unknown"
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	s.metrics.SandboxClaimPhaseDuration.WithLabelValues(template, claimType, phase, status).Observe(time.Since(started).Seconds())
}

func validateClaimMountsForTemplate(req *ClaimRequest, template *v1alpha1.SandboxTemplate) error {
	if req == nil || len(req.Mounts) == 0 {
		return nil
	}
	allowed := declaredVolumeMountsByPath(template)
	for i := range req.Mounts {
		mountPoint := filepath.Clean(req.Mounts[i].MountPoint)
		if _, ok := allowed[mountPoint]; !ok {
			return fmt.Errorf("%w: mounts[%d].mount_point %q is not declared by template", ErrInvalidClaimRequest, i, mountPoint)
		}
	}
	return nil
}

func declaredVolumeMountsByPath(template *v1alpha1.SandboxTemplate) map[string]v1alpha1.VolumeMountSpec {
	if template == nil || len(template.Spec.VolumeMounts) == 0 {
		return nil
	}
	out := make(map[string]v1alpha1.VolumeMountSpec, len(template.Spec.VolumeMounts))
	for _, item := range template.Spec.VolumeMounts {
		mountPath := filepath.Clean(strings.TrimSpace(item.MountPath))
		if mountPath == "." || mountPath == string(filepath.Separator) || !filepath.IsAbs(mountPath) {
			continue
		}
		if mountPath == webhookStateMountPoint || strings.HasPrefix(mountPath, webhookStateMountPoint+string(filepath.Separator)) {
			continue
		}
		out[mountPath] = item
	}
	return out
}

func (s *SandboxService) bindVolumePortals(ctx context.Context, pod *corev1.Pod, req *ClaimRequest, template *v1alpha1.SandboxTemplate) ([]BootstrapMountStatus, error) {
	if req == nil || len(req.Mounts) == 0 {
		return nil, nil
	}
	declared := declaredVolumeMountsByPath(template)
	out := make([]BootstrapMountStatus, 0, len(req.Mounts))
	for _, mount := range req.Mounts {
		mountPoint := filepath.Clean(mount.MountPoint)
		decl := declared[mountPoint]
		if err := s.validateVolumePortalAccess(ctx, req.TeamID, req.UserID, mount.SandboxVolumeID, decl); err != nil {
			return nil, err
		}
		resp, err := s.bindVolumePortal(ctx, pod, req.TeamID, req.UserID, req.TeamID, mount.SandboxVolumeID, mountPoint, decl.Name)
		if err != nil {
			return nil, err
		}
		out = append(out, BootstrapMountStatus{
			SandboxVolumeID:     resp.SandboxVolumeID,
			MountPoint:          resp.MountPoint,
			State:               "mounted",
			MountedAt:           resp.MountedAt,
			MountedDurationSecs: 0,
		})
	}
	return out, nil
}

func (s *SandboxService) validateVolumePortalAccess(ctx context.Context, teamID, userID, volumeID string, mount v1alpha1.VolumeMountSpec) error {
	if s.volumeMetadata == nil {
		return fmt.Errorf("volume metadata client is not configured")
	}
	info, err := s.volumeMetadata.Get(ctx, teamID, userID, volumeID)
	if err != nil {
		return fmt.Errorf("get volume metadata for %s: %w", volumeID, err)
	}
	accessMode := strings.ToUpper(strings.TrimSpace(info.AccessMode))
	if accessMode == "" {
		accessMode = "RWO"
	}
	switch accessMode {
	case "RWO":
		return nil
	case "ROX":
		if mount.ReadOnly {
			return nil
		}
		return fmt.Errorf("%w: volume %s is ROX but template mount %s is read-write", ErrInvalidClaimRequest, volumeID, mount.MountPath)
	case "RWX":
		return fmt.Errorf("%w: RWX volumes require the shared correctness path and cannot use node-local volume portals yet", ErrInvalidClaimRequest)
	default:
		return fmt.Errorf("%w: volume %s has invalid access_mode %q", ErrInvalidClaimRequest, volumeID, info.AccessMode)
	}
}

func (s *SandboxService) bindWebhookStatePortal(ctx context.Context, pod *corev1.Pod, req *ClaimRequest) error {
	if req == nil || s.getWebhookInfo(req) == nil || pod == nil || pod.Annotations == nil {
		return nil
	}
	volumeID := strings.TrimSpace(pod.Annotations[controller.AnnotationWebhookStateVolumeID])
	if volumeID == "" {
		return nil
	}
	_, err := s.bindVolumePortal(ctx, pod, req.TeamID, req.UserID, req.TeamID, volumeID, webhookStateMountPoint, volumeportal.WebhookStatePortalName)
	return err
}

func (s *SandboxService) prepareVolumePortalBind(ctx context.Context, req PrepareVolumePortalBindRequest) error {
	if s == nil || s.volumeMetadata == nil {
		return nil
	}
	preparer, ok := s.volumeMetadata.(SandboxVolumePortalPreparationClient)
	if !ok {
		return nil
	}
	return preparer.PrepareForVolumePortalBind(ctx, req)
}

func (s *SandboxService) bindVolumePortal(ctx context.Context, pod *corev1.Pod, teamID, userID, ownerTeamID, volumeID, mountPoint, portalName string) (*ctldapi.BindVolumePortalResponse, error) {
	if s == nil || s.ctldClient == nil {
		return nil, fmt.Errorf("ctld client is not configured")
	}
	if pod == nil {
		return nil, fmt.Errorf("pod is nil")
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return nil, err
	}
	if err := s.prepareVolumePortalBind(ctx, PrepareVolumePortalBindRequest{
		TeamID:         teamID,
		UserID:         userID,
		VolumeID:       volumeID,
		TargetCtldAddr: ctldAddress,
		Namespace:      pod.Namespace,
		PodName:        pod.Name,
		PodUID:         string(pod.UID),
		PortalName:     volumeportal.NormalizePortalName(portalName, mountPoint),
		MountPath:      mountPoint,
		SandboxID:      pod.Name,
		OwnerTeamID:    ownerTeamID,
	}); err != nil {
		if errors.Is(err, ErrVolumePortalBindConflict) {
			return nil, fmt.Errorf("%w: %v", ErrClaimConflict, err)
		}
		return nil, fmt.Errorf("prepare volume portal bind: %w", err)
	}
	resp, err := s.bindVolumePortalWithRetry(ctx, ctldAddress, ctldapi.BindVolumePortalRequest{
		Namespace:       pod.Namespace,
		PodName:         pod.Name,
		PodUID:          string(pod.UID),
		PortalName:      volumeportal.NormalizePortalName(portalName, mountPoint),
		MountPath:       mountPoint,
		SandboxID:       pod.Name,
		TeamID:          ownerTeamID,
		SandboxVolumeID: volumeID,
	})
	if err != nil {
		return nil, err
	}
	if s.logger != nil {
		s.logger.Info("Bound sandbox volume portal",
			zap.String("sandboxID", pod.Name),
			zap.String("teamID", teamID),
			zap.String("userID", userID),
			zap.String("volumeID", volumeID),
			zap.String("mountPoint", mountPoint),
		)
	}
	return resp, nil
}

func (s *SandboxService) bindVolumePortalWithRetry(ctx context.Context, ctldAddress string, req ctldapi.BindVolumePortalRequest) (*ctldapi.BindVolumePortalResponse, error) {
	if s == nil || s.ctldClient == nil {
		return nil, fmt.Errorf("ctld client is not configured")
	}

	deadline := time.Now().Add(volumePortalBindRetryWindow)
	for {
		resp, err := s.ctldClient.BindVolumePortal(ctx, ctldAddress, req)
		if err == nil {
			return resp, nil
		}
		if isVolumePortalBindConflictError(resp, err) {
			message := strings.TrimSpace(resp.Error)
			if message == "" {
				message = err.Error()
			}
			return nil, fmt.Errorf("%w: %s", ErrClaimConflict, message)
		}
		if !isVolumePortalPendingPublicationError(resp, err) {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, err
		}

		timer := time.NewTimer(volumePortalBindRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func isVolumePortalBindConflictError(resp *ctldapi.BindVolumePortalResponse, err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(message, "status 409") {
		return true
	}
	if resp == nil {
		return false
	}
	message = strings.ToLower(strings.TrimSpace(resp.Error))
	return strings.Contains(message, "already has an active owner") ||
		strings.Contains(message, "actively bound to a portal") ||
		strings.Contains(message, "already bound to") ||
		strings.Contains(message, "handoff already in progress")
}

func isVolumePortalPendingPublicationError(resp *ctldapi.BindVolumePortalResponse, err error) bool {
	if err == nil || resp == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(resp.Error))
	return strings.Contains(message, "is not published")
}

// claimIdlePod claims an idle pod from the pool
func (s *SandboxService) claimIdlePod(ctx context.Context, template *v1alpha1.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, error) {
	var claimedPod *corev1.Pod
	desiredTemplateHash, err := controller.TemplateSpecHash(template)
	if err != nil {
		return nil, fmt.Errorf("compute template hash: %w", err)
	}
	err = retry.OnError(claimIdlePodBackoff, k8serrors.IsConflict, func() error {
		// Get all idle pods for this template
		pods, listErr := s.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
			controller.LabelTemplateID: template.Name,
			controller.LabelPoolType:   controller.PoolTypeIdle,
		}))
		if listErr != nil {
			return listErr
		}

		// Filter hot-claimable pods to Kubernetes-ready instances only.
		var readyPods []*corev1.Pod
		for _, pod := range pods {
			if s.isHotClaimableIdlePod(pod, desiredTemplateHash) {
				readyPods = append(readyPods, pod)
			}
		}

		if len(readyPods) == 0 {
			// No idle pod available, not an error - use a special error to stop retry
			return errNoIdlePod
		}

		// Claim an available pod
		pod := readyPods[rand.Intn(len(readyPods))]

		s.logger.Info("Claiming idle pod",
			zap.String("pod", pod.Name),
			zap.String("sandboxID", pod.Name),
		)

		stateVolume, err := s.prepareWebhookStateVolume(ctx, req, pod.Name)
		if err != nil {
			return fmt.Errorf("prepare webhook state volume: %w", err)
		}
		rollbackStateVolume := func() {
			if stateVolume == nil {
				return
			}
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.webhookStateVolumes.Delete(cleanupCtx, req.TeamID, req.UserID, pod.Name, stateVolume.VolumeID); err != nil && s.logger != nil {
				s.logger.Warn("Failed to roll back webhook state volume",
					zap.String("sandboxID", pod.Name),
					zap.String("volumeID", stateVolume.VolumeID),
					zap.Error(err),
				)
			}
		}

		// Update pod labels and annotations
		pod = pod.DeepCopy()

		// Change pool type from idle to active
		pod.Labels[controller.LabelPoolType] = controller.PoolTypeActive
		pod.Labels[controller.LabelSandboxID] = pod.Name
		ensureSandboxCleanupFinalizer(pod)

		// Remove owner reference (so it's no longer managed by ReplicaSet)
		pod.OwnerReferences = nil

		// Add annotations
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations = controller.ClaimedSandboxPodAnnotations(pod.Annotations)
		pod.Annotations[controller.AnnotationSandboxID] = pod.Name
		pod.Annotations[controller.AnnotationTeamID] = req.TeamID
		pod.Annotations[controller.AnnotationUserID] = req.UserID
		pod.Annotations[controller.AnnotationClaimedAt] = s.clock.Now().Format(time.RFC3339)
		pod.Annotations[controller.AnnotationClaimType] = "hot"
		if stateVolume != nil {
			pod.Annotations[controller.AnnotationWebhookStateVolumeID] = stateVolume.VolumeID
		} else {
			delete(pod.Annotations, controller.AnnotationWebhookStateVolumeID)
		}

		// Set expiration annotations. Explicit 0 disables TTLs; omitted TTL uses the configured default.
		persistedConfig := s.claimConfigForPersistence(req.Config)
		if persistedConfig != nil {
			setExpirationAnnotation(pod.Annotations, s.clock.Now(), persistedConfig.TTL)
			setHardExpirationAnnotation(pod.Annotations, s.clock.Now(), persistedConfig.HardTTL)
		}

		// Serialize config
		if persistedConfig != nil {
			configJSON, marshalErr := json.Marshal(persistedConfig)
			if marshalErr != nil {
				return fmt.Errorf("marshal config: %w", marshalErr)
			}
			pod.Annotations[controller.AnnotationConfig] = string(configJSON)
		}

		// Build and add network policy annotation
		networkState, policyErr := s.applyPoliciesForPod(ctx, pod, template, req)
		if policyErr != nil {
			return policyErr
		}
		rollbackBindings, err := s.syncCredentialBindings(ctx, pod, req.TeamID, networkState)
		if err != nil {
			return fmt.Errorf("stage credential bindings: %w", err)
		}

		// Update the pod
		updatedPod, updateErr := s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
		if updateErr != nil {
			rollbackStateVolume()
			if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
				s.logger.Warn("Failed to roll back credential bindings after hot-claim update failure",
					zap.String("sandboxID", pod.Name),
					zap.Error(rollbackErr),
				)
			}
			if isIdlePodLostDuringClaim(updateErr) {
				return errNoIdlePod
			}
			return updateErr
		}

		if applyErr := s.applyNetworkProvider(ctx, updatedPod, req.TeamID, policySpecFromState(networkState)); applyErr != nil {
			s.requestSandboxDeletionAfterClaimFailure(updatedPod, "network policy apply failed")
			return fmt.Errorf("apply network policy: %w", applyErr)
		}

		s.logger.Info("Successfully claimed idle pod",
			zap.String("pod", updatedPod.Name),
			zap.String("sandboxID", updatedPod.Name),
			zap.String("expiresAt", updatedPod.Annotations[controller.AnnotationExpiresAt]),
		)

		claimedPod = updatedPod
		return nil
	})
	if err != nil {
		if errors.Is(err, errNoIdlePod) {
			return nil, nil // No idle pod available
		}
		return nil, err
	}
	return claimedPod, nil
}

func (s *SandboxService) isHotClaimableIdlePod(pod *corev1.Pod, desiredTemplateHash string) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	if pod.Annotations[controller.AnnotationTemplateSpecHash] != desiredTemplateHash {
		return false
	}
	return controller.IsPodReady(pod) && s.podDataPlaneReady(pod)
}

func isIdlePodLostDuringClaim(err error) bool {
	if k8serrors.IsNotFound(err) {
		return true
	}
	if !k8serrors.IsInvalid(err) {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "metadata.finalizers") &&
		strings.Contains(msg, "no new finalizers can be added if the object is being deleted")
}

// createNewPod creates a new pod for cold start
func (s *SandboxService) createNewPod(ctx context.Context, template *v1alpha1.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, error) {
	// Simulate K8s pod name generation: rs-name + "-" + 5 random chars
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	podName, err := naming.SandboxName(clusterID, template.Name, utilrand.String(5))
	if err != nil {
		return nil, fmt.Errorf("generate sandbox name: %w", err)
	}

	// Build pod spec before side-effecting resources so claims fail fast when the
	// sandbox data plane has no ready nodes to receive the pod.
	spec := v1alpha1.BuildPodSpec(template)
	if err := s.ensureDataPlaneReadyCapacity(spec); err != nil {
		return nil, err
	}
	stateVolume, err := s.prepareWebhookStateVolume(ctx, req, podName)
	if err != nil {
		return nil, fmt.Errorf("prepare webhook state volume: %w", err)
	}
	rollbackStateVolume := func() {
		if stateVolume == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.webhookStateVolumes.Delete(cleanupCtx, req.TeamID, req.UserID, podName, stateVolume.VolumeID); err != nil && s.logger != nil {
			s.logger.Warn("Failed to roll back webhook state volume",
				zap.String("sandboxID", podName),
				zap.String("volumeID", stateVolume.VolumeID),
				zap.Error(err),
			)
		}
	}

	if err := controller.EnsureProcdConfigSecret(ctx, s.k8sClient, s.secretLister, template); err != nil {
		return nil, fmt.Errorf("ensure procd config secret: %w", err)
	}
	if err := controller.EnsureNetdMITMCASecret(ctx, s.k8sClient, template.Namespace); err != nil {
		return nil, fmt.Errorf("ensure netd MITM CA secret: %w", err)
	}

	annotations := controller.ClaimedSandboxPodAnnotations(map[string]string{
		controller.AnnotationSandboxID: podName,
		controller.AnnotationTeamID:    req.TeamID,
		controller.AnnotationUserID:    req.UserID,
		controller.AnnotationClaimedAt: s.clock.Now().Format(time.RFC3339),
		controller.AnnotationClaimType: "cold",
	})
	if stateVolume != nil {
		annotations[controller.AnnotationWebhookStateVolumeID] = stateVolume.VolumeID
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: template.Namespace,
			Finalizers: []string{
				sandboxCleanupFinalizer,
			},
			Labels: map[string]string{
				controller.LabelTemplateID: template.Name,
				controller.LabelPoolType:   controller.PoolTypeActive,
				controller.LabelSandboxID:  podName,
			},
			Annotations: annotations,
		},
		Spec: spec,
	}

	// Set expiration annotations. Explicit 0 disables TTLs; omitted TTL uses the configured default.
	persistedConfig := s.claimConfigForPersistence(req.Config)
	if persistedConfig != nil {
		setExpirationAnnotation(pod.Annotations, s.clock.Now(), persistedConfig.TTL)
		setHardExpirationAnnotation(pod.Annotations, s.clock.Now(), persistedConfig.HardTTL)
	}

	// Serialize config
	if persistedConfig != nil {
		configJSON, err := json.Marshal(persistedConfig)
		if err != nil {
			return nil, fmt.Errorf("marshal config: %w", err)
		}
		pod.Annotations[controller.AnnotationConfig] = string(configJSON)
	}

	// Build and add network policy annotation
	networkState, err := s.applyPoliciesForPod(ctx, pod, template, req)
	if err != nil {
		return nil, err
	}
	rollbackBindings, err := s.syncCredentialBindings(ctx, pod, req.TeamID, networkState)
	if err != nil {
		return nil, fmt.Errorf("stage credential bindings: %w", err)
	}

	// Create the pod
	createdPod, err := s.k8sClient.CoreV1().Pods(template.ObjectMeta.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		rollbackStateVolume()
		if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
			s.logger.Warn("Failed to clean up staged credential bindings after create failure",
				zap.String("sandboxID", pod.Name),
				zap.Error(rollbackErr),
			)
		}
		return nil, fmt.Errorf("create pod: %w", err)
	}

	if err := s.applyNetworkProvider(ctx, createdPod, req.TeamID, policySpecFromState(networkState)); err != nil {
		s.requestSandboxDeletionAfterClaimFailure(createdPod, "network policy apply failed")
		return nil, fmt.Errorf("apply network policy: %w", err)
	}

	s.logger.Info("Created new pod for cold start",
		zap.String("pod", createdPod.Name),
		zap.String("sandboxID", createdPod.Name),
		zap.String("expiresAt", createdPod.Annotations[controller.AnnotationExpiresAt]),
	)

	return createdPod, nil
}

func (s *SandboxService) requestSandboxDeletionAfterClaimFailure(pod *corev1.Pod, reason string) {
	if s == nil || pod == nil || pod.Name == "" || pod.Namespace == "" || s.k8sClient == nil {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	logger := s.logger
	if logger == nil {
		logger = zap.NewNop()
	}

	if !hasSandboxCleanupFinalizer(pod) {
		if _, err := s.ensureSandboxDeletionFinalizer(cleanupCtx, pod); err != nil {
			logger.Warn("Failed to ensure sandbox cleanup finalizer after claim failure",
				zap.String("sandboxID", pod.Name),
				zap.String("namespace", pod.Namespace),
				zap.String("reason", reason),
				zap.Error(err),
			)
		}
	}

	if err := s.k8sClient.CoreV1().Pods(pod.Namespace).Delete(cleanupCtx, pod.Name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		logger.Warn("Delete pod failed after claim failure",
			zap.String("sandboxID", pod.Name),
			zap.String("namespace", pod.Namespace),
			zap.String("reason", reason),
			zap.Error(err),
		)
	}
}

func (s *SandboxService) podDataPlaneReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if !dataplane.SelectorRequiresReadyNode(pod.Spec.NodeSelector) {
		return true
	}
	if pod.Spec.NodeName == "" || s == nil || s.nodeLister == nil {
		return false
	}
	node, err := s.nodeLister.Get(pod.Spec.NodeName)
	if err != nil {
		return false
	}
	return dataplane.NodeReady(node)
}

func (s *SandboxService) ensureDataPlaneReadyCapacity(spec corev1.PodSpec) error {
	if !dataplane.SelectorRequiresReadyNode(spec.NodeSelector) {
		return nil
	}
	if s == nil || s.nodeLister == nil {
		return fmt.Errorf("%w: manager node cache is not configured", ErrDataPlaneNotReady)
	}
	selector := labels.SelectorFromSet(spec.NodeSelector)
	nodes, err := s.nodeLister.List(selector)
	if err != nil {
		return fmt.Errorf("list data-plane-ready nodes: %w", err)
	}
	if len(nodes) == 0 {
		return fmt.Errorf("%w: no nodes match selector %q", ErrDataPlaneNotReady, labels.Set(spec.NodeSelector).String())
	}
	return nil
}

func (s *SandboxService) initializeProcd(
	ctx context.Context,
	pod *corev1.Pod,
	req *ClaimRequest,
	procdAddress string,
) (*InitializeResponse, error) {
	if s.internalTokenGenerator == nil {
		return nil, fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	if pod == nil || req == nil {
		return nil, fmt.Errorf("missing sandbox context")
	}

	teamID := req.TeamID
	userID := req.UserID
	sandboxID := pod.Name

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	webhookInfo := s.getWebhookInfo(req)
	var webhookConfig *InitializeWebhook
	if webhookInfo != nil {
		webhookConfig = &InitializeWebhook{
			URL:      webhookInfo.URL,
			Secret:   webhookInfo.Secret,
			WatchDir: webhookInfo.WatchDir,
		}
	}

	initReq := InitializeRequest{
		SandboxID: sandboxID,
		TeamID:    teamID,
		Webhook:   webhookConfig,
	}

	var initErr error
	var initResp *InitializeResponse
	timeout := s.config.ProcdInitTimeout
	if timeout == 0 {
		timeout = 6 * time.Second
	}

	initCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		initResp, initErr = s.procdClient.Initialize(initCtx, procdAddress, initReq, internalToken)
		if initErr == nil {
			return initResp, nil
		}

		select {
		case <-initCtx.Done():
			return nil, fmt.Errorf("initialize procd timed out after %s: %w", timeout, initErr)
		case <-ticker.C:
			continue
		}
	}
}
