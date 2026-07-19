package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/startlimiter"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
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

const (
	SandboxEnvSandboxID = "SANDBOX0_SANDBOX_ID"
	SandboxEnvAppDomain = "SANDBOX0_APP_DOMAIN"
)

var errIdlePodReservationConflict = errors.New("idle pod reservation conflict")
var errIdlePodClaimLost = errors.New("idle pod claim lost")

// ClaimRequest represents a sandbox claim request
type ClaimRequest struct {
	TeamID     string
	UserID     string
	Template   string         `json:"template"`
	SnapshotID string         `json:"snapshot_id,omitempty"`
	Config     *SandboxConfig `json:"config,omitempty"`
	Mounts     []ClaimMount   `json:"mounts,omitempty"`
	Metadata   *ClaimMetadata `json:"-"`
	// SandboxID is an internal stable ID used when recreating an existing sandbox.
	SandboxID string `json:"-"`
	// RuntimeGeneration identifies the current runtime pod incarnation.
	RuntimeGeneration int64 `json:"-"`
	// HardExpiresAt preserves the absolute hard deadline when recreating a paused sandbox.
	HardExpiresAt time.Time `json:"-"`
	// WebhookStateVolumeID preserves the manager-owned webhook state volume across pod recreation.
	WebhookStateVolumeID string `json:"-"`
	// OperationID is the trusted cross-service idempotency key for quota
	// reservation. It is generated locally when the caller did not supply one.
	OperationID string `json:"-"`
}

type ClaimMount struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
	MountPoint      string `json:"mount_point"`
}

type ClaimMetadata struct {
	OwnerKind string
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
	EnvVars    map[string]string              `json:"env_vars,omitempty"`
	Resources  *SandboxResourceConfig         `json:"resources,omitempty"`
	TTL        *int32                         `json:"ttl,omitempty"`      // Time-to-live in seconds (0 disables)
	HardTTL    *int32                         `json:"hard_ttl,omitempty"` // Hard time-to-live in seconds (0 disables)
	Network    *v1alpha1.SandboxNetworkPolicy `json:"network,omitempty"`
	Webhook    *WebhookConfig                 `json:"webhook,omitempty"`
	AutoResume *bool                          `json:"auto_resume,omitempty"`
	Services   []SandboxAppService            `json:"services,omitempty"`
}

// SandboxUpdateConfig represents sandbox configuration fields that can be updated at runtime.
// EnvVars updates only the default environment for new procd-managed processes.
// Webhook is excluded because it requires reinitializing the sandbox runtime.
type SandboxUpdateConfig struct {
	EnvVars    map[string]string              `json:"env_vars,omitempty"`
	Resources  *SandboxResourceConfig         `json:"resources,omitempty"`
	TTL        *int32                         `json:"ttl,omitempty"`
	HardTTL    *int32                         `json:"hard_ttl,omitempty"`
	Network    *v1alpha1.SandboxNetworkPolicy `json:"network,omitempty"`
	AutoResume *bool                          `json:"auto_resume,omitempty"`
	Services   []SandboxAppService            `json:"services,omitempty"`
}

// SandboxResourceConfig is an instance-level resource override. Only memory is
// accepted; CPU is derived from the platform memory-per-CPU ratio with a 150m minimum.
type SandboxResourceConfig struct {
	Memory string `json:"memory,omitempty"`
}

func int32Ptr(v int32) *int32 {
	return &v
}

func cloneSandboxConfig(cfg *SandboxConfig) *SandboxConfig {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	cloned.EnvVars = cloneEnvVars(cfg.EnvVars)
	cloned.Resources = cloneSandboxResourceConfig(cfg.Resources)
	cloned.TTL = cloneInt32Ptr(cfg.TTL)
	cloned.HardTTL = cloneInt32Ptr(cfg.HardTTL)
	if cloned.Network != nil {
		cloned.Network = sanitizedNetworkPolicyForPersistence(cloned.Network)
	}
	return &cloned
}

func cloneInt32Ptr(v *int32) *int32 {
	if v == nil {
		return nil
	}
	return int32Ptr(*v)
}

func cloneSandboxResourceConfig(resources *SandboxResourceConfig) *SandboxResourceConfig {
	if resources == nil {
		return nil
	}
	return &SandboxResourceConfig{Memory: strings.TrimSpace(resources.Memory)}
}

func cloneEnvVars(envVars map[string]string) map[string]string {
	if len(envVars) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(envVars))
	for key, value := range envVars {
		cloned[key] = value
	}
	return cloned
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

func normalizeSandboxConfigForPersistence(cfg *SandboxConfig) error {
	if cfg == nil {
		return nil
	}
	if err := validateSandboxConfigLifecycle(cfg.TTL, cfg.HardTTL); err != nil {
		return err
	}
	if len(cfg.Services) > 0 {
		services, err := NormalizeSandboxAppServices(cfg.Services)
		if err != nil {
			return err
		}
		cfg.Services = services
	}
	if cfg.AutoResume != nil && !*cfg.AutoResume && SandboxAppServicesHaveResumeRoute(cfg.Services) {
		return fmt.Errorf("cannot set resume=true on public routes when sandbox auto_resume is disabled")
	}
	return nil
}

func validateSandboxConfigLifecycle(ttl, hardTTL *int32) error {
	if ttl != nil && *ttl < 0 {
		return fmt.Errorf("%w: ttl must be >= 0", ErrInvalidClaimRequest)
	}
	if hardTTL != nil && *hardTTL < 0 {
		return fmt.Errorf("%w: hard_ttl must be >= 0", ErrInvalidClaimRequest)
	}
	if ttl == nil || hardTTL == nil || *ttl <= 0 || *hardTTL <= 0 {
		return nil
	}
	if *ttl > *hardTTL {
		return fmt.Errorf("%w: ttl must be <= hard_ttl", ErrInvalidClaimRequest)
	}
	return nil
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

func setClaimHardExpirationAnnotation(annotations map[string]string, now time.Time, hardTTL *int32, hardExpiresAt time.Time) {
	if annotations == nil {
		return
	}
	if !hardExpiresAt.IsZero() {
		annotations[controller.AnnotationHardExpiresAt] = hardExpiresAt.UTC().Format(time.RFC3339)
		return
	}
	setHardExpirationAnnotation(annotations, now, hardTTL)
}

func applyClaimMetadata(pod *corev1.Pod, metadata *ClaimMetadata) {
	if pod == nil || metadata == nil {
		return
	}
	ownerKind := strings.TrimSpace(metadata.OwnerKind)
	if ownerKind == "" {
		return
	}
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	if ownerKind != "" {
		pod.Labels[controller.LabelOwnerKind] = ownerKind
		pod.Annotations[controller.AnnotationOwnerKind] = ownerKind
	}
}

func setMountsAnnotation(annotations map[string]string, mounts []ClaimMount) error {
	if annotations == nil {
		return nil
	}
	if len(mounts) == 0 {
		delete(annotations, controller.AnnotationMounts)
		return nil
	}
	if err := ValidateClaimRequestSize(&ClaimRequest{Mounts: mounts}); err != nil {
		return err
	}
	data, err := json.Marshal(mounts)
	if err != nil {
		return fmt.Errorf("marshal mounts: %w", err)
	}
	annotations[controller.AnnotationMounts] = string(data)
	return nil
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
	if err := ValidateClaimRequestSize(req); err != nil {
		return nil, err
	}
	start := time.Now()
	metrics := s.metrics
	phaseStarted := time.Now()
	canonicalTemplateID, err := naming.CanonicalTemplateID(req.Template)
	s.observeClaimPhase(req.Template, "unknown", "canonicalize_template", phaseStarted, err)
	if err != nil {
		return nil, err
	}
	req.Template = canonicalTemplateID
	req.SnapshotID = strings.TrimSpace(req.SnapshotID)
	phaseStarted = time.Now()
	if err := validateClaimMounts(req); err != nil {
		s.observeClaimPhase(req.Template, "unknown", "validate_claim_mounts", phaseStarted, err)
		return nil, err
	}
	if req.Config != nil {
		if err := normalizeSandboxConfigForPersistence(req.Config); err != nil {
			s.observeClaimPhase(req.Template, "unknown", "validate_claim_mounts", phaseStarted, err)
			return nil, err
		}
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
				return nil, fmt.Errorf("template %s not found in namespace %s: %w", req.Template, publicNamespace, err)
			}
			return nil, fmt.Errorf("get template: %w", err)
		}
	}

	// Enforce tenant isolation from the scheduler-projected template identity.
	templateTeamID, ownershipErr := controller.ValidateTeamOwnedTemplate(template)
	if ownershipErr != nil {
		err := fmt.Errorf("forbidden: invalid team-scoped template ownership: %w", ownershipErr)
		s.observeClaimPhase(req.Template, "unknown", "resolve_template", phaseStarted, err)
		return nil, err
	}
	if templateTeamID != "" {
		if templateTeamID != strings.TrimSpace(req.TeamID) {
			err := fmt.Errorf("forbidden: template belongs to a different team")
			s.observeClaimPhase(req.Template, "unknown", "resolve_template", phaseStarted, err)
			return nil, err
		}
	}
	s.observeClaimPhase(req.Template, "unknown", "resolve_template", phaseStarted, nil)
	if strings.TrimSpace(req.SandboxID) == "" {
		req.SandboxID, err = s.generateStableSandboxID(template)
		if err != nil {
			s.observeClaimPhase(req.Template, "unknown", "generate_sandbox_id", phaseStarted, err)
			return nil, err
		}
	}
	if req.RuntimeGeneration <= 0 {
		req.RuntimeGeneration = 1
	}

	phaseStarted = time.Now()
	if err := validateClaimMountsForTemplate(req, template); err != nil {
		s.observeClaimPhase(req.Template, "unknown", "validate_template_mounts", phaseStarted, err)
		return nil, err
	}
	s.observeClaimPhase(req.Template, "unknown", "validate_template_mounts", phaseStarted, nil)

	_ = resolvedName // reserved for audit/debugging (name used is template.ObjectMeta.Name)

	phaseStarted = time.Now()
	sandboxStartAdmitted, err := s.admitSandboxStartBeforePoolClaim(ctx, req.TeamID, template)
	s.observeClaimPhase(req.Template, "unknown", "admit_sandbox_start_rate", phaseStarted, err)
	if err != nil {
		return nil, err
	}

	var quotaAdmission *sandboxTeamQuotaAdmission
	if !isTeamOwnedWarmPoolTemplate(template, req.TeamID) {
		phaseStarted = time.Now()
		reservation, reserveErr := s.reserveSandboxTeamQuota(ctx, req, template, "claim")
		s.observeClaimPhase(req.Template, "unknown", "reserve_team_quota", phaseStarted, reserveErr)
		if reserveErr != nil {
			return nil, reserveErr
		}
		quotaAdmission = &sandboxTeamQuotaAdmission{Reservation: reservation}
	}
	var pod *corev1.Pod
	teamQuotaFinalized := false
	defer func() {
		if !teamQuotaFinalized {
			s.releaseFailedSandboxTeamQuotaAdmission(quotaAdmission, pod, "sandbox claim failed")
		}
	}()

	// Try to claim an idle pod first
	phaseStarted = time.Now()
	var hotQuotaAdmission *sandboxTeamQuotaAdmission
	pod, hotQuotaAdmission, err = s.claimIdlePodWithTeamQuota(ctx, template, req)
	if hotQuotaAdmission != nil {
		quotaAdmission = hotQuotaAdmission
	}
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
	if pod != nil {
		phaseStarted = time.Now()
		err = s.finalizeSandboxTeamQuotaAdmission(ctx, quotaAdmission, pod)
		s.observeClaimPhase(req.Template, "hot", "commit_team_quota", phaseStarted, err)
		if err != nil {
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, err
		}
	}
	claimType := "hot"
	var lifecycleTracker *podLifecycleStageTracker

	// If no idle pod available, create a new one (cold start)
	if pod == nil {
		claimType = "cold"
		s.logger.Info("No idle pod available, creating new pod",
			zap.String("template", req.Template),
		)
		if !sandboxStartAdmitted {
			phaseStarted = time.Now()
			err = s.admitSandboxStartTeamQuota(ctx, req.TeamID)
			s.observeClaimPhase(req.Template, claimType, "admit_sandbox_start_rate", phaseStarted, err)
			if err != nil {
				return nil, err
			}
		}
		if quotaAdmission == nil {
			phaseStarted = time.Now()
			reservation, reserveErr := s.reserveSandboxTeamQuota(ctx, req, template, "claim")
			s.observeClaimPhase(req.Template, claimType, "reserve_team_quota", phaseStarted, reserveErr)
			if reserveErr != nil {
				return nil, reserveErr
			}
			quotaAdmission = &sandboxTeamQuotaAdmission{Reservation: reservation}
		}

		var claimStartReservation *startlimiter.Reservation
		if s.claimStartLimiter != nil && s.claimStartLimiter.SupportsReservations() {
			claimStartReservation, _, err = s.claimStartLimiter.Reserve(ctx, startlimiter.ReasonColdCreate, 1)
			if err != nil {
				if metrics != nil {
					metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
				}
				return nil, fmt.Errorf("create new pod: reserve claim start budget: %w", err)
			}
			defer claimStartReservation.Release()
		}

		phaseStarted = time.Now()
		pod, err = s.createNewPodWithReservation(ctx, template, req, claimStartReservation)
		s.observeClaimPhase(req.Template, claimType, "create_new_pod", phaseStarted, err)
		if err != nil {
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, fmt.Errorf("create new pod: %w", err)
		}
		phaseStarted = time.Now()
		err = s.finalizeSandboxTeamQuotaAdmission(ctx, quotaAdmission, pod)
		s.observeClaimPhase(req.Template, claimType, "commit_team_quota", phaseStarted, err)
		if err != nil {
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, err
		}
		lifecycleTracker = newPodLifecycleStageTracker(s, req.Template)
		lifecycleTracker.observePod(pod)

		// Note: Network policies are stored in pod annotations.
		// They are set in claimIdlePod() and createNewPod() methods. Hot claims have
		// already selected a Kubernetes-ready idle pod. Cold claims must wait until
		// the pod has the network identity watched by the ctld network runtime before
		// waiting for it to patch the applied policy hash.
		if s.SupportsNetworkPolicy() {
			phaseStarted = time.Now()
			networkPod, err := s.waitForPodNetworkIdentityTracked(ctx, req.Template, pod.Namespace, pod.Name, lifecycleTracker)
			s.observeClaimPhase(req.Template, claimType, "wait_for_pod_network_identity", phaseStarted, err)
			if err != nil {
				s.requestSandboxDeletionAfterClaimFailure(pod, "network identity readiness failed")
				if metrics != nil {
					metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
				}
				return nil, fmt.Errorf("wait for pod network identity: %w", err)
			}
			pod = networkPod

			phaseStarted = time.Now()
			err = s.applyNetworkProviderFromPod(ctx, pod, req.TeamID)
			s.observeClaimPhase(req.Template, claimType, "apply_network_policy", phaseStarted, err)
			if err != nil {
				s.requestSandboxDeletionAfterClaimFailure(pod, "network policy apply failed")
				if metrics != nil {
					metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
				}
				return nil, fmt.Errorf("apply network policy: %w", err)
			}
		}

		phaseStarted = time.Now()
		readyPod, err := s.waitForPodClaimReadyTracked(ctx, pod.Namespace, pod.Name, lifecycleTracker)
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

	claimRecordPersisted := false
	cleanupClaimFailure := func(pod *corev1.Pod, reason string) {
		s.requestSandboxDeletionAfterClaimFailure(pod, reason)
		if claimRecordPersisted {
			s.markSandboxDeletedAfterClaimFailure(req.SandboxID, reason)
		}
	}

	if req.SnapshotID != "" {
		phaseStarted = time.Now()
		var recordPersisted bool
		pod, recordPersisted, err = s.initializeClaimRootFSFromSnapshot(ctx, pod, template, req)
		if recordPersisted {
			claimRecordPersisted = true
		}
		s.observeClaimPhase(req.Template, claimType, "initialize_rootfs_snapshot", phaseStarted, err)
		if err != nil {
			cleanupClaimFailure(pod, "rootfs snapshot initialization failed")
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, fmt.Errorf("initialize rootfs from snapshot: %w", err)
		}
	}

	phaseStarted = time.Now()
	portalMounts, err := s.bindVolumePortals(ctx, pod, req, template)
	s.observeClaimPhase(req.Template, claimType, "bind_volume_portals", phaseStarted, err)
	if err != nil {
		cleanupClaimFailure(pod, "volume portal bind failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("bind volume portals: %w", err)
	}
	phaseStarted = time.Now()
	if err := s.bindWebhookStatePortal(ctx, pod, req); err != nil {
		s.observeClaimPhase(req.Template, claimType, "bind_webhook_state_portal", phaseStarted, err)
		cleanupClaimFailure(pod, "webhook state portal bind failed")
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
		cleanupClaimFailure(pod, "procd address resolution failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	phaseStarted = time.Now()
	if _, err := s.initializeProcd(ctx, pod, template, req, procdAddress); err != nil {
		s.observeClaimPhase(req.Template, claimType, "initialize_procd", phaseStarted, err)
		cleanupClaimFailure(pod, "procd initialization failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("initialize procd: %w", err)
	}
	s.observeClaimPhase(req.Template, claimType, "initialize_procd", phaseStarted, nil)

	phaseStarted = time.Now()
	if err := s.persistClaimedSandbox(ctx, pod, template, req); err != nil {
		s.observeClaimPhase(req.Template, claimType, "persist_sandbox", phaseStarted, err)
		cleanupClaimFailure(pod, "sandbox persistence failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("persist sandbox: %w", err)
	}
	s.observeClaimPhase(req.Template, claimType, "persist_sandbox", phaseStarted, nil)

	phaseStarted = time.Now()
	if err := s.finalizeSandboxTeamQuotaAdmission(ctx, quotaAdmission, pod); err != nil {
		s.observeClaimPhase(req.Template, claimType, "commit_team_quota", phaseStarted, err)
		cleanupClaimFailure(pod, "team quota commit failed")
		return nil, err
	}
	teamQuotaFinalized = true
	s.observeClaimPhase(req.Template, claimType, "commit_team_quota", phaseStarted, nil)

	if metrics != nil {
		metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "success").Inc()
		metrics.SandboxClaimDuration.WithLabelValues(req.Template, claimType).Observe(time.Since(start).Seconds())
	}

	return &ClaimResponse{
		SandboxID:       req.SandboxID,
		Status:          "starting",
		ProcdAddress:    procdAddress,
		PodName:         pod.Name,
		Template:        req.Template,
		ClusterId:       template.Spec.ClusterId,
		BootstrapMounts: portalMounts,
	}, nil
}

func (s *SandboxService) persistClaimedSandbox(ctx context.Context, pod *corev1.Pod, template *v1alpha1.SandboxTemplate, req *ClaimRequest) error {
	if s == nil || s.sandboxStore == nil || pod == nil || template == nil || req == nil {
		return nil
	}
	return s.sandboxStore.UpsertSandbox(ctx, sandboxRecordForClaimedPod(s, pod, template, req))
}

func sandboxRecordForClaimedPod(s *SandboxService, pod *corev1.Pod, template *v1alpha1.SandboxTemplate, req *ClaimRequest) *SandboxRecord {
	sandboxID := sandboxIDFromPod(pod)
	if sandboxID == "" {
		sandboxID = req.SandboxID
	}
	if sandboxID == "" {
		sandboxID = pod.Name
	}
	cfg := parseSandboxConfig(pod.Annotations[controller.AnnotationConfig])
	mounts := parseClaimMounts(pod.Annotations[controller.AnnotationMounts])
	return &SandboxRecord{
		ID:                   sandboxID,
		TeamID:               req.TeamID,
		UserID:               req.UserID,
		TemplateID:           controller.TemplateLogicalID(template),
		TemplateName:         template.Name,
		TemplateNamespace:    template.Namespace,
		ClusterID:            naming.ClusterIDOrDefault(template.Spec.ClusterId),
		Status:               s.podToSandboxStatus(pod),
		Config:               cfg,
		Mounts:               mounts,
		TemplateSpec:         template.Spec,
		CurrentPodName:       pod.Name,
		CurrentPodNamespace:  pod.Namespace,
		RuntimeGeneration:    runtimeGenerationFromPod(pod),
		ClaimedAt:            parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationClaimedAt),
		ExpiresAt:            parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt),
		HardExpiresAt:        parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt),
		WebhookStateVolumeID: webhookStateVolumeIDFromPod(pod),
		OwnerKind:            ownerKindFromPod(pod),
		CreatedAt:            s.clock.Now(),
	}
}

func (s *SandboxService) initializeClaimRootFSFromSnapshot(ctx context.Context, pod *corev1.Pod, template *v1alpha1.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, bool, error) {
	if req == nil || strings.TrimSpace(req.SnapshotID) == "" {
		return pod, false, nil
	}
	snapshotID := strings.TrimSpace(req.SnapshotID)
	if templatepkg.IsBuildSnapshotID(snapshotID) {
		return pod, false, ErrRootFSSnapshotNotFound
	}
	store, err := s.rootFSProductStore()
	if err != nil {
		return pod, false, err
	}
	if _, err := store.GetRootFSSnapshot(ctx, snapshotID, req.TeamID); err != nil {
		return pod, false, err
	}
	record := sandboxRecordForClaimedPod(s, pod, template, req)
	if strings.TrimSpace(record.ID) == "" {
		return pod, false, fmt.Errorf("sandbox_id is required")
	}
	if err := s.sandboxStore.UpsertSandbox(ctx, record); err != nil {
		return pod, false, err
	}
	restorer := sandboxRootFSRestorer(store)
	if _, err := restorer.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID:  record.ID,
		SnapshotID: snapshotID,
		TeamID:     req.TeamID,
	}); err != nil {
		return pod, true, err
	}
	state, err := s.latestRootFSState(ctx, record.ID)
	if err != nil {
		return pod, true, fmt.Errorf("load rootfs snapshot state: %w", err)
	}
	if state == nil {
		return pod, true, fmt.Errorf("%w: snapshot %s", ErrRootFSFilesystemNotFound, snapshotID)
	}
	if err = s.applySandboxRootFSCheckpoint(ctx, pod, state); err != nil {
		return pod, true, err
	}
	return pod, true, nil
}

func (s *SandboxService) markSandboxDeletedAfterClaimFailure(sandboxID, reason string) {
	if s == nil || s.sandboxStore == nil || strings.TrimSpace(sandboxID) == "" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.sandboxStore.MarkSandboxDeleted(cleanupCtx, sandboxID, s.now().UTC()); err != nil && s.logger != nil {
		s.logger.Warn("Failed to mark sandbox deleted after claim failure",
			zap.String("sandboxID", sandboxID),
			zap.String("reason", reason),
			zap.Error(err),
		)
	}
}

func runtimeGenerationFromPod(pod *corev1.Pod) int64 {
	if pod == nil || pod.Annotations == nil {
		return 0
	}
	raw := strings.TrimSpace(pod.Annotations[controller.AnnotationRuntimeGeneration])
	if raw == "" {
		return 0
	}
	generation, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || generation < 0 {
		return 0
	}
	return generation
}

func (s *SandboxService) generateStableSandboxID(template *v1alpha1.SandboxTemplate) (string, error) {
	if template == nil {
		return "", fmt.Errorf("template is required")
	}
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	return naming.SandboxName(clusterID, template.Name, utilrand.String(5))
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

func (s *SandboxService) observeIdleClaim(template, result string) {
	if s == nil || s.metrics == nil || s.metrics.SandboxIdleClaimsTotal == nil {
		return
	}
	s.metrics.SandboxIdleClaimsTotal.WithLabelValues(template, result).Inc()
}

func validateClaimMountsForTemplate(req *ClaimRequest, template *v1alpha1.SandboxTemplate) error {
	allowed := declaredVolumeMountsByPath(template)
	var mounts []ClaimMount
	if req != nil {
		mounts = req.Mounts
	}
	if len(allowed) == 0 {
		if len(mounts) == 0 {
			return nil
		}
		mountPoint := filepath.Clean(mounts[0].MountPoint)
		return fmt.Errorf("%w: mounts[0].mount_point %q is not declared by template", ErrInvalidClaimRequest, mountPoint)
	}
	for i := range mounts {
		mountPoint := filepath.Clean(mounts[i].MountPoint)
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

func declaredVolumeMountDirs(template *v1alpha1.SandboxTemplate) []string {
	declared := declaredVolumeMountsByPath(template)
	if len(declared) == 0 {
		return nil
	}
	dirs := make([]string, 0, len(declared))
	for mountPath := range declared {
		dirs = append(dirs, mountPath)
	}
	sort.Strings(dirs)
	return dirs
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
	sandboxID := sandboxIDFromPod(pod)
	if sandboxID == "" {
		sandboxID = pod.Name
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return nil, err
	}
	if err := s.prepareVolumePortalBind(ctx, PrepareVolumePortalBindRequest{
		TeamID:      teamID,
		UserID:      userID,
		VolumeID:    volumeID,
		Namespace:   pod.Namespace,
		PodName:     pod.Name,
		PodUID:      string(pod.UID),
		PortalName:  volumeportal.NormalizePortalName(portalName, mountPoint),
		MountPath:   mountPoint,
		SandboxID:   sandboxID,
		OwnerTeamID: ownerTeamID,
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
		SandboxID:       sandboxID,
		TeamID:          ownerTeamID,
		SandboxVolumeID: volumeID,
	})
	if err != nil {
		return nil, err
	}
	if s.logger != nil {
		s.logger.Info("Bound sandbox volume portal",
			zap.String("sandboxID", sandboxID),
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
		strings.Contains(message, "snapshot checkpoint already in progress")
}

func isVolumePortalPendingPublicationError(resp *ctldapi.BindVolumePortalResponse, err error) bool {
	if err == nil || resp == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(resp.Error))
	return strings.Contains(message, "is not published")
}

type sandboxTeamQuotaAdmission struct {
	Reservation               *teamquota.Reservation
	WarmPoolCommitment        *warmPoolReplicaCommitment
	Committed                 bool
	Transfer                  bool
	WarmPoolCommitmentDrained bool
}

func (s *SandboxService) claimIdlePod(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	req *ClaimRequest,
) (*corev1.Pod, error) {
	pod, _, err := s.claimIdlePodInternal(ctx, template, req, false)
	return pod, err
}

func (s *SandboxService) claimIdlePodWithTeamQuota(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	req *ClaimRequest,
) (*corev1.Pod, *sandboxTeamQuotaAdmission, error) {
	return s.claimIdlePodInternal(ctx, template, req, true)
}

func (s *SandboxService) claimIdlePodInternal(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	req *ClaimRequest,
	manageTeamQuota bool,
) (*corev1.Pod, *sandboxTeamQuotaAdmission, error) {
	var claimedPod *corev1.Pod
	var quotaAdmission *sandboxTeamQuotaAdmission
	desiredTemplateHash, err := controller.TemplateSpecHash(template)
	if err != nil {
		return nil, nil, fmt.Errorf("compute template hash: %w", err)
	}
	templateID := controller.TemplateLogicalID(template)
	err = retry.OnError(claimIdlePodBackoff, func(err error) bool {
		return k8serrors.IsConflict(err) ||
			errors.Is(err, errIdlePodReservationConflict) ||
			errors.Is(err, errIdlePodClaimLost)
	}, func() error {
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
			if s.isHotClaimableIdlePod(pod, desiredTemplateHash) && !s.isIdlePodReserved(pod) {
				readyPods = append(readyPods, pod)
			}
		}

		if len(readyPods) == 0 {
			// No idle pod available, not an error - use a special error to stop retry
			s.observeIdleClaim(templateID, "no_candidate")
			return errNoIdlePod
		}

		// Claim an available pod
		pod := readyPods[rand.Intn(len(readyPods))]
		if !s.reserveIdlePod(pod) {
			s.observeIdleClaim(templateID, "reservation_conflict")
			return errIdlePodReservationConflict
		}
		reservationHeld := true
		releaseReservation := func() {
			if reservationHeld {
				s.releaseIdlePodReservation(pod)
				reservationHeld = false
			}
		}
		keepReservationUntilTTL := func() {
			reservationHeld = false
		}
		defer releaseReservation()
		s.observeIdleClaim(templateID, "reserved")

		sandboxID := strings.TrimSpace(req.SandboxID)
		if sandboxID == "" {
			sandboxID = pod.Name
		}
		s.logger.Info("Claiming idle pod",
			zap.String("pod", pod.Name),
			zap.String("sandboxID", sandboxID),
		)

		stateVolume, err := s.prepareWebhookStateVolume(ctx, req, sandboxID)
		if err != nil {
			return fmt.Errorf("prepare webhook state volume: %w", err)
		}
		rollbackStateVolume := func() {
			if stateVolume == nil || !stateVolume.Created {
				return
			}
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.webhookStateVolumes.Delete(cleanupCtx, req.TeamID, req.UserID, sandboxID, stateVolume.VolumeID); err != nil && s.logger != nil {
				s.logger.Warn("Failed to roll back webhook state volume",
					zap.String("sandboxID", sandboxID),
					zap.String("volumeID", stateVolume.VolumeID),
					zap.Error(err),
				)
			}
		}

		// Update pod labels and annotations
		originalIdlePod := pod.DeepCopy()
		pod = pod.DeepCopy()
		resourceLimits, err := s.effectiveSandboxResourceLimits(template, req.Config)
		if err != nil {
			return err
		}
		destinationSpec := pod.Spec.DeepCopy()
		if err := applySandboxResourceLimitsToPodSpec(destinationSpec, resourceLimits); err != nil {
			return err
		}
		var resizeLimits *v1alpha1.SandboxResourceLimits
		if sandboxPodNeedsResourceResize(pod, resourceLimits) {
			resizeLimits = &resourceLimits
		}

		// Change pool type from idle to active
		pod.Labels[controller.LabelPoolType] = controller.PoolTypeActive
		pod.Labels[controller.LabelSandboxID] = sandboxID
		ensureSandboxCleanupFinalizer(pod)

		// Remove owner reference (so it's no longer managed by ReplicaSet)
		pod.OwnerReferences = nil

		// Add annotations
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations = controller.ClaimedSandboxPodAnnotations(pod.Annotations)
		pod.Annotations[controller.AnnotationSandboxID] = sandboxID
		pod.Annotations[controller.AnnotationRuntimeGeneration] = strconv.FormatInt(req.RuntimeGeneration, 10)
		pod.Annotations[controller.AnnotationTeamID] = req.TeamID
		pod.Annotations[controller.AnnotationUserID] = req.UserID
		pod.Annotations[controller.AnnotationClaimedAt] = s.clock.Now().Format(time.RFC3339)
		pod.Annotations[controller.AnnotationClaimType] = "hot"
		if stateVolume != nil {
			pod.Annotations[controller.AnnotationWebhookStateVolumeID] = stateVolume.VolumeID
		} else {
			delete(pod.Annotations, controller.AnnotationWebhookStateVolumeID)
		}
		applyClaimMetadata(pod, req.Metadata)

		// Set expiration annotations. Explicit 0 disables TTLs; omitted TTL uses the configured default.
		persistedConfig := s.claimConfigForPersistence(req.Config)
		if persistedConfig != nil {
			setExpirationAnnotation(pod.Annotations, s.clock.Now(), persistedConfig.TTL)
			setClaimHardExpirationAnnotation(pod.Annotations, s.clock.Now(), persistedConfig.HardTTL, req.HardExpiresAt)
		}
		if err := setMountsAnnotation(pod.Annotations, req.Mounts); err != nil {
			return err
		}

		// Serialize config
		if persistedConfig != nil {
			if err := ValidateSandboxConfigSize(persistedConfig); err != nil {
				return err
			}
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

		var transferReservation *teamquota.Reservation
		var replicaCommitment *warmPoolReplicaCommitment
		if manageTeamQuota && isTeamOwnedWarmPoolTemplate(template, req.TeamID) {
			if s.teamQuotaStore == nil {
				rollbackStateVolume()
				if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
					s.logger.Warn("Failed to roll back credential bindings after missing quota store",
						zap.String("sandboxID", sandboxID),
						zap.Error(rollbackErr),
					)
				}
				return fmt.Errorf(
					"%w: capacity store is not configured for team warm-pool transfer",
					ErrTeamQuotaUnavailable,
				)
			}
			transferReservation, err = s.prepareTeamWarmPoolTransfer(
				ctx,
				template,
				req,
				originalIdlePod,
			)
			if err != nil {
				rollbackStateVolume()
				if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
					s.logger.Warn("Failed to roll back credential bindings after quota transfer rejection",
						zap.String("sandboxID", sandboxID),
						zap.Error(rollbackErr),
					)
				}
				return err
			}
			replicaCommitment, err = warmPoolReplicaCommitmentForIdlePod(
				template,
				originalIdlePod,
				transferReservation.Operation.ID,
			)
			if err != nil {
				s.abortTeamWarmPoolTransfer(ctx, transferReservation, "warm-pool ReplicaSet identity is invalid")
				rollbackStateVolume()
				if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
					s.logger.Warn("Failed to roll back credential bindings after warm-pool identity failure",
						zap.String("sandboxID", sandboxID),
						zap.Error(rollbackErr),
					)
				}
				return err
			}
		}
		rollbackTransfer := func(reason string) {
			if transferReservation == nil {
				return
			}
			abortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s.abortTeamWarmPoolTransfer(abortCtx, transferReservation, reason)
		}

		// Update the pod
		var updatedPod *corev1.Pod
		updateAttempted := false
		updatePod := func(updateCtx context.Context) error {
			updateAttempted = true
			var updateErr error
			updatedPod, updateErr = s.k8sClient.CoreV1().Pods(pod.Namespace).Update(updateCtx, pod, metav1.UpdateOptions{})
			return updateErr
		}
		var updateErr error
		if s.claimStartLimiter != nil {
			_, updateErr = s.claimStartLimiter.Admit(ctx, startlimiter.ReasonHotClaim, 1, updatePod)
		} else {
			updateErr = updatePod(ctx)
		}
		if updateErr != nil {
			if updateAttempted {
				observed, observeErr := s.k8sClient.CoreV1().
					Pods(pod.Namespace).
					Get(ctx, pod.Name, metav1.GetOptions{})
				switch {
				case observeErr == nil && hotClaimPodMatchesIntended(observed, pod):
					// The API response was ambiguous, but the intended active
					// ownership is durable. Continue and commit the transfer.
					updatedPod = observed
					updateErr = nil
				case observeErr != nil && !k8serrors.IsNotFound(observeErr):
					// Do not roll back a prepared transfer when ownership cannot
					// be observed. Startup/periodic recovery resolves it.
					claimedPod = pod
					keepReservationUntilTTL()
					s.observeIdleClaim(templateID, "update_observation_error")
					return fmt.Errorf(
						"observe ambiguous hot-claim update after %v: %w",
						updateErr,
						observeErr,
					)
				case observeErr == nil && !hotClaimPodMatchesOriginal(observed, originalIdlePod):
					// A third state may be the partially applied intended
					// mutation. Keep conservative quota holds for recovery.
					claimedPod = observed
					keepReservationUntilTTL()
					s.observeIdleClaim(templateID, "update_observation_ambiguous")
					return fmt.Errorf(
						"hot-claim update returned %v and pod ownership is ambiguous",
						updateErr,
					)
				case observeErr == nil &&
					hotClaimPodMatchesOriginal(observed, originalIdlePod) &&
					!isDefiniteHotClaimPodUpdateRejection(updateErr):
					// A transport or server failure can race a late-applied
					// update even when the first observation is still original.
					// Keep the prepared transfer for a later stable recovery
					// decision instead of aborting under a possibly active Pod.
					keepReservationUntilTTL()
					s.observeIdleClaim(templateID, "update_observation_pending")
					return fmt.Errorf(
						"hot-claim update outcome remains ambiguous after observing the original pod: %w",
						updateErr,
					)
				}
			}
		}
		if updateErr != nil {
			rollbackTransfer("hot-claim pod update failed")
			rollbackStateVolume()
			if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
				s.logger.Warn("Failed to roll back credential bindings after hot-claim update failure",
					zap.String("sandboxID", sandboxID),
					zap.Error(rollbackErr),
				)
			}
			if isIdlePodLostDuringClaim(updateErr) {
				s.observeIdleClaim(templateID, "update_conflict")
				keepReservationUntilTTL()
				return errNoIdlePod
			}
			if k8serrors.IsConflict(updateErr) {
				s.observeIdleClaim(templateID, "update_conflict")
				keepReservationUntilTTL()
				return fmt.Errorf("%w: update pod %s/%s: %w", errIdlePodClaimLost, pod.Namespace, pod.Name, updateErr)
			} else {
				s.observeIdleClaim(templateID, "update_error")
			}
			return updateErr
		}
		keepReservationUntilTTL()
		claimedPod = updatedPod

		if transferReservation != nil {
			quotaAdmission = &sandboxTeamQuotaAdmission{
				Reservation:        transferReservation,
				Transfer:           true,
				WarmPoolCommitment: replicaCommitment,
			}
			if releaseErr := s.releaseWarmPoolReplicaCommitment(ctx, replicaCommitment); releaseErr != nil {
				rollbackStateVolume()
				if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
					s.logger.Warn("Failed to roll back credential bindings after warm-pool scale-down failure",
						zap.String("sandboxID", sandboxID),
						zap.Error(rollbackErr),
					)
				}
				s.requestSandboxDeletionAfterClaimFailure(
					updatedPod,
					"warm-pool ReplicaSet scale-down failed",
				)
				return releaseErr
			}
			quotaAdmission.WarmPoolCommitmentDrained = true
			if commitErr := s.commitTeamWarmPoolTransfer(
				ctx,
				transferReservation,
				replicaCommitment.observedSource,
			); commitErr != nil {
				rollbackStateVolume()
				if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
					s.logger.Warn("Failed to roll back credential bindings after quota transfer commit failure",
						zap.String("sandboxID", sandboxID),
						zap.Error(rollbackErr),
					)
				}
				s.requestSandboxDeletionAfterClaimFailure(updatedPod, "warm-pool quota transfer commit failed")
				return commitErr
			}
			quotaAdmission.Committed = true
			if clearErr := s.clearWarmPoolReplicaCommitment(ctx, replicaCommitment); clearErr != nil {
				s.logWarmPoolCommitmentMarkerCleanupFailure(originalIdlePod, clearErr)
			}
		}

		if resizeLimits != nil {
			var resizedPod *corev1.Pod
			var resizeErr error
			if transferReservation != nil {
				// The committed warm-pool transfer accounts for the Pod's
				// current physical resources. Admit the requested target as a
				// separate resize before changing those resources.
				resizedPod, resizeErr = s.resizeSandboxPodResourcesWithTeamQuota(
					ctx,
					updatedPod,
					template,
					*resizeLimits,
				)
			} else {
				// Non-transfer claims are already covered by their claim
				// reservation. The internal helper is also used by tests and
				// recovery paths that intentionally do not manage team quota.
				resizedPod, resizeErr = s.resizeSandboxPodResources(
					ctx,
					updatedPod,
					*resizeLimits,
				)
			}
			if resizeErr != nil {
				rollbackStateVolume()
				if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
					s.logger.Warn("Failed to roll back credential bindings after hot-claim resize failure",
						zap.String("sandboxID", sandboxID),
						zap.Error(rollbackErr),
					)
				}
				if k8serrors.IsConflict(resizeErr) && transferReservation == nil {
					s.observeIdleClaim(templateID, "resize_conflict")
					restoreCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					if restoreErr := s.restoreIdlePodAfterHotClaimResizeConflict(restoreCtx, updatedPod, originalIdlePod); restoreErr != nil {
						s.logger.Warn("Failed to restore idle pod after hot-claim resize conflict",
							zap.String("sandboxID", sandboxID),
							zap.String("pod", updatedPod.Name),
							zap.Error(restoreErr),
						)
					}
					cancel()
					keepReservationUntilTTL()
					return fmt.Errorf("%w: resize pod %s/%s: %w", errIdlePodClaimLost, updatedPod.Namespace, updatedPod.Name, resizeErr)
				} else {
					s.observeIdleClaim(templateID, "resize_error")
					s.requestSandboxDeletionAfterClaimFailure(updatedPod, "sandbox resource resize failed")
				}
				return fmt.Errorf("resize sandbox resources: %w", resizeErr)
			}
			updatedPod = mergeSandboxMetadataAfterResize(resizedPod, updatedPod)
		}

		if applyErr := s.applyNetworkProvider(ctx, updatedPod, req.TeamID, policySpecFromState(networkState)); applyErr != nil {
			s.requestSandboxDeletionAfterClaimFailure(updatedPod, "network policy apply failed")
			s.observeIdleClaim(templateID, "network_policy_error")
			return fmt.Errorf("apply network policy: %w", applyErr)
		}

		s.logger.Info("Successfully claimed idle pod",
			zap.String("pod", updatedPod.Name),
			zap.String("sandboxID", sandboxID),
			zap.String("expiresAt", updatedPod.Annotations[controller.AnnotationExpiresAt]),
		)

		claimedPod = updatedPod
		s.observeIdleClaim(templateID, "success")
		return nil
	})
	if err != nil {
		if errors.Is(err, errNoIdlePod) ||
			errors.Is(err, errIdlePodReservationConflict) ||
			errors.Is(err, errIdlePodClaimLost) {
			return nil, nil, nil // No idle pod available
		}
		if !manageTeamQuota {
			return nil, nil, err
		}
		return claimedPod, quotaAdmission, err
	}
	return claimedPod, quotaAdmission, nil
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

func (s *SandboxService) reserveIdlePod(pod *corev1.Pod) bool {
	return s.idleReservations().tryReserve(pod, idlePodReservationTTL)
}

func (s *SandboxService) releaseIdlePodReservation(pod *corev1.Pod) {
	s.idleReservations().release(pod)
}

func (s *SandboxService) isIdlePodReserved(pod *corev1.Pod) bool {
	return s.idleReservations().isReserved(pod)
}

func (s *SandboxService) idleReservations() *idlePodReservations {
	if s.idlePodReservations == nil {
		s.idlePodReservations = newIdlePodReservations()
	}
	return s.idlePodReservations
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

func isDefiniteHotClaimPodUpdateRejection(err error) bool {
	if err == nil {
		return false
	}
	if k8serrors.IsNotFound(err) ||
		k8serrors.IsAlreadyExists(err) ||
		k8serrors.IsConflict(err) ||
		k8serrors.IsInvalid(err) ||
		k8serrors.IsGone(err) ||
		k8serrors.IsResourceExpired(err) ||
		k8serrors.IsNotAcceptable(err) ||
		k8serrors.IsUnsupportedMediaType(err) ||
		k8serrors.IsMethodNotSupported(err) ||
		k8serrors.IsBadRequest(err) ||
		k8serrors.IsUnauthorized(err) ||
		k8serrors.IsForbidden(err) ||
		k8serrors.IsRequestEntityTooLargeError(err) {
		return true
	}
	var status k8serrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	code := status.Status().Code
	return code >= http.StatusBadRequest &&
		code < http.StatusInternalServerError &&
		code != http.StatusRequestTimeout
}

func (s *SandboxService) createNewPod(ctx context.Context, template *v1alpha1.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, error) {
	return s.createNewPodWithReservation(ctx, template, req, nil)
}

func (s *SandboxService) createNewPodWithReservation(ctx context.Context, template *v1alpha1.SandboxTemplate, req *ClaimRequest, reservation *startlimiter.Reservation) (*corev1.Pod, error) {
	// Simulate K8s pod name generation: rs-name + "-" + 5 random chars
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	podName, err := naming.SandboxName(clusterID, template.Name, utilrand.String(5))
	if err != nil {
		return nil, fmt.Errorf("generate sandbox name: %w", err)
	}

	// Build pod spec before side-effecting resources so claims fail fast when the
	// sandbox data plane has no ready nodes to receive the pod.
	spec := v1alpha1.BuildPodSpec(template)
	resourceLimits, err := s.effectiveSandboxResourceLimits(template, req.Config)
	if err != nil {
		return nil, err
	}
	if err := applySandboxResourceLimitsToPodSpec(&spec, resourceLimits); err != nil {
		return nil, err
	}
	if err := s.ensureDataPlaneReadyCapacity(spec); err != nil {
		return nil, err
	}
	sandboxID := strings.TrimSpace(req.SandboxID)
	if sandboxID == "" {
		sandboxID = podName
	}
	stateVolume, err := s.prepareWebhookStateVolume(ctx, req, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("prepare webhook state volume: %w", err)
	}
	rollbackStateVolume := func() {
		if stateVolume == nil || !stateVolume.Created {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.webhookStateVolumes.Delete(cleanupCtx, req.TeamID, req.UserID, sandboxID, stateVolume.VolumeID); err != nil && s.logger != nil {
			s.logger.Warn("Failed to roll back webhook state volume",
				zap.String("sandboxID", sandboxID),
				zap.String("volumeID", stateVolume.VolumeID),
				zap.Error(err),
			)
		}
	}

	if err := controller.EnsureProcdConfigSecret(ctx, s.k8sClient, s.secretLister, template); err != nil {
		return nil, fmt.Errorf("ensure procd config secret: %w", err)
	}
	if err := controller.EnsureNetdMITMCASecret(ctx, s.k8sClient, s.secretLister, template.Namespace); err != nil {
		return nil, fmt.Errorf("ensure network-runtime MITM CA secret: %w", err)
	}

	annotations := controller.ClaimedSandboxPodAnnotations(map[string]string{
		controller.AnnotationSandboxID:         sandboxID,
		controller.AnnotationRuntimeGeneration: strconv.FormatInt(req.RuntimeGeneration, 10),
		controller.AnnotationTeamID:            req.TeamID,
		controller.AnnotationUserID:            req.UserID,
		controller.AnnotationClaimedAt:         s.clock.Now().Format(time.RFC3339),
		controller.AnnotationClaimType:         "cold",
	})
	if token := reservation.Token(); token != "" {
		annotations[controller.AnnotationClaimStartReservation] = token
	}
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
				controller.LabelTemplateID:        template.Name,
				controller.LabelTemplateLogicalID: controller.TemplateLogicalID(template),
				controller.LabelPoolType:          controller.PoolTypeActive,
				controller.LabelSandboxID:         sandboxID,
			},
			Annotations: annotations,
		},
		Spec: spec,
	}
	applyClaimMetadata(pod, req.Metadata)

	// Set expiration annotations. Explicit 0 disables TTLs; omitted TTL uses the configured default.
	persistedConfig := s.claimConfigForPersistence(req.Config)
	if persistedConfig != nil {
		setExpirationAnnotation(pod.Annotations, s.clock.Now(), persistedConfig.TTL)
		setClaimHardExpirationAnnotation(pod.Annotations, s.clock.Now(), persistedConfig.HardTTL, req.HardExpiresAt)
	}
	if err := setMountsAnnotation(pod.Annotations, req.Mounts); err != nil {
		return nil, err
	}

	// Serialize config
	if persistedConfig != nil {
		if err := ValidateSandboxConfigSize(persistedConfig); err != nil {
			return nil, err
		}
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
	var createdPod *corev1.Pod
	createAttempted := false
	createPod := func(createCtx context.Context) error {
		createAttempted = true
		var createErr error
		createdPod, createErr = s.k8sClient.CoreV1().Pods(template.ObjectMeta.Namespace).Create(createCtx, pod, metav1.CreateOptions{})
		return createErr
	}
	if reservation != nil {
		err = createPod(ctx)
	} else if s.claimStartLimiter != nil {
		_, err = s.claimStartLimiter.Admit(ctx, startlimiter.ReasonColdCreate, 1, createPod)
	} else {
		err = createPod(ctx)
	}
	if err != nil {
		if createAttempted && !definitivePodCreateRejection(err) {
			observed, observeErr := s.k8sClient.CoreV1().
				Pods(pod.Namespace).
				Get(ctx, pod.Name, metav1.GetOptions{})
			switch {
			case observeErr == nil && coldCreatePodMatchesIntended(observed, pod):
				return observed, fmt.Errorf("create pod response was ambiguous: %w", err)
			case observeErr != nil && !k8serrors.IsNotFound(observeErr):
				// Return the deterministic intended identity so the caller
				// keeps quota pending and requests cleanup conservatively.
				return pod, fmt.Errorf(
					"create pod response was ambiguous (%v) and observation failed: %w",
					err,
					observeErr,
				)
			}
		}
		rollbackStateVolume()
		if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
			s.logger.Warn("Failed to clean up staged credential bindings after create failure",
				zap.String("sandboxID", sandboxID),
				zap.Error(rollbackErr),
			)
		}
		return nil, fmt.Errorf("create pod: %w", err)
	}

	s.logger.Info("Created new pod for cold start",
		zap.String("pod", createdPod.Name),
		zap.String("sandboxID", sandboxID),
		zap.String("expiresAt", createdPod.Annotations[controller.AnnotationExpiresAt]),
	)

	return createdPod, nil
}

func definitivePodCreateRejection(err error) bool {
	return k8serrors.IsInvalid(err) ||
		k8serrors.IsForbidden(err) ||
		k8serrors.IsUnauthorized(err) ||
		k8serrors.IsBadRequest(err) ||
		k8serrors.IsMethodNotSupported(err)
}

func coldCreatePodMatchesIntended(observed, intended *corev1.Pod) bool {
	if observed == nil || intended == nil ||
		observed.Namespace != intended.Namespace ||
		observed.Name != intended.Name {
		return false
	}
	return observed.Labels[controller.LabelPoolType] == controller.PoolTypeActive &&
		sandboxIDFromPod(observed) == sandboxIDFromPod(intended) &&
		strings.TrimSpace(observed.Annotations[controller.AnnotationTeamID]) ==
			strings.TrimSpace(intended.Annotations[controller.AnnotationTeamID]) &&
		runtimeGenerationFromPod(observed) == runtimeGenerationFromPod(intended)
}

func hotClaimPodMatchesIntended(observed, intended *corev1.Pod) bool {
	if !coldCreatePodMatchesIntended(observed, intended) ||
		string(observed.UID) != string(intended.UID) {
		return false
	}
	return metav1.GetControllerOf(observed) == nil
}

func hotClaimPodMatchesOriginal(observed, original *corev1.Pod) bool {
	if observed == nil || original == nil ||
		observed.Namespace != original.Namespace ||
		observed.Name != original.Name ||
		string(observed.UID) != string(original.UID) ||
		observed.Labels[controller.LabelPoolType] != controller.PoolTypeIdle {
		return false
	}
	observedController := metav1.GetControllerOf(observed)
	originalController := metav1.GetControllerOf(original)
	if observedController == nil || originalController == nil {
		return observedController == nil && originalController == nil
	}
	return observedController.UID == originalController.UID
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
				zap.String("sandboxID", sandboxIDFromPod(pod)),
				zap.String("namespace", pod.Namespace),
				zap.String("reason", reason),
				zap.Error(err),
			)
		}
	}

	if err := s.k8sClient.CoreV1().Pods(pod.Namespace).Delete(cleanupCtx, pod.Name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		logger.Warn("Delete pod failed after claim failure",
			zap.String("sandboxID", sandboxIDFromPod(pod)),
			zap.String("namespace", pod.Namespace),
			zap.String("reason", reason),
			zap.Error(err),
		)
	}
}

func (s *SandboxService) restoreIdlePodAfterHotClaimResizeConflict(ctx context.Context, claimedPod, originalIdlePod *corev1.Pod) error {
	if s == nil || s.k8sClient == nil || claimedPod == nil || originalIdlePod == nil {
		return nil
	}
	if claimedPod.Namespace == "" || claimedPod.Name == "" {
		return nil
	}
	namespace, name := claimedPod.Namespace, claimedPod.Name
	claimedSandboxID := sandboxIDFromPod(claimedPod)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := s.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if current.DeletionTimestamp != nil {
			return nil
		}
		if originalIdlePod.UID != "" && current.UID != "" && originalIdlePod.UID != current.UID {
			return nil
		}
		if claimedSandboxID != "" && sandboxIDFromPod(current) != "" && sandboxIDFromPod(current) != claimedSandboxID {
			return nil
		}

		restored := current.DeepCopy()
		restored.Labels = cloneMetadataMap(originalIdlePod.Labels)
		restored.Annotations = cloneMetadataMap(originalIdlePod.Annotations)
		restored.Finalizers = append([]string(nil), originalIdlePod.Finalizers...)
		restored.OwnerReferences = append([]metav1.OwnerReference(nil), originalIdlePod.OwnerReferences...)
		_, err = s.k8sClient.CoreV1().Pods(namespace).Update(ctx, restored, metav1.UpdateOptions{})
		return err
	})
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
		if s != nil && s.config.AllowColdStartWithoutReadyDataPlane {
			return nil
		}
		return fmt.Errorf("%w: manager node cache is not configured", ErrDataPlaneNotReady)
	}
	selector := labels.SelectorFromSet(spec.NodeSelector)
	nodes, err := s.nodeLister.List(selector)
	if err != nil {
		return fmt.Errorf("list data-plane-ready nodes: %w", err)
	}
	if len(nodes) == 0 {
		if s.config.AllowColdStartWithoutReadyDataPlane {
			return nil
		}
		return fmt.Errorf("%w: no nodes match selector %q", ErrDataPlaneNotReady, labels.Set(spec.NodeSelector).String())
	}
	return nil
}

func (s *SandboxService) initializeProcd(
	ctx context.Context,
	pod *corev1.Pod,
	template *v1alpha1.SandboxTemplate,
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
	sandboxID := sandboxIDFromPod(pod)
	if sandboxID == "" {
		sandboxID = req.SandboxID
	}
	if sandboxID == "" {
		sandboxID = pod.Name
	}

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
		SandboxID:         sandboxID,
		TeamID:            teamID,
		RuntimeGeneration: req.RuntimeGeneration,
		EnvVars: sandboxEnvVarsForInitialize(req.Config, sandboxPlatformEnv{
			SandboxID: sandboxID,
			AppDomain: SandboxAppDomain(
				s.config.PublicRegionID,
				s.config.PublicRootDomain,
			),
		}),
		Webhook:   webhookConfig,
		MountDirs: declaredVolumeMountDirs(template),
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

type sandboxPlatformEnv struct {
	SandboxID string
	AppDomain string
}

func sandboxEnvVarsForInitialize(cfg *SandboxConfig, platform sandboxPlatformEnv) map[string]string {
	var envVars map[string]string
	if cfg != nil {
		envVars = cloneEnvVars(cfg.EnvVars)
	}
	if envVars == nil {
		envVars = map[string]string{}
	}
	if sandboxID := strings.TrimSpace(platform.SandboxID); sandboxID != "" {
		envVars[SandboxEnvSandboxID] = sandboxID
	}
	if appDomain := strings.Trim(strings.TrimSpace(platform.AppDomain), "."); appDomain != "" {
		envVars[SandboxEnvAppDomain] = appDomain
	}
	if len(envVars) == 0 {
		return nil
	}
	return envVars
}
