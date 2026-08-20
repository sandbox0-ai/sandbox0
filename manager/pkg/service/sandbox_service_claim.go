package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/appservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/util/retry"
)

var errIdlePodClaimLost = errors.New("idle pod claim lost")

// ClaimRequest represents a sandbox claim request
type ClaimRequest struct {
	TeamID     string
	UserID     string
	Template   string                      `json:"template"`
	SnapshotID string                      `json:"snapshot_id,omitempty"`
	Config     *sandboxstore.SandboxConfig `json:"config,omitempty"`
	Metadata   *ClaimMetadata              `json:"-"`
	// SandboxID is an internal stable ID used when recreating an existing sandbox.
	SandboxID string `json:"-"`
	// RuntimeGeneration identifies the current runtime pod incarnation.
	RuntimeGeneration int64 `json:"-"`
	// HardExpiresAt preserves the absolute hard deadline when recreating a paused sandbox.
	HardExpiresAt time.Time `json:"-"`
	// StartedAt is the trusted regional ingress time propagated through signed
	// internal claims. Public JSON can never set it.
	StartedAt time.Time `json:"-"`
	// OperationID is the signed regional operation identity. Runtime backends
	// use it as the retry key for every durable claim side effect.
	OperationID                       string `json:"-"`
	mayHaveExistingCredentialBindings bool
}

type ClaimMetadata struct {
	OwnerKind string
}

// SandboxUpdateConfig represents sandbox configuration fields that can be updated at runtime.
// EnvVars updates only the default environment for new procd-managed processes.
// Webhook is excluded because it requires reinitializing the sandbox runtime.
type SandboxUpdateConfig struct {
	EnvVars    map[string]string                 `json:"env_vars,omitempty"`
	Resources  *managerapi.SandboxResourceConfig `json:"resources,omitempty"`
	TTL        *int32                            `json:"ttl,omitempty"`
	HardTTL    *int32                            `json:"hard_ttl,omitempty"`
	Network    *v1alpha1.SandboxNetworkPolicy    `json:"network,omitempty"`
	AutoResume *bool                             `json:"auto_resume,omitempty"`
	Services   []managerapi.SandboxAppService    `json:"services,omitempty"`
}

func int32Ptr(v int32) *int32 {
	return &v
}

// CloneSandboxConfig returns an independent claim configuration.
func CloneSandboxConfig(cfg *sandboxstore.SandboxConfig) *sandboxstore.SandboxConfig {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	cloned.EnvVars = cloneEnvVars(cfg.EnvVars)
	cloned.Resources = cloneSandboxResourceConfig(cfg.Resources)
	cloned.TTL = cloneInt32Ptr(cfg.TTL)
	cloned.HardTTL = cloneInt32Ptr(cfg.HardTTL)
	cloned.AutoResume = cloneBoolPtr(cfg.AutoResume)
	if cloned.Network != nil {
		cloned.Network = sanitizedNetworkPolicyForPersistence(cloned.Network)
	}
	return &cloned
}

func cloneSandboxConfig(cfg *sandboxstore.SandboxConfig) *sandboxstore.SandboxConfig {
	return CloneSandboxConfig(cfg)
}

func cloneInt32Ptr(v *int32) *int32 {
	if v == nil {
		return nil
	}
	return int32Ptr(*v)
}

func cloneBoolPtr(v *bool) *bool {
	if v == nil {
		return nil
	}
	cloned := *v
	return &cloned
}

func cloneSandboxResourceConfig(resources *managerapi.SandboxResourceConfig) *managerapi.SandboxResourceConfig {
	if resources == nil {
		return nil
	}
	return &managerapi.SandboxResourceConfig{Memory: strings.TrimSpace(resources.Memory)}
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

func (s *SandboxService) claimConfigForPersistence(cfg *sandboxstore.SandboxConfig) *sandboxstore.SandboxConfig {
	persisted := cloneSandboxConfig(cfg)
	if persisted == nil {
		if s.config.DefaultTTL <= 0 {
			return nil
		}
		persisted = &sandboxstore.SandboxConfig{}
	}
	if persisted.TTL == nil && s.config.DefaultTTL > 0 {
		persisted.TTL = int32Ptr(int32(s.config.DefaultTTL.Seconds()))
	}
	return persisted
}

// NormalizeSandboxConfigForPersistence validates and canonicalizes a public
// claim configuration before any runtime backend persists it.
func NormalizeSandboxConfigForPersistence(cfg *sandboxstore.SandboxConfig) error {
	if cfg == nil {
		return nil
	}
	if err := validateSandboxConfigLifecycle(cfg.TTL, cfg.HardTTL); err != nil {
		return err
	}
	if len(cfg.Services) > 0 {
		services, err := appservice.NormalizeSandboxAppServices(cfg.Services)
		if err != nil {
			return err
		}
		cfg.Services = services
	}
	if cfg.AutoResume != nil && !*cfg.AutoResume && appservice.SandboxAppServicesHaveResumeRoute(cfg.Services) {
		return fmt.Errorf("cannot set resume=true on public routes when sandbox auto_resume is disabled")
	}
	return nil
}

func normalizeSandboxConfigForPersistence(cfg *sandboxstore.SandboxConfig) error {
	return NormalizeSandboxConfigForPersistence(cfg)
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

// ClaimResponse represents a sandbox claim response
type ClaimResponse struct {
	SandboxID    string  `json:"sandbox_id"`
	Status       string  `json:"status"`
	ProcdAddress string  `json:"procd_address"`
	PodName      string  `json:"pod_name"`
	Template     string  `json:"template"`
	ClusterId    *string `json:"cluster_id,omitempty"`
}

// SandboxClaimer is the runtime-neutral public claim boundary.
type SandboxClaimer interface {
	ClaimSandbox(context.Context, *ClaimRequest) (*ClaimResponse, error)
}

// SandboxTerminator persists a sandbox deletion request without requiring the
// public HTTP layer to understand the physical runtime backend.
type SandboxTerminator interface {
	TerminateSandbox(context.Context, string) error
}

// SandboxPauser owns the public checkpoint-and-pause path for one runtime.
type SandboxPauser interface {
	PauseSandboxAndWait(context.Context, string) (*PauseSandboxResponse, error)
}

// SandboxResumer owns the public restore-and-resume path for one runtime.
type SandboxResumer interface {
	ResumeSandboxAndWait(context.Context, string) (*managerapi.ResumeSandboxResponse, error)
}

// SandboxAutoPauser accepts durable automatic pause requests from TTL cleanup.
type SandboxAutoPauser interface {
	PauseSandboxByID(context.Context, string) error
}

// SandboxRuntimeBackend owns public and automatic lifecycle paths for one
// selected physical runtime backend.
type SandboxRuntimeBackend interface {
	SandboxClaimer
	SandboxTerminator
	SandboxPauser
	SandboxResumer
	SandboxAutoPauser
}

var _ SandboxRuntimeBackend = (*SandboxService)(nil)

// ClaimSandbox claims a sandbox from the idle pool or creates a new one
func (s *SandboxService) ClaimSandbox(ctx context.Context, req *ClaimRequest) (*ClaimResponse, error) {
	start := time.Now()
	metrics := s.metrics
	req.mayHaveExistingCredentialBindings = strings.TrimSpace(req.SandboxID) != ""
	phaseStarted := time.Now()
	canonicalTemplateID, err := naming.CanonicalTemplateID(req.Template)
	s.observeClaimPhase(req.Template, "unknown", "canonicalize_template", phaseStarted, err)
	if err != nil {
		return nil, err
	}
	req.Template = canonicalTemplateID
	req.SnapshotID = strings.TrimSpace(req.SnapshotID)
	if req.Config != nil {
		if err := normalizeSandboxConfigForPersistence(req.Config); err != nil {
			s.observeClaimPhase(req.Template, "unknown", "validate_sandbox_config", phaseStarted, err)
			return nil, err
		}
	}
	s.observeClaimPhase(req.Template, "unknown", "validate_sandbox_config", phaseStarted, nil)
	phaseStarted = time.Now()
	if err := s.enforceActiveSandboxQuota(ctx, req.TeamID); err != nil {
		s.observeClaimPhase(req.Template, "unknown", "enforce_active_sandbox_quota", phaseStarted, err)
		return nil, err
	}
	s.observeClaimPhase(req.Template, "unknown", "enforce_active_sandbox_quota", phaseStarted, nil)
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
	if _, err := s.effectiveSandboxResourceQuota(template, req.Config); err != nil {
		s.observeClaimPhase(req.Template, "unknown", "validate_resources", phaseStarted, err)
		return nil, err
	}
	s.observeClaimPhase(req.Template, "unknown", "validate_resources", phaseStarted, nil)
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
	var lifecycleTracker *podLifecycleStageTracker

	// If no idle pod available, create a new one (cold start)
	if pod == nil {
		claimType = "cold"
		s.logger.Info("No idle pod available, creating new pod",
			zap.String("template", req.Template),
		)

		phaseStarted = time.Now()
		pod, err = s.createNewPod(ctx, template, req)
		s.observeClaimPhase(req.Template, claimType, "create_new_pod", phaseStarted, err)
		if err != nil {
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, fmt.Errorf("create new pod: %w", err)
		}
		lifecycleTracker = newPodLifecycleStageTracker(s, req.Template)
		lifecycleTracker.observePod(pod)

		// Note: Network policies are stored in pod annotations.
		// They are set in claimIdlePod() and createNewPod() methods. Hot claims have
		// already selected a Kubernetes-ready idle pod. Cold claims must wait until
		// the pod has the network identity watched by the ctld network runtime before
		// waiting for it to patch the applied policy hash.
		if s.networkProvider != nil {
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
	}

	phaseStarted = time.Now()
	resetCopiedSessionState := req.SnapshotID != "" || templatepkg.HasCopiedRootFS(template.Annotations)
	pod, runtimeRevision, err := s.publishRuntimeAssignment(ctx, pod, resetCopiedSessionState)
	s.observeClaimPhase(req.Template, claimType, "publish_runtime_assignment", phaseStarted, err)
	if err != nil {
		s.requestSandboxDeletionAfterClaimFailure(pod, "runtime assignment publication failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, err
	}
	assignedPodUID := pod.UID

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
		if pod.UID != assignedPodUID {
			pod, runtimeRevision, err = s.publishRuntimeAssignment(ctx, pod, true)
			if err != nil {
				cleanupClaimFailure(pod, "fallback runtime assignment publication failed")
				if metrics != nil {
					metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
				}
				return nil, err
			}
		}
	}

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

	var persistResultCh <-chan error
	if claimType == "hot" && !claimRecordPersisted {
		result := make(chan error, 1)
		persistResultCh = result
		persistPod := pod.DeepCopy()
		persistStarted := time.Now()
		go func() {
			persistErr := s.persistClaimedSandbox(ctx, persistPod, template, req)
			s.observeClaimPhase(req.Template, claimType, "persist_sandbox", persistStarted, persistErr)
			result <- persistErr
		}()
	}

	var persistErr error
	if persistResultCh != nil {
		persistErr = <-persistResultCh
		if persistErr == nil {
			claimRecordPersisted = true
		}
	}
	if persistErr != nil {
		cleanupClaimFailure(pod, "sandbox persistence failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("persist sandbox: %w", persistErr)
	}

	if persistResultCh == nil {
		phaseStarted = time.Now()
		persistErr = s.persistClaimedSandbox(ctx, pod, template, req)
		s.observeClaimPhase(req.Template, claimType, "persist_sandbox", phaseStarted, persistErr)
		if persistErr != nil {
			cleanupClaimFailure(pod, "sandbox persistence failed")
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, fmt.Errorf("persist sandbox: %w", persistErr)
		}
		claimRecordPersisted = true
	}

	phaseStarted = time.Now()
	pod, err = s.activateRuntimeAssignment(ctx, pod, runtimeRevision)
	s.observeClaimPhase(req.Template, claimType, "wait_for_runtime_ready", phaseStarted, err)
	if err != nil {
		cleanupClaimFailure(pod, "runtime activation failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("activate runtime: %w", err)
	}
	if err := s.persistUpdatedSandboxPod(ctx, pod); err != nil {
		cleanupClaimFailure(pod, "runtime readiness persistence failed")
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("persist runtime readiness: %w", err)
	}
	if controller.IsHotClaimReservedPod(pod) {
		phaseStarted = time.Now()
		err = s.completeHotClaimReservation(ctx, pod, template, req)
		s.observeClaimPhase(req.Template, claimType, "complete_hot_claim_reservation", phaseStarted, err)
		if err != nil {
			cleanupClaimFailure(pod, "hot claim reservation completion failed")
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, fmt.Errorf("complete hot claim reservation: %w", err)
		}
	}
	s.enqueueHotClaimReservation(pod)

	if metrics != nil {
		metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "success").Inc()
		metrics.SandboxClaimDuration.WithLabelValues(req.Template, claimType).Observe(time.Since(start).Seconds())
	}

	return &ClaimResponse{
		SandboxID:    req.SandboxID,
		Status:       s.podToSandboxStatus(pod),
		ProcdAddress: procdAddress,
		PodName:      pod.Name,
		Template:     req.Template,
		ClusterId:    template.Spec.ClusterId,
	}, nil
}

func (s *SandboxService) persistClaimedSandbox(ctx context.Context, pod *corev1.Pod, template *v1alpha1.SandboxTemplate, req *ClaimRequest) error {
	if s == nil || s.sandboxStore == nil || pod == nil || template == nil || req == nil {
		return nil
	}
	return s.sandboxStore.UpsertSandbox(ctx, sandboxRecordForClaimedPod(s, pod, template, req))
}

func sandboxRecordForClaimedPod(s *SandboxService, pod *corev1.Pod, template *v1alpha1.SandboxTemplate, req *ClaimRequest) *sandboxstore.SandboxRecord {
	sandboxID := sandboxPodID(pod)
	if sandboxID == "" {
		sandboxID = req.SandboxID
	}
	if sandboxID == "" {
		sandboxID = pod.Name
	}
	cfg := parseSandboxConfig(pod.Annotations[controller.AnnotationConfig])
	record := &sandboxstore.SandboxRecord{
		ID:                  sandboxID,
		TeamID:              req.TeamID,
		UserID:              req.UserID,
		TemplateID:          controller.TemplateLogicalID(template),
		TemplateName:        template.Name,
		TemplateNamespace:   template.Namespace,
		ClusterID:           naming.ClusterIDOrDefault(template.Spec.ClusterId),
		DesiredState:        sandboxstore.SandboxDesiredStateActive,
		Config:              cfg,
		TemplateSpec:        template.Spec,
		CurrentPodName:      pod.Name,
		CurrentPodNamespace: pod.Namespace,
		RuntimeGeneration:   runtimeGenerationFromPod(pod),
		ClaimedAt:           parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationClaimedAt),
		ExpiresAt:           parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt),
		HardExpiresAt:       parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt),
		OwnerKind:           ownerKindFromPod(pod),
		CreatedAt:           s.clock.Now(),
	}
	return record
}

func (s *SandboxService) initializeClaimRootFSFromSnapshot(ctx context.Context, pod *corev1.Pod, template *v1alpha1.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, bool, error) {
	if req == nil || strings.TrimSpace(req.SnapshotID) == "" {
		return pod, false, nil
	}
	snapshotID := strings.TrimSpace(req.SnapshotID)
	if templatepkg.IsBuildSnapshotID(snapshotID) {
		return pod, false, sandboxstore.ErrRootFSSnapshotNotFound
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
	if _, err := restorer.RestoreRootFSFromSnapshot(ctx, &sandboxstore.RestoreRootFSFromSnapshotRequest{
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
		return pod, true, fmt.Errorf("%w: snapshot %s", sandboxstore.ErrRootFSFilesystemNotFound, snapshotID)
	}
	pod, err = s.applySandboxRootFSCheckpointWithFallback(ctx, pod, record, template, req, state, true)
	if err != nil {
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

func (s *SandboxService) claimIdlePod(ctx context.Context, template *v1alpha1.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, error) {
	var claimedPod *corev1.Pod
	lostCandidates := make(map[string]struct{})
	desiredTemplateHash, err := controller.TemplateSpecHash(template)
	if err != nil {
		return nil, fmt.Errorf("compute template hash: %w", err)
	}
	templateID := controller.TemplateLogicalID(template)
	err = retry.OnError(claimIdlePodBackoff, func(err error) bool {
		return k8serrors.IsConflict(err) ||
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
			if _, lost := lostCandidates[pod.Namespace+"/"+pod.Name]; lost {
				continue
			}
			if s.isHotClaimableIdlePod(pod, desiredTemplateHash) {
				readyPods = append(readyPods, pod)
			}
		}

		if len(readyPods) == 0 {
			// No idle pod available, not an error - use a special error to stop retry
			s.observeIdleClaim(templateID, "no_candidate")
			return errNoIdlePod
		}

		// Reserve one informer-cached candidate inside this manager before any
		// claim side effects. The Kubernetes metadata patch remains the
		// cross-process compare-and-swap boundary.
		pod := s.reserveIdleClaimCandidate(readyPods)
		if pod == nil {
			s.observeIdleClaim(templateID, "local_contention")
			return errNoIdlePod
		}
		reservationPersisted := false
		defer func() {
			if !reservationPersisted {
				s.releaseIdleClaimCandidate(pod)
			}
		}()

		sandboxID := strings.TrimSpace(req.SandboxID)
		if sandboxID == "" {
			sandboxID = pod.Name
		}
		reservationToken := utilrand.String(16)
		s.logger.Info("Claiming idle pod",
			zap.String("pod", pod.Name),
			zap.String("sandboxID", sandboxID),
		)

		// Update pod labels and annotations
		originalIdlePod := pod.DeepCopy()
		pod = pod.DeepCopy()
		resourceQuota, err := s.effectiveSandboxResourceQuota(template, req.Config)
		if err != nil {
			return err
		}
		var resizeQuota *v1alpha1.ResourceQuota
		if sandboxPodNeedsResourceResize(pod, resourceQuota) {
			resizeQuota = &resourceQuota
		}

		// Keep the pod attached to its warm-pool ReplicaSet until initialization
		// and durable persistence complete.
		pod.Labels[controller.LabelSandboxID] = sandboxID
		ensureSandboxCleanupFinalizer(pod)

		// Add annotations
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations = controller.ClaimedSandboxPodAnnotations(pod.Annotations, s.config.AutoscalerSafeToEvictAnnotationKeys)
		pod.Annotations[controller.AnnotationSandboxID] = sandboxID
		pod.Annotations[controller.AnnotationRuntimeGeneration] = strconv.FormatInt(req.RuntimeGeneration, 10)
		pod.Annotations[controller.AnnotationTeamID] = req.TeamID
		pod.Annotations[controller.AnnotationUserID] = req.UserID
		pod.Annotations[controller.AnnotationClaimedAt] = s.clock.Now().Format(time.RFC3339)
		pod.Annotations[controller.AnnotationClaimType] = "hot"
		pod.Annotations[controller.AnnotationHotClaimReservation] = reservationToken
		pod.Annotations[controller.AnnotationHotClaimReservationState] = controller.HotClaimReservationStateInitializing
		pod.Annotations[controller.AnnotationHotClaimReservedAt] = s.clock.Now().UTC().Format(time.RFC3339Nano)
		pod.Annotations[controller.AnnotationHotClaimCompletionProtocol] = controller.HotClaimCompletionProtocolRecordV2
		applyClaimMetadata(pod, req.Metadata)

		// Set expiration annotations. Explicit 0 disables TTLs; omitted TTL uses the configured default.
		persistedConfig := s.claimConfigForPersistence(req.Config)
		var ttl, hardTTL *int32
		if persistedConfig != nil {
			ttl = persistedConfig.TTL
			hardTTL = persistedConfig.HardTTL
		}
		setExpirationAnnotation(pod.Annotations, s.clock.Now(), ttl)
		setClaimHardExpirationAnnotation(pod.Annotations, s.clock.Now(), hardTTL, req.HardExpiresAt)
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
		rollbackBindings, err := s.syncCredentialBindings(
			ctx,
			pod,
			req.TeamID,
			networkState,
			req.mayHaveExistingCredentialBindings,
		)
		if err != nil {
			return fmt.Errorf("stage credential bindings: %w", err)
		}

		// Claim through a metadata-only compare-and-swap patch. This avoids
		// replacing status/spec fields owned by kubelet or other controllers.
		updatedPod, updateErr := s.patchClaimedPodMetadata(ctx, originalIdlePod, pod)
		if updateErr != nil {
			if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
				s.logger.Warn("Failed to roll back credential bindings after hot-claim update failure",
					zap.String("sandboxID", sandboxID),
					zap.Error(rollbackErr),
				)
			}
			if s.claimMetadataPatchPreconditionFailed(ctx, originalIdlePod, updateErr) {
				lostCandidates[pod.Namespace+"/"+pod.Name] = struct{}{}
				s.observeIdleClaim(templateID, "update_conflict")
				return fmt.Errorf("%w: patch pod %s/%s: %w", errIdlePodClaimLost, pod.Namespace, pod.Name, updateErr)
			}
			if isIdlePodLostDuringClaim(updateErr) {
				s.observeIdleClaim(templateID, "update_conflict")
				return errNoIdlePod
			}
			if k8serrors.IsConflict(updateErr) {
				s.observeIdleClaim(templateID, "update_conflict")
				return fmt.Errorf("%w: update pod %s/%s: %w", errIdlePodClaimLost, pod.Namespace, pod.Name, updateErr)
			} else {
				s.observeIdleClaim(templateID, "update_error")
			}
			return updateErr
		}
		s.observeIdleClaim(templateID, "reserved")
		// Keep the local reservation until the shared informer observes this
		// durable reservation. Releasing it at PATCH return would reopen the
		// stale-cache window that caused duplicate candidate selection.
		reservationPersisted = true

		if resizeQuota != nil {
			resizedPod, resizeErr := s.resizeSandboxPodResourcesWithClient(
				ctx,
				s.hotClaimClient(),
				updatedPod,
				*resizeQuota,
			)
			if resizeErr != nil {
				if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
					s.logger.Warn("Failed to roll back credential bindings after hot-claim resize failure",
						zap.String("sandboxID", sandboxID),
						zap.Error(rollbackErr),
					)
				}
				if k8serrors.IsConflict(resizeErr) {
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
			errors.Is(err, errIdlePodClaimLost) {
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
	if controller.IsHotClaimReservedPod(pod) {
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

func isClaimMetadataPatchPreconditionFailure(err error) bool {
	if !k8serrors.IsInvalid(err) {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "testing value /metadata/") &&
		strings.Contains(msg, "failed: test failed") {
		return true
	}
	if strings.Contains(msg, "test operation does not apply") &&
		strings.Contains(msg, "/metadata/") {
		return true
	}
	return false
}

// claimMetadataPatchPreconditionFailed resolves sanitized Kubernetes 422
// responses by checking the exact compare-and-swap inputs against live state.
func (s *SandboxService) claimMetadataPatchPreconditionFailed(
	ctx context.Context,
	originalPod *corev1.Pod,
	err error,
) bool {
	if isClaimMetadataPatchPreconditionFailure(err) {
		return true
	}
	if !k8serrors.IsInvalid(err) || s == nil || originalPod == nil {
		return false
	}
	client := s.hotClaimClient()
	if client == nil {
		return false
	}
	current, getErr := client.CoreV1().Pods(originalPod.Namespace).Get(
		ctx,
		originalPod.Name,
		metav1.GetOptions{},
	)
	if k8serrors.IsNotFound(getErr) {
		return true
	}
	if getErr != nil || current == nil {
		return false
	}
	if originalPod.UID != "" && current.UID != originalPod.UID {
		return true
	}
	if originalPod.ResourceVersion != "" && current.ResourceVersion != originalPod.ResourceVersion {
		return true
	}
	return current.Labels[controller.LabelPoolType] != controller.PoolTypeIdle
}

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
	resourceQuota, err := s.effectiveSandboxResourceQuota(template, req.Config)
	if err != nil {
		return nil, err
	}
	if err := applySandboxResourceQuotaToPodSpec(&spec, resourceQuota); err != nil {
		return nil, err
	}
	if err := s.ensureDataPlaneReadyCapacity(spec); err != nil {
		return nil, err
	}
	sandboxID := strings.TrimSpace(req.SandboxID)
	if sandboxID == "" {
		sandboxID = podName
	}
	if err := controller.EnsureProcdConfigSecret(ctx, s.k8sClient, s.secretLister, template); err != nil {
		return nil, fmt.Errorf("ensure procd config secret: %w", err)
	}
	if err := controller.EnsureNetworkMITMCASecret(ctx, s.k8sClient, s.secretLister, template.Namespace); err != nil {
		return nil, fmt.Errorf("ensure network-runtime MITM CA secret: %w", err)
	}

	annotations := controller.ClaimedSandboxPodAnnotations(map[string]string{
		controller.AnnotationSandboxID:         sandboxID,
		controller.AnnotationRuntimeGeneration: strconv.FormatInt(req.RuntimeGeneration, 10),
		controller.AnnotationTeamID:            req.TeamID,
		controller.AnnotationUserID:            req.UserID,
		controller.AnnotationClaimedAt:         s.clock.Now().Format(time.RFC3339),
		controller.AnnotationClaimType:         "cold",
	}, s.config.AutoscalerSafeToEvictAnnotationKeys)
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
	var ttl, hardTTL *int32
	if persistedConfig != nil {
		ttl = persistedConfig.TTL
		hardTTL = persistedConfig.HardTTL
	}
	setExpirationAnnotation(pod.Annotations, s.clock.Now(), ttl)
	setClaimHardExpirationAnnotation(pod.Annotations, s.clock.Now(), hardTTL, req.HardExpiresAt)
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
	rollbackBindings, err := s.syncCredentialBindings(
		ctx,
		pod,
		req.TeamID,
		networkState,
		req.mayHaveExistingCredentialBindings,
	)
	if err != nil {
		return nil, fmt.Errorf("stage credential bindings: %w", err)
	}

	// Create the pod after team quota admission has completed.
	createdPod, err := s.k8sClient.CoreV1().Pods(template.ObjectMeta.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
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
				zap.String("sandboxID", sandboxPodID(pod)),
				zap.String("namespace", pod.Namespace),
				zap.String("reason", reason),
				zap.Error(err),
			)
		}
	}

	if err := s.k8sClient.CoreV1().Pods(pod.Namespace).Delete(cleanupCtx, pod.Name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		logger.Warn("Delete pod failed after claim failure",
			zap.String("sandboxID", sandboxPodID(pod)),
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
	claimedSandboxID := sandboxPodID(claimedPod)

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
		if claimedSandboxID != "" && sandboxPodID(current) != "" && sandboxPodID(current) != claimedSandboxID {
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
	if !selectorRequiresReadyDataPlane(pod.Spec.NodeSelector) {
		return true
	}
	if pod.Spec.NodeName == "" || s == nil || s.nodeLister == nil {
		return false
	}
	node, err := s.nodeLister.Get(pod.Spec.NodeName)
	if err != nil {
		return false
	}
	return nodeDataPlaneReady(node)
}

func (s *SandboxService) ensureDataPlaneReadyCapacity(spec corev1.PodSpec) error {
	if !selectorRequiresReadyDataPlane(spec.NodeSelector) {
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
