package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	egressauth "github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
)

// Sandbox represents a sandbox instance
type Sandbox struct {
	ID            string              `json:"id"`
	TemplateID    string              `json:"template_id"`
	TeamID        string              `json:"team_id"`
	UserID        string              `json:"user_id"`
	InternalAddr  string              `json:"internal_addr"`
	Status        string              `json:"status"`
	Paused        bool                `json:"paused"`
	PowerState    SandboxPowerState   `json:"power_state"`
	AutoResume    bool                `json:"auto_resume"`
	ExposedPorts  []ExposedPortConfig `json:"exposed_ports,omitempty"`
	PodName       string              `json:"pod_name"`
	ExpiresAt     time.Time           `json:"expires_at"`
	HardExpiresAt time.Time           `json:"hard_expires_at"`
	ClaimedAt     time.Time           `json:"claimed_at"`
	CreatedAt     time.Time           `json:"created_at"`
}

// SandboxStatus represents possible sandbox statuses
const (
	SandboxStatusPending      = "pending"
	SandboxStatusStarting     = "starting"
	SandboxStatusRunning      = "running"
	SandboxStatusFailed       = "failed"
	SandboxStatusCompleted    = "completed"
	SandboxPowerStateActive   = "active"
	SandboxPowerStatePaused   = "paused"
	SandboxPowerPhaseStable   = "stable"
	SandboxPowerPhasePausing  = "pausing"
	SandboxPowerPhaseResuming = "resuming"
)

// SandboxPowerState tracks the latest desired and observed power state for async pause/resume.
type SandboxPowerState struct {
	Desired            string `json:"desired"`
	DesiredGeneration  int64  `json:"desired_generation"`
	Observed           string `json:"observed"`
	ObservedGeneration int64  `json:"observed_generation"`
	Phase              string `json:"phase"`
}

// errNoIdlePod is returned when no idle pod is available for claiming.
var errNoIdlePod = errors.New("no idle pod available")
var ErrInvalidClaimRequest = errors.New("invalid claim request")
var errSandboxPowerStateStale = errors.New("sandbox power state changed during execution")

// ErrSandboxPowerTransitionSuperseded is returned when a newer pause/resume request replaces the requested transition.
var ErrSandboxPowerTransitionSuperseded = errors.New("sandbox power transition superseded")

const defaultPodReadyTimeout = 30 * time.Second
const defaultSandboxPowerTransitionTimeout = 2 * time.Minute
const defaultSandboxPowerPollInterval = 100 * time.Millisecond

// claimIdlePodBackoff is the retry backoff for claiming idle pods.
// Designed to balance between:
// - Quick retries to grab an idle pod before other clients
// - Not waiting too long (cold start may be faster than long retries)
// - Not overwhelming the API server with requests
var claimIdlePodBackoff = wait.Backoff{
	Steps:    3, // Max 3 attempts
	Duration: 15 * time.Millisecond,
	Factor:   1.5, // Mild exponential backoff: 15ms, 22ms, 33ms
	Jitter:   0.1, // 10% jitter to spread out concurrent requests
}

// SandboxServiceConfig handles configuration for SandboxService
type SandboxServiceConfig struct {
	DefaultTTL             time.Duration
	PauseMinMemoryRequest  string
	PauseMinMemoryLimit    string
	PauseMemoryBufferRatio float64
	PauseMinCPU            string
	CtldEnabled            bool
	CtldPort               int
	CtldClientTimeout      time.Duration
	ProcdPort              int
	ProcdClientTimeout     time.Duration
	ProcdInitTimeout       time.Duration
}

// SandboxService handles sandbox operations
type SandboxService struct {
	k8sClient              kubernetes.Interface
	podLister              corelisters.PodLister
	sandboxIndex           *SandboxIndex
	secretLister           corelisters.SecretLister
	templateLister         controller.TemplateLister
	NetworkPolicyService   *NetworkPolicyService
	networkProvider        network.Provider
	procdClient            *ProcdClient
	ctldClient             *CtldClient
	internalTokenGenerator TokenGenerator
	procdTokenGenerator    TokenGenerator
	clock                  TimeProvider
	config                 SandboxServiceConfig
	logger                 *zap.Logger
	metrics                *obsmetrics.ManagerMetrics
	autoScaler             AutoScalerInterface
	credentialStore        egressauth.BindingStore
	powerExecutor          SandboxPowerExecutor
	powerStateLocks        sync.Map
	powerStateReconcilers  sync.Map
}

// AutoScalerInterface defines the interface for auto scaling.
// This allows the sandbox service to trigger scale-up during cold claims.
type AutoScalerInterface interface {
	OnColdClaim(ctx context.Context, template *v1alpha1.SandboxTemplate) (*ScaleDecisionResult, error)
}

// ScaleDecisionResult represents the result of a scaling decision.
// This is a local copy to avoid tight coupling with controller package.
type ScaleDecisionResult = controller.ScaleDecision

// TimeProvider provides time functions, allowing for synchronized time across clusters
type TimeProvider interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	Until(t time.Time) time.Duration
}

// systemTime is the default implementation using system time
type systemTime struct{}

func (systemTime) Now() time.Time                  { return time.Now() }
func (systemTime) Since(t time.Time) time.Duration { return time.Since(t) }
func (systemTime) Until(t time.Time) time.Duration { return time.Until(t) }

// TokenGenerator generates internal tokens for procd authentication.
type TokenGenerator interface {
	GenerateToken(teamID, userID, sandboxID string) (string, error)
}

// NewSandboxService creates a new SandboxService
func NewSandboxService(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	sandboxIndex *SandboxIndex,
	secretLister corelisters.SecretLister,
	templateLister controller.TemplateLister,
	networkPolicyService *NetworkPolicyService,
	networkProvider network.Provider,
	internalTokenGenerator TokenGenerator,
	procdTokenGenerator TokenGenerator,
	clock TimeProvider,
	config SandboxServiceConfig,
	logger *zap.Logger,
	metrics *obsmetrics.ManagerMetrics,
) *SandboxService {
	// Use system time as fallback if clock is nil
	if clock == nil {
		clock = systemTime{}
	}
	if config.CtldPort == 0 {
		config.CtldPort = 8095
	}
	if config.CtldClientTimeout == 0 {
		config.CtldClientTimeout = 5 * time.Second
	}
	if networkProvider == nil {
		networkProvider = network.NewNoopProvider()
	}
	service := &SandboxService{
		k8sClient:              k8sClient,
		podLister:              podLister,
		sandboxIndex:           sandboxIndex,
		secretLister:           secretLister,
		templateLister:         templateLister,
		NetworkPolicyService:   networkPolicyService,
		networkProvider:        networkProvider,
		ctldClient:             NewCtldClient(CtldClientConfig{Timeout: config.CtldClientTimeout}),
		procdClient:            NewProcdClient(ProcdClientConfig{Timeout: config.ProcdClientTimeout}),
		internalTokenGenerator: internalTokenGenerator,
		procdTokenGenerator:    procdTokenGenerator,
		clock:                  clock,
		config:                 config,
		logger:                 logger,
		metrics:                metrics,
	}
	service.powerExecutor = newSandboxPowerExecutor(service)
	return service
}

// SupportsNetworkPolicy reports whether this deployment has an active network policy provider.
func (s *SandboxService) SupportsNetworkPolicy() bool {
	return s != nil && s.networkProvider != nil && s.networkProvider.Name() != "noop"
}

// SetProcdClient overrides the procd client (used by tests).
func (s *SandboxService) SetProcdClient(client *ProcdClient) {
	if client == nil {
		return
	}
	s.procdClient = client
}

// SetCtldClient overrides the ctld client (used by tests and future node runtimes).
func (s *SandboxService) SetCtldClient(client *CtldClient) {
	if client == nil {
		return
	}
	s.ctldClient = client
}

// SetAutoScaler injects the auto scaler for automatic pool scaling.
func (s *SandboxService) SetAutoScaler(scaler AutoScalerInterface) {
	s.autoScaler = scaler
}

// SetCredentialStore injects the sandbox credential binding store.
func (s *SandboxService) SetCredentialStore(store egressauth.BindingStore) {
	s.credentialStore = store
}

// SetPowerExecutor overrides sandbox power execution (used by tests and future node executors).
func (s *SandboxService) SetPowerExecutor(executor SandboxPowerExecutor) {
	if executor == nil {
		return
	}
	s.powerExecutor = executor
}

func (s *SandboxService) sandboxPowerExecutor() SandboxPowerExecutor {
	if s.powerExecutor != nil {
		return s.powerExecutor
	}
	return newSandboxPowerExecutor(s)
}

// ClaimRequest represents a sandbox claim request
type ClaimRequest struct {
	TeamID             string
	UserID             string
	Template           string         `json:"template"`
	Config             *SandboxConfig `json:"config,omitempty"`
	Mounts             []ClaimMount   `json:"mounts,omitempty"`
	WaitForMounts      bool           `json:"wait_for_mounts,omitempty"`
	MountWaitTimeoutMs *int32         `json:"mount_wait_timeout_ms,omitempty"`
}

type ClaimMount struct {
	SandboxVolumeID string             `json:"sandboxvolume_id"`
	MountPoint      string             `json:"mount_point"`
	VolumeConfig    *MountVolumeConfig `json:"volume_config,omitempty"`
}

type MountVolumeConfig struct {
	CacheSize  string `json:"cache_size,omitempty"`
	Prefetch   *int32 `json:"prefetch,omitempty"`
	BufferSize string `json:"buffer_size,omitempty"`
	Writeback  *bool  `json:"writeback,omitempty"`
}

type BootstrapMountStatus struct {
	SandboxVolumeID     string `json:"sandboxvolume_id"`
	MountPoint          string `json:"mount_point"`
	State               string `json:"state"`
	MountedAt           string `json:"mounted_at,omitempty"`
	MountedDurationSecs int64  `json:"mounted_duration_sec,omitempty"`
	MountSessionID      string `json:"mount_session_id,omitempty"`
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
	if req.MountWaitTimeoutMs != nil && *req.MountWaitTimeoutMs <= 0 {
		return fmt.Errorf("%w: mount_wait_timeout_ms must be greater than zero", ErrInvalidClaimRequest)
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

func claimMountWaitTimeout(req *ClaimRequest) time.Duration {
	if req == nil || !req.WaitForMounts {
		return 0
	}
	if req.MountWaitTimeoutMs != nil && *req.MountWaitTimeoutMs > 0 {
		return time.Duration(*req.MountWaitTimeoutMs) * time.Millisecond
	}
	return 30 * time.Second
}

func toInitializeMountRequests(in []ClaimMount) []InitializeMountRequest {
	if len(in) == 0 {
		return nil
	}
	out := make([]InitializeMountRequest, len(in))
	for i := range in {
		out[i] = InitializeMountRequest(in[i])
	}
	return out
}

func toBootstrapMountStatuses(in []BootstrapMountStatus) []BootstrapMountStatus {
	if len(in) == 0 {
		return nil
	}
	out := make([]BootstrapMountStatus, 0, len(in))
	out = append(out, in...)
	return out
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
	canonicalTemplateID, err := naming.CanonicalTemplateID(req.Template)
	if err != nil {
		return nil, err
	}
	req.Template = canonicalTemplateID
	if err := validateClaimMounts(req); err != nil {
		return nil, err
	}
	s.logger.Info("Claiming sandbox",
		zap.String("template", req.Template),
		zap.String("teamID", req.TeamID),
	)

	// Resolve tenant template name:
	// prefer team-scoped template, fall back to public, and always enforce ownership checks.
	resolvedName := req.Template
	var template *v1alpha1.SandboxTemplate

	if req.TeamID != "" {
		privateName := naming.TemplateNameForCluster(naming.ScopeTeam, req.TeamID, req.Template)
		privateNamespace, nsErr := naming.TemplateNamespaceForTeam(req.TeamID)
		if nsErr != nil {
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
			return nil, fmt.Errorf("resolve template namespace for %s: %w", req.Template, nsErr)
		}
		template, err = s.templateLister.Get(publicNamespace, req.Template)
		if err != nil {
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
			return nil, fmt.Errorf("forbidden: template belongs to a different team")
		}
	}

	_ = resolvedName // reserved for audit/debugging (name used is template.ObjectMeta.Name)

	// Try to claim an idle pod first
	pod, err := s.claimIdlePod(ctx, template, req)
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

		pod, err = s.createNewPod(ctx, template, req)
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
		pod, err = s.waitForPodClaimReady(ctx, pod.Namespace, pod.Name)
		if err != nil {
			s.cleanupFailedColdClaim(pod, "claim readiness failed")
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, fmt.Errorf("wait for pod claim readiness: %w", err)
		}
		s.refreshSandboxProbeConditionsAsync(pod)
	}

	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		if claimType == "cold" {
			s.cleanupFailedColdClaim(pod, "procd address resolution failed")
		}
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	initResp, err := s.initializeProcd(ctx, pod, req, procdAddress)
	if err != nil {
		if claimType == "cold" {
			s.cleanupFailedColdClaim(pod, "procd initialization failed")
		}
		if metrics != nil {
			metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
		}
		return nil, fmt.Errorf("initialize procd: %w", err)
	}

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
		BootstrapMounts: toBootstrapMountStatuses(initResp.BootstrapMounts),
	}, nil
}

// claimIdlePod claims an idle pod from the pool
func (s *SandboxService) claimIdlePod(ctx context.Context, template *v1alpha1.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, error) {
	var claimedPod *corev1.Pod
	err := retry.OnError(claimIdlePodBackoff, k8serrors.IsConflict, func() error {
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
			if controller.IsPodReady(pod) {
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

		// Update pod labels and annotations
		pod = pod.DeepCopy()

		// Change pool type from idle to active
		pod.Labels[controller.LabelPoolType] = controller.PoolTypeActive
		pod.Labels[controller.LabelSandboxID] = pod.Name

		// Remove owner reference (so it's no longer managed by ReplicaSet)
		pod.OwnerReferences = nil

		// Add annotations
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations[controller.AnnotationSandboxID] = pod.Name
		pod.Annotations[controller.AnnotationTeamID] = req.TeamID
		pod.Annotations[controller.AnnotationUserID] = req.UserID
		pod.Annotations[controller.AnnotationClaimedAt] = s.clock.Now().Format(time.RFC3339)
		pod.Annotations[controller.AnnotationClaimType] = "hot"

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
			if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
				s.logger.Warn("Failed to roll back credential bindings after hot-claim update failure",
					zap.String("sandboxID", pod.Name),
					zap.Error(rollbackErr),
				)
			}
			return updateErr
		}

		if applyErr := s.applyNetworkProvider(ctx, updatedPod, req.TeamID, policySpecFromState(networkState)); applyErr != nil {
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

// createNewPod creates a new pod for cold start
func (s *SandboxService) createNewPod(ctx context.Context, template *v1alpha1.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, error) {
	// Simulate K8s pod name generation: rs-name + "-" + 5 random chars
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	podName, err := naming.SandboxName(clusterID, template.Name, utilrand.String(5))
	if err != nil {
		return nil, fmt.Errorf("generate sandbox name: %w", err)
	}
	if err := controller.EnsureProcdConfigSecret(ctx, s.k8sClient, s.secretLister, template); err != nil {
		return nil, fmt.Errorf("ensure procd config secret: %w", err)
	}
	if err := controller.EnsureNetdMITMCASecret(ctx, s.k8sClient, template.Namespace); err != nil {
		return nil, fmt.Errorf("ensure netd MITM CA secret: %w", err)
	}

	// Build pod spec from template
	spec := v1alpha1.BuildPodSpec(template)
	annotations := map[string]string{
		controller.AnnotationSandboxID: podName,
		controller.AnnotationTeamID:    req.TeamID,
		controller.AnnotationUserID:    req.UserID,
		controller.AnnotationClaimedAt: s.clock.Now().Format(time.RFC3339),
		controller.AnnotationClaimType: "cold",
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: template.Namespace,
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
		if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
			s.logger.Warn("Failed to clean up staged credential bindings after create failure",
				zap.String("sandboxID", pod.Name),
				zap.Error(rollbackErr),
			)
		}
		return nil, fmt.Errorf("create pod: %w", err)
	}

	if err := s.applyNetworkProvider(ctx, createdPod, req.TeamID, policySpecFromState(networkState)); err != nil {
		s.cleanupFailedColdClaim(createdPod, "network policy apply failed")
		return nil, fmt.Errorf("apply network policy: %w", err)
	}

	s.logger.Info("Created new pod for cold start",
		zap.String("pod", createdPod.Name),
		zap.String("sandboxID", createdPod.Name),
		zap.String("expiresAt", createdPod.Annotations[controller.AnnotationExpiresAt]),
	)

	return createdPod, nil
}

func (s *SandboxService) cleanupFailedColdClaim(pod *corev1.Pod, reason string) {
	if s == nil || pod == nil || pod.Name == "" || pod.Namespace == "" || s.k8sClient == nil {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	logger := s.logger
	if logger == nil {
		logger = zap.NewNop()
	}

	if s.networkProvider != nil {
		if err := s.networkProvider.RemoveSandboxPolicy(cleanupCtx, pod.Namespace, pod.Name); err != nil {
			logger.Warn("Network provider cleanup failed after cold claim failure",
				zap.String("provider", s.networkProvider.Name()),
				zap.String("sandboxID", pod.Name),
				zap.String("reason", reason),
				zap.Error(err),
			)
		}
	}

	if err := s.k8sClient.CoreV1().Pods(pod.Namespace).Delete(cleanupCtx, pod.Name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		logger.Warn("Delete pod failed after cold claim failure",
			zap.String("sandboxID", pod.Name),
			zap.String("namespace", pod.Namespace),
			zap.String("reason", reason),
			zap.Error(err),
		)
	}
	if err := s.deleteCredentialBindings(cleanupCtx, pod); err != nil {
		logger.Warn("Credential binding cleanup failed after cold claim failure",
			zap.String("sandboxID", pod.Name),
			zap.String("reason", reason),
			zap.Error(err),
		)
	}
}

type webhookInfo struct {
	URL      string
	Secret   string
	WatchDir string
}

func (s *SandboxService) getWebhookInfo(req *ClaimRequest) *webhookInfo {
	if req == nil || req.Config == nil || req.Config.Webhook == nil {
		return nil
	}
	urlValue := strings.TrimSpace(req.Config.Webhook.URL)
	if urlValue == "" {
		return nil
	}
	return &webhookInfo{
		URL:      urlValue,
		Secret:   strings.TrimSpace(req.Config.Webhook.Secret),
		WatchDir: strings.TrimSpace(req.Config.Webhook.WatchDir),
	}
}

func (s *SandboxService) appendWebhookNetworkPolicy(
	requestNetwork *v1alpha1.SandboxNetworkPolicy,
	webhookURL string,
) *v1alpha1.SandboxNetworkPolicy {
	if webhookURL == "" {
		return requestNetwork
	}
	parsed, err := url.Parse(webhookURL)
	if err != nil {
		s.logger.Warn("Failed to parse webhook URL",
			zap.String("webhook_url", webhookURL),
			zap.Error(err),
		)
		return requestNetwork
	}
	host := parsed.Hostname()
	if host == "" {
		s.logger.Warn("Webhook URL missing hostname",
			zap.String("webhook_url", webhookURL),
		)
		return requestNetwork
	}
	if requestNetwork == nil {
		requestNetwork = &v1alpha1.SandboxNetworkPolicy{}
	}
	if requestNetwork.Egress == nil {
		requestNetwork.Egress = &v1alpha1.NetworkEgressPolicy{}
	}
	if ip := net.ParseIP(host); ip != nil {
		requestNetwork.Egress.AllowedCIDRs = append(requestNetwork.Egress.AllowedCIDRs, formatCIDRForIP(ip))
		return requestNetwork
	}
	requestNetwork.Egress.AllowedDomains = append(requestNetwork.Egress.AllowedDomains, host)
	return requestNetwork
}

func formatCIDRForIP(ip net.IP) string {
	if ip.To4() != nil {
		return ip.String() + "/32"
	}
	return ip.String() + "/128"
}

func (s *SandboxService) applyPoliciesForPod(
	ctx context.Context,
	pod *corev1.Pod,
	template *v1alpha1.SandboxTemplate,
	req *ClaimRequest,
) (*BuildNetworkPolicyResult, error) {
	if s.NetworkPolicyService == nil || pod == nil || template == nil || req == nil {
		return nil, nil
	}

	var requestNetwork *v1alpha1.SandboxNetworkPolicy
	if req.Config != nil {
		requestNetwork = req.Config.Network
	}
	webhookInfo := s.getWebhookInfo(req)
	if webhookInfo != nil {
		requestNetwork = s.appendWebhookNetworkPolicy(requestNetwork, webhookInfo.URL)
	}

	networkState := s.NetworkPolicyService.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID:        pod.Name,
		TeamID:           req.TeamID,
		TemplateSpec:     template.Spec.Network,
		RequestSpec:      requestNetwork,
		TemplateBindings: templateCredentialBindings(template.Spec.Network),
		RequestBindings:  requestCredentialBindings(req.Config),
	})
	if networkState != nil && networkState.PolicySpec != nil {
		if _, err := s.setNetworkPolicyAnnotations(pod, networkState.PolicySpec); err != nil {
			return nil, err
		}
	}

	return networkState, nil
}

func (s *SandboxService) setNetworkPolicyAnnotations(pod *corev1.Pod, spec *v1alpha1.NetworkPolicySpec) (string, error) {
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	annotation, err := v1alpha1.NetworkPolicyToAnnotation(spec)
	if err != nil {
		return "", fmt.Errorf("serialize network policy: %w", err)
	}
	pod.Annotations[controller.AnnotationNetworkPolicy] = annotation
	newHash := policyAnnotationHash(annotation)
	oldHash := pod.Annotations[controller.AnnotationNetworkPolicyHash]
	if newHash != "" {
		pod.Annotations[controller.AnnotationNetworkPolicyHash] = newHash
	} else {
		delete(pod.Annotations, controller.AnnotationNetworkPolicyHash)
	}
	if oldHash != newHash {
		delete(pod.Annotations, controller.AnnotationNetworkPolicyAppliedHash)
	}
	return newHash, nil
}

func policySpecFromState(state *BuildNetworkPolicyResult) *v1alpha1.NetworkPolicySpec {
	if state == nil {
		return nil
	}
	return state.PolicySpec
}

func noopCredentialBindingRollback(context.Context) error {
	return nil
}

func requestCredentialBindings(cfg *SandboxConfig) []v1alpha1.CredentialBinding {
	if cfg == nil || cfg.Network == nil || cfg.Network.CredentialBindings == nil {
		return nil
	}
	return append([]v1alpha1.CredentialBinding(nil), cfg.Network.CredentialBindings...)
}

func templateCredentialBindings(policy *v1alpha1.SandboxNetworkPolicy) []v1alpha1.CredentialBinding {
	if policy == nil || policy.CredentialBindings == nil {
		return nil
	}
	return append([]v1alpha1.CredentialBinding(nil), policy.CredentialBindings...)
}

func (s *SandboxService) syncCredentialBindings(
	ctx context.Context,
	pod *corev1.Pod,
	teamID string,
	state *BuildNetworkPolicyResult,
) (func(context.Context) error, error) {
	if s.credentialStore == nil || pod == nil || state == nil {
		return noopCredentialBindingRollback, nil
	}

	previous, err := s.credentialStore.GetBindings(ctx, teamID, pod.Name)
	if err != nil {
		return nil, err
	}
	previous = cloneBindingRecord(previous)

	rollback := func(rollbackCtx context.Context) error {
		if previous == nil || len(previous.Bindings) == 0 {
			return s.credentialStore.DeleteBindings(rollbackCtx, teamID, pod.Name)
		}
		return s.credentialStore.UpsertBindings(rollbackCtx, previous)
	}

	if len(state.CredentialBindings) == 0 {
		if previous == nil || len(previous.Bindings) == 0 {
			return rollback, nil
		}
		if err := s.credentialStore.DeleteBindings(ctx, teamID, pod.Name); err != nil {
			return nil, err
		}
		return rollback, nil
	}

	storeBindings, err := toStoreCredentialBindings(ctx, s.credentialStore, teamID, state.CredentialBindings)
	if err != nil {
		return nil, err
	}

	if err := s.credentialStore.UpsertBindings(ctx, &egressauth.BindingRecord{
		SandboxID: pod.Name,
		TeamID:    teamID,
		Bindings:  storeBindings,
	}); err != nil {
		return nil, err
	}
	return rollback, nil
}

func cloneBindingRecord(record *egressauth.BindingRecord) *egressauth.BindingRecord {
	if record == nil {
		return nil
	}
	cloned := *record
	cloned.Bindings = cloneStoreCredentialBindings(record.Bindings)
	return &cloned
}

func (s *SandboxService) deleteCredentialBindings(ctx context.Context, pod *corev1.Pod) error {
	if s.credentialStore == nil || pod == nil {
		return nil
	}
	return s.credentialStore.DeleteBindings(ctx, sandboxTeamID(pod), pod.Name)
}

func (s *SandboxService) loadCredentialBindings(ctx context.Context, pod *corev1.Pod) ([]v1alpha1.CredentialBinding, error) {
	if s.credentialStore == nil || pod == nil {
		return nil, nil
	}
	record, err := s.credentialStore.GetBindings(ctx, sandboxTeamID(pod), pod.Name)
	if err != nil {
		return nil, err
	}
	if record == nil || len(record.Bindings) == 0 {
		return nil, nil
	}
	return fromStoreCredentialBindings(record.Bindings), nil
}

func sandboxTeamID(pod *corev1.Pod) string {
	if pod != nil && pod.Annotations != nil {
		if teamID := pod.Annotations[controller.AnnotationTeamID]; teamID != "" {
			return teamID
		}
	}
	return ""
}

func toStoreCredentialBindings(
	ctx context.Context,
	store egressauth.BindingStore,
	teamID string,
	in []v1alpha1.CredentialBinding,
) ([]egressauth.CredentialBinding, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]egressauth.CredentialBinding, 0, len(in))
	for _, binding := range in {
		source, err := store.GetSourceByRef(ctx, teamID, binding.SourceRef)
		if err != nil {
			return nil, fmt.Errorf("resolve credential source %q: %w", binding.SourceRef, err)
		}
		if source == nil {
			return nil, fmt.Errorf("credential source %q not found", binding.SourceRef)
		}
		storeBinding := egressauth.CredentialBinding{
			Ref:           binding.Ref,
			SourceRef:     binding.SourceRef,
			SourceID:      source.ID,
			SourceVersion: source.CurrentVersion,
			Projection:    toStoreProjection(binding.Projection),
			CachePolicy:   toStoreCachePolicy(binding.CachePolicy),
		}
		out = append(out, storeBinding)
	}
	return out, nil
}

func cloneStoreCredentialBindings(in []egressauth.CredentialBinding) []egressauth.CredentialBinding {
	if len(in) == 0 {
		return nil
	}
	out := make([]egressauth.CredentialBinding, 0, len(in))
	for _, binding := range in {
		cloned := egressauth.CredentialBinding{
			Ref:           binding.Ref,
			SourceRef:     binding.SourceRef,
			SourceID:      binding.SourceID,
			SourceVersion: binding.SourceVersion,
			Projection:    cloneStoreProjection(binding.Projection),
			CachePolicy:   cloneStoreCachePolicy(binding.CachePolicy),
		}
		out = append(out, cloned)
	}
	return out
}

func fromStoreCredentialBindings(in []egressauth.CredentialBinding) []v1alpha1.CredentialBinding {
	if len(in) == 0 {
		return nil
	}
	out := make([]v1alpha1.CredentialBinding, 0, len(in))
	for _, binding := range in {
		policyBinding := v1alpha1.CredentialBinding{
			Ref:         binding.Ref,
			SourceRef:   binding.SourceRef,
			Projection:  fromStoreProjection(binding.Projection),
			CachePolicy: fromStoreCachePolicy(binding.CachePolicy),
		}
		out = append(out, policyBinding)
	}
	return out
}

func toStoreProjection(in v1alpha1.ProjectionSpec) egressauth.ProjectionSpec {
	out := egressauth.ProjectionSpec{
		Type: egressauth.CredentialProjectionType(in.Type),
	}
	if in.HTTPHeaders != nil {
		out.HTTPHeaders = &egressauth.HTTPHeadersProjection{
			Headers: make([]egressauth.ProjectedHeader, 0, len(in.HTTPHeaders.Headers)),
		}
		for _, header := range in.HTTPHeaders.Headers {
			out.HTTPHeaders.Headers = append(out.HTTPHeaders.Headers, egressauth.ProjectedHeader{
				Name:          header.Name,
				ValueTemplate: header.ValueTemplate,
			})
		}
	}
	if in.TLSClientCertificate != nil {
		out.TLSClientCertificate = &egressauth.TLSClientCertificateProjection{}
	}
	if in.UsernamePassword != nil {
		out.UsernamePassword = &egressauth.UsernamePasswordProjection{}
	}
	return out
}

func cloneStoreProjection(in egressauth.ProjectionSpec) egressauth.ProjectionSpec {
	out := egressauth.ProjectionSpec{
		Type: in.Type,
	}
	if in.HTTPHeaders != nil {
		out.HTTPHeaders = &egressauth.HTTPHeadersProjection{
			Headers: make([]egressauth.ProjectedHeader, 0, len(in.HTTPHeaders.Headers)),
		}
		out.HTTPHeaders.Headers = append(out.HTTPHeaders.Headers, in.HTTPHeaders.Headers...)
	}
	if in.TLSClientCertificate != nil {
		out.TLSClientCertificate = &egressauth.TLSClientCertificateProjection{}
	}
	if in.UsernamePassword != nil {
		out.UsernamePassword = &egressauth.UsernamePasswordProjection{}
	}
	return out
}

func fromStoreProjection(in egressauth.ProjectionSpec) v1alpha1.ProjectionSpec {
	out := v1alpha1.ProjectionSpec{
		Type: v1alpha1.CredentialProjectionType(in.Type),
	}
	if in.HTTPHeaders != nil {
		out.HTTPHeaders = &v1alpha1.HTTPHeadersProjection{
			Headers: make([]v1alpha1.ProjectedHeader, 0, len(in.HTTPHeaders.Headers)),
		}
		for _, header := range in.HTTPHeaders.Headers {
			out.HTTPHeaders.Headers = append(out.HTTPHeaders.Headers, v1alpha1.ProjectedHeader{
				Name:          header.Name,
				ValueTemplate: header.ValueTemplate,
			})
		}
	}
	if in.TLSClientCertificate != nil {
		out.TLSClientCertificate = &v1alpha1.TLSClientCertificateProjection{}
	}
	if in.UsernamePassword != nil {
		out.UsernamePassword = &v1alpha1.UsernamePasswordProjection{}
	}
	return out
}

func toStoreCachePolicy(in *v1alpha1.CachePolicySpec) *egressauth.CachePolicySpec {
	if in == nil {
		return nil
	}
	return &egressauth.CachePolicySpec{TTL: in.TTL}
}

func cloneStoreCachePolicy(in *egressauth.CachePolicySpec) *egressauth.CachePolicySpec {
	if in == nil {
		return nil
	}
	return &egressauth.CachePolicySpec{TTL: in.TTL}
}

func fromStoreCachePolicy(in *egressauth.CachePolicySpec) *v1alpha1.CachePolicySpec {
	if in == nil {
		return nil
	}
	return &v1alpha1.CachePolicySpec{TTL: in.TTL}
}

func sanitizedNetworkPolicyForPersistence(policy *v1alpha1.SandboxNetworkPolicy) *v1alpha1.SandboxNetworkPolicy {
	if policy == nil {
		return nil
	}
	cloned := policy.DeepCopy()
	cloned.CredentialBindings = nil
	return cloned
}

func (s *SandboxService) applyNetworkProvider(
	ctx context.Context,
	pod *corev1.Pod,
	teamID string,
	networkSpec *v1alpha1.NetworkPolicySpec,
) error {
	if s.networkProvider == nil || pod == nil || networkSpec == nil {
		return nil
	}

	input := network.SandboxPolicyInput{
		SandboxID:     pod.Name,
		Namespace:     pod.Namespace,
		PodName:       pod.Name,
		TeamID:        teamID,
		PodLabels:     pod.Labels,
		NetworkPolicy: networkSpec,
	}
	if err := s.networkProvider.ApplySandboxPolicy(ctx, input); err != nil {
		return err
	}
	return nil
}

func (s *SandboxService) initializeProcd(
	ctx context.Context,
	pod *corev1.Pod,
	req *ClaimRequest,
	procdAddress string,
) (*InitializeResponse, error) {
	if s.internalTokenGenerator == nil || s.procdTokenGenerator == nil {
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

	procdToken, err := s.procdTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate procd token: %w", err)
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
		SandboxID:          sandboxID,
		TeamID:             teamID,
		Webhook:            webhookConfig,
		Mounts:             toInitializeMountRequests(req.Mounts),
		WaitForMounts:      req.WaitForMounts,
		MountWaitTimeoutMs: int32(claimMountWaitTimeout(req) / time.Millisecond),
	}

	var initErr error
	var initResp *InitializeResponse
	timeout := s.config.ProcdInitTimeout
	if timeout == 0 {
		timeout = 6 * time.Second
	}
	if waitTimeout := claimMountWaitTimeout(req); waitTimeout > timeout {
		timeout = waitTimeout + time.Second
	}

	initCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		initResp, initErr = s.procdClient.Initialize(initCtx, procdAddress, initReq, internalToken, procdToken)
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

	// Note: Network policies are stored in pod annotations
	// They are automatically deleted when the pod is deleted
	if s.networkProvider != nil {
		if err := s.networkProvider.RemoveSandboxPolicy(ctx, pod.Namespace, pod.Name); err != nil {
			s.logger.Warn("Network provider remove failed",
				zap.String("provider", s.networkProvider.Name()),
				zap.String("sandboxID", pod.Name),
				zap.Error(err),
			)
		}
	}

	// Delete the pod
	err = s.k8sClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete pod: %w", err)
	}
	if err := s.deleteCredentialBindings(ctx, pod); err != nil {
		s.logger.Warn("Credential binding cleanup failed",
			zap.String("sandboxID", pod.Name),
			zap.Error(err),
		)
	}

	s.logger.Info("Sandbox terminated", zap.String("sandboxID", sandboxID), zap.String("pod", pod.Name))

	return nil
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
		if cfg.ExposedPorts != nil {
			merged.ExposedPorts = cfg.ExposedPorts
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

// GetNetworkPolicy gets the effective sandbox network policy.
func (s *SandboxService) GetNetworkPolicy(ctx context.Context, sandboxID string) (*v1alpha1.SandboxNetworkPolicy, error) {
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	annotation := ""
	if pod.Annotations != nil {
		annotation = pod.Annotations[controller.AnnotationNetworkPolicy]
	}
	spec, err := v1alpha1.ParseNetworkPolicyFromAnnotation(annotation)
	if err != nil {
		return nil, fmt.Errorf("parse network policy annotation: %w", err)
	}
	if spec != nil {
		bindings, err := s.loadCredentialBindings(ctx, pod)
		if err != nil {
			return nil, fmt.Errorf("load credential bindings: %w", err)
		}
		return sandboxNetworkPolicyFromParts(spec, bindings), nil
	}

	templateSpec, templateBindings := s.templateNetworkDefaults(pod)
	if templateSpec != nil || len(templateBindings) > 0 {
		return sandboxNetworkPolicyWithBindings(templateSpec, templateBindings), nil
	}

	return &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}, nil
}

// UpdateNetworkPolicy updates the sandbox network policy.
func (s *SandboxService) UpdateNetworkPolicy(
	ctx context.Context,
	sandboxID string,
	policy *v1alpha1.SandboxNetworkPolicy,
) (*v1alpha1.SandboxNetworkPolicy, error) {
	if policy == nil {
		return nil, fmt.Errorf("network policy is required")
	}
	if s.NetworkPolicyService == nil {
		return nil, fmt.Errorf("network policy service not configured")
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

		teamID := ""
		if current.Annotations != nil {
			teamID = current.Annotations[controller.AnnotationTeamID]
		}
		templateSpec, templateBindings := s.templateNetworkDefaults(current)
		requestBindings := append([]v1alpha1.CredentialBinding(nil), policy.CredentialBindings...)
		if policy.CredentialBindings == nil {
			requestBindings, err = s.loadCredentialBindings(ctx, current)
			if err != nil {
				return fmt.Errorf("load credential bindings: %w", err)
			}
		}

		networkState = s.NetworkPolicyService.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
			SandboxID:        current.Name,
			TeamID:           teamID,
			TemplateSpec:     templateSpec,
			RequestSpec:      policy,
			TemplateBindings: templateBindings,
			RequestBindings:  requestBindings,
		})
		rollbackBindings, err = s.syncCredentialBindings(ctx, current, teamID, networkState)
		if err != nil {
			return fmt.Errorf("stage credential bindings: %w", err)
		}

		updatedPod = current.DeepCopy()
		if updatedPod.Annotations == nil {
			updatedPod.Annotations = make(map[string]string)
		}
		if _, err := s.setNetworkPolicyAnnotations(updatedPod, policySpecFromState(networkState)); err != nil {
			return err
		}

		if configJSON := updatedPod.Annotations[controller.AnnotationConfig]; configJSON != "" {
			var storedConfig SandboxConfig
			if err := json.Unmarshal([]byte(configJSON), &storedConfig); err != nil {
				s.logger.Warn("Failed to parse sandbox config annotation",
					zap.String("sandboxID", sandboxID),
					zap.Error(err),
				)
			} else {
				storedConfig.Network = sanitizedNetworkPolicyForPersistence(policy)
				updatedConfigJSON, err := json.Marshal(storedConfig)
				if err != nil {
					return fmt.Errorf("marshal sandbox config: %w", err)
				}
				updatedPod.Annotations[controller.AnnotationConfig] = string(updatedConfigJSON)
			}
		} else {
			storedConfig := SandboxConfig{Network: sanitizedNetworkPolicyForPersistence(policy)}
			updatedConfigJSON, err := json.Marshal(storedConfig)
			if err != nil {
				return fmt.Errorf("marshal sandbox config: %w", err)
			}
			updatedPod.Annotations[controller.AnnotationConfig] = string(updatedConfigJSON)
		}

		updatedPod, err = s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, updatedPod, metav1.UpdateOptions{})
		if err != nil && rollbackBindings != nil {
			if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
				s.logger.Warn("Failed to roll back credential bindings after network policy update failure",
					zap.String("sandboxID", sandboxID),
					zap.Error(rollbackErr),
				)
			}
		}
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("update pod annotations: %w", err)
	}

	teamID := ""
	if updatedPod.Annotations != nil {
		teamID = updatedPod.Annotations[controller.AnnotationTeamID]
	}
	if err := s.applyNetworkProvider(ctx, updatedPod, teamID, policySpecFromState(networkState)); err != nil {
		return nil, fmt.Errorf("apply network policy: %w", err)
	}

	return sandboxNetworkPolicyFromState(networkState), nil
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

func (s *SandboxService) templateNetworkDefaults(pod *corev1.Pod) (*v1alpha1.SandboxNetworkPolicy, []v1alpha1.CredentialBinding) {
	template := s.templateForPod(pod)
	if template == nil {
		return nil, nil
	}
	return template.Spec.Network, templateCredentialBindings(template.Spec.Network)
}

func (s *SandboxService) templateForPod(pod *corev1.Pod) *v1alpha1.SandboxTemplate {
	if pod == nil || s.templateLister == nil {
		return nil
	}
	templateID := pod.Labels[controller.LabelTemplateID]
	if templateID == "" {
		return nil
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	if teamID != "" {
		namespace, err := naming.TemplateNamespaceForTeam(teamID)
		if err == nil {
			template, getErr := s.templateLister.Get(namespace, templateID)
			if getErr == nil {
				return template
			}
		}
	}

	namespace, err := naming.TemplateNamespaceForBuiltin(templateID)
	if err != nil {
		s.logger.Warn("Failed to resolve template namespace",
			zap.String("templateID", templateID),
			zap.Error(err),
		)
		return nil
	}
	template, err := s.templateLister.Get(namespace, templateID)
	if err != nil {
		s.logger.Warn("Failed to get template for network policy",
			zap.String("templateID", templateID),
			zap.String("namespace", namespace),
			zap.Error(err),
		)
		return nil
	}
	return template
}

func networkPolicyFromSpec(spec *v1alpha1.NetworkPolicySpec) *v1alpha1.SandboxNetworkPolicy {
	if spec == nil {
		return &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	}

	var (
		egressAllowedCIDRs    []string
		egressDeniedCIDRs     []string
		egressAllowedDomains  []string
		egressDeniedDomains   []string
		egressAllowedPorts    []v1alpha1.PortSpec
		egressDeniedPorts     []v1alpha1.PortSpec
		egressTrafficRules    []v1alpha1.TrafficRule
		egressCredentialRules []v1alpha1.EgressCredentialRule
	)
	if spec.Egress != nil {
		egressAllowedCIDRs = append(egressAllowedCIDRs, spec.Egress.AllowedCIDRs...)
		egressDeniedCIDRs = append(egressDeniedCIDRs, spec.Egress.DeniedCIDRs...)
		egressAllowedDomains = append(egressAllowedDomains, spec.Egress.AllowedDomains...)
		egressDeniedDomains = append(egressDeniedDomains, spec.Egress.DeniedDomains...)
		egressAllowedPorts = append(egressAllowedPorts, spec.Egress.AllowedPorts...)
		egressDeniedPorts = append(egressDeniedPorts, spec.Egress.DeniedPorts...)
		egressTrafficRules = append(egressTrafficRules, spec.Egress.TrafficRules...)
		egressCredentialRules = append(egressCredentialRules, spec.Egress.CredentialRules...)
	}

	mode := v1alpha1.NetworkModeAllowAll
	if spec.Mode != "" {
		mode = spec.Mode
	}

	policy := &v1alpha1.SandboxNetworkPolicy{
		Mode: mode,
	}
	if len(egressAllowedCIDRs)+len(egressDeniedCIDRs)+len(egressAllowedDomains)+len(egressDeniedDomains)+len(egressAllowedPorts)+len(egressDeniedPorts)+len(egressTrafficRules)+len(egressCredentialRules) > 0 {
		policy.Egress = &v1alpha1.NetworkEgressPolicy{
			AllowedCIDRs:    egressAllowedCIDRs,
			DeniedCIDRs:     egressDeniedCIDRs,
			AllowedDomains:  egressAllowedDomains,
			DeniedDomains:   egressDeniedDomains,
			AllowedPorts:    egressAllowedPorts,
			DeniedPorts:     egressDeniedPorts,
			TrafficRules:    egressTrafficRules,
			CredentialRules: egressCredentialRules,
		}
	}

	return policy
}

func sandboxNetworkPolicyWithBindings(policy *v1alpha1.SandboxNetworkPolicy, bindings []v1alpha1.CredentialBinding) *v1alpha1.SandboxNetworkPolicy {
	result := &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	if policy != nil {
		result.Mode = policy.Mode
		result.Egress = policy.Egress.DeepCopy()
	}
	if len(bindings) > 0 {
		result.CredentialBindings = append(result.CredentialBindings, bindings...)
	}
	return result
}

func sandboxNetworkPolicyFromParts(spec *v1alpha1.NetworkPolicySpec, bindings []v1alpha1.CredentialBinding) *v1alpha1.SandboxNetworkPolicy {
	return sandboxNetworkPolicyWithBindings(networkPolicyFromSpec(spec), bindings)
}

func sandboxNetworkPolicyFromState(state *BuildNetworkPolicyResult) *v1alpha1.SandboxNetworkPolicy {
	if state == nil {
		return &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	}
	return sandboxNetworkPolicyFromParts(state.PolicySpec, state.CredentialBindings)
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
		TemplateID:    pod.Labels[controller.LabelTemplateID],
		TeamID:        pod.Annotations[controller.AnnotationTeamID],
		UserID:        pod.Annotations[controller.AnnotationUserID],
		InternalAddr:  internalAddr,
		Status:        status,
		Paused:        powerState.Observed == SandboxPowerStatePaused,
		PowerState:    powerState,
		AutoResume:    autoResume,
		ExposedPorts:  cfg.ExposedPorts,
		PodName:       pod.Name,
		ExpiresAt:     expiresAt,
		HardExpiresAt: hardExpiresAt,
		ClaimedAt:     claimedAt,
		CreatedAt:     createdAt,
	}
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

func sandboxPowerStateFromAnnotations(annotations map[string]string) SandboxPowerState {
	legacyObserved := SandboxPowerStateActive
	if len(annotations) > 0 && annotations[controller.AnnotationPaused] == "true" {
		legacyObserved = SandboxPowerStatePaused
	}

	state := SandboxPowerState{
		Desired:            normalizeSandboxPowerState(annotationValue(annotations, controller.AnnotationPowerStateDesired), legacyObserved),
		DesiredGeneration:  parseInt64Annotation(annotations, controller.AnnotationPowerStateDesiredGeneration),
		Observed:           normalizeSandboxPowerState(annotationValue(annotations, controller.AnnotationPowerStateObserved), legacyObserved),
		ObservedGeneration: parseInt64Annotation(annotations, controller.AnnotationPowerStateObservedGeneration),
	}
	state.Phase = normalizeSandboxPowerPhase(annotationValue(annotations, controller.AnnotationPowerStatePhase), state.Desired, state.Observed)
	return state
}

func normalizeSandboxPowerState(raw, fallback string) string {
	switch raw {
	case SandboxPowerStateActive, SandboxPowerStatePaused:
		return raw
	default:
		return fallback
	}
}

func normalizeSandboxPowerPhase(raw, desired, observed string) string {
	switch raw {
	case SandboxPowerPhaseStable, SandboxPowerPhasePausing, SandboxPowerPhaseResuming:
		return raw
	}
	if desired == observed {
		return SandboxPowerPhaseStable
	}
	if desired == SandboxPowerStatePaused {
		return SandboxPowerPhasePausing
	}
	if desired == SandboxPowerStateActive {
		return SandboxPowerPhaseResuming
	}
	return SandboxPowerPhaseStable
}

func parseInt64Annotation(annotations map[string]string, key string) int64 {
	raw := strings.TrimSpace(annotationValue(annotations, key))
	if raw == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

func annotationValue(annotations map[string]string, key string) string {
	if len(annotations) == 0 {
		return ""
	}
	return annotations[key]
}

func hasExplicitSandboxPowerStateAnnotations(annotations map[string]string) bool {
	if len(annotations) == 0 {
		return false
	}
	return annotations[controller.AnnotationPowerStateDesired] != "" ||
		annotations[controller.AnnotationPowerStateDesiredGeneration] != "" ||
		annotations[controller.AnnotationPowerStateObserved] != "" ||
		annotations[controller.AnnotationPowerStateObservedGeneration] != "" ||
		annotations[controller.AnnotationPowerStatePhase] != ""
}

func nextSandboxPowerStateGeneration(current SandboxPowerState) int64 {
	base := current.DesiredGeneration
	if current.ObservedGeneration > base {
		base = current.ObservedGeneration
	}
	return base + 1
}

func requestedSandboxPowerState(annotations map[string]string, target string) SandboxPowerState {
	current := sandboxPowerStateFromAnnotations(annotations)
	generation := current.DesiredGeneration
	if generation == 0 || current.Desired != target {
		generation = nextSandboxPowerStateGeneration(current)
	}
	state := SandboxPowerState{
		Desired:            target,
		DesiredGeneration:  generation,
		Observed:           current.Observed,
		ObservedGeneration: current.ObservedGeneration,
	}
	if state.Observed == target {
		state.ObservedGeneration = generation
		state.Phase = SandboxPowerPhaseStable
		return state
	}
	state.Phase = normalizeSandboxPowerPhase("", target, state.Observed)
	return state
}

func completedSandboxPowerState(annotations map[string]string, target string) SandboxPowerState {
	current := sandboxPowerStateFromAnnotations(annotations)
	generation := current.DesiredGeneration
	if generation == 0 || current.Desired != target {
		generation = nextSandboxPowerStateGeneration(current)
	}
	return SandboxPowerState{
		Desired:            target,
		DesiredGeneration:  generation,
		Observed:           target,
		ObservedGeneration: generation,
		Phase:              SandboxPowerPhaseStable,
	}
}

func currentSandboxPowerExpectation(annotations map[string]string, target string) expectedSandboxPowerState {
	state := sandboxPowerStateFromAnnotations(annotations)
	return expectedSandboxPowerState{Desired: target, Generation: state.DesiredGeneration}
}

func staleSandboxPowerStateError(current SandboxPowerState) error {
	return fmt.Errorf("%w: desired=%s generation=%d", errSandboxPowerStateStale, current.Desired, current.DesiredGeneration)
}

func (s *SandboxService) matchSandboxPowerExpectation(pod *corev1.Pod, expected expectedSandboxPowerState) (SandboxPowerState, error) {
	if pod == nil {
		return SandboxPowerState{}, fmt.Errorf("pod is nil")
	}
	current := sandboxPowerStateFromAnnotations(pod.Annotations)
	if expected.Generation > 0 && (current.Desired != expected.Desired || current.DesiredGeneration != expected.Generation) {
		return current, staleSandboxPowerStateError(current)
	}
	return current, nil
}

func applySandboxPowerStateAnnotations(annotations map[string]string, state SandboxPowerState) {
	if annotations == nil {
		return
	}
	annotations[controller.AnnotationPowerStateDesired] = state.Desired
	annotations[controller.AnnotationPowerStateDesiredGeneration] = strconv.FormatInt(state.DesiredGeneration, 10)
	annotations[controller.AnnotationPowerStateObserved] = state.Observed
	annotations[controller.AnnotationPowerStateObservedGeneration] = strconv.FormatInt(state.ObservedGeneration, 10)
	annotations[controller.AnnotationPowerStatePhase] = state.Phase
}

func sandboxPowerStateEqual(a, b SandboxPowerState) bool {
	return a.Desired == b.Desired &&
		a.DesiredGeneration == b.DesiredGeneration &&
		a.Observed == b.Observed &&
		a.ObservedGeneration == b.ObservedGeneration &&
		a.Phase == b.Phase
}

func (s *SandboxService) sandboxPowerStateLock(sandboxID string) *sync.Mutex {
	if existing, ok := s.powerStateLocks.Load(sandboxID); ok {
		return existing.(*sync.Mutex)
	}
	mu := &sync.Mutex{}
	actual, _ := s.powerStateLocks.LoadOrStore(sandboxID, mu)
	return actual.(*sync.Mutex)
}

func (s *SandboxService) updateSandboxPowerStateAnnotations(ctx context.Context, pod *corev1.Pod, state SandboxPowerState) (*corev1.Pod, error) {
	if pod == nil {
		return nil, fmt.Errorf("pod is nil")
	}
	updated := pod.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	applySandboxPowerStateAnnotations(updated.Annotations, state)
	result, err := s.k8sClient.CoreV1().Pods(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *SandboxService) requestSandboxPowerState(ctx context.Context, sandboxID, target string) (SandboxPowerState, error) {
	lock := s.sandboxPowerStateLock(sandboxID)
	lock.Lock()
	defer lock.Unlock()

	var state SandboxPowerState
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := s.getSandboxPodForPowerState(ctx, sandboxID)
		if err != nil {
			return err
		}

		state = requestedSandboxPowerState(pod.Annotations, target)
		current := sandboxPowerStateFromAnnotations(pod.Annotations)
		if hasExplicitSandboxPowerStateAnnotations(pod.Annotations) && sandboxPowerStateEqual(current, state) {
			return nil
		}
		_, err = s.updateSandboxPowerStateAnnotations(ctx, pod, state)
		return err
	})
	if err != nil {
		return SandboxPowerState{}, fmt.Errorf("update power state annotations: %w", err)
	}

	if state.Phase != SandboxPowerPhaseStable {
		s.triggerSandboxPowerStateReconcile(sandboxID)
	}

	return state, nil
}

func (s *SandboxService) waitForSandboxPowerState(ctx context.Context, sandboxID, target string, generation int64) (SandboxPowerState, error) {
	var state SandboxPowerState
	err := wait.PollUntilContextCancel(ctx, defaultSandboxPowerPollInterval, true, func(ctx context.Context) (bool, error) {
		pod, err := s.getSandboxPodForPowerState(ctx, sandboxID)
		if err != nil {
			return false, err
		}
		state = sandboxPowerStateFromAnnotations(pod.Annotations)
		if generation > 0 && (state.Desired != target || state.DesiredGeneration != generation) {
			return false, fmt.Errorf("%w: %w", ErrSandboxPowerTransitionSuperseded, staleSandboxPowerStateError(state))
		}
		return state.Desired == target && state.Observed == target && state.Phase == SandboxPowerPhaseStable, nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return state, ctx.Err()
		}
		return state, err
	}
	return state, nil
}

func sandboxPowerTransitionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, defaultSandboxPowerTransitionTimeout)
}

func (s *SandboxService) triggerSandboxPowerStateReconcile(sandboxID string) {
	if _, loaded := s.powerStateReconcilers.LoadOrStore(sandboxID, struct{}{}); loaded {
		return
	}
	go func() {
		defer s.finishSandboxPowerStateReconcile(sandboxID)
		s.reconcileSandboxPowerState(sandboxID)
	}()
}

// StartPowerStateReconciler periodically reconciles power transitions left pending by another manager replica.
func (s *SandboxService) StartPowerStateReconciler(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	s.reconcilePendingSandboxPowerStates(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcilePendingSandboxPowerStates(ctx)
		}
	}
}

func (s *SandboxService) reconcilePendingSandboxPowerStates(ctx context.Context) {
	if ctx.Err() != nil || s.podLister == nil {
		return
	}
	pods, err := s.podLister.List(labels.Everything())
	if err != nil {
		s.logger.Warn("Failed to list sandboxes for power state reconcile", zap.Error(err))
		return
	}
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		sandboxID := strings.TrimSpace(pod.Labels[controller.LabelSandboxID])
		if sandboxID == "" {
			continue
		}
		state := sandboxPowerStateFromAnnotations(pod.Annotations)
		if state.Phase != SandboxPowerPhaseStable || state.Desired != state.Observed {
			s.triggerSandboxPowerStateReconcile(sandboxID)
		}
	}
}

func (s *SandboxService) finishSandboxPowerStateReconcile(sandboxID string) {
	s.powerStateReconcilers.Delete(sandboxID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pod, err := s.getSandboxPodForPowerState(ctx, sandboxID)
	if err != nil {
		return
	}
	state := sandboxPowerStateFromAnnotations(pod.Annotations)
	if state.Phase != SandboxPowerPhaseStable || state.Desired != state.Observed {
		s.triggerSandboxPowerStateReconcile(sandboxID)
	}
}

func (s *SandboxService) reconcileSandboxPowerState(sandboxID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for {
		pod, err := s.getSandboxPodForPowerState(ctx, sandboxID)
		if err != nil {
			s.logger.Warn("Power state reconcile failed to load sandbox",
				zap.String("sandboxID", sandboxID),
				zap.Error(err),
			)
			return
		}

		state := sandboxPowerStateFromAnnotations(pod.Annotations)
		if state.Phase == SandboxPowerPhaseStable && state.Desired == state.Observed {
			return
		}

		s.logger.Info("Reconciling sandbox power state",
			zap.String("sandboxID", sandboxID),
			zap.String("desired", state.Desired),
			zap.Int64("desiredGeneration", state.DesiredGeneration),
			zap.String("observed", state.Observed),
			zap.Int64("observedGeneration", state.ObservedGeneration),
			zap.String("phase", state.Phase),
		)

		switch state.Desired {
		case SandboxPowerStatePaused:
			if _, err := s.PauseSandbox(ctx, sandboxID); err != nil {
				s.logger.Error("Pause reconcile failed",
					zap.String("sandboxID", sandboxID),
					zap.Error(err),
				)
				return
			}
		case SandboxPowerStateActive:
			if _, err := s.ResumeSandbox(ctx, sandboxID); err != nil {
				s.logger.Error("Resume reconcile failed",
					zap.String("sandboxID", sandboxID),
					zap.Error(err),
				)
				return
			}
		default:
			s.logger.Warn("Skipping power state reconcile with unsupported desired state",
				zap.String("sandboxID", sandboxID),
				zap.String("desired", state.Desired),
			)
			return
		}

		nextPod, err := s.getSandboxPodForPowerState(ctx, sandboxID)
		if err != nil {
			return
		}
		nextState := sandboxPowerStateFromAnnotations(nextPod.Annotations)
		if nextState.Phase == SandboxPowerPhaseStable && nextState.Desired == nextState.Observed {
			return
		}
		if nextState.Desired == state.Desired && nextState.DesiredGeneration == state.DesiredGeneration {
			return
		}
	}
}

func (s *SandboxService) getSandboxPodForPowerState(ctx context.Context, sandboxID string) (*corev1.Pod, error) {
	if s.k8sClient == nil {
		return s.getSandboxPod(ctx, sandboxID)
	}
	if s.sandboxIndex != nil {
		if namespace, ok := s.sandboxIndex.GetNamespace(sandboxID); ok {
			return s.k8sClient.CoreV1().Pods(namespace).Get(ctx, sandboxID, metav1.GetOptions{})
		}
	}
	selector := labels.SelectorFromSet(map[string]string{controller.LabelSandboxID: sandboxID}).String()
	pods, err := s.k8sClient.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "pod"}, sandboxID)
	}
	return pods.Items[0].DeepCopy(), nil
}

func (s *SandboxService) prodAddress(ctx context.Context, pod *corev1.Pod) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("pod is nil")
	}
	if podIP := strings.TrimSpace(pod.Status.PodIP); podIP != "" {
		return fmt.Sprintf("http://%s:%d", podIP, s.config.ProcdPort), nil
	}

	podIP, err := s.waitForPodIP(ctx, pod.Namespace, pod.Name)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("http://%s:%d", podIP, s.config.ProcdPort), nil
}

func (s *SandboxService) waitForPodIP(ctx context.Context, namespace, name string) (string, error) {
	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()

	for {
		pod, err := s.podLister.Pods(namespace).Get(name)
		if err != nil {
			return "", fmt.Errorf("get pod for ip: %w", err)
		}
		if podIP := strings.TrimSpace(pod.Status.PodIP); podIP != "" {
			return podIP, nil
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("pod ip not assigned")
		case <-ticker.C:
		}
	}
}

func (s *SandboxService) waitForPodReady(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	timeout := s.config.ProcdInitTimeout
	if timeout < defaultPodReadyTimeout {
		timeout = defaultPodReadyTimeout
	}

	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		pod, err := s.podLister.Pods(namespace).Get(name)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				select {
				case <-readyCtx.Done():
					return nil, fmt.Errorf("pod %s/%s not visible after %s", namespace, name, timeout)
				case <-ticker.C:
					continue
				}
			}
			return nil, fmt.Errorf("get pod for readiness: %w", err)
		}
		pod, err = s.refreshSandboxProbeConditions(readyCtx, pod)
		if err != nil {
			return nil, fmt.Errorf("ensure pod probe conditions: %w", err)
		}
		if controller.IsPodReady(pod) {
			return pod, nil
		}

		select {
		case <-readyCtx.Done():
			return nil, fmt.Errorf("pod %s/%s not ready after %s", namespace, name, timeout)
		case <-ticker.C:
		}
	}
}

func (s *SandboxService) waitForPodClaimReady(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	timeout := s.config.ProcdInitTimeout
	if timeout < defaultPodReadyTimeout {
		timeout = defaultPodReadyTimeout
	}

	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	lastReason := "pod is not ready"
	for {
		pod, err := s.podLister.Pods(namespace).Get(name)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				lastReason = fmt.Sprintf("pod %s/%s is not visible", namespace, name)
				select {
				case <-readyCtx.Done():
					return nil, fmt.Errorf("pod %s/%s not claim-ready after %s: %s", namespace, name, timeout, lastReason)
				case <-ticker.C:
					continue
				}
			}
			return nil, fmt.Errorf("get pod for claim readiness: %w", err)
		}

		ready, reason := s.isPodClaimReady(readyCtx, pod)
		if ready {
			return pod, nil
		}
		if reason != "" {
			lastReason = reason
		}

		select {
		case <-readyCtx.Done():
			return nil, fmt.Errorf("pod %s/%s not claim-ready after %s: %s", namespace, name, timeout, lastReason)
		case <-ticker.C:
		}
	}
}

func (s *SandboxService) isPodClaimReady(ctx context.Context, pod *corev1.Pod) (bool, string) {
	if pod == nil {
		return false, "pod is nil"
	}
	if pod.Status.Phase != corev1.PodRunning {
		return false, fmt.Sprintf("pod phase is %s", pod.Status.Phase)
	}
	if strings.TrimSpace(pod.Status.PodIP) == "" {
		return false, "pod IP is not assigned"
	}
	if !podContainerRunning(pod, "procd") {
		return false, "procd container is not running"
	}
	if !controller.HasSandboxPodReadinessGate(pod) {
		return true, ""
	}

	result, err := s.ProbeSandboxPod(ctx, pod, sandboxprobe.KindReadiness)
	if err != nil {
		return false, err.Error()
	}
	if result == nil {
		return false, "sandbox readiness probe returned no result"
	}
	if result.Status != sandboxprobe.StatusPassed {
		message := strings.TrimSpace(result.Message)
		if message != "" {
			return false, message
		}
		if result.Reason != "" {
			return false, result.Reason
		}
		return false, fmt.Sprintf("sandbox readiness probe is %s", result.Status)
	}
	return true, ""
}

func podContainerRunning(pod *corev1.Pod, name string) bool {
	if pod == nil {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == name && status.State.Running != nil {
			return true
		}
	}
	return false
}

func (s *SandboxService) refreshSandboxProbeConditionsAsync(pod *corev1.Pod) {
	if s == nil || pod == nil || !controller.HasSandboxPodReadinessGate(pod) {
		return
	}
	go func(snapshot *corev1.Pod) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := s.refreshSandboxProbeConditions(ctx, snapshot); err != nil && s.logger != nil {
			s.logger.Warn("Failed to refresh sandbox probe conditions asynchronously",
				zap.String("pod", snapshot.Name),
				zap.String("namespace", snapshot.Namespace),
				zap.Error(err),
			)
		}
	}(pod.DeepCopy())
}

func (s *SandboxService) refreshSandboxProbeConditions(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	if !controller.HasSandboxPodReadinessGate(pod) {
		return pod, nil
	}
	startup := s.probeSandboxPodOrFailure(ctx, pod, sandboxprobe.KindStartup)
	readiness := s.probeSandboxPodOrFailure(ctx, pod, sandboxprobe.KindReadiness)
	liveness := s.probeSandboxPodOrFailure(ctx, pod, sandboxprobe.KindLiveness)
	return controller.EnsureSandboxPodProbeConditions(ctx, s.k8sClient, pod, startup, readiness, liveness)
}

func (s *SandboxService) ProbeSandboxPod(ctx context.Context, pod *corev1.Pod, kind sandboxprobe.Kind) (*sandboxprobe.Response, error) {
	if pod == nil {
		return nil, fmt.Errorf("pod is nil")
	}
	if pod.Status.Phase != corev1.PodRunning {
		result := sandboxprobe.Failed(kind, "PodNotRunning", fmt.Sprintf("pod phase is %s", pod.Status.Phase), nil)
		return &result, nil
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return nil, err
	}
	result, err := s.ctldClient.ProbePod(ctx, ctldAddress, pod.Namespace, pod.Name, kind)
	if result != nil && result.Status != "" {
		return result, nil
	}
	return result, err
}

func (s *SandboxService) probeSandboxPodOrFailure(ctx context.Context, pod *corev1.Pod, kind sandboxprobe.Kind) *sandboxprobe.Response {
	result, err := s.ProbeSandboxPod(ctx, pod, kind)
	if err != nil {
		failure := sandboxprobe.Failed(kind, "SandboxProbeFailed", err.Error(), nil)
		return &failure
	}
	if result == nil {
		failure := sandboxprobe.Failed(kind, "SandboxProbeMissing", "sandbox probe returned no result", nil)
		return &failure
	}
	return result
}

// podPhaseToSandboxStatus converts pod phase to sandbox status
func (s *SandboxService) podPhaseToSandboxStatus(phase corev1.PodPhase) string {
	switch phase {
	case corev1.PodPending:
		return SandboxStatusStarting
	case corev1.PodRunning:
		return SandboxStatusRunning
	case corev1.PodSucceeded:
		return SandboxStatusCompleted
	case corev1.PodFailed:
		return SandboxStatusFailed
	default:
		return SandboxStatusPending
	}
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

// PauseSandboxResponse represents the response from pausing a sandbox.
type PauseSandboxResponse struct {
	SandboxID     string                `json:"sandbox_id"`
	Paused        bool                  `json:"paused"`
	PowerState    SandboxPowerState     `json:"power_state"`
	ResourceUsage *SandboxResourceUsage `json:"resource_usage,omitempty"`
	UpdatedMemory string                `json:"updated_memory,omitempty"`
	UpdatedCPU    string                `json:"updated_cpu,omitempty"`
}

// ResumeSandboxResponse represents the response from resuming a sandbox.
type ResumeSandboxResponse struct {
	SandboxID      string            `json:"sandbox_id"`
	Resumed        bool              `json:"resumed"`
	PowerState     SandboxPowerState `json:"power_state"`
	RestoredMemory string            `json:"restored_memory,omitempty"`
}

// PausedState stores the sandbox state before pause for restoration on resume.
type PausedState struct {
	// Resources stores original pod resources before pause.
	Resources map[string]ContainerResources `json:"resources"`
	// OriginalTTL stores the original TTL (in seconds) set by user or default.
	// On resume, this TTL is reused to reset the countdown.
	OriginalTTL *int32 `json:"original_ttl,omitempty"`
}

// ContainerResources stores resource requests/limits for a container.
type ContainerResources struct {
	Requests corev1.ResourceList `json:"requests,omitempty"`
	Limits   corev1.ResourceList `json:"limits,omitempty"`
}

type resumeSandboxPreparation struct {
	Pod            *corev1.Pod
	PowerState     SandboxPowerState
	RestoredMemory string
	HadPausedState bool
}

type expectedSandboxPowerState struct {
	Desired    string
	Generation int64
}

// PauseSandbox delegates sandbox pause execution to the configured power executor.
func (s *SandboxService) PauseSandbox(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	return s.sandboxPowerExecutor().Pause(ctx, sandboxID)
}

// pauseSandboxLocal pauses a sandbox and reduces pod resources based on actual usage.
// This uses Kubernetes 1.35+ in-place pod update feature.
func (s *SandboxService) pauseSandboxLocal(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	s.logger.Info("Pausing sandbox", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	// Check if already paused
	if pod.Annotations[controller.AnnotationPaused] == "true" {
		return &PauseSandboxResponse{
			SandboxID:  sandboxID,
			Paused:     true,
			PowerState: sandboxPowerStateFromAnnotations(pod.Annotations),
		}, nil
	}
	expected := currentSandboxPowerExpectation(pod.Annotations, SandboxPowerStatePaused)

	// Generate internal token for procd authentication
	if s.internalTokenGenerator == nil || s.procdTokenGenerator == nil {
		return nil, fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	procdToken, err := s.procdTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate procd token: %w", err)
	}

	// Call procd pause API
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	pauseResp, err := s.procdClient.Pause(ctx, procdAddress, internalToken, procdToken)
	if err != nil {
		return nil, fmt.Errorf("call procd pause: %w", err)
	}

	if !pauseResp.Paused {
		return nil, fmt.Errorf("procd pause failed: %s", pauseResp.Error)
	}

	completedResp, err := s.completePausedSandbox(ctx, pod, sandboxID, pauseResp.ResourceUsage, expected)
	if err != nil && errors.Is(err, errSandboxPowerStateStale) && completedResp != nil && completedResp.PowerState.Desired == SandboxPowerStateActive {
		resumeResp, resumeErr := s.procdClient.Resume(ctx, procdAddress, internalToken, procdToken)
		if resumeErr != nil {
			return completedResp, fmt.Errorf("resume procd after stale pause: %w", resumeErr)
		}
		if !resumeResp.Resumed {
			return completedResp, fmt.Errorf("procd resume after stale pause failed: %s", resumeResp.Error)
		}
		return &PauseSandboxResponse{SandboxID: sandboxID, Paused: false, PowerState: completedResp.PowerState, ResourceUsage: pauseResp.ResourceUsage}, nil
	}
	return completedResp, err
}

// RequestPauseSandbox records a desired paused state and reconciles it asynchronously.
func (s *SandboxService) RequestPauseSandbox(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	state, err := s.requestSandboxPowerState(ctx, sandboxID, SandboxPowerStatePaused)
	if err != nil {
		return nil, err
	}
	return &PauseSandboxResponse{
		SandboxID:  sandboxID,
		Paused:     true,
		PowerState: state,
	}, nil
}

// PauseSandboxAndWait records a desired paused state and waits until the sandbox observes it.
func (s *SandboxService) PauseSandboxAndWait(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	resp, err := s.RequestPauseSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if resp.PowerState.Desired == SandboxPowerStatePaused && resp.PowerState.Observed == SandboxPowerStatePaused && resp.PowerState.Phase == SandboxPowerPhaseStable {
		return resp, nil
	}
	waitCtx, cancel := sandboxPowerTransitionContext(ctx)
	defer cancel()
	state, err := s.waitForSandboxPowerState(waitCtx, sandboxID, SandboxPowerStatePaused, resp.PowerState.DesiredGeneration)
	if err != nil {
		return &PauseSandboxResponse{SandboxID: sandboxID, Paused: state.Observed == SandboxPowerStatePaused, PowerState: state}, err
	}
	resp.PowerState = state
	resp.Paused = true
	return resp, nil
}

// ResumeSandbox delegates sandbox resume execution to the configured power executor.
func (s *SandboxService) ResumeSandbox(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	return s.sandboxPowerExecutor().Resume(ctx, sandboxID)
}

// resumeSandboxLocal resumes a paused sandbox and restores original pod resources.
func (s *SandboxService) resumeSandboxLocal(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	s.logger.Info("Resuming sandbox", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pods: %w", err)
	}
	expected := currentSandboxPowerExpectation(pod.Annotations, SandboxPowerStateActive)

	prep, resp, err := s.prepareSandboxResume(ctx, pod, sandboxID, expected)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		return resp, nil
	}
	pod = prep.Pod

	// Generate internal token for procd authentication
	if s.internalTokenGenerator == nil || s.procdTokenGenerator == nil {
		return nil, fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	procdToken, err := s.procdTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate procd token: %w", err)
	}

	// Call procd resume API
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	resumeResp, err := s.procdClient.Resume(ctx, procdAddress, internalToken, procdToken)
	if err != nil {
		return nil, fmt.Errorf("call procd resume: %w", err)
	}

	if !resumeResp.Resumed {
		return nil, fmt.Errorf("procd resume failed: %s", resumeResp.Error)
	}

	powerState, err := s.completeSandboxResume(ctx, sandboxID, expected)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Sandbox resumed successfully",
		zap.String("sandboxID", sandboxID),
		zap.String("restoredMemory", prep.RestoredMemory),
	)

	return &ResumeSandboxResponse{
		SandboxID:      sandboxID,
		Resumed:        true,
		PowerState:     powerState,
		RestoredMemory: prep.RestoredMemory,
	}, nil
}

func (s *SandboxService) completePausedSandbox(ctx context.Context, pod *corev1.Pod, sandboxID string, usage *SandboxResourceUsage, expected expectedSandboxPowerState) (*PauseSandboxResponse, error) {
	if pod.Annotations[controller.AnnotationPaused] == "true" {
		return &PauseSandboxResponse{
			SandboxID:  sandboxID,
			Paused:     true,
			PowerState: sandboxPowerStateFromAnnotations(pod.Annotations),
		}, nil
	}

	pausedState := PausedState{Resources: s.extractOriginalResources(pod)}
	if configJSON := pod.Annotations[controller.AnnotationConfig]; configJSON != "" {
		var config SandboxConfig
		if err := json.Unmarshal([]byte(configJSON), &config); err == nil && config.TTL != nil {
			pausedState.OriginalTTL = config.TTL
		}
	}
	pausedStateJSON, err := json.Marshal(pausedState)
	if err != nil {
		return nil, fmt.Errorf("marshal paused state: %w", err)
	}

	var newRequestMemory resource.Quantity
	var newLimitMemory resource.Quantity
	if usage != nil && usage.ContainerMemoryWorkingSet > 0 {
		workingSet := usage.ContainerMemoryWorkingSet
		reqBytes := int64(workingSet)
		minReq, err := resource.ParseQuantity(s.config.PauseMinMemoryRequest)
		if err == nil && reqBytes < minReq.Value() {
			reqBytes = minReq.Value()
		}
		newRequestMemory = *resource.NewQuantity(reqBytes, resource.BinarySI)

		limitBytes := int64(float64(workingSet) * s.config.PauseMemoryBufferRatio)
		minLimit, err := resource.ParseQuantity(s.config.PauseMinMemoryLimit)
		if err == nil && limitBytes < minLimit.Value() {
			limitBytes = minLimit.Value()
		}
		newLimitMemory = *resource.NewQuantity(limitBytes, resource.BinarySI)
	}

	minCPU := resource.MustParse(s.config.PauseMinCPU)
	var powerState SandboxPowerState
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		currentPod, getErr := s.getSandboxPodForPowerState(ctx, sandboxID)
		if getErr != nil {
			return getErr
		}
		if currentPod.Annotations[controller.AnnotationPaused] == "true" {
			powerState = sandboxPowerStateFromAnnotations(currentPod.Annotations)
			pod = currentPod
			return nil
		}
		currentState, matchErr := s.matchSandboxPowerExpectation(currentPod, expected)
		if matchErr != nil {
			powerState = currentState
			return matchErr
		}
		annotatedPod := currentPod.DeepCopy()
		if annotatedPod.Annotations == nil {
			annotatedPod.Annotations = make(map[string]string)
		}
		annotatedPod.Annotations[controller.AnnotationPaused] = "true"
		annotatedPod.Annotations[controller.AnnotationPausedAt] = s.clock.Now().Format(time.RFC3339)
		annotatedPod.Annotations[controller.AnnotationPausedState] = string(pausedStateJSON)
		powerState = completedSandboxPowerState(annotatedPod.Annotations, SandboxPowerStatePaused)
		applySandboxPowerStateAnnotations(annotatedPod.Annotations, powerState)
		delete(annotatedPod.Annotations, controller.AnnotationExpiresAt)

		updatedPod, updateErr := s.k8sClient.CoreV1().Pods(currentPod.Namespace).Update(ctx, annotatedPod, metav1.UpdateOptions{})
		if updateErr != nil {
			return updateErr
		}
		pod = updatedPod
		return nil
	})
	if err != nil {
		if errors.Is(err, errSandboxPowerStateStale) {
			return &PauseSandboxResponse{
				SandboxID:     sandboxID,
				Paused:        powerState.Desired == SandboxPowerStatePaused,
				PowerState:    powerState,
				ResourceUsage: usage,
			}, err
		}
		return nil, fmt.Errorf("update pod annotations after pause: %w", err)
	}

	if !newLimitMemory.IsZero() || !minCPU.IsZero() {
		resizePod := pod.DeepCopy()
		found := s.applyPausedResourceTargets(resizePod, newRequestMemory, newLimitMemory, minCPU)
		if !found {
			s.logger.Warn("Main container 'procd' not found during pause resource update", zap.String("sandboxID", sandboxID))
		} else if _, err = s.k8sClient.CoreV1().Pods(pod.Namespace).UpdateResize(ctx, pod.Name, resizePod, metav1.UpdateOptions{}); err != nil {
			s.logger.Error("Failed to update pod resources after pause",
				zap.String("sandboxID", sandboxID),
				zap.Error(err),
			)
		}
	}

	workingSet := int64(0)
	if usage != nil {
		workingSet = usage.ContainerMemoryWorkingSet
	}
	s.logger.Info("Sandbox paused successfully",
		zap.String("sandboxID", sandboxID),
		zap.String("newRequest", newRequestMemory.String()),
		zap.String("newLimit", newLimitMemory.String()),
		zap.Int64("workingSet", workingSet),
	)

	return &PauseSandboxResponse{
		SandboxID:     sandboxID,
		Paused:        true,
		PowerState:    powerState,
		ResourceUsage: usage,
		UpdatedMemory: newLimitMemory.String(),
		UpdatedCPU:    minCPU.String(),
	}, nil
}

func (s *SandboxService) applyPausedResourceTargets(resizePod *corev1.Pod, newRequestMemory, newLimitMemory, minCPU resource.Quantity) bool {
	if resizePod == nil {
		return false
	}
	for i := range resizePod.Spec.Containers {
		container := &resizePod.Spec.Containers[i]
		if container.Name != "procd" {
			continue
		}
		if container.Resources.Requests == nil {
			container.Resources.Requests = make(corev1.ResourceList)
		}
		if !newRequestMemory.IsZero() {
			container.Resources.Requests[corev1.ResourceMemory] = newRequestMemory
		}
		container.Resources.Requests[corev1.ResourceCPU] = minCPU
		if container.Resources.Limits == nil {
			container.Resources.Limits = make(corev1.ResourceList)
		}
		if !newLimitMemory.IsZero() {
			container.Resources.Limits[corev1.ResourceMemory] = newLimitMemory
		}
		container.Resources.Limits[corev1.ResourceCPU] = minCPU
		return true
	}
	return false
}

func (s *SandboxService) prepareSandboxResume(ctx context.Context, pod *corev1.Pod, sandboxID string, expected expectedSandboxPowerState) (*resumeSandboxPreparation, *ResumeSandboxResponse, error) {
	if pod.Annotations[controller.AnnotationPaused] != "true" {
		return nil, &ResumeSandboxResponse{
			SandboxID:  sandboxID,
			Resumed:    true,
			PowerState: sandboxPowerStateFromAnnotations(pod.Annotations),
		}, nil
	}

	var restoredMemory string
	var powerState SandboxPowerState
	var hadPausedState bool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		currentPod, getErr := s.getSandboxPodForPowerState(ctx, sandboxID)
		if getErr != nil {
			return getErr
		}
		if currentPod.Annotations[controller.AnnotationPaused] != "true" {
			powerState = sandboxPowerStateFromAnnotations(currentPod.Annotations)
			pod = currentPod
			return nil
		}
		currentState, matchErr := s.matchSandboxPowerExpectation(currentPod, expected)
		if matchErr != nil {
			powerState = currentState
			return matchErr
		}

		powerState = requestedSandboxPowerState(currentPod.Annotations, SandboxPowerStateActive)
		pod = currentPod

		pausedStateJSON := currentPod.Annotations[controller.AnnotationPausedState]
		if pausedStateJSON == "" {
			hadPausedState = false
			return nil
		}
		var pausedState PausedState
		if err := json.Unmarshal([]byte(pausedStateJSON), &pausedState); err != nil {
			hadPausedState = false
			return nil
		}
		hadPausedState = true
		resizePod := currentPod.DeepCopy()
		for i := range resizePod.Spec.Containers {
			container := &resizePod.Spec.Containers[i]
			if orig, ok := pausedState.Resources[container.Name]; ok {
				container.Resources.Requests = orig.Requests
				container.Resources.Limits = orig.Limits
				if memReq, ok := orig.Requests[corev1.ResourceMemory]; ok {
					restoredMemory = memReq.String()
				}
			}
		}
		_, updateErr := s.k8sClient.CoreV1().Pods(currentPod.Namespace).UpdateResize(ctx, currentPod.Name, resizePod, metav1.UpdateOptions{})
		return updateErr
	})
	if err != nil {
		if errors.Is(err, errSandboxPowerStateStale) {
			return nil, &ResumeSandboxResponse{
				SandboxID:  sandboxID,
				Resumed:    powerState.Desired == SandboxPowerStateActive,
				PowerState: powerState,
			}, err
		}
		return nil, nil, fmt.Errorf("restore pod resources before resume: %w", err)
	}

	if pod.Annotations[controller.AnnotationPaused] != "true" {
		return nil, &ResumeSandboxResponse{
			SandboxID:  sandboxID,
			Resumed:    true,
			PowerState: powerState,
		}, nil
	}

	return &resumeSandboxPreparation{Pod: pod, PowerState: powerState, RestoredMemory: restoredMemory, HadPausedState: hadPausedState}, nil, nil
}

func (s *SandboxService) completeSandboxResume(ctx context.Context, sandboxID string, expected expectedSandboxPowerState) (SandboxPowerState, error) {
	var powerState SandboxPowerState
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		currentPod, getErr := s.getSandboxPodForPowerState(ctx, sandboxID)
		if getErr != nil {
			return getErr
		}
		if currentPod.Annotations[controller.AnnotationPaused] != "true" {
			powerState = sandboxPowerStateFromAnnotations(currentPod.Annotations)
			return nil
		}
		currentState, matchErr := s.matchSandboxPowerExpectation(currentPod, expected)
		if matchErr != nil {
			powerState = currentState
			return matchErr
		}

		annotationPod := currentPod.DeepCopy()
		if annotationPod.Annotations == nil {
			annotationPod.Annotations = make(map[string]string)
		}
		var ttlToRestore *int32
		pausedStateJSON := currentPod.Annotations[controller.AnnotationPausedState]
		if pausedStateJSON != "" {
			var pausedState PausedState
			if err := json.Unmarshal([]byte(pausedStateJSON), &pausedState); err == nil {
				ttlToRestore = pausedState.OriginalTTL
			}
		}
		if ttlToRestore == nil && s.config.DefaultTTL > 0 {
			ttlToRestore = int32Ptr(int32(s.config.DefaultTTL.Seconds()))
		}
		setExpirationAnnotation(annotationPod.Annotations, s.clock.Now(), ttlToRestore)
		delete(annotationPod.Annotations, controller.AnnotationPaused)
		delete(annotationPod.Annotations, controller.AnnotationPausedAt)
		delete(annotationPod.Annotations, controller.AnnotationPausedState)
		powerState = completedSandboxPowerState(annotationPod.Annotations, SandboxPowerStateActive)
		applySandboxPowerStateAnnotations(annotationPod.Annotations, powerState)
		_, updateErr := s.k8sClient.CoreV1().Pods(currentPod.Namespace).Update(ctx, annotationPod, metav1.UpdateOptions{})
		return updateErr
	})
	if err != nil {
		return powerState, err
	}
	return powerState, nil
}

// RequestResumeSandbox records a desired active state and reconciles it asynchronously.
func (s *SandboxService) RequestResumeSandbox(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	state, err := s.requestSandboxPowerState(ctx, sandboxID, SandboxPowerStateActive)
	if err != nil {
		return nil, err
	}
	return &ResumeSandboxResponse{
		SandboxID:  sandboxID,
		Resumed:    true,
		PowerState: state,
	}, nil
}

// ResumeSandboxAndWait records a desired active state and waits until the sandbox observes it.
func (s *SandboxService) ResumeSandboxAndWait(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	resp, err := s.RequestResumeSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if resp.PowerState.Desired == SandboxPowerStateActive && resp.PowerState.Observed == SandboxPowerStateActive && resp.PowerState.Phase == SandboxPowerPhaseStable {
		return resp, nil
	}
	waitCtx, cancel := sandboxPowerTransitionContext(ctx)
	defer cancel()
	state, err := s.waitForSandboxPowerState(waitCtx, sandboxID, SandboxPowerStateActive, resp.PowerState.DesiredGeneration)
	if err != nil {
		return &ResumeSandboxResponse{SandboxID: sandboxID, Resumed: state.Observed == SandboxPowerStateActive, PowerState: state}, err
	}
	resp.PowerState = state
	resp.Resumed = true
	return resp, nil
}

// PauseSandboxByID implements the SandboxPauser interface from controller package.
// It wraps PauseSandbox and returns only the error.
func (s *SandboxService) PauseSandboxByID(ctx context.Context, sandboxID string) error {
	_, err := s.PauseSandbox(ctx, sandboxID)
	return err
}

// TerminateSandboxByID implements the SandboxTerminator interface from controller package.
// It wraps TerminateSandbox and returns only the error.
func (s *SandboxService) TerminateSandboxByID(ctx context.Context, sandboxID string) error {
	return s.TerminateSandbox(ctx, sandboxID)
}

// GetSandboxResourceUsage gets the resource usage of a sandbox.
func (s *SandboxService) GetSandboxResourceUsage(ctx context.Context, sandboxID string) (*SandboxResourceUsage, error) {
	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	// Generate internal token for procd authentication
	if s.internalTokenGenerator == nil || s.procdTokenGenerator == nil {
		return nil, fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	procdToken, err := s.procdTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate procd token: %w", err)
	}

	// Call procd stats API
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	statsResp, err := s.procdClient.Stats(ctx, procdAddress, internalToken, procdToken)
	if err != nil {
		return nil, fmt.Errorf("call procd stats: %w", err)
	}

	return &statsResp.SandboxResourceUsage, nil
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

// extractOriginalResources extracts current resources from pod containers.
func (s *SandboxService) extractOriginalResources(pod *corev1.Pod) map[string]ContainerResources {
	resources := make(map[string]ContainerResources)

	for _, container := range pod.Spec.Containers {
		resources[container.Name] = ContainerResources{
			Requests: container.Resources.Requests.DeepCopy(),
			Limits:   container.Resources.Limits.DeepCopy(),
		}
	}

	return resources
}

// ListSandboxesRequest represents a request to list sandboxes
type ListSandboxesRequest struct {
	TeamID     string
	Status     string
	TemplateID string
	Paused     *bool
	Limit      int
	Offset     int
}

// ListSandboxesResponse represents the response from listing sandboxes
type ListSandboxesResponse struct {
	Sandboxes []*SandboxSummary `json:"sandboxes"`
	Count     int               `json:"count"`
	HasMore   bool              `json:"has_more"`
}

// SandboxSummary represents a summary of a sandbox for listing
type SandboxSummary struct {
	ID            string            `json:"id"`
	TemplateID    string            `json:"template_id"`
	Status        string            `json:"status"`
	Paused        bool              `json:"paused"`
	PowerState    SandboxPowerState `json:"power_state"`
	CreatedAt     time.Time         `json:"created_at"`
	ExpiresAt     time.Time         `json:"expires_at"`
	HardExpiresAt time.Time         `json:"hard_expires_at"`
}

// ListSandboxes lists all sandboxes for a team with optional filters
func (s *SandboxService) ListSandboxes(ctx context.Context, req *ListSandboxesRequest) (*ListSandboxesResponse, error) {
	s.logger.Info("Listing sandboxes",
		zap.String("teamID", req.TeamID),
		zap.String("status", req.Status),
		zap.String("templateID", req.TemplateID),
	)

	// Set defaults
	if req.Limit <= 0 {
		req.Limit = 50
	}
	if req.Limit > 200 {
		req.Limit = 200
	}

	// List all active pods (exclude idle pool)
	pods, err := s.podLister.List(labels.SelectorFromSet(map[string]string{
		controller.LabelPoolType: controller.PoolTypeActive,
	}))
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	// Filter and convert pods to sandbox summaries
	var summaries []*SandboxSummary
	for _, pod := range pods {
		// Filter by team_id from annotations
		teamID := pod.Annotations[controller.AnnotationTeamID]
		if teamID != req.TeamID {
			continue
		}

		// Get status from pod phase
		status := s.podPhaseToSandboxStatus(pod.Status.Phase)

		// Filter by status if specified
		if req.Status != "" && status != req.Status {
			continue
		}

		// Filter by template_id if specified
		templateID := pod.Labels[controller.LabelTemplateID]
		if req.TemplateID != "" && templateID != req.TemplateID {
			continue
		}

		// Filter by paused state if specified
		powerState := sandboxPowerStateFromAnnotations(pod.Annotations)
		paused := powerState.Observed == SandboxPowerStatePaused
		if req.Paused != nil && paused != *req.Paused {
			continue
		}

		// Parse timestamps (both can be zero when disabled or not set).
		expiresAt := parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt)
		hardExpiresAt := parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt)

		summaries = append(summaries, &SandboxSummary{
			ID:            pod.Name,
			TemplateID:    templateID,
			Status:        status,
			Paused:        paused,
			PowerState:    powerState,
			CreatedAt:     pod.CreationTimestamp.Time,
			ExpiresAt:     expiresAt,
			HardExpiresAt: hardExpiresAt,
		})
	}

	// Sort by creation timestamp (descending - newest first)
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].CreatedAt.After(summaries[j].CreatedAt)
	})

	// Get total count before pagination
	totalCount := len(summaries)

	// Apply pagination
	hasMore := false
	if req.Offset >= totalCount {
		summaries = []*SandboxSummary{}
	} else {
		end := req.Offset + req.Limit
		if end >= totalCount {
			end = totalCount
		} else {
			hasMore = true
		}
		summaries = summaries[req.Offset:end]
	}

	s.logger.Info("Listed sandboxes",
		zap.String("teamID", req.TeamID),
		zap.Int("count", totalCount),
		zap.Int("returned", len(summaries)),
		zap.Bool("hasMore", hasMore),
	)

	return &ListSandboxesResponse{
		Sandboxes: summaries,
		Count:     totalCount,
		HasMore:   hasMore,
	}, nil
}
