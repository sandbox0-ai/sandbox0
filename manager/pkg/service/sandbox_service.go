package service

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	egressauth "github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
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
var ErrDataPlaneNotReady = errors.New("data plane not ready")
var errSandboxPowerStateStale = errors.New("sandbox power state changed during execution")

// ErrSandboxPowerTransitionSuperseded is returned when a newer pause/resume request replaces the requested transition.
var ErrSandboxPowerTransitionSuperseded = errors.New("sandbox power transition superseded")

const defaultPodReadyTimeout = 30 * time.Second
const defaultPodClaimReadyTimeout = 90 * time.Second
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
	CtldHTTPClient         *http.Client
	ProcdPort              int
	ProcdClientTimeout     time.Duration
	ProcdHTTPClient        *http.Client
	ProcdInitTimeout       time.Duration
}

// SandboxService handles sandbox operations
type SandboxService struct {
	k8sClient              kubernetes.Interface
	podLister              corelisters.PodLister
	nodeLister             corelisters.NodeLister
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
	nodeLister corelisters.NodeLister,
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
	ctldClient := NewCtldClient(CtldClientConfig{Timeout: config.CtldClientTimeout})
	if config.CtldHTTPClient != nil {
		ctldClient = NewCtldClientWithHTTPClient(config.CtldHTTPClient)
	}
	procdClient := NewProcdClient(ProcdClientConfig{Timeout: config.ProcdClientTimeout})
	if config.ProcdHTTPClient != nil {
		procdClient = NewProcdClientWithHTTPClient(config.ProcdHTTPClient)
	}

	service := &SandboxService{
		k8sClient:              k8sClient,
		podLister:              podLister,
		nodeLister:             nodeLister,
		sandboxIndex:           sandboxIndex,
		secretLister:           secretLister,
		templateLister:         templateLister,
		NetworkPolicyService:   networkPolicyService,
		networkProvider:        networkProvider,
		ctldClient:             ctldClient,
		procdClient:            procdClient,
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
