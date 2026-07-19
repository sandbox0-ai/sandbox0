package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/startlimiter"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	egressauth "github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// Sandbox represents a sandbox instance
type Sandbox struct {
	ID                string                 `json:"id"`
	TemplateID        string                 `json:"template_id"`
	TeamID            string                 `json:"team_id"`
	UserID            string                 `json:"user_id"`
	InternalAddr      string                 `json:"internal_addr"`
	Status            string                 `json:"status"`
	Paused            bool                   `json:"paused"`
	AutoResume        bool                   `json:"auto_resume"`
	Resources         *SandboxResourceConfig `json:"resources,omitempty"`
	Services          []SandboxAppService    `json:"services,omitempty"`
	Mounts            []ClaimMount           `json:"mounts,omitempty"`
	PodName           string                 `json:"pod_name"`
	RuntimeGeneration int64                  `json:"runtime_generation"`
	ExpiresAt         time.Time              `json:"expires_at"`
	HardExpiresAt     time.Time              `json:"hard_expires_at"`
	ClaimedAt         time.Time              `json:"claimed_at"`
	CreatedAt         time.Time              `json:"created_at"`
	UpdatedAt         time.Time              `json:"updated_at"`
}

// SandboxStatus represents possible sandbox statuses
const (
	SandboxStatusStarting    = "starting"
	SandboxStatusRunning     = "running"
	SandboxStatusPaused      = "paused"
	SandboxStatusFailed      = "failed"
	SandboxStatusTerminating = "terminating"
)

// errNoIdlePod is returned when no idle pod is available for claiming.
var errNoIdlePod = errors.New("no idle pod available")
var ErrInvalidClaimRequest = errors.New("invalid claim request")
var ErrClaimConflict = errors.New("claim conflict")
var ErrDataPlaneNotReady = errors.New("data plane not ready")
var ErrQuotaExceeded = errors.New("quota exceeded")
var ErrTeamQuotaUnavailable = errors.New("team quota unavailable")
var ErrInvalidNetworkPolicy = errors.New("invalid network policy")
var ErrSandboxCheckpointRequiresCtld = errors.New("sandbox checkpoint requires ctld")
var ErrClaimStartThrottled = startlimiter.ErrThrottled

const defaultPodClaimReadyTimeout = 90 * time.Second
const defaultSandboxRestoreTimeout = 5 * time.Minute

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
	DefaultTTL                          time.Duration
	SandboxMemoryPerCPU                 string
	SandboxMaxMemory                    string
	PauseMinMemoryRequest               string
	PauseMinMemoryLimit                 string
	PauseMemoryBufferRatio              float64
	PauseMinCPU                         string
	CtldEnabled                         bool
	CtldPort                            int
	CtldClientTimeout                   time.Duration
	CtldHTTPClient                      *http.Client
	CtldRootFSTokenProvider             ctldapi.RootFSTokenProvider
	ProcdPort                           int
	ProcdClientTimeout                  time.Duration
	ProcdHTTPClient                     *http.Client
	ProcdInitTimeout                    time.Duration
	AllowColdStartWithoutReadyDataPlane bool
	RootFSSquashDisabled                bool
	RootFSSquashMaxChainDepth           int
	RootFSSquashMaxChainBytes           int64
	PublicRootDomain                    string
	PublicRegionID                      string
}

// SandboxService handles sandbox operations
type SandboxService struct {
	k8sClient                              kubernetes.Interface
	podLister                              corelisters.PodLister
	nodeLister                             corelisters.NodeLister
	sandboxIndex                           *SandboxIndex
	secretLister                           corelisters.SecretLister
	templateLister                         controller.TemplateLister
	NetworkPolicyService                   *NetworkPolicyService
	networkProvider                        network.Provider
	procdClient                            *ProcdClient
	ctldClient                             *CtldClient
	internalTokenGenerator                 TokenGenerator
	clock                                  TimeProvider
	config                                 SandboxServiceConfig
	logger                                 *zap.Logger
	metrics                                *obsmetrics.ManagerMetrics
	pauseEnqueuer                          SandboxPauseEnqueuer
	credentialStore                        egressauth.BindingStore
	webhookStateVolumes                    SandboxSystemVolumeClient
	volumeMetadata                         SandboxVolumeMetadataClient
	deletionWebhookEmitter                 SandboxDeletionWebhookEmitter
	teamQuotaStore                         teamquota.CapacityStore
	teamQuotaRateLimiter                   TeamQuotaRateLimiter
	sandboxStore                           SandboxStore
	rootFSObjectDeleter                    RootFSObjectDeleter
	templateImageBuildCapabilityConfigured bool
	templateImageBuildAvailable            bool
	claimStartLimiter                      *startlimiter.Limiter
	resumeGroup                            singleflight.Group
	idlePodReservations                    *idlePodReservations
	podWaiterMu                            sync.Mutex
	podWaiter                              *podEventWaiter
}

// TeamQuotaRateLimiter is the distributed rate admission contract.
type TeamQuotaRateLimiter interface {
	Take(ctx context.Context, teamID string, key teamquota.Key, cost int64) (tokenbucket.Decision, error)
}

// SandboxPauseEnqueuer schedules durable pause transactions for background completion.
type SandboxPauseEnqueuer interface {
	EnqueueSandboxPause(sandboxID string)
}

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
		config.CtldClientTimeout = defaultCtldClientTimeout
	}
	if config.RootFSSquashMaxChainDepth <= 0 {
		config.RootFSSquashMaxChainDepth = 8
	}
	if config.RootFSSquashMaxChainBytes <= 0 {
		config.RootFSSquashMaxChainBytes = 512 * 1024 * 1024
	}
	if networkProvider == nil {
		networkProvider = network.NewNoopProvider()
	}
	ctldClient := NewCtldClient(CtldClientConfig{
		Timeout:             config.CtldClientTimeout,
		RootFSTokenProvider: config.CtldRootFSTokenProvider,
	})
	if config.CtldHTTPClient != nil {
		ctldClient = &CtldClient{
			httpClient:          config.CtldHTTPClient,
			api:                 ctldapi.NewClientWithRootFSAuth(config.CtldHTTPClient, config.CtldRootFSTokenProvider),
			rootFSTokenProvider: config.CtldRootFSTokenProvider,
		}
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
		clock:                  clock,
		config:                 config,
		logger:                 logger,
		metrics:                metrics,
		idlePodReservations:    newIdlePodReservations(),
		podWaiter:              newPodEventWaiter(),
	}
	return service
}

// PodEventHandler wakes cold-claim waiters from shared pod informer events.
func (s *SandboxService) PodEventHandler() cache.ResourceEventHandlerFuncs {
	if s == nil {
		return cache.ResourceEventHandlerFuncs{}
	}
	return s.ensurePodEventWaiter().ResourceEventHandler()
}

func (s *SandboxService) SetClaimStartLimiter(limiter *startlimiter.Limiter) {
	if s == nil {
		return
	}
	s.claimStartLimiter = limiter
}

func ClaimStartRetryAfter(err error) time.Duration {
	return startlimiter.RetryAfter(err)
}

func (s *SandboxService) ensurePodEventWaiter() *podEventWaiter {
	s.podWaiterMu.Lock()
	defer s.podWaiterMu.Unlock()
	if s.podWaiter == nil {
		s.podWaiter = newPodEventWaiter()
	}
	return s.podWaiter
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

// SetPauseEnqueuer injects the background worker used to complete accepted pause operations.
func (s *SandboxService) SetPauseEnqueuer(enqueuer SandboxPauseEnqueuer) {
	s.pauseEnqueuer = enqueuer
}

// SetCredentialStore injects the sandbox credential binding store.
func (s *SandboxService) SetCredentialStore(store egressauth.BindingStore) {
	s.credentialStore = store
}

// SetWebhookStateVolumeClient injects the system volume client used for durable webhook state.
func (s *SandboxService) SetWebhookStateVolumeClient(client SandboxSystemVolumeClient) {
	s.webhookStateVolumes = client
	if metadataClient, ok := client.(SandboxVolumeMetadataClient); ok {
		s.volumeMetadata = metadataClient
	}
}

// SetVolumeMetadataClient injects the metadata client used to validate user volume mounts.
func (s *SandboxService) SetVolumeMetadataClient(client SandboxVolumeMetadataClient) {
	s.volumeMetadata = client
}

// SetDeletionWebhookEmitter injects the emitter for manager-owned sandbox deletion events.
func (s *SandboxService) SetDeletionWebhookEmitter(emitter SandboxDeletionWebhookEmitter) {
	s.deletionWebhookEmitter = emitter
}

// SetTeamQuotaStore injects the region-wide, strongly consistent team quota
// allocation store.
func (s *SandboxService) SetTeamQuotaStore(store teamquota.CapacityStore) {
	s.teamQuotaStore = store
}

// SetTeamQuotaRateLimiter injects region-shared token-bucket admission.
func (s *SandboxService) SetTeamQuotaRateLimiter(limiter TeamQuotaRateLimiter) {
	s.teamQuotaRateLimiter = limiter
}

// ValidateTeamQuotaReady verifies the mandatory production admission wiring.
// Manager startup must fail before serving requests when either capacity or
// rate enforcement is unavailable.
func (s *SandboxService) ValidateTeamQuotaReady() error {
	if s == nil {
		return fmt.Errorf("%w: sandbox service is nil", ErrTeamQuotaUnavailable)
	}
	if s.teamQuotaStore == nil {
		return fmt.Errorf("%w: capacity store is not configured", ErrTeamQuotaUnavailable)
	}
	if _, ok := s.teamQuotaStore.(teamquota.ObservedExactCapacityStore); !ok {
		return fmt.Errorf("%w: observed exact capacity store is not configured", ErrTeamQuotaUnavailable)
	}
	if s.teamQuotaRateLimiter == nil {
		return fmt.Errorf("%w: rate limiter is not configured", ErrTeamQuotaUnavailable)
	}
	if s.claimStartLimiter == nil {
		return fmt.Errorf("%w: cluster claim-start limiter is not configured", ErrTeamQuotaUnavailable)
	}
	return nil
}

// SetSandboxStore injects durable sandbox identity storage.
func (s *SandboxService) SetSandboxStore(store SandboxStore) {
	s.sandboxStore = store
}

// SetRootFSObjectDeleter injects the object-store deleter used to clean up
// rootfs diffs that were uploaded but never committed into the DB rootfs head.
func (s *SandboxService) SetRootFSObjectDeleter(deleter RootFSObjectDeleter) {
	s.rootFSObjectDeleter = deleter
}

// SetTemplateImageBuildAvailable controls source capability preflight. It is
// configured before HTTP serving begins and remains stable for the process.
func (s *SandboxService) SetTemplateImageBuildAvailable(available bool) {
	if s == nil {
		return
	}
	s.templateImageBuildCapabilityConfigured = true
	s.templateImageBuildAvailable = available
}
