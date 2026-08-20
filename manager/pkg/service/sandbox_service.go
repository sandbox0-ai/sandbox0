package service

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	egressauth "github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxindex"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func optionalTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

// errNoIdlePod is returned when no idle pod is available for claiming.
var errNoIdlePod = errors.New("no idle pod available")
var ErrInvalidClaimRequest = errors.New("invalid claim request")
var ErrClaimConflict = errors.New("claim conflict")
var ErrDataPlaneNotReady = errors.New("data plane not ready")
var ErrQuotaExceeded = errors.New("quota exceeded")
var ErrTemplateNotFound = errors.New("template not found")
var ErrInvalidNetworkPolicy = errors.New("invalid network policy")
var ErrSandboxCheckpointRequiresCtld = errors.New("sandbox checkpoint requires ctld")
var ErrSandboxRuntimeUpdateUnavailable = errors.New("sandbox runtime update is unavailable")

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
	ClusterID                           string
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
	ProcdPort                           int
	ProcdClientTimeout                  time.Duration
	ProcdHTTPClient                     *http.Client
	RuntimeReadyTimeout                 time.Duration
	AllowColdStartWithoutReadyDataPlane bool
	PreferredNodeSelector               map[string]string
	RootFSSquashDisabled                bool
	RootFSSquashMaxChainDepth           int
	RootFSSquashMaxChainBytes           int64
	PublicRootDomain                    string
	PublicRegionID                      string
	AutoscalerSafeToEvictAnnotationKeys []string
}

// SandboxService handles sandbox operations
type SandboxService struct {
	k8sClient                              kubernetes.Interface
	hotClaimK8sClient                      kubernetes.Interface
	podLister                              corelisters.PodLister
	nodeLister                             corelisters.NodeLister
	sandboxIndex                           *sandboxindex.SandboxIndex
	secretLister                           corelisters.SecretLister
	templateLister                         controller.TemplateLister
	networkPolicyService                   *networkpolicy.NetworkPolicyService
	networkProvider                        network.Provider
	procdClient                            *procdapi.ProcdClient
	ctldClient                             *ctldapi.Client
	internalTokenGenerator                 TokenGenerator
	clock                                  TimeProvider
	config                                 SandboxServiceConfig
	logger                                 *zap.Logger
	metrics                                *obsmetrics.ManagerMetrics
	pauseEnqueuer                          SandboxPauseEnqueuer
	hotClaimReservationEnqueuer            HotClaimReservationEnqueuer
	credentialStore                        egressauth.BindingStore
	quotaStore                             TeamQuotaLimitStore
	sandboxStore                           sandboxstore.SandboxStore
	rootFSObjectDeleter                    sandboxstore.RootFSObjectDeleter
	templateImageBuildCapabilityConfigured bool
	templateImageBuildAvailable            bool
	resumeGroup                            singleflight.Group
	idleClaimMu                            sync.Mutex
	idleClaimReservations                  map[string]string
	podWaiterMu                            sync.Mutex
	podWaiter                              *podEventWaiter
}

type TeamQuotaLimitStore interface {
	GetLimit(ctx context.Context, teamID string, dimension quota.Dimension) (*quota.Limit, error)
	CurrentUsage(ctx context.Context, teamID string, dimension quota.Dimension) (int64, error)
}

// SandboxPauseEnqueuer schedules durable pause transactions for background completion.
type SandboxPauseEnqueuer interface {
	EnqueueSandboxPause(sandboxID string)
}

// SandboxRecoveryEnqueuer schedules a durable pause followed by runtime reconstruction.
type SandboxRecoveryEnqueuer interface {
	EnqueueSandboxRecovery(sandboxID string)
}

// HotClaimReservationEnqueuer schedules a completed hot claim for warm-pool detachment.
type HotClaimReservationEnqueuer interface {
	EnqueueHotClaimReservation(namespace, podName string)
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

// SandboxServiceDependencies names the collaborators required by
// SandboxService. Optional stores and workers may be nil to disable their
// corresponding capability.
type SandboxServiceDependencies struct {
	K8sClient                   kubernetes.Interface
	HotClaimK8sClient           kubernetes.Interface
	PodLister                   corelisters.PodLister
	NodeLister                  corelisters.NodeLister
	SandboxIndex                *sandboxindex.SandboxIndex
	SecretLister                corelisters.SecretLister
	TemplateLister              controller.TemplateLister
	NetworkPolicyService        *networkpolicy.NetworkPolicyService
	NetworkProvider             network.Provider
	InternalTokenGenerator      TokenGenerator
	Clock                       TimeProvider
	Config                      SandboxServiceConfig
	Logger                      *zap.Logger
	Metrics                     *obsmetrics.ManagerMetrics
	ProcdClient                 *procdapi.ProcdClient
	CtldClient                  *ctldapi.Client
	PauseEnqueuer               SandboxPauseEnqueuer
	HotClaimReservationEnqueuer HotClaimReservationEnqueuer
	CredentialStore             egressauth.BindingStore
	QuotaStore                  TeamQuotaLimitStore
	SandboxStore                sandboxstore.SandboxStore
	RootFSObjectDeleter         sandboxstore.RootFSObjectDeleter
}

// NewSandboxServiceWithDependencies creates a SandboxService from named
// collaborators so production composition does not depend on argument order or
// follow-up setter calls.
func NewSandboxServiceWithDependencies(deps SandboxServiceDependencies) *SandboxService {
	config := deps.Config
	// Use system time as fallback if clock is nil
	if deps.Clock == nil {
		deps.Clock = systemTime{}
	}
	if config.CtldPort == 0 {
		config.CtldPort = 8095
	}
	if config.CtldClientTimeout == 0 {
		config.CtldClientTimeout = ctldapi.DefaultRequestTimeout
	}
	if config.RootFSSquashMaxChainDepth <= 0 {
		config.RootFSSquashMaxChainDepth = 8
	}
	if config.RootFSSquashMaxChainBytes <= 0 {
		config.RootFSSquashMaxChainBytes = 512 * 1024 * 1024
	}
	if deps.NetworkProvider == nil {
		deps.NetworkProvider = network.NewNoopProvider()
	}
	if deps.CtldClient == nil {
		deps.CtldClient = ctldapi.NewClientWithTimeout(config.CtldClientTimeout)
		if config.CtldHTTPClient != nil {
			deps.CtldClient = ctldapi.NewClient(config.CtldHTTPClient)
		}
	}
	if deps.ProcdClient == nil {
		deps.ProcdClient = procdapi.NewProcdClient(procdapi.ProcdClientConfig{Timeout: config.ProcdClientTimeout})
		if config.ProcdHTTPClient != nil {
			deps.ProcdClient = procdapi.NewProcdClientWithHTTPClient(config.ProcdHTTPClient)
		}
	}
	service := &SandboxService{
		k8sClient:                   deps.K8sClient,
		hotClaimK8sClient:           deps.HotClaimK8sClient,
		podLister:                   deps.PodLister,
		nodeLister:                  deps.NodeLister,
		sandboxIndex:                deps.SandboxIndex,
		secretLister:                deps.SecretLister,
		templateLister:              deps.TemplateLister,
		networkPolicyService:        deps.NetworkPolicyService,
		networkProvider:             deps.NetworkProvider,
		ctldClient:                  deps.CtldClient,
		procdClient:                 deps.ProcdClient,
		internalTokenGenerator:      deps.InternalTokenGenerator,
		clock:                       deps.Clock,
		config:                      config,
		logger:                      deps.Logger,
		metrics:                     deps.Metrics,
		pauseEnqueuer:               deps.PauseEnqueuer,
		hotClaimReservationEnqueuer: deps.HotClaimReservationEnqueuer,
		credentialStore:             deps.CredentialStore,
		quotaStore:                  deps.QuotaStore,
		sandboxStore:                deps.SandboxStore,
		rootFSObjectDeleter:         deps.RootFSObjectDeleter,
		idleClaimReservations:       make(map[string]string),
		podWaiter:                   newPodEventWaiter(),
	}
	return service
}

// PodEventHandler wakes cold-claim waiters from shared pod informer events.
func (s *SandboxService) PodEventHandler() cache.ResourceEventHandlerFuncs {
	if s == nil {
		return cache.ResourceEventHandlerFuncs{}
	}
	waiterHandler := s.ensurePodEventWaiter().ResourceEventHandler()
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if waiterHandler.AddFunc != nil {
				waiterHandler.AddFunc(obj)
			}
			s.observeIdleClaimPodEvent(obj, false)
		},
		UpdateFunc: func(oldObj, newObj any) {
			if waiterHandler.UpdateFunc != nil {
				waiterHandler.UpdateFunc(oldObj, newObj)
			}
			s.observeIdleClaimPodEvent(newObj, false)
		},
		DeleteFunc: func(obj any) {
			if waiterHandler.DeleteFunc != nil {
				waiterHandler.DeleteFunc(obj)
			}
			s.observeIdleClaimPodEvent(obj, true)
		},
	}
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

// SetPauseEnqueuer injects the background worker used to complete accepted pause operations.
func (s *SandboxService) SetPauseEnqueuer(enqueuer SandboxPauseEnqueuer) {
	s.pauseEnqueuer = enqueuer
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
