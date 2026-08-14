package rootfs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"k8s.io/client-go/kubernetes"
)

var (
	ErrNotFound   = errors.New("rootfs target not found")
	ErrConflict   = errors.New("rootfs validation conflict")
	ErrBadRequest = errors.New("invalid rootfs request")
)

type Config struct {
	Context          context.Context
	Runtime          rootFSV3Runtime
	PortalBackings   rootFSPortalBackingAttacher
	Store            objectstore.Store
	WatchFenceRoot   string
	CaptureLeases    rootFSCaptureLeaseStore
	MetricsRegistry  prometheus.Registerer
	KubernetesClient kubernetes.Interface
	NodeName         string
	KubeletPodsRoot  string
}

type rootFSPortalBackingAttacher interface {
	AttachRootFSBackings(context.Context, string, string) error
}

type rootFSCaptureLeaseStore interface {
	EnsureCapture(context.Context, string, string, int64) (string, error)
	BeginCapture(context.Context, string, string, int64) error
	CheckpointCapture(context.Context, string, string, int64, []rootfshead.Object) error
	ResetCapture(context.Context, string, string, int64) error
	ReleaseCapture(context.Context, string, string, int64) error
}

type Controller struct {
	store           objectstore.Store
	v3Runtime       rootFSV3Runtime
	portalBackings  rootFSPortalBackingAttacher
	v3Context       context.Context
	watchFenceRoot  string
	captureLeases   rootFSCaptureLeaseStore
	v3Mu            sync.Mutex
	v3Sessions      map[string]*rootFSSyncBinding
	v3InitMu        sync.Mutex
	v3InitLocks     map[string]*rootFSSyncInitLock
	k8sClient       kubernetes.Interface
	nodeName        string
	kubeletPodsRoot string
}

type rootFSSyncInitLock struct {
	mu   sync.Mutex
	refs int
}

func NewController(cfg Config) *Controller {
	v3Context := cfg.Context
	if v3Context == nil {
		v3Context = context.Background()
	}
	controller := &Controller{
		store:           cfg.Store,
		v3Runtime:       cfg.Runtime,
		portalBackings:  cfg.PortalBackings,
		v3Context:       v3Context,
		watchFenceRoot:  cfg.WatchFenceRoot,
		captureLeases:   cfg.CaptureLeases,
		v3Sessions:      make(map[string]*rootFSSyncBinding),
		v3InitLocks:     make(map[string]*rootFSSyncInitLock),
		k8sClient:       cfg.KubernetesClient,
		nodeName:        strings.TrimSpace(cfg.NodeName),
		kubeletPodsRoot: strings.TrimSpace(cfg.KubeletPodsRoot),
	}
	if cfg.MetricsRegistry != nil {
		cfg.MetricsRegistry.MustRegister(newRootFSSyncCollector(controller))
	}
	return controller
}

func (c *Controller) lockRootFSSyncInitialization(key string) func() {
	c.v3InitMu.Lock()
	lock := c.v3InitLocks[key]
	if lock == nil {
		lock = &rootFSSyncInitLock{}
		c.v3InitLocks[key] = lock
	}
	lock.refs++
	c.v3InitMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		c.v3InitMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(c.v3InitLocks, key)
		}
		c.v3InitMu.Unlock()
	}
}

func requestContext(r *http.Request) context.Context {
	if r != nil && r.Context() != nil {
		return r.Context()
	}
	return context.Background()
}

func validateTarget(target ctldapi.RootFSContainerRef) error {
	if strings.TrimSpace(target.Namespace) == "" {
		return fmt.Errorf("%w: namespace is required", ErrBadRequest)
	}
	if strings.TrimSpace(target.PodName) == "" {
		return fmt.Errorf("%w: pod_name is required", ErrBadRequest)
	}
	if strings.TrimSpace(target.ContainerName) == "" {
		return fmt.Errorf("%w: container_name is required", ErrBadRequest)
	}
	return nil
}

func statusForError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if errors.Is(err, ErrBadRequest) {
		return http.StatusBadRequest
	}
	if errors.Is(err, ErrConflict) {
		return http.StatusConflict
	}
	if errors.Is(err, ErrNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, rootfsstore.ErrBackendUnavailable) {
		return http.StatusServiceUnavailable
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout
	}
	return http.StatusInternalServerError
}
