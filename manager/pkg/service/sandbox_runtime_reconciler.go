package service

import (
	"context"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	defaultSandboxRuntimeReconcilePeriod   = 30 * time.Second
	defaultSandboxRuntimeReconcilePageSize = 500
)

type sandboxRuntimeReconcileStore interface {
	ListRuntimeReconcileCandidates(ctx context.Context, clusterID, afterSandboxID string, limit int) ([]sandboxstore.SandboxRuntimeReconcileCandidate, error)
}

type sandboxRuntimeStateReconciler interface {
	ReconcileSandboxRuntime(ctx context.Context, sandboxID string) error
}

// SandboxRuntimeReconciler repairs drift between durable sandbox intent and
// the disposable Kubernetes runtime. Informer events are the fast path; the
// paginated store scan supplies anti-entropy after missed events or restarts.
type SandboxRuntimeReconciler struct {
	clusterID    string
	store        sandboxRuntimeReconcileStore
	podLister    corelisters.PodLister
	reconciler   sandboxRuntimeStateReconciler
	logger       *zap.Logger
	queue        workqueue.TypedRateLimitingInterface[string]
	resyncPeriod time.Duration
	pageSize     int
}

func NewSandboxRuntimeReconciler(
	clusterID string,
	store sandboxRuntimeReconcileStore,
	podLister corelisters.PodLister,
	reconciler sandboxRuntimeStateReconciler,
	logger *zap.Logger,
) *SandboxRuntimeReconciler {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxRuntimeReconciler{
		clusterID:    strings.TrimSpace(clusterID),
		store:        store,
		podLister:    podLister,
		reconciler:   reconciler,
		logger:       logger,
		queue:        workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		resyncPeriod: defaultSandboxRuntimeReconcilePeriod,
		pageSize:     defaultSandboxRuntimeReconcilePageSize,
	}
}

func (c *SandboxRuntimeReconciler) ResourceEventHandler() cache.ResourceEventHandlerFuncs {
	if c == nil {
		return cache.ResourceEventHandlerFuncs{}
	}
	return cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj any) {
			oldPod := sandboxPodFromInformerEvent(oldObj)
			newPod := sandboxPodFromInformerEvent(newObj)
			if newPod != nil && newPod.DeletionTimestamp != nil && (oldPod == nil || oldPod.DeletionTimestamp == nil) {
				c.enqueuePod(newPod)
			}
		},
		DeleteFunc: c.enqueuePod,
	}
}

func (c *SandboxRuntimeReconciler) Run(ctx context.Context, workers int) error {
	if c == nil {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if c.pageSize <= 0 {
		c.pageSize = defaultSandboxRuntimeReconcilePageSize
	}
	if c.resyncPeriod <= 0 {
		c.resyncPeriod = defaultSandboxRuntimeReconcilePeriod
	}
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting sandbox runtime reconciler", zap.Int("workers", workers), zap.String("clusterID", c.clusterID))
	c.enqueueDriftCandidates(ctx)
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	ticker := time.NewTicker(c.resyncPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Sandbox runtime reconciler stopped")
			return ctx.Err()
		case <-ticker.C:
			c.enqueueDriftCandidates(ctx)
		}
	}
}

func (c *SandboxRuntimeReconciler) enqueuePod(obj any) {
	if c == nil || c.queue == nil {
		return
	}
	pod := sandboxPodFromInformerEvent(obj)
	if pod == nil || !sandboxRuntimePodOwnedBySandbox(pod) {
		return
	}
	c.enqueueSandbox(sandboxPodID(pod))
}

func (c *SandboxRuntimeReconciler) enqueueSandbox(sandboxID string) {
	if c == nil || c.queue == nil {
		return
	}
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID != "" {
		c.queue.Add(sandboxID)
	}
}

func (c *SandboxRuntimeReconciler) enqueueDriftCandidates(ctx context.Context) {
	if c == nil || c.store == nil {
		return
	}
	afterSandboxID := ""
	for {
		candidates, err := c.store.ListRuntimeReconcileCandidates(ctx, c.clusterID, afterSandboxID, c.pageSize)
		if err != nil {
			c.logger.Warn("Failed to list sandbox runtime reconcile candidates", zap.Error(err))
			return
		}
		for _, candidate := range candidates {
			if c.candidateNeedsReconcile(candidate) {
				c.enqueueSandbox(candidate.SandboxID)
			}
		}
		if len(candidates) < c.pageSize {
			return
		}
		next := strings.TrimSpace(candidates[len(candidates)-1].SandboxID)
		if next == "" || next <= afterSandboxID {
			c.logger.Error("Sandbox runtime reconcile pagination did not advance", zap.String("afterSandboxID", afterSandboxID), zap.String("nextSandboxID", next))
			return
		}
		afterSandboxID = next
	}
}

func (c *SandboxRuntimeReconciler) candidateNeedsReconcile(candidate sandboxstore.SandboxRuntimeReconcileCandidate) bool {
	if candidate.RuntimeBackend == sandboxstore.SandboxRuntimeBackendNomad {
		return false
	}
	if candidate.DesiredState == sandboxstore.SandboxDesiredStateTerminating {
		return true
	}
	if strings.TrimSpace(candidate.PodNamespace) == "" || strings.TrimSpace(candidate.PodName) == "" || c.podLister == nil {
		return true
	}
	pod, err := c.podLister.Pods(candidate.PodNamespace).Get(candidate.PodName)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			c.logger.Warn("Failed to inspect cached sandbox runtime", zap.String("sandboxID", candidate.SandboxID), zap.Error(err))
		}
		return true
	}
	return pod.DeletionTimestamp != nil ||
		!sandboxRuntimePodOwnedBySandbox(pod) ||
		sandboxPodID(pod) != candidate.SandboxID ||
		runtimeGenerationFromPod(pod) != candidate.RuntimeGeneration
}

func (c *SandboxRuntimeReconciler) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *SandboxRuntimeReconciler) processNextWorkItem(ctx context.Context) bool {
	sandboxID, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(sandboxID)
	if c.reconciler == nil {
		c.queue.Forget(sandboxID)
		return true
	}
	if err := c.reconciler.ReconcileSandboxRuntime(ctx, sandboxID); err != nil {
		c.logger.Warn("Sandbox runtime reconcile failed, requeueing", zap.String("sandboxID", sandboxID), zap.Error(err))
		c.queue.AddRateLimited(sandboxID)
		return true
	}
	c.queue.Forget(sandboxID)
	return true
}

func sandboxRuntimePodOwnedBySandbox(pod *corev1.Pod) bool {
	return pod != nil && controller.IsClaimedSandboxPod(pod) && strings.TrimSpace(sandboxPodID(pod)) != ""
}

var _ sandboxRuntimeStateReconciler = (*SandboxService)(nil)
