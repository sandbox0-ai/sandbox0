package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type sandboxRuntimeRecoverer interface {
	RecoverTerminatedSandboxRuntime(ctx context.Context, pod *corev1.Pod) error
	RecoverUnhealthySandboxRuntime(ctx context.Context, pod *corev1.Pod) error
}

const (
	sandboxRecoveryCauseTerminated = "terminated"
	sandboxRecoveryCauseUnhealthy  = "unhealthy"

	defaultSandboxCrashRecoveryResyncPeriod = 30 * time.Second
)

type sandboxCrashRecoveryItem struct {
	Namespace string
	PodName   string
	PodUID    string
	Cause     string
}

// SandboxCrashRecoveryController starts durable rootfs recovery when a claimed
// procd container terminates or remains unresponsive past the health threshold.
type SandboxCrashRecoveryController struct {
	k8sClient      kubernetes.Interface
	podLister      corelisters.PodLister
	recoverer      sandboxRuntimeRecoverer
	logger         *zap.Logger
	queue          workqueue.TypedRateLimitingInterface[sandboxCrashRecoveryItem]
	unhealthyAfter time.Duration
	resyncPeriod   time.Duration
	now            func() time.Time
}

func NewSandboxCrashRecoveryController(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	recoverer sandboxRuntimeRecoverer,
	logger *zap.Logger,
) *SandboxCrashRecoveryController {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxCrashRecoveryController{
		k8sClient:      k8sClient,
		podLister:      podLister,
		recoverer:      recoverer,
		logger:         logger,
		unhealthyAfter: defaultSandboxRuntimeUnhealthyAfter,
		resyncPeriod:   defaultSandboxCrashRecoveryResyncPeriod,
		now:            time.Now,
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.NewTypedItemExponentialFailureRateLimiter[sandboxCrashRecoveryItem](100*time.Millisecond, 5*time.Second),
		),
	}
}

func (c *SandboxCrashRecoveryController) ResourceEventHandler() cache.ResourceEventHandlerFuncs {
	if c == nil {
		return cache.ResourceEventHandlerFuncs{}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			c.enqueueRuntimeRecovery(nil, sandboxPodFromInformerEvent(obj))
		},
		UpdateFunc: func(oldObj, newObj any) {
			c.enqueueRuntimeRecovery(sandboxPodFromInformerEvent(oldObj), sandboxPodFromInformerEvent(newObj))
		},
	}
}

func (c *SandboxCrashRecoveryController) Run(ctx context.Context, workers int) error {
	if c == nil {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting sandbox crash recovery controller", zap.Int("workers", workers))
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	c.enqueueRecoveryCandidates()
	resyncPeriod := c.resyncPeriod
	if resyncPeriod <= 0 {
		resyncPeriod = defaultSandboxCrashRecoveryResyncPeriod
	}
	ticker := time.NewTicker(resyncPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Sandbox crash recovery controller stopped")
			return ctx.Err()
		case <-ticker.C:
			c.enqueueRecoveryCandidates()
		}
	}
}

func (c *SandboxCrashRecoveryController) enqueueRecoveryCandidates() {
	if c == nil || c.podLister == nil {
		return
	}
	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		c.logger.Warn("Failed to list sandbox pods for crash recovery resync", zap.Error(err))
		return
	}
	for _, pod := range pods {
		c.enqueueRuntimeRecovery(nil, pod)
	}
}

func (c *SandboxCrashRecoveryController) enqueueRuntimeRecovery(oldPod, newPod *corev1.Pod) {
	if c == nil || c.queue == nil || !sandboxCrashRecoveryPodEligible(newPod) {
		return
	}
	newStatus, newTerminated := terminatedProcdContainer(newPod)
	cause := sandboxRecoveryCauseUnhealthy
	if newTerminated != nil || sandboxRuntimePodTerminal(newPod) {
		cause = sandboxRecoveryCauseTerminated
		if oldPod != nil && oldPod.UID == newPod.UID {
			oldStatus, oldTerminated := terminatedProcdContainer(oldPod)
			if newStatus != nil && newTerminated != nil && oldStatus != nil && oldTerminated != nil &&
				oldStatus.ContainerID == newStatus.ContainerID &&
				oldTerminated.FinishedAt.Equal(&newTerminated.FinishedAt) {
				return
			}
			if newTerminated == nil && oldTerminated == nil && oldPod.Status.Phase == newPod.Status.Phase {
				return
			}
		}
	} else {
		newLiveness := sandboxRuntimeLivenessCondition(newPod)
		if newLiveness == nil || newLiveness.Status != corev1.ConditionFalse {
			return
		}
		if oldPod != nil && oldPod.UID == newPod.UID {
			oldLiveness := sandboxRuntimeLivenessCondition(oldPod)
			if oldLiveness != nil && oldLiveness.Status == corev1.ConditionFalse &&
				oldLiveness.LastTransitionTime.Equal(&newLiveness.LastTransitionTime) {
				return
			}
		}
	}
	c.queue.Add(sandboxCrashRecoveryItem{
		Namespace: newPod.Namespace,
		PodName:   newPod.Name,
		PodUID:    string(newPod.UID),
		Cause:     cause,
	})
}

func (c *SandboxCrashRecoveryController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *SandboxCrashRecoveryController) processNextWorkItem(ctx context.Context) bool {
	item, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(item)

	requeueAfter, err := c.reconcile(ctx, item)
	if err != nil {
		c.logger.Warn("Sandbox crash recovery reconcile failed, requeueing",
			zap.String("namespace", item.Namespace),
			zap.String("pod", item.PodName),
			zap.String("podUID", item.PodUID),
			zap.Error(err),
		)
		c.queue.AddRateLimited(item)
		return true
	}
	c.queue.Forget(item)
	if requeueAfter > 0 {
		c.queue.AddAfter(item, requeueAfter)
	}
	return true
}

func (c *SandboxCrashRecoveryController) reconcile(ctx context.Context, item sandboxCrashRecoveryItem) (time.Duration, error) {
	if c == nil || c.recoverer == nil || strings.TrimSpace(item.Namespace) == "" || strings.TrimSpace(item.PodName) == "" {
		return 0, nil
	}
	pod, err := c.getPod(ctx, item.Namespace, item.PodName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("get sandbox pod for runtime recovery: %w", err)
	}
	if item.PodUID != "" && string(pod.UID) != item.PodUID {
		return 0, nil
	}
	if pod.DeletionTimestamp != nil {
		return 0, nil
	}
	switch item.Cause {
	case sandboxRecoveryCauseUnhealthy:
		if _, terminated := terminatedProcdContainer(pod); terminated != nil || sandboxRuntimePodTerminal(pod) {
			return 0, c.recoverer.RecoverTerminatedSandboxRuntime(ctx, pod)
		}
		condition := sandboxRuntimeLivenessCondition(pod)
		if condition == nil || condition.Status != corev1.ConditionFalse {
			return 0, nil
		}
		unhealthyAfter := c.unhealthyAfter
		if unhealthyAfter <= 0 {
			unhealthyAfter = defaultSandboxRuntimeUnhealthyAfter
		}
		now := time.Now()
		if c.now != nil {
			now = c.now()
		}
		if !sandboxRuntimeLivenessFailureSustained(condition, now, unhealthyAfter) {
			transitionedAt := condition.LastTransitionTime.Time
			if transitionedAt.IsZero() {
				return unhealthyAfter, nil
			}
			return max(transitionedAt.Add(unhealthyAfter).Sub(now), time.Millisecond), nil
		}
		return 0, c.recoverer.RecoverUnhealthySandboxRuntime(ctx, pod)
	default:
		if _, terminated := terminatedProcdContainer(pod); terminated == nil && !sandboxRuntimePodTerminal(pod) {
			return 0, nil
		}
		return 0, c.recoverer.RecoverTerminatedSandboxRuntime(ctx, pod)
	}
}

func (c *SandboxCrashRecoveryController) getPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	if c.podLister != nil {
		return c.podLister.Pods(namespace).Get(name)
	}
	if c.k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client is not configured")
	}
	return c.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
}
