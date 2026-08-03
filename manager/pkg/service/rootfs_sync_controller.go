package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const defaultRootFSSyncResyncPeriod = 30 * time.Second

type rootFSSyncBindFunc func(context.Context, *corev1.Pod) error

// RootFSSyncController ensures every active runtime generation has a ctld
// continuous-sync session. The periodic pass covers manager and ctld restarts,
// including running sandboxes that predate the metadata-head rollout.
type RootFSSyncController struct {
	podLister      corelisters.PodLister
	logger         *zap.Logger
	queue          workqueue.TypedRateLimitingInterface[string]
	resyncInterval time.Duration
	bind           rootFSSyncBindFunc
}

func NewRootFSSyncController(podLister corelisters.PodLister, service *SandboxService, logger *zap.Logger) *RootFSSyncController {
	if logger == nil {
		logger = zap.NewNop()
	}
	result := &RootFSSyncController{
		podLister:      podLister,
		logger:         logger,
		queue:          workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		resyncInterval: defaultRootFSSyncResyncPeriod,
	}
	if service != nil {
		result.bind = func(ctx context.Context, pod *corev1.Pod) error {
			return service.bindSandboxRootFSSync(ctx, pod, &ClaimRequest{
				SandboxID: sandboxIDFromPod(pod),
				TeamID:    strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID]),
			})
		}
	}
	return result
}

// ResourceEventHandler schedules sync after an active Pod is created or its
// procd container becomes available.
func (c *RootFSSyncController) ResourceEventHandler() cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: c.handlePod,
		UpdateFunc: func(_, newObj any) {
			c.handlePod(newObj)
		},
	}
}

// Run starts event-driven reconciliation plus a periodic recovery scan.
func (c *RootFSSyncController) Run(ctx context.Context, workers int) error {
	if c == nil {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if c.queue == nil {
		c.queue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	}
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting rootfs sync controller", zap.Int("workers", workers))
	c.enqueueActiveSandboxes()
	for range workers {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	interval := c.resyncInterval
	if interval <= 0 {
		interval = defaultRootFSSyncResyncPeriod
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Rootfs sync controller stopped")
			return ctx.Err()
		case <-ticker.C:
			c.enqueueActiveSandboxes()
		}
	}
}

func (c *RootFSSyncController) handlePod(obj any) {
	pod := extractPod(obj)
	if !rootFSSyncCandidate(pod) {
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(pod)
	if err == nil {
		c.queue.Add(key)
	}
}

func (c *RootFSSyncController) enqueueActiveSandboxes() {
	if c == nil || c.podLister == nil {
		return
	}
	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		c.logger.Warn("Failed to list pods for rootfs sync reconcile", zap.Error(err))
		return
	}
	for _, pod := range pods {
		c.handlePod(pod)
	}
}

func (c *RootFSSyncController) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *RootFSSyncController) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.reconcile(ctx, key); err != nil {
		c.queue.AddRateLimited(key)
		c.logger.Warn("Rootfs sync reconciliation failed",
			zap.String("pod", key),
			zap.Error(err),
		)
		return true
	}
	c.queue.Forget(key)
	return true
}

func (c *RootFSSyncController) reconcile(ctx context.Context, key string) error {
	if c == nil || c.podLister == nil || c.bind == nil {
		return nil
	}
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	pod, err := c.podLister.Pods(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if !rootFSSyncCandidate(pod) {
		return nil
	}
	bindCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if err := c.bind(bindCtx, pod); err != nil {
		return fmt.Errorf("bind sandbox %s rootfs sync: %w", sandboxIDFromPod(pod), err)
	}
	return nil
}

func rootFSSyncCandidate(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if pod.Labels[controller.LabelPoolType] != controller.PoolTypeActive || sandboxIDFromPod(pod) == "" {
		return false
	}
	if strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID]) == "" || strings.TrimSpace(pod.Spec.NodeName) == "" {
		return false
	}
	status := procdContainerStatus(pod)
	return status != nil && status.State.Running != nil && strings.TrimSpace(status.ContainerID) != ""
}
