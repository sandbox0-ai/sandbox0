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
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type terminatedSandboxRuntimeRecoverer interface {
	RecoverTerminatedSandboxRuntime(ctx context.Context, pod *corev1.Pod) error
}

type sandboxCrashRecoveryItem struct {
	Namespace string
	PodName   string
	PodUID    string
}

// SandboxCrashRecoveryController starts durable rootfs recovery as soon as a
// claimed procd container terminates.
type SandboxCrashRecoveryController struct {
	k8sClient kubernetes.Interface
	podLister corelisters.PodLister
	recoverer terminatedSandboxRuntimeRecoverer
	logger    *zap.Logger
	queue     workqueue.TypedRateLimitingInterface[sandboxCrashRecoveryItem]
}

func NewSandboxCrashRecoveryController(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	recoverer terminatedSandboxRuntimeRecoverer,
	logger *zap.Logger,
) *SandboxCrashRecoveryController {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxCrashRecoveryController{
		k8sClient: k8sClient,
		podLister: podLister,
		recoverer: recoverer,
		logger:    logger,
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
			c.enqueueTerminatedPod(nil, extractPod(obj))
		},
		UpdateFunc: func(oldObj, newObj any) {
			c.enqueueTerminatedPod(extractPod(oldObj), extractPod(newObj))
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
	<-ctx.Done()
	c.logger.Info("Sandbox crash recovery controller stopped")
	return ctx.Err()
}

func (c *SandboxCrashRecoveryController) enqueueTerminatedPod(oldPod, newPod *corev1.Pod) {
	if c == nil || c.queue == nil || !sandboxCrashRecoveryPodEligible(newPod) {
		return
	}
	newStatus, newTerminated := terminatedProcdContainer(newPod)
	if newTerminated == nil && !sandboxRuntimePodTerminal(newPod) {
		return
	}
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
	c.queue.Add(sandboxCrashRecoveryItem{
		Namespace: newPod.Namespace,
		PodName:   newPod.Name,
		PodUID:    string(newPod.UID),
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

	if err := c.reconcile(ctx, item); err != nil {
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
	return true
}

func (c *SandboxCrashRecoveryController) reconcile(ctx context.Context, item sandboxCrashRecoveryItem) error {
	if c == nil || c.recoverer == nil || strings.TrimSpace(item.Namespace) == "" || strings.TrimSpace(item.PodName) == "" {
		return nil
	}
	pod, err := c.getPod(ctx, item.Namespace, item.PodName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get terminated sandbox pod: %w", err)
	}
	if item.PodUID != "" && string(pod.UID) != item.PodUID {
		return nil
	}
	if _, terminated := terminatedProcdContainer(pod); (terminated == nil && !sandboxRuntimePodTerminal(pod)) || pod.DeletionTimestamp != nil {
		return nil
	}
	return c.recoverer.RecoverTerminatedSandboxRuntime(ctx, pod)
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
