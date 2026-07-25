package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	hotClaimReservationRecoveryGracePeriod = 5 * time.Minute
	hotClaimReservationResyncPeriod        = 30 * time.Second
	hotClaimDetachmentSettleWindow         = 3 * time.Second
	hotClaimDetachmentMaxDelay             = 2 * time.Minute
	hotClaimDetachmentLowWatermark         = 16
	hotClaimDetachmentRatePerSecond        = 5
)

// HotClaimReservationController safely detaches completed hot claims from
// warm-pool ReplicaSets and deletes abandoned partial claims.
type HotClaimReservationController struct {
	k8sClient      kubernetes.Interface
	podLister      corelisters.PodLister
	sandboxStore   SandboxStore
	clock          TimeProvider
	logger         *zap.Logger
	queue          workqueue.TypedRateLimitingInterface[string]
	resyncInterval time.Duration
	recoveryGrace  time.Duration
	detachPacer    hotClaimDetachmentPacer
	settleWindow   time.Duration
	maxDetachDelay time.Duration
	lowWatermark   int
}

type hotClaimDetachmentPacer interface {
	Wait(context.Context) error
}

func NewHotClaimReservationController(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	sandboxStore SandboxStore,
	logger *zap.Logger,
) *HotClaimReservationController {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &HotClaimReservationController{
		k8sClient:      k8sClient,
		podLister:      podLister,
		sandboxStore:   sandboxStore,
		clock:          systemTime{},
		logger:         logger,
		queue:          workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		resyncInterval: hotClaimReservationResyncPeriod,
		recoveryGrace:  hotClaimReservationRecoveryGracePeriod,
		detachPacer:    rate.NewLimiter(rate.Limit(hotClaimDetachmentRatePerSecond), 1),
		settleWindow:   hotClaimDetachmentSettleWindow,
		maxDetachDelay: hotClaimDetachmentMaxDelay,
		lowWatermark:   hotClaimDetachmentLowWatermark,
	}
}

// ResourceEventHandler enqueues every Pod carrying a hot-claim reservation.
func (c *HotClaimReservationController) ResourceEventHandler() cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: c.handlePod,
		UpdateFunc: func(_, newObj any) {
			c.handlePod(newObj)
		},
		DeleteFunc: c.handlePod,
	}
}

// EnqueueHotClaimReservation requests immediate reconciliation after durable
// sandbox persistence succeeds.
func (c *HotClaimReservationController) EnqueueHotClaimReservation(namespace, podName string) {
	if c == nil || c.queue == nil || namespace == "" || podName == "" {
		return
	}
	c.queue.Add(namespace + "/" + podName)
}

// Run reconciles reservations from informer events and periodic recovery scans.
func (c *HotClaimReservationController) Run(ctx context.Context, _ int) error {
	if c == nil {
		return nil
	}
	const workers = 1
	if c.queue == nil {
		c.queue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	}

	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting hot claim reservation controller", zap.Int("workers", workers))
	c.enqueueReservations()
	for range workers {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	ticker := time.NewTicker(c.resyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Hot claim reservation controller stopped")
			return ctx.Err()
		case <-ticker.C:
			c.enqueueReservations()
		}
	}
}

func (c *HotClaimReservationController) handlePod(obj any) {
	pod := extractPod(obj)
	if pod == nil || !controller.IsHotClaimReservedPod(pod) {
		return
	}
	c.EnqueueHotClaimReservation(pod.Namespace, pod.Name)
}

func (c *HotClaimReservationController) enqueueReservations() {
	if c == nil || c.podLister == nil {
		return
	}
	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		c.logger.Warn("Failed to list hot claim reservations", zap.Error(err))
		return
	}
	for _, pod := range pods {
		if controller.IsHotClaimReservedPod(pod) {
			c.EnqueueHotClaimReservation(pod.Namespace, pod.Name)
		}
	}
}

func (c *HotClaimReservationController) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *HotClaimReservationController) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	requeueAfter, err := c.reconcile(ctx, key)
	if err != nil {
		c.queue.AddRateLimited(key)
		c.logger.Warn("Hot claim reservation reconciliation failed",
			zap.String("pod", key),
			zap.Error(err),
		)
		return true
	}
	c.queue.Forget(key)
	if requeueAfter > 0 {
		c.queue.AddAfter(key, requeueAfter)
	}
	return true
}

func (c *HotClaimReservationController) reconcile(ctx context.Context, key string) (time.Duration, error) {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return 0, err
	}
	pod, err := c.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return 0, nil
		}
		return 0, err
	}
	if !controller.IsHotClaimReservedPod(pod) || pod.DeletionTimestamp != nil {
		return 0, nil
	}

	record, err := c.getSandboxRecord(ctx, sandboxIDFromPod(pod))
	if err != nil {
		return 0, err
	}
	if pod.Annotations[controller.AnnotationHotClaimReservationState] == controller.HotClaimReservationStateReady &&
		hotClaimReservationMatchesRecord(pod, record) {
		if requeueAfter := c.detachmentTimeRemaining(pod); requeueAfter > 0 {
			return requeueAfter, nil
		}
		if !c.detachmentMaxDelayElapsed(pod) && c.detachPacer != nil {
			if err := c.detachPacer.Wait(ctx); err != nil {
				return 0, fmt.Errorf("wait for hot claim detachment pace: %w", err)
			}
		}
		pod, err = c.ensureActiveResourceRequests(ctx, pod)
		if err != nil {
			return 0, err
		}
		return 0, c.finalizeReservation(ctx, pod)
	}

	if remaining := c.recoveryTimeRemaining(pod); remaining > 0 {
		return remaining, nil
	}
	return 0, c.deleteAbandonedReservation(ctx, pod, record)
}

func (c *HotClaimReservationController) getSandboxRecord(
	ctx context.Context,
	sandboxID string,
) (*SandboxRecord, error) {
	if c.sandboxStore == nil || strings.TrimSpace(sandboxID) == "" {
		return nil, nil
	}
	record, err := c.sandboxStore.GetSandbox(ctx, sandboxID)
	if errors.Is(err, ErrSandboxRecordNotFound) {
		return nil, nil
	}
	return record, err
}

func (c *HotClaimReservationController) recoveryTimeRemaining(pod *corev1.Pod) time.Duration {
	if pod == nil {
		return 0
	}
	rawReservedAt := strings.TrimSpace(pod.Annotations[controller.AnnotationHotClaimReservedAt])
	reservedAt, err := time.Parse(time.RFC3339Nano, rawReservedAt)
	if err != nil {
		return 0
	}
	now := c.clock.Now()
	if reservedAt.After(now) {
		return c.recoveryGrace
	}
	remaining := c.recoveryGrace - now.Sub(reservedAt)
	if remaining <= 0 {
		return 0
	}
	return remaining
}

func (c *HotClaimReservationController) detachmentTimeRemaining(pod *corev1.Pod) time.Duration {
	readyAt, ok := hotClaimReservationReadyTime(pod)
	if !ok || c.settleWindow <= 0 || c.detachmentMaxDelayElapsedAt(readyAt) {
		return 0
	}
	if c.claimableIdleBelowLowWatermark(pod) {
		return 0
	}
	now := c.clock.Now()
	if readyAt.After(now) {
		return c.settleWindow
	}
	remaining := c.settleWindow - now.Sub(readyAt)
	if remaining <= 0 {
		return 0
	}
	return remaining
}

func (c *HotClaimReservationController) detachmentMaxDelayElapsed(pod *corev1.Pod) bool {
	readyAt, ok := hotClaimReservationReadyTime(pod)
	return !ok || c.detachmentMaxDelayElapsedAt(readyAt)
}

func (c *HotClaimReservationController) detachmentMaxDelayElapsedAt(readyAt time.Time) bool {
	if c.maxDetachDelay <= 0 {
		return true
	}
	now := c.clock.Now()
	return !readyAt.After(now) && now.Sub(readyAt) >= c.maxDetachDelay
}

func (c *HotClaimReservationController) claimableIdleBelowLowWatermark(pod *corev1.Pod) bool {
	if c.lowWatermark <= 0 || c.podLister == nil || pod == nil {
		return false
	}
	templateID := strings.TrimSpace(pod.Labels[controller.LabelTemplateID])
	if templateID == "" {
		return false
	}
	pods, err := c.podLister.Pods(pod.Namespace).List(labels.SelectorFromSet(map[string]string{
		controller.LabelTemplateID: templateID,
		controller.LabelPoolType:   controller.PoolTypeIdle,
	}))
	if err != nil {
		c.logger.Warn("Failed to count claimable idle pods before hot claim detachment",
			zap.String("template", templateID),
			zap.Error(err),
		)
		return false
	}
	claimable := 0
	for _, candidate := range pods {
		if candidate.DeletionTimestamp != nil ||
			controller.IsHotClaimReservedPod(candidate) ||
			!controller.IsPodReady(candidate) {
			continue
		}
		claimable++
		if claimable >= c.lowWatermark {
			return false
		}
	}
	return true
}

func hotClaimReservationReadyTime(pod *corev1.Pod) (time.Time, bool) {
	if pod == nil {
		return time.Time{}, false
	}
	for _, key := range []string{
		controller.AnnotationHotClaimReadyAt,
		controller.AnnotationHotClaimReservedAt,
	} {
		value := strings.TrimSpace(pod.Annotations[key])
		if value == "" {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func (c *HotClaimReservationController) ensureActiveResourceRequests(
	ctx context.Context,
	pod *corev1.Pod,
) (*corev1.Pod, error) {
	if pod == nil {
		return nil, nil
	}
	rawResources := strings.TrimSpace(pod.Annotations[controller.AnnotationHotClaimActiveResources])
	if rawResources == "" {
		return pod, nil
	}
	var desired corev1.ResourceRequirements
	if err := json.Unmarshal([]byte(rawResources), &desired); err != nil {
		return pod, fmt.Errorf("decode hot claim active resources: %w", err)
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == "procd" && resizeResourcesEqual(container.Resources, desired) {
			return pod, nil
		}
	}

	resized, err := resizeSandboxPodToResources(ctx, c.k8sClient, pod, desired)
	if err != nil {
		return pod, fmt.Errorf("expand active sandbox resource requests: %w", err)
	}
	return mergeSandboxMetadataAfterResize(resized, pod), nil
}

func (c *HotClaimReservationController) finalizeReservation(ctx context.Context, pod *corev1.Pod) error {
	operations := hotClaimReservationPreconditions(pod)
	operations = appendReplicaSetOwnerRemovalPatch(
		operations,
		pod.OwnerReferences,
		removeReplicaSetControllerOwnerReferences(pod.OwnerReferences),
	)
	operations = append(operations,
		claimMetadataPatchOperation{
			Operation: "replace",
			Path:      metadataMapPath("labels", controller.LabelPoolType),
			Value:     controller.PoolTypeActive,
		},
		claimMetadataPatchOperation{
			Operation: "remove",
			Path:      metadataMapPath("annotations", controller.AnnotationHotClaimReservationState),
		},
		claimMetadataPatchOperation{
			Operation: "remove",
			Path:      metadataMapPath("annotations", controller.AnnotationHotClaimReservedAt),
		},
	)
	if pod.Annotations[controller.AnnotationHotClaimReadyAt] != "" {
		operations = append(operations, claimMetadataPatchOperation{
			Operation: "remove",
			Path:      metadataMapPath("annotations", controller.AnnotationHotClaimReadyAt),
		})
	}
	if pod.Annotations[controller.AnnotationHotClaimActiveResources] != "" {
		operations = append(operations, claimMetadataPatchOperation{
			Operation: "remove",
			Path:      metadataMapPath("annotations", controller.AnnotationHotClaimActiveResources),
		})
	}
	operations = append(operations, claimMetadataPatchOperation{
		Operation: "remove",
		Path:      metadataMapPath("annotations", controller.AnnotationHotClaimReservation),
	})
	patch, err := json.Marshal(operations)
	if err != nil {
		return fmt.Errorf("marshal hot claim detachment patch: %w", err)
	}
	_, err = c.k8sClient.CoreV1().Pods(pod.Namespace).Patch(
		ctx,
		pod.Name,
		types.JSONPatchType,
		patch,
		metav1.PatchOptions{},
	)
	if err == nil {
		c.logger.Info("Detached completed hot claim from warm pool",
			zap.String("pod", pod.Namespace+"/"+pod.Name),
			zap.String("sandboxID", sandboxIDFromPod(pod)),
		)
	}
	return err
}

func hotClaimReservationPreconditions(pod *corev1.Pod) []claimMetadataPatchOperation {
	operations := make([]claimMetadataPatchOperation, 0, 5)
	if pod.UID != "" {
		operations = append(operations, claimMetadataPatchOperation{
			Operation: "test",
			Path:      "/metadata/uid",
			Value:     pod.UID,
		})
	}
	if pod.ResourceVersion != "" {
		operations = append(operations, claimMetadataPatchOperation{
			Operation: "test",
			Path:      "/metadata/resourceVersion",
			Value:     pod.ResourceVersion,
		})
	}
	return append(operations,
		claimMetadataPatchOperation{
			Operation: "test",
			Path:      metadataMapPath("labels", controller.LabelPoolType),
			Value:     controller.PoolTypeIdle,
		},
		claimMetadataPatchOperation{
			Operation: "test",
			Path:      metadataMapPath("annotations", controller.AnnotationHotClaimReservation),
			Value:     pod.Annotations[controller.AnnotationHotClaimReservation],
		},
		claimMetadataPatchOperation{
			Operation: "test",
			Path:      metadataMapPath("annotations", controller.AnnotationHotClaimReservationState),
			Value:     controller.HotClaimReservationStateReady,
		},
	)
}

func (c *HotClaimReservationController) deleteAbandonedReservation(
	ctx context.Context,
	pod *corev1.Pod,
	record *SandboxRecord,
) error {
	if hotClaimReservationMatchesRecord(pod, record) && c.sandboxStore != nil {
		if err := c.sandboxStore.MarkSandboxDeleted(ctx, record.ID, c.clock.Now().UTC()); err != nil {
			return fmt.Errorf("mark abandoned sandbox deleted: %w", err)
		}
	}
	uid := pod.UID
	resourceVersion := pod.ResourceVersion
	err := c.k8sClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{
			UID:             &uid,
			ResourceVersion: &resourceVersion,
		},
	})
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	c.logger.Warn("Deleted abandoned hot claim reservation",
		zap.String("pod", pod.Namespace+"/"+pod.Name),
		zap.String("sandboxID", sandboxIDFromPod(pod)),
	)
	return nil
}

func hotClaimReservationMatchesRecord(pod *corev1.Pod, record *SandboxRecord) bool {
	if pod == nil || record == nil || !record.DeletedAt.IsZero() {
		return false
	}
	if record.Status != SandboxStatusStarting && record.Status != SandboxStatusRunning {
		return false
	}
	if record.ID != sandboxIDFromPod(pod) ||
		record.CurrentPodNamespace != pod.Namespace ||
		record.CurrentPodName != pod.Name {
		return false
	}
	podGeneration := runtimeGenerationFromPod(pod)
	return podGeneration <= 0 || record.RuntimeGeneration <= 0 || record.RuntimeGeneration == podGeneration
}
