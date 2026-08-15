package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
)

const (
	sandboxCleanupFinalizer             = "sandbox0.ai/sandbox-cleanup"
	defaultSandboxLifecycleResyncPeriod = 30 * time.Second

	sandboxDeleteCleanupScopeSandboxDelete = "sandbox_delete"
	sandboxDeleteCleanupScopeRuntimeOnly   = "runtime_only"
	sandboxDeleteCleanupScopeUnknown       = "unknown"
)

// SandboxLifecycleInfo carries the durable identity needed to clean sandbox-scoped state.
type SandboxLifecycleInfo struct {
	Namespace         string
	PodName           string
	SandboxID         string
	TeamID            string
	UserID            string
	RuntimeGeneration int64
}

// SandboxDeletionCleaner cleans external state for a deleted sandbox.
type SandboxDeletionCleaner interface {
	CleanupDeletedSandbox(ctx context.Context, info SandboxLifecycleInfo) error
}

type sandboxLifecycleQueueItem struct {
	Namespace         string
	PodName           string
	SandboxID         string
	TeamID            string
	UserID            string
	RuntimeGeneration int64
	Deleted           bool
}

// SandboxLifecycleController reconciles sandbox deletion side effects from Pod lifecycle state.
type SandboxLifecycleController struct {
	k8sClient      kubernetes.Interface
	podLister      corelisters.PodLister
	cleaner        SandboxDeletionCleaner
	logger         *zap.Logger
	metrics        *obsmetrics.ManagerMetrics
	queue          workqueue.TypedRateLimitingInterface[sandboxLifecycleQueueItem]
	resyncInterval time.Duration
}

func NewSandboxLifecycleController(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	cleaner SandboxDeletionCleaner,
	logger *zap.Logger,
) *SandboxLifecycleController {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxLifecycleController{
		k8sClient:      k8sClient,
		podLister:      podLister,
		cleaner:        cleaner,
		logger:         logger,
		queue:          workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[sandboxLifecycleQueueItem]()),
		resyncInterval: defaultSandboxLifecycleResyncPeriod,
	}
}

func (c *SandboxLifecycleController) SetMetrics(metrics *obsmetrics.ManagerMetrics) {
	if c == nil {
		return
	}
	c.metrics = metrics
}

func (c *SandboxLifecycleController) ResourceEventHandler() cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    c.handlePodUpsert,
		UpdateFunc: func(_, newObj any) { c.handlePodUpsert(newObj) },
		DeleteFunc: c.handlePodDelete,
	}
}

func (c *SandboxLifecycleController) Run(ctx context.Context, workers int) error {
	if c == nil {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if c.queue == nil {
		c.queue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[sandboxLifecycleQueueItem]())
	}

	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting sandbox lifecycle controller", zap.Int("workers", workers))
	c.enqueueActiveSandboxes()
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	ticker := time.NewTicker(c.resyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Sandbox lifecycle controller stopped")
			return ctx.Err()
		case <-ticker.C:
			c.enqueueActiveSandboxes()
		}
	}
}

func (c *SandboxLifecycleController) handlePodUpsert(obj any) {
	pod := sandboxPodFromInformerEvent(obj)
	if info, ok := sandboxLifecycleInfoFromPod(pod); ok {
		c.queue.Add(sandboxLifecycleItemFromInfo(info, false))
	}
}

func (c *SandboxLifecycleController) handlePodDelete(obj any) {
	pod := sandboxPodFromInformerEvent(obj)
	if pod == nil {
		return
	}
	if info, ok := sandboxLifecycleInfoFromPod(pod); ok {
		c.queue.Add(sandboxLifecycleItemFromInfo(info, true))
	}
}

func (c *SandboxLifecycleController) enqueueActiveSandboxes() {
	if c == nil || c.podLister == nil {
		return
	}
	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		c.logger.Warn("Failed to list pods for sandbox lifecycle reconcile", zap.Error(err))
		return
	}
	for _, pod := range pods {
		if info, ok := sandboxLifecycleInfoFromPod(pod); ok {
			c.queue.Add(sandboxLifecycleItemFromInfo(info, false))
		}
	}
}

func (c *SandboxLifecycleController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *SandboxLifecycleController) processNextWorkItem(ctx context.Context) bool {
	item, shutdown := c.queue.Get()
	if shutdown {
		return false
	}

	defer c.queue.Done(item)
	if err := c.reconcile(ctx, item); err != nil {
		c.logger.Warn("Sandbox lifecycle reconcile failed, requeueing",
			zap.String("sandboxID", item.SandboxID),
			zap.String("namespace", item.Namespace),
			zap.Error(err),
		)
		c.queue.AddRateLimited(item)
		return true
	}
	c.queue.Forget(item)
	return true
}

func (c *SandboxLifecycleController) reconcile(ctx context.Context, item sandboxLifecycleQueueItem) error {
	if c == nil || c.cleaner == nil {
		return nil
	}
	if item.Namespace == "" || item.PodName == "" {
		return nil
	}

	pod, err := c.getCachedPod(ctx, item.Namespace, item.PodName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.cleanupDeletedSandbox(ctx, item)
		}
		return fmt.Errorf("get sandbox pod: %w", err)
	}

	info, ok := sandboxLifecycleInfoFromPod(pod)
	if !ok {
		return nil
	}
	item = sandboxLifecycleItemFromInfo(info, item.Deleted)
	if pod.DeletionTimestamp == nil && !item.Deleted {
		if !hasSandboxCleanupFinalizer(pod) {
			if err := c.ensurePodCleanupFinalizer(ctx, pod.Namespace, pod.Name); err != nil {
				return fmt.Errorf("ensure sandbox cleanup finalizer: %w", err)
			}
		}
		return nil
	}

	if err := c.cleanupDeletedSandbox(ctx, item); err != nil {
		return err
	}
	if !hasSandboxCleanupFinalizer(pod) {
		return nil
	}
	started := time.Now()
	if err := c.removeSandboxCleanupFinalizer(ctx, pod.Namespace, pod.Name); err != nil {
		observeSandboxDeleteCleanupPhase(c.metrics, "remove_cleanup_finalizer", sandboxDeleteCleanupScopeUnknown, started, err)
		return fmt.Errorf("remove sandbox cleanup finalizer: %w", err)
	}
	observeSandboxDeleteCleanupPhase(c.metrics, "remove_cleanup_finalizer", sandboxDeleteCleanupScopeUnknown, started, nil)
	return nil
}

func (c *SandboxLifecycleController) getCachedPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	if c.podLister != nil {
		return c.podLister.Pods(namespace).Get(name)
	}
	return c.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (c *SandboxLifecycleController) ensurePodCleanupFinalizer(ctx context.Context, namespace, name string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := c.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if pod.DeletionTimestamp != nil || hasSandboxCleanupFinalizer(pod) {
			return nil
		}
		if _, ok := sandboxLifecycleInfoFromPod(pod); !ok {
			return nil
		}
		updated := pod.DeepCopy()
		ensureSandboxCleanupFinalizer(updated)
		_, err = c.k8sClient.CoreV1().Pods(namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
}

func (c *SandboxLifecycleController) cleanupDeletedSandbox(ctx context.Context, item sandboxLifecycleQueueItem) error {
	info := SandboxLifecycleInfo{
		Namespace:         item.Namespace,
		PodName:           item.PodName,
		SandboxID:         item.SandboxID,
		TeamID:            item.TeamID,
		UserID:            item.UserID,
		RuntimeGeneration: item.RuntimeGeneration,
	}
	if info.SandboxID == "" {
		info.SandboxID = info.PodName
	}
	return c.cleaner.CleanupDeletedSandbox(ctx, info)
}

func (c *SandboxLifecycleController) removeSandboxCleanupFinalizer(ctx context.Context, namespace, name string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := c.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !hasSandboxCleanupFinalizer(pod) {
			return nil
		}
		updated := pod.DeepCopy()
		updated.Finalizers = removeFinalizer(updated.Finalizers, sandboxCleanupFinalizer)
		_, err = c.k8sClient.CoreV1().Pods(namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
}

// CleanupDeletedSandbox implements SandboxDeletionCleaner for SandboxService.
func (s *SandboxService) CleanupDeletedSandbox(ctx context.Context, info SandboxLifecycleInfo) error {
	if s == nil {
		return nil
	}
	sandboxID := strings.TrimSpace(info.SandboxID)
	if sandboxID == "" {
		sandboxID = strings.TrimSpace(info.PodName)
	}
	if sandboxID == "" {
		return nil
	}

	classifyStarted := time.Now()
	runtimeOnly, retainHot, err := s.runtimeDeletionDisposition(ctx, info)
	scope := sandboxDeleteCleanupScope(runtimeOnly)
	if err != nil {
		scope = sandboxDeleteCleanupScopeUnknown
	}
	observeSandboxDeleteCleanupPhase(s.metrics, "classify_runtime_deletion", scope, classifyStarted, err)
	if err != nil {
		return err
	}
	return s.cleanupDeletedSandbox(ctx, info, runtimeOnly, retainHot)
}

func (s *SandboxService) cleanupDeletedSandbox(ctx context.Context, info SandboxLifecycleInfo, runtimeOnly, _ bool) (cleanupErr error) {
	logger := s.logger
	if logger == nil {
		logger = zap.NewNop()
	}
	sandboxID := strings.TrimSpace(info.SandboxID)
	if sandboxID == "" {
		sandboxID = strings.TrimSpace(info.PodName)
	}
	if sandboxID == "" {
		return nil
	}
	scope := sandboxDeleteCleanupScope(runtimeOnly)
	cleanupStarted := time.Now()
	logger.Info("Cleaning sandbox deletion state",
		zap.String("sandboxID", sandboxID),
		zap.String("namespace", info.Namespace),
		zap.String("pod", info.PodName),
		zap.String("scope", scope),
	)
	defer func() {
		observeSandboxDeleteCleanupPhase(s.metrics, "cleanup_total", scope, cleanupStarted, cleanupErr)
		fields := []zap.Field{
			zap.String("sandboxID", sandboxID),
			zap.String("namespace", info.Namespace),
			zap.String("pod", info.PodName),
			zap.String("scope", scope),
			zap.Duration("duration", time.Since(cleanupStarted)),
		}
		if cleanupErr != nil {
			logger.Warn("Sandbox deletion state cleanup failed", append(fields, zap.Error(cleanupErr))...)
			return
		}
		logger.Info("Sandbox deletion state cleanup completed", fields...)
	}()

	var errs []error
	if s.networkProvider != nil && info.Namespace != "" {
		if err := s.runSandboxDeleteCleanupPhase(ctx, info, scope, "remove_network_policy", func() error {
			return s.networkProvider.RemoveSandboxPolicy(ctx, info.Namespace, sandboxID)
		}); err != nil {
			errs = append(errs, fmt.Errorf("remove network policy: %w", err))
		}
	}
	if !runtimeOnly && s.credentialStore != nil {
		teamID := strings.TrimSpace(info.TeamID)
		if teamID == "" {
			logger.Warn("Skipping credential binding cleanup for sandbox without team ID",
				zap.String("sandboxID", sandboxID),
				zap.String("namespace", info.Namespace),
			)
		} else if err := s.runSandboxDeleteCleanupPhase(ctx, info, scope, "delete_credential_bindings", func() error {
			return s.credentialStore.DeleteBindings(ctx, teamID, sandboxID)
		}); err != nil {
			errs = append(errs, fmt.Errorf("delete credential bindings: %w", err))
		}
	}
	cleanupErr = errors.Join(errs...)
	return cleanupErr
}

func (s *SandboxService) runSandboxDeleteCleanupPhase(_ context.Context, info SandboxLifecycleInfo, scope, phase string, fn func() error) error {
	started := time.Now()
	err := fn()
	observeSandboxDeleteCleanupPhase(s.metrics, phase, scope, started, err)
	logger := s.logger
	if logger == nil {
		logger = zap.NewNop()
	}
	fields := []zap.Field{
		zap.String("sandboxID", strings.TrimSpace(info.SandboxID)),
		zap.String("namespace", info.Namespace),
		zap.String("pod", info.PodName),
		zap.String("phase", phase),
		zap.String("scope", scope),
		zap.Duration("duration", time.Since(started)),
	}
	if err != nil {
		logger.Warn("Sandbox delete cleanup phase failed", append(fields, zap.Error(err))...)
		return err
	}
	logger.Debug("Sandbox delete cleanup phase completed", fields...)
	return nil
}

func (s *SandboxService) runtimeDeletionDisposition(ctx context.Context, info SandboxLifecycleInfo) (bool, bool, error) {
	if s == nil || s.sandboxStore == nil {
		return false, false, nil
	}
	sandboxID := strings.TrimSpace(info.SandboxID)
	if sandboxID == "" {
		sandboxID = strings.TrimSpace(info.PodName)
	}
	if sandboxID == "" {
		return false, false, nil
	}
	record, err := s.sandboxStore.GetSandbox(ctx, sandboxID)
	if err != nil {
		return false, false, fmt.Errorf("get sandbox record for runtime deletion cleanup: %w", err)
	}
	runtimeOnly := SandboxRecordDeletionIsRuntimeOnly(record, info.Namespace, info.PodName, info.RuntimeGeneration)
	return runtimeOnly, runtimeOnly && record != nil && record.DesiredState == sandboxstore.SandboxDesiredStatePaused, nil
}

// SandboxRecordDeletionIsRuntimeOnly reports whether Pod deletion is limited to
// runtime-scoped cleanup. For a tracked sandbox, only durable
// terminating/deleted intent authorizes sandbox-wide cleanup; untracked legacy
// Pods retain the existing sandbox-wide cleanup behavior.
func SandboxRecordDeletionIsRuntimeOnly(record *sandboxstore.SandboxRecord, _ string, _ string, _ int64) bool {
	if record == nil {
		return false
	}
	switch record.DesiredState {
	case sandboxstore.SandboxDesiredStateTerminating, sandboxstore.SandboxDesiredStateDeleted:
		return false
	default:
		return true
	}
}

func (s *SandboxService) ensureSandboxDeletionFinalizer(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	if s == nil || pod == nil || s.k8sClient == nil || hasSandboxCleanupFinalizer(pod) || pod.DeletionTimestamp != nil {
		return pod, nil
	}
	var updated *corev1.Pod
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := s.k8sClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if hasSandboxCleanupFinalizer(current) || current.DeletionTimestamp != nil {
			updated = current
			return nil
		}
		updated = current.DeepCopy()
		ensureSandboxCleanupFinalizer(updated)
		updated, err = s.k8sClient.CoreV1().Pods(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func sandboxLifecycleItemFromInfo(info SandboxLifecycleInfo, deleted bool) sandboxLifecycleQueueItem {
	return sandboxLifecycleQueueItem{
		Namespace:         info.Namespace,
		PodName:           info.PodName,
		SandboxID:         info.SandboxID,
		TeamID:            info.TeamID,
		UserID:            info.UserID,
		RuntimeGeneration: info.RuntimeGeneration,
		Deleted:           deleted,
	}
}

func sandboxLifecycleInfoFromPod(pod *corev1.Pod) (SandboxLifecycleInfo, bool) {
	if pod == nil || pod.Labels == nil {
		return SandboxLifecycleInfo{}, false
	}
	if !controller.IsClaimedSandboxPod(pod) {
		return SandboxLifecycleInfo{}, false
	}
	sandboxID := strings.TrimSpace(pod.Labels[controller.LabelSandboxID])
	if sandboxID == "" {
		return SandboxLifecycleInfo{}, false
	}
	teamID := ""
	userID := ""
	if pod.Annotations != nil {
		teamID = strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID])
		userID = strings.TrimSpace(pod.Annotations[controller.AnnotationUserID])
	}
	return SandboxLifecycleInfo{
		Namespace:         pod.Namespace,
		PodName:           pod.Name,
		SandboxID:         sandboxID,
		TeamID:            teamID,
		UserID:            userID,
		RuntimeGeneration: runtimeGenerationFromPod(pod),
	}, true
}

func ensureSandboxCleanupFinalizer(pod *corev1.Pod) {
	if pod == nil || hasSandboxCleanupFinalizer(pod) {
		return
	}
	pod.Finalizers = append(pod.Finalizers, sandboxCleanupFinalizer)
}

func hasSandboxCleanupFinalizer(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, finalizer := range pod.Finalizers {
		if finalizer == sandboxCleanupFinalizer {
			return true
		}
	}
	return false
}

func removeFinalizer(finalizers []string, target string) []string {
	if len(finalizers) == 0 {
		return nil
	}
	out := finalizers[:0]
	for _, finalizer := range finalizers {
		if finalizer != target {
			out = append(out, finalizer)
		}
	}
	return out
}

func observeSandboxDeleteCleanupPhase(metrics *obsmetrics.ManagerMetrics, phase, scope string, started time.Time, err error) {
	if metrics == nil || metrics.SandboxDeleteCleanupPhase == nil {
		return
	}
	phase = strings.TrimSpace(phase)
	if phase == "" {
		phase = "unknown"
	}
	scope = strings.TrimSpace(scope)
	if scope == "" {
		scope = sandboxDeleteCleanupScopeUnknown
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.SandboxDeleteCleanupPhase.WithLabelValues(phase, status, scope).Observe(time.Since(started).Seconds())
}

func sandboxDeleteCleanupScope(runtimeOnly bool) string {
	if runtimeOnly {
		return sandboxDeleteCleanupScopeRuntimeOnly
	}
	return sandboxDeleteCleanupScopeSandboxDelete
}
