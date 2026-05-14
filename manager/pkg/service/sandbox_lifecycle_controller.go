package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
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
)

// SandboxLifecycleInfo carries the durable identity needed to clean sandbox-scoped state.
type SandboxLifecycleInfo struct {
	Namespace            string
	PodName              string
	SandboxID            string
	TeamID               string
	UserID               string
	WebhookURL           string
	WebhookSecret        string
	WebhookStateVolumeID string
	PodUID               string
	NodeName             string
	HostIP               string
	VolumePortals        []SandboxLifecycleVolumePortal
}

// SandboxLifecycleVolumePortal carries the ctld identity for a bound sandbox volume portal.
type SandboxLifecycleVolumePortal struct {
	SandboxVolumeID string
	MountPoint      string
	PortalName      string
}

// SandboxDeletionCleaner cleans external state for a deleted sandbox.
type SandboxDeletionCleaner interface {
	CleanupDeletedSandbox(ctx context.Context, info SandboxLifecycleInfo) error
}

type sandboxLifecycleQueueItem struct {
	Namespace            string
	PodName              string
	SandboxID            string
	TeamID               string
	UserID               string
	WebhookURL           string
	WebhookSecret        string
	WebhookStateVolumeID string
	PodUID               string
	NodeName             string
	HostIP               string
	VolumePortalsJSON    string
	Deleted              bool
}

// SandboxLifecycleController reconciles sandbox deletion side effects from Pod lifecycle state.
type SandboxLifecycleController struct {
	k8sClient      kubernetes.Interface
	podLister      corelisters.PodLister
	cleaner        SandboxDeletionCleaner
	logger         *zap.Logger
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
	pod := extractPod(obj)
	if info, ok := sandboxLifecycleInfoFromPod(pod); ok {
		c.queue.Add(sandboxLifecycleItemFromInfo(info, false))
	}
}

func (c *SandboxLifecycleController) handlePodDelete(obj any) {
	pod := extractPod(obj)
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

	pod, err := c.k8sClient.CoreV1().Pods(item.Namespace).Get(ctx, item.PodName, metav1.GetOptions{})
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
	if err := c.removeSandboxCleanupFinalizer(ctx, pod.Namespace, pod.Name); err != nil {
		return fmt.Errorf("remove sandbox cleanup finalizer: %w", err)
	}
	return nil
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
		Namespace:            item.Namespace,
		PodName:              item.PodName,
		SandboxID:            item.SandboxID,
		TeamID:               item.TeamID,
		UserID:               item.UserID,
		WebhookURL:           item.WebhookURL,
		WebhookSecret:        item.WebhookSecret,
		WebhookStateVolumeID: item.WebhookStateVolumeID,
		PodUID:               item.PodUID,
		NodeName:             item.NodeName,
		HostIP:               item.HostIP,
		VolumePortals:        decodeSandboxLifecycleVolumePortals(item.VolumePortalsJSON),
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

	var errs []error
	if s.deletionWebhookEmitter != nil && strings.TrimSpace(info.WebhookURL) != "" {
		if err := s.deletionWebhookEmitter.EmitSandboxDeleted(ctx, info); err != nil {
			errs = append(errs, fmt.Errorf("emit sandbox.deleted webhook: %w", err))
		}
	}
	if s.networkProvider != nil && info.Namespace != "" {
		if err := s.networkProvider.RemoveSandboxPolicy(ctx, info.Namespace, sandboxID); err != nil {
			errs = append(errs, fmt.Errorf("remove network policy: %w", err))
		}
	}
	if s.credentialStore != nil {
		teamID := strings.TrimSpace(info.TeamID)
		if teamID == "" {
			logger.Warn("Skipping credential binding cleanup for sandbox without team ID",
				zap.String("sandboxID", sandboxID),
				zap.String("namespace", info.Namespace),
			)
		} else if err := s.credentialStore.DeleteBindings(ctx, teamID, sandboxID); err != nil {
			errs = append(errs, fmt.Errorf("delete credential bindings: %w", err))
		}
	}
	if err := s.deleteWebhookStateVolume(ctx, info); err != nil {
		errs = append(errs, fmt.Errorf("delete webhook state volume: %w", err))
	}
	if err := s.unbindDeletedSandboxVolumePortals(ctx, info); err != nil {
		errs = append(errs, fmt.Errorf("unbind sandbox volume portals: %w", err))
	}
	s.powerStateLocks.Delete(sandboxID)
	s.powerStateReconcilers.Delete(sandboxID)
	return errors.Join(errs...)
}

func (s *SandboxService) unbindDeletedSandboxVolumePortals(ctx context.Context, info SandboxLifecycleInfo) error {
	if s == nil || !s.config.CtldEnabled || len(info.VolumePortals) == 0 {
		return nil
	}
	if s.ctldClient == nil {
		return fmt.Errorf("ctld client is not configured")
	}
	if strings.TrimSpace(info.PodUID) == "" {
		if s.logger != nil {
			s.logger.Warn("Skipping sandbox volume portal cleanup without pod UID",
				zap.String("sandboxID", info.SandboxID),
				zap.String("namespace", info.Namespace),
				zap.String("pod", info.PodName),
			)
		}
		return nil
	}
	ctldAddress, err := s.ctldAddressForLifecycleInfo(ctx, info)
	if err != nil {
		return err
	}
	var errs []error
	for _, portal := range info.VolumePortals {
		volumeID := strings.TrimSpace(portal.SandboxVolumeID)
		mountPoint := filepath.Clean(strings.TrimSpace(portal.MountPoint))
		portalName := volumeportal.NormalizePortalName(portal.PortalName, mountPoint)
		if volumeID == "" || mountPoint == "." || !filepath.IsAbs(mountPoint) || portalName == "" {
			continue
		}
		if _, err := s.ctldClient.UnbindVolumePortal(ctx, ctldAddress, ctldapi.UnbindVolumePortalRequest{
			Namespace:       info.Namespace,
			PodName:         info.PodName,
			PodUID:          info.PodUID,
			PortalName:      portalName,
			MountPath:       mountPoint,
			SandboxVolumeID: volumeID,
		}); err != nil {
			errs = append(errs, fmt.Errorf("%s at %s: %w", volumeID, mountPoint, err))
		}
	}
	return errors.Join(errs...)
}

func (s *SandboxService) ctldAddressForLifecycleInfo(ctx context.Context, info SandboxLifecycleInfo) (string, error) {
	if strings.TrimSpace(info.HostIP) != "" {
		return fmt.Sprintf("http://%s:%d", strings.TrimSpace(info.HostIP), s.config.CtldPort), nil
	}
	nodeName := strings.TrimSpace(info.NodeName)
	if nodeName == "" {
		return "", fmt.Errorf("sandbox pod %s/%s has no node identity for ctld cleanup", info.Namespace, info.PodName)
	}
	if s.nodeLister != nil {
		node, err := s.nodeLister.Get(nodeName)
		if err == nil {
			return ctldAddressForNode(node, s.config.CtldPort)
		}
	}
	if s.k8sClient == nil {
		return "", fmt.Errorf("kubernetes client is not configured")
	}
	node, err := s.k8sClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get node %s: %w", nodeName, err)
	}
	return ctldAddressForNode(node, s.config.CtldPort)
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
		Namespace:            info.Namespace,
		PodName:              info.PodName,
		SandboxID:            info.SandboxID,
		TeamID:               info.TeamID,
		UserID:               info.UserID,
		WebhookURL:           info.WebhookURL,
		WebhookSecret:        info.WebhookSecret,
		WebhookStateVolumeID: info.WebhookStateVolumeID,
		PodUID:               info.PodUID,
		NodeName:             info.NodeName,
		HostIP:               info.HostIP,
		VolumePortalsJSON:    encodeSandboxLifecycleVolumePortals(info.VolumePortals),
		Deleted:              deleted,
	}
}

func encodeSandboxLifecycleVolumePortals(portals []SandboxLifecycleVolumePortal) string {
	if len(portals) == 0 {
		return ""
	}
	data, err := json.Marshal(portals)
	if err != nil {
		return ""
	}
	return string(data)
}

func decodeSandboxLifecycleVolumePortals(portalsJSON string) []SandboxLifecycleVolumePortal {
	if portalsJSON == "" {
		return nil
	}
	var portals []SandboxLifecycleVolumePortal
	if err := json.Unmarshal([]byte(portalsJSON), &portals); err != nil {
		return nil
	}
	return portals
}

func sandboxLifecycleInfoFromPod(pod *corev1.Pod) (SandboxLifecycleInfo, bool) {
	if pod == nil || pod.Labels == nil {
		return SandboxLifecycleInfo{}, false
	}
	if pod.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		return SandboxLifecycleInfo{}, false
	}
	sandboxID := strings.TrimSpace(pod.Labels[controller.LabelSandboxID])
	if sandboxID == "" {
		return SandboxLifecycleInfo{}, false
	}
	teamID := ""
	userID := ""
	webhookURL := ""
	webhookSecret := ""
	webhookStateVolumeID := ""
	if pod.Annotations != nil {
		teamID = strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID])
		userID = strings.TrimSpace(pod.Annotations[controller.AnnotationUserID])
		webhookStateVolumeID = strings.TrimSpace(pod.Annotations[controller.AnnotationWebhookStateVolumeID])
		if configJSON := strings.TrimSpace(pod.Annotations[controller.AnnotationConfig]); configJSON != "" {
			var cfg SandboxConfig
			if err := json.Unmarshal([]byte(configJSON), &cfg); err == nil && cfg.Webhook != nil {
				webhookURL = strings.TrimSpace(cfg.Webhook.URL)
				webhookSecret = strings.TrimSpace(cfg.Webhook.Secret)
			}
		}
	}
	return SandboxLifecycleInfo{
		Namespace:            pod.Namespace,
		PodName:              pod.Name,
		SandboxID:            sandboxID,
		TeamID:               teamID,
		UserID:               userID,
		WebhookURL:           webhookURL,
		WebhookSecret:        webhookSecret,
		WebhookStateVolumeID: webhookStateVolumeID,
		PodUID:               string(pod.UID),
		NodeName:             pod.Spec.NodeName,
		HostIP:               pod.Status.HostIP,
		VolumePortals:        sandboxLifecycleVolumePortalsFromPod(pod, webhookStateVolumeID),
	}, true
}

func sandboxLifecycleVolumePortalsFromPod(pod *corev1.Pod, webhookStateVolumeID string) []SandboxLifecycleVolumePortal {
	if pod == nil {
		return nil
	}
	portalNamesByMountPath := make(map[string]string)
	for _, ref := range expectedVolumePortalsForPod(pod) {
		mountPoint := filepath.Clean(strings.TrimSpace(ref.MountPath))
		if mountPoint == "." || !filepath.IsAbs(mountPoint) {
			continue
		}
		portalName := volumeportal.NormalizePortalName(ref.PortalName, mountPoint)
		if portalName != "" {
			portalNamesByMountPath[mountPoint] = portalName
		}
	}

	var out []SandboxLifecycleVolumePortal
	addPortal := func(volumeID, mountPoint, fallbackPortalName string) {
		volumeID = strings.TrimSpace(volumeID)
		mountPoint = filepath.Clean(strings.TrimSpace(mountPoint))
		if volumeID == "" || mountPoint == "." || !filepath.IsAbs(mountPoint) {
			return
		}
		portalName := portalNamesByMountPath[mountPoint]
		if portalName == "" {
			portalName = fallbackPortalName
		}
		portalName = volumeportal.NormalizePortalName(portalName, mountPoint)
		if portalName == "" {
			return
		}
		out = append(out, SandboxLifecycleVolumePortal{
			SandboxVolumeID: volumeID,
			MountPoint:      mountPoint,
			PortalName:      portalName,
		})
	}

	if pod.Annotations != nil {
		for _, mount := range parseClaimMounts(pod.Annotations[controller.AnnotationMounts]) {
			addPortal(mount.SandboxVolumeID, mount.MountPoint, "")
		}
	}
	addPortal(webhookStateVolumeID, webhookStateMountPoint, volumeportal.WebhookStatePortalName)
	return out
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
