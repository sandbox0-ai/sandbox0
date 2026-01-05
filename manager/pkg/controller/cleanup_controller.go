package controller

import (
	"context"
	"sort"
	"time"

	"github.com/sandbox0ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
)

// CleanupController handles cleanup of excess idle pods and expired active pods
type CleanupController struct {
	k8sClient      kubernetes.Interface
	podLister      corelisters.PodLister
	templateLister TemplateLister
	recorder       record.EventRecorder
	logger         *zap.Logger
	interval       time.Duration
}

// TemplateLister interface for listing templates
type TemplateLister interface {
	List() ([]*v1alpha1.SandboxTemplate, error)
	Get(namespace, name string) (*v1alpha1.SandboxTemplate, error)
}

// NewCleanupController creates a new CleanupController
func NewCleanupController(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	templateLister TemplateLister,
	recorder record.EventRecorder,
	logger *zap.Logger,
	interval time.Duration,
) *CleanupController {
	return &CleanupController{
		k8sClient:      k8sClient,
		podLister:      podLister,
		templateLister: templateLister,
		recorder:       recorder,
		logger:         logger,
		interval:       interval,
	}
}

// Start starts the cleanup controller
func (cc *CleanupController) Start(ctx context.Context) error {
	cc.logger.Info("Starting cleanup controller", zap.Duration("interval", cc.interval))

	ticker := time.NewTicker(cc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			cc.logger.Info("Cleanup controller stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := cc.runCleanup(ctx); err != nil {
				cc.logger.Error("Cleanup failed", zap.Error(err))
			}
		}
	}
}

// runCleanup runs a cleanup cycle
func (cc *CleanupController) runCleanup(ctx context.Context) error {
	cc.logger.Debug("Running cleanup cycle")

	templates, err := cc.templateLister.List()
	if err != nil {
		return err
	}

	for _, template := range templates {
		if err := cc.cleanupTemplate(ctx, template); err != nil {
			cc.logger.Error("Failed to cleanup template",
				zap.String("template", template.ObjectMeta.Name),
				zap.Error(err),
			)
		}
	}

	return nil
}

// cleanupTemplate cleans up pods for a specific template
func (cc *CleanupController) cleanupTemplate(ctx context.Context, template *v1alpha1.SandboxTemplate) error {
	// 1. Enforce maxIdle limit
	if err := cc.enforceMaxIdle(ctx, template); err != nil {
		return err
	}

	// 2. Clean up expired active pods
	if err := cc.cleanupExpired(ctx, template); err != nil {
		return err
	}

	return nil
}

// enforceMaxIdle enforces the maxIdle limit by deleting excess idle pods
func (cc *CleanupController) enforceMaxIdle(ctx context.Context, template *v1alpha1.SandboxTemplate) error {
	maxIdle := template.Spec.Pool.MaxIdle

	// Get all idle pods for this template from informer cache
	pods, err := cc.podLister.Pods(template.ObjectMeta.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.ObjectMeta.Name,
		LabelPoolType:   PoolTypeIdle,
	}))
	if err != nil {
		return err
	}

	idleCount := int32(len(pods))
	if idleCount <= maxIdle {
		return nil
	}

	// Delete excess idle pods (keep the newest ones)
	excess := int(idleCount - maxIdle)

	// Sort pods by creation time (newest first)
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].CreationTimestamp.After(pods[j].CreationTimestamp.Time)
	})

	cc.logger.Info("Enforcing maxIdle",
		zap.String("template", template.ObjectMeta.Name),
		zap.Int32("idle", idleCount),
		zap.Int32("maxIdle", maxIdle),
		zap.Int("toDelete", excess),
	)

	// Delete the oldest pods
	for i := len(pods) - excess; i < len(pods); i++ {
		pod := pods[i]
		cc.logger.Debug("Deleting excess idle pod",
			zap.String("pod", pod.Name),
			zap.Time("created", pod.CreationTimestamp.Time),
		)

		err := cc.k8sClient.CoreV1().Pods(template.ObjectMeta.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		if err != nil {
			cc.logger.Error("Failed to delete pod",
				zap.String("pod", pod.Name),
				zap.Error(err),
			)
			continue
		}

		cc.recorder.Eventf(template, corev1.EventTypeNormal, "ExcessPodDeleted",
			"Deleted excess idle pod %s", pod.Name)
	}

	return nil
}

// cleanupExpired cleans up expired active pods
func (cc *CleanupController) cleanupExpired(ctx context.Context, template *v1alpha1.SandboxTemplate) error {
	// Get all active pods for this template
	pods, err := cc.podLister.Pods(template.ObjectMeta.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.ObjectMeta.Name,
		LabelPoolType:   PoolTypeActive,
	}))
	if err != nil {
		return err
	}

	now := time.Now()
	expiredCount := 0

	for _, pod := range pods {
		// Check if pod has expiration annotation
		expiresAtStr, ok := pod.Annotations[AnnotationExpiresAt]
		if !ok {
			continue
		}

		expiresAt, err := time.Parse(time.RFC3339, expiresAtStr)
		if err != nil {
			cc.logger.Warn("Invalid expires-at annotation",
				zap.String("pod", pod.Name),
				zap.String("value", expiresAtStr),
			)
			continue
		}

		// Check if pod is expired
		if now.After(expiresAt) {
			cc.logger.Info("Deleting expired pod",
				zap.String("pod", pod.Name),
				zap.Time("expiresAt", expiresAt),
			)

			err := cc.k8sClient.CoreV1().Pods(template.ObjectMeta.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
			if err != nil {
				cc.logger.Error("Failed to delete expired pod",
					zap.String("pod", pod.Name),
					zap.Error(err),
				)
				continue
			}

			cc.recorder.Eventf(template, corev1.EventTypeNormal, "ExpiredPodDeleted",
				"Deleted expired pod %s", pod.Name)
			expiredCount++
		}
	}

	if expiredCount > 0 {
		cc.logger.Info("Cleaned up expired pods",
			zap.String("template", template.ObjectMeta.Name),
			zap.Int("count", expiredCount),
		)
	}

	return nil
}
