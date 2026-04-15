package controller

import (
	"context"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
)

// SandboxPauseRequester defines the interface for declaring that a sandbox should be paused.
type SandboxPauseRequester interface {
	RequestPauseSandboxByID(ctx context.Context, sandboxID string) error
}

// SandboxTerminator defines the interface for terminating sandboxes.
type SandboxTerminator interface {
	TerminateSandboxByID(ctx context.Context, sandboxID string) error
}

// CleanupController handles cleanup of excess idle pods and expired active pods
type CleanupController struct {
	k8sClient         kubernetes.Interface
	podLister         corelisters.PodLister
	templateLister    TemplateLister
	recorder          record.EventRecorder
	clock             TimeProvider
	pauseRequester    SandboxPauseRequester
	sandboxTerminator SandboxTerminator
	logger            *zap.Logger
	interval          time.Duration
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
	clock TimeProvider,
	pauseRequester SandboxPauseRequester,
	sandboxTerminator SandboxTerminator,
	logger *zap.Logger,
	interval time.Duration,
) *CleanupController {
	// Use system time as fallback if clock is nil
	if clock == nil {
		clock = systemTime{}
	}

	return &CleanupController{
		k8sClient:         k8sClient,
		podLister:         podLister,
		templateLister:    templateLister,
		recorder:          recorder,
		clock:             clock,
		pauseRequester:    pauseRequester,
		sandboxTerminator: sandboxTerminator,
		logger:            logger,
		interval:          interval,
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
		if err := cc.cleanupExpired(ctx, template); err != nil {
			cc.logger.Error("Failed to cleanup expired sandbox",
				zap.String("template", template.Name),
				zap.Error(err),
			)
		}
	}

	return nil
}

// cleanupExpired cleans up expired active pods
func (cc *CleanupController) cleanupExpired(ctx context.Context, template *v1alpha1.SandboxTemplate) error {
	// Get all active pods for this template
	pods, err := cc.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeActive,
	}))
	if err != nil {
		return err
	}

	now := cc.clock.Now()
	expiredCount := 0

	for _, pod := range pods {
		// Hard expiry: delete even if paused.
		if hardExpiresAtStr := pod.Annotations[AnnotationHardExpiresAt]; hardExpiresAtStr != "" {
			hardExpiresAt, err := time.Parse(time.RFC3339, hardExpiresAtStr)
			if err != nil {
				cc.logger.Warn("Invalid hard-expires-at annotation",
					zap.String("pod", pod.Name),
					zap.String("value", hardExpiresAtStr),
				)
			} else if now.After(hardExpiresAt) {
				cc.logger.Info("Deleting hard-expired pod",
					zap.String("pod", pod.Name),
					zap.Time("hardExpiresAt", hardExpiresAt),
				)
				if cc.sandboxTerminator != nil {
					if err := cc.sandboxTerminator.TerminateSandboxByID(ctx, pod.Name); err != nil {
						cc.logger.Error("Failed to delete hard-expired pod",
							zap.String("pod", pod.Name),
							zap.Error(err),
						)
						continue
					}
				} else {
					cc.logger.Warn("SandboxTerminator not configured, deleting pod directly",
						zap.String("pod", pod.Name),
					)
					if err := cc.k8sClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
						cc.logger.Error("Failed to delete hard-expired pod",
							zap.String("pod", pod.Name),
							zap.Error(err),
						)
						continue
					}
				}

				cc.recorder.Eventf(template, corev1.EventTypeNormal, "HardExpiredPodDeleted",
					"Deleted hard-expired pod %s", pod.Name)
				expiredCount++
				continue
			}
		}

		// Skip paused pods for soft expiry.
		if pod.Annotations[AnnotationPaused] == "true" {
			continue
		}

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
			cc.logger.Info("Requesting pause for expired pod",
				zap.String("pod", pod.Name),
				zap.Time("expiresAt", expiresAt),
			)

			if cc.pauseRequester != nil {
				err := cc.pauseRequester.RequestPauseSandboxByID(ctx, pod.Name)
				if err != nil {
					cc.logger.Error("Failed to request pause for expired pod",
						zap.String("pod", pod.Name),
						zap.Error(err),
					)
					continue
				}

				cc.recorder.Eventf(template, corev1.EventTypeNormal, "ExpiredPodPauseRequested",
					"Requested pause for expired pod %s", pod.Name)
				expiredCount++
			} else {
				cc.logger.Warn("Sandbox pause requester not configured, skipping pause request for expired pod",
					zap.String("pod", pod.Name),
				)
			}
		}
	}

	if expiredCount > 0 {
		cc.logger.Info("Processed expired pods",
			zap.String("template", template.Name),
			zap.Int("count", expiredCount),
		)
	}

	return nil
}
