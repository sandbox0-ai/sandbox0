package reconciler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/pkg/clock"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SingleClusterReconciler syncs templates from the store to the local cluster.
type SingleClusterReconciler struct {
	templateStore    TemplateStore
	applier          TemplateApplier
	clusterID        string
	logger           *zap.Logger
	interval         time.Duration
	clock            *clock.Clock
	lastReconcileAt  time.Time
	lastReconcileErr error
	statusMu         sync.RWMutex
}

// NewSingleClusterReconciler creates a new SingleClusterReconciler.
func NewSingleClusterReconciler(
	templateStore TemplateStore,
	applier TemplateApplier,
	clusterID string,
	interval time.Duration,
	clk *clock.Clock,
	logger *zap.Logger,
) *SingleClusterReconciler {
	if clusterID == "" {
		clusterID = naming.DefaultClusterID
	}
	return &SingleClusterReconciler{
		templateStore: templateStore,
		applier:       applier,
		clusterID:     clusterID,
		interval:      interval,
		clock:         clk,
		logger:        logger,
	}
}

// Start starts the reconciliation loop.
func (r *SingleClusterReconciler) Start(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Reconciler stopped")
			return
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

// TriggerReconcile triggers an immediate reconciliation.
func (r *SingleClusterReconciler) TriggerReconcile(ctx context.Context) {
	go r.reconcile(ctx)
}

// GetStatus returns the current reconciler status.
func (r *SingleClusterReconciler) GetStatus() (lastReconcile time.Time, lastError error) {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()
	return r.lastReconcileAt, r.lastReconcileErr
}

func (r *SingleClusterReconciler) now() time.Time {
	if r.clock != nil {
		return r.clock.Now()
	}
	return time.Now()
}

func (r *SingleClusterReconciler) reconcile(ctx context.Context) {
	start := r.now()
	defer func() {
		r.statusMu.Lock()
		r.lastReconcileAt = r.now()
		r.statusMu.Unlock()
		r.logger.Info("Single-cluster reconciliation completed",
			zap.Duration("duration", time.Since(start)),
		)
	}()

	templates, err := r.templateStore.ListTemplates(ctx)
	if err != nil {
		r.logger.Error("Failed to list templates", zap.Error(err))
		r.statusMu.Lock()
		r.lastReconcileErr = err
		r.statusMu.Unlock()
		return
	}

	expected := make(map[string]bool, len(templates))
	for _, tpl := range templates {
		clusterName := naming.TemplateNameForCluster(tpl.Scope, tpl.TeamID, tpl.TemplateID)
		expected[clusterName] = true

		clusterSpec := tpl.Spec
		clusterSpec.ClusterId = &r.clusterID

		crd := &v1alpha1.SandboxTemplate{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1alpha1.SchemeGroupVersion.String(),
				Kind:       "SandboxTemplate",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: clusterName,
				Labels: map[string]string{
					"sandbox0.ai/template-scope":      tpl.Scope,
					"sandbox0.ai/template-logical-id": tpl.TemplateID,
				},
				Annotations: map[string]string{
					"sandbox0.ai/template-team-id": tpl.TeamID,
					"sandbox0.ai/template-user-id": tpl.UserID,
				},
			},
			Spec: clusterSpec,
		}

		if err := r.createOrUpdateTemplate(ctx, crd); err != nil {
			r.logger.Error("Failed to sync template to cluster",
				zap.String("template_id", tpl.TemplateID),
				zap.Error(err),
			)
			r.statusMu.Lock()
			r.lastReconcileErr = err
			r.statusMu.Unlock()
		}
	}

	// Cleanup orphan templates.
	existing, err := r.applier.ListTemplates(ctx)
	if err != nil {
		r.logger.Error("Failed to list templates for cleanup", zap.Error(err))
		return
	}
	orphansRemoved := 0
	for _, tpl := range existing {
		if tpl.Labels == nil || tpl.Labels["sandbox0.ai/template-logical-id"] == "" {
			continue
		}
		if !expected[tpl.Name] {
			if err := r.applier.DeleteTemplate(ctx, tpl.Name); err != nil {
				r.logger.Error("Failed to delete orphan template",
					zap.String("template_id", tpl.Name),
					zap.Error(err),
				)
				continue
			}
			orphansRemoved++
		}
	}

	if orphansRemoved > 0 {
		r.logger.Info("Orphan templates removed",
			zap.Int("count", orphansRemoved),
		)
	}

	r.statusMu.Lock()
	r.lastReconcileErr = nil
	r.statusMu.Unlock()
}

func (r *SingleClusterReconciler) createOrUpdateTemplate(ctx context.Context, tpl *v1alpha1.SandboxTemplate) error {
	existing, err := r.applier.GetTemplate(ctx, tpl.Name)
	if err == nil && existing != nil {
		tpl.ResourceVersion = existing.ResourceVersion
		tpl.Status = existing.Status
		_, err = r.applier.UpdateTemplate(ctx, tpl)
		if err != nil {
			return fmt.Errorf("update template: %w", err)
		}
		return nil
	}

	if err != nil {
		return fmt.Errorf("get template: %w", err)
	}

	if _, err := r.applier.CreateTemplate(ctx, tpl); err != nil {
		return fmt.Errorf("create template: %w", err)
	}
	return nil
}
