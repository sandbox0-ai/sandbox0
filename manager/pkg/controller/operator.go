package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/manager/pkg/metrics"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const (
	maxRetries = 5
)

// Operator is the main controller for SandboxTemplate CRD
type Operator struct {
	k8sClient   kubernetes.Interface
	podLister   corelisters.PodLister
	podsSynced  cache.InformerSynced
	poolManager *PoolManager
	autoScaler  *AutoScaler
	recorder    record.EventRecorder
	logger      *zap.Logger

	workqueue workqueue.RateLimitingInterface

	// Template informer and lister (to be injected)
	templateInformer cache.SharedIndexInformer
	templateLister   TemplateListerImpl
}

// TemplateListerImpl implements TemplateLister
type TemplateListerImpl struct {
	indexer cache.Indexer
}

// List lists all SandboxTemplates
func (t *TemplateListerImpl) List() ([]*v1alpha1.SandboxTemplate, error) {
	var templates []*v1alpha1.SandboxTemplate
	for _, obj := range t.indexer.List() {
		template := obj.(*v1alpha1.SandboxTemplate)
		templates = append(templates, template)
	}
	return templates, nil
}

// Get gets a SandboxTemplate by namespace and name
func (t *TemplateListerImpl) Get(namespace, name string) (*v1alpha1.SandboxTemplate, error) {
	key := namespace + "/" + name
	obj, exists, err := t.indexer.GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1alpha1.Resource("sandboxtemplate"), name)
	}
	return obj.(*v1alpha1.SandboxTemplate), nil
}

// NewOperator creates a new Operator
func NewOperator(
	k8sClient kubernetes.Interface,
	podInformer cache.SharedIndexInformer,
	templateInformer cache.SharedIndexInformer,
	recorder record.EventRecorder,
	logger *zap.Logger,
) *Operator {
	podLister := corelisters.NewPodLister(podInformer.GetIndexer())
	poolManager := NewPoolManager(k8sClient, podLister, recorder, logger)
	autoScaler := NewAutoScaler(k8sClient, podLister, logger)

	op := &Operator{
		k8sClient:        k8sClient,
		podLister:        podLister,
		podsSynced:       podInformer.HasSynced,
		poolManager:      poolManager,
		autoScaler:       autoScaler,
		recorder:         recorder,
		logger:           logger,
		workqueue:        workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		templateInformer: templateInformer,
		templateLister: TemplateListerImpl{
			indexer: templateInformer.GetIndexer(),
		},
	}

	// Setup event handlers for SandboxTemplate
	templateInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    op.handleTemplateAdd,
		UpdateFunc: op.handleTemplateUpdate,
		DeleteFunc: op.handleTemplateDelete,
	})

	return op
}

// Run starts the operator
func (op *Operator) Run(ctx context.Context, workers int) error {
	defer runtime.HandleCrash()
	defer op.workqueue.ShutDown()

	op.logger.Info("Starting operator")

	// Wait for cache sync
	op.logger.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), op.podsSynced, op.templateInformer.HasSynced) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	op.logger.Info("Starting workers", zap.Int("count", workers))
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, op.runWorker, time.Second)
	}

	op.logger.Info("Operator started")
	<-ctx.Done()
	op.logger.Info("Shutting down operator")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the workqueue
func (op *Operator) runWorker(ctx context.Context) {
	for op.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler
func (op *Operator) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := op.workqueue.Get()
	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer op.workqueue.Done(obj)

		key, ok := obj.(string)
		if !ok {
			op.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		if err := op.syncHandler(ctx, key); err != nil {
			// Requeue the item if there's an error
			if op.workqueue.NumRequeues(key) < maxRetries {
				op.logger.Error("Error syncing template, requeueing",
					zap.String("key", key),
					zap.Error(err),
				)
				op.workqueue.AddRateLimited(key)
				return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
			}

			// Drop the item after max retries
			op.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("dropping template %q out of the queue: %v", key, err))
			return nil
		}

		op.workqueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

// syncHandler reconciles a single SandboxTemplate
func (op *Operator) syncHandler(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the template
	template, err := op.templateLister.Get(namespace, name)
	if err != nil {
		if errors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("template '%s' in work queue no longer exists", key))
			return nil
		}
		return err
	}

	op.logger.Debug("Reconciling template", zap.String("name", name))

	// Reconcile the pool (ReplicaSet)
	if err := op.poolManager.ReconcilePool(ctx, template); err != nil {
		return fmt.Errorf("reconcile pool: %w", err)
	}

	// Update status
	if err := op.updateTemplateStatus(ctx, template); err != nil {
		return fmt.Errorf("update status: %w", err)
	}

	// Autoscale (may update template.spec.pool.minIdle, which will trigger another reconcile)
	if template.Spec.Pool.AutoScale && op.autoScaler != nil {
		if err := op.autoScaler.ReconcileAutoScale(ctx, template, time.Now()); err != nil {
			op.logger.Warn("Autoscale reconcile failed",
				zap.String("template", template.Name),
				zap.String("namespace", template.Namespace),
				zap.Error(err),
			)
			// Don't fail the reconcile; pool + status are still correct.
		}
	}

	return nil
}

// updateTemplateStatus updates the status of a SandboxTemplate
func (op *Operator) updateTemplateStatus(ctx context.Context, template *v1alpha1.SandboxTemplate) error {
	// Get idle pods
	idlePods, err := op.podLister.Pods(template.ObjectMeta.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.ObjectMeta.Name,
		LabelPoolType:   PoolTypeIdle,
	}))
	if err != nil {
		return err
	}

	// Get active pods
	activePods, err := op.podLister.Pods(template.ObjectMeta.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.ObjectMeta.Name,
		LabelPoolType:   PoolTypeActive,
	}))
	if err != nil {
		return err
	}

	// Count running pods only
	idleCount := int32(0)
	for _, pod := range idlePods {
		if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
			idleCount++
		}
	}

	activeCount := int32(0)
	for _, pod := range activePods {
		if pod.Status.Phase == corev1.PodRunning {
			activeCount++
		}
	}

	metrics.IdlePodsTotal.WithLabelValues(template.Name).Set(float64(idleCount))
	metrics.ActivePodsTotal.WithLabelValues(template.Name).Set(float64(activeCount))

	// Update status if changed
	if template.Status.IdleCount != idleCount || template.Status.ActiveCount != activeCount {
		template.Status.IdleCount = idleCount
		template.Status.ActiveCount = activeCount
		template.Status.LastUpdateTime = metav1.Now()

		// Update conditions
		template.Status.Conditions = op.computeConditions(template, idleCount, activeCount)

		// Note: In a real implementation, we should use a status subresource update
		// For now, we'll just log the status
		op.logger.Info("Template status updated",
			zap.String("template", template.ObjectMeta.Name),
			zap.Int32("idle", idleCount),
			zap.Int32("active", activeCount),
		)
	}

	return nil
}

// computeConditions computes the conditions for a template
func (op *Operator) computeConditions(template *v1alpha1.SandboxTemplate, idleCount, activeCount int32) []v1alpha1.SandboxTemplateCondition {
	conditions := []v1alpha1.SandboxTemplateCondition{}

	// Ready condition
	readyStatus := v1alpha1.ConditionTrue
	readyReason := "PoolReady"
	readyMessage := "Pool is ready"
	if idleCount < template.Spec.Pool.MinIdle {
		readyStatus = v1alpha1.ConditionFalse
		readyReason = "InsufficientIdlePods"
		readyMessage = fmt.Sprintf("Idle pod count (%d) is less than minIdle (%d)", idleCount, template.Spec.Pool.MinIdle)
	}

	conditions = append(conditions, v1alpha1.SandboxTemplateCondition{
		Type:               v1alpha1.SandboxTemplateReady,
		Status:             readyStatus,
		LastTransitionTime: metav1.Now(),
		Reason:             readyReason,
		Message:            readyMessage,
	})

	// PoolHealthy condition
	healthyStatus := v1alpha1.ConditionTrue
	healthyReason := "PoolHealthy"
	healthyMessage := "Pool is healthy"
	if idleCount > template.Spec.Pool.MaxIdle {
		healthyStatus = v1alpha1.ConditionFalse
		healthyReason = "ExcessIdlePods"
		healthyMessage = fmt.Sprintf("Idle pod count (%d) exceeds maxIdle (%d)", idleCount, template.Spec.Pool.MaxIdle)
	}

	conditions = append(conditions, v1alpha1.SandboxTemplateCondition{
		Type:               v1alpha1.SandboxTemplatePoolHealthy,
		Status:             healthyStatus,
		LastTransitionTime: metav1.Now(),
		Reason:             healthyReason,
		Message:            healthyMessage,
	})

	return conditions
}

// Event handlers

func (op *Operator) handleTemplateAdd(obj interface{}) {
	template := obj.(*v1alpha1.SandboxTemplate)
	op.logger.Info("Template added", zap.String("name", template.ObjectMeta.Name))
	op.enqueueTemplate(template)
}

func (op *Operator) handleTemplateUpdate(oldObj, newObj interface{}) {
	oldTemplate := oldObj.(*v1alpha1.SandboxTemplate)
	newTemplate := newObj.(*v1alpha1.SandboxTemplate)

	if oldTemplate.ObjectMeta.ResourceVersion == newTemplate.ObjectMeta.ResourceVersion {
		return
	}

	op.logger.Info("Template updated", zap.String("name", newTemplate.Name))
	op.enqueueTemplate(newTemplate)
}

func (op *Operator) handleTemplateDelete(obj interface{}) {
	template, ok := obj.(*v1alpha1.SandboxTemplate)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		template, ok = tombstone.Obj.(*v1alpha1.SandboxTemplate)
		if !ok {
			runtime.HandleError(fmt.Errorf("tombstone contained object that is not a SandboxTemplate %#v", obj))
			return
		}
	}

	op.logger.Info("Template deleted", zap.String("name", template.ObjectMeta.Name))
	// Cleanup is handled by owner references
}

func (op *Operator) enqueueTemplate(template *v1alpha1.SandboxTemplate) {
	key, err := cache.MetaNamespaceKeyFunc(template)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	op.workqueue.Add(key)
}

// GetTemplateLister returns the template lister
func (op *Operator) GetTemplateLister() TemplateLister {
	return &op.templateLister
}
