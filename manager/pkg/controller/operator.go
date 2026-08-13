package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	managerconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/carrierpool"
	clientset "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/namespacepolicy"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fsrollout"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
)

const (
	maxRetries = 5
)

// Operator is the main controller for SandboxTemplate CRD
type Operator struct {
	k8sClient      kubernetes.Interface
	crdClient      clientset.Interface
	podLister      corelisters.PodLister
	podsSynced     cache.InformerSynced
	poolManager    *PoolManager
	recorder       record.EventRecorder
	clock          TimeProvider
	logger         *zap.Logger
	statsPublisher TemplateStatsPublisher

	workqueue workqueue.TypedRateLimitingInterface[string]

	metrics *obsmetrics.ManagerMetrics

	namespacePolicy namespacepolicy.TemplateNamespaceReconciler

	// Template informer and lister (to be injected)
	templateInformer cache.SharedIndexInformer
	templateLister   TemplateListerImpl

	statsMu   sync.Mutex
	lastStats map[string]TemplateCounts
}

// SetNamespacePolicyReconciler installs the manager-owned template namespace baseline reconciler.
func (op *Operator) SetNamespacePolicyReconciler(reconciler namespacepolicy.TemplateNamespaceReconciler) {
	op.namespacePolicy = reconciler
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
	crdClient clientset.Interface,
	podInformer cache.SharedIndexInformer,
	replicaSetInformer cache.SharedIndexInformer,
	secretInformer cache.SharedIndexInformer,
	templateInformer cache.SharedIndexInformer,
	recorder record.EventRecorder,
	clock TimeProvider,
	logger *zap.Logger,
	metrics *obsmetrics.ManagerMetrics,
	teardown *PodTeardownCoordinator,
	autoscalerAnnotationKeys []string,
) *Operator {
	// Use system time as fallback if clock is nil
	if clock == nil {
		clock = systemTime{}
	}

	podLister := corelisters.NewPodLister(podInformer.GetIndexer())
	replicaSetLister := appslisters.NewReplicaSetLister(replicaSetInformer.GetIndexer())
	secretLister := corelisters.NewSecretLister(secretInformer.GetIndexer())
	poolManager := NewPoolManager(k8sClient, podLister, replicaSetLister, secretLister, recorder, logger, teardown, autoscalerAnnotationKeys)

	op := &Operator{
		k8sClient:        k8sClient,
		crdClient:        crdClient,
		podLister:        podLister,
		podsSynced:       podInformer.HasSynced,
		poolManager:      poolManager,
		recorder:         recorder,
		clock:            clock,
		logger:           logger,
		workqueue:        workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		metrics:          metrics,
		templateInformer: templateInformer,
		templateLister: TemplateListerImpl{
			indexer: templateInformer.GetIndexer(),
		},
		lastStats: make(map[string]TemplateCounts),
	}

	// Setup event handlers for SandboxTemplate
	templateInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    op.handleTemplateAdd,
		UpdateFunc: op.handleTemplateUpdate,
		DeleteFunc: op.handleTemplateDelete,
	})

	// Setup event handlers for Pods to refresh template stats on pod changes.
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    op.handlePodAdd,
		UpdateFunc: op.handlePodUpdate,
		DeleteFunc: op.handlePodDelete,
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
	key, shutdown := op.workqueue.Get()
	if shutdown {
		return false
	}

	err := func(key string) error {
		defer op.workqueue.Done(key)

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
			op.workqueue.Forget(key)
			runtime.HandleError(fmt.Errorf("dropping template %q out of the queue: %v", key, err))
			return nil
		}

		op.workqueue.Forget(key)
		return nil
	}(key)

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
	if op.namespacePolicy != nil {
		if err := op.namespacePolicy.EnsureBaseline(ctx, template.Namespace); err != nil {
			return fmt.Errorf("reconcile template namespace baseline: %w", err)
		}
	}

	poolTemplate := template
	poolMode := v1alpha1.SandboxTemplatePoolModeLegacy
	if admissionMode, admitted := templateS0FSAdmissionMode(template); admitted {
		poolTemplate = template.DeepCopy()
		poolTemplate.Spec.Pool.MinIdle = 0
		poolTemplate.Spec.Pool.MaxIdle = 0
		poolMode = v1alpha1.SandboxTemplatePoolModeCold
		if admissionMode == s0fsrollout.AdmissionModeShared {
			if compatible, _ := carrierpool.Compatible(template); compatible {
				poolMode = v1alpha1.SandboxTemplatePoolModeShared
			}
		}
	}
	// During migration, a zero-sized legacy ReplicaSet drains old idle Pods
	// without letting the shared cohort create template-owned capacity.
	poolRequeueAfter, err := op.poolManager.ReconcilePool(ctx, poolTemplate)
	if err != nil {
		return fmt.Errorf("reconcile pool: %w", err)
	}

	// Update status
	if err := op.updateTemplateStatus(ctx, template, poolMode); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	requeueAfter := poolRequeueAfter
	if requeueAfter > 0 {
		op.workqueue.AddAfter(key, requeueAfter)
	}

	return nil
}

// updateTemplateStatus updates the status of a SandboxTemplate
func (op *Operator) updateTemplateStatus(ctx context.Context, template *v1alpha1.SandboxTemplate, poolModes ...v1alpha1.SandboxTemplatePoolMode) error {
	poolMode := v1alpha1.SandboxTemplatePoolModeLegacy
	if len(poolModes) > 0 && poolModes[0] != "" {
		poolMode = poolModes[0]
	}
	// Get idle pods
	idlePods, err := op.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeIdle,
	}))
	if err != nil {
		return err
	}

	// Get active pods
	activePods, err := op.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeActive,
	}))
	if err != nil {
		return err
	}

	// Count only ready idle pods as available pooled capacity.
	idleCount := int32(0)
	reservedActiveCount := int32(0)
	for _, pod := range idlePods {
		if IsHotClaimReservedPod(pod) {
			if pod.Status.Phase == corev1.PodRunning {
				reservedActiveCount++
			}
			continue
		}
		if IsPodReady(pod) {
			idleCount++
		}
	}

	activeCount := reservedActiveCount
	for _, pod := range activePods {
		if pod.Status.Phase == corev1.PodRunning {
			activeCount++
		}
	}

	if op.metrics != nil {
		op.metrics.IdlePodsTotal.WithLabelValues(template.Name).Set(float64(idleCount))
		op.metrics.ActivePodsTotal.WithLabelValues(template.Name).Set(float64(activeCount))
	}

	// Publish stats if changed.
	statsKey := template.Namespace + "/" + template.Name
	shouldPublish := false
	op.statsMu.Lock()
	last := op.lastStats[statsKey]
	if last.IdleCount != idleCount || last.ActiveCount != activeCount {
		op.lastStats[statsKey] = TemplateCounts{
			IdleCount:   idleCount,
			ActiveCount: activeCount,
		}
		shouldPublish = true
	}
	op.statsMu.Unlock()

	if shouldPublish && op.statsPublisher != nil {
		if err := op.statsPublisher.PublishTemplateStats(ctx, template, idleCount, activeCount); err != nil {
			op.logger.Warn("Failed to publish template stats",
				zap.String("template", template.Name),
				zap.Error(err),
			)
		}
	}

	if err := op.persistTemplateStatus(ctx, template.Namespace, template.Name, idleCount, activeCount, poolMode); err != nil {
		return err
	}

	return nil
}

// persistTemplateStatus writes pool observations without mutating informer
// objects or overwriting status owned by another controller.
func (op *Operator) persistTemplateStatus(
	ctx context.Context,
	namespace string,
	name string,
	idleCount int32,
	activeCount int32,
	poolMode v1alpha1.SandboxTemplatePoolMode,
) error {
	if op.crdClient == nil {
		return fmt.Errorf("sandbox template client is not configured")
	}

	templates := op.crdClient.Sandbox0V1alpha1().SandboxTemplates(namespace)
	updated := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := templates.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		conditions := op.computeConditions(current, idleCount, activeCount)
		preserveConditionTransitionTimes(current.Status.Conditions, conditions)
		if current.Status.IdleCount == idleCount &&
			current.Status.ActiveCount == activeCount &&
			current.Status.PoolMode == poolMode &&
			templateConditionsEqual(current.Status.Conditions, conditions) {
			return nil
		}

		next := current.DeepCopy()
		next.Status.IdleCount = idleCount
		next.Status.ActiveCount = activeCount
		next.Status.PoolMode = poolMode
		next.Status.Conditions = conditions
		next.Status.LastUpdateTime = metav1.Now()
		if _, err := templates.UpdateStatus(ctx, next, metav1.UpdateOptions{}); err != nil {
			return err
		}
		updated = true
		return nil
	})
	if err != nil {
		return err
	}
	if updated {
		op.logger.Info("Template status updated",
			zap.String("template", name),
			zap.Int32("idle", idleCount),
			zap.Int32("active", activeCount),
		)
	}
	return nil
}

func templateS0FSAdmissionMode(template *v1alpha1.SandboxTemplate) (s0fsrollout.AdmissionMode, bool) {
	cfg := managerconfig.LoadManagerConfig()
	if cfg == nil || !cfg.S0FSRuntimeEnabled() || template == nil ||
		template.Status.ImageRevision == nil ||
		template.Status.ImageRevision.State != v1alpha1.TemplateImageRevisionStateReady ||
		strings.TrimSpace(template.Status.ImageRevision.ImageFSHeadID) == "" {
		return "", false
	}
	admission, err := cfg.S0FSAdmission()
	if err != nil {
		return "", false
	}
	scope := ""
	logicalTemplateID := template.Name
	teamID := ""
	if template.Labels != nil {
		scope = strings.TrimSpace(template.Labels["sandbox0.ai/template-scope"])
		if value := strings.TrimSpace(template.Labels["sandbox0.ai/template-logical-id"]); value != "" {
			logicalTemplateID = value
		}
	}
	if template.Annotations != nil {
		teamID = strings.TrimSpace(template.Annotations["sandbox0.ai/template-team-id"])
	}
	if !admission.Admits(scope, teamID, logicalTemplateID) {
		return "", false
	}
	return admission.Mode(), true
}

func preserveConditionTransitionTimes(
	current []v1alpha1.SandboxTemplateCondition,
	next []v1alpha1.SandboxTemplateCondition,
) {
	for i := range next {
		for _, condition := range current {
			if condition.Type == next[i].Type &&
				condition.Status == next[i].Status &&
				!condition.LastTransitionTime.IsZero() {
				next[i].LastTransitionTime = condition.LastTransitionTime
				break
			}
		}
	}
}

func templateConditionsEqual(
	left []v1alpha1.SandboxTemplateCondition,
	right []v1alpha1.SandboxTemplateCondition,
) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
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

func (op *Operator) handleTemplateAdd(obj any) {
	template := obj.(*v1alpha1.SandboxTemplate)
	op.logger.Info("Template added", zap.String("name", template.Name))
	op.enqueueTemplate(template)
}

func (op *Operator) handleTemplateUpdate(oldObj, newObj any) {
	oldTemplate := oldObj.(*v1alpha1.SandboxTemplate)
	newTemplate := newObj.(*v1alpha1.SandboxTemplate)

	if oldTemplate.ResourceVersion == newTemplate.ResourceVersion {
		return
	}

	op.logger.Info("Template updated", zap.String("name", newTemplate.Name))
	op.enqueueTemplate(newTemplate)
}

func (op *Operator) handleTemplateDelete(obj any) {
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

	op.logger.Info("Template deleted", zap.String("name", template.Name))
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

func (op *Operator) enqueueTemplateKey(namespace, name string) {
	key := namespace + "/" + name
	op.workqueue.Add(key)
}

func (op *Operator) handlePodAdd(obj any) {
	pod := obj.(*corev1.Pod)
	op.enqueueTemplateForPod(pod)
}

func (op *Operator) handlePodUpdate(oldObj, newObj any) {
	oldPod := oldObj.(*corev1.Pod)
	newPod := newObj.(*corev1.Pod)
	if oldPod.ResourceVersion == newPod.ResourceVersion {
		return
	}
	if !podUpdateRequiresPoolReconcile(oldPod, newPod) {
		return
	}
	if oldPod.Namespace != newPod.Namespace || oldPod.Labels[LabelTemplateID] != newPod.Labels[LabelTemplateID] {
		op.enqueueTemplateForPod(oldPod)
	}
	op.enqueueTemplateForPod(newPod)
}

func podUpdateRequiresPoolReconcile(oldPod, newPod *corev1.Pod) bool {
	if oldPod == nil || newPod == nil {
		return true
	}
	if oldPod.Namespace != newPod.Namespace ||
		oldPod.Labels[LabelTemplateID] != newPod.Labels[LabelTemplateID] ||
		oldPod.Labels[LabelPoolType] != newPod.Labels[LabelPoolType] ||
		oldPod.Spec.NodeName != newPod.Spec.NodeName ||
		oldPod.Status.Phase != newPod.Status.Phase ||
		IsPodReady(oldPod) != IsPodReady(newPod) ||
		IsHotClaimReservedPod(oldPod) != IsHotClaimReservedPod(newPod) ||
		oldPod.Annotations[AnnotationTemplateSpecHash] != newPod.Annotations[AnnotationTemplateSpecHash] {
		return true
	}
	oldDeleting := oldPod.DeletionTimestamp != nil
	newDeleting := newPod.DeletionTimestamp != nil
	return oldDeleting != newDeleting
}

func (op *Operator) handlePodDelete(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("couldn't get pod from tombstone %#v", obj))
			return
		}
		pod, ok = tombstone.Obj.(*corev1.Pod)
		if !ok {
			runtime.HandleError(fmt.Errorf("tombstone contained object that is not a Pod %#v", obj))
			return
		}
	}
	op.enqueueTemplateForPod(pod)
}

func (op *Operator) enqueueTemplateForPod(pod *corev1.Pod) {
	if pod == nil || pod.Labels == nil {
		return
	}
	templateID := pod.Labels[LabelTemplateID]
	if templateID == "" {
		return
	}
	op.enqueueTemplateKey(pod.Namespace, templateID)
}

// GetTemplateLister returns the template lister
func (op *Operator) GetTemplateLister() TemplateLister {
	return &op.templateLister
}

// SetTemplateStatsPublisher injects a stats publisher (optional).
func (op *Operator) SetTemplateStatsPublisher(publisher TemplateStatsPublisher) {
	op.statsPublisher = publisher
}
