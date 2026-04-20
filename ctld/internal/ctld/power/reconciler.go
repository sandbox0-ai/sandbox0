package power

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
)

const (
	powerStateActive   = "active"
	powerStatePaused   = "paused"
	powerPhaseStable   = "stable"
	powerPhasePausing  = "pausing"
	powerPhaseResuming = "resuming"

	defaultPauseMinMemoryRequest  = "10Mi"
	defaultPauseMinMemoryLimit    = "32Mi"
	defaultPauseMemoryBufferRatio = 1.1
	defaultPauseMinCPU            = "10m"
)

type PowerReconcilerConfig struct {
	PauseMinMemoryRequest  string
	PauseMinMemoryLimit    string
	PauseMemoryBufferRatio float64
	PauseMinCPU            string
	DefaultSandboxTTL      time.Duration
}

type PowerReconciler struct {
	k8sClient  kubernetes.Interface
	podLister  corelisters.PodLister
	resolver   *PodResolver
	controller *Controller
	cfg        PowerReconcilerConfig
	queue      workqueue.TypedRateLimitingInterface[string]
}

type powerState struct {
	Desired            string
	DesiredGeneration  int64
	Observed           string
	ObservedGeneration int64
	Phase              string
}

type pausedState struct {
	Resources   map[string]containerResources `json:"resources"`
	OriginalTTL *int32                        `json:"original_ttl,omitempty"`
}

type containerResources struct {
	Requests corev1.ResourceList `json:"requests,omitempty"`
	Limits   corev1.ResourceList `json:"limits,omitempty"`
}

type sandboxConfig struct {
	TTL *int32 `json:"ttl,omitempty"`
}

func NewPowerReconciler(k8sClient kubernetes.Interface, podLister corelisters.PodLister, resolver *PodResolver, controller *Controller, cfg PowerReconcilerConfig) *PowerReconciler {
	cfg = normalizePowerReconcilerConfig(cfg)
	return &PowerReconciler{
		k8sClient:  k8sClient,
		podLister:  podLister,
		resolver:   resolver,
		controller: controller,
		cfg:        cfg,
		queue:      workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
	}
}

func normalizePowerReconcilerConfig(cfg PowerReconcilerConfig) PowerReconcilerConfig {
	if strings.TrimSpace(cfg.PauseMinMemoryRequest) == "" {
		cfg.PauseMinMemoryRequest = defaultPauseMinMemoryRequest
	}
	if strings.TrimSpace(cfg.PauseMinMemoryLimit) == "" {
		cfg.PauseMinMemoryLimit = defaultPauseMinMemoryLimit
	}
	if cfg.PauseMemoryBufferRatio <= 0 {
		cfg.PauseMemoryBufferRatio = defaultPauseMemoryBufferRatio
	}
	if strings.TrimSpace(cfg.PauseMinCPU) == "" {
		cfg.PauseMinCPU = defaultPauseMinCPU
	}
	return cfg
}

func (r *PowerReconciler) EventHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: r.enqueueObject,
		UpdateFunc: func(_, newObj interface{}) {
			r.enqueueObject(newObj)
		},
		DeleteFunc: r.enqueueObject,
	}
}

func (r *PowerReconciler) EnqueueAll() {
	if r == nil || r.podLister == nil {
		return
	}
	pods, err := r.podLister.List(labels.Everything())
	if err != nil {
		log.Printf("ctld power reconciler list pods: %v", err)
		return
	}
	for _, pod := range pods {
		r.enqueuePod(pod)
	}
}

func (r *PowerReconciler) Run(ctx context.Context, workers int) {
	if r == nil || r.queue == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if workers <= 0 {
		workers = 1
	}
	defer r.queue.ShutDown()
	for i := 0; i < workers; i++ {
		go func() {
			for r.processNext(ctx) {
			}
		}()
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.EnqueueAll()
		}
	}
}

func (r *PowerReconciler) processNext(ctx context.Context) bool {
	key, shutdown := r.queue.Get()
	if shutdown {
		return false
	}
	defer r.queue.Done(key)

	if err := r.reconcileKey(ctx, key); err != nil {
		log.Printf("ctld power reconcile failed key=%s err=%v", key, err)
		r.queue.AddRateLimited(key)
		return true
	}
	r.queue.Forget(key)
	return true
}

func (r *PowerReconciler) enqueueObject(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return
	}
	r.enqueuePod(pod)
}

func (r *PowerReconciler) enqueuePod(pod *corev1.Pod) {
	if r == nil || r.queue == nil || pod == nil {
		return
	}
	if pod.Labels[controller.LabelSandboxID] == "" {
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		return
	}
	r.queue.Add(key)
}

func (r *PowerReconciler) reconcileKey(ctx context.Context, key string) error {
	if r == nil || r.podLister == nil {
		return nil
	}
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	pod, err := r.podLister.Pods(namespace).Get(name)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.reconcilePod(ctx, pod.DeepCopy())
}

func (r *PowerReconciler) reconcilePod(ctx context.Context, pod *corev1.Pod) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if pod == nil || pod.Labels[controller.LabelSandboxID] == "" {
		return nil
	}
	if pod.DeletionTimestamp != nil {
		return r.thawDeletingPod(ctx, pod)
	}
	if pod.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		return nil
	}
	state := powerStateFromAnnotations(pod.Annotations)
	if state.Desired == state.Observed && state.Phase == powerPhaseStable {
		return nil
	}
	switch state.Desired {
	case powerStatePaused:
		return r.reconcilePause(ctx, pod, state)
	case powerStateActive:
		return r.reconcileResume(ctx, pod, state)
	default:
		return nil
	}
}

func (r *PowerReconciler) reconcilePause(ctx context.Context, pod *corev1.Pod, state powerState) error {
	target, err := r.resolvePodTarget(pod)
	if err != nil {
		return err
	}
	sandboxID := pod.Labels[controller.LabelSandboxID]
	resp, status := r.controller.PauseTarget(ctx, sandboxID, target)
	if status != http.StatusOK || !resp.Paused {
		return fmt.Errorf("pause target: status=%d error=%s", status, resp.Error)
	}
	requeue, err := r.recordPaused(ctx, pod, state.DesiredGeneration, resp.ResourceUsage)
	if err != nil {
		return err
	}
	if requeue {
		r.enqueuePod(pod)
	}
	return nil
}

func (r *PowerReconciler) reconcileResume(ctx context.Context, pod *corev1.Pod, state powerState) error {
	target, err := r.resolvePodTarget(pod)
	if err != nil {
		return err
	}
	if err := r.restoreResourcesBeforeResume(ctx, pod); err != nil {
		return err
	}
	sandboxID := pod.Labels[controller.LabelSandboxID]
	resp, status := r.controller.ResumeTarget(sandboxID, target)
	if status != http.StatusOK || !resp.Resumed {
		return fmt.Errorf("resume target: status=%d error=%s", status, resp.Error)
	}
	requeue, err := r.recordActive(ctx, pod, state.DesiredGeneration)
	if err != nil {
		return err
	}
	if requeue {
		r.enqueuePod(pod)
	}
	return nil
}

func (r *PowerReconciler) thawDeletingPod(ctx context.Context, pod *corev1.Pod) error {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return nil
	}
	target, err := r.resolvePodTarget(pod)
	if apierrors.IsNotFound(err) || apierrors.IsGone(err) {
		return nil
	}
	if stderrors.Is(err, ErrRuntimeTargetNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	frozen, err := r.controller.FS.IsFrozen(target.CgroupDir)
	if err != nil || !frozen {
		return nil
	}
	sandboxID := pod.Labels[controller.LabelSandboxID]
	resp, status := r.controller.ResumeTarget(sandboxID, target)
	if status != http.StatusOK || !resp.Resumed {
		return fmt.Errorf("thaw deleting pod: status=%d error=%s", status, resp.Error)
	}
	return nil
}

func (r *PowerReconciler) resolvePodTarget(pod *corev1.Pod) (Target, error) {
	if r == nil || r.resolver == nil {
		return Target{}, ErrNotImplemented
	}
	return r.resolver.resolvePodTarget(pod, pod.Labels[controller.LabelSandboxID])
}

func (r *PowerReconciler) recordPaused(ctx context.Context, actionPod *corev1.Pod, observedGeneration int64, usage *ctldapi.SandboxResourceUsage) (bool, error) {
	var requeue bool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := r.k8sClient.CoreV1().Pods(actionPod.Namespace).Get(ctx, actionPod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		updated := current.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = map[string]string{}
		}
		if updated.Annotations[controller.AnnotationPausedState] == "" {
			stateJSON, err := marshalPausedState(actionPod)
			if err != nil {
				return err
			}
			updated.Annotations[controller.AnnotationPausedState] = stateJSON
		}
		updated.Annotations[controller.AnnotationPaused] = "true"
		if updated.Annotations[controller.AnnotationPausedAt] == "" {
			updated.Annotations[controller.AnnotationPausedAt] = time.Now().UTC().Format(time.RFC3339)
		}
		delete(updated.Annotations, controller.AnnotationExpiresAt)

		observed := powerStatePaused
		currentState := powerStateFromAnnotations(updated.Annotations)
		currentState.Observed = observed
		currentState.ObservedGeneration = observedGeneration
		currentState.Phase = phaseFor(currentState.Desired, observed)
		applyPowerStateAnnotations(updated.Annotations, currentState)

		_, err = r.k8sClient.CoreV1().Pods(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		requeue = currentState.Desired != observed || currentState.DesiredGeneration != observedGeneration
		return nil
	})
	if err != nil {
		return false, err
	}
	if err := r.applyPausedResources(ctx, actionPod, usageWorkingSet(usage)); err != nil {
		log.Printf("ctld failed to resize paused sandbox %s/%s: %v", actionPod.Namespace, actionPod.Name, err)
	}
	return requeue, nil
}

func (r *PowerReconciler) recordActive(ctx context.Context, actionPod *corev1.Pod, observedGeneration int64) (bool, error) {
	var requeue bool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := r.k8sClient.CoreV1().Pods(actionPod.Namespace).Get(ctx, actionPod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		updated := current.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = map[string]string{}
		}
		ttl := pausedStateOriginalTTL(updated)
		if ttl == nil && r.cfg.DefaultSandboxTTL > 0 {
			ttlValue := int32(r.cfg.DefaultSandboxTTL.Seconds())
			ttl = &ttlValue
		}
		setExpirationAnnotation(updated.Annotations, time.Now().UTC(), ttl)
		delete(updated.Annotations, controller.AnnotationPaused)
		delete(updated.Annotations, controller.AnnotationPausedAt)
		delete(updated.Annotations, controller.AnnotationPausedState)

		observed := powerStateActive
		currentState := powerStateFromAnnotations(updated.Annotations)
		currentState.Observed = observed
		currentState.ObservedGeneration = observedGeneration
		currentState.Phase = phaseFor(currentState.Desired, observed)
		applyPowerStateAnnotations(updated.Annotations, currentState)

		_, err = r.k8sClient.CoreV1().Pods(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		requeue = currentState.Desired != observed || currentState.DesiredGeneration != observedGeneration
		return nil
	})
	return requeue, err
}

func (r *PowerReconciler) applyPausedResources(ctx context.Context, pod *corev1.Pod, workingSet int64) error {
	if pod == nil {
		return nil
	}
	minCPU := parseQuantityOrDefault(r.cfg.PauseMinCPU, defaultPauseMinCPU)
	var newRequestMemory resource.Quantity
	var newLimitMemory resource.Quantity
	if workingSet > 0 {
		reqBytes := workingSet
		minReq := parseQuantityOrDefault(r.cfg.PauseMinMemoryRequest, defaultPauseMinMemoryRequest)
		if reqBytes < minReq.Value() {
			reqBytes = minReq.Value()
		}
		newRequestMemory = *resource.NewQuantity(reqBytes, resource.BinarySI)

		limitBytes := int64(float64(workingSet) * r.cfg.PauseMemoryBufferRatio)
		minLimit := parseQuantityOrDefault(r.cfg.PauseMinMemoryLimit, defaultPauseMinMemoryLimit)
		if limitBytes < minLimit.Value() {
			limitBytes = minLimit.Value()
		}
		newLimitMemory = *resource.NewQuantity(limitBytes, resource.BinarySI)
	}
	if newLimitMemory.IsZero() && minCPU.IsZero() {
		return nil
	}
	resizePod, err := r.k8sClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if !applyPausedResourceTargets(resizePod, newRequestMemory, newLimitMemory, minCPU) {
		return nil
	}
	_, err = r.k8sClient.CoreV1().Pods(pod.Namespace).UpdateResize(ctx, pod.Name, resizePod, metav1.UpdateOptions{})
	return err
}

func (r *PowerReconciler) restoreResourcesBeforeResume(ctx context.Context, pod *corev1.Pod) error {
	if pod == nil || pod.Annotations[controller.AnnotationPausedState] == "" {
		return nil
	}
	var state pausedState
	if err := json.Unmarshal([]byte(pod.Annotations[controller.AnnotationPausedState]), &state); err != nil || len(state.Resources) == 0 {
		return nil
	}
	resizePod, err := r.k8sClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	changed := false
	for i := range resizePod.Spec.Containers {
		container := &resizePod.Spec.Containers[i]
		orig, ok := state.Resources[container.Name]
		if !ok {
			continue
		}
		container.Resources.Requests = orig.Requests
		container.Resources.Limits = orig.Limits
		changed = true
	}
	if !changed {
		return nil
	}
	_, err = r.k8sClient.CoreV1().Pods(pod.Namespace).UpdateResize(ctx, pod.Name, resizePod, metav1.UpdateOptions{})
	return err
}

func marshalPausedState(pod *corev1.Pod) (string, error) {
	state := pausedState{Resources: map[string]containerResources{}}
	if pod != nil {
		for _, container := range pod.Spec.Containers {
			state.Resources[container.Name] = containerResources{
				Requests: container.Resources.Requests.DeepCopy(),
				Limits:   container.Resources.Limits.DeepCopy(),
			}
		}
		if raw := pod.Annotations[controller.AnnotationConfig]; raw != "" {
			var cfg sandboxConfig
			if err := json.Unmarshal([]byte(raw), &cfg); err == nil {
				state.OriginalTTL = cfg.TTL
			}
		}
	}
	data, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("marshal paused state: %w", err)
	}
	return string(data), nil
}

func pausedStateOriginalTTL(pod *corev1.Pod) *int32 {
	if pod == nil || pod.Annotations[controller.AnnotationPausedState] == "" {
		return nil
	}
	var state pausedState
	if err := json.Unmarshal([]byte(pod.Annotations[controller.AnnotationPausedState]), &state); err != nil {
		return nil
	}
	return state.OriginalTTL
}

func applyPausedResourceTargets(pod *corev1.Pod, newRequestMemory, newLimitMemory, minCPU resource.Quantity) bool {
	if pod == nil {
		return false
	}
	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		if container.Name != "procd" {
			continue
		}
		if container.Resources.Requests == nil {
			container.Resources.Requests = corev1.ResourceList{}
		}
		if !newRequestMemory.IsZero() {
			container.Resources.Requests[corev1.ResourceMemory] = newRequestMemory
		}
		container.Resources.Requests[corev1.ResourceCPU] = minCPU
		if container.Resources.Limits == nil {
			container.Resources.Limits = corev1.ResourceList{}
		}
		if !newLimitMemory.IsZero() {
			container.Resources.Limits[corev1.ResourceMemory] = newLimitMemory
		}
		container.Resources.Limits[corev1.ResourceCPU] = minCPU
		return true
	}
	return false
}

func usageWorkingSet(usage *ctldapi.SandboxResourceUsage) int64 {
	if usage == nil {
		return 0
	}
	return usage.ContainerMemoryWorkingSet
}

func parseQuantityOrDefault(raw, fallback string) resource.Quantity {
	quantity, err := resource.ParseQuantity(raw)
	if err == nil {
		return quantity
	}
	return resource.MustParse(fallback)
}

func powerStateFromAnnotations(annotations map[string]string) powerState {
	legacyObserved := powerStateActive
	if annotations[controller.AnnotationPaused] == "true" {
		legacyObserved = powerStatePaused
	}
	state := powerState{
		Desired:            normalizePowerState(annotations[controller.AnnotationPowerStateDesired], legacyObserved),
		DesiredGeneration:  parseInt64Annotation(annotations, controller.AnnotationPowerStateDesiredGeneration),
		Observed:           normalizePowerState(annotations[controller.AnnotationPowerStateObserved], legacyObserved),
		ObservedGeneration: parseInt64Annotation(annotations, controller.AnnotationPowerStateObservedGeneration),
	}
	state.Phase = normalizePowerPhase(annotations[controller.AnnotationPowerStatePhase], state.Desired, state.Observed)
	return state
}

func normalizePowerState(raw, fallback string) string {
	switch strings.TrimSpace(raw) {
	case powerStateActive:
		return powerStateActive
	case powerStatePaused:
		return powerStatePaused
	default:
		return fallback
	}
}

func normalizePowerPhase(raw, desired, observed string) string {
	switch strings.TrimSpace(raw) {
	case powerPhaseStable:
		return powerPhaseStable
	case powerPhasePausing:
		return powerPhasePausing
	case powerPhaseResuming:
		return powerPhaseResuming
	}
	return phaseFor(desired, observed)
}

func phaseFor(desired, observed string) string {
	if desired == observed {
		return powerPhaseStable
	}
	if desired == powerStatePaused {
		return powerPhasePausing
	}
	return powerPhaseResuming
}

func applyPowerStateAnnotations(annotations map[string]string, state powerState) {
	annotations[controller.AnnotationPowerStateDesired] = state.Desired
	annotations[controller.AnnotationPowerStateDesiredGeneration] = strconv.FormatInt(state.DesiredGeneration, 10)
	annotations[controller.AnnotationPowerStateObserved] = state.Observed
	annotations[controller.AnnotationPowerStateObservedGeneration] = strconv.FormatInt(state.ObservedGeneration, 10)
	annotations[controller.AnnotationPowerStatePhase] = state.Phase
}

func parseInt64Annotation(annotations map[string]string, key string) int64 {
	raw := strings.TrimSpace(annotations[key])
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0
	}
	return value
}

func setExpirationAnnotation(annotations map[string]string, now time.Time, ttl *int32) {
	if ttl == nil || *ttl <= 0 {
		delete(annotations, controller.AnnotationExpiresAt)
		return
	}
	annotations[controller.AnnotationExpiresAt] = now.Add(time.Duration(*ttl) * time.Second).Format(time.RFC3339)
}
