// Package watcher provides Kubernetes resource watchers for netd.
package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// SandboxInfo contains sandbox identity information
type SandboxInfo struct {
	SandboxID string
	TeamID    string
	PodName   string
	Namespace string
	PodIP     string
	HostIP    string
	NodeName  string
	VethName  string
	IfIndex   int
	IsActive  bool
}

// PolicyCache holds cached network and bandwidth policies parsed from pod annotations
type PolicyCache struct {
	mu                sync.RWMutex
	networkPolicies   map[string]*v1alpha1.NetworkPolicySpec   // sandboxID -> policy spec
	bandwidthPolicies map[string]*v1alpha1.BandwidthPolicySpec // sandboxID -> policy spec
}

// Watcher watches Kubernetes resources for netd
type Watcher struct {
	k8sClient       kubernetes.Interface
	informerFactory informers.SharedInformerFactory
	nodeName        string
	namespace       string
	logger          *zap.Logger

	// Pod mapping: podIP -> SandboxInfo
	podMappingMu sync.RWMutex
	podMapping   map[string]*SandboxInfo

	// Policy cache (parsed from pod annotations)
	policyCache *PolicyCache

	// Event handlers
	onPodAdd                func(*SandboxInfo)
	onPodUpdate             func(*SandboxInfo, *SandboxInfo)
	onPodDelete             func(*SandboxInfo)
	onNetworkPolicyChange   func(sandboxID string, policy *v1alpha1.NetworkPolicySpec)
	onBandwidthPolicyChange func(sandboxID string, policy *v1alpha1.BandwidthPolicySpec)
}

// NewWatcher creates a new Watcher
func NewWatcher(
	k8sClient kubernetes.Interface,
	nodeName string,
	namespace string,
	resyncPeriod time.Duration,
	logger *zap.Logger,
) *Watcher {
	// Create informer factory for the specific namespace or all namespaces
	var informerFactory informers.SharedInformerFactory
	if namespace != "" {
		informerFactory = informers.NewSharedInformerFactoryWithOptions(
			k8sClient,
			resyncPeriod,
			informers.WithNamespace(namespace),
		)
	} else {
		informerFactory = informers.NewSharedInformerFactory(k8sClient, resyncPeriod)
	}

	return &Watcher{
		k8sClient:       k8sClient,
		informerFactory: informerFactory,
		nodeName:        nodeName,
		namespace:       namespace,
		logger:          logger,
		podMapping:      make(map[string]*SandboxInfo),
		policyCache: &PolicyCache{
			networkPolicies:   make(map[string]*v1alpha1.NetworkPolicySpec),
			bandwidthPolicies: make(map[string]*v1alpha1.BandwidthPolicySpec),
		},
	}
}

// SetPodEventHandlers sets the event handlers for pod events
func (w *Watcher) SetPodEventHandlers(
	onAdd func(*SandboxInfo),
	onUpdate func(*SandboxInfo, *SandboxInfo),
	onDelete func(*SandboxInfo),
) {
	w.onPodAdd = onAdd
	w.onPodUpdate = onUpdate
	w.onPodDelete = onDelete
}

// SetNetworkPolicyHandler sets the handler for network policy changes
func (w *Watcher) SetNetworkPolicyHandler(handler func(sandboxID string, policy *v1alpha1.NetworkPolicySpec)) {
	w.onNetworkPolicyChange = handler
}

// SetBandwidthPolicyHandler sets the handler for bandwidth policy changes
func (w *Watcher) SetBandwidthPolicyHandler(handler func(sandboxID string, policy *v1alpha1.BandwidthPolicySpec)) {
	w.onBandwidthPolicyChange = handler
}

// Start starts the watcher
func (w *Watcher) Start(ctx context.Context) error {
	// Setup pod informer
	podInformer := w.informerFactory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.handlePodAdd,
		UpdateFunc: w.handlePodUpdate,
		DeleteFunc: w.handlePodDelete,
	})

	// Start informers
	w.informerFactory.Start(ctx.Done())

	// Wait for cache sync
	w.logger.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		return nil
	}

	w.logger.Info("Watcher started and caches synced")
	return nil
}

// handlePodAdd handles pod add events
func (w *Watcher) handlePodAdd(obj any) {
	pod := obj.(*corev1.Pod)
	info := w.podToSandboxInfo(pod)
	if info == nil {
		return // Not a managed sandbox pod
	}

	// Only handle pods on this node that are active
	if info.NodeName != w.nodeName || !info.IsActive {
		return
	}

	w.podMappingMu.Lock()
	w.podMapping[info.PodIP] = info
	w.podMappingMu.Unlock()

	// Parse and cache policies from pod annotations
	w.updatePoliciesFromPod(pod, info.SandboxID)

	w.logger.Info("Sandbox pod added",
		zap.String("sandboxID", info.SandboxID),
		zap.String("podIP", info.PodIP),
		zap.String("podName", info.PodName),
	)

	if w.onPodAdd != nil {
		w.onPodAdd(info)
	}
}

// handlePodUpdate handles pod update events
func (w *Watcher) handlePodUpdate(oldObj, newObj any) {
	oldPod := oldObj.(*corev1.Pod)
	newPod := newObj.(*corev1.Pod)

	oldInfo := w.podToSandboxInfo(oldPod)
	newInfo := w.podToSandboxInfo(newPod)

	if newInfo == nil {
		return
	}

	// Only handle pods on this node
	if newInfo.NodeName != w.nodeName {
		return
	}

	// Check if pod became active
	wasActive := oldInfo != nil && oldInfo.IsActive
	isActive := newInfo.IsActive

	w.podMappingMu.Lock()
	if isActive {
		w.podMapping[newInfo.PodIP] = newInfo
	} else if wasActive && !isActive {
		delete(w.podMapping, newInfo.PodIP)
	}
	w.podMappingMu.Unlock()

	// Check if policies changed (by comparing annotation values)
	if isActive {
		oldNetworkPolicy := ""
		oldBandwidthPolicy := ""
		if oldPod.Annotations != nil {
			oldNetworkPolicy = oldPod.Annotations[controller.AnnotationNetworkPolicy]
			oldBandwidthPolicy = oldPod.Annotations[controller.AnnotationBandwidthPolicy]
		}
		newNetworkPolicy := ""
		newBandwidthPolicy := ""
		if newPod.Annotations != nil {
			newNetworkPolicy = newPod.Annotations[controller.AnnotationNetworkPolicy]
			newBandwidthPolicy = newPod.Annotations[controller.AnnotationBandwidthPolicy]
		}

		// Update policies if they changed
		if oldNetworkPolicy != newNetworkPolicy || oldBandwidthPolicy != newBandwidthPolicy {
			w.updatePoliciesFromPod(newPod, newInfo.SandboxID)
		}
	}

	if w.onPodUpdate != nil && (wasActive || isActive) {
		w.onPodUpdate(oldInfo, newInfo)
	}
}

// handlePodDelete handles pod delete events
func (w *Watcher) handlePodDelete(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		pod, ok = tombstone.Obj.(*corev1.Pod)
		if !ok {
			return
		}
	}

	info := w.podToSandboxInfo(pod)
	if info == nil {
		return
	}

	if info.NodeName != w.nodeName {
		return
	}

	w.podMappingMu.Lock()
	delete(w.podMapping, info.PodIP)
	w.podMappingMu.Unlock()

	// Remove policies from cache
	w.removePolicies(info.SandboxID)

	w.logger.Info("Sandbox pod deleted",
		zap.String("sandboxID", info.SandboxID),
		zap.String("podIP", info.PodIP),
	)

	if w.onPodDelete != nil {
		w.onPodDelete(info)
	}
}

// podToSandboxInfo converts a Pod to SandboxInfo
func (w *Watcher) podToSandboxInfo(pod *corev1.Pod) *SandboxInfo {
	// Check if this is a sandbox pod (has sandbox-id label)
	sandboxID, ok := pod.Labels[controller.LabelSandboxID]
	if !ok || sandboxID == "" {
		return nil
	}

	// Check pool type to determine if active
	poolType := pod.Labels[controller.LabelPoolType]
	isActive := poolType == controller.PoolTypeActive

	// Get team ID from annotation
	teamID := pod.Annotations[controller.AnnotationTeamID]

	return &SandboxInfo{
		SandboxID: sandboxID,
		TeamID:    teamID,
		PodName:   pod.Name,
		Namespace: pod.Namespace,
		PodIP:     pod.Status.PodIP,
		HostIP:    pod.Status.HostIP,
		NodeName:  pod.Spec.NodeName,
		IsActive:  isActive,
	}
}

// updatePoliciesFromPod parses and caches policies from pod annotations
func (w *Watcher) updatePoliciesFromPod(pod *corev1.Pod, sandboxID string) {
	if pod.Annotations == nil {
		return
	}

	// Parse network policy from annotation
	networkPolicyAnnotation := pod.Annotations[controller.AnnotationNetworkPolicy]
	networkPolicy, err := v1alpha1.ParseNetworkPolicyFromAnnotation(networkPolicyAnnotation)
	networkPolicyParsed := err == nil
	if err != nil {
		w.logger.Error("Failed to parse network policy from annotation",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
	}

	// Parse bandwidth policy from annotation
	bandwidthPolicyAnnotation := pod.Annotations[controller.AnnotationBandwidthPolicy]
	bandwidthPolicy, err := v1alpha1.ParseBandwidthPolicyFromAnnotation(bandwidthPolicyAnnotation)
	bandwidthPolicyParsed := err == nil
	if err != nil {
		w.logger.Error("Failed to parse bandwidth policy from annotation",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
	}

	// Update cache and notify handlers
	w.policyCache.mu.Lock()
	oldNetworkPolicy := w.policyCache.networkPolicies[sandboxID]
	oldBandwidthPolicy := w.policyCache.bandwidthPolicies[sandboxID]
	if networkPolicyAnnotation == "" {
		delete(w.policyCache.networkPolicies, sandboxID)
	} else if networkPolicyParsed {
		w.policyCache.networkPolicies[sandboxID] = networkPolicy
	}
	if bandwidthPolicyAnnotation == "" {
		delete(w.policyCache.bandwidthPolicies, sandboxID)
	} else if bandwidthPolicyParsed {
		w.policyCache.bandwidthPolicies[sandboxID] = bandwidthPolicy
	}
	w.policyCache.mu.Unlock()

	// Notify handlers if policies changed
	if networkPolicyAnnotation == "" && oldNetworkPolicy != nil {
		w.logger.Info("Network policy cleared from annotation",
			zap.String("sandboxID", sandboxID),
		)
		if w.onNetworkPolicyChange != nil {
			w.onNetworkPolicyChange(sandboxID, nil)
		}
	} else if networkPolicyParsed && networkPolicy != nil {
		w.logger.Info("Network policy updated from annotation",
			zap.String("sandboxID", sandboxID),
		)
		if w.onNetworkPolicyChange != nil {
			w.onNetworkPolicyChange(sandboxID, networkPolicy)
		}
	}

	if bandwidthPolicyAnnotation == "" && oldBandwidthPolicy != nil {
		w.logger.Info("Bandwidth policy cleared from annotation",
			zap.String("sandboxID", sandboxID),
		)
		if w.onBandwidthPolicyChange != nil {
			w.onBandwidthPolicyChange(sandboxID, nil)
		}
	} else if bandwidthPolicyParsed && bandwidthPolicy != nil {
		w.logger.Info("Bandwidth policy updated from annotation",
			zap.String("sandboxID", sandboxID),
		)
		if w.onBandwidthPolicyChange != nil {
			w.onBandwidthPolicyChange(sandboxID, bandwidthPolicy)
		}
	}
}

// removePolicies removes policies from cache and notifies handlers
func (w *Watcher) removePolicies(sandboxID string) {
	w.policyCache.mu.Lock()
	delete(w.policyCache.networkPolicies, sandboxID)
	delete(w.policyCache.bandwidthPolicies, sandboxID)
	w.policyCache.mu.Unlock()

	// Notify handlers that policies are removed
	if w.onNetworkPolicyChange != nil {
		w.onNetworkPolicyChange(sandboxID, nil)
	}
	if w.onBandwidthPolicyChange != nil {
		w.onBandwidthPolicyChange(sandboxID, nil)
	}
}

// GetSandboxByIP returns sandbox info by pod IP
func (w *Watcher) GetSandboxByIP(podIP string) *SandboxInfo {
	w.podMappingMu.RLock()
	defer w.podMappingMu.RUnlock()
	return w.podMapping[podIP]
}

// GetNetworkPolicy returns network policy for a sandbox
func (w *Watcher) GetNetworkPolicy(sandboxID string) *v1alpha1.NetworkPolicySpec {
	w.policyCache.mu.RLock()
	defer w.policyCache.mu.RUnlock()
	return w.policyCache.networkPolicies[sandboxID]
}

// GetBandwidthPolicy returns bandwidth policy for a sandbox
func (w *Watcher) GetBandwidthPolicy(sandboxID string) *v1alpha1.BandwidthPolicySpec {
	w.policyCache.mu.RLock()
	defer w.policyCache.mu.RUnlock()
	return w.policyCache.bandwidthPolicies[sandboxID]
}

// ListActiveSandboxes returns all active sandboxes on this node
func (w *Watcher) ListActiveSandboxes() []*SandboxInfo {
	w.podMappingMu.RLock()
	defer w.podMappingMu.RUnlock()

	var result []*SandboxInfo
	for _, info := range w.podMapping {
		result = append(result, info)
	}
	return result
}

// ListSandboxPods lists all sandbox pods matching the given selector
func (w *Watcher) ListSandboxPods(selector labels.Selector) ([]*corev1.Pod, error) {
	podLister := w.informerFactory.Core().V1().Pods().Lister()
	return podLister.List(selector)
}

// MarkPolicyApplied updates the applied policy hash annotations after rules are applied.
func (w *Watcher) MarkPolicyApplied(ctx context.Context, sandboxID string) error {
	pod, err := w.getSandboxPodByID(sandboxID)
	if err != nil {
		return err
	}
	if pod == nil || pod.Annotations == nil {
		return nil
	}

	networkHash := pod.Annotations[controller.AnnotationNetworkPolicyHash]
	bandwidthHash := pod.Annotations[controller.AnnotationBandwidthPolicyHash]
	if networkHash == "" && bandwidthHash == "" {
		return nil
	}

	annotations := map[string]any{}
	if networkHash != "" && pod.Annotations[controller.AnnotationNetworkPolicyAppliedHash] != networkHash {
		annotations[controller.AnnotationNetworkPolicyAppliedHash] = networkHash
	}
	if bandwidthHash != "" && pod.Annotations[controller.AnnotationBandwidthPolicyAppliedHash] != bandwidthHash {
		annotations[controller.AnnotationBandwidthPolicyAppliedHash] = bandwidthHash
	}
	if len(annotations) == 0 {
		return nil
	}

	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": annotations,
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal policy applied patch: %w", err)
	}

	_, err = w.k8sClient.CoreV1().Pods(pod.Namespace).Patch(
		ctx,
		pod.Name,
		types.MergePatchType,
		data,
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("patch policy applied annotations: %w", err)
	}
	return nil
}

func (w *Watcher) getSandboxPodByID(sandboxID string) (*corev1.Pod, error) {
	podLister := w.informerFactory.Core().V1().Pods().Lister()
	pods, err := podLister.List(labels.SelectorFromSet(map[string]string{
		controller.LabelSandboxID: sandboxID,
	}))
	if err != nil {
		return nil, err
	}
	if len(pods) == 0 {
		return nil, fmt.Errorf("sandbox pod not found: %s", sandboxID)
	}
	return pods[0], nil
}

// ListNetworkPolicies returns all cached network policies
func (w *Watcher) ListNetworkPolicies() []*v1alpha1.NetworkPolicySpec {
	w.policyCache.mu.RLock()
	defer w.policyCache.mu.RUnlock()

	result := make([]*v1alpha1.NetworkPolicySpec, 0, len(w.policyCache.networkPolicies))
	for _, policy := range w.policyCache.networkPolicies {
		result = append(result, policy)
	}
	return result
}

// ListBandwidthPolicies returns all cached bandwidth policies
func (w *Watcher) ListBandwidthPolicies() []*v1alpha1.BandwidthPolicySpec {
	w.policyCache.mu.RLock()
	defer w.policyCache.mu.RUnlock()

	result := make([]*v1alpha1.BandwidthPolicySpec, 0, len(w.policyCache.bandwidthPolicies))
	for _, policy := range w.policyCache.bandwidthPolicies {
		result = append(result, policy)
	}
	return result
}
