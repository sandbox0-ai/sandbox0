// Package watcher provides Kubernetes resource watchers for netd.
package watcher

import (
	"context"
	"fmt"
	"sync"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
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

// PolicyCache holds cached network and bandwidth policies
type PolicyCache struct {
	mu                sync.RWMutex
	networkPolicies   map[string]*v1alpha1.SandboxNetworkPolicy   // sandboxID -> policy
	bandwidthPolicies map[string]*v1alpha1.SandboxBandwidthPolicy // sandboxID -> policy
}

// Watcher watches Kubernetes resources for netd
type Watcher struct {
	k8sClient       kubernetes.Interface
	informerFactory informers.SharedInformerFactory
	nodeName        string
	logger          *zap.Logger

	// Pod mapping: podIP -> SandboxInfo
	podMappingMu sync.RWMutex
	podMapping   map[string]*SandboxInfo

	// Policy cache
	policyCache *PolicyCache

	// Network policy informer
	networkPolicyInformer cache.SharedIndexInformer

	// Bandwidth policy informer
	bandwidthPolicyInformer cache.SharedIndexInformer

	// Event handlers
	onPodAdd                func(*SandboxInfo)
	onPodUpdate             func(*SandboxInfo, *SandboxInfo)
	onPodDelete             func(*SandboxInfo)
	onNetworkPolicyChange   func(sandboxID string, policy *v1alpha1.SandboxNetworkPolicy)
	onBandwidthPolicyChange func(sandboxID string, policy *v1alpha1.SandboxBandwidthPolicy)
}

// NewWatcher creates a new Watcher
func NewWatcher(
	k8sClient kubernetes.Interface,
	nodeName string,
	namespace string,
	resyncPeriod interface{},
	logger *zap.Logger,
) *Watcher {
	// Create informer factory for the specific namespace or all namespaces
	var informerFactory informers.SharedInformerFactory
	if namespace != "" {
		informerFactory = informers.NewSharedInformerFactoryWithOptions(
			k8sClient,
			0,
			informers.WithNamespace(namespace),
		)
	} else {
		informerFactory = informers.NewSharedInformerFactory(k8sClient, 0)
	}

	return &Watcher{
		k8sClient:       k8sClient,
		informerFactory: informerFactory,
		nodeName:        nodeName,
		logger:          logger,
		podMapping:      make(map[string]*SandboxInfo),
		policyCache: &PolicyCache{
			networkPolicies:   make(map[string]*v1alpha1.SandboxNetworkPolicy),
			bandwidthPolicies: make(map[string]*v1alpha1.SandboxBandwidthPolicy),
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
func (w *Watcher) SetNetworkPolicyHandler(handler func(sandboxID string, policy *v1alpha1.SandboxNetworkPolicy)) {
	w.onNetworkPolicyChange = handler
}

// SetBandwidthPolicyHandler sets the handler for bandwidth policy changes
func (w *Watcher) SetBandwidthPolicyHandler(handler func(sandboxID string, policy *v1alpha1.SandboxBandwidthPolicy)) {
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

	// Setup CRD informers (mock implementation for now)
	// In production, use generated clientset for CRDs
	w.setupNetworkPolicyInformer(ctx)
	w.setupBandwidthPolicyInformer(ctx)

	// Start informers
	w.informerFactory.Start(ctx.Done())

	// Wait for cache sync
	w.logger.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		return fmt.Errorf("failed to sync pod informer cache")
	}

	w.logger.Info("Watcher started and caches synced")
	return nil
}

// setupNetworkPolicyInformer creates informer for SandboxNetworkPolicy
func (w *Watcher) setupNetworkPolicyInformer(ctx context.Context) {
	// Create ListWatch for SandboxNetworkPolicy
	// In production, use generated clientset
	listWatch := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return &v1alpha1.SandboxNetworkPolicyList{Items: []v1alpha1.SandboxNetworkPolicy{}}, nil
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return watch.NewFake(), nil
		},
	}

	w.networkPolicyInformer = cache.NewSharedIndexInformer(
		listWatch,
		&v1alpha1.SandboxNetworkPolicy{},
		0,
		cache.Indexers{},
	)

	w.networkPolicyInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.handleNetworkPolicyAdd,
		UpdateFunc: w.handleNetworkPolicyUpdate,
		DeleteFunc: w.handleNetworkPolicyDelete,
	})

	go w.networkPolicyInformer.Run(ctx.Done())
}

// setupBandwidthPolicyInformer creates informer for SandboxBandwidthPolicy
func (w *Watcher) setupBandwidthPolicyInformer(ctx context.Context) {
	// Create ListWatch for SandboxBandwidthPolicy
	listWatch := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return &v1alpha1.SandboxBandwidthPolicyList{Items: []v1alpha1.SandboxBandwidthPolicy{}}, nil
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return watch.NewFake(), nil
		},
	}

	w.bandwidthPolicyInformer = cache.NewSharedIndexInformer(
		listWatch,
		&v1alpha1.SandboxBandwidthPolicy{},
		0,
		cache.Indexers{},
	)

	w.bandwidthPolicyInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.handleBandwidthPolicyAdd,
		UpdateFunc: w.handleBandwidthPolicyUpdate,
		DeleteFunc: w.handleBandwidthPolicyDelete,
	})

	go w.bandwidthPolicyInformer.Run(ctx.Done())
}

// handlePodAdd handles pod add events
func (w *Watcher) handlePodAdd(obj interface{}) {
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
func (w *Watcher) handlePodUpdate(oldObj, newObj interface{}) {
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

	if w.onPodUpdate != nil && (wasActive || isActive) {
		w.onPodUpdate(oldInfo, newInfo)
	}
}

// handlePodDelete handles pod delete events
func (w *Watcher) handlePodDelete(obj interface{}) {
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

// Network policy handlers
func (w *Watcher) handleNetworkPolicyAdd(obj interface{}) {
	policy := obj.(*v1alpha1.SandboxNetworkPolicy)
	sandboxID := policy.Spec.SandboxID

	w.policyCache.mu.Lock()
	w.policyCache.networkPolicies[sandboxID] = policy
	w.policyCache.mu.Unlock()

	w.logger.Info("Network policy added",
		zap.String("sandboxID", sandboxID),
		zap.String("name", policy.Name),
	)

	if w.onNetworkPolicyChange != nil {
		w.onNetworkPolicyChange(sandboxID, policy)
	}
}

func (w *Watcher) handleNetworkPolicyUpdate(oldObj, newObj interface{}) {
	policy := newObj.(*v1alpha1.SandboxNetworkPolicy)
	sandboxID := policy.Spec.SandboxID

	w.policyCache.mu.Lock()
	w.policyCache.networkPolicies[sandboxID] = policy
	w.policyCache.mu.Unlock()

	w.logger.Info("Network policy updated",
		zap.String("sandboxID", sandboxID),
	)

	if w.onNetworkPolicyChange != nil {
		w.onNetworkPolicyChange(sandboxID, policy)
	}
}

func (w *Watcher) handleNetworkPolicyDelete(obj interface{}) {
	policy, ok := obj.(*v1alpha1.SandboxNetworkPolicy)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		policy, ok = tombstone.Obj.(*v1alpha1.SandboxNetworkPolicy)
		if !ok {
			return
		}
	}

	sandboxID := policy.Spec.SandboxID

	w.policyCache.mu.Lock()
	delete(w.policyCache.networkPolicies, sandboxID)
	w.policyCache.mu.Unlock()

	w.logger.Info("Network policy deleted",
		zap.String("sandboxID", sandboxID),
	)

	if w.onNetworkPolicyChange != nil {
		w.onNetworkPolicyChange(sandboxID, nil)
	}
}

// Bandwidth policy handlers
func (w *Watcher) handleBandwidthPolicyAdd(obj interface{}) {
	policy := obj.(*v1alpha1.SandboxBandwidthPolicy)
	sandboxID := policy.Spec.SandboxID

	w.policyCache.mu.Lock()
	w.policyCache.bandwidthPolicies[sandboxID] = policy
	w.policyCache.mu.Unlock()

	w.logger.Info("Bandwidth policy added",
		zap.String("sandboxID", sandboxID),
	)

	if w.onBandwidthPolicyChange != nil {
		w.onBandwidthPolicyChange(sandboxID, policy)
	}
}

func (w *Watcher) handleBandwidthPolicyUpdate(oldObj, newObj interface{}) {
	policy := newObj.(*v1alpha1.SandboxBandwidthPolicy)
	sandboxID := policy.Spec.SandboxID

	w.policyCache.mu.Lock()
	w.policyCache.bandwidthPolicies[sandboxID] = policy
	w.policyCache.mu.Unlock()

	if w.onBandwidthPolicyChange != nil {
		w.onBandwidthPolicyChange(sandboxID, policy)
	}
}

func (w *Watcher) handleBandwidthPolicyDelete(obj interface{}) {
	policy, ok := obj.(*v1alpha1.SandboxBandwidthPolicy)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		policy, ok = tombstone.Obj.(*v1alpha1.SandboxBandwidthPolicy)
		if !ok {
			return
		}
	}

	sandboxID := policy.Spec.SandboxID

	w.policyCache.mu.Lock()
	delete(w.policyCache.bandwidthPolicies, sandboxID)
	w.policyCache.mu.Unlock()

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
func (w *Watcher) GetNetworkPolicy(sandboxID string) *v1alpha1.SandboxNetworkPolicy {
	w.policyCache.mu.RLock()
	defer w.policyCache.mu.RUnlock()
	return w.policyCache.networkPolicies[sandboxID]
}

// GetBandwidthPolicy returns bandwidth policy for a sandbox
func (w *Watcher) GetBandwidthPolicy(sandboxID string) *v1alpha1.SandboxBandwidthPolicy {
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
