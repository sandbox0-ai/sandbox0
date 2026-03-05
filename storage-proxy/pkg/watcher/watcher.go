// Package watcher provides Kubernetes resource watchers for storage-proxy.
// It monitors sandbox pod lifecycle events to automatically unmount volumes
// when sandboxes are deleted, preventing data loss.
package watcher

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// SandboxInfo contains sandbox identity information
type SandboxInfo struct {
	SandboxID string
	PodName   string
	Namespace string
	PodIP     string
	NodeName  string
}

// Watcher watches Kubernetes Pods for sandbox lifecycle events
type Watcher struct {
	k8sClient       kubernetes.Interface
	informerFactory informers.SharedInformerFactory
	namespace       string
	logger          *zap.Logger

	// Event handlers
	onPodDelete func(*SandboxInfo)
}

// NewWatcher creates a new Watcher
func NewWatcher(
	k8sClient kubernetes.Interface,
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
		namespace:       namespace,
		logger:          logger,
	}
}

// SetPodDeleteHandler sets the handler for pod delete events
func (w *Watcher) SetPodDeleteHandler(handler func(*SandboxInfo)) {
	w.onPodDelete = handler
}

// Start starts the watcher
func (w *Watcher) Start(ctx context.Context) error {
	// Setup pod informer
	podInformer := w.informerFactory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    nil, // Not needed for storage-proxy
		UpdateFunc: nil, // Not needed for storage-proxy
		DeleteFunc: w.handlePodDelete,
	})

	// Start informers
	w.informerFactory.Start(ctx.Done())

	// Wait for cache sync
	w.logger.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		return fmt.Errorf("failed to sync pod informer cache")
	}

	w.logger.Info("Watcher started and cache synced")
	return nil
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
		return // Not a managed sandbox pod
	}

	w.logger.Info("Sandbox pod deleted",
		zap.String("sandbox_id", info.SandboxID),
		zap.String("pod_name", info.PodName),
		zap.String("namespace", info.Namespace),
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

	// Check pool type - only care about active sandboxes (those with mounted volumes)
	poolType := pod.Labels[controller.LabelPoolType]
	if poolType != controller.PoolTypeActive {
		return nil
	}

	return &SandboxInfo{
		SandboxID: sandboxID,
		PodName:   pod.Name,
		Namespace: pod.Namespace,
		PodIP:     pod.Status.PodIP,
		NodeName:  pod.Spec.NodeName,
	}
}
