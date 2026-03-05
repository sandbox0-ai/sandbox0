package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const (
	defaultNetdApplyTimeout = 5 * time.Second
)

// NetdProviderConfig configures the netd provider behavior.
type NetdProviderConfig struct {
	ApplyTimeout time.Duration
	PollInterval time.Duration
}

type policyWaiter struct {
	expectedNetworkHash string
	done                chan struct{}
	once                sync.Once
}

// NetdProvider waits for netd to apply policies.
type NetdProvider struct {
	podLister    corelisters.PodLister
	podInformer  cache.SharedIndexInformer
	logger       *zap.Logger
	applyTimeout time.Duration
	pollInterval time.Duration
	waitersMu    sync.Mutex
	waiters      map[string]map[*policyWaiter]struct{}
}

// NewNetdProvider creates a netd-backed Provider.
func NewNetdProvider(
	podInformer coreinformers.PodInformer,
	podLister corelisters.PodLister,
	config NetdProviderConfig,
	logger *zap.Logger,
) Provider {
	if logger == nil {
		logger = zap.NewNop()
	}
	applyTimeout := config.ApplyTimeout
	if applyTimeout == 0 {
		applyTimeout = defaultNetdApplyTimeout
	}
	pollInterval := config.PollInterval
	provider := &NetdProvider{
		podLister:    podLister,
		podInformer:  podInformer.Informer(),
		logger:       logger,
		applyTimeout: applyTimeout,
		pollInterval: pollInterval,
		waiters:      make(map[string]map[*policyWaiter]struct{}),
	}
	provider.registerPodHandlers()
	return provider
}

func (p *NetdProvider) Name() string { return "netd" }

func (p *NetdProvider) EnsureBaseline(ctx context.Context, namespace string) error {
	return nil
}

func (p *NetdProvider) ApplySandboxPolicy(ctx context.Context, input SandboxPolicyInput) error {
	if p.podLister == nil {
		return fmt.Errorf("netd provider missing pod lister")
	}
	if p.podInformer == nil {
		return fmt.Errorf("netd provider missing pod informer")
	}
	if input.Namespace == "" || input.PodName == "" {
		return fmt.Errorf("netd provider missing pod identity")
	}

	networkHash, err := p.networkPolicyHash(input.NetworkPolicy)
	if err != nil {
		return fmt.Errorf("hash network policy: %w", err)
	}
	if networkHash == "" {
		return nil
	}

	waitCtx := ctx
	if p.applyTimeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, p.applyTimeout)
		defer cancel()
	}

	return p.waitForAppliedHashes(waitCtx, input.Namespace, input.PodName, networkHash)
}

func (p *NetdProvider) RemoveSandboxPolicy(ctx context.Context, namespace, sandboxID string) error {
	return nil
}

func (p *NetdProvider) waitForAppliedHashes(
	ctx context.Context,
	namespace string,
	podName string,
	expectedNetworkHash string,
) error {
	key := namespace + "/" + podName
	waiter := &policyWaiter{
		expectedNetworkHash: expectedNetworkHash,
		done:                make(chan struct{}),
	}
	p.addWaiter(key, waiter)
	defer p.removeWaiter(key, waiter)

	if p.isApplied(namespace, podName, expectedNetworkHash) {
		p.completeWaiter(key, waiter)
		return nil
	}

	var ticker *time.Ticker
	if p.pollInterval > 0 {
		ticker = time.NewTicker(p.pollInterval)
		defer ticker.Stop()
	}

	for {
		select {
		case <-waiter.done:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for netd policy apply for pod %s/%s", namespace, podName)
		case <-p.tick(ticker):
			if p.isApplied(namespace, podName, expectedNetworkHash) {
				p.completeWaiter(key, waiter)
				return nil
			}
		}
	}
}

func (p *NetdProvider) appliedHashesMatch(
	pod *corev1.Pod,
	expectedNetworkHash string,
) bool {
	if pod == nil || pod.Annotations == nil {
		return false
	}
	if expectedNetworkHash != "" {
		if pod.Annotations[controller.AnnotationNetworkPolicyAppliedHash] != expectedNetworkHash {
			return false
		}
	}
	return true
}

func (p *NetdProvider) registerPodHandlers() {
	p.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			p.handlePodEvent(obj)
		},
		UpdateFunc: func(_, newObj any) {
			p.handlePodEvent(newObj)
		},
	})
}

func (p *NetdProvider) handlePodEvent(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return
	}
	key := pod.Namespace + "/" + pod.Name

	p.waitersMu.Lock()
	waiters := p.waiters[key]
	for waiter := range waiters {
		if p.appliedHashesMatch(pod, waiter.expectedNetworkHash) {
			p.completeWaiterLocked(key, waiter)
		}
	}
	p.waitersMu.Unlock()
}

func (p *NetdProvider) isApplied(
	namespace string,
	podName string,
	expectedNetworkHash string,
) bool {
	pod, err := p.podLister.Pods(namespace).Get(podName)
	if err != nil {
		return false
	}
	return p.appliedHashesMatch(pod, expectedNetworkHash)
}

func (p *NetdProvider) addWaiter(key string, waiter *policyWaiter) {
	p.waitersMu.Lock()
	defer p.waitersMu.Unlock()
	waiters := p.waiters[key]
	if waiters == nil {
		waiters = make(map[*policyWaiter]struct{})
		p.waiters[key] = waiters
	}
	waiters[waiter] = struct{}{}
}

func (p *NetdProvider) removeWaiter(key string, waiter *policyWaiter) {
	p.waitersMu.Lock()
	defer p.waitersMu.Unlock()
	waiters := p.waiters[key]
	if waiters == nil {
		return
	}
	delete(waiters, waiter)
	if len(waiters) == 0 {
		delete(p.waiters, key)
	}
}

func (p *NetdProvider) completeWaiter(key string, waiter *policyWaiter) {
	p.waitersMu.Lock()
	defer p.waitersMu.Unlock()
	p.completeWaiterLocked(key, waiter)
}

func (p *NetdProvider) completeWaiterLocked(key string, waiter *policyWaiter) {
	waiter.once.Do(func() {
		close(waiter.done)
	})
	waiters := p.waiters[key]
	if waiters == nil {
		return
	}
	delete(waiters, waiter)
	if len(waiters) == 0 {
		delete(p.waiters, key)
	}
}

func (p *NetdProvider) tick(ticker *time.Ticker) <-chan time.Time {
	if ticker == nil {
		return nil
	}
	return ticker.C
}

func (p *NetdProvider) networkPolicyHash(spec *v1alpha1.NetworkPolicySpec) (string, error) {
	if spec == nil {
		return "", nil
	}
	annotation, err := v1alpha1.NetworkPolicyToAnnotation(spec)
	if err != nil {
		return "", err
	}
	return hashAnnotation(annotation), nil
}

func hashAnnotation(annotation string) string {
	if annotation == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(annotation))
	return hex.EncodeToString(sum[:])
}
