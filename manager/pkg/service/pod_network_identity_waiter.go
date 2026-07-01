package service

import (
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

type podNetworkIdentityEvent struct {
	pod     *corev1.Pod
	deleted bool
}

type podNetworkIdentityWaiter struct {
	mu      sync.Mutex
	waiters map[string]map[chan podNetworkIdentityEvent]struct{}
}

func newPodNetworkIdentityWaiter() *podNetworkIdentityWaiter {
	return &podNetworkIdentityWaiter{
		waiters: make(map[string]map[chan podNetworkIdentityEvent]struct{}),
	}
}

func (w *podNetworkIdentityWaiter) ResourceEventHandler() cache.ResourceEventHandlerFuncs {
	if w == nil {
		return cache.ResourceEventHandlerFuncs{}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			w.notifyObject(obj, false)
		},
		UpdateFunc: func(_, newObj any) {
			w.notifyObject(newObj, false)
		},
		DeleteFunc: func(obj any) {
			w.notifyObject(obj, true)
		},
	}
}

func (w *podNetworkIdentityWaiter) register(namespace, name string) (<-chan podNetworkIdentityEvent, func()) {
	ch := make(chan podNetworkIdentityEvent, 1)
	if w == nil {
		return ch, func() {}
	}
	key := podNetworkIdentityKey(namespace, name)

	w.mu.Lock()
	if w.waiters[key] == nil {
		w.waiters[key] = make(map[chan podNetworkIdentityEvent]struct{})
	}
	w.waiters[key][ch] = struct{}{}
	w.mu.Unlock()

	return ch, func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		waiters := w.waiters[key]
		if waiters == nil {
			return
		}
		delete(waiters, ch)
		if len(waiters) == 0 {
			delete(w.waiters, key)
		}
	}
}

func (w *podNetworkIdentityWaiter) notifyObject(obj any, deleted bool) {
	if w == nil {
		return
	}
	pod := podFromInformerObject(obj)
	if pod == nil {
		return
	}
	key := podNetworkIdentityKey(pod.Namespace, pod.Name)
	event := podNetworkIdentityEvent{pod: pod, deleted: deleted}

	w.mu.Lock()
	defer w.mu.Unlock()
	for ch := range w.waiters[key] {
		select {
		case ch <- event:
		default:
		}
	}
}

func podFromInformerObject(obj any) *corev1.Pod {
	if pod, ok := obj.(*corev1.Pod); ok {
		return pod
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil
	}
	pod, _ := tombstone.Obj.(*corev1.Pod)
	return pod
}

func podNetworkIdentityKey(namespace, name string) string {
	return namespace + "/" + name
}
