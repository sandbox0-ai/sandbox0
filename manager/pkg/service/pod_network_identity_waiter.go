package service

import (
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

type podEvent struct {
	pod     *corev1.Pod
	deleted bool
}

type podEventWaiter struct {
	mu      sync.Mutex
	waiters map[string]map[chan podEvent]struct{}
}

func newPodEventWaiter() *podEventWaiter {
	return &podEventWaiter{
		waiters: make(map[string]map[chan podEvent]struct{}),
	}
}

func (w *podEventWaiter) ResourceEventHandler() cache.ResourceEventHandlerFuncs {
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

func (w *podEventWaiter) register(namespace, name string) (<-chan podEvent, func()) {
	ch := make(chan podEvent, 1)
	if w == nil {
		return ch, func() {}
	}
	key := podEventKey(namespace, name)

	w.mu.Lock()
	if w.waiters[key] == nil {
		w.waiters[key] = make(map[chan podEvent]struct{})
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

func (w *podEventWaiter) notifyObject(obj any, deleted bool) {
	if w == nil {
		return
	}
	pod := podFromInformerObject(obj)
	if pod == nil {
		return
	}
	key := podEventKey(pod.Namespace, pod.Name)
	event := podEvent{pod: pod, deleted: deleted}

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

func podEventKey(namespace, name string) string {
	return namespace + "/" + name
}
