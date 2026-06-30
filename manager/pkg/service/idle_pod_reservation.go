package service

import (
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
)

const idlePodReservationTTL = 30 * time.Second

type idlePodReservations struct {
	mu     sync.Mutex
	byName map[string]time.Time
}

func newIdlePodReservations() *idlePodReservations {
	return &idlePodReservations{byName: map[string]time.Time{}}
}

func (r *idlePodReservations) tryReserve(pod *corev1.Pod, ttl time.Duration) bool {
	if pod == nil {
		return false
	}
	if ttl <= 0 {
		ttl = idlePodReservationTTL
	}
	key := podReservationKey(pod)
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeExpiredLocked(now)
	if expiresAt, ok := r.byName[key]; ok && now.Before(expiresAt) {
		return false
	}
	r.byName[key] = now.Add(ttl)
	return true
}

func (r *idlePodReservations) release(pod *corev1.Pod) {
	if pod == nil {
		return
	}
	key := podReservationKey(pod)
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byName, key)
}

func (r *idlePodReservations) isReserved(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	key := podReservationKey(pod)
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeExpiredLocked(now)
	expiresAt, ok := r.byName[key]
	return ok && now.Before(expiresAt)
}

func (r *idlePodReservations) removeExpiredLocked(now time.Time) {
	for key, expiresAt := range r.byName {
		if !now.Before(expiresAt) {
			delete(r.byName, key)
		}
	}
}

func podReservationKey(pod *corev1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}
