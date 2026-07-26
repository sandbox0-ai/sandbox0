package service

import (
	"math/rand"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// reserveIdleClaimCandidate prevents concurrent claims in one manager process
// from selecting the same informer-cached idle Pod. Kubernetes metadata CAS
// remains the cross-process source of truth.
func (s *SandboxService) reserveIdleClaimCandidate(candidates []*corev1.Pod) *corev1.Pod {
	if s == nil || len(candidates) == 0 {
		return nil
	}

	s.idleClaimMu.Lock()
	defer s.idleClaimMu.Unlock()
	if s.idleClaimReservations == nil {
		s.idleClaimReservations = make(map[string]string)
	}

	start := rand.Intn(len(candidates))
	for offset := range len(candidates) {
		candidate := candidates[(start+offset)%len(candidates)]
		if candidate == nil {
			continue
		}
		key := podEventKey(candidate.Namespace, candidate.Name)
		uid := string(candidate.UID)
		if reservedUID, reserved := s.idleClaimReservations[key]; reserved {
			if reservedUID == "" || uid == "" || reservedUID == uid {
				continue
			}
			delete(s.idleClaimReservations, key)
		}
		s.idleClaimReservations[key] = uid
		return candidate
	}
	return nil
}

func (s *SandboxService) releaseIdleClaimCandidate(pod *corev1.Pod) {
	if s == nil || pod == nil {
		return
	}
	key := podEventKey(pod.Namespace, pod.Name)

	s.idleClaimMu.Lock()
	defer s.idleClaimMu.Unlock()
	if reservedUID, ok := s.idleClaimReservations[key]; ok {
		uid := string(pod.UID)
		if reservedUID == "" || uid == "" || reservedUID == uid {
			delete(s.idleClaimReservations, key)
		}
	}
}

// observeIdleClaimPodEvent hands a successful local reservation back only
// after the informer observes the durable Kubernetes reservation (or deletion).
func (s *SandboxService) observeIdleClaimPodEvent(obj any, deleted bool) {
	if s == nil {
		return
	}
	pod := podFromInformerObject(obj)
	if pod == nil {
		return
	}
	key := podEventKey(pod.Namespace, pod.Name)

	s.idleClaimMu.Lock()
	defer s.idleClaimMu.Unlock()
	reservedUID, reserved := s.idleClaimReservations[key]
	if !reserved {
		return
	}
	uid := string(pod.UID)
	if deleted ||
		(reservedUID != "" && uid != "" && reservedUID != uid) ||
		pod.DeletionTimestamp != nil ||
		pod.Labels[controller.LabelPoolType] != controller.PoolTypeIdle ||
		controller.IsHotClaimReservedPod(pod) {
		delete(s.idleClaimReservations, key)
	}
}

func (s *SandboxService) hotClaimClient() kubernetes.Interface {
	if s != nil && s.hotClaimK8sClient != nil {
		return s.hotClaimK8sClient
	}
	if s == nil {
		return nil
	}
	return s.k8sClient
}
