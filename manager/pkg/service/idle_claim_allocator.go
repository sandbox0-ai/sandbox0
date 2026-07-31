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
	candidateGroups := s.idleClaimCandidateGroups(candidates)

	s.idleClaimMu.Lock()
	defer s.idleClaimMu.Unlock()
	if s.idleClaimReservations == nil {
		s.idleClaimReservations = make(map[string]string)
	}

	for _, group := range candidateGroups {
		if len(group) == 0 {
			continue
		}
		start := rand.Intn(len(group))
		for offset := range len(group) {
			candidate := group[(start+offset)%len(group)]
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
	}
	return nil
}

func (s *SandboxService) idleClaimCandidateGroups(candidates []*corev1.Pod) [][]*corev1.Pod {
	if len(s.config.PreferredNodeSelector) == 0 || s.nodeLister == nil {
		return [][]*corev1.Pod{candidates}
	}

	preferred := make([]*corev1.Pod, 0, len(candidates))
	fallback := make([]*corev1.Pod, 0, len(candidates))
	for _, candidate := range candidates {
		if s.idleClaimCandidateMatchesPreferredNode(candidate) {
			preferred = append(preferred, candidate)
			continue
		}
		fallback = append(fallback, candidate)
	}
	return [][]*corev1.Pod{preferred, fallback}
}

func (s *SandboxService) idleClaimCandidateMatchesPreferredNode(candidate *corev1.Pod) bool {
	if candidate == nil || candidate.Spec.NodeName == "" || s.nodeLister == nil {
		return false
	}
	node, err := s.nodeLister.Get(candidate.Spec.NodeName)
	if err != nil || node == nil {
		return false
	}
	for key, value := range s.config.PreferredNodeSelector {
		nodeValue, exists := node.Labels[key]
		if !exists || nodeValue != value {
			return false
		}
	}
	return true
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
