// Package sandboxindex maintains the informer-backed sandbox-to-pod index.
package sandboxindex

import (
	"sort"
	"sync"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/podmeta"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

type SandboxPodRef struct {
	Namespace string
	Name      string
}

// SandboxIndex keeps an in-memory index of sandbox IDs by namespace.
// All methods are safe for concurrent use.
type SandboxIndex struct {
	mu          sync.RWMutex
	bySandboxID map[string]map[SandboxPodRef]struct{}
}

// NewSandboxIndex creates a new SandboxIndex instance.
func NewSandboxIndex() *SandboxIndex {
	return &SandboxIndex{
		bySandboxID: make(map[string]map[SandboxPodRef]struct{}),
	}
}

// ResourceEventHandler returns handlers to keep the index in sync.
func (s *SandboxIndex) ResourceEventHandler() cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    s.handleAdd,
		UpdateFunc: s.handleUpdate,
		DeleteFunc: s.handleDelete,
	}
}

// GetPodRefs returns all known runtime pod references for a sandbox ID.
func (s *SandboxIndex) GetPodRefs(sandboxID string) []SandboxPodRef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := s.bySandboxID[sandboxID]
	if len(set) == 0 {
		return nil
	}
	refs := make([]SandboxPodRef, 0, len(set))
	for ref := range set {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Namespace == refs[j].Namespace {
			return refs[i].Name < refs[j].Name
		}
		return refs[i].Namespace < refs[j].Namespace
	})
	return refs
}

func (s *SandboxIndex) handleAdd(obj interface{}) {
	if pod := podmeta.FromInformerEvent(obj); pod != nil {
		s.upsertPod(pod)
	}
}

func (s *SandboxIndex) handleUpdate(oldObj, newObj interface{}) {
	oldPod := podmeta.FromInformerEvent(oldObj)
	newPod := podmeta.FromInformerEvent(newObj)
	s.refreshPodIndex(oldPod, newPod)
}

func (s *SandboxIndex) handleDelete(obj interface{}) {
	if pod := podmeta.FromInformerEvent(obj); pod != nil {
		s.deletePod(pod)
	}
}

func (s *SandboxIndex) refreshPodIndex(oldPod, newPod *corev1.Pod) {
	if oldPod != nil {
		oldID := podmeta.SandboxID(oldPod)
		if oldID != "" {
			newID := ""
			newNamespace := ""
			if newPod != nil {
				newID = podmeta.SandboxID(newPod)
				newNamespace = newPod.Namespace
			}
			if newID != oldID || oldPod.Namespace != newNamespace {
				s.removePodRef(oldID, SandboxPodRef{Namespace: oldPod.Namespace, Name: oldPod.Name})
			}
		}
	}
	if newPod != nil {
		s.upsertPod(newPod)
	}
}

func (s *SandboxIndex) upsertPod(pod *corev1.Pod) {
	sandboxID := podmeta.SandboxID(pod)
	if sandboxID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	refSet, ok := s.bySandboxID[sandboxID]
	if !ok {
		refSet = make(map[SandboxPodRef]struct{})
		s.bySandboxID[sandboxID] = refSet
	}
	refSet[SandboxPodRef{Namespace: pod.Namespace, Name: pod.Name}] = struct{}{}
}

func (s *SandboxIndex) deletePod(pod *corev1.Pod) {
	sandboxID := podmeta.SandboxID(pod)
	if sandboxID == "" {
		return
	}
	s.removePodRef(sandboxID, SandboxPodRef{Namespace: pod.Namespace, Name: pod.Name})
}

func (s *SandboxIndex) removePodRef(sandboxID string, ref SandboxPodRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeBySandboxIDLocked(sandboxID, ref)
}

func (s *SandboxIndex) removeBySandboxIDLocked(sandboxID string, ref SandboxPodRef) {
	refSet, ok := s.bySandboxID[sandboxID]
	if !ok {
		return
	}
	delete(refSet, ref)
	if len(refSet) == 0 {
		delete(s.bySandboxID, sandboxID)
	}
}
