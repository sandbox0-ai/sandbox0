package service

import (
	"sort"
	"strings"
	"sync"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
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
	byNamespace map[string]map[string]struct{}
	bySandboxID map[string]map[SandboxPodRef]struct{}
}

// NewSandboxIndex creates a new SandboxIndex instance.
func NewSandboxIndex() *SandboxIndex {
	return &SandboxIndex{
		byNamespace: make(map[string]map[string]struct{}),
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

// GetNamespace returns the namespace of a sandbox ID if present.
func (s *SandboxIndex) GetNamespace(sandboxID string) (string, bool) {
	ref, ok := s.GetPodRef(sandboxID)
	return ref.Namespace, ok
}

// GetPodRef returns the current runtime pod reference for a sandbox ID if present.
func (s *SandboxIndex) GetPodRef(sandboxID string) (SandboxPodRef, bool) {
	refs := s.GetPodRefs(sandboxID)
	if len(refs) == 0 {
		return SandboxPodRef{}, false
	}
	return refs[0], true
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

// ListSandboxIDs returns sandbox IDs in the given namespace.
func (s *SandboxIndex) ListSandboxIDs(namespace string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := s.byNamespace[namespace]
	if len(set) == 0 {
		return nil
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *SandboxIndex) handleAdd(obj interface{}) {
	if pod := extractPod(obj); pod != nil {
		s.upsertPod(pod)
	}
}

func (s *SandboxIndex) handleUpdate(oldObj, newObj interface{}) {
	oldPod := extractPod(oldObj)
	newPod := extractPod(newObj)
	s.refreshPodIndex(oldPod, newPod)
}

func (s *SandboxIndex) handleDelete(obj interface{}) {
	if pod := extractPod(obj); pod != nil {
		s.deletePod(pod)
	}
}

func (s *SandboxIndex) refreshPodIndex(oldPod, newPod *corev1.Pod) {
	if oldPod != nil {
		oldID := sandboxIDFromPod(oldPod)
		if oldID != "" {
			newID := ""
			newNamespace := ""
			if newPod != nil {
				newID = sandboxIDFromPod(newPod)
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
	sandboxID := sandboxIDFromPod(pod)
	if sandboxID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	set, ok := s.byNamespace[pod.Namespace]
	if !ok {
		set = make(map[string]struct{})
		s.byNamespace[pod.Namespace] = set
	}
	set[sandboxID] = struct{}{}
	refSet, ok := s.bySandboxID[sandboxID]
	if !ok {
		refSet = make(map[SandboxPodRef]struct{})
		s.bySandboxID[sandboxID] = refSet
	}
	refSet[SandboxPodRef{Namespace: pod.Namespace, Name: pod.Name}] = struct{}{}
}

func (s *SandboxIndex) deletePod(pod *corev1.Pod) {
	sandboxID := sandboxIDFromPod(pod)
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
	refStillInNamespace := false
	if refSet, ok := s.bySandboxID[sandboxID]; ok {
		delete(refSet, ref)
		for remaining := range refSet {
			if remaining.Namespace == ref.Namespace {
				refStillInNamespace = true
				break
			}
		}
		if len(refSet) == 0 {
			delete(s.bySandboxID, sandboxID)
		}
	}
	if refStillInNamespace {
		return
	}
	if set, ok := s.byNamespace[ref.Namespace]; ok {
		delete(set, sandboxID)
		if len(set) == 0 {
			delete(s.byNamespace, ref.Namespace)
		}
	}
}

func sandboxIDFromPod(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	if pod.Labels != nil {
		if sandboxID := strings.TrimSpace(pod.Labels[controller.LabelSandboxID]); sandboxID != "" {
			return sandboxID
		}
	}
	if pod.Annotations != nil {
		if sandboxID := strings.TrimSpace(pod.Annotations[controller.AnnotationSandboxID]); sandboxID != "" {
			return sandboxID
		}
	}
	return pod.Name
}

func extractPod(obj interface{}) *corev1.Pod {
	switch pod := obj.(type) {
	case *corev1.Pod:
		return pod
	case cache.DeletedFinalStateUnknown:
		if pod, ok := pod.Obj.(*corev1.Pod); ok {
			return pod
		}
	case *cache.DeletedFinalStateUnknown:
		if pod, ok := pod.Obj.(*corev1.Pod); ok {
			return pod
		}
	}
	return nil
}
