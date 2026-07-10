package runtimemetrics

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const sandboxContainerName = "procd"

type sandboxIdentity struct {
	TeamID            string
	SandboxID         string
	RuntimeGeneration int64
	PodUID            string
	Namespace         string
	PodName           string
	CPULimitCores     *float64
	MemoryLimitBytes  *uint64
}

type identityIndex struct {
	byUID  map[string]sandboxIdentity
	byName map[string]sandboxIdentity
}

func buildIdentityIndex(podLister corelisters.PodLister, nodeName string) (identityIndex, error) {
	if podLister == nil {
		return identityIndex{}, fmt.Errorf("pod lister is nil")
	}
	pods, err := podLister.List(labels.Everything())
	if err != nil {
		return identityIndex{}, fmt.Errorf("list cached pods: %w", err)
	}

	index := identityIndex{
		byUID:  make(map[string]sandboxIdentity, len(pods)),
		byName: make(map[string]sandboxIdentity, len(pods)),
	}
	for _, pod := range pods {
		identity, ok := identityFromPod(pod, nodeName)
		if !ok {
			continue
		}
		if identity.PodUID != "" {
			index.byUID[identity.PodUID] = identity
		}
		index.byName[podNameKey(identity.Namespace, identity.PodName)] = identity
	}
	return index, nil
}

func identityFromPod(pod *corev1.Pod, nodeName string) (sandboxIdentity, bool) {
	if pod == nil || pod.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		return sandboxIdentity{}, false
	}
	if strings.TrimSpace(nodeName) != "" && strings.TrimSpace(pod.Spec.NodeName) != strings.TrimSpace(nodeName) {
		return sandboxIdentity{}, false
	}
	if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return sandboxIdentity{}, false
	}

	sandboxID := strings.TrimSpace(pod.Labels[controller.LabelSandboxID])
	teamID := strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID])
	rawGeneration := strings.TrimSpace(pod.Annotations[controller.AnnotationRuntimeGeneration])
	if sandboxID == "" || teamID == "" || rawGeneration == "" {
		return sandboxIdentity{}, false
	}
	runtimeGeneration, err := strconv.ParseInt(rawGeneration, 10, 64)
	if err != nil || runtimeGeneration < 0 {
		return sandboxIdentity{}, false
	}

	identity := sandboxIdentity{
		TeamID:            teamID,
		SandboxID:         sandboxID,
		RuntimeGeneration: runtimeGeneration,
		PodUID:            strings.TrimSpace(string(pod.UID)),
		Namespace:         strings.TrimSpace(pod.Namespace),
		PodName:           strings.TrimSpace(pod.Name),
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != sandboxContainerName {
			continue
		}
		if cpu, ok := container.Resources.Limits[corev1.ResourceCPU]; ok && !cpu.IsZero() {
			value := cpu.AsApproximateFloat64()
			if value > 0 {
				identity.CPULimitCores = &value
			}
		}
		if memory, ok := container.Resources.Limits[corev1.ResourceMemory]; ok && !memory.IsZero() {
			value := memory.Value()
			if value > 0 {
				unsigned := uint64(value)
				identity.MemoryLimitBytes = &unsigned
			}
		}
		break
	}
	return identity, true
}

func (i identityIndex) resolve(attributes *runtimeapi.PodSandboxAttributes) (sandboxIdentity, bool) {
	if attributes == nil || attributes.Metadata == nil {
		return sandboxIdentity{}, false
	}
	metadata := attributes.Metadata
	if uid := strings.TrimSpace(metadata.Uid); uid != "" {
		identity, ok := i.byUID[uid]
		return identity, ok
	}
	identity, ok := i.byName[podNameKey(metadata.Namespace, metadata.Name)]
	return identity, ok
}

func podNameKey(namespace, name string) string {
	return strings.TrimSpace(namespace) + "\x00" + strings.TrimSpace(name)
}
