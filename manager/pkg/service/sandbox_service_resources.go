package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	s0template "github.com/sandbox0-ai/sandbox0/pkg/template"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

const (
	defaultSandboxMinMemory = "128Mi"
	defaultSandboxMaxMemory = "32Gi"
)

func (s *SandboxService) effectiveSandboxResourceQuota(template *v1alpha1.SandboxTemplate, cfg *SandboxConfig) (v1alpha1.ResourceQuota, error) {
	if template == nil {
		return v1alpha1.ResourceQuota{}, fmt.Errorf("%w: template is required", ErrInvalidClaimRequest)
	}
	quota := *template.Spec.MainContainer.Resources.DeepCopy()
	if cfg == nil || cfg.Resources == nil {
		return quota, nil
	}
	memory, err := s.validateSandboxMemory(cfg.Resources.Memory)
	if err != nil {
		return v1alpha1.ResourceQuota{}, err
	}
	quota.Memory = memory
	quota.CPU = s0template.CPUForMemory(memory, s.sandboxMemoryPerCPU())
	return quota, nil
}

func (s *SandboxService) applySandboxResourceQuota(pod *corev1.Pod, quota v1alpha1.ResourceQuota) error {
	if pod == nil {
		return fmt.Errorf("%w: pod is required", ErrInvalidClaimRequest)
	}
	return applySandboxResourceQuotaToPodSpec(&pod.Spec, quota)
}

func (s *SandboxService) resizeSandboxPodResources(ctx context.Context, pod *corev1.Pod, quota v1alpha1.ResourceQuota) (*corev1.Pod, error) {
	if s == nil || s.k8sClient == nil {
		return nil, fmt.Errorf("%w: kubernetes client is not configured", ErrInvalidClaimRequest)
	}
	if pod == nil || pod.Namespace == "" || pod.Name == "" {
		return nil, fmt.Errorf("%w: pod is required", ErrInvalidClaimRequest)
	}

	namespace, name := pod.Namespace, pod.Name
	var updated *corev1.Pod
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := s.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		resized := current.DeepCopy()
		if err := s.applySandboxResourceQuota(resized, quota); err != nil {
			return err
		}
		result, err := s.k8sClient.CoreV1().Pods(namespace).UpdateResize(ctx, name, resized, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		if result == nil || result.Name == "" {
			updated = resized
		} else {
			updated = result
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func mergeSandboxMetadataAfterResize(resizedPod, metadataPod *corev1.Pod) *corev1.Pod {
	if resizedPod == nil {
		return metadataPod
	}
	if metadataPod == nil {
		return resizedPod
	}
	merged := resizedPod.DeepCopy()
	merged.Labels = cloneMetadataMap(metadataPod.Labels)
	merged.Annotations = cloneMetadataMap(metadataPod.Annotations)
	merged.Finalizers = append([]string(nil), metadataPod.Finalizers...)
	merged.OwnerReferences = append([]metav1.OwnerReference(nil), metadataPod.OwnerReferences...)
	return merged
}

func cloneMetadataMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func applySandboxResourceQuotaToPodSpec(spec *corev1.PodSpec, quota v1alpha1.ResourceQuota) error {
	if spec == nil {
		return fmt.Errorf("%w: pod spec is required", ErrInvalidClaimRequest)
	}
	for i := range spec.Containers {
		if spec.Containers[i].Name != "procd" {
			continue
		}
		spec.Containers[i].Resources = v1alpha1.BuildResourceRequirements(quota)
		ensureSandboxResizePolicy(&spec.Containers[i])
		return nil
	}
	return fmt.Errorf("%w: sandbox runtime container not found", ErrInvalidClaimRequest)
}

func sandboxPodNeedsResourceResize(pod *corev1.Pod, quota v1alpha1.ResourceQuota) bool {
	if quota.CPU.Sign() <= 0 && quota.Memory.Sign() <= 0 {
		return false
	}
	if pod == nil {
		return true
	}
	desired := v1alpha1.BuildResourceRequirements(quota)
	for _, container := range pod.Spec.Containers {
		if container.Name != "procd" {
			continue
		}
		return !resizeResourcesEqual(container.Resources, desired)
	}
	return true
}

func resizeResourcesEqual(a, b corev1.ResourceRequirements) bool {
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		if !resourceListQuantityEqual(a.Requests, b.Requests, name) {
			return false
		}
		if !resourceListQuantityEqual(a.Limits, b.Limits, name) {
			return false
		}
	}
	return true
}

func resourceListQuantityEqual(a, b corev1.ResourceList, name corev1.ResourceName) bool {
	aValue, aOK := a[name]
	bValue, bOK := b[name]
	if !aOK || aValue.IsZero() {
		return !bOK || bValue.IsZero()
	}
	return bOK && aValue.Cmp(bValue) == 0
}

func ensureSandboxResizePolicy(container *corev1.Container) {
	if container == nil {
		return
	}
	upsert := func(name corev1.ResourceName) {
		for i := range container.ResizePolicy {
			if container.ResizePolicy[i].ResourceName == name {
				container.ResizePolicy[i].RestartPolicy = corev1.NotRequired
				return
			}
		}
		container.ResizePolicy = append(container.ResizePolicy, corev1.ContainerResizePolicy{
			ResourceName:  name,
			RestartPolicy: corev1.NotRequired,
		})
	}
	upsert(corev1.ResourceCPU)
	upsert(corev1.ResourceMemory)
}

func (s *SandboxService) validateSandboxMemory(value string) (resource.Quantity, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return resource.Quantity{}, fmt.Errorf("%w: config.resources.memory is required", ErrInvalidClaimRequest)
	}
	memory, err := resource.ParseQuantity(raw)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("%w: config.resources.memory is invalid: %v", ErrInvalidClaimRequest, err)
	}
	if memory.Sign() <= 0 {
		return resource.Quantity{}, fmt.Errorf("%w: config.resources.memory must be > 0", ErrInvalidClaimRequest)
	}
	minMemory := sandboxMemoryQuantityOrDefault(defaultSandboxMinMemory, defaultSandboxMinMemory)
	if memory.Cmp(minMemory) < 0 {
		return resource.Quantity{}, fmt.Errorf("%w: config.resources.memory must be >= %s", ErrInvalidClaimRequest, minMemory.String())
	}
	maxMemory := s.sandboxMaxMemory()
	if memory.Cmp(maxMemory) > 0 {
		return resource.Quantity{}, fmt.Errorf("%w: config.resources.memory must be <= %s", ErrInvalidClaimRequest, maxMemory.String())
	}
	return memory, nil
}

func (s *SandboxService) sandboxMemoryPerCPU() resource.Quantity {
	if s == nil {
		return s0template.MemoryPerCPUOrDefault("")
	}
	return s0template.MemoryPerCPUOrDefault(s.config.SandboxMemoryPerCPU)
}

func (s *SandboxService) sandboxMaxMemory() resource.Quantity {
	if s == nil {
		return sandboxMemoryQuantityOrDefault("", defaultSandboxMaxMemory)
	}
	return sandboxMemoryQuantityOrDefault(s.config.SandboxMaxMemory, defaultSandboxMaxMemory)
}

func sandboxMemoryQuantityOrDefault(value, fallback string) resource.Quantity {
	parsed, err := resource.ParseQuantity(strings.TrimSpace(value))
	if err == nil && parsed.Sign() > 0 {
		return parsed
	}
	return resource.MustParse(fallback)
}

func (s *SandboxService) enforceSandboxResourceQuota(ctx context.Context, teamID string, template *v1alpha1.SandboxTemplate, cfg *SandboxConfig) error {
	quota, err := s.effectiveSandboxResourceQuota(template, cfg)
	if err != nil {
		return err
	}
	if err := s.enforceSandboxCPUQuota(ctx, teamID, quota); err != nil {
		return err
	}
	return s.enforceSandboxMemoryQuota(ctx, teamID, quota)
}

func (s *SandboxService) enforceSandboxResourceQuotaIncrease(ctx context.Context, teamID string, current *corev1.Pod, next v1alpha1.ResourceQuota) error {
	currentCPU, currentMemoryBytes := podContainerResourceAllocation(current)
	oldCPU, oldMemoryBytes, ok := podRuntimeContainerResourceAllocation(current)
	if !ok {
		return fmt.Errorf("%w: sandbox runtime container not found", ErrInvalidClaimRequest)
	}
	nextCPU := currentCPU - oldCPU + next.CPU.MilliValue()
	nextMemoryMiB := bytesToMiBRoundUp(currentMemoryBytes - oldMemoryBytes + next.Memory.Value())
	currentMemoryMiB := bytesToMiBRoundUp(currentMemoryBytes)
	if delta := nextCPU - currentCPU; delta > 0 {
		if err := s.enforceQuota(ctx, teamID, quota.DimensionCPU, delta); err != nil {
			return err
		}
	}
	if delta := nextMemoryMiB - currentMemoryMiB; delta > 0 {
		if err := s.enforceQuota(ctx, teamID, quota.DimensionMemory, delta); err != nil {
			return err
		}
	}
	return nil
}

func podContainerResourceAllocation(pod *corev1.Pod) (int64, int64) {
	if pod == nil {
		return 0, 0
	}
	var cpuMillis int64
	var memoryBytes int64
	for _, container := range pod.Spec.Containers {
		cpuMillis += resourceListCPU(container.Resources)
		memoryBytes += resourceListMemory(container.Resources)
	}
	return cpuMillis, memoryBytes
}

func podRuntimeContainerResourceAllocation(pod *corev1.Pod) (int64, int64, bool) {
	if pod == nil {
		return 0, 0, false
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != "procd" {
			continue
		}
		return resourceListCPU(container.Resources), resourceListMemory(container.Resources), true
	}
	return 0, 0, false
}

func resourceListCPU(requirements corev1.ResourceRequirements) int64 {
	if quantity, ok := requirements.Limits[corev1.ResourceCPU]; ok && !quantity.IsZero() {
		return quantity.MilliValue()
	}
	if quantity, ok := requirements.Requests[corev1.ResourceCPU]; ok && !quantity.IsZero() {
		return quantity.MilliValue()
	}
	return 0
}

func resourceListMemory(requirements corev1.ResourceRequirements) int64 {
	if quantity, ok := requirements.Limits[corev1.ResourceMemory]; ok && !quantity.IsZero() {
		return quantity.Value()
	}
	if quantity, ok := requirements.Requests[corev1.ResourceMemory]; ok && !quantity.IsZero() {
		return quantity.Value()
	}
	return 0
}
