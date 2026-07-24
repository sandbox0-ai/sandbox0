package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	s0template "github.com/sandbox0-ai/sandbox0/pkg/template"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	if cfg != nil && cfg.Resources != nil {
		memory, err := s.validateSandboxMemory(cfg.Resources.Memory)
		if err != nil {
			return v1alpha1.ResourceQuota{}, err
		}
		quota.Memory = memory
		quota.CPU = s0template.CPUForMemory(memory, s.sandboxMemoryPerCPU())
	}
	minCPU := *resource.NewMilliQuantity(v1alpha1.MinimumClaimedSandboxCPULimitMilli, resource.DecimalSI)
	if quota.CPU.Cmp(minCPU) < 0 {
		quota.CPU = minCPU
	}
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
	resources := v1alpha1.BuildResourceRequirements(quota)
	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"containers": []map[string]any{{
				"name":      "procd",
				"resources": resources,
			}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("build sandbox resource resize patch: %w", err)
	}
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, err := s.k8sClient.CoreV1().Pods(namespace).Patch(
			ctx,
			name,
			types.StrategicMergePatchType,
			patch,
			metav1.PatchOptions{},
			"resize",
		)
		if err != nil {
			return err
		}
		if result == nil || result.Name == "" {
			updated = pod.DeepCopy()
			if applyErr := s.applySandboxResourceQuota(updated, quota); applyErr != nil {
				return applyErr
			}
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
		// Warm pods already carry the template limits while keeping scheduling
		// requests low. Preserve those dense requests when the claim does not
		// change either enforceable limit; only resource overrides and limit
		// corrections require an in-place resize.
		return !resourceLimitsEqual(container.Resources.Limits, desired.Limits)
	}
	return true
}

func resourceLimitsEqual(a, b corev1.ResourceList) bool {
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		if !resourceListQuantityEqual(a, b, name) {
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
