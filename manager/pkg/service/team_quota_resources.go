package service

import (
	"context"
	"fmt"
	"math"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeclassquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	corev1 "k8s.io/api/core/v1"
)

func (s *SandboxService) sandboxTeamQuotaTarget(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	cfg *SandboxConfig,
) (teamquota.Values, error) {
	if template == nil {
		return nil, fmt.Errorf("%w: template is required", ErrInvalidClaimRequest)
	}
	spec := v1alpha1.BuildPodSpec(template)
	resources, err := s.effectiveSandboxResourceLimits(template, cfg)
	if err != nil {
		return nil, err
	}
	if err := applySandboxResourceLimitsToPodSpec(&spec, resources); err != nil {
		return nil, err
	}
	quotaSpec, err := runtimeclassquota.ResolvePodSpec(ctx, s.k8sClient, &spec)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve runtime class quota overhead: %v", ErrTeamQuotaUnavailable, err)
	}
	target := PodSpecTeamQuotaResources(quotaSpec)
	target[teamquota.KeySandboxIdentityCount] = 1
	target[teamquota.KeySandboxRuntimeCount] = 1
	return target, nil
}

func multiplyQuotaValue(value, multiplier int64) (int64, error) {
	if value < 0 || multiplier < 0 {
		return 0, fmt.Errorf("quota values must be non-negative")
	}
	if value != 0 && multiplier > math.MaxInt64/value {
		return 0, fmt.Errorf("quota value overflows int64")
	}
	return value * multiplier, nil
}

// PodSpecTeamQuotaResources returns the maximum schedulable resource
// allocation of a pod. Restartable init containers are accounted for using
// the Kubernetes sidecar scheduling model; ordinary init containers use the
// maximum phase requirement because they do not overlap with app containers.
func PodSpecTeamQuotaResources(spec *corev1.PodSpec) teamquota.Values {
	target := teamquota.Values{
		teamquota.KeySandboxCPUMillicores:         0,
		teamquota.KeySandboxMemoryBytes:           0,
		teamquota.KeySandboxEphemeralStorageBytes: 0,
	}
	if spec == nil {
		return target
	}

	app := quotaResourceVector{}
	for i := range spec.Containers {
		app = app.add(containerQuotaResources(&spec.Containers[i]))
	}

	restartableInit := quotaResourceVector{}
	maxInitPhase := quotaResourceVector{}
	for i := range spec.InitContainers {
		container := &spec.InitContainers[i]
		current := containerQuotaResources(container)
		if container.RestartPolicy != nil && *container.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			restartableInit = restartableInit.add(current)
			maxInitPhase = maxInitPhase.max(restartableInit)
			continue
		}
		maxInitPhase = maxInitPhase.max(restartableInit.add(current))
	}

	effective := app.add(restartableInit).max(maxInitPhase)
	if spec.Overhead != nil {
		effective = effective.add(resourceListQuotaResources(spec.Overhead, nil))
	}

	target[teamquota.KeySandboxCPUMillicores] = effective.cpuMillicores
	target[teamquota.KeySandboxMemoryBytes] = effective.memoryBytes
	target[teamquota.KeySandboxEphemeralStorageBytes] = effective.ephemeralStorageBytes
	return target
}

type quotaResourceVector struct {
	cpuMillicores         int64
	memoryBytes           int64
	ephemeralStorageBytes int64
}

func (v quotaResourceVector) add(other quotaResourceVector) quotaResourceVector {
	return quotaResourceVector{
		cpuMillicores:         saturatingAdd(v.cpuMillicores, other.cpuMillicores),
		memoryBytes:           saturatingAdd(v.memoryBytes, other.memoryBytes),
		ephemeralStorageBytes: saturatingAdd(v.ephemeralStorageBytes, other.ephemeralStorageBytes),
	}
}

func (v quotaResourceVector) max(other quotaResourceVector) quotaResourceVector {
	return quotaResourceVector{
		cpuMillicores:         max(v.cpuMillicores, other.cpuMillicores),
		memoryBytes:           max(v.memoryBytes, other.memoryBytes),
		ephemeralStorageBytes: max(v.ephemeralStorageBytes, other.ephemeralStorageBytes),
	}
}

func saturatingAdd(left, right int64) int64 {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func containerQuotaResources(container *corev1.Container) quotaResourceVector {
	if container == nil {
		return quotaResourceVector{}
	}
	return resourceListQuotaResources(container.Resources.Limits, container.Resources.Requests)
}

func resourceListQuotaResources(limits, requests corev1.ResourceList) quotaResourceVector {
	return quotaResourceVector{
		cpuMillicores: resourceLimitValue(
			limits,
			requests,
			corev1.ResourceCPU,
			func(quantity corev1.ResourceList, name corev1.ResourceName) int64 {
				value := quantity[name]
				return value.MilliValue()
			},
		),
		memoryBytes: resourceLimitValue(
			limits,
			requests,
			corev1.ResourceMemory,
			func(quantity corev1.ResourceList, name corev1.ResourceName) int64 {
				value := quantity[name]
				return value.Value()
			},
		),
		ephemeralStorageBytes: resourceLimitValue(
			limits,
			requests,
			corev1.ResourceEphemeralStorage,
			func(quantity corev1.ResourceList, name corev1.ResourceName) int64 {
				value := quantity[name]
				return value.Value()
			},
		),
	}
}

func resourceLimitValue(
	limits corev1.ResourceList,
	requests corev1.ResourceList,
	name corev1.ResourceName,
	value func(corev1.ResourceList, corev1.ResourceName) int64,
) int64 {
	if quantity, ok := limits[name]; ok && quantity.Sign() > 0 {
		return value(limits, name)
	}
	if quantity, ok := requests[name]; ok && quantity.Sign() > 0 {
		return value(requests, name)
	}
	return 0
}
