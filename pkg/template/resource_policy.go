package template

import (
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const DefaultMemoryPerCPU = "4Gi"

// MemoryPerCPUOrDefault parses memory-per-CPU settings and falls back to the platform default.
func MemoryPerCPUOrDefault(value string) resource.Quantity {
	parsed, err := resource.ParseQuantity(strings.TrimSpace(value))
	if err != nil || parsed.Sign() <= 0 {
		return resource.MustParse(DefaultMemoryPerCPU)
	}
	return parsed
}

// MemoryForCPU returns the memory limit required for a CPU limit at the given memory-per-CPU ratio.
func MemoryForCPU(cpu, memoryPerCPU resource.Quantity) resource.Quantity {
	if cpu.Sign() <= 0 || memoryPerCPU.Sign() <= 0 {
		return resource.Quantity{}
	}
	requiredBytes := cpu.MilliValue() * memoryPerCPU.Value() / 1000
	return *resource.NewQuantity(requiredBytes, resource.BinarySI)
}

// ValidateResourceRatio enforces the platform CPU-to-memory resource shape for template specs.
func ValidateResourceRatio(spec v1alpha1.SandboxTemplateSpec, memoryPerCPU resource.Quantity, subject string) error {
	if subject == "" {
		subject = "template"
	}
	if memoryPerCPU.Sign() <= 0 {
		memoryPerCPU = MemoryPerCPUOrDefault("")
	}
	totalCPU := spec.MainContainer.Resources.CPU.DeepCopy()
	totalMemory := spec.MainContainer.Resources.Memory.DeepCopy()
	requiredMemory := MemoryForCPU(totalCPU, memoryPerCPU)
	if totalMemory.Cmp(requiredMemory) != 0 {
		return fmt.Errorf(
			"%s total memory must equal total cpu * %s (got cpu=%s memory=%s expectedMemory=%s)",
			subject,
			memoryPerCPU.String(),
			totalCPU.String(),
			totalMemory.String(),
			requiredMemory.String(),
		)
	}
	return nil
}
