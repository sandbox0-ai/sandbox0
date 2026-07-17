package template

import (
	"fmt"
	"math/big"
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

// CPUForMemory returns the CPU limit required for a memory limit at the given
// memory-per-CPU ratio, rounded up to Kubernetes millicpu precision.
func CPUForMemory(memory, memoryPerCPU resource.Quantity) resource.Quantity {
	if memory.Sign() <= 0 || memoryPerCPU.Sign() <= 0 {
		return resource.Quantity{}
	}
	numerator := big.NewInt(memory.Value())
	numerator.Mul(numerator, big.NewInt(1000))
	denominator := big.NewInt(memoryPerCPU.Value())
	quotient, remainder := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return *resource.NewMilliQuantity(1<<63-1, resource.DecimalSI)
	}
	return *resource.NewMilliQuantity(quotient.Int64(), resource.DecimalSI)
}

// ValidateResourceRatio enforces the platform memory-derived CPU shape for template specs.
func ValidateResourceRatio(spec v1alpha1.SandboxTemplateSpec, memoryPerCPU resource.Quantity, subject string) error {
	if subject == "" {
		subject = "template"
	}
	if memoryPerCPU.Sign() <= 0 {
		memoryPerCPU = MemoryPerCPUOrDefault("")
	}
	totalCPU := spec.MainContainer.Resources.CPU.DeepCopy()
	totalMemory := spec.MainContainer.Resources.Memory.DeepCopy()
	requiredCPU := CPUForMemory(totalMemory, memoryPerCPU)
	if totalCPU.Cmp(requiredCPU) != 0 {
		return fmt.Errorf(
			"%s total cpu must match the value derived from memory (got cpu=%s memory=%s expectedCPU=%s)",
			subject,
			totalCPU.String(),
			totalMemory.String(),
			requiredCPU.String(),
		)
	}
	return nil
}
