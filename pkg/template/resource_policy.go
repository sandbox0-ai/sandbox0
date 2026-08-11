package template

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	DefaultMemoryPerCPU     = "4Gi"
	DefaultSandboxMinMemory = "128Mi"
	DefaultSandboxMaxMemory = "16Gi"
)

// ResourcePolicy defines the platform-wide resource bounds shared by
// templates and sandbox lifecycle operations. Its zero value uses platform
// defaults.
type ResourcePolicy struct {
	memoryPerCPU resource.Quantity
	maxMemory    resource.Quantity
}

// NewResourcePolicy resolves configured resource settings with platform defaults.
func NewResourcePolicy(memoryPerCPU, maxMemory string) ResourcePolicy {
	return ResourcePolicy{
		memoryPerCPU: positiveQuantityOrDefault(memoryPerCPU, DefaultMemoryPerCPU),
		maxMemory:    positiveQuantityOrDefault(maxMemory, DefaultSandboxMaxMemory),
	}
}

// MemoryPerCPU returns the effective memory-per-CPU ratio.
func (p ResourcePolicy) MemoryPerCPU() resource.Quantity {
	if p.memoryPerCPU.Sign() <= 0 {
		return resource.MustParse(DefaultMemoryPerCPU)
	}
	return p.memoryPerCPU.DeepCopy()
}

// MaxMemory returns the effective maximum memory limit for one sandbox.
func (p ResourcePolicy) MaxMemory() resource.Quantity {
	if p.maxMemory.Sign() <= 0 {
		return resource.MustParse(DefaultSandboxMaxMemory)
	}
	return p.maxMemory.DeepCopy()
}

// ParseMemory parses and validates a sandbox memory limit against the shared
// platform bounds.
func (p ResourcePolicy) ParseMemory(value, field string) (resource.Quantity, error) {
	if strings.TrimSpace(field) == "" {
		field = "memory"
	}
	raw := strings.TrimSpace(value)
	if raw == "" {
		return resource.Quantity{}, fmt.Errorf("%s is required", field)
	}
	memory, err := resource.ParseQuantity(raw)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("%s is invalid: %w", field, err)
	}
	if err := p.ValidateMemory(memory, field); err != nil {
		return resource.Quantity{}, err
	}
	return memory, nil
}

// ValidateMemory validates a parsed sandbox memory limit against the shared
// platform bounds.
func (p ResourcePolicy) ValidateMemory(memory resource.Quantity, field string) error {
	if strings.TrimSpace(field) == "" {
		field = "memory"
	}
	if memory.Sign() <= 0 {
		return fmt.Errorf("%s must be > 0", field)
	}
	minMemory := resource.MustParse(DefaultSandboxMinMemory)
	if memory.Cmp(minMemory) < 0 {
		return fmt.Errorf("%s must be >= %s", field, minMemory.String())
	}
	return p.ValidateMaxMemory(memory, field)
}

// ValidateMaxMemory enforces the configurable platform ceiling. Callers that
// accept raw user input should use ValidateMemory to enforce the fixed lower
// bound as well.
func (p ResourcePolicy) ValidateMaxMemory(memory resource.Quantity, field string) error {
	if strings.TrimSpace(field) == "" {
		field = "memory"
	}
	maxMemory := p.MaxMemory()
	if memory.Cmp(maxMemory) > 0 {
		return fmt.Errorf("%s must be <= %s", field, maxMemory.String())
	}
	return nil
}

// ValidateTemplate enforces the platform memory ceiling and resource ratio on a template.
func (p ResourcePolicy) ValidateTemplate(spec v1alpha1.SandboxTemplateSpec, subject string) error {
	if err := p.ValidateMaxMemory(spec.MainContainer.Resources.Memory, "spec.mainContainer.resources.memory"); err != nil {
		return err
	}
	return ValidateResourceRatio(spec, p.MemoryPerCPU(), subject)
}

// MemoryPerCPUOrDefault parses memory-per-CPU settings and falls back to the platform default.
func MemoryPerCPUOrDefault(value string) resource.Quantity {
	return positiveQuantityOrDefault(value, DefaultMemoryPerCPU)
}

func positiveQuantityOrDefault(value, fallback string) resource.Quantity {
	parsed, err := resource.ParseQuantity(strings.TrimSpace(value))
	if err != nil || parsed.Sign() <= 0 {
		return resource.MustParse(fallback)
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
