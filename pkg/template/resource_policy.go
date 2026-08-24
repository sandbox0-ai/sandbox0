package template

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/quantity"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
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
	memoryPerCPU quantity.Quantity
	maxMemory    quantity.Quantity
}

// ClaimResources is the exact resource lease shape derived for one claim.
type ClaimResources struct {
	Quota         sandboxspec.ResourceQuota
	CPUMillicores int64
	MemoryBytes   int64
}

// NewResourcePolicy resolves configured resource settings with platform defaults.
func NewResourcePolicy(memoryPerCPU, maxMemory string) ResourcePolicy {
	return ResourcePolicy{
		memoryPerCPU: positiveQuantityOrDefault(memoryPerCPU, DefaultMemoryPerCPU),
		maxMemory:    positiveQuantityOrDefault(maxMemory, DefaultSandboxMaxMemory),
	}
}

// MemoryPerCPU returns the effective memory-per-CPU ratio.
func (p ResourcePolicy) MemoryPerCPU() quantity.Quantity {
	if p.memoryPerCPU.Sign() <= 0 {
		return quantity.MustParse(DefaultMemoryPerCPU)
	}
	return p.memoryPerCPU
}

// MaxMemory returns the effective maximum memory limit for one sandbox.
func (p ResourcePolicy) MaxMemory() quantity.Quantity {
	if p.maxMemory.Sign() <= 0 {
		return quantity.MustParse(DefaultSandboxMaxMemory)
	}
	return p.maxMemory
}

// ParseMemory parses and validates a sandbox memory limit against the shared
// platform bounds.
func (p ResourcePolicy) ParseMemory(value, field string) (quantity.Quantity, error) {
	if strings.TrimSpace(field) == "" {
		field = "memory"
	}
	raw := strings.TrimSpace(value)
	if raw == "" {
		return quantity.Quantity{}, fmt.Errorf("%s is required", field)
	}
	memory, err := quantity.Parse(raw)
	if err != nil {
		return quantity.Quantity{}, fmt.Errorf("%s is invalid: %w", field, err)
	}
	if err := p.ValidateMemory(memory, field); err != nil {
		return quantity.Quantity{}, err
	}
	return memory, nil
}

// ValidateMemory validates a parsed sandbox memory limit against the shared
// platform bounds.
func (p ResourcePolicy) ValidateMemory(memory quantity.Quantity, field string) error {
	if strings.TrimSpace(field) == "" {
		field = "memory"
	}
	if memory.Sign() <= 0 {
		return fmt.Errorf("%s must be > 0", field)
	}
	minMemory := quantity.MustParse(DefaultSandboxMinMemory)
	if memory.Cmp(minMemory) < 0 {
		return fmt.Errorf("%s must be >= %s", field, minMemory.String())
	}
	return p.ValidateMaxMemory(memory, field)
}

// ValidateMaxMemory enforces the configurable platform ceiling. Callers that
// accept raw user input should use ValidateMemory to enforce the fixed lower
// bound as well.
func (p ResourcePolicy) ValidateMaxMemory(memory quantity.Quantity, field string) error {
	if strings.TrimSpace(field) == "" {
		field = "memory"
	}
	maxMemory := p.MaxMemory()
	if memory.Cmp(maxMemory) > 0 {
		return fmt.Errorf("%s must be <= %s", field, maxMemory.String())
	}
	return nil
}

// ResolveClaimResources applies an optional memory override and returns exact
// integer values shared by regional routing and the manager claim transaction.
func (p ResourcePolicy) ResolveClaimResources(
	spec sandboxspec.TemplateSpec,
	memoryOverride *string,
) (ClaimResources, error) {
	quota := spec.MainContainer.Resources
	memory, err := quantity.Parse(quota.Memory)
	if err != nil {
		return ClaimResources{}, fmt.Errorf("template memory is invalid: %w", err)
	}
	if memoryOverride != nil {
		memory, err = p.ParseMemory(*memoryOverride, "config.resources.memory")
		if err != nil {
			return ClaimResources{}, err
		}
		quota.Memory = memory.String()
		quota.CPU = CPUForMemory(memory, p.MemoryPerCPU()).String()
	}
	if err := p.ValidateMaxMemory(memory, "sandbox memory limit"); err != nil {
		return ClaimResources{}, err
	}

	cpu, err := quantity.Parse(quota.CPU)
	if err != nil {
		return ClaimResources{}, fmt.Errorf("CPU is invalid: %w", err)
	}
	millicpu := cpu.MilliValue()
	if millicpu <= 0 || quantity.NewMilli(millicpu).Cmp(cpu) != 0 {
		return ClaimResources{}, fmt.Errorf("CPU must be a positive exact millicore quantity")
	}
	memoryBytes := memory.Value()
	if memoryBytes <= 0 || quantity.New(memoryBytes).Cmp(memory) != 0 {
		return ClaimResources{}, fmt.Errorf("memory must be a positive exact byte quantity")
	}
	return ClaimResources{
		Quota: quota, CPUMillicores: millicpu, MemoryBytes: memoryBytes,
	}, nil
}

// ValidateTemplate enforces the platform memory ceiling and resource ratio on a template.
func (p ResourcePolicy) ValidateTemplate(spec sandboxspec.TemplateSpec, subject string) error {
	memory, err := quantity.Parse(spec.MainContainer.Resources.Memory)
	if err != nil {
		return fmt.Errorf("spec.mainContainer.resources.memory is invalid: %w", err)
	}
	if err := p.ValidateMaxMemory(memory, "spec.mainContainer.resources.memory"); err != nil {
		return err
	}
	return ValidateResourceRatio(spec, p.MemoryPerCPU(), subject)
}

// MemoryPerCPUOrDefault parses memory-per-CPU settings and falls back to the platform default.
func MemoryPerCPUOrDefault(value string) quantity.Quantity {
	return positiveQuantityOrDefault(value, DefaultMemoryPerCPU)
}

func positiveQuantityOrDefault(value, fallback string) quantity.Quantity {
	parsed, err := quantity.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Sign() <= 0 {
		return quantity.MustParse(fallback)
	}
	return parsed
}

// CPUForMemory returns the CPU limit required for a memory limit at the given
// memory-per-CPU ratio, rounded up to exact millicore precision.
func CPUForMemory(memory, memoryPerCPU quantity.Quantity) quantity.Quantity {
	if memory.Sign() <= 0 || memoryPerCPU.Sign() <= 0 {
		return quantity.Quantity{}
	}
	numerator := big.NewInt(memory.Value())
	numerator.Mul(numerator, big.NewInt(1000))
	denominator := big.NewInt(memoryPerCPU.Value())
	quotient, remainder := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return quantity.NewMilli(1<<63 - 1)
	}
	return quantity.NewMilli(quotient.Int64())
}

// ValidateResourceRatio enforces the platform memory-derived CPU shape for template specs.
func ValidateResourceRatio(spec sandboxspec.TemplateSpec, memoryPerCPU quantity.Quantity, subject string) error {
	if subject == "" {
		subject = "template"
	}
	if memoryPerCPU.Sign() <= 0 {
		memoryPerCPU = MemoryPerCPUOrDefault("")
	}
	totalCPU, err := quantity.Parse(spec.MainContainer.Resources.CPU)
	if err != nil {
		return fmt.Errorf("%s cpu is invalid: %w", subject, err)
	}
	totalMemory, err := quantity.Parse(spec.MainContainer.Resources.Memory)
	if err != nil {
		return fmt.Errorf("%s memory is invalid: %w", subject, err)
	}
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
