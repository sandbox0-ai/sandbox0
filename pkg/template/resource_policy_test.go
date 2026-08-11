package template

import (
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestMemoryPerCPUOrDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "configured", in: "8Gi", want: "8Gi"},
		{name: "blank", in: "", want: DefaultMemoryPerCPU},
		{name: "invalid", in: "bad", want: DefaultMemoryPerCPU},
		{name: "zero", in: "0", want: DefaultMemoryPerCPU},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := MemoryPerCPUOrDefault(tt.in)
			want := resource.MustParse(tt.want)
			if got.Cmp(want) != 0 {
				t.Fatalf("MemoryPerCPUOrDefault(%q) = %s, want %s", tt.in, got.String(), want.String())
			}
		})
	}
}

func TestResourcePolicyDefaultsAndOverrides(t *testing.T) {
	t.Parallel()

	defaults := ResourcePolicy{}
	if got := defaults.MemoryPerCPU(); got.Cmp(resource.MustParse(DefaultMemoryPerCPU)) != 0 {
		t.Fatalf("default memory per CPU = %s, want %s", got.String(), DefaultMemoryPerCPU)
	}
	if got := defaults.MaxMemory(); got.Cmp(resource.MustParse(DefaultSandboxMaxMemory)) != 0 {
		t.Fatalf("default max memory = %s, want %s", got.String(), DefaultSandboxMaxMemory)
	}

	configured := NewResourcePolicy("2Gi", "32Gi")
	if got := configured.MemoryPerCPU(); got.Cmp(resource.MustParse("2Gi")) != 0 {
		t.Fatalf("configured memory per CPU = %s, want 2Gi", got.String())
	}
	if got := configured.MaxMemory(); got.Cmp(resource.MustParse("32Gi")) != 0 {
		t.Fatalf("configured max memory = %s, want 32Gi", got.String())
	}
}

func TestResourcePolicyParseMemory(t *testing.T) {
	t.Parallel()

	policy := NewResourcePolicy("", "16Gi")
	tests := []struct {
		name    string
		memory  string
		wantErr string
	}{
		{name: "minimum accepted", memory: "128Mi"},
		{name: "maximum accepted", memory: "16Gi"},
		{name: "below minimum rejected", memory: "127Mi", wantErr: "must be >= 128Mi"},
		{name: "above maximum rejected", memory: "17Gi", wantErr: "must be <= 16Gi"},
		{name: "invalid rejected", memory: "large", wantErr: "is invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := policy.ParseMemory(tt.memory, "config.resources.memory")
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ParseMemory() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ParseMemory() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestResourcePolicyValidateTemplateRejectsMemoryAboveMaximum(t *testing.T) {
	t.Parallel()

	policy := NewResourcePolicy("2Gi", "16Gi")
	spec := v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Resources: v1alpha1.ResourceQuota{
				CPU:    resource.MustParse("16"),
				Memory: resource.MustParse("32Gi"),
			},
		},
	}
	err := policy.ValidateTemplate(spec, "team-owned template")
	if err == nil || !strings.Contains(err.Error(), "spec.mainContainer.resources.memory must be <= 16Gi") {
		t.Fatalf("ValidateTemplate() error = %v, want max-memory rejection", err)
	}
}

func TestCPUForMemory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		memory string
		want   string
	}{
		{name: "whole cpu", memory: "4Gi", want: "1"},
		{name: "fractional minimum sandbox memory rounds up to millicpu", memory: "128Mi", want: "32m"},
		{name: "half cpu", memory: "2Gi", want: "500m"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := CPUForMemory(resource.MustParse(tt.memory), resource.MustParse("4Gi"))
			want := resource.MustParse(tt.want)
			if got.Cmp(want) != 0 {
				t.Fatalf("CPUForMemory(%s) = %s, want %s", tt.memory, got.String(), want.String())
			}
		})
	}
}

func TestValidateResourceRatio(t *testing.T) {
	t.Parallel()

	spec := v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Resources: v1alpha1.ResourceQuota{
				CPU:    resource.MustParse("1"),
				Memory: resource.MustParse("4Gi"),
			},
		},
	}

	if err := ValidateResourceRatio(spec, resource.MustParse("4Gi"), "builtin template default"); err != nil {
		t.Fatalf("expected ratio to pass, got %v", err)
	}

	spec.MainContainer.Resources.CPU = resource.MustParse("32m")
	spec.MainContainer.Resources.Memory = resource.MustParse("129Mi")
	if err := ValidateResourceRatio(spec, resource.MustParse("4Gi"), "rounded template"); err != nil {
		t.Fatalf("expected rounded memory-derived cpu to pass, got %v", err)
	}

	spec.MainContainer.Resources.CPU = resource.MustParse("1")
	spec.MainContainer.Resources.Memory = resource.MustParse("1Gi")
	err := ValidateResourceRatio(spec, resource.MustParse("4Gi"), "builtin template default")
	if err == nil {
		t.Fatal("expected ratio validation to fail")
	}
	if got := err.Error(); !strings.Contains(got, "builtin template default total cpu must match the value derived from memory") {
		t.Fatalf("unexpected error %q", got)
	}
}
