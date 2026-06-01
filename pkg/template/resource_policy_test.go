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

func TestMemoryForCPU(t *testing.T) {
	t.Parallel()

	got := MemoryForCPU(resource.MustParse("500m"), resource.MustParse("4Gi"))
	want := resource.MustParse("2Gi")
	if got.Cmp(want) != 0 {
		t.Fatalf("MemoryForCPU = %s, want %s", got.String(), want.String())
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

	if err := ValidateResourceRatio(spec, resource.MustParse("4Gi"), "builtin template dins"); err != nil {
		t.Fatalf("expected ratio to pass, got %v", err)
	}

	spec.MainContainer.Resources.Memory = resource.MustParse("1Gi")
	err := ValidateResourceRatio(spec, resource.MustParse("4Gi"), "builtin template dins")
	if err == nil {
		t.Fatal("expected ratio validation to fail")
	}
	if got := err.Error(); !strings.Contains(got, "builtin template dins total memory must equal total cpu * 4Gi") {
		t.Fatalf("unexpected error %q", got)
	}
}
