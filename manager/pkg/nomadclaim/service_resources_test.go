package nomadclaim

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

func TestEffectiveResourcesCarryCPUFloorIntoRuntimeRequest(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		config *sandboxstore.SandboxConfig
	}{
		{name: "stored template"},
		{name: "memory override", config: &sandboxstore.SandboxConfig{
			Resources: &sandboxstore.SandboxResourceConfig{Memory: "128Mi"},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spec := sandboxspec.TemplateSpec{MainContainer: sandboxspec.ContainerSpec{
				Resources: sandboxspec.ResourceQuota{CPU: "63m", Memory: "128Mi"},
			}}
			quota, err := effectiveResources(template.NewResourcePolicy("2Gi", "16Gi"), spec, test.config)
			if err != nil {
				t.Fatal(err)
			}
			request, err := runtimeResourceRequest(quota)
			if err != nil {
				t.Fatal(err)
			}
			if request.CPUMillicores != 150 || request.MemoryBytes != 128<<20 {
				t.Fatalf("runtime request = %#v, want 150m/128Mi", request)
			}
		})
	}
}
