package template

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

func TestResolveRootFSLogicalSizeUsesDefaultAndExactValues(t *testing.T) {
	for name, raw := range map[string]string{
		"default":  "",
		"explicit": "768Mi",
	} {
		t.Run(name, func(t *testing.T) {
			spec := sandboxspec.TemplateSpec{}
			spec.MainContainer.Resources.EphemeralStorage = raw
			got, err := ResolveRootFSLogicalSize(spec)
			if err != nil {
				t.Fatal(err)
			}
			want := int64(8 << 30)
			if raw != "" {
				want = 768 << 20
			}
			if got != want {
				t.Fatalf("logical size = %d, want %d", got, want)
			}
		})
	}
}

func TestResolveRootFSLogicalSizeRejectsUnsafeValues(t *testing.T) {
	values := []string{
		"1.5",
		"299Mi",
		"314572801",
		"104857601Mi",
	}
	for _, raw := range values {
		t.Run(raw, func(t *testing.T) {
			spec := sandboxspec.TemplateSpec{}
			spec.MainContainer.Resources.EphemeralStorage = raw
			if _, err := ResolveRootFSLogicalSize(spec); err == nil {
				t.Fatal("unsafe logical size was accepted")
			}
		})
	}

	spec := sandboxspec.TemplateSpec{}
	spec.MainContainer.Resources.EphemeralStorage = "300Mi"
	if got, err := ResolveRootFSLogicalSize(spec); err != nil || got != rootfsartifact.MinimumLogicalSizeBytes {
		t.Fatalf("minimum logical size = %d, %v", got, err)
	}
}
