package nomadclaim

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestLoadRuntimeClassCatalogResolvesWithoutResourceShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-classes.json")
	payload := `{
  "version": 3,
  "classes": [{
    "name": "amd64-systrap-directfs",
    "cluster_id": "cluster-1",
    "artifact_platform": {"os": "linux", "architecture": "amd64"},
    "compatibility": {
      "version": 2,
      "architecture": "amd64",
      "driver_version": "0.1.0",
      "runsc_version": "runsc release-20260820.0",
      "platform": "systrap",
      "overlay2": "none",
      "file_access": "shared",
      "directfs": true,
      "command": "/procd",
      "procd_port": 49983,
      "runtime_mode": "static",
      "security_class": "standard"
    }
  }]
}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	catalog, err := LoadRuntimeClassCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	class, err := catalog.Resolve("cluster-1")
	if err != nil {
		t.Fatal(err)
	}
	if class.Name != "amd64-systrap-directfs" || class.ClusterID != "cluster-1" ||
		class.ArtifactPlatform.OS != "linux" || class.ArtifactPlatform.Architecture != "amd64" ||
		class.Compatibility.Version != protocol.RuntimeCompatibilityVersion ||
		class.Compatibility.RuntimeMode != runtimecontrol.ControlModeStatic ||
		class.Compatibility.SecurityClass != "standard" || class.CompatibilityDigest == "" {
		t.Fatalf("runtime class = %+v", class)
	}
	if _, err := catalog.Resolve("cluster-2"); !errors.Is(err, ErrRuntimeClassUnavailable) {
		t.Fatalf("missing cluster error = %v", err)
	}
}

func TestRuntimeClassCatalogRejectsAmbiguousOrLooseInput(t *testing.T) {
	directory := t.TempDir()
	compatibility := `{
      "version":2,"architecture":"amd64","driver_version":"0.1.0",
      "runsc_version":"runsc-1","platform":"systrap","overlay2":"none",
      "file_access":"shared","directfs":true,"command":"/procd",
      "procd_port":49983,"runtime_mode":"static","security_class":"standard"
    }`
	class := func(name, cluster string) string {
		return `{"name":"` + name + `","cluster_id":"` + cluster +
			`","artifact_platform":{"os":"linux","architecture":"amd64"},"compatibility":` + compatibility + `}`
	}
	tests := map[string]string{
		"fixed-resource v2": `{"version":2,"classes":[` + class("one", "cluster-1") + `]}`,
		"unknown field":     `{"version":3,"classes":[` + class("one", "cluster-1") + `],"extra":true}`,
		"trailing value":    `{"version":3,"classes":[` + class("one", "cluster-1") + `]} {}`,
		"duplicate name":    `{"version":3,"classes":[` + class("one", "cluster-1") + `,` + class("one", "cluster-2") + `]}`,
		"resource field": `{"version":3,"classes":[{"name":"one","cluster_id":"cluster-1",` +
			`"template_cpu":"1","artifact_platform":{"os":"linux","architecture":"amd64"},"compatibility":` + compatibility + `}]}`,
		"platform mismatch": `{"version":3,"classes":[{"name":"one","cluster_id":"cluster-1",` +
			`"artifact_platform":{"os":"linux","architecture":"arm64"},"compatibility":` + compatibility + `}]}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(directory, name+".json")
			if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadRuntimeClassCatalog(path); err == nil {
				t.Fatal("invalid runtime class catalog was accepted")
			}
		})
	}
	if _, err := LoadRuntimeClassCatalog("relative.json"); err == nil {
		t.Fatal("relative runtime class catalog was accepted")
	}
}

func TestRuntimeClassCatalogRejectsImplicitAmbiguousSelection(t *testing.T) {
	catalog := &RuntimeClassCatalog{classes: []RuntimeClass{
		{Name: "standard", ClusterID: "cluster-1"},
		{Name: "gpu", ClusterID: "cluster-1"},
	}}
	if _, err := catalog.Resolve("cluster-1"); !errors.Is(err, ErrRuntimeClassAmbiguous) {
		t.Fatalf("ambiguous class error = %v", err)
	}
}

func TestRuntimeClassCatalogResolvesLegacyNomadMeteringResources(t *testing.T) {
	catalog := &RuntimeClassCatalog{classes: []RuntimeClass{{Name: "one", ClusterID: "cluster-1"}}}
	record := &sandboxstore.SandboxRecord{
		RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad,
		ClusterID:      "cluster-1",
	}
	record.TemplateSpec.MainContainer.Resources.CPU = resource.MustParse("1500m")
	record.TemplateSpec.MainContainer.Resources.Memory = resource.MustParse("1Gi")

	millicpu, memoryMiB, err := catalog.ResolveLegacyMeteringResources(
		record,
		templatepkg.NewResourcePolicy("1Gi", "8Gi"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if millicpu != 1500 || memoryMiB != 1024 {
		t.Fatalf("metering resources = %dm/%dMiB", millicpu, memoryMiB)
	}

	record.ClusterID = "cluster-2"
	if _, _, err := catalog.ResolveLegacyMeteringResources(
		record,
		templatepkg.NewResourcePolicy("1Gi", "8Gi"),
	); err == nil {
		t.Fatal("persisted cluster mismatch was accepted")
	}
}
