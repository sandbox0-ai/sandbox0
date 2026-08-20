package nomadclaim

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func TestLoadProfileCatalogResolvesExactResourceShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-profiles.json")
	payload := `{
  "version": 2,
  "profiles": [{
    "name": "amd64-1cpu-1g",
    "cluster_id": "cluster-1",
    "template_cpu": "1",
    "template_memory": "1Gi",
    "artifact_platform": {"os": "linux", "architecture": "amd64"},
    "compatibility": {
      "version": 1,
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
      "cpu_period": 100000,
      "cpu_quota": 100000,
      "cpu_shares": 1024,
      "memory_limit_bytes": 1073741824
    }
  }]
}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	catalog, err := LoadProfileCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	profile, ok := catalog.Resolve(mustQuantity("1"), mustQuantity("1Gi"))
	if !ok {
		t.Fatal("exact resource profile was not resolved")
	}
	if profile.Name != "amd64-1cpu-1g" || profile.ClusterID != "cluster-1" ||
		profile.ArtifactPlatform.OS != "linux" || profile.ArtifactPlatform.Architecture != "amd64" ||
		profile.Compatibility.Version != protocol.RuntimeCompatibilityVersion ||
		profile.Compatibility.RuntimeMode != runtimecontrol.ControlModeStatic ||
		profile.CompatibilityDigest == "" {
		t.Fatalf("profile = %+v", profile)
	}
	if _, ok := catalog.Resolve(mustQuantity("2"), mustQuantity("1Gi")); ok {
		t.Fatal("non-matching resource profile was resolved")
	}
}

func TestLoadProfileCatalogRejectsAmbiguousOrLooseInput(t *testing.T) {
	directory := t.TempDir()
	compatibility := `{
      "version":1,"architecture":"amd64","driver_version":"0.1.0",
      "runsc_version":"runsc-1","platform":"systrap","overlay2":"none",
      "file_access":"shared","directfs":true,"command":"/procd",
      "procd_port":49983,"runtime_mode":"static","cpu_period":0,
      "cpu_quota":0,"cpu_shares":0,"memory_limit_bytes":0
    }`
	profile := func(name, cpu string) string {
		return `{"name":"` + name + `","cluster_id":"cluster-1","template_cpu":"` + cpu +
			`","template_memory":"1Gi","artifact_platform":{"os":"linux","architecture":"amd64"},"compatibility":` + compatibility + `}`
	}
	tests := map[string]string{
		"legacy version":   `{"version":1,"profiles":[` + profile("one", "1") + `]}`,
		"unknown field":    `{"version":2,"profiles":[` + profile("one", "1") + `],"extra":true}`,
		"trailing value":   `{"version":2,"profiles":[` + profile("one", "1") + `]} {}`,
		"duplicate name":   `{"version":2,"profiles":[` + profile("one", "1") + "," + profile("one", "2") + `]}`,
		"ambiguous shape":  `{"version":2,"profiles":[` + profile("one", "1") + "," + profile("two", "1") + `]}`,
		"noncanonical cpu": `{"version":2,"profiles":[` + profile("one", "1000m") + `]}`,
		"platform mismatch": `{"version":2,"profiles":[{"name":"one","cluster_id":"cluster-1",` +
			`"template_cpu":"1","template_memory":"1Gi","artifact_platform":{"os":"linux","architecture":"arm64"},` +
			`"compatibility":` + compatibility + `}]}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(directory, name+".json")
			if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadProfileCatalog(path); err == nil {
				t.Fatal("invalid profile catalog was accepted")
			}
		})
	}
	if _, err := LoadProfileCatalog("relative.json"); err == nil {
		t.Fatal("relative profile catalog was accepted")
	}
}
