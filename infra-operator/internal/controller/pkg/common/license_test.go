package common

import (
	"context"
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/infra/infra-operator/api/v1alpha1"
)

func TestAppendEnterpriseLicenseVolume_DefaultPath(t *testing.T) {
	mounts, volumes := AppendEnterpriseLicenseVolume("demo", "", nil, nil)
	if len(mounts) != 1 {
		t.Fatalf("expected 1 volume mount, got %d", len(mounts))
	}
	if mounts[0].MountPath != EnterpriseLicenseDefaultPath {
		t.Fatalf("unexpected mount path: %q", mounts[0].MountPath)
	}
	if len(volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(volumes))
	}
	if volumes[0].Name != "enterprise-license" {
		t.Fatalf("unexpected volume name: %q", volumes[0].Name)
	}
}

func TestEnsureEnterpriseLicense_NoOpWhenNotRequired(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{}
	licenseFile := ""

	if err := EnsureEnterpriseLicense(context.Background(), nil, infra, &licenseFile, false, "test"); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if licenseFile != "" {
		t.Fatalf("license file should stay unchanged, got: %q", licenseFile)
	}
}
