package common

import (
	"context"
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestAppendEnterpriseLicenseVolume_DefaultPath(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{}
	infra.Name = "demo"
	mounts, volumes := AppendEnterpriseLicenseVolume(infra, "", nil, nil)
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
	if volumes[0].Secret == nil || volumes[0].Secret.SecretName != "demo-enterprise-license" {
		t.Fatalf("unexpected secret name: %#v", volumes[0].Secret)
	}
	if mounts[0].SubPath != EnterpriseLicenseSecretKey {
		t.Fatalf("unexpected subPath: %q", mounts[0].SubPath)
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

func TestNormalizeEnterpriseLicenseFile(t *testing.T) {
	t.Run("sets default path when required", func(t *testing.T) {
		licenseFile := ""
		NormalizeEnterpriseLicenseFile(&licenseFile, true)
		if licenseFile != EnterpriseLicenseDefaultPath {
			t.Fatalf("expected default path %q, got %q", EnterpriseLicenseDefaultPath, licenseFile)
		}
	})

	t.Run("keeps explicit path when required", func(t *testing.T) {
		licenseFile := "/custom/license.lic"
		NormalizeEnterpriseLicenseFile(&licenseFile, true)
		if licenseFile != "/custom/license.lic" {
			t.Fatalf("expected explicit path to be preserved, got %q", licenseFile)
		}
	})

	t.Run("does nothing when not required", func(t *testing.T) {
		licenseFile := ""
		NormalizeEnterpriseLicenseFile(&licenseFile, false)
		if licenseFile != "" {
			t.Fatalf("expected license path to remain empty, got %q", licenseFile)
		}
	})
}

func TestResolveEnterpriseLicenseSecretRef(t *testing.T) {
	t.Run("uses backward compatible default", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{}
		infra.Name = "demo"
		ref := ResolveEnterpriseLicenseSecretRef(infra)
		if ref.Name != "demo-enterprise-license" {
			t.Fatalf("expected default secret name, got %q", ref.Name)
		}
		if ref.Key != EnterpriseLicenseSecretKey {
			t.Fatalf("expected default secret key, got %q", ref.Key)
		}
	})

	t.Run("uses configured secret ref", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				EnterpriseLicense: &infrav1alpha1.EnterpriseLicenseConfig{
					SecretRef: infrav1alpha1.SecretKeyRef{
						Name: "custom-license",
						Key:  "signed.lic",
					},
				},
			},
		}
		infra.Name = "demo"
		ref := ResolveEnterpriseLicenseSecretRef(infra)
		if ref.Name != "custom-license" {
			t.Fatalf("expected configured secret name, got %q", ref.Name)
		}
		if ref.Key != "signed.lic" {
			t.Fatalf("expected configured secret key, got %q", ref.Key)
		}
	})
}
