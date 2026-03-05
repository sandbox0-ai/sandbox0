package licensing

import (
	"errors"
	"testing"
)

func TestStaticEntitlements(t *testing.T) {
	t.Parallel()

	entitlements := NewStaticEntitlements(FeatureSSO)

	if !entitlements.Enabled(FeatureSSO) {
		t.Fatalf("expected %q to be enabled", FeatureSSO)
	}
	if entitlements.Enabled(FeatureMultiCluster) {
		t.Fatalf("expected %q to be disabled", FeatureMultiCluster)
	}
	if err := entitlements.Require(FeatureMultiCluster); err == nil {
		t.Fatalf("expected missing feature error")
	}
}

func TestFileEntitlements_RequireIncludesLoadError(t *testing.T) {
	t.Parallel()

	entitlements := LoadFileEntitlements("/path/does/not/exist/license.lic")

	err := entitlements.Require(FeatureSSO)
	if err == nil {
		t.Fatalf("expected missing feature error")
	}

	var notLicensedErr *FeatureNotLicensedError
	if !errors.As(err, &notLicensedErr) {
		t.Fatalf("expected FeatureNotLicensedError, got %T", err)
	}
	if notLicensedErr.Feature != FeatureSSO {
		t.Fatalf("unexpected feature: %q", notLicensedErr.Feature)
	}
	if notLicensedErr.Cause == nil {
		t.Fatalf("expected load cause to be preserved")
	}
}
