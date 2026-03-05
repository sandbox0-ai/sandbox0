package licensing

import "testing"

func TestRequireLicenseFile(t *testing.T) {
	t.Parallel()

	if err := RequireLicenseFile(""); err == nil {
		t.Fatalf("expected error for empty license path")
	}
	if err := RequireLicenseFile("  "); err == nil {
		t.Fatalf("expected error for blank license path")
	}
	if err := RequireLicenseFile("/licenses/license.lic"); err != nil {
		t.Fatalf("expected non-empty path to pass, got: %v", err)
	}
}
