package pathnorm

import "testing"

func TestDefaultFilesystemCapabilitiesForWindows(t *testing.T) {
	caps := DefaultFilesystemCapabilities("windows", true)
	if caps.CaseSensitive {
		t.Fatal("CaseSensitive = true, want false for windows")
	}
	if !caps.UnicodeNormalizationInsensitive {
		t.Fatal("UnicodeNormalizationInsensitive = false, want true for windows")
	}
	if !caps.WindowsCompatiblePaths {
		t.Fatal("WindowsCompatiblePaths = false, want true for windows")
	}
}

func TestValidatePathCompatibilityRejectsWindowsReservedName(t *testing.T) {
	issues := ValidatePathCompatibility("/app/CON.txt", FilesystemCapabilities{WindowsCompatiblePaths: true})
	if len(issues) != 1 {
		t.Fatalf("issues = %d, want 1", len(issues))
	}
	if issues[0].Code != IssueCodeWindowsReservedName {
		t.Fatalf("issue code = %q, want %q", issues[0].Code, IssueCodeWindowsReservedName)
	}
}

func TestValidatePathCompatibilityRejectsTrailingDotSpace(t *testing.T) {
	issues := ValidatePathCompatibility("/app/readme. ", FilesystemCapabilities{WindowsCompatiblePaths: true})
	if len(issues) != 1 {
		t.Fatalf("issues = %d, want 1", len(issues))
	}
	if issues[0].Code != IssueCodeWindowsTrailingDotSpace {
		t.Fatalf("issue code = %q, want %q", issues[0].Code, IssueCodeWindowsTrailingDotSpace)
	}
}

func TestCompatibilityPathKeyHonorsCapabilities(t *testing.T) {
	caps := FilesystemCapabilities{
		CaseSensitive:                   false,
		UnicodeNormalizationInsensitive: true,
	}
	got := CompatibilityPathKey("/app/Caf\u00e9.txt", caps)
	want := "/app/cafe\u0301.txt"
	if got != want {
		t.Fatalf("CompatibilityPathKey() = %q, want %q", got, want)
	}
}
