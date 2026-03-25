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

func TestValidatePathCompatibilityRejectsWindowsForbiddenCharacter(t *testing.T) {
	issues := ValidatePathCompatibility(`/app/has:colon.txt`, FilesystemCapabilities{WindowsCompatiblePaths: true})
	if len(issues) != 1 {
		t.Fatalf("issues = %d, want 1", len(issues))
	}
	if issues[0].Code != IssueCodeWindowsForbiddenRune {
		t.Fatalf("issue code = %q, want %q", issues[0].Code, IssueCodeWindowsForbiddenRune)
	}
}

func TestValidatePathCompatibilityRejectsWindowsControlCharacter(t *testing.T) {
	issues := ValidatePathCompatibility("/app/control\x01.txt", FilesystemCapabilities{WindowsCompatiblePaths: true})
	if len(issues) != 1 {
		t.Fatalf("issues = %d, want 1", len(issues))
	}
	if issues[0].Code != IssueCodeWindowsControlCharacter {
		t.Fatalf("issue code = %q, want %q", issues[0].Code, IssueCodeWindowsControlCharacter)
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

func TestMergeFilesystemCapabilitiesTightensToPortableSubset(t *testing.T) {
	merged := MergeFilesystemCapabilities(
		FilesystemCapabilities{
			CaseSensitive:                   true,
			UnicodeNormalizationInsensitive: false,
			WindowsCompatiblePaths:          false,
		},
		FilesystemCapabilities{
			CaseSensitive:                   false,
			UnicodeNormalizationInsensitive: true,
			WindowsCompatiblePaths:          true,
		},
	)

	if merged.CaseSensitive {
		t.Fatal("CaseSensitive = true, want false")
	}
	if !merged.UnicodeNormalizationInsensitive {
		t.Fatal("UnicodeNormalizationInsensitive = false, want true")
	}
	if !merged.WindowsCompatiblePaths {
		t.Fatal("WindowsCompatiblePaths = false, want true")
	}
}

func TestNormalizeFilesystemCapabilitiesHonorsProvidedPortableCapabilities(t *testing.T) {
	caps := NormalizeFilesystemCapabilities("linux", true, &FilesystemCapabilities{
		CaseSensitive:                   false,
		UnicodeNormalizationInsensitive: false,
		WindowsCompatiblePaths:          true,
	})

	if caps.CaseSensitive {
		t.Fatal("CaseSensitive = true, want false")
	}
	if !caps.UnicodeNormalizationInsensitive {
		t.Fatal("UnicodeNormalizationInsensitive = false, want true when case-insensitive")
	}
	if !caps.WindowsCompatiblePaths {
		t.Fatal("WindowsCompatiblePaths = false, want true")
	}
}
