package functions

import "testing"

func TestSlugFromNameNormalizesDNSLabel(t *testing.T) {
	got := SlugFromName(" My API / v1 ")
	if got != "my-api-v1" {
		t.Fatalf("SlugFromName() = %q, want my-api-v1", got)
	}
}

func TestDomainLabelIncludesStableTeamSuffix(t *testing.T) {
	got := DomainLabel("api", "team-123")
	again := DomainLabel("api", "team-123")
	if got != again {
		t.Fatalf("DomainLabel not stable: %q != %q", got, again)
	}
	if got == "api" {
		t.Fatalf("DomainLabel() = %q, expected team suffix", got)
	}
	if err := ValidateAlias("production"); err != nil {
		t.Fatalf("ValidateAlias(production): %v", err)
	}
}
