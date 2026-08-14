package s0fsrollout

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

func TestAdmissionCohortScope(t *testing.T) {
	admission, err := NewAdmission("cold", []string{" team-a "}, []string{"public-canary"}, true, false)
	if err != nil {
		t.Fatalf("NewAdmission() error = %v", err)
	}
	tests := []struct {
		name       string
		scope      string
		teamID     string
		templateID string
		want       bool
	}{
		{name: "private exact cohort", scope: naming.ScopeTeam, teamID: "team-a", templateID: "public-canary", want: true},
		{name: "private team alone does not match", scope: naming.ScopeTeam, teamID: "team-a", templateID: "private", want: false},
		{name: "team cohort does not admit public", scope: naming.ScopePublic, teamID: "team-a", templateID: "private", want: false},
		{name: "template cohort admits public", scope: naming.ScopePublic, templateID: "public-canary", want: true},
		{name: "private template alone does not match", scope: naming.ScopeTeam, teamID: "team-b", templateID: "public-canary", want: false},
		{name: "unknown scope fails closed", scope: "unknown", teamID: "team-a", templateID: "public-canary", want: false},
		{name: "unmatched", scope: naming.ScopeTeam, teamID: "team-b", templateID: "private", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := admission.Admits(tt.scope, tt.teamID, tt.templateID); got != tt.want {
				t.Fatalf("Admits() = %v, want %v", got, tt.want)
			}
		})
	}
	if admission.Mode() != AdmissionModeCold || admission.UsesSharedCarrier() || !admission.RejectLegacyClaims() {
		t.Fatalf("admission = %#v, want cold fail-closed policy", admission)
	}
}

func TestAdmissionOffOverridesAllowlist(t *testing.T) {
	admission, err := NewAdmission("off", []string{"team-a"}, []string{"template-a"}, true, true)
	if err != nil {
		t.Fatalf("NewAdmission() error = %v", err)
	}
	if admission.Admits(naming.ScopeTeam, "team-a", "template-a") {
		t.Fatal("off admission unexpectedly admitted a template")
	}
}

func TestAdmissionPreservesLegacySharedCarrierConfiguration(t *testing.T) {
	admission, err := NewAdmission("", nil, nil, false, true)
	if err != nil {
		t.Fatalf("NewAdmission() error = %v", err)
	}
	if !admission.Admits(naming.ScopePublic, "", "any-template") || !admission.UsesSharedCarrier() {
		t.Fatal("legacy shared carrier configuration did not admit all templates")
	}
}

func TestAdmissionRejectsUnknownMode(t *testing.T) {
	if _, err := NewAdmission("gradual", nil, nil, false, false); err == nil {
		t.Fatal("NewAdmission() error = nil, want unsupported mode error")
	}
}

func TestCohortMatchesPrivateTeamAndLogicalTemplate(t *testing.T) {
	cohort := NewCohort([]string{" team-a "}, []string{" python "})
	if cohort.Empty() {
		t.Fatal("explicit cohort is empty")
	}
	if !cohort.Matches(naming.ScopeTeam, "team-a", "python") {
		t.Fatal("exact private team and template selectors did not match")
	}
	if !cohort.Matches(naming.ScopePublic, "", "python") {
		t.Fatal("public template selector did not match")
	}
	if cohort.Matches(naming.ScopeTeam, "team-a", "other") {
		t.Fatal("private team selector matched without the configured template")
	}
	if cohort.Matches(naming.ScopeTeam, "team-b", "python") {
		t.Fatal("private template selector matched without the configured team")
	}
	if cohort.Matches(naming.ScopePublic, "team-a", "other") {
		t.Fatal("team selector matched a public template")
	}
}

func TestCohortPreservesSingleSelectorBehavior(t *testing.T) {
	if !NewCohort([]string{"team-a"}, nil).Matches(naming.ScopeTeam, "team-a", "other") {
		t.Fatal("team-only selector did not match a private template")
	}
	if !NewCohort(nil, []string{"python"}).Matches(naming.ScopeTeam, "team-b", "python") {
		t.Fatal("template-only selector did not match a private template")
	}
}

func TestEmptyCohortMatchesNothing(t *testing.T) {
	cohort := NewCohort(nil, nil)
	if !cohort.Empty() || cohort.Matches(naming.ScopeTeam, "team-a", "python") {
		t.Fatal("empty cohort must remain empty and match nothing")
	}
}
