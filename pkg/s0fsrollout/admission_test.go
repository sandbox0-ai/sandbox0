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
		{name: "private team cohort", scope: naming.ScopeTeam, teamID: "team-a", templateID: "private", want: true},
		{name: "team cohort does not admit public", scope: naming.ScopePublic, teamID: "team-a", templateID: "private", want: false},
		{name: "template cohort admits public", scope: naming.ScopePublic, templateID: "public-canary", want: true},
		{name: "template cohort admits private", scope: naming.ScopeTeam, teamID: "team-b", templateID: "public-canary", want: true},
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
