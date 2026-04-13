package sshgateway

import "testing"

func TestEncodeParseAuthorizedGrants(t *testing.T) {
	raw := EncodeAuthorizedGrants([]AuthorizedGrant{
		{TeamID: " team-1 ", UserID: " user-1 "},
		{TeamID: "team-1", UserID: "user-1"},
		{TeamID: "team-2", UserID: "user-2"},
		{TeamID: "", UserID: "user-3"},
	})
	if raw != "team-1=user-1,team-2=user-2" {
		t.Fatalf("encoded grants = %q", raw)
	}

	grants := ParseAuthorizedGrants(raw + ",invalid,team-3=")
	if len(grants) != 2 {
		t.Fatalf("grants len = %d, want 2", len(grants))
	}
	if got, ok := UserIDForTeam(grants, "team-2"); !ok || got != "user-2" {
		t.Fatalf("UserIDForTeam(team-2) = %q, %v", got, ok)
	}
	if got, ok := UserIDForTeam(grants, "team-3"); ok || got != "" {
		t.Fatalf("UserIDForTeam(team-3) = %q, %v", got, ok)
	}
}
