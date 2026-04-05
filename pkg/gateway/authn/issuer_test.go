package authn

import (
	"errors"
	"testing"
	"time"
)

func TestIssueTokenPair_UsesUniqueSessionIDs(t *testing.T) {
	fixedNow := time.Now().UTC().Truncate(time.Second)
	sessionIDs := []string{"session-1", "session-2"}

	issuer := newIssuerWithDeps(
		"test-issuer",
		"test-secret",
		time.Minute,
		time.Hour,
		func() time.Time { return fixedNow },
		func() (string, error) {
			if len(sessionIDs) == 0 {
				return "", errors.New("no session IDs left")
			}
			id := sessionIDs[0]
			sessionIDs = sessionIDs[1:]
			return id, nil
		},
	)

	first, err := issuer.IssueTokenPair("user-1", "team-1", "admin", "user@example.com", "User", false, []TeamGrant{{TeamID: "team-1", TeamRole: "admin", HomeRegionID: "aws-us-east-1"}})
	if err != nil {
		t.Fatalf("issue first token pair: %v", err)
	}
	second, err := issuer.IssueTokenPair("user-1", "team-1", "admin", "user@example.com", "User", false, []TeamGrant{{TeamID: "team-1", TeamRole: "admin", HomeRegionID: "aws-us-east-1"}})
	if err != nil {
		t.Fatalf("issue second token pair: %v", err)
	}

	if first.AccessToken == second.AccessToken {
		t.Fatalf("expected access tokens to differ for separate issued sessions")
	}
	if first.RefreshToken == second.RefreshToken {
		t.Fatalf("expected refresh tokens to differ for separate issued sessions")
	}

	firstAccessClaims, err := issuer.ValidateAccessToken(first.AccessToken)
	if err != nil {
		t.Fatalf("validate first access token: %v", err)
	}
	firstRefreshClaims, err := issuer.ValidateRefreshToken(first.RefreshToken)
	if err != nil {
		t.Fatalf("validate first refresh token: %v", err)
	}
	secondRefreshClaims, err := issuer.ValidateRefreshToken(second.RefreshToken)
	if err != nil {
		t.Fatalf("validate second refresh token: %v", err)
	}

	if firstAccessClaims.ID != "session-1" {
		t.Fatalf("first access token jti = %q, want session-1", firstAccessClaims.ID)
	}
	if firstRefreshClaims.ID != "session-1" {
		t.Fatalf("first refresh token jti = %q, want session-1", firstRefreshClaims.ID)
	}
	if secondRefreshClaims.ID != "session-2" {
		t.Fatalf("second refresh token jti = %q, want session-2", secondRefreshClaims.ID)
	}
	if len(firstAccessClaims.TeamGrants) != 1 || firstAccessClaims.TeamGrants[0].TeamID != "team-1" {
		t.Fatalf("expected access token to include team grants, got %+v", firstAccessClaims.TeamGrants)
	}
}
