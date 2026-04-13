package http

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	sharedssh "github.com/sandbox0-ai/sandbox0/pkg/sshgateway"
)

type fakeTeamMembershipLookup struct {
	calls int
	err   error
}

func (f *fakeTeamMembershipLookup) GetTeamMember(_ context.Context, teamID, userID string) (*identity.TeamMember, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &identity.TeamMember{TeamID: teamID, UserID: userID}, nil
}

func TestAuthorizeSSHUserForSandboxTeamUsesGrantsWithoutMembershipLookup(t *testing.T) {
	membership := &fakeTeamMembershipLookup{err: identity.ErrMemberNotFound}

	userID, err := authorizeSSHUserForSandboxTeam(context.Background(), "team-1", []sharedssh.AuthorizedGrant{
		{TeamID: "team-1", UserID: "user-1"},
	}, "", membership)
	if err != nil {
		t.Fatalf("authorizeSSHUserForSandboxTeam() error = %v", err)
	}
	if userID != "user-1" {
		t.Fatalf("userID = %q, want user-1", userID)
	}
	if membership.calls != 0 {
		t.Fatalf("membership calls = %d, want 0", membership.calls)
	}
}

func TestAuthorizeSSHUserForSandboxTeamRejectsGrantForDifferentTeam(t *testing.T) {
	membership := &fakeTeamMembershipLookup{}

	_, err := authorizeSSHUserForSandboxTeam(context.Background(), "team-1", []sharedssh.AuthorizedGrant{
		{TeamID: "team-2", UserID: "user-1"},
	}, "", membership)
	if !errors.Is(err, identity.ErrMemberNotFound) {
		t.Fatalf("authorizeSSHUserForSandboxTeam() error = %v, want %v", err, identity.ErrMemberNotFound)
	}
	if membership.calls != 0 {
		t.Fatalf("membership calls = %d, want 0", membership.calls)
	}
}

func TestAuthorizeSSHUserForSandboxTeamKeepsLegacyMembershipFallback(t *testing.T) {
	membership := &fakeTeamMembershipLookup{}

	userID, err := authorizeSSHUserForSandboxTeam(context.Background(), "team-1", nil, "user-1", membership)
	if err != nil {
		t.Fatalf("authorizeSSHUserForSandboxTeam() error = %v", err)
	}
	if userID != "user-1" {
		t.Fatalf("userID = %q, want user-1", userID)
	}
	if membership.calls != 1 {
		t.Fatalf("membership calls = %d, want 1", membership.calls)
	}
}
