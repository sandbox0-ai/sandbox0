package server

import (
	"context"
	"errors"
	"testing"
	"time"

	managerclient "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"go.uber.org/zap/zaptest"
	"golang.org/x/crypto/ssh"
)

const authTestPublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ4dLZLZOA/asaP+5QO6t81jzbe5G4jrI2F+jbjL6TY8 sandbox0-e2e"

type fakeIdentityStore struct {
	key    *identity.UserSSHPublicKey
	keyErr error

	member    *identity.TeamMember
	memberErr error
}

func (f *fakeIdentityStore) GetUserSSHPublicKeyByFingerprint(context.Context, string) (*identity.UserSSHPublicKey, error) {
	if f.keyErr != nil {
		return nil, f.keyErr
	}
	return f.key, nil
}

func (f *fakeIdentityStore) GetTeamMember(context.Context, string, string) (*identity.TeamMember, error) {
	if f.memberErr != nil {
		return nil, f.memberErr
	}
	return f.member, nil
}

type fakeSandboxStore struct {
	sandbox   *mgr.Sandbox
	getErr    error
	resumed   bool
	resumeErr error
	getCalls  int
}

func (f *fakeSandboxStore) GetSandboxInternal(context.Context, string) (*mgr.Sandbox, error) {
	f.getCalls++
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.sandbox, nil
}

func (f *fakeSandboxStore) ResumeSandbox(context.Context, string, string, string) error {
	f.resumed = true
	if f.resumeErr != nil {
		return f.resumeErr
	}
	if f.sandbox != nil {
		f.sandbox.Paused = false
		f.sandbox.PowerState.Desired = mgr.SandboxPowerStateActive
		f.sandbox.InternalAddr = "http://10.0.0.8:8091"
	}
	return nil
}

func TestParseSSHUsername(t *testing.T) {
	tests := []struct {
		name     string
		username string
		want     string
		wantErr  bool
	}{
		{name: "sandbox id only", username: "sb_123", want: "sb_123"},
		{name: "main suffix", username: "sb_123+main", want: "sb_123"},
		{name: "empty", username: "", wantErr: true},
		{name: "unsupported target", username: "sb_123+sidecar", wantErr: true},
		{name: "invalid characters", username: "sb 123", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseSSHUsername(tc.username)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseSSHUsername() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("ParseSSHUsername() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAuthenticatorAuthenticate(t *testing.T) {
	parsedKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(authTestPublicKey))
	if err != nil {
		t.Fatalf("ParseAuthorizedKey() error = %v", err)
	}

	repo := &fakeIdentityStore{
		key:    &identity.UserSSHPublicKey{UserID: "user-1", FingerprintSHA256: ssh.FingerprintSHA256(parsedKey)},
		member: &identity.TeamMember{UserID: "user-1", TeamID: "team-1", Role: "owner"},
	}
	manager := &fakeSandboxStore{
		sandbox: &mgr.Sandbox{ID: "sb_123", TeamID: "team-1", InternalAddr: "http://10.0.0.8:8091", PowerState: mgr.SandboxPowerState{Desired: mgr.SandboxPowerStateActive}},
	}
	authenticator := NewAuthenticator(repo, manager, 2*time.Second, 10*time.Millisecond, zaptest.NewLogger(t))

	target, err := authenticator.Authenticate(context.Background(), "sb_123", parsedKey)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if target.SandboxID != "sb_123" || target.UserID != "user-1" || target.TeamID != "team-1" {
		t.Fatalf("Authenticate() target = %+v", target)
	}
	if target.ProcdURL != "http://10.0.0.8:8091" {
		t.Fatalf("ProcdURL = %q, want %q", target.ProcdURL, "http://10.0.0.8:8091")
	}
}

func TestAuthenticatorAuthenticateRejectsUnknownKey(t *testing.T) {
	parsedKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(authTestPublicKey))
	if err != nil {
		t.Fatalf("ParseAuthorizedKey() error = %v", err)
	}

	repo := &fakeIdentityStore{keyErr: identity.ErrSSHPublicKeyNotFound}
	authenticator := NewAuthenticator(repo, &fakeSandboxStore{}, time.Second, 10*time.Millisecond, zaptest.NewLogger(t))

	_, err = authenticator.Authenticate(context.Background(), "sb_123", parsedKey)
	if !errors.Is(err, ErrUnauthorizedSSHPublicKey) {
		t.Fatalf("Authenticate() error = %v, want %v", err, ErrUnauthorizedSSHPublicKey)
	}
}

func TestAuthenticatorAuthenticateRejectsNonMember(t *testing.T) {
	parsedKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(authTestPublicKey))
	if err != nil {
		t.Fatalf("ParseAuthorizedKey() error = %v", err)
	}

	repo := &fakeIdentityStore{
		key:       &identity.UserSSHPublicKey{UserID: "user-1", FingerprintSHA256: ssh.FingerprintSHA256(parsedKey)},
		memberErr: identity.ErrMemberNotFound,
	}
	manager := &fakeSandboxStore{
		sandbox: &mgr.Sandbox{ID: "sb_123", TeamID: "team-1", InternalAddr: "http://10.0.0.8:8091", PowerState: mgr.SandboxPowerState{Desired: mgr.SandboxPowerStateActive}},
	}
	authenticator := NewAuthenticator(repo, manager, time.Second, 10*time.Millisecond, zaptest.NewLogger(t))

	_, err = authenticator.Authenticate(context.Background(), "sb_123", parsedKey)
	if !errors.Is(err, ErrSandboxAccessDenied) {
		t.Fatalf("Authenticate() error = %v, want %v", err, ErrSandboxAccessDenied)
	}
}

func TestAuthenticatorAuthenticateResumesPausedSandbox(t *testing.T) {
	parsedKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(authTestPublicKey))
	if err != nil {
		t.Fatalf("ParseAuthorizedKey() error = %v", err)
	}

	repo := &fakeIdentityStore{
		key:    &identity.UserSSHPublicKey{UserID: "user-1", FingerprintSHA256: ssh.FingerprintSHA256(parsedKey)},
		member: &identity.TeamMember{UserID: "user-1", TeamID: "team-1", Role: "owner"},
	}
	manager := &fakeSandboxStore{
		sandbox: &mgr.Sandbox{ID: "sb_123", TeamID: "team-1", Paused: true, PowerState: mgr.SandboxPowerState{Desired: mgr.SandboxPowerStatePaused}},
	}
	authenticator := NewAuthenticator(repo, manager, time.Second, 10*time.Millisecond, zaptest.NewLogger(t))

	target, err := authenticator.Authenticate(context.Background(), "sb_123", parsedKey)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if !manager.resumed {
		t.Fatal("expected paused sandbox to be resumed")
	}
	if target.ProcdURL == "" {
		t.Fatal("expected non-empty ProcdURL after resume")
	}
}

func TestAuthenticatorAuthenticateHandlesMissingSandbox(t *testing.T) {
	parsedKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(authTestPublicKey))
	if err != nil {
		t.Fatalf("ParseAuthorizedKey() error = %v", err)
	}

	repo := &fakeIdentityStore{key: &identity.UserSSHPublicKey{UserID: "user-1"}}
	manager := &fakeSandboxStore{getErr: managerclient.ErrSandboxNotFound}
	authenticator := NewAuthenticator(repo, manager, time.Second, 10*time.Millisecond, zaptest.NewLogger(t))

	_, err = authenticator.Authenticate(context.Background(), "sb_123", parsedKey)
	if !errors.Is(err, ErrSandboxUnavailable) {
		t.Fatalf("Authenticate() error = %v, want %v", err, ErrSandboxUnavailable)
	}
}
