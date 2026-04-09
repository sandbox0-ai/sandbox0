package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	sharedssh "github.com/sandbox0-ai/sandbox0/pkg/sshgateway"
	"go.uber.org/zap/zaptest"
	"golang.org/x/crypto/ssh"
)

const authTestPublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ4dLZLZOA/asaP+5QO6t81jzbe5G4jrI2F+jbjL6TY8 sandbox0-e2e"

type fakeIdentityStore struct {
	key    *identity.UserSSHPublicKey
	keyErr error
}

func (f *fakeIdentityStore) GetUserSSHPublicKeyByFingerprint(context.Context, string) (*identity.UserSSHPublicKey, error) {
	if f.keyErr != nil {
		return nil, f.keyErr
	}
	return f.key, nil
}

type fakeSandboxResolver struct {
	targets []*sharedssh.ResolvedTarget
	errs    []error
	calls   int
}

func (f *fakeSandboxResolver) ResolveSandbox(context.Context, string, string) (*sharedssh.ResolvedTarget, error) {
	f.calls++
	if len(f.errs) > 0 {
		err := f.errs[0]
		if len(f.errs) > 1 {
			f.errs = f.errs[1:]
		}
		if err != nil {
			return nil, err
		}
	}
	if len(f.targets) == 0 {
		return nil, ErrSandboxUnavailable
	}
	target := f.targets[0]
	if len(f.targets) > 1 {
		f.targets = f.targets[1:]
	}
	return target, nil
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
		key: &identity.UserSSHPublicKey{
			UserID:            "user-1",
			FingerprintSHA256: ssh.FingerprintSHA256(parsedKey),
		},
	}
	resolver := &fakeSandboxResolver{
		targets: []*sharedssh.ResolvedTarget{{
			SandboxID: "sb_123",
			TeamID:    "team-1",
			UserID:    "user-1",
			ProcdURL:  "http://10.0.0.8:8091",
		}},
	}
	authenticator := NewAuthenticator(repo, resolver, 2*time.Second, 10*time.Millisecond, zaptest.NewLogger(t))

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
	authenticator := NewAuthenticator(repo, &fakeSandboxResolver{}, time.Second, 10*time.Millisecond, zaptest.NewLogger(t))

	_, err = authenticator.Authenticate(context.Background(), "sb_123", parsedKey)
	if !errors.Is(err, ErrUnauthorizedSSHPublicKey) {
		t.Fatalf("Authenticate() error = %v, want %v", err, ErrUnauthorizedSSHPublicKey)
	}
}

func TestAuthenticatorAuthenticateRejectsAccessDenied(t *testing.T) {
	parsedKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(authTestPublicKey))
	if err != nil {
		t.Fatalf("ParseAuthorizedKey() error = %v", err)
	}

	repo := &fakeIdentityStore{
		key: &identity.UserSSHPublicKey{
			UserID:            "user-1",
			FingerprintSHA256: ssh.FingerprintSHA256(parsedKey),
		},
	}
	authenticator := NewAuthenticator(repo, &fakeSandboxResolver{errs: []error{ErrSandboxAccessDenied}}, time.Second, 10*time.Millisecond, zaptest.NewLogger(t))

	_, err = authenticator.Authenticate(context.Background(), "sb_123", parsedKey)
	if !errors.Is(err, ErrSandboxAccessDenied) {
		t.Fatalf("Authenticate() error = %v, want %v", err, ErrSandboxAccessDenied)
	}
}

func TestAuthenticatorAuthenticatePollsWhileSandboxWakesUp(t *testing.T) {
	parsedKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(authTestPublicKey))
	if err != nil {
		t.Fatalf("ParseAuthorizedKey() error = %v", err)
	}

	repo := &fakeIdentityStore{
		key: &identity.UserSSHPublicKey{
			UserID:            "user-1",
			FingerprintSHA256: ssh.FingerprintSHA256(parsedKey),
		},
	}
	resolver := &fakeSandboxResolver{
		errs: []error{ErrSandboxWakingUp, nil},
		targets: []*sharedssh.ResolvedTarget{{
			SandboxID: "sb_123",
			TeamID:    "team-1",
			UserID:    "user-1",
			ProcdURL:  "http://10.0.0.8:8091",
		}},
	}
	authenticator := NewAuthenticator(repo, resolver, time.Second, 10*time.Millisecond, zaptest.NewLogger(t))

	target, err := authenticator.Authenticate(context.Background(), "sb_123", parsedKey)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if target.ProcdURL == "" {
		t.Fatal("expected non-empty ProcdURL after wake-up")
	}
	if resolver.calls < 2 {
		t.Fatalf("resolver.calls = %d, want >= 2", resolver.calls)
	}
}

func TestAuthenticatorAuthenticateHandlesMissingSandbox(t *testing.T) {
	parsedKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(authTestPublicKey))
	if err != nil {
		t.Fatalf("ParseAuthorizedKey() error = %v", err)
	}

	repo := &fakeIdentityStore{key: &identity.UserSSHPublicKey{UserID: "user-1"}}
	authenticator := NewAuthenticator(repo, &fakeSandboxResolver{errs: []error{ErrSandboxUnavailable}}, time.Second, 10*time.Millisecond, zaptest.NewLogger(t))

	_, err = authenticator.Authenticate(context.Background(), "sb_123", parsedKey)
	if !errors.Is(err, ErrSandboxUnavailable) {
		t.Fatalf("Authenticate() error = %v, want %v", err, ErrSandboxUnavailable)
	}
}
