package authn

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestIssueTokenPair_UsesUniqueSessionIDs(t *testing.T) {
	fixedNow := time.Now().UTC().Truncate(time.Second)
	sessionIDs := []string{"session-1", "session-2"}

	issuer := newIssuerWithDeps(
		"test-issuer",
		jwt.SigningMethodHS256,
		[]byte("test-secret"),
		[]byte("test-secret"),
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

	first, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, []TeamGrant{{TeamID: "team-1", TeamRole: "admin", HomeRegionID: "aws-us-east-1"}})
	if err != nil {
		t.Fatalf("issue first token pair: %v", err)
	}
	second, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, []TeamGrant{{TeamID: "team-1", TeamRole: "admin", HomeRegionID: "aws-us-east-1"}})
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

func TestEd25519IssuerAndVerifier(t *testing.T) {
	privateKeyPEM, publicKeyPEM, err := internalauth.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("generate key pair: %v", err)
	}
	privateKey, err := internalauth.LoadEd25519PrivateKey(privateKeyPEM)
	if err != nil {
		t.Fatalf("load private key: %v", err)
	}
	publicKey, err := internalauth.LoadEd25519PublicKey(publicKeyPEM)
	if err != nil {
		t.Fatalf("load public key: %v", err)
	}

	issuer := NewIssuerWithEd25519("test-issuer", privateKey, time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, []TeamGrant{{TeamID: "team-1", TeamRole: "admin", HomeRegionID: "aws-us-east-1"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	verifier := NewVerifierWithEd25519("test-issuer", publicKey)
	claims, err := verifier.ValidateAccessToken(tokens.AccessToken)
	if err != nil {
		t.Fatalf("validate access token: %v", err)
	}
	if claims.UserID != "user-1" {
		t.Fatalf("unexpected user id: %q", claims.UserID)
	}
	if _, err := verifier.IssueTokenPair("user-1", "user@example.com", "User", false, nil); !errors.Is(err, ErrJWTSigningDisabled) {
		t.Fatalf("expected verifier-only issuer to reject signing, got %v", err)
	}

	wrongPrivatePEM, wrongPublicPEM, err := internalauth.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("generate wrong key pair: %v", err)
	}
	_ = wrongPrivatePEM
	wrongPublicKey, err := internalauth.LoadEd25519PublicKey(wrongPublicPEM)
	if err != nil {
		t.Fatalf("load wrong public key: %v", err)
	}
	wrongVerifier := NewVerifierWithEd25519("test-issuer", wrongPublicKey)
	if _, err := wrongVerifier.ValidateAccessToken(tokens.AccessToken); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected wrong public key to fail validation, got %v", err)
	}
}

func TestNewIssuerFromConfigSupportsKeyFiles(t *testing.T) {
	privateKeyPEM, publicKeyPEM, err := internalauth.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("generate key pair: %v", err)
	}
	privateFile, err := os.CreateTemp(t.TempDir(), "jwt-private-*.pem")
	if err != nil {
		t.Fatalf("create private key file: %v", err)
	}
	defer privateFile.Close()
	if _, err := privateFile.Write(privateKeyPEM); err != nil {
		t.Fatalf("write private key file: %v", err)
	}
	publicFile, err := os.CreateTemp(t.TempDir(), "jwt-public-*.pem")
	if err != nil {
		t.Fatalf("create public key file: %v", err)
	}
	defer publicFile.Close()
	if _, err := publicFile.Write(publicKeyPEM); err != nil {
		t.Fatalf("write public key file: %v", err)
	}

	issuer, err := NewIssuerFromConfig("global-gateway", "", "", "", privateFile.Name(), publicFile.Name(), time.Minute, time.Hour)
	if err != nil {
		t.Fatalf("NewIssuerFromConfig signer: %v", err)
	}
	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, []TeamGrant{{TeamID: "team-1", TeamRole: "admin", HomeRegionID: "aws-us-east-1"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	verifier, err := NewIssuerFromConfig("global-gateway", "", "", "", "", publicFile.Name(), time.Minute, time.Hour)
	if err != nil {
		t.Fatalf("NewIssuerFromConfig verifier: %v", err)
	}
	claims, err := verifier.ValidateAccessToken(tokens.AccessToken)
	if err != nil {
		t.Fatalf("validate access token: %v", err)
	}
	if claims.UserID != "user-1" {
		t.Fatalf("unexpected user id: %q", claims.UserID)
	}
}
