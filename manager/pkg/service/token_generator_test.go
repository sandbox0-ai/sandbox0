package service

import (
	"crypto/ed25519"
	"testing"

	"github.com/sandbox0-ai/infra/pkg/internalauth"
)

func TestProcdTokenGenerator_GenerateToken_IncludesSandboxID(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key pair: %v", err)
	}

	gen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "manager",
		PrivateKey: privateKey,
	})
	procdGen := NewProcdTokenGenerator(gen)

	token, err := procdGen.GenerateToken("team-1", "user-1", "sandbox-1")
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    "storage-proxy",
		PublicKey: publicKey,
	})

	claims, err := validator.Validate(token)
	if err != nil {
		t.Fatalf("validate token: %v", err)
	}

	if claims.SandboxID != "sandbox-1" {
		t.Fatalf("expected sandbox_id sandbox-1, got %q", claims.SandboxID)
	}
}
