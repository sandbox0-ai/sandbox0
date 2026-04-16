package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestGenerateManagerTokenUsesSystemTokenForTeamlessSystemAdmin(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := &Server{internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute})}

	token, err := server.generateManagerToken(&authn.AuthContext{IsSystemAdmin: true}, nil, []string{authn.PermTemplateCreate})
	if err != nil {
		t.Fatalf("generateManagerToken: %v", err)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{Target: "manager", PublicKey: publicKey}).Validate(token)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !claims.IsSystemToken() {
		t.Fatalf("expected system token, got team_id=%q", claims.TeamID)
	}
	if claims.TeamID != "" {
		t.Fatalf("TeamID = %q, want empty", claims.TeamID)
	}
}

func TestGenerateManagerTokenUsesSystemTokenForPlatformAPIKey(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := &Server{internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute})}

	token, err := server.generateManagerToken(&authn.AuthContext{
		AuthMethod:    authn.AuthMethodAPIKey,
		TeamID:        "team-1",
		UserID:        "user-1",
		APIKeyID:      "key-1",
		IsSystemAdmin: true,
		Permissions:   []string{"*"},
	}, nil, []string{authn.PermTemplateCreate})
	if err != nil {
		t.Fatalf("generateManagerToken: %v", err)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{Target: "manager", PublicKey: publicKey}).Validate(token)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !claims.IsSystemToken() {
		t.Fatalf("expected system token, got team_id=%q", claims.TeamID)
	}
	if claims.TeamID != "" {
		t.Fatalf("TeamID = %q, want empty", claims.TeamID)
	}
	if len(claims.Permissions) != 1 || claims.Permissions[0] != authn.PermTemplateCreate {
		t.Fatalf("Permissions = %v, want [%s]", claims.Permissions, authn.PermTemplateCreate)
	}
}
