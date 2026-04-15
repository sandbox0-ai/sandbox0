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

func TestGenerateTemplateManagerTokenUsesSystemScopeForSystemAdminAPIKey(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := &Server{internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute})}

	token, err := server.generateTemplateManagerToken(&authn.AuthContext{
		AuthMethod:    authn.AuthMethodAPIKey,
		TeamID:        "team-1",
		UserID:        "admin-user",
		IsSystemAdmin: true,
	}, nil, []string{authn.PermTemplateCreate})
	if err != nil {
		t.Fatalf("generateTemplateManagerToken: %v", err)
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
	if claims.UserID != "admin-user" {
		t.Fatalf("UserID = %q, want admin-user", claims.UserID)
	}
}

func TestGenerateTemplateManagerTokenKeepsSelectedTeamForSystemAdminJWT(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := &Server{internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute})}

	token, err := server.generateTemplateManagerToken(&authn.AuthContext{
		AuthMethod:    authn.AuthMethodJWT,
		TeamID:        "team-1",
		UserID:        "admin-user",
		IsSystemAdmin: true,
	}, nil, []string{authn.PermTemplateCreate})
	if err != nil {
		t.Fatalf("generateTemplateManagerToken: %v", err)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{Target: "manager", PublicKey: publicKey}).Validate(token)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if claims.IsSystemToken() {
		t.Fatalf("selected-team JWT should remain team scoped")
	}
	if claims.TeamID != "team-1" {
		t.Fatalf("TeamID = %q, want team-1", claims.TeamID)
	}
}

func TestGenerateManagerTokenKeepsSystemAdminAPIKeyTeamScopedByDefault(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := &Server{internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute})}

	token, err := server.generateManagerToken(&authn.AuthContext{
		AuthMethod:    authn.AuthMethodAPIKey,
		TeamID:        "team-1",
		UserID:        "admin-user",
		IsSystemAdmin: true,
	}, nil, []string{authn.PermSandboxCreate})
	if err != nil {
		t.Fatalf("generateManagerToken: %v", err)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{Target: "manager", PublicKey: publicKey}).Validate(token)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if claims.IsSystemToken() {
		t.Fatalf("default manager token should remain team scoped")
	}
	if claims.TeamID != "team-1" {
		t.Fatalf("TeamID = %q, want team-1", claims.TeamID)
	}
}
