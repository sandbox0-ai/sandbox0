package internalauth

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"
)

var (
	testPrivateKey ed25519.PrivateKey
	testPublicKey  ed25519.PublicKey
)

func init() {
	var err error
	testPublicKey, testPrivateKey, err = ed25519.GenerateKey(nil)
	if err != nil {
		panic(err)
	}
}

func TestGeneratorGenerate(t *testing.T) {
	generator := NewGenerator(GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: testPrivateKey,
		TTL:        30 * time.Second,
	})

	token, err := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{
		Permissions: []string{"sandboxvolume:read"},
	})

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if token == "" {
		t.Fatal("Token is empty")
	}

	// Token should be a valid JWT format
	if len(token) < 50 {
		t.Fatalf("Token too short: %d", len(token))
	}
}

func TestValidatorValidate(t *testing.T) {
	generator := NewGenerator(GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: testPrivateKey,
		TTL:        30 * time.Second,
	})

	validator := NewValidator(ValidatorConfig{
		Target:    "storage-proxy",
		PublicKey: testPublicKey,
	})

	token, _ := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{
		Permissions: []string{"sandboxvolume:read"},
		SandboxID:   "sandbox-123",
	})

	claims, err := validator.Validate(token)

	if err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	if claims.Caller != "cluster-gateway" {
		t.Errorf("Expected caller 'cluster-gateway', got '%s'", claims.Caller)
	}

	if claims.TeamID != "team-123" {
		t.Errorf("Expected team_id 'team-123', got '%s'", claims.TeamID)
	}

	if claims.UserID != "user-456" {
		t.Errorf("Expected user_id 'user-456', got '%s'", claims.UserID)
	}

	if claims.SandboxID != "sandbox-123" {
		t.Errorf("Expected sandbox_id 'sandbox-123', got '%s'", claims.SandboxID)
	}
}

func TestValidatorInvalidTarget(t *testing.T) {
	generator := NewGenerator(GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: testPrivateKey,
	})

	validator := NewValidator(ValidatorConfig{
		Target:    "manager", // Different from generation target
		PublicKey: testPublicKey,
	})

	token, _ := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{})

	_, err := validator.Validate(token)

	if !errors.Is(err, ErrInvalidTarget) {
		t.Errorf("Expected ErrInvalidTarget, got: %v", err)
	}
}

func TestValidatorInvalidSignature(t *testing.T) {
	// Generate a different key pair for signing
	otherPublicKey, otherPrivateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = otherPublicKey // Not used, but we need a different private key

	generator := NewGenerator(GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: otherPrivateKey,
	})

	validator := NewValidator(ValidatorConfig{
		Target:    "storage-proxy",
		PublicKey: testPublicKey, // Different public key
	})

	token, _ := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{})

	_, err = validator.Validate(token)

	if !errors.Is(err, ErrInvalidSignature) {
		t.Errorf("Expected ErrInvalidSignature, got: %v", err)
	}
}

func TestValidatorTokenExpired(t *testing.T) {
	now := time.Now()

	generator := NewGenerator(GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: testPrivateKey,
		TTL:        1 * time.Second,
		NowFunc: func() time.Time {
			return now
		},
	})

	validator := NewValidator(ValidatorConfig{
		Target:    "storage-proxy",
		PublicKey: testPublicKey,
		NowFunc: func() time.Time {
			return now.Add(2 * time.Second) // Time is past expiration
		},
	})

	token, _ := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{})

	_, err := validator.Validate(token)

	if err != ErrTokenExpired {
		t.Errorf("Expected ErrTokenExpired, got: %v", err)
	}
}

func TestValidatorAllowedCallers(t *testing.T) {
	generator := NewGenerator(GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: testPrivateKey,
	})

	validator := NewValidator(ValidatorConfig{
		Target:         "storage-proxy",
		PublicKey:      testPublicKey,
		AllowedCallers: []string{"manager", "procd"}, // Not cluster-gateway
	})

	token, _ := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{})

	_, err := validator.Validate(token)

	if !errors.Is(err, ErrInvalidCaller) {
		t.Errorf("Expected ErrInvalidCaller, got: %v", err)
	}
}

func TestValidateWithOptions(t *testing.T) {
	generator := NewGenerator(GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: testPrivateKey,
	})

	validator := NewValidator(ValidatorConfig{
		Target:    "storage-proxy",
		PublicKey: testPublicKey,
	})

	token, _ := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{
		Permissions: []string{"sandboxvolume:read", "sandboxvolume:write"},
	})

	// Test with required permissions
	_, err := validator.ValidateWithOptions(token, ValidateOptions{
		RequirePermissions: []string{"sandboxvolume:read"},
	})

	if err != nil {
		t.Errorf("Validate with permissions failed: %v", err)
	}

	// Test missing required permission
	_, err = validator.ValidateWithOptions(token, ValidateOptions{
		RequirePermissions: []string{"admin:delete"},
	})

	if err == nil {
		t.Error("Expected error for missing permission")
	}
}

func TestContextHelpers(t *testing.T) {
	claims := &Claims{
		TeamID:      "team-123",
		UserID:      "user-456",
		Caller:      "cluster-gateway",
		Permissions: []string{"read", "write"},
	}

	ctx := WithClaims(context.Background(), claims)

	if GetTeamID(ctx) != "team-123" {
		t.Error("GetTeamID failed")
	}

	if GetUserID(ctx) != "user-456" {
		t.Error("GetUserID failed")
	}

	if GetCaller(ctx) != "cluster-gateway" {
		t.Error("GetCaller failed")
	}

	if !HasPermission(ctx, "read") {
		t.Error("HasPermission failed")
	}

	if HasPermission(ctx, "delete") {
		t.Error("HasPermission should return false")
	}

	if !HasAllPermissions(ctx, "read", "write") {
		t.Error("HasAllPermissions failed")
	}

	if !HasAnyPermission(ctx, "delete", "read") {
		t.Error("HasAnyPermission failed")
	}
}

func TestReplayDetection(t *testing.T) {
	generator := NewGenerator(GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: testPrivateKey,
	})

	validator := NewValidator(ValidatorConfig{
		Target:                 "storage-proxy",
		PublicKey:              testPublicKey,
		ReplayDetectionEnabled: true,
	})

	token, _ := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{})

	// First validation should succeed
	_, err := validator.Validate(token)
	if err != nil {
		t.Fatalf("First validation failed: %v", err)
	}

	// Second validation with same token should fail (replay attack)
	_, err = validator.Validate(token)
	if err != ErrReplayAttack {
		t.Errorf("Expected ErrReplayAttack, got: %v", err)
	}
}

func TestNewGeneratorPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for nil private key")
		}
	}()

	NewGenerator(GeneratorConfig{
		Caller:     "test",
		PrivateKey: nil,
	})
}

func TestNewValidatorPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for nil public key")
		}
	}()

	NewValidator(ValidatorConfig{
		Target:    "test",
		PublicKey: nil,
	})
}

func TestGenerateSystem(t *testing.T) {
	generator := NewGenerator(GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: testPrivateKey,
		TTL:        30 * time.Second,
	})

	validator := NewValidator(ValidatorConfig{
		Target:    "manager",
		PublicKey: testPublicKey,
	})

	token, err := generator.GenerateSystem("manager", GenerateOptions{
		Permissions: []string{"*:*"},
		UserID:      "admin-user",
	})
	if err != nil {
		t.Fatalf("GenerateSystem failed: %v", err)
	}

	claims, err := validator.Validate(token)
	if err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	if !claims.IsSystem {
		t.Error("Expected IsSystem to be true")
	}

	if !claims.IsSystemToken() {
		t.Error("Expected IsSystemToken() to return true")
	}

	if claims.TeamID != "" {
		t.Errorf("Expected empty TeamID for system token, got '%s'", claims.TeamID)
	}

	if claims.UserID != "admin-user" {
		t.Errorf("Expected UserID 'admin-user', got '%s'", claims.UserID)
	}

	if claims.Subject != "system" {
		t.Errorf("Expected subject 'system', got '%s'", claims.Subject)
	}

	if claims.Caller != "cluster-gateway" {
		t.Errorf("Expected caller 'cluster-gateway', got '%s'", claims.Caller)
	}
}

func TestSystemTokenBypassTeamIDRequirement(t *testing.T) {
	generator := NewGenerator(GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: testPrivateKey,
	})

	validator := NewValidator(ValidatorConfig{
		Target:    "manager",
		PublicKey: testPublicKey,
	})

	// System token should pass even with RequireTeamID
	token, _ := generator.GenerateSystem("manager", GenerateOptions{})

	_, err := validator.ValidateWithOptions(token, ValidateOptions{
		RequireTeamID: true,
	})
	if err != nil {
		t.Errorf("System token should bypass RequireTeamID: %v", err)
	}

	// Regular token without teamID should fail (but Generate doesn't allow empty teamID)
	// This test verifies the validation logic works correctly
}

func TestGenerateSystemEmptyTarget(t *testing.T) {
	generator := NewGenerator(GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: testPrivateKey,
	})

	_, err := generator.GenerateSystem("", GenerateOptions{})
	if err == nil {
		t.Error("Expected error for empty target")
	}
}
