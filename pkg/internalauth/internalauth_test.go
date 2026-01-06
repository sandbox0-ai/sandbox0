package internalauth

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGeneratorGenerate(t *testing.T) {
	secret := []byte("test-secret-key-32-bytes-long!!!")
	generator := NewGenerator(GeneratorConfig{
		Caller: "internal-gateway",
		Secret: secret,
		TTL:    30 * time.Second,
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
	secret := []byte("test-secret-key-32-bytes-long!!!")

	generator := NewGenerator(GeneratorConfig{
		Caller: "internal-gateway",
		Secret: secret,
		TTL:    30 * time.Second,
	})

	validator := NewValidator(ValidatorConfig{
		Target: "storage-proxy",
		Secret: secret,
	})

	token, _ := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{
		Permissions: []string{"sandboxvolume:read"},
	})

	claims, err := validator.Validate(token)

	if err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	if claims.Caller != "internal-gateway" {
		t.Errorf("Expected caller 'internal-gateway', got '%s'", claims.Caller)
	}

	if claims.TeamID != "team-123" {
		t.Errorf("Expected team_id 'team-123', got '%s'", claims.TeamID)
	}

	if claims.UserID != "user-456" {
		t.Errorf("Expected user_id 'user-456', got '%s'", claims.UserID)
	}
}

func TestValidatorInvalidTarget(t *testing.T) {
	secret := []byte("test-secret-key-32-bytes-long!!!")

	generator := NewGenerator(GeneratorConfig{
		Caller: "internal-gateway",
		Secret: secret,
	})

	validator := NewValidator(ValidatorConfig{
		Target: "manager", // Different from generation target
		Secret: secret,
	})

	token, _ := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{})

	_, err := validator.Validate(token)

	if !errors.Is(err, ErrInvalidTarget) {
		t.Errorf("Expected ErrInvalidTarget, got: %v", err)
	}
}

func TestValidatorInvalidSignature(t *testing.T) {
	generator := NewGenerator(GeneratorConfig{
		Caller: "internal-gateway",
		Secret: []byte("secret1"),
	})

	validator := NewValidator(ValidatorConfig{
		Target: "storage-proxy",
		Secret: []byte("secret2"), // Different secret
	})

	token, _ := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{})

	_, err := validator.Validate(token)

	if !errors.Is(err, ErrInvalidSignature) {
		t.Errorf("Expected ErrInvalidSignature, got: %v", err)
	}
}

func TestValidatorTokenExpired(t *testing.T) {
	secret := []byte("test-secret-key-32-bytes-long!!!")

	now := time.Now()

	generator := NewGenerator(GeneratorConfig{
		Caller: "internal-gateway",
		Secret: secret,
		TTL:    1 * time.Second,
		NowFunc: func() time.Time {
			return now
		},
	})

	validator := NewValidator(ValidatorConfig{
		Target: "storage-proxy",
		Secret: secret,
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
	secret := []byte("test-secret-key-32-bytes-long!!!")

	generator := NewGenerator(GeneratorConfig{
		Caller: "internal-gateway",
		Secret: secret,
	})

	validator := NewValidator(ValidatorConfig{
		Target:         "storage-proxy",
		Secret:         secret,
		AllowedCallers: []string{"manager", "procd"}, // Not internal-gateway
	})

	token, _ := generator.Generate("storage-proxy", "team-123", "user-456", GenerateOptions{})

	_, err := validator.Validate(token)

	if !errors.Is(err, ErrInvalidCaller) {
		t.Errorf("Expected ErrInvalidCaller, got: %v", err)
	}
}

func TestValidateWithOptions(t *testing.T) {
	secret := []byte("test-secret-key-32-bytes-long!!!")

	generator := NewGenerator(GeneratorConfig{
		Caller: "internal-gateway",
		Secret: secret,
	})

	validator := NewValidator(ValidatorConfig{
		Target: "storage-proxy",
		Secret: secret,
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
		Caller:      "internal-gateway",
		Permissions: []string{"read", "write"},
	}

	ctx := WithClaims(context.Background(), claims)

	if GetTeamID(ctx) != "team-123" {
		t.Error("GetTeamID failed")
	}

	if GetUserID(ctx) != "user-456" {
		t.Error("GetUserID failed")
	}

	if GetCaller(ctx) != "internal-gateway" {
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
	secret := []byte("test-secret-key-32-bytes-long!!!")

	generator := NewGenerator(GeneratorConfig{
		Caller: "internal-gateway",
		Secret: secret,
	})

	validator := NewValidator(ValidatorConfig{
		Target:                 "storage-proxy",
		Secret:                 secret,
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
			t.Error("Expected panic for empty secret")
		}
	}()

	NewGenerator(GeneratorConfig{
		Caller: "test",
		Secret: []byte{},
	})
}

func TestNewValidatorPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for empty secret")
		}
	}()

	NewValidator(ValidatorConfig{
		Target: "test",
		Secret: []byte{},
	})
}
