package identity

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

func TestUserSSHPublicKeyRepositoryLifecycle(t *testing.T) {
	pool, schema := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	user := &User{
		Email: "ssh-user@example.com",
		Name:  "SSH User",
	}
	if err := repo.CreateUser(ctx, user); err != nil {
		t.Fatalf("create user: %v", err)
	}

	publicKey, keyType, fingerprint, comment, err := NormalizeAuthorizedSSHPublicKey("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ4dLZLZOA/asaP+5QO6t81jzbe5G4jrI2F+jbjL6TY8 sandbox0-e2e")
	if err != nil {
		t.Fatalf("normalize ssh public key: %v", err)
	}
	key := &UserSSHPublicKey{
		UserID:            user.ID,
		Name:              "Laptop",
		PublicKey:         publicKey,
		KeyType:           keyType,
		FingerprintSHA256: fingerprint,
		Comment:           comment,
	}
	if err := repo.CreateUserSSHPublicKey(ctx, key); err != nil {
		t.Fatalf("create ssh public key: %v", err)
	}

	keys, err := repo.ListUserSSHPublicKeysByUserID(ctx, user.ID)
	if err != nil {
		t.Fatalf("list ssh public keys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("keys len = %d, want 1 (schema %s)", len(keys), schema)
	}

	loaded, err := repo.GetUserSSHPublicKeyByFingerprint(ctx, fingerprint)
	if err != nil {
		t.Fatalf("get ssh public key by fingerprint: %v", err)
	}
	if loaded.UserID != user.ID {
		t.Fatalf("loaded user_id = %q, want %q", loaded.UserID, user.ID)
	}

	if err := repo.DeleteUserSSHPublicKey(ctx, user.ID, key.ID); err != nil {
		t.Fatalf("delete ssh public key: %v", err)
	}
	if _, err := repo.GetUserSSHPublicKeyByFingerprint(ctx, fingerprint); err != ErrSSHPublicKeyNotFound {
		t.Fatalf("get after delete err = %v, want %v", err, ErrSSHPublicKeyNotFound)
	}
}

func TestUserSSHPublicKeyRepositoryAllowsFederatedUserID(t *testing.T) {
	pool, schema := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	userID := uuid.NewString()
	publicKey, keyType, fingerprint, comment, err := NormalizeAuthorizedSSHPublicKey("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ4dLZLZOA/asaP+5QO6t81jzbe5G4jrI2F+jbjL6TY8 sandbox0-e2e")
	if err != nil {
		t.Fatalf("normalize ssh public key: %v", err)
	}

	key := &UserSSHPublicKey{
		UserID:            userID,
		Name:              "Federated laptop",
		PublicKey:         publicKey,
		KeyType:           keyType,
		FingerprintSHA256: fingerprint,
		Comment:           comment,
	}
	if err := repo.CreateUserSSHPublicKey(ctx, key); err != nil {
		t.Fatalf("create ssh public key for federated user: %v", err)
	}

	keys, err := repo.ListUserSSHPublicKeysByUserID(ctx, userID)
	if err != nil {
		t.Fatalf("list federated user ssh public keys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("keys len = %d, want 1 (schema %s)", len(keys), schema)
	}
}

func TestDeleteUserRemovesSSHPublicKeys(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	user := &User{
		Email: "delete-ssh-user@example.com",
		Name:  "Delete SSH User",
	}
	if err := repo.CreateUser(ctx, user); err != nil {
		t.Fatalf("create user: %v", err)
	}

	publicKey, keyType, fingerprint, comment, err := NormalizeAuthorizedSSHPublicKey("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ4dLZLZOA/asaP+5QO6t81jzbe5G4jrI2F+jbjL6TY8 sandbox0-e2e")
	if err != nil {
		t.Fatalf("normalize ssh public key: %v", err)
	}
	key := &UserSSHPublicKey{
		UserID:            user.ID,
		Name:              "Laptop",
		PublicKey:         publicKey,
		KeyType:           keyType,
		FingerprintSHA256: fingerprint,
		Comment:           comment,
	}
	if err := repo.CreateUserSSHPublicKey(ctx, key); err != nil {
		t.Fatalf("create ssh public key: %v", err)
	}

	if err := repo.DeleteUser(ctx, user.ID); err != nil {
		t.Fatalf("delete user: %v", err)
	}
	if _, err := repo.GetUserSSHPublicKeyByFingerprint(ctx, fingerprint); err != ErrSSHPublicKeyNotFound {
		t.Fatalf("get ssh public key after user delete err = %v, want %v", err, ErrSSHPublicKeyNotFound)
	}
}

func newGatewayIdentityTestPool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()

	ctx := context.Background()
	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
		return nil, ""
	}

	schema := fmt.Sprintf("gateway_identity_test_%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
	adminPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(adminPool.Close)

	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: dbURL,
		Schema:      schema,
	})
	if err != nil {
		t.Fatalf("connect schema-scoped pool: %v", err)
	}
	t.Cleanup(pool.Close)
	t.Cleanup(func() {
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})

	if err := migrate.Up(ctx, pool, ".", migrate.WithBaseFS(migrations.FS), migrate.WithSchema(schema)); err != nil {
		t.Fatalf("migrate gateway schema: %v", err)
	}

	return pool, schema
}
