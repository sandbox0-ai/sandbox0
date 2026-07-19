package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	teamquotatestutil "github.com/sandbox0-ai/sandbox0/pkg/teamquota/testutil"
	"golang.org/x/crypto/ssh"
)

func TestUserSSHPublicKeyRepositoryLifecycle(t *testing.T) {
	pool, schema := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithTeamQuotaStore(teamquotatestutil.NewPermissiveCapacityStore()))
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
		TeamID:            "team-1",
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

	keys, err := repo.ListUserSSHPublicKeysByTeamAndUserID(ctx, "team-1", user.ID)
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
	if loaded.TeamID != "team-1" {
		t.Fatalf("loaded team_id = %q, want team-1", loaded.TeamID)
	}

	fingerprintKeys, err := repo.ListUserSSHPublicKeysByFingerprint(ctx, fingerprint)
	if err != nil {
		t.Fatalf("list ssh public keys by fingerprint: %v", err)
	}
	if len(fingerprintKeys) != 1 || fingerprintKeys[0].TeamID != "team-1" {
		t.Fatalf("fingerprint keys = %+v, want one team-1 key", fingerprintKeys)
	}

	if err := repo.DeleteUserSSHPublicKeyByTeamAndUserID(ctx, "team-1", user.ID, key.ID); err != nil {
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
	repo := NewRepository(pool, WithTeamQuotaStore(teamquotatestutil.NewPermissiveCapacityStore()))
	userID := uuid.NewString()
	publicKey, keyType, fingerprint, comment, err := NormalizeAuthorizedSSHPublicKey("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ4dLZLZOA/asaP+5QO6t81jzbe5G4jrI2F+jbjL6TY8 sandbox0-e2e")
	if err != nil {
		t.Fatalf("normalize ssh public key: %v", err)
	}

	key := &UserSSHPublicKey{
		TeamID:            "team-1",
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

	keys, err := repo.ListUserSSHPublicKeysByTeamAndUserID(ctx, "team-1", userID)
	if err != nil {
		t.Fatalf("list federated user ssh public keys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("keys len = %d, want 1 (schema %s)", len(keys), schema)
	}
}

func TestCreateUserSSHPublicKeyRejectsOversizedInputBeforePostgresWrite(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithTeamQuotaStore(teamquotatestutil.NewPermissiveCapacityStore()))
	var before int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM user_ssh_public_keys`).Scan(&before); err != nil {
		t.Fatalf("count SSH public keys before rejected create: %v", err)
	}

	key := generatedSSHPublicKey(t, uuid.NewString(), uuid.NewString(), "oversized")
	key.Name = strings.Repeat("n", int(MaxSSHPublicKeyNameBytes)+1)
	err := repo.CreateUserSSHPublicKey(ctx, key)
	if !resourceguard.IsTooLarge(err) {
		t.Fatalf("CreateUserSSHPublicKey() error = %v, want TooLargeError", err)
	}

	var after int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM user_ssh_public_keys`).Scan(&after); err != nil {
		t.Fatalf("count SSH public keys after rejected create: %v", err)
	}
	if after != before {
		t.Fatalf("SSH public key count = %d, want unchanged %d", after, before)
	}
}

func TestUserSSHPublicKeyRepositoryAllowsSameFingerprintAcrossTeams(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithTeamQuotaStore(teamquotatestutil.NewPermissiveCapacityStore()))
	userID := uuid.NewString()
	publicKey, keyType, fingerprint, comment, err := NormalizeAuthorizedSSHPublicKey("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ4dLZLZOA/asaP+5QO6t81jzbe5G4jrI2F+jbjL6TY8 sandbox0-e2e")
	if err != nil {
		t.Fatalf("normalize ssh public key: %v", err)
	}

	for _, teamID := range []string{"team-1", "team-2"} {
		key := &UserSSHPublicKey{
			TeamID:            teamID,
			UserID:            userID,
			Name:              "Laptop " + teamID,
			PublicKey:         publicKey,
			KeyType:           keyType,
			FingerprintSHA256: fingerprint,
			Comment:           comment,
		}
		if err := repo.CreateUserSSHPublicKey(ctx, key); err != nil {
			t.Fatalf("create ssh public key for %s: %v", teamID, err)
		}
	}

	keys, err := repo.ListUserSSHPublicKeysByFingerprint(ctx, fingerprint)
	if err != nil {
		t.Fatalf("list ssh public keys by fingerprint: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("keys len = %d, want 2", len(keys))
	}
}

func TestDeleteUserRemovesSSHPublicKeys(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithTeamQuotaStore(teamquotatestutil.NewPermissiveCapacityStore()))
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
		TeamID:            "team-1",
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

func TestSSHPublicKeyControlPlaneQuotaReleasesOnDelete(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	teamID := "team-ssh-quota-" + uuid.NewString()
	userID := uuid.NewString()
	if err := teamquota.NewRepository(pool).UnsafePutTeamPolicyForTest(ctx, teamID, teamquota.Policy{
		Key:   teamquota.KeyControlPlaneObjectCount,
		Kind:  teamquota.KindCapacity,
		Limit: 1,
	}); err != nil {
		t.Fatalf("set team quota: %v", err)
	}

	first := generatedSSHPublicKey(t, teamID, userID, "First")
	if err := repo.CreateUserSSHPublicKey(ctx, first); err != nil {
		t.Fatalf("create first ssh public key: %v", err)
	}
	second := generatedSSHPublicKey(t, teamID, userID, "Second")
	if err := repo.CreateUserSSHPublicKey(ctx, second); !teamquota.IsExceeded(err) {
		t.Fatalf("create second ssh public key error = %v, want quota exceeded", err)
	}
	if err := repo.DeleteUserSSHPublicKeyByTeamAndUserID(ctx, teamID, userID, first.ID); err != nil {
		t.Fatalf("delete first ssh public key: %v", err)
	}
	if err := repo.CreateUserSSHPublicKey(ctx, second); err != nil {
		t.Fatalf("create replacement ssh public key: %v", err)
	}
}

func TestDeleteUserReleasesSSHPublicKeyControlPlaneQuota(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	quotaRepo := teamquota.NewRepository(pool)
	teamID := "team-delete-user-quota-" + uuid.NewString()
	if err := quotaRepo.UnsafePutTeamPolicyForTest(ctx, teamID, teamquota.Policy{
		Key:   teamquota.KeyControlPlaneObjectCount,
		Kind:  teamquota.KindCapacity,
		Limit: 1,
	}); err != nil {
		t.Fatalf("set team quota: %v", err)
	}
	user := &User{
		Email: "delete-user-quota-" + uuid.NewString() + "@example.com",
		Name:  "Delete User Quota",
	}
	if err := repo.CreateUser(ctx, user); err != nil {
		t.Fatalf("create user: %v", err)
	}
	key := generatedSSHPublicKey(t, teamID, user.ID, "Delete with user")
	if err := repo.CreateUserSSHPublicKey(ctx, key); err != nil {
		t.Fatalf("create ssh public key: %v", err)
	}
	if err := repo.DeleteUser(ctx, user.ID); err != nil {
		t.Fatalf("delete user: %v", err)
	}
	statuses, err := quotaRepo.ListStatus(ctx, teamID)
	if err != nil {
		t.Fatalf("list team quota status: %v", err)
	}
	for _, status := range statuses {
		if status.Key == teamquota.KeyControlPlaneObjectCount && status.Committed != 0 {
			t.Fatalf("committed control-plane objects = %d, want 0", status.Committed)
		}
	}
}

func generatedSSHPublicKey(t *testing.T, teamID, userID, name string) *UserSSHPublicKey {
	t.Helper()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}
	sshKey, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatalf("convert ed25519 public key: %v", err)
	}
	authorizedKey := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshKey))) + " sandbox0-quota-test"
	publicKey, keyType, fingerprint, comment, err := NormalizeAuthorizedSSHPublicKey(authorizedKey)
	if err != nil {
		t.Fatalf("normalize generated ssh public key: %v", err)
	}
	return &UserSSHPublicKey{
		TeamID:            teamID,
		UserID:            userID,
		Name:              name,
		PublicKey:         publicKey,
		KeyType:           keyType,
		FingerprintSHA256: fingerprint,
		Comment:           comment,
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
		DatabaseURL:     dbURL,
		Schema:          schema,
		DefaultMaxConns: 10,
		DefaultMinConns: 1,
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
	if err := teamquota.RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("migrate team quota schema: %v", err)
	}
	if err := teamquota.NewRepository(pool).UnsafeReplaceDefaultPoliciesForTest(
		ctx,
		teamquotatestutil.CompleteDefaultPolicies(),
	); err != nil {
		t.Fatalf("configure team quota defaults: %v", err)
	}

	return pool, schema
}
