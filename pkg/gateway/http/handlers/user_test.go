package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

const testSSHPublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ4dLZLZOA/asaP+5QO6t81jzbe5G4jrI2F+jbjL6TY8 sandbox0-e2e"

type fakeUserRepository struct {
	user                   *identity.User
	identities             []*identity.UserIdentity
	sshKeys                []*identity.UserSSHPublicKey
	createSSHPublicKeyErr  error
	listSSHPublicKeysErr   error
	deleteSSHPublicKeyErr  error
	lastDeletedSSHPublicID string
	lastDeletedTeamID      string
	lastDeletedUserID      string
}

func (f *fakeUserRepository) GetUserByID(_ context.Context, id string) (*identity.User, error) {
	if f.user == nil || f.user.ID != id {
		return nil, identity.ErrUserNotFound
	}
	copied := *f.user
	return &copied, nil
}

func (f *fakeUserRepository) UpdateUser(_ context.Context, user *identity.User) error {
	f.user = user
	return nil
}

func (f *fakeUserRepository) GetUserIdentitiesByUserID(_ context.Context, userID string) ([]*identity.UserIdentity, error) {
	if f.user == nil || f.user.ID != userID {
		return nil, nil
	}
	return f.identities, nil
}

func (f *fakeUserRepository) DeleteUserIdentity(_ context.Context, _ string) error {
	return nil
}

func (f *fakeUserRepository) CreateUserSSHPublicKey(_ context.Context, key *identity.UserSSHPublicKey) error {
	if f.createSSHPublicKeyErr != nil {
		return f.createSSHPublicKeyErr
	}
	copied := *key
	copied.ID = "sshkey-1"
	copied.CreatedAt = time.Unix(1700000000, 0).UTC()
	copied.UpdatedAt = copied.CreatedAt
	f.sshKeys = append(f.sshKeys, &copied)
	*key = copied
	return nil
}

func (f *fakeUserRepository) ListUserSSHPublicKeysByTeamAndUserID(_ context.Context, teamID, userID string) ([]*identity.UserSSHPublicKey, error) {
	if f.listSSHPublicKeysErr != nil {
		return nil, f.listSSHPublicKeysErr
	}
	if f.user == nil || f.user.ID != userID {
		return nil, nil
	}
	keys := make([]*identity.UserSSHPublicKey, 0, len(f.sshKeys))
	for _, key := range f.sshKeys {
		if key.TeamID == teamID {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (f *fakeUserRepository) DeleteUserSSHPublicKeyByTeamAndUserID(_ context.Context, teamID, userID, keyID string) error {
	f.lastDeletedTeamID = teamID
	f.lastDeletedUserID = userID
	f.lastDeletedSSHPublicID = keyID
	if f.deleteSSHPublicKeyErr != nil {
		return f.deleteSSHPublicKeyErr
	}
	return nil
}

func newAuthenticatedUserContext(t *testing.T, method, target, body string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	ctx.Request = req
	ctx.Set("auth_context", &authn.AuthContext{TeamID: "team-1", UserID: "user-1"})
	return ctx, rec
}

func TestCreateUserSSHPublicKey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeUserRepository{user: &identity.User{ID: "user-1"}}
	handler := NewUserHandler(repo, zap.NewNop())
	ctx, rec := newAuthenticatedUserContext(t, http.MethodPost, "/users/me/ssh-keys", `{"name":"Laptop","public_key":"`+testSSHPublicKey+`"}`)

	handler.CreateUserSSHPublicKey(ctx)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusCreated)
	}
	if len(repo.sshKeys) != 1 {
		t.Fatalf("stored keys = %d, want 1", len(repo.sshKeys))
	}
	if repo.sshKeys[0].Name != "Laptop" {
		t.Fatalf("stored key name = %q, want %q", repo.sshKeys[0].Name, "Laptop")
	}
	if repo.sshKeys[0].TeamID != "team-1" {
		t.Fatalf("stored key team_id = %q, want team-1", repo.sshKeys[0].TeamID)
	}
	if repo.sshKeys[0].PublicKey != "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ4dLZLZOA/asaP+5QO6t81jzbe5G4jrI2F+jbjL6TY8" {
		t.Fatalf("stored key public_key = %q", repo.sshKeys[0].PublicKey)
	}
	if repo.sshKeys[0].FingerprintSHA256 == "" {
		t.Fatal("expected fingerprint to be populated")
	}

	resp, apiErr, err := spec.DecodeResponse[SSHPublicKeyResponse](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %#v", apiErr)
	}
	if resp.Name != "Laptop" {
		t.Fatalf("response name = %q, want %q", resp.Name, "Laptop")
	}
}

func TestCreateUserSSHPublicKeyRequiresTeam(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeUserRepository{user: &identity.User{ID: "user-1"}}
	handler := NewUserHandler(repo, zap.NewNop())
	ctx, rec := newAuthenticatedUserContext(t, http.MethodPost, "/users/me/ssh-keys", `{"name":"Laptop","public_key":"`+testSSHPublicKey+`"}`)
	ctx.Set("auth_context", &authn.AuthContext{UserID: "user-1"})

	handler.CreateUserSSHPublicKey(ctx)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if len(repo.sshKeys) != 0 {
		t.Fatalf("stored keys = %d, want 0", len(repo.sshKeys))
	}
}

func TestCreateUserSSHPublicKeyRejectsDuplicate(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeUserRepository{
		user:                  &identity.User{ID: "user-1"},
		createSSHPublicKeyErr: identity.ErrSSHPublicKeyAlreadyExists,
	}
	handler := NewUserHandler(repo, zap.NewNop())
	ctx, rec := newAuthenticatedUserContext(t, http.MethodPost, "/users/me/ssh-keys", `{"name":"Laptop","public_key":"`+testSSHPublicKey+`"}`)

	handler.CreateUserSSHPublicKey(ctx)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestListUserSSHPublicKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeUserRepository{
		user: &identity.User{ID: "user-1"},
		sshKeys: []*identity.UserSSHPublicKey{{
			ID:                "sshkey-1",
			TeamID:            "team-1",
			UserID:            "user-1",
			Name:              "Laptop",
			PublicKey:         "ssh-ed25519 AAAAC3Nza...",
			KeyType:           "ssh-ed25519",
			FingerprintSHA256: "SHA256:abc",
			Comment:           "sandbox0-e2e",
			CreatedAt:         time.Unix(1700000000, 0).UTC(),
			UpdatedAt:         time.Unix(1700000000, 0).UTC(),
		}},
	}
	handler := NewUserHandler(repo, zap.NewNop())
	ctx, rec := newAuthenticatedUserContext(t, http.MethodGet, "/users/me/ssh-keys", "")

	handler.ListUserSSHPublicKeys(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	resp, apiErr, err := spec.DecodeResponse[struct {
		SSHKeys []SSHPublicKeyResponse `json:"ssh_keys"`
	}](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %#v", apiErr)
	}
	payload := *resp
	if len(payload.SSHKeys) != 1 {
		t.Fatalf("ssh_keys len = %d, want 1", len(payload.SSHKeys))
	}
	if payload.SSHKeys[0].FingerprintSHA256 != "SHA256:abc" {
		t.Fatalf("fingerprint = %q, want %q", payload.SSHKeys[0].FingerprintSHA256, "SHA256:abc")
	}
}

func TestDeleteUserSSHPublicKey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeUserRepository{user: &identity.User{ID: "user-1"}}
	handler := NewUserHandler(repo, zap.NewNop())
	ctx, rec := newAuthenticatedUserContext(t, http.MethodDelete, "/users/me/ssh-keys/sshkey-1", "")
	ctx.Params = gin.Params{{Key: "id", Value: "sshkey-1"}}

	handler.DeleteUserSSHPublicKey(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if repo.lastDeletedTeamID != "team-1" || repo.lastDeletedUserID != "user-1" || repo.lastDeletedSSHPublicID != "sshkey-1" {
		t.Fatalf("delete args = (%q, %q, %q), want (%q, %q, %q)", repo.lastDeletedTeamID, repo.lastDeletedUserID, repo.lastDeletedSSHPublicID, "team-1", "user-1", "sshkey-1")
	}
}

func TestDeleteUserSSHPublicKeyReturnsNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeUserRepository{
		user:                  &identity.User{ID: "user-1"},
		deleteSSHPublicKeyErr: identity.ErrSSHPublicKeyNotFound,
	}
	handler := NewUserHandler(repo, zap.NewNop())
	ctx, rec := newAuthenticatedUserContext(t, http.MethodDelete, "/users/me/ssh-keys/sshkey-1", "")
	ctx.Params = gin.Params{{Key: "id", Value: "sshkey-1"}}

	handler.DeleteUserSSHPublicKey(ctx)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestCreateUserSSHPublicKeyRejectsInvalidKey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeUserRepository{user: &identity.User{ID: "user-1"}}
	handler := NewUserHandler(repo, zap.NewNop())
	ctx, rec := newAuthenticatedUserContext(t, http.MethodPost, "/users/me/ssh-keys", `{"name":"Laptop","public_key":"not-a-key"}`)

	handler.CreateUserSSHPublicKey(ctx)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if !errors.Is(repo.createSSHPublicKeyErr, identity.ErrSSHPublicKeyAlreadyExists) && len(repo.sshKeys) != 0 {
		t.Fatalf("expected no key to be stored on invalid input")
	}
}
