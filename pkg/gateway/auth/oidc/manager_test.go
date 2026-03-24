package oidc

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"go.uber.org/zap"
)

type fakeIdentityStore struct {
	usersByID           map[string]*identity.User
	usersByEmail        map[string]*identity.User
	identitiesByKey     map[string]*identity.UserIdentity
	createUserCalls     int
	createIdentityCalls int
}

func newFakeIdentityStore() *fakeIdentityStore {
	return &fakeIdentityStore{
		usersByID:       map[string]*identity.User{},
		usersByEmail:    map[string]*identity.User{},
		identitiesByKey: map[string]*identity.UserIdentity{},
	}
}

func identityKey(provider, subject string) string {
	return provider + ":" + subject
}

func (f *fakeIdentityStore) GetUserIdentityByProviderSubject(_ context.Context, provider, subject string) (*identity.UserIdentity, error) {
	record, ok := f.identitiesByKey[identityKey(provider, subject)]
	if !ok {
		return nil, identity.ErrIdentityNotFound
	}
	copied := *record
	return &copied, nil
}

func (f *fakeIdentityStore) GetUserByID(_ context.Context, id string) (*identity.User, error) {
	user, ok := f.usersByID[id]
	if !ok {
		return nil, identity.ErrUserNotFound
	}
	copied := *user
	return &copied, nil
}

func (f *fakeIdentityStore) UpdateUserIdentityClaims(_ context.Context, id string, rawClaims []byte) error {
	for key, record := range f.identitiesByKey {
		if record.ID != id {
			continue
		}
		copied := *record
		copied.RawClaims = append([]byte(nil), rawClaims...)
		f.identitiesByKey[key] = &copied
		return nil
	}
	return identity.ErrIdentityNotFound
}

func (f *fakeIdentityStore) GetUserByEmail(_ context.Context, email string) (*identity.User, error) {
	user, ok := f.usersByEmail[email]
	if !ok {
		return nil, identity.ErrUserNotFound
	}
	copied := *user
	return &copied, nil
}

func (f *fakeIdentityStore) CreateUser(_ context.Context, user *identity.User) error {
	f.createUserCalls++
	user.ID = "user-new"
	copied := *user
	f.usersByID[user.ID] = &copied
	f.usersByEmail[user.Email] = &copied
	return nil
}

func (f *fakeIdentityStore) CreateUserIdentity(_ context.Context, record *identity.UserIdentity) error {
	f.createIdentityCalls++
	record.ID = "identity-new"
	copied := *record
	f.identitiesByKey[identityKey(record.Provider, record.Subject)] = &copied
	return nil
}

func TestManagerFindOrCreateUserAutoProvisionCreatesUserWithoutDefaultTeam(t *testing.T) {
	t.Parallel()

	store := newFakeIdentityStore()
	manager := &Manager{
		repo:   store,
		logger: zap.NewNop(),
	}
	provider := &Provider{
		id: "supabase",
		config: &config.OIDCProviderConfig{
			AutoProvision: true,
		},
	}
	rawClaims, err := json.Marshal(map[string]any{"sub": "subject-1"})
	if err != nil {
		t.Fatalf("marshal raw claims: %v", err)
	}

	user, err := manager.findOrCreateUser(context.Background(), provider, &UserInfo{
		Subject:       "subject-1",
		Email:         "new@example.com",
		Name:          "New User",
		EmailVerified: true,
		RawClaims:     rawClaims,
	})
	if err != nil {
		t.Fatalf("findOrCreateUser: %v", err)
	}

	if user.ID == "" {
		t.Fatalf("expected created user id")
	}
	if user.DefaultTeamID != nil {
		t.Fatalf("expected no default team, got %q", *user.DefaultTeamID)
	}
	if store.createUserCalls != 1 {
		t.Fatalf("expected 1 create user call, got %d", store.createUserCalls)
	}
	if store.createIdentityCalls != 1 {
		t.Fatalf("expected 1 create identity call, got %d", store.createIdentityCalls)
	}
	record, ok := store.identitiesByKey[identityKey("supabase", "subject-1")]
	if !ok {
		t.Fatalf("expected identity record to be created")
	}
	if record.UserID != user.ID {
		t.Fatalf("identity user id = %q, want %q", record.UserID, user.ID)
	}
}
