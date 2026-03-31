package builtin

import (
	"context"
	"fmt"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"golang.org/x/crypto/bcrypt"
)

type fakeIdentityStore struct {
	usersByID    map[string]*identity.User
	usersByEmail map[string]*identity.User
	teamsByID    map[string]*identity.Team
	members      []*identity.TeamMember
	nextID       int
}

func newFakeIdentityStore() *fakeIdentityStore {
	return &fakeIdentityStore{
		usersByID:    map[string]*identity.User{},
		usersByEmail: map[string]*identity.User{},
		teamsByID:    map[string]*identity.Team{},
	}
}

func (f *fakeIdentityStore) next(prefix string) string {
	f.nextID++
	return fmt.Sprintf("%s-%d", prefix, f.nextID)
}

func (f *fakeIdentityStore) GetUserByEmail(_ context.Context, email string) (*identity.User, error) {
	user, ok := f.usersByEmail[email]
	if !ok {
		return nil, identity.ErrUserNotFound
	}
	copy := *user
	return &copy, nil
}

func (f *fakeIdentityStore) CreateUserWithDefaultTeam(_ context.Context, user *identity.User, teamName string, homeRegionID *string) (*identity.Team, *identity.TeamMember, error) {
	if _, exists := f.usersByEmail[user.Email]; exists {
		return nil, nil, identity.ErrUserAlreadyExists
	}
	createdUser := *user
	createdUser.ID = f.next("user")
	team := &identity.Team{
		ID:           f.next("team"),
		Name:         teamName,
		Slug:         "user-" + createdUser.ID,
		OwnerID:      &createdUser.ID,
		HomeRegionID: homeRegionID,
	}
	member := &identity.TeamMember{
		ID:     f.next("member"),
		TeamID: team.ID,
		UserID: createdUser.ID,
		Role:   "admin",
	}
	createdUser.DefaultTeamID = &team.ID
	createdUser.DefaultTeam = team
	f.usersByID[createdUser.ID] = &createdUser
	f.usersByEmail[createdUser.Email] = &createdUser
	f.teamsByID[team.ID] = team
	f.members = append(f.members, member)
	*user = createdUser
	return team, member, nil
}

func (f *fakeIdentityStore) GetUserByID(_ context.Context, id string) (*identity.User, error) {
	user, ok := f.usersByID[id]
	if !ok {
		return nil, identity.ErrUserNotFound
	}
	copy := *user
	return &copy, nil
}

func (f *fakeIdentityStore) UpdateUserPassword(_ context.Context, userID, passwordHash string) error {
	user, ok := f.usersByID[userID]
	if !ok {
		return identity.ErrUserNotFound
	}
	user.PasswordHash = passwordHash
	return nil
}

func (f *fakeIdentityStore) CountUsers(_ context.Context) (int64, error) {
	return int64(len(f.usersByID)), nil
}

func (f *fakeIdentityStore) CreateUser(_ context.Context, user *identity.User) error {
	if _, exists := f.usersByEmail[user.Email]; exists {
		return identity.ErrUserAlreadyExists
	}
	createdUser := *user
	createdUser.ID = f.next("user")
	f.usersByID[createdUser.ID] = &createdUser
	f.usersByEmail[createdUser.Email] = &createdUser
	*user = createdUser
	return nil
}

func (f *fakeIdentityStore) CreateTeam(_ context.Context, team *identity.Team) error {
	createdTeam := *team
	createdTeam.ID = f.next("team")
	f.teamsByID[createdTeam.ID] = &createdTeam
	*team = createdTeam
	return nil
}

func (f *fakeIdentityStore) AddTeamMember(_ context.Context, member *identity.TeamMember) error {
	createdMember := *member
	createdMember.ID = f.next("member")
	f.members = append(f.members, &createdMember)
	*member = createdMember
	return nil
}

func (f *fakeIdentityStore) UpdateUser(_ context.Context, user *identity.User) error {
	stored, ok := f.usersByID[user.ID]
	if !ok {
		return identity.ErrUserNotFound
	}
	*stored = *user
	f.usersByEmail[user.Email] = stored
	return nil
}

func TestEnsureInitUserCreatesAdminWithPasswordWhenBuiltInEnabled(t *testing.T) {
	store := newFakeIdentityStore()
	provider := NewProvider(store, &config.BuiltInAuthConfig{
		Enabled: true,
		InitUser: &config.InitUserConfig{
			Email:    "admin@example.com",
			Name:     "Admin",
			Password: "super-secret",
		},
	}, "Personal Team")

	if err := provider.EnsureInitUser(context.Background()); err != nil {
		t.Fatalf("EnsureInitUser: %v", err)
	}

	user, err := store.GetUserByEmail(context.Background(), "admin@example.com")
	if err != nil {
		t.Fatalf("GetUserByEmail: %v", err)
	}
	if !user.IsAdmin {
		t.Fatal("expected init user to be admin")
	}
	if user.DefaultTeamID == nil || *user.DefaultTeamID == "" {
		t.Fatal("expected init user to have a default team")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte("super-secret")); err != nil {
		t.Fatalf("expected stored password hash to match: %v", err)
	}
}

func TestEnsureInitUserCreatesPasswordlessAdminWhenBuiltInAuthDisabled(t *testing.T) {
	store := newFakeIdentityStore()
	provider := NewProvider(store, &config.BuiltInAuthConfig{
		Enabled: false,
		InitUser: &config.InitUserConfig{
			Email: "admin@example.com",
			Name:  "Admin",
		},
	}, "Personal Team")

	if err := provider.EnsureInitUser(context.Background()); err != nil {
		t.Fatalf("EnsureInitUser: %v", err)
	}

	user, err := store.GetUserByEmail(context.Background(), "admin@example.com")
	if err != nil {
		t.Fatalf("GetUserByEmail: %v", err)
	}
	if !user.IsAdmin {
		t.Fatal("expected init user to be admin")
	}
	if user.PasswordHash != "" {
		t.Fatalf("expected no password hash for OIDC bootstrap user, got %q", user.PasswordHash)
	}
	if user.DefaultTeamID == nil || *user.DefaultTeamID == "" {
		t.Fatal("expected init user to have a default team")
	}
}

func TestEnsureInitUserSkipsWhenUsersAlreadyExist(t *testing.T) {
	store := newFakeIdentityStore()
	if err := store.CreateUser(context.Background(), &identity.User{
		Email: "existing@example.com",
		Name:  "Existing",
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	provider := NewProvider(store, &config.BuiltInAuthConfig{
		Enabled: true,
		InitUser: &config.InitUserConfig{
			Email:    "admin@example.com",
			Name:     "Admin",
			Password: "super-secret",
		},
	}, "Personal Team")

	if err := provider.EnsureInitUser(context.Background()); err != nil {
		t.Fatalf("EnsureInitUser: %v", err)
	}

	if _, err := store.GetUserByEmail(context.Background(), "admin@example.com"); err == nil {
		t.Fatal("expected init user bootstrap to be skipped when users already exist")
	}
}
