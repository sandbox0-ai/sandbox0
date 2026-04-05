package builtin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"golang.org/x/crypto/bcrypt"
)

var (
	ErrInvalidCredentials      = errors.New("invalid email or password")
	ErrRegistrationDisabled    = errors.New("registration is disabled")
	ErrEmailAlreadyExists      = errors.New("email already exists")
	ErrBuiltInAuthDisabled     = errors.New("built-in authentication is disabled")
	ErrAdminOnlyAuth           = errors.New("only admin accounts can use built-in auth")
	ErrPasswordTooWeak         = errors.New("password must be at least 8 characters")
	ErrInitUserPasswordMissing = errors.New("init user password is required when built-in authentication is enabled")
)

type identityStore interface {
	GetUserByEmail(ctx context.Context, email string) (*identity.User, error)
	CreateUserWithInitialTeam(ctx context.Context, user *identity.User, teamName string, homeRegionID *string) (*identity.Team, *identity.TeamMember, error)
	GetUserByID(ctx context.Context, id string) (*identity.User, error)
	UpdateUserPassword(ctx context.Context, userID, passwordHash string) error
	CountUsers(ctx context.Context) (int64, error)
	CreateUser(ctx context.Context, user *identity.User) error
	CreateTeam(ctx context.Context, team *identity.Team) error
	AddTeamMember(ctx context.Context, member *identity.TeamMember) error
	UpdateUser(ctx context.Context, user *identity.User) error
}

// Provider handles built-in email/password authentication
type Provider struct {
	repo            identityStore
	config          *config.BuiltInAuthConfig
	defaultTeamName string
}

// NewProvider creates a new built-in auth provider
func NewProvider(repo identityStore, cfg *config.BuiltInAuthConfig, defaultTeamName string) *Provider {
	return &Provider{
		repo:            repo,
		config:          cfg,
		defaultTeamName: defaultTeamName,
	}
}

// Authenticate validates email and password
func (p *Provider) Authenticate(ctx context.Context, email, password string) (*identity.User, error) {
	if !p.config.Enabled {
		return nil, ErrBuiltInAuthDisabled
	}

	user, err := p.repo.GetUserByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, identity.ErrUserNotFound) {
			return nil, ErrInvalidCredentials
		}
		return nil, fmt.Errorf("get user: %w", err)
	}

	// Check if admin-only mode is enabled
	if p.config.AdminOnly && !user.IsAdmin {
		return nil, ErrAdminOnlyAuth
	}

	// Verify password
	if user.PasswordHash == "" {
		return nil, ErrInvalidCredentials
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, ErrInvalidCredentials
	}

	return user, nil
}

// Register creates a new user with email/password
func (p *Provider) Register(ctx context.Context, email, password, name string, homeRegionID *string) (*identity.User, error) {
	if !p.config.Enabled {
		return nil, ErrBuiltInAuthDisabled
	}

	if !p.config.AllowRegistration {
		return nil, ErrRegistrationDisabled
	}

	if len(password) < 8 {
		return nil, ErrPasswordTooWeak
	}

	// Hash password
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	user := &identity.User{
		Email:         email,
		Name:          name,
		PasswordHash:  string(passwordHash),
		EmailVerified: !p.config.EmailVerificationRequired,
		IsAdmin:       false,
	}

	teamName := p.defaultTeamName
	if user.Name != "" {
		teamName = fmt.Sprintf("%s Team", user.Name)
	}

	if _, _, err := p.repo.CreateUserWithInitialTeam(ctx, user, teamName, homeRegionID); err != nil {
		if errors.Is(err, identity.ErrUserAlreadyExists) {
			return nil, ErrEmailAlreadyExists
		}
		return nil, fmt.Errorf("create user with team: %w", err)
	}

	return user, nil
}

// ChangePassword changes a user's password
func (p *Provider) ChangePassword(ctx context.Context, userID, oldPassword, newPassword string) error {
	if !p.config.Enabled {
		return ErrBuiltInAuthDisabled
	}

	user, err := p.repo.GetUserByID(ctx, userID)
	if err != nil {
		return fmt.Errorf("get user: %w", err)
	}

	// Verify old password
	if user.PasswordHash != "" {
		if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(oldPassword)); err != nil {
			return ErrInvalidCredentials
		}
	}

	if len(newPassword) < 8 {
		return ErrPasswordTooWeak
	}

	// Hash new password
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}

	return p.repo.UpdateUserPassword(ctx, userID, string(passwordHash))
}

// SetPassword sets a password for a user (admin function, no old password required)
func (p *Provider) SetPassword(ctx context.Context, userID, newPassword string) error {
	if len(newPassword) < 8 {
		return ErrPasswordTooWeak
	}

	passwordHash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}

	return p.repo.UpdateUserPassword(ctx, userID, string(passwordHash))
}

// EnsureInitUser ensures the initial user exists (for self-hosted deployments)
func (p *Provider) EnsureInitUser(ctx context.Context) error {
	if p.config.InitUser == nil {
		return nil
	}

	// Check if any users exist
	count, err := p.repo.CountUsers(ctx)
	if err != nil {
		return fmt.Errorf("count users: %w", err)
	}

	// If users exist, skip initialization
	if count > 0 {
		return nil
	}

	passwordHash := ""
	if p.config.Enabled {
		if strings.TrimSpace(p.config.InitUser.Password) == "" {
			return ErrInitUserPasswordMissing
		}
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(p.config.InitUser.Password), bcrypt.DefaultCost)
		if err != nil {
			return fmt.Errorf("hash password: %w", err)
		}
		passwordHash = string(hashedPassword)
	}

	user := &identity.User{
		Email:         p.config.InitUser.Email,
		Name:          p.config.InitUser.Name,
		PasswordHash:  passwordHash,
		EmailVerified: true,
		IsAdmin:       true,
	}

	if err := p.repo.CreateUser(ctx, user); err != nil {
		if errors.Is(err, identity.ErrUserAlreadyExists) {
			// User already exists, that's fine
			return nil
		}
		return fmt.Errorf("create init user: %w", err)
	}

	// Create an initial team for the bootstrap user.
	team := &identity.Team{
		Name:    "Default",
		Slug:    "default",
		OwnerID: &user.ID,
	}
	if p.config.InitUser != nil {
		if trimmed := strings.TrimSpace(p.config.InitUser.HomeRegionID); trimmed != "" {
			team.HomeRegionID = &trimmed
		}
	}

	if err := p.repo.CreateTeam(ctx, team); err != nil {
		return fmt.Errorf("create initial team: %w", err)
	}

	// Add user to team
	member := &identity.TeamMember{
		TeamID: team.ID,
		UserID: user.ID,
		Role:   "admin",
	}

	if err := p.repo.AddTeamMember(ctx, member); err != nil {
		return fmt.Errorf("add team member: %w", err)
	}

	return nil
}

// IsEnabled returns whether built-in auth is enabled
func (p *Provider) IsEnabled() bool {
	return p.config.Enabled
}
