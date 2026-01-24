package builtin

import (
	"context"
	"errors"
	"fmt"

	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/pkg/gateway/db"
	"golang.org/x/crypto/bcrypt"
)

var (
	ErrInvalidCredentials   = errors.New("invalid email or password")
	ErrRegistrationDisabled = errors.New("registration is disabled")
	ErrEmailAlreadyExists   = errors.New("email already exists")
	ErrBuiltInAuthDisabled  = errors.New("built-in authentication is disabled")
	ErrAdminOnlyAuth        = errors.New("only admin accounts can use built-in auth")
	ErrPasswordTooWeak      = errors.New("password must be at least 8 characters")
)

// Provider handles built-in email/password authentication
type Provider struct {
	repo            *db.Repository
	config          *config.BuiltInAuthConfig
	defaultTeamName string
}

// NewProvider creates a new built-in auth provider
func NewProvider(repo *db.Repository, cfg *config.BuiltInAuthConfig, defaultTeamName string) *Provider {
	return &Provider{
		repo:            repo,
		config:          cfg,
		defaultTeamName: defaultTeamName,
	}
}

// Authenticate validates email and password
func (p *Provider) Authenticate(ctx context.Context, email, password string) (*db.User, error) {
	if !p.config.Enabled {
		return nil, ErrBuiltInAuthDisabled
	}

	user, err := p.repo.GetUserByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, db.ErrUserNotFound) {
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
func (p *Provider) Register(ctx context.Context, email, password, name string) (*db.User, error) {
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

	user := &db.User{
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

	if _, _, err := p.repo.CreateUserWithDefaultTeam(ctx, user, teamName); err != nil {
		if errors.Is(err, db.ErrUserAlreadyExists) {
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

	// Create initial user
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(p.config.InitUser.Password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}

	user := &db.User{
		Email:         p.config.InitUser.Email,
		Name:          p.config.InitUser.Name,
		PasswordHash:  string(passwordHash),
		EmailVerified: true,
		IsAdmin:       true,
	}

	if err := p.repo.CreateUser(ctx, user); err != nil {
		if errors.Is(err, db.ErrUserAlreadyExists) {
			// User already exists, that's fine
			return nil
		}
		return fmt.Errorf("create init user: %w", err)
	}

	// Create a default team for the initial user
	team := &db.Team{
		Name:    "Default",
		Slug:    "default",
		OwnerID: &user.ID,
	}

	if err := p.repo.CreateTeam(ctx, team); err != nil {
		return fmt.Errorf("create default team: %w", err)
	}

	// Add user to team
	member := &db.TeamMember{
		TeamID: team.ID,
		UserID: user.ID,
		Role:   "admin",
	}

	if err := p.repo.AddTeamMember(ctx, member); err != nil {
		return fmt.Errorf("add team member: %w", err)
	}

	// Set default team
	user.DefaultTeamID = &team.ID
	if err := p.repo.UpdateUser(ctx, user); err != nil {
		return fmt.Errorf("update user default team: %w", err)
	}

	return nil
}

// IsEnabled returns whether built-in auth is enabled
func (p *Provider) IsEnabled() bool {
	return p.config.Enabled
}
