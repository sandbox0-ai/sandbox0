package oidc

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/pkg/gateway/db"
	"go.uber.org/zap"
)

// StateData stores OAuth state information
type StateData struct {
	Provider  string    `json:"provider"`
	Nonce     string    `json:"nonce"`
	ReturnURL string    `json:"return_url"`
	CreatedAt time.Time `json:"created_at"`
}

// Manager manages multiple OIDC providers
type Manager struct {
	providers       map[string]*Provider
	repo            *db.Repository
	baseURL         string
	defaultTeamName string
	stateTTL        time.Duration
	cleanupInterval time.Duration
	logger          *zap.Logger

	// State management (in-memory for simplicity, use Redis in production)
	states   map[string]*StateData
	statesMu sync.RWMutex
}

// NewManager creates a new OIDC manager
func NewManager(ctx context.Context, cfg *config.EdgeGatewayConfig, repo *db.Repository, logger *zap.Logger) (*Manager, error) {
	m := &Manager{
		providers:       make(map[string]*Provider),
		repo:            repo,
		baseURL:         cfg.BaseURL,
		defaultTeamName: cfg.DefaultTeamName,
		stateTTL:        cfg.OIDCStateTTL.Duration,
		cleanupInterval: cfg.OIDCStateCleanupInterval.Duration,
		logger:          logger,
		states:          make(map[string]*StateData),
	}

	// Initialize enabled providers
	for _, providerCfg := range cfg.OIDCProviders {
		if !providerCfg.Enabled {
			continue
		}

		provider, err := NewProvider(ctx, &providerCfg, cfg.BaseURL)
		if err != nil {
			logger.Error("Failed to initialize OIDC provider",
				zap.String("provider", providerCfg.ID),
				zap.Error(err),
			)
			continue
		}

		m.providers[providerCfg.ID] = provider
		logger.Info("Initialized OIDC provider",
			zap.String("provider", providerCfg.ID),
			zap.String("name", providerCfg.Name),
		)
	}

	// Start state cleanup goroutine
	go m.cleanupStates(ctx)

	return m, nil
}

// GetProvider returns a provider by ID
func (m *Manager) GetProvider(id string) (*Provider, error) {
	provider, ok := m.providers[id]
	if !ok {
		return nil, ErrProviderNotFound
	}
	return provider, nil
}

// ListProviders returns all enabled providers
func (m *Manager) ListProviders() []*Provider {
	providers := make([]*Provider, 0, len(m.providers))
	for _, p := range m.providers {
		providers = append(providers, p)
	}
	return providers
}

// GenerateAuthURL generates an OAuth authorization URL
func (m *Manager) GenerateAuthURL(providerID, returnURL string) (string, error) {
	provider, err := m.GetProvider(providerID)
	if err != nil {
		return "", err
	}

	// Generate state
	stateBytes := make([]byte, 32)
	if _, err := rand.Read(stateBytes); err != nil {
		return "", fmt.Errorf("generate state: %w", err)
	}
	state := base64.URLEncoding.EncodeToString(stateBytes)

	// Store state
	m.statesMu.Lock()
	m.states[state] = &StateData{
		Provider:  providerID,
		Nonce:     state,
		ReturnURL: returnURL,
		CreatedAt: time.Now(),
	}
	m.statesMu.Unlock()

	return provider.AuthURL(state), nil
}

// ValidateState validates and consumes a state parameter
func (m *Manager) ValidateState(state string) (*StateData, error) {
	m.statesMu.Lock()
	defer m.statesMu.Unlock()

	data, ok := m.states[state]
	if !ok {
		return nil, ErrInvalidState
	}

	// Check expiration
	if time.Since(data.CreatedAt) > m.stateTTL {
		delete(m.states, state)
		return nil, ErrInvalidState
	}

	// Consume state (one-time use)
	delete(m.states, state)
	return data, nil
}

// HandleCallback processes an OIDC callback
func (m *Manager) HandleCallback(ctx context.Context, providerID, code, state string) (*db.User, error) {
	// Validate state
	stateData, err := m.ValidateState(state)
	if err != nil {
		return nil, err
	}

	if stateData.Provider != providerID {
		return nil, ErrInvalidState
	}

	// Get provider
	provider, err := m.GetProvider(providerID)
	if err != nil {
		return nil, err
	}

	// Exchange code for token
	token, err := provider.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("exchange code: %w", err)
	}

	// Verify token and get user info
	userInfo, err := provider.VerifyToken(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("verify token: %w", err)
	}

	if userInfo.Email == "" {
		return nil, ErrMissingEmail
	}

	// Validate email domain if configured
	if err := provider.ValidateEmailDomain(userInfo.Email); err != nil {
		return nil, err
	}

	// Find or create user
	return m.findOrCreateUser(ctx, provider, userInfo)
}

// findOrCreateUser finds an existing user or creates a new one
func (m *Manager) findOrCreateUser(ctx context.Context, provider *Provider, userInfo *UserInfo) (*db.User, error) {
	// Check if identity already exists
	identity, err := m.repo.GetUserIdentityByProviderSubject(ctx, provider.ID(), userInfo.Subject)
	if err == nil {
		// Identity exists, get the user
		user, err := m.repo.GetUserByID(ctx, identity.UserID)
		if err != nil {
			return nil, fmt.Errorf("get user: %w", err)
		}

		// Update claims
		_ = m.repo.UpdateUserIdentityClaims(ctx, identity.ID, userInfo.RawClaims)

		return user, nil
	}

	if !errors.Is(err, db.ErrIdentityNotFound) {
		return nil, fmt.Errorf("get identity: %w", err)
	}

	// Check if user exists by email
	user, err := m.repo.GetUserByEmail(ctx, userInfo.Email)
	if err == nil {
		// User exists, link identity
		identity := &db.UserIdentity{
			UserID:    user.ID,
			Provider:  provider.ID(),
			Subject:   userInfo.Subject,
			RawClaims: userInfo.RawClaims,
		}
		if err := m.repo.CreateUserIdentity(ctx, identity); err != nil {
			m.logger.Warn("Failed to link identity", zap.Error(err))
		}
		return user, nil
	}

	if !errors.Is(err, db.ErrUserNotFound) {
		return nil, fmt.Errorf("get user by email: %w", err)
	}

	// User doesn't exist, check if auto-provision is enabled
	if !provider.ShouldAutoProvision() {
		return nil, errors.New("user not found and auto-provisioning is disabled")
	}

	// Create new user
	user = &db.User{
		Email:         userInfo.Email,
		Name:          userInfo.Name,
		AvatarURL:     userInfo.Picture,
		EmailVerified: userInfo.EmailVerified,
		IsAdmin:       false,
	}

	teamName := m.defaultTeamName
	if user.Name != "" {
		teamName = fmt.Sprintf("%s Team", user.Name)
	}
	if _, _, err := m.repo.CreateUserWithDefaultTeam(ctx, user, teamName); err != nil {
		return nil, fmt.Errorf("create user with team: %w", err)
	}

	// Create identity
	identity = &db.UserIdentity{
		UserID:    user.ID,
		Provider:  provider.ID(),
		Subject:   userInfo.Subject,
		RawClaims: userInfo.RawClaims,
	}
	if err := m.repo.CreateUserIdentity(ctx, identity); err != nil {
		m.logger.Warn("Failed to create identity", zap.Error(err))
	}

	return user, nil
}

// cleanupStates periodically cleans up expired states
func (m *Manager) cleanupStates(ctx context.Context) {
	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.statesMu.Lock()
			now := time.Now()
			for state, data := range m.states {
				if now.Sub(data.CreatedAt) > m.stateTTL {
					delete(m.states, state)
				}
			}
			m.statesMu.Unlock()
		}
	}
}

// ProviderInfo contains public info about an OIDC provider
type ProviderInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// ListProviderInfo returns public info about all enabled providers
func (m *Manager) ListProviderInfo() []ProviderInfo {
	var info []ProviderInfo
	for _, p := range m.providers {
		info = append(info, ProviderInfo{
			ID:   p.ID(),
			Name: p.Name(),
		})
	}
	return info
}

// HasProvider checks if any OIDC providers are configured
func (m *Manager) HasProvider() bool {
	return len(m.providers) > 0
}
