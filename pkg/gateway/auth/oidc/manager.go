package oidc

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
)

// StateData stores OAuth state information
type StateData struct {
	Provider     string    `json:"provider"`
	Nonce        string    `json:"nonce"`
	CodeVerifier string    `json:"code_verifier"`
	ReturnURL    string    `json:"return_url"`
	CreatedAt    time.Time `json:"created_at"`
}

type identityStore interface {
	GetUserIdentityByProviderSubject(ctx context.Context, provider, subject string) (*identity.UserIdentity, error)
	GetUserByID(ctx context.Context, id string) (*identity.User, error)
	UpdateUserIdentityClaims(ctx context.Context, id string, rawClaims []byte) error
	GetUserByEmail(ctx context.Context, email string) (*identity.User, error)
	CreateUser(ctx context.Context, user *identity.User) error
	CreateUserIdentity(ctx context.Context, identity *identity.UserIdentity) error
}

// Manager manages multiple OIDC providers
type Manager struct {
	providers       map[string]*Provider
	providerOrder   []string
	repo            identityStore
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
func NewManager(ctx context.Context, cfg *config.GatewayConfig, repo *identity.Repository, logger *zap.Logger) (*Manager, error) {
	m := &Manager{
		providers:       make(map[string]*Provider),
		providerOrder:   make([]string, 0),
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
		m.providerOrder = append(m.providerOrder, providerCfg.ID)
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
	verifier := oauth2.GenerateVerifier()

	// Store state
	m.statesMu.Lock()
	m.states[state] = &StateData{
		Provider:     providerID,
		Nonce:        state,
		CodeVerifier: verifier,
		ReturnURL:    returnURL,
		CreatedAt:    time.Now(),
	}
	m.statesMu.Unlock()

	return provider.AuthURL(state, verifier), nil
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
func (m *Manager) HandleCallback(ctx context.Context, providerID, code, state string) (*identity.User, string, error) {
	// Validate state
	stateData, err := m.ValidateState(state)
	if err != nil {
		return nil, "", err
	}

	if stateData.Provider != providerID {
		return nil, "", ErrInvalidState
	}

	// Get provider
	provider, err := m.GetProvider(providerID)
	if err != nil {
		return nil, "", err
	}

	// Exchange code for token
	token, err := provider.Exchange(ctx, code, stateData.CodeVerifier)
	if err != nil {
		return nil, "", fmt.Errorf("exchange code: %w", err)
	}

	// Verify token and get user info
	userInfo, err := provider.VerifyToken(ctx, token)
	if err != nil {
		return nil, "", fmt.Errorf("verify token: %w", err)
	}

	if userInfo.Email == "" {
		return nil, "", ErrMissingEmail
	}

	// Validate email domain if configured
	if err := provider.ValidateEmailDomain(userInfo.Email); err != nil {
		return nil, "", err
	}

	// Find or create user
	user, err := m.findOrCreateUser(ctx, provider, userInfo)
	if err != nil {
		return nil, "", err
	}
	return user, stateData.ReturnURL, nil
}

// findOrCreateUser finds an existing user or creates a new one
func (m *Manager) findOrCreateUser(ctx context.Context, provider *Provider, userInfo *UserInfo) (*identity.User, error) {
	// Check if identity already exists
	identityRecord, err := m.repo.GetUserIdentityByProviderSubject(ctx, provider.ID(), userInfo.Subject)
	if err == nil {
		// Identity exists, get the user
		user, err := m.repo.GetUserByID(ctx, identityRecord.UserID)
		if err != nil {
			return nil, fmt.Errorf("get user: %w", err)
		}

		// Update claims
		_ = m.repo.UpdateUserIdentityClaims(ctx, identityRecord.ID, userInfo.RawClaims)

		return user, nil
	}

	if !errors.Is(err, identity.ErrIdentityNotFound) {
		return nil, fmt.Errorf("get identity: %w", err)
	}

	// Check if user exists by email
	user, err := m.repo.GetUserByEmail(ctx, userInfo.Email)
	if err == nil {
		// User exists, link identity
		identityRecord := &identity.UserIdentity{
			UserID:    user.ID,
			Provider:  provider.ID(),
			Subject:   userInfo.Subject,
			RawClaims: userInfo.RawClaims,
		}
		if err := m.repo.CreateUserIdentity(ctx, identityRecord); err != nil {
			m.logger.Warn("Failed to link identity", zap.Error(err))
		}
		return user, nil
	}

	if !errors.Is(err, identity.ErrUserNotFound) {
		return nil, fmt.Errorf("get user by email: %w", err)
	}

	// User doesn't exist, check if auto-provision is enabled
	if !provider.ShouldAutoProvision() {
		return nil, errors.New("user not found and auto-provisioning is disabled")
	}

	// Create new user
	user = &identity.User{
		Email:         userInfo.Email,
		Name:          userInfo.Name,
		AvatarURL:     userInfo.Picture,
		EmailVerified: userInfo.EmailVerified,
		IsAdmin:       false,
	}

	if err := m.repo.CreateUser(ctx, user); err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}

	// Create identity
	identityRecord = &identity.UserIdentity{
		UserID:    user.ID,
		Provider:  provider.ID(),
		Subject:   userInfo.Subject,
		RawClaims: userInfo.RawClaims,
	}
	if err := m.repo.CreateUserIdentity(ctx, identityRecord); err != nil {
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
	ID                    string `json:"id"`
	Name                  string `json:"name"`
	ExternalAuthPortalURL string `json:"external_auth_portal_url,omitempty"`
}

// ListProviderInfo returns public info about all enabled providers
func (m *Manager) ListProviderInfo() []ProviderInfo {
	var info []ProviderInfo
	for _, id := range m.providerOrder {
		p, ok := m.providers[id]
		if !ok {
			continue
		}
		info = append(info, ProviderInfo{
			ID:                    p.ID(),
			Name:                  p.Name(),
			ExternalAuthPortalURL: p.Config().ExternalAuthPortalURL,
		})
	}
	return info
}

// HasProvider checks if any OIDC providers are configured
func (m *Manager) HasProvider() bool {
	return len(m.providers) > 0
}
