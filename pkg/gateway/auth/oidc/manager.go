package oidc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
)

// StateData stores OAuth state information
type StateData struct {
	Provider        string    `json:"provider"`
	Nonce           string    `json:"nonce"`
	CodeVerifier    string    `json:"code_verifier"`
	ReturnURL       string    `json:"return_url"`
	WebLoginHandoff bool      `json:"web_login_handoff"`
	CreatedAt       time.Time `json:"created_at"`
}

// CallbackResult contains the resolved user and post-login return metadata.
type CallbackResult struct {
	User            *identity.User
	ReturnURL       string
	WebLoginHandoff bool
}

type identityStore interface {
	GetUserIdentityByProviderSubject(ctx context.Context, provider, subject string) (*identity.UserIdentity, error)
	GetUserByID(ctx context.Context, id string) (*identity.User, error)
	UpdateUserIdentityClaims(ctx context.Context, id string, rawClaims []byte) error
	GetUserByEmail(ctx context.Context, email string) (*identity.User, error)
	CreateUser(ctx context.Context, user *identity.User) error
	CreateUserIdentity(ctx context.Context, identity *identity.UserIdentity) error
	CreateOIDCPendingState(ctx context.Context, state *identity.OIDCPendingState) error
	ConsumeOIDCPendingState(ctx context.Context, stateHash string) (*identity.OIDCPendingState, error)
}

// Manager manages multiple OIDC providers
type Manager struct {
	providers        map[string]*Provider
	providerOrder    []string
	repo             identityStore
	baseURL          string
	publicRootDomain string
	defaultTeamName  string
	stateTTL         time.Duration
	logger           *zap.Logger
}

// NewManager creates a new OIDC manager
func NewManager(ctx context.Context, cfg *config.GatewayConfig, repo *identity.Repository, logger *zap.Logger) (*Manager, error) {
	stateTTL := cfg.OIDCStateTTL.Duration
	if stateTTL <= 0 {
		stateTTL = 10 * time.Minute
	}
	m := &Manager{
		providers:        make(map[string]*Provider),
		providerOrder:    make([]string, 0),
		repo:             repo,
		baseURL:          cfg.BaseURL,
		publicRootDomain: cfg.PublicRootDomain,
		defaultTeamName:  cfg.DefaultTeamName,
		stateTTL:         stateTTL,
		logger:           logger,
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

type authURLConfig struct {
	webLoginHandoff bool
}

// AuthURLOption configures OIDC authorization URL generation.
type AuthURLOption func(*authURLConfig)

// WithWebLoginHandoff requests a one-time code redirect after OIDC callback.
func WithWebLoginHandoff() AuthURLOption {
	return func(cfg *authURLConfig) {
		cfg.webLoginHandoff = true
	}
}

// GenerateAuthURL generates an OAuth authorization URL.
func (m *Manager) GenerateAuthURL(
	ctx context.Context,
	providerID,
	returnURL string,
	opts ...AuthURLOption,
) (string, error) {
	provider, err := m.GetProvider(providerID)
	if err != nil {
		return "", err
	}
	cfg := authURLConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.webLoginHandoff && !m.IsAllowedWebReturnURL(returnURL) {
		return "", ErrInvalidReturnURL
	}
	if len(returnURL) > identity.MaxIdentityReturnURLBytes {
		return "", &identity.IdentityPayloadTooLargeError{
			Field:       "return_url",
			ActualBytes: len(returnURL),
			MaxBytes:    identity.MaxIdentityReturnURLBytes,
		}
	}

	// Generate state
	stateBytes := make([]byte, 32)
	if _, err := rand.Read(stateBytes); err != nil {
		return "", fmt.Errorf("generate state: %w", err)
	}
	state := base64.URLEncoding.EncodeToString(stateBytes)
	verifier := oauth2.GenerateVerifier()

	now := time.Now()
	if err := m.repo.CreateOIDCPendingState(ctx, &identity.OIDCPendingState{
		StateHash:       hashOIDCState(state),
		Provider:        providerID,
		CodeVerifier:    verifier,
		ReturnURL:       returnURL,
		WebLoginHandoff: cfg.webLoginHandoff,
		ExpiresAt:       now.Add(m.stateTTL),
	}); err != nil {
		return "", err
	}

	return provider.AuthURL(state, verifier), nil
}

// GenerateLogoutURL builds a provider logout URL for browser logout flows.
func (m *Manager) GenerateLogoutURL(providerID, returnURL string) (string, error) {
	provider, err := m.GetProvider(providerID)
	if err != nil {
		return "", err
	}
	if !m.IsAllowedWebReturnURL(returnURL) {
		return "", ErrInvalidReturnURL
	}
	return provider.LogoutURL(returnURL)
}

// IsAllowedWebReturnURL reports whether returnURL may receive a web login code.
func (m *Manager) IsAllowedWebReturnURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Hostname() == "" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	switch parsed.Scheme {
	case "https":
	case "http":
		if !isLocalhost(host) {
			return false
		}
	default:
		return false
	}
	if isLocalhost(host) {
		return true
	}
	if baseHost := hostFromURL(m.baseURL); baseHost != "" && host == baseHost {
		return true
	}
	rootDomain := strings.ToLower(strings.TrimSpace(m.publicRootDomain))
	if rootDomain != "" && (host == rootDomain || strings.HasSuffix(host, "."+rootDomain)) {
		return true
	}
	return false
}

func hostFromURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}

func isLocalhost(host string) bool {
	switch strings.ToLower(strings.Trim(host, "[]")) {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

// ValidateState validates and atomically consumes a state parameter.
func (m *Manager) ValidateState(ctx context.Context, state string) (*StateData, error) {
	pending, err := m.repo.ConsumeOIDCPendingState(ctx, hashOIDCState(state))
	if err != nil {
		if errors.Is(err, identity.ErrOIDCPendingStateNotFound) {
			return nil, ErrInvalidState
		}
		return nil, fmt.Errorf("consume OIDC state: %w", err)
	}
	if pending == nil {
		return nil, ErrInvalidState
	}
	return &StateData{
		Provider:        pending.Provider,
		Nonce:           state,
		CodeVerifier:    pending.CodeVerifier,
		ReturnURL:       pending.ReturnURL,
		WebLoginHandoff: pending.WebLoginHandoff,
		CreatedAt:       pending.CreatedAt,
	}, nil
}

func hashOIDCState(state string) string {
	sum := sha256.Sum256([]byte(state))
	return hex.EncodeToString(sum[:])
}

// HandleCallback processes an OIDC callback.
func (m *Manager) HandleCallback(ctx context.Context, providerID, code, state string) (*CallbackResult, error) {
	// Validate state
	stateData, err := m.ValidateState(ctx, state)
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
	token, err := provider.Exchange(ctx, code, stateData.CodeVerifier)
	if err != nil {
		return nil, fmt.Errorf("exchange code: %w", err)
	}

	// Verify token and get user info
	userInfo, err := provider.VerifyToken(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("verify token: %w", err)
	}

	user, err := m.completeUserInfo(ctx, provider, userInfo)
	if err != nil {
		return nil, err
	}
	return &CallbackResult{
		User:            user,
		ReturnURL:       stateData.ReturnURL,
		WebLoginHandoff: stateData.WebLoginHandoff,
	}, nil
}

// StartDeviceAuthorization begins a device authorization flow for the provider.
func (m *Manager) StartDeviceAuthorization(ctx context.Context, providerID string) (*DeviceAuthorization, error) {
	provider, err := m.GetProvider(providerID)
	if err != nil {
		return nil, err
	}
	return provider.StartDeviceAuthorization(ctx)
}

// CompleteToken verifies an upstream token response and maps it to a Sandbox0 user.
func (m *Manager) CompleteToken(ctx context.Context, providerID string, token *oauth2.Token) (*identity.User, error) {
	provider, err := m.GetProvider(providerID)
	if err != nil {
		return nil, err
	}
	userInfo, err := provider.VerifyToken(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("verify token: %w", err)
	}
	return m.completeUserInfo(ctx, provider, userInfo)
}

// PollDeviceAuthorization exchanges a device code and returns the resolved user when complete.
func (m *Manager) PollDeviceAuthorization(ctx context.Context, providerID, deviceCode string) (*identity.User, error) {
	provider, err := m.GetProvider(providerID)
	if err != nil {
		return nil, err
	}
	token, err := provider.ExchangeDeviceCode(ctx, deviceCode)
	if err != nil {
		return nil, err
	}
	return m.CompleteToken(ctx, providerID, token)
}

func (m *Manager) completeUserInfo(ctx context.Context, provider *Provider, userInfo *UserInfo) (*identity.User, error) {

	if userInfo.Email == "" {
		return nil, ErrMissingEmail
	}

	// Validate email domain if configured
	if err := provider.ValidateEmailDomain(userInfo.Email); err != nil {
		return nil, err
	}

	// Find or create user
	user, err := m.findOrCreateUser(ctx, provider, userInfo)
	if err != nil {
		return nil, err
	}
	return user, nil
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
			return nil, fmt.Errorf("link user identity: %w", err)
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
		return nil, fmt.Errorf("create user identity: %w", err)
	}

	return user, nil
}

// ProviderInfo contains public info about an OIDC provider
type ProviderInfo struct {
	ID                    string `json:"id"`
	Name                  string `json:"name"`
	ExternalAuthPortalURL string `json:"external_auth_portal_url,omitempty"`
	BrowserLoginEnabled   bool   `json:"browser_login_enabled"`
	DeviceLoginEnabled    bool   `json:"device_login_enabled"`
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
			BrowserLoginEnabled:   true,
			DeviceLoginEnabled:    p.SupportsDeviceAuthorization(),
		})
	}
	return info
}

// HasProvider checks if any OIDC providers are configured
func (m *Manager) HasProvider() bool {
	return len(m.providers) > 0
}
