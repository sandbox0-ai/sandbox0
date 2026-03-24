package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"golang.org/x/oauth2"
)

var (
	ErrProviderNotFound      = errors.New("OIDC provider not found")
	ErrProviderDisabled      = errors.New("OIDC provider is disabled")
	ErrInvalidState          = errors.New("invalid OAuth state")
	ErrInvalidCode           = errors.New("invalid authorization code")
	ErrMissingEmail          = errors.New("email not provided by IdP")
	ErrMissingHomeRegion     = errors.New("home_region_id is required for OIDC auto-provisioning")
	ErrHomeRegionNotRoutable = errors.New("home region is not routable")
	ErrEmailDomainMismatch   = errors.New("email domain not allowed")
)

// UserInfo contains user information from OIDC token
type UserInfo struct {
	Subject       string          `json:"sub"`
	Email         string          `json:"email"`
	EmailVerified bool            `json:"email_verified"`
	Name          string          `json:"name"`
	Picture       string          `json:"picture"`
	RawClaims     json.RawMessage `json:"raw_claims"`
}

// Provider represents a configured OIDC provider
type Provider struct {
	id           string
	name         string
	config       *config.OIDCProviderConfig
	oidcProvider *oidc.Provider
	oauth2Config *oauth2.Config
	verifier     *oidc.IDTokenVerifier
}

const wellKnownOIDCConfigPath = "/.well-known/openid-configuration"

// NewProvider creates a new OIDC provider
func NewProvider(ctx context.Context, cfg *config.OIDCProviderConfig, baseURL string) (*Provider, error) {
	// go-oidc expects an issuer URL here. Accept either the issuer URL itself
	// or a full discovery document URL and normalize to the issuer form.
	issuerURL := normalizeOIDCIssuerURL(cfg.DiscoveryURL)
	oidcProvider, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return nil, fmt.Errorf("create OIDC provider: %w", err)
	}

	authStyle, err := resolveTokenEndpointAuthStyle(cfg.TokenEndpointAuthMethod)
	if err != nil {
		return nil, err
	}

	// Configure OAuth2
	oauth2Config := &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL:  fmt.Sprintf("%s/auth/oidc/%s/callback", baseURL, cfg.ID),
		Endpoint: oauth2.Endpoint{
			AuthURL:   oidcProvider.Endpoint().AuthURL,
			TokenURL:  oidcProvider.Endpoint().TokenURL,
			AuthStyle: authStyle,
		},
		Scopes: cfg.Scopes,
	}

	// Create token verifier
	verifier := oidcProvider.Verifier(&oidc.Config{
		ClientID: cfg.ClientID,
	})

	return &Provider{
		id:           cfg.ID,
		name:         cfg.Name,
		config:       cfg,
		oidcProvider: oidcProvider,
		oauth2Config: oauth2Config,
		verifier:     verifier,
	}, nil
}

func normalizeOIDCIssuerURL(value string) string {
	trimmed := strings.TrimSpace(value)
	return strings.TrimSuffix(trimmed, wellKnownOIDCConfigPath)
}

func resolveTokenEndpointAuthStyle(method string) (oauth2.AuthStyle, error) {
	switch strings.TrimSpace(method) {
	case "", "auto":
		return oauth2.AuthStyleAutoDetect, nil
	case "client_secret_basic":
		return oauth2.AuthStyleInHeader, nil
	case "client_secret_post":
		return oauth2.AuthStyleInParams, nil
	default:
		return oauth2.AuthStyleAutoDetect, fmt.Errorf("unsupported token endpoint auth method %q", method)
	}
}

// ID returns the provider ID
func (p *Provider) ID() string {
	return p.id
}

// Name returns the provider display name
func (p *Provider) Name() string {
	return p.name
}

// Config returns the provider configuration
func (p *Provider) Config() *config.OIDCProviderConfig {
	return p.config
}

// AuthURL returns the authorization URL with the given state and PKCE verifier.
func (p *Provider) AuthURL(state, verifier string) string {
	return p.oauth2Config.AuthCodeURL(
		state,
		oauth2.AccessTypeOffline,
		oauth2.S256ChallengeOption(verifier),
	)
}

// Exchange exchanges an authorization code for tokens
func (p *Provider) Exchange(ctx context.Context, code, verifier string) (*oauth2.Token, error) {
	token, err := p.oauth2Config.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return nil, fmt.Errorf("exchange code: %w", err)
	}
	return token, nil
}

// VerifyToken verifies an ID token and returns user info
func (p *Provider) VerifyToken(ctx context.Context, token *oauth2.Token) (*UserInfo, error) {
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		return nil, errors.New("no id_token in response")
	}

	idToken, err := p.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return nil, fmt.Errorf("verify id_token: %w", err)
	}

	// Extract claims
	var claims map[string]any
	if err := idToken.Claims(&claims); err != nil {
		return nil, fmt.Errorf("parse claims: %w", err)
	}

	rawClaims, _ := json.Marshal(claims)

	userInfo := &UserInfo{
		Subject:   idToken.Subject,
		RawClaims: rawClaims,
	}

	// Extract standard claims
	if email, ok := claims["email"].(string); ok {
		userInfo.Email = email
	}
	if verified, ok := claims["email_verified"].(bool); ok {
		userInfo.EmailVerified = verified
	}
	if name, ok := claims["name"].(string); ok {
		userInfo.Name = name
	}
	if picture, ok := claims["picture"].(string); ok {
		userInfo.Picture = picture
	}

	return userInfo, nil
}

// ValidateEmailDomain checks if the email domain matches the configured domain
func (p *Provider) ValidateEmailDomain(email string) error {
	if p.config.TeamMapping == nil || p.config.TeamMapping.Domain == "" {
		return nil
	}

	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return ErrMissingEmail
	}

	domain := strings.ToLower(parts[1])
	allowedDomain := strings.ToLower(p.config.TeamMapping.Domain)

	if domain != allowedDomain {
		return ErrEmailDomainMismatch
	}

	return nil
}

// ShouldAutoProvision returns whether users should be auto-provisioned
func (p *Provider) ShouldAutoProvision() bool {
	return p.config.AutoProvision
}

// GetDefaultRole returns the default role for new users
func (p *Provider) GetDefaultRole() string {
	if p.config.TeamMapping != nil && p.config.TeamMapping.DefaultRole != "" {
		return p.config.TeamMapping.DefaultRole
	}
	return "viewer"
}

// GetDefaultTeamID returns the default team ID for new users
func (p *Provider) GetDefaultTeamID() string {
	if p.config.TeamMapping != nil {
		return p.config.TeamMapping.DefaultTeamID
	}
	return ""
}
