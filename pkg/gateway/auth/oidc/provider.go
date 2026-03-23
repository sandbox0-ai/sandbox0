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
	ErrProviderNotFound    = errors.New("OIDC provider not found")
	ErrProviderDisabled    = errors.New("OIDC provider is disabled")
	ErrInvalidState        = errors.New("invalid OAuth state")
	ErrInvalidCode         = errors.New("invalid authorization code")
	ErrMissingEmail        = errors.New("email not provided by IdP")
	ErrEmailDomainMismatch = errors.New("email domain not allowed")
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

// NewProvider creates a new OIDC provider
func NewProvider(ctx context.Context, cfg *config.OIDCProviderConfig, baseURL string) (*Provider, error) {
	// Create OIDC provider from discovery URL
	oidcProvider, err := oidc.NewProvider(ctx, cfg.DiscoveryURL)
	if err != nil {
		return nil, fmt.Errorf("create OIDC provider: %w", err)
	}

	// Configure OAuth2
	oauth2Config := &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL:  fmt.Sprintf("%s/auth/oidc/%s/callback", baseURL, cfg.ID),
		Endpoint:     oidcProvider.Endpoint(),
		Scopes:       cfg.Scopes,
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
