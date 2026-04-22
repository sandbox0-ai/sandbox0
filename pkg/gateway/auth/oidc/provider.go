package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"golang.org/x/oauth2"
)

var (
	ErrProviderNotFound            = errors.New("OIDC provider not found")
	ErrProviderDisabled            = errors.New("OIDC provider is disabled")
	ErrInvalidState                = errors.New("invalid OAuth state")
	ErrInvalidCode                 = errors.New("invalid authorization code")
	ErrInvalidReturnURL            = errors.New("invalid return URL")
	ErrProviderLogoutNotSupported  = errors.New("OIDC provider logout is not supported")
	ErrMissingEmail                = errors.New("email not provided by IdP")
	ErrHomeRegionNotRoutable       = errors.New("home region is not routable")
	ErrEmailDomainMismatch         = errors.New("email domain not allowed")
	ErrDeviceFlowNotSupported      = errors.New("device authorization is not supported by this provider")
	ErrDeviceAuthorizationPending  = errors.New("device authorization pending")
	ErrDeviceAuthorizationSlowDown = errors.New("device authorization slow down")
	ErrDeviceAuthorizationDeclined = errors.New("device authorization declined")
	ErrDeviceAuthorizationExpired  = errors.New("device authorization expired")
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
	id                          string
	name                        string
	config                      *config.OIDCProviderConfig
	oidcProvider                *oidc.Provider
	oauth2Config                *oauth2.Config
	verifier                    *oidc.IDTokenVerifier
	deviceVerifier              *oidc.IDTokenVerifier
	deviceAuthorizationEndpoint string
	endSessionEndpoint          string
}

const wellKnownOIDCConfigPath = "/.well-known/openid-configuration"

type discoveryMetadata struct {
	Issuer                      string `json:"issuer"`
	DeviceAuthorizationEndpoint string `json:"device_authorization_endpoint"`
	EndSessionEndpoint          string `json:"end_session_endpoint"`
}

// NewProvider creates a new OIDC provider
func NewProvider(ctx context.Context, cfg *config.OIDCProviderConfig, baseURL string) (*Provider, error) {
	// go-oidc expects an issuer URL here. Accept either the issuer URL itself
	// or a full discovery document URL and normalize to the issuer form.
	// When a provider publishes an issuer that differs only by a trailing slash,
	// prefer the discovery document's issuer exactly so initialization remains
	// compatible across OIDC providers and custom domains.
	issuerURL := normalizeOIDCIssuerURL(cfg.DiscoveryURL)
	metadata, err := fetchDiscoveryMetadata(ctx, cfg.DiscoveryURL)
	if err == nil && strings.TrimSpace(metadata.Issuer) != "" {
		issuerURL = strings.TrimSpace(metadata.Issuer)
	}
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
	var deviceVerifier *oidc.IDTokenVerifier
	if deviceClientID := strings.TrimSpace(cfg.DeviceClientID); deviceClientID != "" && deviceClientID != strings.TrimSpace(cfg.ClientID) {
		deviceVerifier = oidcProvider.Verifier(&oidc.Config{
			ClientID: deviceClientID,
		})
	}

	if strings.TrimSpace(metadata.DeviceAuthorizationEndpoint) == "" {
		if err := oidcProvider.Claims(&metadata); err != nil {
			metadata.DeviceAuthorizationEndpoint = ""
		}
	}
	if strings.TrimSpace(cfg.DeviceAuthorizationEndpoint) != "" {
		metadata.DeviceAuthorizationEndpoint = strings.TrimSpace(cfg.DeviceAuthorizationEndpoint)
	}

	return &Provider{
		id:                          cfg.ID,
		name:                        cfg.Name,
		config:                      cfg,
		oidcProvider:                oidcProvider,
		oauth2Config:                oauth2Config,
		verifier:                    verifier,
		deviceVerifier:              deviceVerifier,
		deviceAuthorizationEndpoint: strings.TrimSpace(metadata.DeviceAuthorizationEndpoint),
		endSessionEndpoint:          strings.TrimSpace(metadata.EndSessionEndpoint),
	}, nil
}

func normalizeOIDCIssuerURL(value string) string {
	trimmed := strings.TrimSpace(value)
	return strings.TrimSuffix(trimmed, wellKnownOIDCConfigPath)
}

func resolveOIDCDiscoveryURL(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if strings.HasSuffix(trimmed, wellKnownOIDCConfigPath) {
		return trimmed
	}
	return strings.TrimRight(trimmed, "/") + wellKnownOIDCConfigPath
}

func fetchDiscoveryMetadata(ctx context.Context, value string) (discoveryMetadata, error) {
	discoveryURL := resolveOIDCDiscoveryURL(value)
	if discoveryURL == "" {
		return discoveryMetadata{}, errors.New("missing discovery URL")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		return discoveryMetadata{}, fmt.Errorf("build discovery request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return discoveryMetadata{}, fmt.Errorf("fetch discovery document: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return discoveryMetadata{}, fmt.Errorf("fetch discovery document: unexpected status %s", resp.Status)
	}

	var metadata discoveryMetadata
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return discoveryMetadata{}, fmt.Errorf("decode discovery document: %w", err)
	}
	return metadata, nil
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
	if !ok || strings.TrimSpace(rawIDToken) == "" {
		return p.fetchUserInfo(ctx, token)
	}

	idToken, err := p.verifyIDToken(ctx, rawIDToken)
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

// LogoutURL returns the provider logout URL when the IdP supports RP-initiated logout.
func (p *Provider) LogoutURL(returnURL string) (string, error) {
	if strings.TrimSpace(p.endSessionEndpoint) == "" {
		return "", ErrProviderLogoutNotSupported
	}
	logoutURL, err := url.Parse(p.endSessionEndpoint)
	if err != nil {
		return "", fmt.Errorf("parse end session endpoint: %w", err)
	}
	query := logoutURL.Query()
	query.Set("post_logout_redirect_uri", returnURL)
	if clientID := strings.TrimSpace(p.config.ClientID); clientID != "" {
		query.Set("client_id", clientID)
	}
	logoutURL.RawQuery = query.Encode()
	return logoutURL.String(), nil
}

func (p *Provider) verifyIDToken(ctx context.Context, rawIDToken string) (*oidc.IDToken, error) {
	idToken, err := p.verifier.Verify(ctx, rawIDToken)
	if err == nil {
		return idToken, nil
	}
	if p.deviceVerifier != nil {
		deviceToken, deviceErr := p.deviceVerifier.Verify(ctx, rawIDToken)
		if deviceErr == nil {
			return deviceToken, nil
		}
	}
	return nil, err
}

func (p *Provider) fetchUserInfo(ctx context.Context, token *oauth2.Token) (*UserInfo, error) {
	if strings.TrimSpace(token.AccessToken) == "" {
		return nil, errors.New("no access_token in response")
	}
	userInfo, err := p.oidcProvider.UserInfo(ctx, oauth2.StaticTokenSource(token))
	if err != nil {
		return nil, fmt.Errorf("fetch userinfo: %w", err)
	}

	var claims map[string]any
	if err := userInfo.Claims(&claims); err != nil {
		return nil, fmt.Errorf("parse userinfo claims: %w", err)
	}
	rawClaims, _ := json.Marshal(claims)

	result := &UserInfo{
		Subject:       userInfo.Subject,
		Email:         userInfo.Email,
		EmailVerified: userInfo.EmailVerified,
		RawClaims:     rawClaims,
	}
	if name, ok := claims["name"].(string); ok {
		result.Name = name
	}
	if picture, ok := claims["picture"].(string); ok {
		result.Picture = picture
	}
	return result, nil
}

// DeviceAuthorization contains the user-facing data needed to complete device login.
type DeviceAuthorization struct {
	DeviceCode              string
	UserCode                string
	VerificationURI         string
	VerificationURIComplete string
	ExpiresIn               int
	Interval                int
}

// SupportsDeviceAuthorization reports whether device flow is configured and available.
func (p *Provider) SupportsDeviceAuthorization() bool {
	return p.config.DeviceAuthorizationEnabled &&
		strings.TrimSpace(p.deviceAuthorizationEndpoint) != "" &&
		strings.TrimSpace(p.deviceClientID()) != ""
}

func (p *Provider) deviceClientID() string {
	if value := strings.TrimSpace(p.config.DeviceClientID); value != "" {
		return value
	}
	return strings.TrimSpace(p.config.ClientID)
}

func (p *Provider) deviceClientSecret() string {
	if value := strings.TrimSpace(p.config.DeviceClientSecret); value != "" {
		return value
	}
	if strings.TrimSpace(p.config.DeviceClientID) != "" {
		return ""
	}
	return strings.TrimSpace(p.config.ClientSecret)
}

// StartDeviceAuthorization starts the upstream device authorization flow.
func (p *Provider) StartDeviceAuthorization(ctx context.Context) (*DeviceAuthorization, error) {
	if !p.SupportsDeviceAuthorization() {
		return nil, ErrDeviceFlowNotSupported
	}

	form := url.Values{}
	form.Set("client_id", p.deviceClientID())
	if secret := p.deviceClientSecret(); secret != "" {
		form.Set("client_secret", secret)
	}
	if scopes := strings.Join(p.config.Scopes, " "); scopes != "" {
		form.Set("scope", scopes)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		p.deviceAuthorizationEndpoint,
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, fmt.Errorf("create device authorization request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("start device authorization: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read device authorization response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("device authorization failed: %s", strings.TrimSpace(string(body)))
	}

	var data struct {
		DeviceCode              string `json:"device_code"`
		UserCode                string `json:"user_code"`
		VerificationURI         string `json:"verification_uri"`
		VerificationURIComplete string `json:"verification_uri_complete"`
		ExpiresIn               int    `json:"expires_in"`
		Interval                int    `json:"interval"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("decode device authorization response: %w", err)
	}
	if data.Interval <= 0 {
		data.Interval = 5
	}
	return &DeviceAuthorization{
		DeviceCode:              data.DeviceCode,
		UserCode:                data.UserCode,
		VerificationURI:         data.VerificationURI,
		VerificationURIComplete: data.VerificationURIComplete,
		ExpiresIn:               data.ExpiresIn,
		Interval:                data.Interval,
	}, nil
}

// ExchangeDeviceCode polls the token endpoint for a completed device authorization flow.
func (p *Provider) ExchangeDeviceCode(ctx context.Context, deviceCode string) (*oauth2.Token, error) {
	if !p.SupportsDeviceAuthorization() {
		return nil, ErrDeviceFlowNotSupported
	}

	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:device_code")
	form.Set("device_code", deviceCode)
	form.Set("client_id", p.deviceClientID())

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		p.oauth2Config.Endpoint.TokenURL,
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, fmt.Errorf("create device token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	switch p.oauth2Config.Endpoint.AuthStyle {
	case oauth2.AuthStyleInHeader:
		if secret := p.deviceClientSecret(); secret != "" {
			req.SetBasicAuth(p.deviceClientID(), secret)
		}
	case oauth2.AuthStyleInParams, oauth2.AuthStyleAutoDetect:
		if secret := p.deviceClientSecret(); secret != "" {
			form.Set("client_secret", secret)
			req.Body = io.NopCloser(strings.NewReader(form.Encode()))
			req.ContentLength = int64(len(form.Encode()))
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("exchange device code: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read device token response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			Error            string `json:"error"`
			ErrorDescription string `json:"error_description"`
		}
		_ = json.Unmarshal(body, &apiErr)
		switch apiErr.Error {
		case "authorization_pending":
			return nil, ErrDeviceAuthorizationPending
		case "slow_down":
			return nil, ErrDeviceAuthorizationSlowDown
		case "access_denied":
			return nil, ErrDeviceAuthorizationDeclined
		case "expired_token":
			return nil, ErrDeviceAuthorizationExpired
		default:
			msg := strings.TrimSpace(apiErr.ErrorDescription)
			if msg == "" {
				msg = strings.TrimSpace(string(body))
			}
			return nil, fmt.Errorf("device token exchange failed: %s", msg)
		}
	}

	var data struct {
		AccessToken  string `json:"access_token"`
		TokenType    string `json:"token_type"`
		RefreshToken string `json:"refresh_token"`
		IDToken      string `json:"id_token"`
		ExpiresIn    int64  `json:"expires_in"`
		Scope        string `json:"scope"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("decode device token response: %w", err)
	}

	token := &oauth2.Token{
		AccessToken:  data.AccessToken,
		TokenType:    data.TokenType,
		RefreshToken: data.RefreshToken,
	}
	if data.ExpiresIn > 0 {
		token.Expiry = time.Now().Add(time.Duration(data.ExpiresIn) * time.Second)
	}
	extra := map[string]any{}
	if data.IDToken != "" {
		extra["id_token"] = data.IDToken
	}
	if data.Scope != "" {
		extra["scope"] = data.Scope
	}
	if len(extra) > 0 {
		token = token.WithExtra(extra)
	}
	return token, nil
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
