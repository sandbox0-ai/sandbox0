package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"golang.org/x/oauth2"
)

func TestProviderAuthURLIncludesPKCEChallenge(t *testing.T) {
	provider := &Provider{
		oauth2Config: &oauth2.Config{
			ClientID: "client-id",
			Endpoint: oauth2.Endpoint{
				AuthURL: "https://issuer.example.com/oauth/authorize",
			},
		},
	}

	const (
		state    = "state-123"
		verifier = "verifier-123"
	)

	authURL := provider.AuthURL(state, verifier, authorizationHints{
		LoginHint:  "invitee@example.com",
		ScreenHint: "signup",
	})
	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatalf("parse auth url: %v", err)
	}

	query := parsed.Query()
	if got := query.Get("state"); got != state {
		t.Fatalf("unexpected state %q", got)
	}
	if got := query.Get("code_challenge_method"); got != "S256" {
		t.Fatalf("unexpected code_challenge_method %q", got)
	}
	if got := query.Get("code_challenge"); got == "" {
		t.Fatal("expected code_challenge to be present")
	}
	if got := query.Get("login_hint"); got != "invitee@example.com" {
		t.Fatalf("unexpected login_hint %q", got)
	}
	if got := query.Get("screen_hint"); got != "signup" {
		t.Fatalf("unexpected screen_hint %q", got)
	}
}

func TestNormalizeOIDCIssuerURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "keeps issuer url",
			input: "https://example.com/auth/v1",
			want:  "https://example.com/auth/v1",
		},
		{
			name:  "strips discovery path",
			input: "https://example.com/auth/v1/.well-known/openid-configuration",
			want:  "https://example.com/auth/v1",
		},
		{
			name:  "trims surrounding whitespace",
			input: "  https://example.com/auth/v1/.well-known/openid-configuration  ",
			want:  "https://example.com/auth/v1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := normalizeOIDCIssuerURL(tt.input); got != tt.want {
				t.Fatalf("normalizeOIDCIssuerURL(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestResolveOIDCDiscoveryURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "keeps discovery document url",
			input: "https://example.com/auth/v1/.well-known/openid-configuration",
			want:  "https://example.com/auth/v1/.well-known/openid-configuration",
		},
		{
			name:  "builds discovery url from issuer",
			input: "https://example.com/auth/v1",
			want:  "https://example.com/auth/v1/.well-known/openid-configuration",
		},
		{
			name:  "trims trailing slash before appending discovery path",
			input: "https://example.com/",
			want:  "https://example.com/.well-known/openid-configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := resolveOIDCDiscoveryURL(tt.input); got != tt.want {
				t.Fatalf("resolveOIDCDiscoveryURL(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFetchDiscoveryMetadata(t *testing.T) {
	t.Parallel()

	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wellKnownOIDCConfigPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                        serverURL + "/",
			"authorization_endpoint":        serverURL + "/authorize",
			"token_endpoint":                serverURL + "/oauth/token",
			"jwks_uri":                      serverURL + "/jwks",
			"device_authorization_endpoint": serverURL + "/oauth/device/code",
		})
	}))
	serverURL = server.URL
	defer server.Close()

	metadata, err := fetchDiscoveryMetadata(context.Background(), server.URL, server.Client())
	if err != nil {
		t.Fatalf("fetchDiscoveryMetadata: %v", err)
	}
	if metadata.Issuer != serverURL+"/" {
		t.Fatalf("unexpected issuer %q", metadata.Issuer)
	}
	if metadata.DeviceAuthorizationEndpoint != serverURL+"/oauth/device/code" {
		t.Fatalf("unexpected device_authorization_endpoint %q", metadata.DeviceAuthorizationEndpoint)
	}
}

func TestResolveHTTPClientUsesTimeoutDefault(t *testing.T) {
	t.Parallel()

	got := resolveHTTPClient(nil)
	if got == nil {
		t.Fatal("resolveHTTPClient returned nil")
	}
	if got.Timeout != defaultHTTPTimeout {
		t.Fatalf("timeout = %s, want %s", got.Timeout, defaultHTTPTimeout)
	}
}

func TestProviderStartDeviceAuthorizationUsesConfiguredHTTPClient(t *testing.T) {
	var calls int
	provider := &Provider{
		config: &config.OIDCProviderConfig{
			DeviceAuthorizationEnabled: true,
			DeviceClientID:             "device-client",
			Scopes:                     []string{"openid", "email"},
		},
		deviceAuthorizationEndpoint: "https://issuer.example.com/oauth/device/code",
		httpClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				if req.URL.String() != "https://issuer.example.com/oauth/device/code" {
					t.Fatalf("request URL = %q, want device endpoint", req.URL.String())
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body: io.NopCloser(strings.NewReader(`{
						"device_code": "device-code",
						"user_code": "user-code",
						"verification_uri": "https://issuer.example.com/activate",
						"expires_in": 600
					}`)),
				}, nil
			}),
		},
	}

	auth, err := provider.StartDeviceAuthorization(context.Background())
	if err != nil {
		t.Fatalf("StartDeviceAuthorization returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("http client calls = %d, want 1", calls)
	}
	if auth.DeviceCode != "device-code" || auth.Interval != 5 {
		t.Fatalf("device authorization = %#v, want decoded response with default interval", auth)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNewProviderAcceptsIssuerWithTrailingSlashFromDiscovery(t *testing.T) {
	t.Parallel()

	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wellKnownOIDCConfigPath:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"issuer":                        serverURL + "/",
				"authorization_endpoint":        serverURL + "/authorize",
				"token_endpoint":                serverURL + "/oauth/token",
				"jwks_uri":                      serverURL + "/jwks",
				"device_authorization_endpoint": serverURL + "/oauth/device/code",
			})
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"keys":[]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	serverURL = server.URL
	defer server.Close()

	provider, err := NewProvider(context.Background(), &config.OIDCProviderConfig{
		ID:                         "auth0",
		Name:                       "Auth0",
		Enabled:                    true,
		ClientID:                   "client-id",
		ClientSecret:               "client-secret",
		DiscoveryURL:               serverURL + wellKnownOIDCConfigPath,
		TokenEndpointAuthMethod:    "client_secret_basic",
		Scopes:                     []string{"openid", "email", "profile"},
		DeviceAuthorizationEnabled: true,
	}, "https://api.sandbox0.ai")
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	if provider.deviceAuthorizationEndpoint != serverURL+"/oauth/device/code" {
		t.Fatalf("unexpected device authorization endpoint %q", provider.deviceAuthorizationEndpoint)
	}
}

func TestVerifyTokenAcceptsPrimaryAndDeviceClientAudiences(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}

	keyID := "test-key"
	publicJWK := jose.JSONWebKey{Key: &privateKey.PublicKey, KeyID: keyID, Algorithm: string(jose.RS256), Use: "sig"}
	privateJWK := jose.JSONWebKey{Key: privateKey, KeyID: keyID, Algorithm: string(jose.RS256), Use: "sig"}

	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wellKnownOIDCConfigPath:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"issuer":                 serverURL + "/",
				"authorization_endpoint": serverURL + "/authorize",
				"token_endpoint":         serverURL + "/oauth/token",
				"jwks_uri":               serverURL + "/jwks",
			})
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{publicJWK}})
		default:
			http.NotFound(w, r)
		}
	}))
	serverURL = server.URL
	defer server.Close()

	provider, err := NewProvider(context.Background(), &config.OIDCProviderConfig{
		ID:             "auth0",
		Name:           "Auth0",
		Enabled:        true,
		ClientID:       "browser-client",
		ClientSecret:   "browser-secret",
		DeviceClientID: "device-client",
		DiscoveryURL:   serverURL + wellKnownOIDCConfigPath,
		Scopes:         []string{"openid", "email", "profile"},
	}, "https://api.sandbox0.ai")
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	for _, tc := range []struct {
		name string
		aud  string
	}{
		{name: "primary client audience", aud: "browser-client"},
		{name: "device client audience", aud: "device-client"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tokenString := mustSignTestIDToken(t, privateJWK, serverURL+"/", tc.aud)
			token := (&oauth2.Token{AccessToken: "access-token"}).WithExtra(map[string]any{"id_token": tokenString})

			userInfo, err := provider.VerifyToken(context.Background(), token)
			if err != nil {
				t.Fatalf("VerifyToken: %v", err)
			}
			if userInfo.Subject != "auth0|user-123" {
				t.Fatalf("unexpected subject %q", userInfo.Subject)
			}
			if userInfo.Email != "dev@sandbox0.ai" {
				t.Fatalf("unexpected email %q", userInfo.Email)
			}
		})
	}
}

func mustSignTestIDToken(t *testing.T, signingKey jose.JSONWebKey, issuer, audience string) string {
	t.Helper()

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: signingKey}, nil)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}

	raw, err := jwt.Signed(signer).Claims(jwt.Claims{
		Issuer:   issuer,
		Subject:  "auth0|user-123",
		Audience: jwt.Audience{audience},
		IssuedAt: jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}).Claims(map[string]any{
		"email":          "dev@sandbox0.ai",
		"email_verified": true,
		"name":           "Sandbox0 Dev",
		"picture":        "https://example.com/avatar.png",
	}).Serialize()
	if err != nil {
		t.Fatalf("sign id token: %v", err)
	}
	return raw
}

func TestStartDeviceAuthorizationUsesDeviceClientWithoutBrowserSecretFallback(t *testing.T) {
	t.Parallel()

	var (
		gotClientID     string
		gotClientSecret string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth/device/code" {
			http.NotFound(w, r)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		gotClientID = r.Form.Get("client_id")
		gotClientSecret = r.Form.Get("client_secret")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"device_code":"device-code","user_code":"ABCD-EFGH","verification_uri":"https://example.com/activate","verification_uri_complete":"https://example.com/activate?user_code=ABCD-EFGH","expires_in":900,"interval":5}`)
	}))
	defer server.Close()

	provider := &Provider{
		config: &config.OIDCProviderConfig{
			ClientID:                   "browser-client",
			ClientSecret:               "browser-secret",
			DeviceAuthorizationEnabled: true,
			DeviceClientID:             "device-client",
			Scopes:                     []string{"openid", "email", "profile"},
		},
		deviceAuthorizationEndpoint: server.URL + "/oauth/device/code",
	}

	if _, err := provider.StartDeviceAuthorization(context.Background()); err != nil {
		t.Fatalf("StartDeviceAuthorization: %v", err)
	}
	if gotClientID != "device-client" {
		t.Fatalf("unexpected client_id %q", gotClientID)
	}
	if gotClientSecret != "" {
		t.Fatalf("expected no client_secret fallback, got %q", gotClientSecret)
	}
}

func TestStartDeviceAuthorizationFallsBackToPrimaryClientSecretWhenSharingClient(t *testing.T) {
	t.Parallel()

	var (
		gotClientID     string
		gotClientSecret string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth/device/code" {
			http.NotFound(w, r)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		gotClientID = r.Form.Get("client_id")
		gotClientSecret = r.Form.Get("client_secret")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"device_code":"device-code","user_code":"ABCD-EFGH","verification_uri":"https://example.com/activate","verification_uri_complete":"https://example.com/activate?user_code=ABCD-EFGH","expires_in":900,"interval":5}`)
	}))
	defer server.Close()

	provider := &Provider{
		config: &config.OIDCProviderConfig{
			ClientID:                   "browser-client",
			ClientSecret:               "browser-secret",
			DeviceAuthorizationEnabled: true,
			Scopes:                     []string{"openid", "email", "profile"},
		},
		deviceAuthorizationEndpoint: server.URL + "/oauth/device/code",
	}

	if _, err := provider.StartDeviceAuthorization(context.Background()); err != nil {
		t.Fatalf("StartDeviceAuthorization: %v", err)
	}
	if gotClientID != "browser-client" {
		t.Fatalf("unexpected client_id %q", gotClientID)
	}
	if gotClientSecret != "browser-secret" {
		t.Fatalf("unexpected client_secret %q", gotClientSecret)
	}
}

func TestResolveTokenEndpointAuthStyle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    oauth2.AuthStyle
		wantErr string
	}{
		{
			name:  "defaults to auto detect",
			input: "",
			want:  oauth2.AuthStyleAutoDetect,
		},
		{
			name:  "accepts auto",
			input: "auto",
			want:  oauth2.AuthStyleAutoDetect,
		},
		{
			name:  "maps client secret basic",
			input: "client_secret_basic",
			want:  oauth2.AuthStyleInHeader,
		},
		{
			name:  "maps client secret post",
			input: "client_secret_post",
			want:  oauth2.AuthStyleInParams,
		},
		{
			name:    "rejects unsupported values",
			input:   "private_key_jwt",
			wantErr: `unsupported token endpoint auth method "private_key_jwt"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveTokenEndpointAuthStyle(tt.input)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("resolveTokenEndpointAuthStyle(%q) error = %v, want %q", tt.input, err, tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("resolveTokenEndpointAuthStyle(%q): %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("resolveTokenEndpointAuthStyle(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestProviderExchangeSendsPKCEVerifier(t *testing.T) {
	var (
		gotAuthorization string
		gotBody          url.Values
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		gotAuthorization = r.Header.Get("Authorization")
		gotBody = r.PostForm
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"token","token_type":"Bearer","expires_in":3600}`)
	}))
	defer server.Close()

	provider := &Provider{
		oauth2Config: &oauth2.Config{
			ClientID:     "client-id",
			ClientSecret: "client-secret",
			RedirectURL:  "https://api.sandbox0.ai/auth/oidc/supabase/callback",
			Endpoint: oauth2.Endpoint{
				TokenURL: server.URL,
			},
		},
	}

	if _, err := provider.Exchange(context.Background(), "code-123", "verifier-123"); err != nil {
		t.Fatalf("exchange code: %v", err)
	}

	if got := gotBody.Get("code_verifier"); got != "verifier-123" {
		t.Fatalf("unexpected code_verifier %q", got)
	}
	if got := gotBody.Get("code"); got != "code-123" {
		t.Fatalf("unexpected code %q", got)
	}
	if got := gotBody.Get("grant_type"); got != "authorization_code" {
		t.Fatalf("unexpected grant_type %q", got)
	}

	wantAuthorization := "Basic " + base64.StdEncoding.EncodeToString([]byte("client-id:client-secret"))
	if gotAuthorization != wantAuthorization {
		t.Fatalf("unexpected authorization header %q", gotAuthorization)
	}
}

func TestManagerGenerateAuthURLStoresVerifierInState(t *testing.T) {
	manager := &Manager{
		providers: map[string]*Provider{
			"supabase": {
				id: "supabase",
				oauth2Config: &oauth2.Config{
					ClientID: "client-id",
					Endpoint: oauth2.Endpoint{
						AuthURL: "https://issuer.example.com/oauth/authorize",
					},
				},
			},
		},
		stateTTL: time.Hour,
		states:   map[string]*StateData{},
	}

	authURL, err := manager.GenerateAuthURL(
		"supabase",
		"/",
		WithLoginHint(" invitee@example.com "),
		WithSignupScreen(),
	)
	if err != nil {
		t.Fatalf("generate auth url: %v", err)
	}

	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatalf("parse auth url: %v", err)
	}
	state := parsed.Query().Get("state")
	if state == "" {
		t.Fatal("expected state query parameter")
	}
	if got := parsed.Query().Get("login_hint"); got != "invitee@example.com" {
		t.Fatalf("unexpected login_hint %q", got)
	}
	if got := parsed.Query().Get("screen_hint"); got != "signup" {
		t.Fatalf("unexpected screen_hint %q", got)
	}

	stateData, err := manager.ValidateState(state)
	if err != nil {
		t.Fatalf("validate state: %v", err)
	}
	if strings.TrimSpace(stateData.CodeVerifier) == "" {
		t.Fatal("expected code verifier to be stored with state")
	}
}
