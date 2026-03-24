package oidc

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

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

	authURL := provider.AuthURL(state, verifier)
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

	authURL, err := manager.GenerateAuthURL("supabase", "/")
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

	stateData, err := manager.ValidateState(state)
	if err != nil {
		t.Fatalf("validate state: %v", err)
	}
	if strings.TrimSpace(stateData.CodeVerifier) == "" {
		t.Fatal("expected code verifier to be stored with state")
	}
}
