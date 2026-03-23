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
