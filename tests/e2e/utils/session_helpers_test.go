package utils

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestSessionDoJSONRequestAddsSelectedTeamHeader(t *testing.T) {
	t.Parallel()

	var gotAuth string
	var gotTeamID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotTeamID = r.Header.Get(internalauth.TeamIDHeader)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()

	session := &Session{
		baseURL: server.URL,
		token:   "token-123",
		teamID:  "team-123",
		client:  server.Client(),
	}

	status, _, err := session.doJSONRequest(context.Background(), http.MethodGet, "/api/v1/templates", nil, true)
	if err != nil {
		t.Fatalf("doJSONRequest returned error: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, status)
	}
	if gotAuth != "Bearer token-123" {
		t.Fatalf("expected authorization header to be set, got %q", gotAuth)
	}
	if gotTeamID != "team-123" {
		t.Fatalf("expected team header to be set, got %q", gotTeamID)
	}
}

func TestSessionDoJSONRequestOmitsSelectedTeamHeaderWhenUnset(t *testing.T) {
	t.Parallel()

	var gotTeamID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTeamID = r.Header.Get(internalauth.TeamIDHeader)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()

	session := &Session{
		baseURL: server.URL,
		token:   "token-123",
		client:  server.Client(),
	}

	if _, _, err := session.doJSONRequest(context.Background(), http.MethodGet, "/teams", nil, true); err != nil {
		t.Fatalf("doJSONRequest returned error: %v", err)
	}
	if gotTeamID != "" {
		t.Fatalf("expected team header to be omitted, got %q", gotTeamID)
	}
}
