package policy

import (
	"net/http"
	"testing"
)

func TestMatchHTTPRequestMatchesMethodPathHeaderAndQuery(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://api.example.com/repos/acme/app/issues?dry_run=true", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/vnd.example+json")

	match := &CompiledHTTPMatch{
		Methods:      []string{http.MethodPost},
		PathPrefixes: []string{"/repos/"},
		Query: []CompiledHTTPValueMatch{{
			Name:   "dry_run",
			Values: []string{"true"},
		}},
		Headers: []CompiledHTTPValueMatch{{
			Name:   "accept",
			Values: []string{"application/vnd.example+json"},
		}},
	}

	if !MatchHTTPRequest(match, req) {
		t.Fatal("expected request match")
	}
}

func TestMatchHTTPRequestRejectsMethodMismatch(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://api.example.com/repos/acme/app/issues", nil)
	if err != nil {
		t.Fatal(err)
	}

	match := &CompiledHTTPMatch{
		Methods: []string{http.MethodPost},
	}

	if MatchHTTPRequest(match, req) {
		t.Fatal("expected method mismatch")
	}
}

func TestMatchHTTPRequestSupportsPresenceOnlyHeader(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://api.example.com/v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Trace-ID", "trace-123")

	match := &CompiledHTTPMatch{
		Headers: []CompiledHTTPValueMatch{{
			Name:    "x-trace-id",
			Present: true,
		}},
	}

	if !MatchHTTPRequest(match, req) {
		t.Fatal("expected presence-only header match")
	}
}
