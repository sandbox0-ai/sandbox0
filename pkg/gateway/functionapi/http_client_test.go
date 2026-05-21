package functionapi

import (
	"net/http"
	"testing"
)

func TestResolveHTTPClientUsesConfiguredClient(t *testing.T) {
	t.Parallel()

	configured := &http.Client{}
	if got := resolveHTTPClient(configured); got != configured {
		t.Fatal("resolveHTTPClient did not preserve configured client")
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
