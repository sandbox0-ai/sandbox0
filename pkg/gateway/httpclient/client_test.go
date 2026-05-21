package httpclient

import (
	"net/http"
	"testing"
	"time"
)

func TestResolveReturnsConfiguredClient(t *testing.T) {
	configured := &http.Client{}

	if got := Resolve(configured, time.Second); got != configured {
		t.Fatal("Resolve() did not return configured client")
	}
}

func TestResolveReturnsTimedFallbackClient(t *testing.T) {
	got := Resolve(nil, 2*time.Second)

	if got == nil {
		t.Fatal("Resolve() = nil")
	}
	if got.Timeout != 2*time.Second {
		t.Fatalf("Timeout = %s, want 2s", got.Timeout)
	}
}

func TestResolveUsesDefaultTimeout(t *testing.T) {
	got := Resolve(nil, 0)

	if got == nil {
		t.Fatal("Resolve() = nil")
	}
	if got.Timeout != DefaultTimeout {
		t.Fatalf("Timeout = %s, want %s", got.Timeout, DefaultTimeout)
	}
}
