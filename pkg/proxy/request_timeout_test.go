package proxy

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestClientForRequestDisablesClientTimeoutForWhitelistedRequest(t *testing.T) {
	client := &http.Client{Timeout: time.Second}
	req, err := http.NewRequestWithContext(
		WithUpstreamTimeoutDisabled(context.Background()),
		http.MethodGet,
		"http://example.com",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	got := ClientForRequest(client, req)

	if got == client {
		t.Fatal("ClientForRequest() returned the original timed client")
	}
	if got.Timeout != 0 {
		t.Fatalf("ClientForRequest() timeout = %s, want 0", got.Timeout)
	}
	if client.Timeout != time.Second {
		t.Fatalf("original client timeout = %s, want 1s", client.Timeout)
	}
}

func TestClientForRequestPreservesDefaultTimeout(t *testing.T) {
	client := &http.Client{Timeout: time.Second}
	req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	if got := ClientForRequest(client, req); got != client {
		t.Fatal("ClientForRequest() replaced the client for a normal request")
	}
}
