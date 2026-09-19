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

func TestAcquisitionTimeoutIsBoundedAndPreservesEarlierDeadline(t *testing.T) {
	for _, path := range []string{"/api/v1/sandboxes", "/api/v1/sandboxes/sb/resume", "/api/v1/sandboxes/sb/fork"} {
		req, _ := http.NewRequest(http.MethodPost, "http://upstream"+path, nil)
		got, cancel := ApplyRequestTimeout(req, 10*time.Second)
		deadline, ok := got.Context().Deadline()
		if !ok || time.Until(deadline) < 44*time.Second || time.Until(deadline) > 45*time.Second {
			t.Fatalf("unexpected acquisition deadline: %v", deadline)
		}
		cancel()
		client := &http.Client{Timeout: 10 * time.Second}
		if ClientForRequest(client, req).Timeout != 45*time.Second || client.Timeout != 10*time.Second {
			t.Fatal("client deadline was not adjusted independently")
		}
		ctx, stop := context.WithTimeout(t.Context(), time.Second)
		got, cancel = ApplyRequestTimeout(req.WithContext(ctx), 10*time.Second)
		deadline, _ = got.Context().Deadline()
		if time.Until(deadline) > time.Second {
			t.Fatal("caller deadline extended")
		}
		stop()
		if got.Context().Err() != context.Canceled {
			t.Fatal("caller cancellation not propagated")
		}
		cancel()
	}
	for _, path := range []string{"/api/v1/sandboxes//resume", "/api/v1/sandboxes/sb/resume/extra", "/api/v1/sandboxes/sb/pause"} {
		req, _ := http.NewRequest(http.MethodPost, "http://upstream"+path, nil)
		if requestUpstreamTimeout(req, 10*time.Second) != 10*time.Second {
			t.Fatalf("unexpected extension for %s", path)
		}
	}
}
