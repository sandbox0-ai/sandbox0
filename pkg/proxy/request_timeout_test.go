package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
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

func TestAutoResumeRoutesUseAcquisitionBudget(t *testing.T) {
	for _, tc := range []struct {
		method, path string
		want         bool
	}{
		{"POST", "/api/v1/sandboxes/sb/contexts", true},
		{"GET", "/api/v1/sandboxes/sb/contexts/ctx", true},
		{"POST", "/api/v1/sandboxes/sb/contexts/ctx/exec", true},
		{"PUT", "/api/v1/sandboxes/sb/sessions/session/desired-state", true},
		{"GET", "/api/v1/sandboxes/sb/files/list", true},
		{"DELETE", "/api/v1/sandboxes/sb/files", true},
		{"POST", "/api/v1/sandboxes/sb/previews", true},
		{"PUT", "/api/v1/sandboxes/sb/previews/preview", true},
		{"DELETE", "/api/v1/sandboxes/sb/previews/preview", false},
		{"GET", "/api/v1/sandboxes/sb", false},
		{"GET", "/api/v1/sandboxes/sb/observability/logs", false},
		{"PUT", "/api/v1/sandboxes/sb/network", false},
		{"GET", "/api/v1/sandboxes//files", false},
		{"GET", "/api/v1/sandboxes/sb/files-other", false},
		{"OPTIONS", "/api/v1/sandboxes/sb/contexts", false},
		{"GET", "/api/v1/contexts", false},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			want := 10 * time.Second
			if tc.want {
				want = RuntimeAcquisitionTimeout
			}
			if got := requestUpstreamTimeout(req, 10*time.Second); got != want {
				t.Fatalf("timeout = %v, want %v", got, want)
			}
			client := &http.Client{Timeout: 10 * time.Second}
			if got := ClientForRequest(client, req).Timeout; got != want {
				t.Fatalf("client timeout = %v, want %v", got, want)
			}
			if got := requestUpstreamTimeout(WithLongLivedRequestRequest(req), 10*time.Second); got != 0 {
				t.Fatalf("stream timeout = %v", got)
			}
		})
	}
}
