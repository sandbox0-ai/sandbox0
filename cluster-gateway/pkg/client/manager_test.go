package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestManagerUnavailableStatusErrorUsesSpecMessage(t *testing.T) {
	err := managerUnavailableStatusError(503, []byte(`{"success":false,"error":{"code":"unavailable","message":"manager is draining"}}`))

	if !errors.Is(err, ErrManagerUnavailable) {
		t.Fatalf("error = %v, want ErrManagerUnavailable", err)
	}
	if !strings.Contains(err.Error(), "manager is draining") {
		t.Fatalf("error = %q, want spec message", err.Error())
	}
}

func TestManagerUnavailableStatusErrorFallsBackToBody(t *testing.T) {
	err := managerUnavailableStatusError(502, []byte(`plain error`))

	if !errors.Is(err, ErrManagerUnavailable) {
		t.Fatalf("error = %v, want ErrManagerUnavailable", err)
	}
	if !strings.Contains(err.Error(), "unexpected status code 502: plain error") {
		t.Fatalf("error = %q, want status and body", err.Error())
	}
}

func TestResumeSandboxUsesRequestContextInsteadOfClientTimeout(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	tokenGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/sandboxes/sandbox-1/auto-resume" {
			t.Errorf("resume path = %q", r.URL.Path)
		}
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer manager.Close()

	client := NewManagerClient(manager.URL, tokenGen, zap.NewNop(), 5*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := client.ResumeSandbox(ctx, "sandbox-1", "user-1", "team-1"); err != nil {
		t.Fatalf("ResumeSandbox() error = %v, want nil", err)
	}
}

func TestResumeSandboxMarksManagerErrorResponseAsDefinitiveFailure(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	tokenGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGatewayTimeout)
		_, _ = w.Write([]byte(`{"success":false,"error":{"code":"unavailable","message":"runtime initialization timed out"}}`))
	}))
	defer manager.Close()

	client := NewManagerClient(manager.URL, tokenGen, zap.NewNop(), time.Second)
	err = client.ResumeSandbox(context.Background(), "sandbox-1", "user-1", "team-1")
	if !errors.Is(err, ErrSandboxResumeFailed) {
		t.Fatalf("ResumeSandbox() error = %v, want ErrSandboxResumeFailed", err)
	}
	if strings.Contains(err.Error(), "unexpected status code") {
		t.Fatalf("ResumeSandbox() error = %q, want decoded manager message", err.Error())
	}
	if !strings.Contains(err.Error(), "runtime initialization timed out") {
		t.Fatalf("ResumeSandbox() error = %q, want manager message", err.Error())
	}
}

func TestResumeSandboxDoesNotMarkTransportUncertaintyAsDefinitiveFailure(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	tokenGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	client := NewManagerClient("http://127.0.0.1:1", tokenGen, zap.NewNop(), time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = client.ResumeSandbox(ctx, "sandbox-1", "user-1", "team-1")
	if err == nil {
		t.Fatal("ResumeSandbox() error = nil, want canceled request error")
	}
	if errors.Is(err, ErrSandboxResumeFailed) {
		t.Fatalf("ResumeSandbox() error = %v, do not want ErrSandboxResumeFailed", err)
	}
}

type resumeDeadlineTransport struct{ t *testing.T }

func (rt resumeDeadlineTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	deadline, ok := req.Context().Deadline()
	remaining := time.Until(deadline)
	if !ok || remaining <= 44*time.Second || remaining > 45*time.Second {
		rt.t.Errorf("resume deadline = %v", remaining)
	}
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
}

func TestResumeSandboxBoundsStartupWithoutCallerDeadline(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	gen := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: key, TTL: time.Minute})
	c := NewManagerClient("http://manager", gen, zap.NewNop(), time.Second)
	c.httpClient.Transport = resumeDeadlineTransport{t: t}
	if err := c.ResumeSandbox(context.Background(), "sb", "user", "team"); err != nil {
		t.Fatal(err)
	}
}
