package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
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
