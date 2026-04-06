package registry

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

func TestNewProvider_BuiltinWithDirectCredentials(t *testing.T) {
	p, err := NewProvider(config.RegistryConfig{
		Provider:     "builtin",
		PushRegistry: "registry.example.com",
		PullRegistry: "registry.internal.svc:5000",
		Builtin: &config.RegistryBuiltinConfig{
			Username: "u",
			Password: "p",
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewProvider returned error: %v", err)
	}
	if p == nil {
		t.Fatal("NewProvider returned nil provider")
	}

	cred, err := p.GetPushCredentials(context.Background(), PushCredentialsRequest{TeamID: "team-1"})
	if err != nil {
		t.Fatalf("GetPushCredentials returned error: %v", err)
	}
	if cred.Provider != "builtin" {
		t.Fatalf("unexpected provider: %s", cred.Provider)
	}
	wantPush := naming.TeamScopedImageRegistry("registry.example.com", "team-1")
	if cred.PushRegistry != wantPush {
		t.Fatalf("unexpected push registry: %s", cred.PushRegistry)
	}
	wantPull := naming.TeamScopedImageRegistry("registry.internal.svc:5000", "team-1")
	if cred.PullRegistry != wantPull {
		t.Fatalf("unexpected pull registry: %s", cred.PullRegistry)
	}
	if cred.Username != "u" || cred.Password != "p" {
		t.Fatalf("unexpected credentials: %s/%s", cred.Username, cred.Password)
	}
}

func TestNewProvider_HarborWithDirectCredentials(t *testing.T) {
	p, err := NewProvider(config.RegistryConfig{
		Provider: "harbor",
		Harbor: &config.RegistryHarborConfig{
			Registry: "harbor.example.com",
			Username: "robot$ci",
			Password: "secret-token",
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewProvider returned error: %v", err)
	}
	if p == nil {
		t.Fatal("NewProvider returned nil provider")
	}

	cred, err := p.GetPushCredentials(context.Background(), PushCredentialsRequest{TeamID: "team-1"})
	if err != nil {
		t.Fatalf("GetPushCredentials returned error: %v", err)
	}
	if cred.Provider != "harbor" {
		t.Fatalf("unexpected provider: %s", cred.Provider)
	}
	wantPush := naming.TeamScopedImageRegistry("harbor.example.com", "team-1")
	if cred.PushRegistry != wantPush {
		t.Fatalf("unexpected push registry: %s", cred.PushRegistry)
	}
	wantPull := naming.TeamScopedImageRegistry("harbor.example.com", "team-1")
	if cred.PullRegistry != wantPull {
		t.Fatalf("unexpected pull registry: %s", cred.PullRegistry)
	}
	if cred.Username != "robot$ci" || cred.Password != "secret-token" {
		t.Fatalf("unexpected credentials: %s/%s", cred.Username, cred.Password)
	}
}
