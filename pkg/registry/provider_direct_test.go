package registry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"golang.org/x/oauth2"
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

	cred, err := p.GetPushCredentials(context.Background(), PushCredentialsRequest{TeamID: "team-1", TargetImage: "my-app:v1"})
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

func TestNewProvider_BuiltinAllowsTeamScopedTargetImages(t *testing.T) {
	p, err := NewProvider(config.RegistryConfig{
		Provider:     "builtin",
		PushRegistry: "registry.example.com",
		Builtin: &config.RegistryBuiltinConfig{
			Username: "u",
			Password: "p",
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewProvider returned error: %v", err)
	}

	prefix := naming.TeamImageRepositoryPrefix("team-1")
	tests := []string{
		"my-app:v1",
		prefix + "/my-app:v1",
		"registry.example.com/" + prefix + "/my-app:v1",
	}
	for _, targetImage := range tests {
		t.Run(targetImage, func(t *testing.T) {
			cred, err := p.GetPushCredentials(context.Background(), PushCredentialsRequest{TeamID: "team-1", TargetImage: targetImage})
			if err != nil {
				t.Fatalf("GetPushCredentials returned error: %v", err)
			}
			if cred.Provider != "builtin" {
				t.Fatalf("unexpected provider: %s", cred.Provider)
			}
		})
	}
}

func TestNewProvider_BuiltinRejectsOutOfScopeTargets(t *testing.T) {
	p, err := NewProvider(config.RegistryConfig{
		Provider:     "builtin",
		PushRegistry: "registry.example.com",
		Builtin: &config.RegistryBuiltinConfig{
			Username: "u",
			Password: "p",
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewProvider returned error: %v", err)
	}

	tests := []string{
		"",
		"t-other/probe:v1",
		"registry.example.com/t-other/probe:v1",
		"other-registry.example.com/my-app:v1",
	}
	for _, targetImage := range tests {
		t.Run(targetImage, func(t *testing.T) {
			_, err := p.GetPushCredentials(context.Background(), PushCredentialsRequest{TeamID: "team-1", TargetImage: targetImage})
			if !errors.Is(err, ErrInvalidTargetImage) {
				t.Fatalf("error = %v, want ErrInvalidTargetImage", err)
			}
		})
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

func TestNewProvider_GCPWithoutServiceAccountUsesADC(t *testing.T) {
	originalDefault := gcpDefaultTokenSource
	defer func() { gcpDefaultTokenSource = originalDefault }()

	expiresAt := time.Now().Add(5 * time.Minute).UTC()
	gcpDefaultTokenSource = func(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "adc-token", Expiry: expiresAt}), nil
	}

	p, err := NewProvider(config.RegistryConfig{
		Provider: "gcp",
		GCP: &config.RegistryGCPConfig{
			Registry: "us-east4-docker.pkg.dev",
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewProvider returned error: %v", err)
	}

	cred, err := p.GetPushCredentials(context.Background(), PushCredentialsRequest{TeamID: "team-1"})
	if err != nil {
		t.Fatalf("GetPushCredentials returned error: %v", err)
	}
	if cred.Username != "oauth2accesstoken" || cred.Password != "adc-token" {
		t.Fatalf("unexpected gcp credentials: %#v", cred)
	}
	if cred.ExpiresAt == nil || !cred.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("unexpected expiry: %#v", cred.ExpiresAt)
	}
	if got := cred.PushRegistry; got != naming.TeamScopedImageRegistry("us-east4-docker.pkg.dev", "team-1") {
		t.Fatalf("unexpected push registry: %q", got)
	}
}

func TestNewProvider_GCPADCErrorIsReturned(t *testing.T) {
	originalDefault := gcpDefaultTokenSource
	defer func() { gcpDefaultTokenSource = originalDefault }()

	gcpDefaultTokenSource = func(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
		return nil, errors.New("adc unavailable")
	}

	p, err := NewProvider(config.RegistryConfig{
		Provider: "gcp",
		GCP: &config.RegistryGCPConfig{
			Registry: "us-east4-docker.pkg.dev",
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewProvider returned error: %v", err)
	}

	if _, err := p.GetPushCredentials(context.Background(), PushCredentialsRequest{}); err == nil || err.Error() != "resolve gcp application default credentials: adc unavailable" {
		t.Fatalf("unexpected error: %v", err)
	}
}
