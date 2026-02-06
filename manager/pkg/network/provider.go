package network

import (
	"context"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
)

// SandboxPolicyInput contains the policy data needed by a network provider.
type SandboxPolicyInput struct {
	SandboxID       string
	Namespace       string
	PodName         string
	TeamID          string
	PodLabels       map[string]string
	NetworkPolicy   *v1alpha1.NetworkPolicySpec
}

// Provider applies sandbox network policies using an external dataplane.
type Provider interface {
	Name() string
	EnsureBaseline(ctx context.Context, namespace string) error
	ApplySandboxPolicy(ctx context.Context, input SandboxPolicyInput) error
	RemoveSandboxPolicy(ctx context.Context, namespace, sandboxID string) error
}

// NoopProvider is a default provider that performs no actions.
type NoopProvider struct{}

func (NoopProvider) Name() string { return "noop" }

func (NoopProvider) EnsureBaseline(ctx context.Context, namespace string) error {
	return nil
}

func (NoopProvider) ApplySandboxPolicy(ctx context.Context, input SandboxPolicyInput) error {
	return nil
}

func (NoopProvider) RemoveSandboxPolicy(ctx context.Context, namespace, sandboxID string) error {
	return nil
}

// NewNoopProvider returns a Provider that is safe for production defaults.
func NewNoopProvider() Provider {
	return NoopProvider{}
}
