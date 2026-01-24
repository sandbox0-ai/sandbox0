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
	BandwidthPolicy *v1alpha1.BandwidthPolicySpec
}

// Provider applies sandbox network policies using an external dataplane.
type Provider interface {
	Name() string
	EnsureBaseline(ctx context.Context, namespace string) error
	ApplySandboxPolicy(ctx context.Context, input SandboxPolicyInput) error
	RemoveSandboxPolicy(ctx context.Context, namespace, sandboxID string) error
}
