package resolver

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

// ResolveResult contains one resolved broker response and its cache lifetime.
type ResolveResult struct {
	Response *egressauth.ResolveResponse
	TTL      time.Duration
}

// Provider resolves one credential binding into runtime auth material.
type Provider interface {
	Resolve(ctx context.Context, req *egressauth.ResolveRequest, binding *egressauth.CredentialBinding, defaultTTL time.Duration) (*ResolveResult, error)
}

// UnsupportedProviderError indicates the binding references a provider that is
// not available in this broker process.
type UnsupportedProviderError struct {
	Provider string
}

func (e *UnsupportedProviderError) Error() string {
	return fmt.Sprintf("credential binding provider %q is not supported", e.Provider)
}
