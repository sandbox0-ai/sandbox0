package service

import (
	"github.com/sandbox0-ai/infra/pkg/internalauth"
)

// InternalTokenGenerator generates internal tokens using Ed25519 signing.
type InternalTokenGenerator struct {
	generator *internalauth.Generator
}

// NewInternalTokenGenerator creates a new token generator.
func NewInternalTokenGenerator(generator *internalauth.Generator) *InternalTokenGenerator {
	return &InternalTokenGenerator{
		generator: generator,
	}
}

// GenerateToken generates an internal token for procd authentication.
func (g *InternalTokenGenerator) GenerateToken(teamID, sandboxID string) (string, error) {
	// Note: sandboxID is passed for logging/tracing purposes but not embedded in the token
	// The token authenticates the manager to call procd, procd will use the X-Sandbox-ID header
	return g.generator.Generate("procd", teamID, "", internalauth.GenerateOptions{
		RequestID: sandboxID, // Use sandboxID as request ID for tracing
	})
}
