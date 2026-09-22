package runtimeauth

import (
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
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
func (g *InternalTokenGenerator) GenerateToken(teamID, userID, sandboxID string) (string, error) {
	// Note: sandboxID is passed for logging/tracing purposes but not embedded in the token
	// The token authenticates the manager to call procd, procd will use the X-Sandbox-ID header
	return g.generator.Generate("procd", teamID, userID, internalauth.GenerateOptions{})
}

// GenerateMigrationToken scopes a system-owned command to the exact sandbox,
// process instance, lifecycle epoch, action and immutable assignment digest.
func (g *InternalTokenGenerator) GenerateMigrationToken(request procdapi.RuntimeMigrationRequest) (string, error) {
	permission, err := request.Permission()
	if err != nil {
		return "", err
	}
	return g.generator.Generate("procd", request.Assignment.Target.TeamID, "", internalauth.GenerateOptions{
		SandboxID: request.Assignment.Target.SandboxID, Permissions: []string{permission},
	})
}
