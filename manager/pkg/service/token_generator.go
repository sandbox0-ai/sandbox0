package service

import (
	"context"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
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

// NewCtldRootFSTokenProvider creates manager-scoped tokens whose signed
// ownership claims are checked before ctld admits node-local rootfs work.
func NewCtldRootFSTokenProvider(generator *internalauth.Generator) ctldapi.RootFSTokenProvider {
	if generator == nil {
		return nil
	}
	return func(_ context.Context, teamID, sandboxID string) (string, error) {
		return generator.Generate(
			internalauth.ServiceCtld,
			teamID,
			"",
			internalauth.GenerateOptions{SandboxID: sandboxID},
		)
	}
}

// ManagerStorageAdminTokenGenerator generates manager tokens for volume lifecycle calls.
type ManagerStorageAdminTokenGenerator struct {
	generator *internalauth.Generator
}

func NewManagerStorageAdminTokenGenerator(generator *internalauth.Generator) *ManagerStorageAdminTokenGenerator {
	return &ManagerStorageAdminTokenGenerator{generator: generator}
}

func (g *ManagerStorageAdminTokenGenerator) GenerateToken(teamID, userID, sandboxID string) (string, error) {
	if teamID == "" {
		return g.generator.GenerateSystem(internalauth.ServiceManagerStorage, internalauth.GenerateOptions{
			Permissions: []string{
				"sandboxvolume:create",
				"sandboxvolume:read",
				"sandboxvolume:write",
				"sandboxvolume:delete",
			},
			SandboxID: sandboxID,
		})
	}
	return g.generator.Generate(internalauth.ServiceManagerStorage, teamID, userID, internalauth.GenerateOptions{
		Permissions: []string{
			"sandboxvolume:create",
			"sandboxvolume:read",
			"sandboxvolume:write",
			"sandboxvolume:delete",
		},
		SandboxID: sandboxID,
	})
}
