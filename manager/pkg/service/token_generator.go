package service

import (
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

// ProcdTokenGenerator generates tokens for procd to access storage-proxy.
type ProcdTokenGenerator struct {
	generator *internalauth.Generator
}

// NewProcdTokenGenerator creates a new procd token generator.
func NewProcdTokenGenerator(generator *internalauth.Generator) *ProcdTokenGenerator {
	return &ProcdTokenGenerator{
		generator: generator,
	}
}

// GenerateToken generates a token for procd to access storage-proxy.
func (g *ProcdTokenGenerator) GenerateToken(teamID, userID, sandboxID string) (string, error) {
	// Permissions required by procd to access storage-proxy
	return g.generator.Generate("storage-proxy", teamID, userID, internalauth.GenerateOptions{
		Permissions: []string{"sandboxvolume:read", "sandboxvolume:write"},
		SandboxID:   sandboxID,
	})
}

// StorageProxyAdminTokenGenerator generates manager tokens for storage-proxy volume lifecycle calls.
type StorageProxyAdminTokenGenerator struct {
	generator *internalauth.Generator
}

func NewStorageProxyAdminTokenGenerator(generator *internalauth.Generator) *StorageProxyAdminTokenGenerator {
	return &StorageProxyAdminTokenGenerator{generator: generator}
}

func (g *StorageProxyAdminTokenGenerator) GenerateToken(teamID, userID, sandboxID string) (string, error) {
	return g.generator.Generate("storage-proxy", teamID, userID, internalauth.GenerateOptions{
		Permissions: []string{
			"sandboxvolume:create",
			"sandboxvolume:read",
			"sandboxvolume:write",
			"sandboxvolume:delete",
		},
		SandboxID: sandboxID,
	})
}
