package auth

import (
	"context"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type SessionClaimsResolver interface {
	ResolveVolumeSessionClaims(ctx context.Context, volumeID, sessionID, sessionSecret string) (*internalauth.Claims, error)
}

type SessionClaimsResolverFunc func(ctx context.Context, volumeID, sessionID, sessionSecret string) (*internalauth.Claims, error)

// ResolveVolumeSessionClaims implements SessionClaimsResolver.
func (f SessionClaimsResolverFunc) ResolveVolumeSessionClaims(ctx context.Context, volumeID, sessionID, sessionSecret string) (*internalauth.Claims, error) {
	return f(ctx, volumeID, sessionID, sessionSecret)
}
