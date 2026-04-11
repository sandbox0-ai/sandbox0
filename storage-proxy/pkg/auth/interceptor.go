package auth

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// GRPCAuthenticator handles gRPC request authentication using internalauth.
type GRPCAuthenticator struct {
	validator       *internalauth.Validator
	sessionResolver SessionClaimsResolver
	logger          *zap.Logger
}

// SessionClaimsResolver resolves volume session credentials into internalauth
// claims for downstream request handling.
type SessionClaimsResolver interface {
	ResolveVolumeSessionClaims(ctx context.Context, volumeID, sessionID, sessionSecret string) (*internalauth.Claims, error)
}

// SessionClaimsResolverFunc adapts a function to SessionClaimsResolver.
type SessionClaimsResolverFunc func(ctx context.Context, volumeID, sessionID, sessionSecret string) (*internalauth.Claims, error)

// ResolveVolumeSessionClaims implements SessionClaimsResolver.
func (f SessionClaimsResolverFunc) ResolveVolumeSessionClaims(ctx context.Context, volumeID, sessionID, sessionSecret string) (*internalauth.Claims, error) {
	return f(ctx, volumeID, sessionID, sessionSecret)
}

// NewGRPCAuthenticator creates a new gRPC authenticator.
func NewGRPCAuthenticator(validator *internalauth.Validator, sessionResolver SessionClaimsResolver, logger *zap.Logger) *GRPCAuthenticator {
	return &GRPCAuthenticator{
		validator:       validator,
		sessionResolver: sessionResolver,
		logger:          logger,
	}
}

// UnaryInterceptor returns a gRPC unary interceptor for authentication.
func (a *GRPCAuthenticator) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Skip logging and authentication for health check
		if info.FullMethod == "/grpc.health.v1.Health/Check" || info.FullMethod == "/grpc.health.v1.Health/Watch" {
			return handler(ctx, req)
		}

		// Extract and validate token
		claims, err := a.authenticate(ctx, req)
		if err != nil {
			a.logger.Warn("Authentication failed",
				zap.String("method", info.FullMethod),
				zap.Error(err),
			)
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}

		// Add claims to context for downstream handlers
		ctx = internalauth.WithClaims(ctx, claims)

		a.logger.Info("Request authenticated",
			zap.String("method", info.FullMethod),
			zap.String("team_id", claims.TeamID),
			zap.String("caller", claims.Caller),
		)

		return handler(ctx, req)
	}
}

// authenticate extracts token from gRPC metadata and validates it.
func (a *GRPCAuthenticator) authenticate(ctx context.Context, req any) (*internalauth.Claims, error) {
	// Extract metadata from context
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	var tokenString string
	// Check x-internal-token header
	if tokenHeaders := md["x-internal-token"]; len(tokenHeaders) > 0 {
		tokenString = tokenHeaders[0]
	}
	if tokenString != "" {
		// Validate token using internalauth.
		claims, err := a.validator.Validate(tokenString)
		if err == nil {
			return claims, nil
		}

		// Allow a valid volume session credential to recover from stale JWTs.
		if a.sessionResolver != nil {
			sessionClaims, sessionErr := a.authenticateWithSession(ctx, req, md)
			if sessionErr == nil {
				return sessionClaims, nil
			}
		}
		return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}

	if a.sessionResolver != nil {
		return a.authenticateWithSession(ctx, req, md)
	}

	return nil, status.Error(codes.Unauthenticated, "missing authentication token")
}

func (a *GRPCAuthenticator) authenticateWithSession(ctx context.Context, req any, md metadata.MD) (*internalauth.Claims, error) {
	sessionID := firstMetadataValue(md, strings.ToLower(internalauth.VolumeSessionIDHeader))
	sessionSecret := firstMetadataValue(md, strings.ToLower(internalauth.VolumeSessionSecretHeader))
	if sessionID == "" || sessionSecret == "" {
		return nil, status.Error(codes.Unauthenticated, "missing authentication token")
	}

	volumeID, err := volumeIDFromRequest(req)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	claims, err := a.sessionResolver.ResolveVolumeSessionClaims(ctx, volumeID, sessionID, sessionSecret)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid session credential: %v", err)
	}
	if claims == nil {
		return nil, status.Error(codes.Unauthenticated, "invalid session credential")
	}

	return claims, nil
}

func firstMetadataValue(md metadata.MD, key string) string {
	values := md[key]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

type volumeIDRequest interface {
	GetVolumeId() string
}

func volumeIDFromRequest(req any) (string, error) {
	if req == nil {
		return "", fmt.Errorf("missing request for session authentication")
	}
	requestWithVolumeID, ok := req.(volumeIDRequest)
	if !ok {
		return "", fmt.Errorf("request does not support session authentication")
	}
	volumeID := requestWithVolumeID.GetVolumeId()
	if volumeID == "" {
		return "", fmt.Errorf("missing volume id for session authentication")
	}
	return volumeID, nil
}

// StreamInterceptor returns a gRPC stream interceptor for authentication.
// This is useful for streaming RPCs if needed in the future.
func (a *GRPCAuthenticator) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		// Skip for health check
		if info.FullMethod == "/grpc.health.v1.Health/Check" || info.FullMethod == "/grpc.health.v1.Health/Watch" {
			return handler(srv, ss)
		}

		// Extract and validate token
		claims, err := a.authenticate(ss.Context(), nil)
		if err != nil {
			return status.Error(codes.Unauthenticated, err.Error())
		}

		a.logger.Info("Stream request authenticated",
			zap.String("method", info.FullMethod),
			zap.String("team_id", claims.TeamID),
			zap.String("caller", claims.Caller),
		)

		// Wrap the stream with authenticated context
		wrappedStream := &authenticatedStream{
			ServerStream: ss,
			ctx:          internalauth.WithClaims(ss.Context(), claims),
		}

		return handler(srv, wrappedStream)
	}
}

// authenticatedStream wraps grpc.ServerStream with an authenticated context.
type authenticatedStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the authenticated context.
func (s *authenticatedStream) Context() context.Context {
	return s.ctx
}
