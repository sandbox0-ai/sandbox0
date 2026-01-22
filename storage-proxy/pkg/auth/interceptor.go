package auth

import (
	"context"

	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// GRPCAuthenticator handles gRPC request authentication using internalauth.
type GRPCAuthenticator struct {
	validator *internalauth.Validator
	logger    *zap.Logger
}

// NewGRPCAuthenticator creates a new gRPC authenticator.
func NewGRPCAuthenticator(validator *internalauth.Validator, logger *zap.Logger) *GRPCAuthenticator {
	return &GRPCAuthenticator{
		validator: validator,
		logger:    logger,
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
		claims, err := a.authenticate(ctx)
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
func (a *GRPCAuthenticator) authenticate(ctx context.Context) (*internalauth.Claims, error) {
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
	if tokenString == "" {
		return nil, status.Error(codes.Unauthenticated, "missing authentication token")
	}

	// Validate token using internalauth
	claims, err := a.validator.Validate(tokenString)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}

	return claims, nil
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
		claims, err := a.authenticate(ss.Context())
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
