package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func newTestAuthenticator(t *testing.T, resolver SessionClaimsResolver) *GRPCAuthenticator {
	t.Helper()

	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:                 internalauth.ServiceStorageProxy,
		PublicKey:              publicKey,
		ClockSkewTolerance:     0,
		ReplayDetectionEnabled: false,
	})
	return NewGRPCAuthenticator(validator, resolver, zap.NewNop())
}

func TestUnaryInterceptorAcceptsVolumeSessionCredential(t *testing.T) {
	t.Parallel()

	authenticator := newTestAuthenticator(t, SessionClaimsResolverFunc(func(_ context.Context, volumeID, sessionID, sessionSecret string) (*internalauth.Claims, error) {
		if volumeID != "vol-1" || sessionID != "session-1" || sessionSecret != "secret-1" {
			t.Fatalf("resolver input = (%q, %q, %q)", volumeID, sessionID, sessionSecret)
		}
		return &internalauth.Claims{TeamID: "team-1", Caller: internalauth.ServiceProcd}, nil
	}))

	md := metadata.Pairs(
		strings.ToLower(internalauth.VolumeSessionIDHeader), "session-1",
		strings.ToLower(internalauth.VolumeSessionSecretHeader), "secret-1",
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	interceptor := authenticator.UnaryInterceptor()
	_, err := interceptor(
		ctx,
		&pb.GetAttrRequest{VolumeId: "vol-1"},
		&grpc.UnaryServerInfo{FullMethod: "/sandbox0.fs.FileSystem/GetAttr"},
		func(callCtx context.Context, _ any) (any, error) {
			claims := internalauth.ClaimsFromContext(callCtx)
			if claims == nil || claims.TeamID != "team-1" {
				t.Fatalf("claims = %+v, want team-1", claims)
			}
			return &pb.GetAttrResponse{}, nil
		},
	)
	if err != nil {
		t.Fatalf("UnaryInterceptor() error = %v", err)
	}
}

func TestUnaryInterceptorFallsBackToSessionWhenTokenInvalid(t *testing.T) {
	t.Parallel()

	authenticator := newTestAuthenticator(t, SessionClaimsResolverFunc(func(_ context.Context, volumeID, sessionID, sessionSecret string) (*internalauth.Claims, error) {
		if volumeID != "vol-1" || sessionID != "session-1" || sessionSecret != "secret-1" {
			t.Fatalf("resolver input = (%q, %q, %q)", volumeID, sessionID, sessionSecret)
		}
		return &internalauth.Claims{TeamID: "team-1", Caller: internalauth.ServiceProcd}, nil
	}))

	md := metadata.Pairs(
		"x-internal-token", "not-a-valid-token",
		strings.ToLower(internalauth.VolumeSessionIDHeader), "session-1",
		strings.ToLower(internalauth.VolumeSessionSecretHeader), "secret-1",
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	interceptor := authenticator.UnaryInterceptor()
	_, err := interceptor(
		ctx,
		&pb.GetAttrRequest{VolumeId: "vol-1"},
		&grpc.UnaryServerInfo{FullMethod: "/sandbox0.fs.FileSystem/GetAttr"},
		func(callCtx context.Context, _ any) (any, error) {
			claims := internalauth.ClaimsFromContext(callCtx)
			if claims == nil || claims.TeamID != "team-1" {
				t.Fatalf("claims = %+v, want team-1", claims)
			}
			return &pb.GetAttrResponse{}, nil
		},
	)
	if err != nil {
		t.Fatalf("UnaryInterceptor() error = %v", err)
	}
}

func TestUnaryInterceptorRejectsSessionAuthWithoutVolumeID(t *testing.T) {
	t.Parallel()

	authenticator := newTestAuthenticator(t, SessionClaimsResolverFunc(func(context.Context, string, string, string) (*internalauth.Claims, error) {
		return &internalauth.Claims{TeamID: "team-1", Caller: internalauth.ServiceProcd}, nil
	}))

	md := metadata.Pairs(
		strings.ToLower(internalauth.VolumeSessionIDHeader), "session-1",
		strings.ToLower(internalauth.VolumeSessionSecretHeader), "secret-1",
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	interceptor := authenticator.UnaryInterceptor()
	_, err := interceptor(
		ctx,
		&pb.Empty{},
		&grpc.UnaryServerInfo{FullMethod: "/sandbox0.fs.FileSystem/GetAttr"},
		func(callCtx context.Context, _ any) (any, error) {
			_ = callCtx
			return &pb.Empty{}, nil
		},
	)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("status code = %v, want %v (err=%v)", status.Code(err), codes.Unauthenticated, err)
	}
}
