package runtimeauth

import (
	"crypto/ed25519"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
)

func TestMigrationTokenBindsExactCommandAndSandbox(t *testing.T) {
	public, private, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	generator := NewInternalTokenGenerator(internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "manager", PrivateKey: private}))
	source := runtimecontrol.Assignment{SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 1, SecurityClass: "standard"}
	revision, err := source.Revision()
	require.NoError(t, err)
	target := source
	target.RuntimeGeneration++
	request := procdapi.RuntimeMigrationRequest{Action: procdapi.MigrationRestore, InstanceID: "0daa8273-94ad-47a6-9e2d-54ed58d15fd7", LifecycleEpoch: 3,
		Assignment: runtimecontrol.MigrationAssignment{OperationID: "migration-1", SourceGeneration: 1, SourceRevision: revision, Target: target}}
	token, err := generator.GenerateMigrationToken(request)
	require.NoError(t, err)
	claims := &internalauth.Claims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) { return public, nil }, jwt.WithValidMethods([]string{"EdDSA"}))
	require.NoError(t, err)
	require.True(t, parsed.Valid)
	require.Equal(t, "manager", claims.Caller)
	require.Equal(t, "procd", claims.Target)
	require.Equal(t, source.TeamID, claims.TeamID)
	require.Equal(t, source.SandboxID, claims.SandboxID)
	require.Empty(t, claims.UserID)
	permission, err := request.Permission()
	require.NoError(t, err)
	require.Equal(t, []string{permission}, claims.Permissions)
	for _, mutate := range []func(*procdapi.RuntimeMigrationRequest){
		func(r *procdapi.RuntimeMigrationRequest) { r.Action = procdapi.MigrationPrepare },
		func(r *procdapi.RuntimeMigrationRequest) { r.LifecycleEpoch++ },
		func(r *procdapi.RuntimeMigrationRequest) { r.InstanceID = "499739fa-087e-4d3b-9416-7c507ac51fcd" },
		func(r *procdapi.RuntimeMigrationRequest) { r.Assignment.OperationID += "-other" },
	} {
		other := request
		mutate(&other)
		changed, err := other.Permission()
		require.NoError(t, err)
		require.NotContains(t, claims.Permissions, changed)
	}
	request.Assignment.Target.SandboxID = "foreign"
	_, err = generator.GenerateMigrationToken(request)
	require.Error(t, err)
}
