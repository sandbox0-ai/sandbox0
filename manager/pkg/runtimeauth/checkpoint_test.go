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

func TestCheckpointTokenSeparatesCaptureFromForkIdentity(t *testing.T) {
	public, private, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	generator := NewInternalTokenGenerator(internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "manager", PrivateKey: private}))
	source := runtimecontrol.Assignment{SandboxID: "parent", TeamID: "team", RuntimeGeneration: 2, SecurityClass: "standard"}
	capture, err := runtimecontrol.NewCheckpointCaptureAssignment("capture", source)
	require.NoError(t, err)
	prepare := procdapi.RuntimeCheckpointRequest{Action: procdapi.MigrationPrepare, InstanceID: "0daa8273-94ad-47a6-9e2d-54ed58d15fd7", CaptureEpoch: 3, LifecycleEpoch: 3, Capture: capture}
	child := source
	child.SandboxID = "child"
	child.RuntimeGeneration = 1
	restore := prepare
	restore.Action = procdapi.MigrationRestore
	restore.LifecycleEpoch = 4
	restore.Restore = &runtimecontrol.CheckpointRestoreAssignment{OperationID: "fork-restore", Kind: runtimecontrol.CheckpointFork, Capture: capture, Target: child}
	for _, request := range []procdapi.RuntimeCheckpointRequest{prepare, restore} {
		token, err := generator.GenerateCheckpointToken(request)
		require.NoError(t, err)
		claims := &internalauth.Claims{}
		parsed, err := jwt.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) { return public, nil }, jwt.WithValidMethods([]string{"EdDSA"}))
		require.NoError(t, err)
		require.True(t, parsed.Valid)
		team, sandbox := request.ActingSandbox()
		require.Equal(t, team, claims.TeamID)
		require.Equal(t, sandbox, claims.SandboxID)
		require.Equal(t, "manager", claims.Caller)
		require.Equal(t, "procd", claims.Target)
		require.Empty(t, claims.UserID)
		permission, err := request.Permission()
		require.NoError(t, err)
		require.Equal(t, []string{permission}, claims.Permissions)
		other := request
		other.InstanceID = "499739fa-087e-4d3b-9416-7c507ac51fcd"
		permission, err = other.Permission()
		require.NoError(t, err)
		require.NotContains(t, claims.Permissions, permission)
	}
	restore.Restore.Target.TeamID = "foreign"
	token, err := generator.GenerateCheckpointToken(restore)
	require.Error(t, err)
	require.Empty(t, token)
}
