package nomadruntime

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
)

type migrationFinalizeHost struct{ *retirementReserveHost }

func (*migrationFinalizeHost) FreezeXFS(string) error { return nil }
func (*migrationFinalizeHost) ThawXFS(string) error   { return nil }

func TestMigrationRootFSFinalizationRequiresRegionalTerminalWriterBeforeDeletingWAL(t *testing.T) {
	fixture := newRuntimeTerminalExpiryFixture(t)
	require.NoError(t, fixture.manager.Close())
	host := &migrationFinalizeHost{&retirementReserveHost{runtimeTerminalExpiryHost: fixture.host, devices: make(map[string]*retirementReserveDevice)}}
	fixture.config.Runtime = host
	var err error
	fixture.manager, err = rootfssession.New(fixture.config)
	require.NoError(t, err)
	stage := fixture.stage
	_, err = fixture.manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	cut, err := fixture.manager.CaptureMigrationRootFS(t.Context(), stage, rootfshandoff.MigrationRootFSCutRequest{OperationID: "migration", GenerationID: "migration-generation",
		CaptureRequestDigest: strings.Repeat("a", 64), SourceBindingDigest: hex.EncodeToString(binding[:])})
	require.NoError(t, err)
	detached, err := fixture.manager.DetachMigrationRootFS(t.Context(), stage, rootfshandoff.MigrationRootFSDetachRequest{OperationID: "migration", CutDigest: cut.Digest, AuthorizationDigest: strings.Repeat("b", 64)})
	require.NoError(t, err)
	request := rootfshandoff.MigrationRootFSFinalizeRequest{OperationID: "migration", DetachProofDigest: detached.Digest, AuthorizationDigest: strings.Repeat("c", 64)}
	authority := &retirementReserveAuthority{err: errdefs.ErrPermissionDenied}
	runtime := &rootfsRuntime{sessions: fixture.manager, authority: authority}
	_, err = runtime.FinalizeMigrationRootFS(t.Context(), stage, request)
	require.ErrorIs(t, err, errdefs.ErrPermissionDenied)
	require.FileExists(t, detached.Session.BranchPath)
	require.Equal(t, []rootfshandoff.StageRequest{stage}, authority.requests)
	authority.err = nil
	proof, err := runtime.FinalizeMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(stage, detached, request))
	require.NoFileExists(t, detached.Session.BranchPath)
	require.NoError(t, runtime.ForgetFinalizedMigrationRootFS(stage, request))
}
