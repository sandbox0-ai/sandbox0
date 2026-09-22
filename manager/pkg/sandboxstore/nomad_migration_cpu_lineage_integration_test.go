package sandboxstore

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadMigrationCPULineageSurvivesCompletedMoveAndNextCaptureIntegration(t *testing.T) {
	f, adoption, adopted := migrationFinalizationStoreFixture(t, "cpu-lineage")
	var payload []byte
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT restore_request FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, adoption.OperationID).Scan(&payload))
	var restored protocol.MigrationRestoreRequest
	require.NoError(t, json.Unmarshal(payload, &restored))
	guest := restored.Image.Publication.CPULaunch.GuestCPUProfile()
	host := protocol.MigrationCPUObservation{CPUSet: restored.Image.Resources.CPUSetCPUs, Profile: guest}
	host.Profile.Features = append(append([]string(nil), guest.Features...), "zzz_host_only")
	launch, err := protocol.BindMigrationCPURestore(restored, host, host)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, adoption, adopted))
	command, err := f.store.AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFinalization(f.ctx, *command, migrationFinalizationStoreProof(t, *command)))
	source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: source.ID, AllocationID: source.AllocationID,
		NodeUID: source.NodeUID, NodeBootID: source.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0x83}, 32)})
	require.NoError(t, err)
	_, err = f.store.CompleteNomadSandboxMigrationSource(f.ctx, adoption.OperationID)
	require.NoError(t, err)

	nextTarget := restored.Image.Publication.Assignment.Target
	revision, err := nextTarget.Revision()
	require.NoError(t, err)
	nextTarget.RuntimeGeneration++
	next := runtimecontrol.MigrationAssignment{OperationID: "cpu-lineage-next", SourceGeneration: launch.RuntimeGeneration, SourceRevision: revision, Target: nextTarget}
	migrationReadyTarget(t, f, "cpu-lineage-next", "c")
	_, err = f.store.ReserveNomadSandboxMigration(f.ctx, next)
	require.NoError(t, err)
	request, err := f.store.AuthorizeNomadSandboxMigrationCPUPreflight(f.ctx, next)
	require.NoError(t, err)
	want, err := request.Digest()
	require.NoError(t, err)
	for _, mode := range []string{"missing-lineage", "widened-guest", "narrowed-guest", "other-source", "other-restore"} {
		changed := launch.Clone()
		switch mode {
		case "missing-lineage":
			changed.Restored = nil
		case "widened-guest":
			changed.Restored.GuestProfile = host.Profile
		case "narrowed-guest":
			// Substitute another supported feature; provenance must reject it
			// even though this host is capable of running such a profile.
			changed.Restored.GuestProfile.Features = []string{"zzz_host_only"}
		case "other-source":
			changed.Restored.SourceLaunchDigest = "sha256:" + strings.Repeat("f", 64)
		case "other-restore":
			changed.Restored.RestoreRequestDigest = strings.Repeat("f", 64)
		}
		result := protocol.MigrationCPUPreflight{RequestDigest: want, Launch: *changed, Observation: host}
		require.NoError(t, result.ValidateFor(*request), mode)
		require.ErrorIs(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, next, *request, result), ErrNomadSandboxMigrationConflict, mode)
	}
	result := protocol.MigrationCPUPreflight{RequestDigest: want, Launch: *launch, Observation: host}
	require.NoError(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, next, *request, result))
	destination, err := f.store.AuthorizeNomadSandboxMigrationTargetCPUPreflight(f.ctx, next)
	require.NoError(t, err)
	targetDigest, err := destination.Digest()
	require.NoError(t, err)
	targetResult := protocol.MigrationCPUPreflight{RequestDigest: targetDigest, Launch: *destination.Launch,
		Observation: protocol.MigrationCPUObservation{CPUSet: destination.DestinationResources.CPUSetCPUs, Profile: guest}}
	require.NoError(t, targetResult.ValidateFor(*destination), "node C needs original guest features, not node B's extra features")
	require.NoError(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, next, *destination, targetResult))
	retainMigrationStagingFixture(t, f, next)
	prepare, err := f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, next, migrationSourcePolicy(f.sandboxID, next.Target.TeamID))
	require.NoError(t, err)
	capture, err := f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, preparedMigrationResponse(t, *prepare))
	require.NoError(t, err)
	captureDigest, err := capture.Digest()
	require.NoError(t, err)
	generation := *restored.Stage.Generation
	generation.GenerationID, generation.WriterEpoch = "migration-"+captureDigest, restored.Stage.Identity.WriterEpoch
	generation.LocatorVersion++
	cut, err := rootfshandoff.NewMigrationRootFSCut(rootfshandoff.MigrationRootFSCutRequest{OperationID: next.OperationID, CaptureRequestDigest: captureDigest,
		SourceBindingDigest: capture.BindingDigest, GenerationID: generation.GenerationID}, generation, 4)
	require.NoError(t, err)
	completed := protocol.MigrationCapture{Request: *capture, RequestDigest: captureDigest, State: protocol.MigrationCaptureComplete, RootFS: &cut}
	guestDigest, err := guest.Digest()
	require.NoError(t, err)
	hostDigest, err := host.Profile.Digest()
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, next, completed, hostDigest)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	publication, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, next, completed, guestDigest)
	require.NoError(t, err)
	require.Equal(t, launch, publication.CPULaunch)
	require.Equal(t, guestDigest, publication.CPUFeaturesDigest)
}
