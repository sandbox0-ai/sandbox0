package sandboxstore

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func checkpointStoreFixture(t *testing.T, name string) (*nomadPauseStoreFixture, runtimecontrol.Assignment, string, string) {
	t.Helper()
	f, a := migrationStoreFixture(t, "checkpoint-"+name)
	source := a.Target
	source.RuntimeGeneration = a.SourceGeneration
	return f, source, "checkpoint-" + name, migrationSourcePolicy(source.SandboxID, source.TeamID)
}

func prepareCheckpointStoreFixture(t *testing.T, f *nomadPauseStoreFixture, source runtimecontrol.Assignment, operation, policy string) (*NomadSandboxCheckpoint, *procdapi.RuntimeCheckpointRequest) {
	t.Helper()
	c, err := f.store.ReserveNomadSandboxMemoryPause(f.ctx, operation, source, policy)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointCPU(f.ctx, c.Evidence.Preflight, migrationCPUStoreResult(t, f, c.Evidence.Preflight)))
	staging, err := f.store.AuthorizeNomadCheckpointStaging(f.ctx, operation)
	require.NoError(t, err)
	digest, err := staging.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointStaging(f.ctx, *staging, protocol.MigrationStagingReserved{RequestDigest: digest}))
	preparation, err := f.store.AuthorizeNomadCheckpointPreparation(f.ctx, operation)
	require.NoError(t, err)
	return c, preparation
}

func checkpointPreparedResponse(t *testing.T, request procdapi.RuntimeCheckpointRequest) procdapi.RuntimeCheckpointResponse {
	t.Helper()
	digest, err := request.Digest()
	require.NoError(t, err)
	return procdapi.RuntimeCheckpointResponse{InstanceID: request.InstanceID, RequestDigest: digest,
		RuntimeGeneration: request.Capture.RuntimeGeneration, State: "prepared"}
}

func TestNomadCheckpointReservesOnlySourceAndSerializesLifecycleIntegration(t *testing.T) {
	f, source, operation, policy := checkpointStoreFixture(t, "reservation")
	c, err := f.store.ReserveNomadSandboxMemoryPause(f.ctx, operation, source, policy)
	require.NoError(t, err)
	require.Equal(t, SandboxLifecycleKindPause, c.Lifecycle.Kind)
	require.Empty(t, c.Lifecycle.ToRuntimeID)
	require.Zero(t, c.Lifecycle.ToGeneration)
	require.True(t, c.Evidence.Preflight.CaptureOnly)
	require.Empty(t, c.Evidence.Preflight.Destination)
	require.True(t, c.Evidence.Preflight.DestinationResources.IsZero())
	require.False(t, c.Evidence.CaptureAuthorized)
	var slots, leases int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_slots`).Scan(&slots))
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
	require.Equal(t, 1, slots)
	require.Equal(t, 1, leases, "only the already-running source holds compute")
	before, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, f.initialGenerationID, before.HeadGenerationID)
	require.Equal(t, f.writerEpoch, before.WriterEpoch)
	require.Nil(t, c.Evidence.Preparation)
	require.Nil(t, c.Evidence.Staging)
	_, err = f.store.RequestNomadSandboxPause(f.ctx, source.SandboxID, SandboxLifecycleSourceManual)
	require.ErrorIs(t, err, ErrNomadSandboxPauseConflict, "ordinary filesystem pause cannot steal this writer")
	_, err = f.store.ReserveNomadSandboxMemoryPause(f.ctx, operation+"-other", source, policy)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	changed := source
	changed.EnvVars = map[string]string{"changed": "value"}
	_, err = f.store.ReserveNomadSandboxMemoryPause(f.ctx, operation, changed, policy)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	again, err := NewPGSandboxStore(f.pool).ReserveNomadSandboxMemoryPause(f.ctx, operation, source, policy)
	require.NoError(t, err)
	require.Equal(t, c.Evidence, again.Evidence)
	require.Equal(t, c.Lifecycle.Epoch, again.Lifecycle.Epoch)
}

func TestNomadCheckpointCaptureRequiresCPUStagingAndAuthenticatedBarrierIntegration(t *testing.T) {
	f, source, operation, policy := checkpointStoreFixture(t, "capture-gates")
	c, err := f.store.ReserveNomadSandboxMemoryPause(f.ctx, operation, source, policy)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadCheckpointStaging(f.ctx, operation)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	_, err = f.store.AuthorizeNomadCheckpointPreparation(f.ctx, operation)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	cpu := migrationCPUStoreResult(t, f, c.Evidence.Preflight)
	wrongCPU := cpu
	wrongCPU.Launch.LaunchAttempt = "another-launch"
	require.ErrorIs(t, f.store.CommitNomadCheckpointCPU(f.ctx, c.Evidence.Preflight, wrongCPU), ErrNomadCheckpointConflict)
	require.NoError(t, f.store.CommitNomadCheckpointCPU(f.ctx, c.Evidence.Preflight, cpu))
	wrongCPU = cpu
	wrongCPU.Launch.ExecutableDigest = "sha256:" + strings.Repeat("b", 64)
	require.ErrorIs(t, f.store.CommitNomadCheckpointCPU(f.ctx, c.Evidence.Preflight, wrongCPU), ErrNomadCheckpointConflict)
	staging, err := f.store.AuthorizeNomadCheckpointStaging(f.ctx, operation)
	require.NoError(t, err)
	require.True(t, staging.CaptureOnly)
	require.Equal(t, c.Evidence.Preflight.Source, staging.Source)
	require.Greater(t, staging.Bytes, c.Evidence.Preflight.SourceResources.MemoryBytes)
	require.Equal(t, 1, staging.CaptureUpload.Version)
	require.Equal(t, source.TeamID, staging.CaptureUpload.TeamID)
	require.NoError(t, staging.CaptureUpload.ValidateFor(staging.Source, staging.Bytes))
	_, err = f.store.AuthorizeNomadCheckpointPreparation(f.ctx, operation)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "issuing a staging command is not a receipt")
	digest, err := staging.Digest()
	require.NoError(t, err)
	receipt := protocol.MigrationStagingReserved{RequestDigest: digest}
	require.NoError(t, f.store.CommitNomadCheckpointStaging(f.ctx, *staging, receipt))
	preparation, err := f.store.AuthorizeNomadCheckpointPreparation(f.ctx, operation)
	require.NoError(t, err)
	require.Equal(t, c.Lifecycle.Epoch, preparation.CaptureEpoch)
	require.Equal(t, source.SandboxID, preparation.Capture.SandboxID)
	require.Nil(t, preparation.Restore)
	ack := checkpointPreparedResponse(t, *preparation)
	changed := ack
	changed.RuntimeGeneration++
	_, err = f.store.AuthorizeNomadCheckpointCapture(f.ctx, *preparation, changed)
	require.Error(t, err)
	capture, err := f.store.AuthorizeNomadCheckpointCapture(f.ctx, *preparation, ack)
	require.NoError(t, err)
	require.Equal(t, c.Evidence.Preflight.Source, *capture)
	_, err = f.store.AuthorizeNomadCheckpointPreparation(f.ctx, operation)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "capture cannot reopen preparation")
	retry, err := NewPGSandboxStore(f.pool).AuthorizeNomadCheckpointCapture(f.ctx, *preparation, ack)
	require.NoError(t, err)
	require.Equal(t, capture, retry)
	var phase string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, operation).Scan(&phase))
	require.Equal(t, SandboxLifecyclePhasePublishing, phase)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoints SET evidence=evidence-'capture_authorized' WHERE operation_id=$1`, operation)
	require.Error(t, err, "SQL cannot forget dispatch authority after an uncertain capture")
}

func TestNomadCheckpointConcurrentReservationKeepsOneEpochIntegration(t *testing.T) {
	f, source, operation, policy := checkpointStoreFixture(t, "concurrent")
	const count = 8
	results := make([]*NomadSandboxCheckpoint, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Go(func() { results[i], errs[i] = f.store.ReserveNomadSandboxMemoryPause(f.ctx, operation, source, policy) })
	}
	wg.Wait()
	for i := range count {
		require.NoError(t, errs[i])
		require.Equal(t, results[0].Lifecycle.Epoch, results[i].Lifecycle.Epoch)
		require.Equal(t, results[0].Evidence, results[i].Evidence)
	}
	var operations int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.sandbox_runtime_checkpoints`).Scan(&operations))
	require.Equal(t, 1, operations)
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoints
		SET evidence=jsonb_set(evidence,'{preflight,capture_only}','false') WHERE operation_id=$1`, operation)
	require.Error(t, err, "source-only authority is not interchangeable with migration")
}

func TestNomadCheckpointCaptureRejectsChangedSourceAndKeepsEvidenceIntegration(t *testing.T) {
	f, source, operation, policy := checkpointStoreFixture(t, "source-change")
	c, prepare := prepareCheckpointStoreFixture(t, f, source, operation, policy)
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET control_endpoint='unix:///replacement.sock' WHERE slot_id=$1`, f.slotID)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadCheckpointCapture(f.ctx, *prepare, checkpointPreparedResponse(t, *prepare))
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	var payload []byte
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT evidence FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, operation).Scan(&payload))
	var evidence NomadCheckpointEvidence
	require.NoError(t, json.Unmarshal(payload, &evidence))
	require.False(t, evidence.CaptureAuthorized)
	require.Equal(t, c.Evidence.Preflight.Source.Target, evidence.Preflight.Source.Target)
	require.NotNil(t, evidence.Preparation, "failed capture cannot lose ownership of the prepared API barrier")
}
