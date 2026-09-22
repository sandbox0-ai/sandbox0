package sandboxstore

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationCPUStoreResult(t *testing.T, f *nomadPauseStoreFixture, request protocol.MigrationCPUPreflightRequest) protocol.MigrationCPUPreflight {
	t.Helper()
	source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	launch := protocol.MigrationCPULaunch{Version: protocol.MigrationCPULaunchVersion, Target: request.Source.Target,
		SandboxID: request.Source.SandboxID, RuntimeGeneration: request.Source.SourceGeneration, LaunchAttempt: source.LaunchAttempt,
		BindingDigest: request.Source.BindingDigest, ResourceLeaseDigest: request.Source.ResourceLeaseDigest, Resources: request.SourceResources,
		AssignmentRevision: request.Source.AssignmentRevision, ExecutableDigest: digest.FromString("test-runsc").String(),
		Observation: protocol.MigrationCPUObservation{CPUSet: request.SourceResources.CPUSetCPUs, Profile: protocol.MigrationCPUProfile{
			Version: protocol.MigrationCPUProfileVersion, Architecture: "amd64", RunscVersion: "runsc version release-20260817.0",
			Features: []string{"aes"}, CacheLineBytes: 64, XStateLayoutDigest: digest.FromString("test-xstate").String()}}}
	if request.Launch != nil {
		launch = *request.Launch
	}
	want, err := request.Digest()
	require.NoError(t, err)
	result := protocol.MigrationCPUPreflight{RequestDigest: want, Launch: launch, Observation: launch.Observation}
	if !request.IsSource() {
		result.Observation.CPUSet = request.DestinationResources.CPUSetCPUs
	}
	require.NoError(t, result.ValidateFor(request))
	return result
}

// These are controlled node receipts for transaction tests, not CPU attestation.
func retainMigrationCPUFixture(t *testing.T, f *nomadPauseStoreFixture, assignment runtimecontrol.MigrationAssignment) *protocol.MigrationCPULaunch {
	t.Helper()
	source, err := f.store.AuthorizeNomadSandboxMigrationCPUPreflight(f.ctx, assignment)
	require.NoError(t, err)
	sourceResult := migrationCPUStoreResult(t, f, *source)
	require.NoError(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, assignment, *source, sourceResult))
	target, err := f.store.AuthorizeNomadSandboxMigrationTargetCPUPreflight(f.ctx, assignment)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, assignment, *target, migrationCPUStoreResult(t, f, *target)))
	return &sourceResult.Launch
}

func TestNomadMigrationCPUPreflightRequiresBothExactReceiptsIntegration(t *testing.T) {
	f, a := migrationStoreFixture(t, "cpu-gates")
	migrationReadyTarget(t, f, "cpu-gates", "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
	require.NoError(t, err)
	policy := migrationSourcePolicy(f.sandboxID, a.Target.TeamID)
	_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.store.AuthorizeNomadSandboxMigrationTargetCPUPreflight(f.ctx, a)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	source, err := f.store.AuthorizeNomadSandboxMigrationCPUPreflight(f.ctx, a)
	require.NoError(t, err)
	result := migrationCPUStoreResult(t, f, *source)
	wrong := result
	wrong.Launch.LaunchAttempt = "another-launch"
	require.NoError(t, wrong.ValidateFor(*source), "regional launch custody is a separate gate")
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, a, *source, wrong), ErrNomadSandboxMigrationConflict)
	require.NoError(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, a, *source, result))
	_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "source alone cannot authorize a barrier")
	target, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationTargetCPUPreflight(f.ctx, a)
	require.NoError(t, err)
	require.Equal(t, result.Launch, *target.Launch)
	changed := *target
	changed.Destination.ControlEndpoint = "unix:///run/another.sock"
	changed.Target = changed.Destination
	require.NoError(t, changed.Validate())
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, a, changed, migrationCPUStoreResult(t, f, changed)), ErrNomadSandboxMigrationConflict)
	require.NoError(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, a, *target, migrationCPUStoreResult(t, f, *target)))
	retainMigrationStagingFixture(t, f, a)
	_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationCPUPreflight(f.ctx, a)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "preparation cannot restart CPU eligibility")
}

func TestNomadMigrationCPUPreflightConcurrentRetryRetainsDeadlineIntegration(t *testing.T) {
	f, a := migrationStoreFixture(t, "cpu-retry")
	migrationReadyTarget(t, f, "cpu-retry", "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
	require.NoError(t, err)
	request, err := f.store.AuthorizeNomadSandboxMigrationCPUPreflight(f.ctx, a)
	require.NoError(t, err)
	result := migrationCPUStoreResult(t, f, *request)
	var before, after time.Time
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT cpu_preflight_requested_at FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, a.OperationID).Scan(&before))
	const workers = 8
	errors := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			s := NewPGSandboxStore(f.pool)
			_, errors[i] = s.AuthorizeNomadSandboxMigrationCPUPreflight(f.ctx, a)
			if errors[i] == nil {
				errors[i] = s.CommitNomadSandboxMigrationCPUPreflight(f.ctx, a, *request, result)
			}
		})
	}
	wg.Wait()
	for _, err := range errors {
		require.NoError(t, err)
	}
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT cpu_preflight_requested_at FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, a.OperationID).Scan(&after))
	require.Equal(t, before, after)
	changed := result
	changed.Launch.ExecutableDigest = digest.FromString("other-runtime").String()
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, a, *request, changed), ErrNomadSandboxMigrationConflict)
	for _, sql := range []string{
		`UPDATE manager.sandbox_runtime_migrations SET cpu_preflight_requested_at=clock_timestamp() WHERE operation_id=$1`,
		`UPDATE manager.sandbox_runtime_migrations SET cpu_preflight_source=NULL WHERE operation_id=$1`,
		`UPDATE manager.sandbox_runtime_migrations SET cpu_preflight_request='{}' WHERE operation_id=$1`,
	} {
		_, err := f.pool.Exec(f.ctx, sql, a.OperationID)
		require.Error(t, err, "database must retain original CPU custody")
	}
}

// Model elapsed time without sleeping for two minutes. This deliberately
// bypasses only the immutability guard inside this isolated integration DB.
func ageMigrationCPUPreflight(t *testing.T, f *nomadPauseStoreFixture, operation string) {
	t.Helper()
	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.sandbox_runtime_migrations DISABLE TRIGGER runtime_migration_cpu_preflight_guard`)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET cpu_preflight_requested_at=clock_timestamp()-INTERVAL '3 minutes' WHERE operation_id=$1`, operation)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.sandbox_runtime_migrations ENABLE TRIGGER runtime_migration_cpu_preflight_guard`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(f.ctx))
}

func TestNomadMigrationCPUPreflightExpiryCannotAuthorizeNewCaptureIntegration(t *testing.T) {
	for _, phase := range []string{"preflight", "prepared", "captured"} {
		t.Run(phase, func(t *testing.T) {
			f, a := migrationStoreFixture(t, "cpu-expiry-"+phase)
			migrationReadyTarget(t, f, "cpu-expiry-"+phase, "b")
			_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
			require.NoError(t, err)
			retainMigrationEligibilityFixture(t, f, a)
			policy := migrationSourcePolicy(f.sandboxID, a.Target.TeamID)
			if phase == "preflight" {
				ageMigrationCPUPreflight(t, f, a.OperationID)
				_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
				require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
				_, err = f.store.AuthorizeNomadSandboxMigrationCPUPreflight(f.ctx, a)
				require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
				_, err = f.store.AuthorizeNomadSandboxMigrationTargetCPUPreflight(f.ctx, a)
				require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
				require.NoError(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, a.OperationID, "CPU evidence expired before preparation"))
				return
			}
			prepare, err := f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
			require.NoError(t, err)
			if phase == "captured" {
				_, err = f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, preparedMigrationResponse(t, *prepare))
				require.NoError(t, err)
			}
			ageMigrationCPUPreflight(t, f, a.OperationID)
			_, err = NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, preparedMigrationResponse(t, *prepare))
			if phase == "captured" {
				require.NoError(t, err, "committed capture still needs exact recovery")
			} else {
				require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
				recovered, err := f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
				require.NoError(t, err)
				require.Equal(t, prepare, recovered)
			}
		})
	}
}

func TestNomadMigrationPublicationUsesCapturedCPUProfileAfterExpiryIntegration(t *testing.T) {
	f, _, publication := migrationPublicationStoreFixture(t, "cpu-publication")
	_, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, "sha256:"+strings.Repeat("f", 64))
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "caller cannot select a digest before first publication")
	ageMigrationCPUPreflight(t, f, publication.Assignment.OperationID)
	retained, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
	require.NoError(t, err, "upload reuses the eligibility that authorized capture")
	require.NotNil(t, retained.CPULaunch)
	require.Equal(t, publication.CPULaunch, retained.CPULaunch)
	require.NoError(t, retained.CPULaunch.ValidateCapture(retained.Capture.Request, retained.CPULaunch.LaunchAttempt, retained.CPULaunch.Resources))
}

func TestNomadMigrationCPUPreflightRejectsLateReceiptAtStoreAndDatabaseIntegration(t *testing.T) {
	for _, destination := range []bool{false, true} {
		name := "late-source"
		if destination {
			name = "late-destination"
		}
		t.Run(name, func(t *testing.T) {
			f, a := migrationStoreFixture(t, name)
			migrationReadyTarget(t, f, name, "b")
			_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
			require.NoError(t, err)
			request, err := f.store.AuthorizeNomadSandboxMigrationCPUPreflight(f.ctx, a)
			require.NoError(t, err)
			if destination {
				require.NoError(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, a, *request, migrationCPUStoreResult(t, f, *request)))
				request, err = f.store.AuthorizeNomadSandboxMigrationTargetCPUPreflight(f.ctx, a)
				require.NoError(t, err)
			}
			result := migrationCPUStoreResult(t, f, *request)
			ageMigrationCPUPreflight(t, f, a.OperationID)
			require.ErrorIs(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, a, *request, result), ErrNomadSandboxMigrationConflict)
			payload, err := json.Marshal(result)
			require.NoError(t, err)
			query := `UPDATE manager.sandbox_runtime_migrations SET cpu_preflight_source=$2 WHERE operation_id=$1`
			if destination {
				query = `UPDATE manager.sandbox_runtime_migrations SET cpu_preflight_destination=$2 WHERE operation_id=$1`
			}
			_, err = f.pool.Exec(f.ctx, query, a.OperationID, payload)
			require.Error(t, err, "database must also reject a response arriving after its dispatch deadline")
		})
	}
}

func TestNomadMigrationCPUPreflightCannotFollowReplacedRuntimeIntegration(t *testing.T) {
	for _, target := range []bool{false, true} {
		name := "changed-launch"
		if target {
			name = "changed-target-endpoint"
		}
		t.Run(name, func(t *testing.T) {
			f, a := migrationStoreFixture(t, name)
			destination := migrationReadyTarget(t, f, name, "b")
			_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
			require.NoError(t, err)
			retainMigrationEligibilityFixture(t, f, a)
			if target {
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET control_endpoint='unix:///run/other.sock' WHERE slot_id=$1`, destination.SlotID)
			} else {
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET launch_attempt='replacement-launch' WHERE slot_id=$1`, f.slotID)
			}
			require.NoError(t, err)
			_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, migrationSourcePolicy(f.sandboxID, a.Target.TeamID))
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			var untouched bool
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT preparation_request IS NULL AND capture_request IS NULL FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, a.OperationID).Scan(&untouched))
			require.True(t, untouched)
		})
	}
}

func TestNomadMigrationCPUPreflightUsesTargetReadAfterCarrierLockIntegration(t *testing.T) {
	f, a := migrationStoreFixture(t, "cpu-lock-order")
	destination := migrationReadyTarget(t, f, "cpu-lock-order", "b")
	reservation, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
	require.NoError(t, err)
	retainMigrationEligibilityFixture(t, f, a)
	// Model a target update after the reservation projection was read but before
	// acquiring its carrier lock. Reusing that projection would accept old proof.
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET control_endpoint='unix:///run/after-projection.sock' WHERE slot_id=$1`, destination.SlotID)
	require.NoError(t, err)
	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer tx.Rollback(f.ctx)
	require.NoError(t, validateNomadMigrationSourceAndTarget(f.ctx, tx, reservation, a))
	require.Equal(t, "unix:///run/after-projection.sock", reservation.TargetSlot.ControlEndpoint)
	require.ErrorIs(t, requireNomadMigrationCPUPreflight(f.ctx, tx, reservation), ErrNomadSandboxMigrationConflict)
}
