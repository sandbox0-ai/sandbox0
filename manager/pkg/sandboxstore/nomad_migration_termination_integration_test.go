package sandboxstore

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/stretchr/testify/require"
)

func TestNomadMigrationUnpreparedTerminationReleasesReservationIntegration(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "atomic"
		if legacy {
			name = "repair"
		}
		t.Run(name, func(t *testing.T) {
			f, assignment := migrationStoreFixture(t, "terminate-"+name)
			target := migrationReadyTarget(t, f, "terminate-"+name, "b")
			reservation, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
			require.NoError(t, err)
			var originalAbort time.Time
			if legacy {
				// Reproduce an older cleanup transaction which changed only the
				// lifecycle, leaving an unattached destination capacity lease.
				require.NoError(t, f.pool.QueryRow(f.ctx, `UPDATE manager.sandbox_lifecycle_txns
					SET phase='aborted',error='sandbox termination requested',aborted_at=clock_timestamp()
					WHERE txn_id=$1 RETURNING aborted_at`, assignment.OperationID).Scan(&originalAbort))
				count, err := NewPGSandboxStore(f.pool).RecoverExpiredNomadMigrationReservations(f.ctx, 10)
				require.NoError(t, err)
				require.Equal(t, 1, count)
			} else {
				_, err := f.store.RequestSandboxRuntimeClaimCleanup(f.ctx, f.sandboxID, "acceptance deletion")
				require.NoError(t, err)
			}
			var state, reason string
			var aborted time.Time
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT lease_state FROM manager.runtime_resource_leases WHERE lease_id=$1`, reservation.TargetResourceLease.LeaseID).Scan(&state))
			require.Equal(t, RuntimeResourceLeaseReleased, state)
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT error,aborted_at FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, assignment.OperationID).Scan(&reason, &aborted))
			require.Equal(t, "sandbox termination requested", reason)
			if legacy {
				require.Equal(t, originalAbort, aborted)
			}
			var retired bool
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT carrier_retired FROM manager.runtime_slots WHERE slot_id=$1`, target.SlotID).Scan(&retired))
			require.True(t, retired)
			count, err := NewPGSandboxStore(f.pool).RecoverExpiredNomadMigrationReservations(f.ctx, 10)
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
}

func TestNomadMigrationPreparedTerminationPreservesRecoveryAuthorityIntegration(t *testing.T) {
	for _, captured := range []bool{false, true} {
		name := "prepared"
		if captured {
			name = "captured"
		}
		t.Run(name, func(t *testing.T) {
			f, assignment, preparation := migrationPreparedForCancellation(t, "terminate-"+name)
			if captured {
				_, err := f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *preparation, preparedMigrationResponse(t, *preparation))
				require.NoError(t, err)
			}
			before, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
			require.NoError(t, err)
			source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			for range 2 {
				candidate, err := f.store.RequestSandboxRuntimeClaimCleanup(f.ctx, f.sandboxID, "delete during migration")
				require.NoError(t, err)
				require.Nil(t, candidate, "migration recovery must retain both physical incarnations")
				candidate, err = f.store.FenceSandboxRuntimeClaimForCleanup(f.ctx, f.sandboxID, source.ClaimOperationID, "cleanup retry")
				require.NoError(t, err)
				require.Nil(t, candidate)
			}
			after, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Equal(t, before, after, "termination intent must not erase migration recovery phase")
			record, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Equal(t, SandboxDesiredStateTerminating, record.DesiredState)
			unchanged, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			unchanged.AuthorityObservedAt = source.AuthorityObservedAt
			require.Equal(t, source, unchanged)
			var leases int
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
			require.Equal(t, 2, leases)
			if captured {
				_, err = f.store.AuthorizeNomadSandboxMigrationPreparationCancellation(f.ctx, assignment.OperationID)
				require.Error(t, err, "capture authority requires physical fencing, not preparation cancellation")
				return
			}
			command, err := f.store.AuthorizeNomadSandboxMigrationPreparationCancellation(f.ctx, assignment.OperationID)
			require.NoError(t, err)
			require.NotNil(t, command, "committed deletion intent must make preparation cancellation due")
			digest, err := command.Request.Digest()
			require.NoError(t, err)
			require.NoError(t, f.store.CommitNomadSandboxMigrationPreparationCancellation(f.ctx, *command,
				procdapi.RuntimeMigrationResponse{InstanceID: preparation.InstanceID, RequestDigest: digest,
					RuntimeGeneration: assignment.SourceGeneration, State: "ready"}))
			candidate, err := f.store.FenceSandboxRuntimeClaimForCleanup(f.ctx, f.sandboxID, source.ClaimOperationID, "after cancellation")
			require.NoError(t, err)
			require.NotNil(t, candidate, "ordinary source cleanup may proceed after exact cancellation")
		})
	}
}

func TestNomadMigrationAbortedPreparationCannotReleaseReservationIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "aborted-prepared")
	migrationReadyTarget(t, f, "aborted-prepared", "b")
	reservation, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	retainMigrationEligibilityFixture(t, f, assignment)
	_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, assignment, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID))
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='aborted',aborted_at=NOW() WHERE txn_id=$1`, assignment.OperationID)
	require.NoError(t, err)
	count, err := NewPGSandboxStore(f.pool).RecoverExpiredNomadMigrationReservations(f.ctx, 10)
	require.NoError(t, err)
	require.Zero(t, count)
	require.ErrorIs(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, assignment.OperationID, "cannot erase dispatch"), ErrNomadSandboxMigrationConflict)
	var state string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT lease_state FROM manager.runtime_resource_leases WHERE lease_id=$1`, reservation.TargetResourceLease.LeaseID).Scan(&state))
	require.Equal(t, RuntimeResourceLeaseActive, state)
}
