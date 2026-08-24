package sandboxstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func setStoredRootFSWriterBindingVersion(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	grantID string,
	version int,
) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants
		SET binding_version = $2
		WHERE grant_id = $1
	`, grantID, version)
	require.NoError(t, err)
}

func TestRootFSWriterCrashAbandonPreservesDurableGenerationAndAbortsLifecycleAtomically(t *testing.T) {
	fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, true)
	ctx := context.Background()
	rollback := errors.New("force crash-abandon rollback")

	begun, err := fixture.store.BeginRootFSWriterCrashAbandon(ctx, fixture.beginRequest)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetiring, begun.State)
	require.Equal(t, RootFSWriterRetireKindCrashAbandon, begun.RetireKind)
	beginStartedAt := begun.RetireStartedAt
	begunRetry, err := fixture.store.BeginRootFSWriterCrashAbandon(ctx, fixture.beginRequest)
	require.NoError(t, err)
	require.Equal(t, beginStartedAt, begunRetry.RetireStartedAt)

	err = fixture.store.WithSandboxLock(ctx, fixture.sandboxID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		writerTx, ok := tx.(RootFSWriterCrashAbandonTx)
		require.True(t, ok)
		_, abandonErr := writerTx.CompleteRootFSWriterCrashAbandon(lockCtx, fixture.request)
		if abandonErr != nil {
			return abandonErr
		}
		return rollback
	})
	require.ErrorIs(t, err, rollback)
	fixture.assertBeforeComplete(t, RootFSWriterGrantStateRetiring)

	var abandoned *RootFSWriterGrant
	err = fixture.store.WithSandboxLock(ctx, fixture.sandboxID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		writerTx, ok := tx.(RootFSWriterCrashAbandonTx)
		require.True(t, ok)
		var abandonErr error
		abandoned, abandonErr = writerTx.CompleteRootFSWriterCrashAbandon(lockCtx, fixture.request)
		return abandonErr
	})
	require.NoError(t, err)
	require.NotNil(t, abandoned)
	require.Equal(t, RootFSWriterGrantStateRetired, abandoned.State)
	require.Equal(t, RootFSWriterRetireKindCrashAbandon, abandoned.RetireKind)
	require.Equal(t, fixture.request.OperationID, abandoned.RetireOperationID)
	require.Equal(t, fixture.request.ProofDigest, abandoned.RetireProofDigest)
	require.True(t, abandoned.LeaseExpiresAt.IsZero())
	firstRetiredAt := abandoned.RetiredAt

	filesystem, err := fixture.store.GetRootFSFilesystem(ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, fixture.initial.ID, filesystem.HeadGenerationID)
	require.Equal(t, fixture.issued.Grant.WriterEpoch, filesystem.WriterEpoch)
	var generationCount int
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.rootfs_generations WHERE filesystem_id = $1
	`, fixture.filesystem.ID).Scan(&generationCount))
	require.Equal(t, 1, generationCount)
	record, err := fixture.store.GetSandbox(ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, record.DesiredState)
	require.Empty(t, record.RuntimeNamespace)
	require.Empty(t, record.RuntimeID)
	require.Equal(t, fixture.runtimeGeneration, record.RuntimeGeneration)
	var lifecyclePhase, lifecycleError, preparedHead string
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT phase, error, prepared_generation_id
		FROM manager.sandbox_lifecycle_txns
		WHERE txn_id = $1
	`, fixture.request.LifecycleTxnID).Scan(&lifecyclePhase, &lifecycleError, &preparedHead))
	require.Equal(t, SandboxLifecyclePhaseAborted, lifecyclePhase)
	require.Equal(t, RootFSWriterCrashAbandonReason, lifecycleError)
	require.Empty(t, preparedHead)

	var retried *RootFSWriterGrant
	err = fixture.store.WithSandboxLock(ctx, fixture.sandboxID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		writerTx := tx.(RootFSWriterCrashAbandonTx)
		var retryErr error
		retried, retryErr = writerTx.CompleteRootFSWriterCrashAbandon(lockCtx, fixture.request)
		return retryErr
	})
	require.NoError(t, err)
	require.Equal(t, firstRetiredAt, retried.RetiredAt)

	differentProof := sha256.Sum256([]byte("different-terminal-proof"))
	different := *fixture.request
	different.ProofDigest = differentProof[:]
	err = fixture.store.WithSandboxLock(ctx, fixture.sandboxID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		_, retryErr := tx.(RootFSWriterCrashAbandonTx).CompleteRootFSWriterCrashAbandon(lockCtx, &different)
		return retryErr
	})
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)

	nextBinding := sha256.Sum256([]byte("next-writer-binding"))
	nextIssue := rootFSWriterGrantTestIssueRequest(
		fixture.sandboxID, "grant-after-crash", "claim-after-crash", "slot-after-crash", nextBinding[:],
	)
	nextIssue.ExpectedFilesystemID = fixture.filesystem.ID
	nextIssue.InitialGenerationID = fixture.initial.ID
	nextIssue.ExpectedWriterEpoch = fixture.issued.Grant.WriterEpoch
	next, err := fixture.store.IssueRootFSWriterGrant(ctx, nextIssue)
	require.NoError(t, err)
	require.Equal(t, fixture.issued.Grant.WriterEpoch+1, next.Grant.WriterEpoch)
}

func TestRootFSWriterCrashAbandonCompletesWithRecoveryLifecycleAfterBeginOwnerAborted(t *testing.T) {
	fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, true)
	ctx := context.Background()
	_, err := fixture.store.BeginRootFSWriterCrashAbandon(ctx, fixture.beginRequest)
	require.NoError(t, err)

	originalLifecycleID := fixture.request.LifecycleTxnID
	recoveryLifecycleID := originalLifecycleID + "-lost-recovery"
	err = fixture.store.WithSandboxLock(ctx, fixture.sandboxID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		if abortErr := tx.AbortLifecycleTxn(lockCtx, originalLifecycleID,
			"runtime deletion requested during crash recovery"); abortErr != nil {
			return abortErr
		}
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: recoveryLifecycleID, SandboxID: fixture.sandboxID,
			Kind: SandboxLifecycleKindPause, Phase: SandboxLifecyclePhasePublishing,
			Source: SandboxLifecycleSourceLost, FromGeneration: fixture.runtimeGeneration,
			FromRuntimeNamespace: "sandbox0", FromRuntimeID: "sandbox-crash-abandon-pod",
			ExpectedGenerationID: fixture.initial.ID,
		})
	})
	require.NoError(t, err)
	fixture.request.LifecycleTxnID = recoveryLifecycleID

	require.NoError(t, fixture.complete(t))
	grant, err := fixture.store.GetRootFSWriterGrant(ctx, fixture.issued.Grant.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, grant.State)
	require.Equal(t, originalLifecycleID, grant.RetireOperationID)
	var phase, lifecycleError string
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT phase, error FROM manager.sandbox_lifecycle_txns WHERE txn_id = $1
	`, recoveryLifecycleID).Scan(&phase, &lifecycleError))
	require.Equal(t, SandboxLifecyclePhaseAborted, phase)
	require.Equal(t, RootFSWriterCrashAbandonReason, lifecycleError)
}

func TestRootFSWriterCrashAbandonCompletesAcceptedPrecommitResumePod(t *testing.T) {
	fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceLost, true)
	ctx := context.Background()
	previousRuntimeGeneration := fixture.runtimeGeneration - 1
	_, err := fixture.pool.Exec(ctx, `
		UPDATE manager.sandboxes
		SET desired_state = $2,
			runtime_namespace = '',
			runtime_id = '',
			runtime_generation = $3
		WHERE sandbox_id = $1
	`, fixture.sandboxID, SandboxDesiredStatePaused, previousRuntimeGeneration)
	require.NoError(t, err)
	_, err = fixture.store.BeginRootFSWriterCrashAbandon(ctx, fixture.beginRequest)
	require.NoError(t, err)
	require.NoError(t, fixture.complete(t))

	record, err := fixture.store.GetSandbox(ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, record.DesiredState)
	require.Equal(t, previousRuntimeGeneration, record.RuntimeGeneration)
	require.Empty(t, record.RuntimeNamespace)
	require.Empty(t, record.RuntimeID)
	grant, err := fixture.store.GetRootFSWriterGrant(ctx, fixture.issued.Grant.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, grant.State)
	require.Equal(t, RootFSWriterRetireKindCrashAbandon, grant.RetireKind)
}

func TestRootFSWriterCrashAbandonRevokesRenewableTerminatingRuntime(t *testing.T) {
	fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, false)
	ctx := context.Background()
	_, err := fixture.pool.Exec(ctx, `
		UPDATE manager.sandboxes
		SET desired_state = $2
		WHERE sandbox_id = $1
	`, fixture.sandboxID, SandboxDesiredStateTerminating)
	require.NoError(t, err)

	begun, err := fixture.store.BeginRootFSWriterCrashAbandon(ctx, fixture.beginRequest)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetiring, begun.State)
	require.Equal(t, RootFSWriterRetireKindCrashAbandon, begun.RetireKind)
	require.NoError(t, fixture.complete(t))

	grant, err := fixture.store.GetRootFSWriterGrant(ctx, fixture.issued.Grant.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, grant.State)
	record, err := fixture.store.GetSandbox(ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateTerminating, record.DesiredState)
	require.Equal(t, "sandbox0", record.RuntimeNamespace)
	require.Equal(t, "sandbox-crash-abandon-pod", record.RuntimeID)
}

func TestRootFSWriterCrashAbandonCompletesAcceptedFailedInitialClaim(t *testing.T) {
	fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceLost, true)
	ctx := context.Background()

	require.NoError(t, fixture.store.MarkSandboxDeleted(ctx, fixture.sandboxID, time.Now()))
	recoveryTxnID := "failed-initial-claim-recovery"
	require.NoError(t, fixture.store.WithSandboxLock(ctx, fixture.sandboxID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: recoveryTxnID, SandboxID: fixture.sandboxID, Kind: SandboxLifecycleKindPause,
			Phase: SandboxLifecyclePhasePublishing, Source: SandboxLifecycleSourceLost,
			Cancelable: false, FromGeneration: fixture.runtimeGeneration,
			FromRuntimeNamespace: "sandbox0", FromRuntimeID: "sandbox-crash-abandon-pod",
			ExpectedGenerationID: fixture.initial.ID,
		})
	}))
	fixture.beginRequest.OperationID = recoveryTxnID
	fixture.request.LifecycleTxnID = recoveryTxnID
	fixture.request.OperationID = recoveryTxnID

	_, err := fixture.store.BeginRootFSWriterCrashAbandon(ctx, fixture.beginRequest)
	require.NoError(t, err)
	require.NoError(t, fixture.complete(t))

	grant, err := fixture.store.GetRootFSWriterGrant(ctx, fixture.issued.Grant.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, grant.State)
	require.Equal(t, RootFSWriterRetireKindCrashAbandon, grant.RetireKind)
	record, err := fixture.store.GetSandbox(ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateDeleted, record.DesiredState)
	require.True(t, record.DeletedAt.IsZero() == false)
	require.Empty(t, record.RuntimeNamespace)
	require.Empty(t, record.RuntimeID)
	filesystem, err := fixture.store.GetRootFSFilesystem(ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, fixture.initial.ID, filesystem.HeadGenerationID)
	require.NoError(t, fixture.store.MarkSandboxDeleted(ctx, fixture.sandboxID, record.DeletedAt))
	filesystem, err = fixture.store.GetRootFSFilesystem(ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Nil(t, filesystem)
	_, err = fixture.store.GetRootFSWriterGrant(ctx, fixture.issued.Grant.ID)
	require.ErrorIs(t, err, ErrRootFSWriterGrantNotFound)
}

func TestRootFSWriterCrashAbandonRejectsMissingBindingForLiveRuntime(t *testing.T) {
	fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, true)
	ctx := context.Background()
	_, err := fixture.pool.Exec(ctx, `
		DELETE FROM manager.sandbox_rootfs_bindings WHERE sandbox_id = $1
	`, fixture.sandboxID)
	require.NoError(t, err)

	_, err = fixture.store.BeginRootFSWriterCrashAbandon(ctx, fixture.beginRequest)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	grant, getErr := fixture.store.GetRootFSWriterGrant(ctx, fixture.issued.Grant.ID)
	require.NoError(t, getErr)
	require.Equal(t, RootFSWriterGrantStateConsumed, grant.State)
}

func TestRootFSWriterCrashAbandonRequiresMatureLeaseAndCrashLifecycle(t *testing.T) {
	t.Run("renewable lease", func(t *testing.T) {
		fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, false)
		_, err := fixture.store.BeginRootFSWriterCrashAbandon(context.Background(), fixture.beginRequest)
		require.ErrorIs(t, err, ErrRootFSWriterFenceNotMature)
		fixture.assertBeforeComplete(t, RootFSWriterGrantStateConsumed)
	})

	t.Run("manual planned pause crashed while publishing", func(t *testing.T) {
		fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceManual, true)
		_, err := fixture.store.BeginRootFSWriterCrashAbandon(context.Background(), fixture.beginRequest)
		require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
		manualTxnID := fixture.request.LifecycleTxnID
		crashTxnID := manualTxnID + "-recovery"
		err = fixture.store.WithSandboxLock(context.Background(), fixture.sandboxID, func(
			lockCtx context.Context,
			tx SandboxStoreTx,
			_ *SandboxRecord,
		) error {
			if abortErr := tx.AbortLifecycleTxn(lockCtx, manualTxnID, "runtime crashed during planned pause"); abortErr != nil {
				return abortErr
			}
			return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
				ID: crashTxnID, SandboxID: fixture.sandboxID, Kind: SandboxLifecycleKindPause,
				Phase: SandboxLifecyclePhasePublishing, Source: SandboxLifecycleSourceCrash,
				FromGeneration: fixture.runtimeGeneration, FromRuntimeNamespace: "sandbox0",
				FromRuntimeID: "sandbox-crash-abandon-pod", ExpectedGenerationID: fixture.initial.ID,
			})
		})
		require.NoError(t, err)
		fixture.beginRequest.OperationID = crashTxnID
		fixture.request.LifecycleTxnID = crashTxnID
		fixture.request.OperationID = crashTxnID
		_, err = fixture.store.BeginRootFSWriterCrashAbandon(context.Background(), fixture.beginRequest)
		require.NoError(t, err)
		err = fixture.complete(t)
		require.NoError(t, err)
		grant, getErr := fixture.store.GetRootFSWriterGrant(context.Background(), fixture.issued.Grant.ID)
		require.NoError(t, getErr)
		require.Equal(t, RootFSWriterGrantStateRetired, grant.State)
		require.Equal(t, RootFSWriterRetireKindCrashAbandon, grant.RetireKind)
		var phase, lifecycleError string
		require.NoError(t, fixture.pool.QueryRow(context.Background(), `
			SELECT phase, error FROM manager.sandbox_lifecycle_txns WHERE txn_id = $1
		`, crashTxnID).Scan(&phase, &lifecycleError))
		require.Equal(t, SandboxLifecyclePhaseAborted, phase)
		require.Equal(t, RootFSWriterCrashAbandonReason, lifecycleError)
		require.NoError(t, fixture.pool.QueryRow(context.Background(), `
			SELECT phase, error FROM manager.sandbox_lifecycle_txns WHERE txn_id = $1
		`, manualTxnID).Scan(&phase, &lifecycleError))
		require.Equal(t, SandboxLifecyclePhaseAborted, phase)
		require.Equal(t, "runtime crashed during planned pause", lifecycleError)
	})

	t.Run("wrong node boot", func(t *testing.T) {
		fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, true)
		fixture.beginRequest.NodeBootID = "another-boot"
		_, err := fixture.store.BeginRootFSWriterCrashAbandon(context.Background(), fixture.beginRequest)
		require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
		fixture.assertBeforeComplete(t, RootFSWriterGrantStateConsumed)
	})

	t.Run("prepared planned output", func(t *testing.T) {
		fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, true)
		_, err := fixture.store.BeginRootFSWriterCrashAbandon(context.Background(), fixture.beginRequest)
		require.NoError(t, err)
		_, err = fixture.pool.Exec(context.Background(), `
			UPDATE manager.sandbox_lifecycle_txns
			SET prepared_generation_id = 'unpublished-dirty-generation'
			WHERE txn_id = $1
		`, fixture.request.LifecycleTxnID)
		require.NoError(t, err)
		err = fixture.complete(t)
		require.ErrorIs(t, err, ErrRootFSWriterGrantInvalidState)
		grant, getErr := fixture.store.GetRootFSWriterGrant(context.Background(), fixture.issued.Grant.ID)
		require.NoError(t, getErr)
		require.Equal(t, RootFSWriterGrantStateRetiring, grant.State)
	})

	t.Run("existing planned retire owner", func(t *testing.T) {
		fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, false)
		planned, err := fixture.store.BeginRootFSWriterRetire(context.Background(), &BeginRootFSWriterRetireRequest{
			GrantID: fixture.issued.Grant.ID, WriterEpoch: fixture.issued.Grant.WriterEpoch,
			OperationID: "planned-retire-owner", BindingVersion: RootFSWriterBindingVersion,
			BindingDigest:           fixture.beginRequest.BindingDigest,
			ExpectedOldGenerationID: fixture.initial.ID,
		})
		require.NoError(t, err)
		require.Equal(t, RootFSWriterGrantStateRetiring, planned.State)
		require.Equal(t, RootFSWriterRetireKindPlannedPublish, planned.RetireKind)
		_, err = fixture.store.BeginRootFSWriterCrashAbandon(context.Background(), fixture.beginRequest)
		require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
		grant, getErr := fixture.store.GetRootFSWriterGrant(context.Background(), fixture.issued.Grant.ID)
		require.NoError(t, getErr)
		require.Equal(t, RootFSWriterRetireKindPlannedPublish, grant.RetireKind)
		require.Equal(t, "planned-retire-owner", grant.RetireOperationID)
	})
}

func TestRootFSWriterCrashAbandonRejectsNonTerminalRecoveryLifecycle(t *testing.T) {
	tests := []struct {
		name      string
		mutateSQL string
		wantError error
	}{
		{
			name:      "preparing",
			mutateSQL: `UPDATE manager.sandbox_lifecycle_txns SET phase = 'preparing' WHERE txn_id = $1`,
			wantError: ErrRootFSWriterGrantInvalidState,
		},
		{
			name:      "barriered",
			mutateSQL: `UPDATE manager.sandbox_lifecycle_txns SET phase = 'barriered' WHERE txn_id = $1`,
			wantError: ErrRootFSWriterGrantInvalidState,
		},
		{
			name:      "canceled",
			mutateSQL: `UPDATE manager.sandbox_lifecycle_txns SET cancel_requested_at = NOW() WHERE txn_id = $1`,
			wantError: ErrRootFSWriterGrantInvalidState,
		},
		{
			name:      "other kind",
			mutateSQL: `UPDATE manager.sandbox_lifecycle_txns SET kind = 'resume' WHERE txn_id = $1`,
			wantError: ErrRootFSWriterGrantConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, true)
			_, err := fixture.store.BeginRootFSWriterCrashAbandon(context.Background(), fixture.beginRequest)
			require.NoError(t, err)
			_, err = fixture.pool.Exec(context.Background(), test.mutateSQL, fixture.request.LifecycleTxnID)
			require.NoError(t, err)
			err = fixture.complete(t)
			require.ErrorIs(t, err, test.wantError)
			grant, getErr := fixture.store.GetRootFSWriterGrant(context.Background(), fixture.issued.Grant.ID)
			require.NoError(t, getErr)
			require.Equal(t, RootFSWriterGrantStateRetiring, grant.State)
		})
	}
}

func TestRootFSWriterCrashAbandonBeginWinsAgainstExpiredRenewal(t *testing.T) {
	fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, true)
	ctx := context.Background()
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, err := fixture.store.BeginRootFSWriterCrashAbandon(ctx, fixture.beginRequest)
		results <- err
	}()
	go func() {
		<-start
		_, err := fixture.store.RenewRootFSWriterGrant(ctx, &RenewRootFSWriterGrantRequest{
			GrantID: fixture.issued.Grant.ID, WriterEpoch: fixture.issued.Grant.WriterEpoch,
			BindingVersion: RootFSWriterBindingVersion, BindingDigest: fixture.beginRequest.BindingDigest,
			ConsumerNodeUID: fixture.beginRequest.NodeUID,
		}, RootFSWriterLeaseRenewalPolicy{
			LeaseTTL: time.Minute, GracePeriod: RootFSWriterMaxRenewGrace,
		})
		results <- err
	}()
	close(start)

	successes := 0
	for range 2 {
		if err := <-results; err == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)
	grant, err := fixture.store.GetRootFSWriterGrant(ctx, fixture.issued.Grant.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetiring, grant.State)
	require.Equal(t, RootFSWriterRetireKindCrashAbandon, grant.RetireKind)
}

type rootFSWriterCrashAbandonFixture struct {
	pool              *pgxpool.Pool
	store             *PGSandboxStore
	sandboxID         string
	runtimeGeneration int64
	filesystem        *RootFSFilesystem
	initial           *RootFSGeneration
	issued            *IssuedRootFSWriterGrant
	beginRequest      *BeginRootFSWriterCrashAbandonRequest
	request           *CompleteRootFSWriterCrashAbandonRequest
}

func TestRootFSWriterCrashAbandonCompletesAbandonedInitialNomadClaimIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record, filesystem, generation, registration := sandboxRuntimeClaimReadySlotFixture(t, store, "failed-writer")
	record.RuntimeGeneration = 1
	_, err := pool.Exec(ctx, `
		UPDATE manager.sandboxes SET runtime_generation = $2 WHERE sandbox_id = $1
	`, record.ID, record.RuntimeGeneration)
	require.NoError(t, err)

	acquire := sandboxRuntimeSlotAcquireRequest(record, filesystem, generation, registration, "failed-writer")
	claimed, err := store.AcquireRuntimeSlot(ctx, acquire)
	require.NoError(t, err)
	binding := sha256.Sum256([]byte("failed-initial-claim-writer"))
	issue := rootFSWriterGrantTestIssueRequest(
		record.ID, "grant-failed-writer", acquire.ClaimID, claimed.ID, binding[:],
	)
	issue.ExpectedFilesystemID = filesystem.ID
	issue.InitialGenerationID = generation.ID
	issue.NodeUID = claimed.NodeUID
	issue.NodeBootID = claimed.NodeBootID
	issue.RuntimeNamespace = claimed.AllocationNamespace
	issue.RuntimeID = "slot"
	issue.RuntimeIncarnationID = claimed.AllocationID
	issue.NodeName = claimed.NodeID
	issue.RuntimeGeneration = "1"
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	_, err = store.BindRuntimeSlotWriterGrant(ctx, &BindRuntimeSlotWriterGrantRequest{
		SlotID: claimed.ID, OperationID: acquire.OperationID,
		ClaimID: acquire.ClaimID, GrantID: issued.Grant.ID,
	})
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issued.Grant.ID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: claimed.NodeUID, ConsumerAgentUID: "ctld-failed-writer", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE manager.sandbox_runtime_claims
		SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE sandbox_id = $1
	`, record.ID)
	require.NoError(t, err)
	candidate, err := store.FenceSandboxRuntimeClaimForCleanup(
		ctx, record.ID, acquire.OperationID, "claim lease expired before commit",
	)
	require.NoError(t, err)
	require.Equal(t, claimed.ID, candidate.SlotID)
	require.Equal(t, RuntimeSlotStateQuiescing, candidate.SlotState)

	lifecycleID := "reconcile-failed-initial-claim-writer"
	err = store.WithSandboxLock(ctx, record.ID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		locked *SandboxRecord,
	) error {
		require.Empty(t, locked.RuntimeNamespace)
		require.Empty(t, locked.RuntimeID)
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: lifecycleID, SandboxID: record.ID, Kind: SandboxLifecycleKindPause,
			Phase: SandboxLifecyclePhasePublishing, Source: SandboxLifecycleSourceCrash, Cancelable: false,
			FromGeneration: record.RuntimeGeneration, FromRuntimeNamespace: claimed.AllocationNamespace,
			FromRuntimeID: claimed.AllocationID, ExpectedGenerationID: generation.ID,
		})
	})
	require.NoError(t, err)
	begin := &BeginRootFSWriterCrashAbandonRequest{
		GrantID: issued.Grant.ID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: lifecycleID, BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: binding[:], NodeUID: claimed.NodeUID, NodeBootID: claimed.NodeBootID,
		ExpectedOldGenerationID: generation.ID,
	}
	_, err = store.BeginRootFSWriterCrashAbandon(ctx, begin)
	require.NoError(t, err)
	proof := sha256.Sum256([]byte("failed-initial-claim-node-cleanup-proof"))
	err = store.WithSandboxLock(ctx, record.ID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		_, completeErr := tx.(RootFSWriterCrashAbandonTx).CompleteRootFSWriterCrashAbandon(
			lockCtx, &CompleteRootFSWriterCrashAbandonRequest{
				LifecycleTxnID: lifecycleID, GrantID: issued.Grant.ID,
				WriterEpoch: issued.Grant.WriterEpoch, OperationID: lifecycleID,
				BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
				ProofVersion: RootFSWriterCrashAbandonProofVersion, ProofDigest: proof[:],
				NodeUID: claimed.NodeUID, NodeBootID: claimed.NodeBootID,
				ExpectedOldGenerationID: generation.ID,
			},
		)
		return completeErr
	})
	require.NoError(t, err)
	grant, err := store.GetRootFSWriterGrant(ctx, issued.Grant.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, grant.State)
	stored, err := store.GetSandbox(ctx, record.ID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateActive, stored.DesiredState)
	require.Empty(t, stored.RuntimeNamespace)
	require.Empty(t, stored.RuntimeID)
	var claimPhase, lifecyclePhase string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT phase FROM manager.sandbox_runtime_claims WHERE sandbox_id = $1
	`, record.ID).Scan(&claimPhase))
	require.Equal(t, SandboxRuntimeClaimPhaseCleanupPending, claimPhase)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT phase FROM manager.sandbox_lifecycle_txns WHERE txn_id = $1
	`, lifecycleID).Scan(&lifecyclePhase))
	require.Equal(t, SandboxLifecyclePhaseAborted, lifecyclePhase)

	terminalProof := sha256.Sum256([]byte("failed-initial-claim-terminal-proof"))
	terminal, err := store.FinalizeRuntimeSlot(ctx, &FinalizeRuntimeSlotRequest{
		SlotID: claimed.ID, OperationID: acquire.OperationID, ClaimID: acquire.ClaimID,
		Reason: "reconciled_orphan", ProofDigest: terminalProof[:],
		ResourceLeaseID:     claimed.ResourceLease.LeaseID,
		ResourceLeaseDigest: claimed.ResourceLeaseDigest, ResourceCgroupAbsent: true,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateTerminal, terminal.State)
	require.Equal(t, filesystem.ID, terminal.FilesystemID)
	require.Equal(t, generation.ID, terminal.SourceGenerationID)
	require.Equal(t, issued.Grant.ID, terminal.WriterGrantID)

	require.NoError(t, store.MarkSandboxDeleted(ctx, record.ID, time.Now().UTC()))
	terminal, err = store.GetRuntimeSlot(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateTerminal, terminal.State)
	require.Equal(t, record.ID, terminal.SandboxID)
	require.Equal(t, claimed.AllocationID, terminal.AllocationID)
	require.Equal(t, acquire.OperationID, terminal.ClaimOperationID)
	require.Equal(t, acquire.ClaimID, terminal.ClaimID)
	require.Empty(t, terminal.FilesystemID)
	require.Empty(t, terminal.SourceGenerationID)
	require.Empty(t, terminal.WriterGrantID)
	for table, identifier := range map[string]string{
		"rootfs_filesystems":   filesystem.ID,
		"rootfs_generations":   generation.ID,
		"rootfs_writer_grants": issued.Grant.ID,
	} {
		var count int
		require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT COUNT(*) FROM manager.%s WHERE %s = $1",
			table,
			map[string]string{
				"rootfs_filesystems":   "filesystem_id",
				"rootfs_generations":   "generation_id",
				"rootfs_writer_grants": "grant_id",
			}[table],
		), identifier).Scan(&count))
		require.Zero(t, count, "%s retained deleted sandbox storage", table)
	}
	terminalWriter, err := store.GetRootFSWriterTerminalProof(ctx, issued.Grant.ID)
	require.NoError(t, err)
	require.Equal(t, issued.Grant.ID, terminalWriter.GrantID)
	require.Equal(t, record.ID, terminalWriter.SandboxID)
	require.Equal(t, issued.Grant.WriterEpoch, terminalWriter.WriterEpoch)
	require.Equal(t, issued.Grant.BindingVersion, terminalWriter.BindingVersion)
	require.Equal(t, issued.Grant.BindingDigest, terminalWriter.BindingDigest)
	require.Equal(t, issued.Grant.NodeUID, terminalWriter.NodeUID)
	require.Equal(t, RootFSWriterGrantStateRetired, terminalWriter.State)
	require.WithinDuration(t, time.Now().UTC().Add(RootFSWriterTerminalProofRetention), terminalWriter.ExpiresAt, time.Minute)
	require.NoError(t, store.MarkSandboxRuntimeClaimCleaned(ctx, record.ID, acquire.OperationID))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT phase FROM manager.sandbox_runtime_claims WHERE sandbox_id = $1
	`, record.ID).Scan(&claimPhase))
	require.Equal(t, SandboxRuntimeClaimPhaseCleaned, claimPhase)
}

func newRootFSWriterCrashAbandonFixture(
	t *testing.T,
	lifecycleSource string,
	matureLease bool,
) *rootFSWriterCrashAbandonFixture {
	t.Helper()
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	sandboxID := "sandbox-crash-abandon"
	runtimeGeneration := int64(7)
	record := rootFSTestSandboxRecord(sandboxID, "team-a")
	record.RuntimeGeneration = runtimeGeneration
	record.RuntimeNamespace = "sandbox0"
	record.RuntimeID = "sandbox-crash-abandon-pod"
	require.NoError(t, store.UpsertSandbox(ctx, record))

	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	filesystem, initial, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: sandboxID, TeamID: "team-a", SourceOCIRef: artifact.SourceOCIRef,
		SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)
	binding := sha256.Sum256([]byte("crashed-writer-binding"))
	issue := rootFSWriterGrantTestIssueRequest(sandboxID, "grant-crash-abandon", "claim-crash-abandon", "slot-crash-abandon", binding[:])
	issue.ExpectedFilesystemID = filesystem.ID
	issue.InitialGenerationID = initial.ID
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-a", ConsumerAgentUID: "ctld-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	if matureLease {
		_, err = pool.Exec(ctx, `
			UPDATE manager.rootfs_writer_grants
			SET lease_expires_at = NOW() - ($2::bigint * INTERVAL '1 millisecond')
			WHERE grant_id = $1
		`, issue.GrantID, RootFSWriterCrashAbandonGrace.Milliseconds()+1000)
		require.NoError(t, err)
	}
	lifecycleTxnID := "crash-abandon-txn"
	require.NoError(t, store.WithSandboxLock(ctx, sandboxID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: lifecycleTxnID, SandboxID: sandboxID, Kind: SandboxLifecycleKindPause,
			Phase: SandboxLifecyclePhasePublishing, Source: lifecycleSource, Cancelable: false,
			FromGeneration: runtimeGeneration, FromRuntimeNamespace: record.RuntimeNamespace,
			FromRuntimeID: record.RuntimeID, ExpectedGenerationID: initial.ID,
		})
	}))
	proof := sha256.Sum256([]byte("node-task-container-mount-nbd-writer-zero-proof"))
	return &rootFSWriterCrashAbandonFixture{
		pool: pool, store: store, sandboxID: sandboxID, runtimeGeneration: runtimeGeneration,
		filesystem: filesystem, initial: initial, issued: issued,
		beginRequest: &BeginRootFSWriterCrashAbandonRequest{
			GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
			OperationID: lifecycleTxnID, BindingVersion: RootFSWriterBindingVersion,
			BindingDigest: binding[:], NodeUID: issue.NodeUID, NodeBootID: issue.NodeBootID,
			ExpectedOldGenerationID: initial.ID,
		},
		request: &CompleteRootFSWriterCrashAbandonRequest{
			LifecycleTxnID: lifecycleTxnID, GrantID: issue.GrantID,
			WriterEpoch: issued.Grant.WriterEpoch, OperationID: lifecycleTxnID,
			BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
			ProofVersion: RootFSWriterCrashAbandonProofVersion, ProofDigest: proof[:],
			NodeUID: issue.NodeUID, NodeBootID: issue.NodeBootID,
			ExpectedOldGenerationID: initial.ID,
		},
	}
}

func (f *rootFSWriterCrashAbandonFixture) complete(t *testing.T) error {
	t.Helper()
	return f.store.WithSandboxLock(context.Background(), f.sandboxID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		writerTx, ok := tx.(RootFSWriterCrashAbandonTx)
		require.True(t, ok)
		_, err := writerTx.CompleteRootFSWriterCrashAbandon(lockCtx, f.request)
		return err
	})
}

func (f *rootFSWriterCrashAbandonFixture) assertBeforeComplete(t *testing.T, wantGrantState string) {
	t.Helper()
	ctx := context.Background()
	grant, err := f.store.GetRootFSWriterGrant(ctx, f.issued.Grant.ID)
	require.NoError(t, err)
	require.Equal(t, wantGrantState, grant.State)
	record, err := f.store.GetSandbox(ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateActive, record.DesiredState)
	require.Equal(t, "sandbox0", record.RuntimeNamespace)
	require.Equal(t, "sandbox-crash-abandon-pod", record.RuntimeID)
	var phase, lifecycleError string
	require.NoError(t, f.pool.QueryRow(ctx, `
		SELECT phase, error FROM manager.sandbox_lifecycle_txns WHERE txn_id = $1
	`, f.request.LifecycleTxnID).Scan(&phase, &lifecycleError))
	require.Equal(t, SandboxLifecyclePhasePublishing, phase)
	require.Empty(t, lifecycleError)
	filesystem, err := f.store.GetRootFSFilesystem(ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, f.initial.ID, filesystem.HeadGenerationID)
}

func rootFSWriterGrantTestIssueRequest(sandboxID, grantID, claimID, slotID string, binding []byte) *IssueRootFSWriterGrantRequest {
	return &IssueRootFSWriterGrantRequest{
		GrantID:        grantID,
		SandboxID:      sandboxID,
		ClaimID:        claimID,
		SlotID:         slotID,
		OperationID:    "issue-" + grantID,
		RawToken:       "0123456789abcdef0123456789abcdef-" + grantID,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: append([]byte(nil), binding...),
		NodeUID:              "node-a",
		NodeBootID:           "boot-a",
		RuntimeNamespace:     "sandbox0",
		RuntimeID:            sandboxID + "-pod",
		RuntimeIncarnationID: sandboxID + "-pod-uid",
		NodeName:             "node-a",
		GateParent:           "gate-" + slotID,
		RuntimeGeneration:    "17",
		ConsumeExpiresAt:     time.Now().Add(time.Minute),
	}
}
