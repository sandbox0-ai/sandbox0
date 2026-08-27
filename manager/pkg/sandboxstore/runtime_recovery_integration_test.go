package sandboxstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSandboxRuntimeRecoveryClaimIsExclusiveAndDurablyDeferredIntegration(t *testing.T) {
	fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, true)
	ctx := context.Background()
	_, err := fixture.store.BeginRootFSWriterCrashAbandon(ctx, fixture.beginRequest)
	require.NoError(t, err)
	require.NoError(t, fixture.complete(t))

	first, err := fixture.store.ClaimSandboxRuntimeRecovery(
		ctx, fixture.sandboxID, "manager-a", time.Minute,
	)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, fixture.request.LifecycleTxnID, first.LifecycleTxnID)
	require.Equal(t, 1, first.AttemptCount)

	contender, err := fixture.store.ClaimSandboxRuntimeRecovery(
		ctx, fixture.sandboxID, "manager-b", time.Minute,
	)
	require.NoError(t, err)
	require.Nil(t, contender)
	require.NoError(t, fixture.store.RenewSandboxRuntimeRecoveryClaim(ctx, first, time.Minute))

	pending, err := fixture.store.IsRuntimeRecoveryPending(ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.True(t, pending, "the scheduling lease must not replace recovery truth")
	pendingIDs, err := fixture.store.ListPendingRuntimeRecoverySandboxIDs(ctx, 10)
	require.NoError(t, err)
	require.NotContains(t, pendingIDs, fixture.sandboxID,
		"a claimed obligation must not be offered to another manager")

	retryDelay := 5 * time.Second
	require.NoError(t, fixture.store.FailSandboxRuntimeRecoveryClaim(
		ctx, first, retryDelay, strings.Repeat("failure-", 700),
	))
	contender, err = fixture.store.ClaimSandboxRuntimeRecovery(
		ctx, fixture.sandboxID, "manager-b", time.Minute,
	)
	require.NoError(t, err)
	require.Nil(t, contender, "the durable not-before time must gate every replica")

	var attempts int
	var claimedBy, claimToken, lastError string
	var claimedUntil *time.Time
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT recovery_attempts, recovery_claimed_by, recovery_claim_token,
			recovery_claimed_until, recovery_last_error
		FROM manager.sandbox_lifecycle_txns
		WHERE txn_id = $1
	`, first.LifecycleTxnID).Scan(
		&attempts, &claimedBy, &claimToken, &claimedUntil, &lastError,
	))
	require.Equal(t, 1, attempts)
	require.Empty(t, claimedBy)
	require.Empty(t, claimToken)
	require.Nil(t, claimedUntil)
	require.LessOrEqual(t, len(lastError), maxRuntimeRecoveryErrorBytes)
	require.True(t, strings.HasPrefix(lastError, "failure-"))

	_, err = fixture.pool.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET recovery_next_attempt_at = NOW() - INTERVAL '1 second'
		WHERE txn_id = $1
	`, first.LifecycleTxnID)
	require.NoError(t, err)
	second, err := fixture.store.ClaimSandboxRuntimeRecovery(
		ctx, fixture.sandboxID, "manager-b", time.Minute,
	)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, 2, second.AttemptCount)
	require.ErrorIs(t,
		fixture.store.CompleteSandboxRuntimeRecoveryClaim(ctx, first),
		ErrSandboxRuntimeRecoveryClaimLost,
		"an old token must not settle a later worker's claim",
	)
	require.NoError(t, fixture.store.CompleteSandboxRuntimeRecoveryClaim(ctx, second))
}

func TestSandboxRuntimeRecoveryExpiredClaimCanBeReclaimedIntegration(t *testing.T) {
	fixture := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceLost, true)
	ctx := context.Background()
	_, err := fixture.store.BeginRootFSWriterCrashAbandon(ctx, fixture.beginRequest)
	require.NoError(t, err)
	require.NoError(t, fixture.complete(t))

	first, err := fixture.store.ClaimSandboxRuntimeRecovery(
		ctx, fixture.sandboxID, "manager-a", time.Minute,
	)
	require.NoError(t, err)
	require.NotNil(t, first)
	_, err = fixture.pool.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET recovery_claimed_until = NOW() - INTERVAL '1 second'
		WHERE txn_id = $1
	`, first.LifecycleTxnID)
	require.NoError(t, err)

	second, err := fixture.store.ClaimSandboxRuntimeRecovery(
		ctx, fixture.sandboxID, "manager-b", time.Minute,
	)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, 2, second.AttemptCount)
	require.NotEqual(t, first.Token, second.Token)
	require.ErrorIs(t,
		fixture.store.FailSandboxRuntimeRecoveryClaim(ctx, first, time.Second, "stale"),
		ErrSandboxRuntimeRecoveryClaimLost,
	)
	require.NoError(t, fixture.store.CompleteSandboxRuntimeRecoveryClaim(ctx, second))
}
