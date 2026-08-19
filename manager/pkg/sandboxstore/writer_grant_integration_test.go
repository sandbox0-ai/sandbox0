package sandboxstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootFSWriterGrantIssueConsumeBeginRetireIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))

	binding := sha256.Sum256([]byte("exact-stage-binding"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)
	require.NotNil(t, issued)
	assert.Equal(t, int64(1), issued.Grant.WriterEpoch)
	assert.Equal(t, issueReq.RawToken, issued.RawToken)

	retried, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)
	assert.Equal(t, issued.Grant.ID, retried.Grant.ID)
	assert.Equal(t, issueReq.RawToken, retried.RawToken)

	var storedBindingVersion int
	var storedTokenHex string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT binding_version, encode(token_digest, 'hex')
		FROM manager.rootfs_writer_grants
		WHERE grant_id = $1
	`, issueReq.GrantID).Scan(&storedBindingVersion, &storedTokenHex))
	assert.Equal(t, RootFSWriterBindingVersion, storedBindingVersion)
	wantTokenDigest := sha256.Sum256([]byte(issueReq.RawToken))
	assert.Equal(t, hex.EncodeToString(wantTokenDigest[:]), storedTokenHex)
	assert.NotContains(t, storedTokenHex, issueReq.RawToken)

	consumeReq := &ConsumeRootFSWriterGrantRequest{
		GrantID:        issueReq.GrantID,
		WriterEpoch:    issued.Grant.WriterEpoch,
		RawToken:       issueReq.RawToken,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID:    "node-a",
		ConsumerCtldPodUID: "snapshotter-pod-a",
		LeaseTTL:           time.Minute,
	}
	consumed, err := store.ConsumeRootFSWriterGrant(ctx, consumeReq)
	require.NoError(t, err)
	assert.Equal(t, RootFSWriterGrantStateConsumed, consumed.State)
	assert.False(t, consumed.LeaseExpiresAt.IsZero())

	consumeRetry, err := store.ConsumeRootFSWriterGrant(ctx, consumeReq)
	require.NoError(t, err)
	assert.Equal(t, consumed.LeaseExpiresAt, consumeRetry.LeaseExpiresAt)

	_, err = store.CancelRootFSWriterGrant(ctx, &CancelRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: 1,
		OperationID: issueReq.OperationID, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
	})
	require.ErrorIs(t, err, ErrRootFSWriterGrantInvalidState)

	beginReq := &BeginRootFSWriterRetireRequest{
		GrantID: issueReq.GrantID, WriterEpoch: 1,
		OperationID: "retire-a", BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ExpectedOldHeadLayerID: "",
	}
	retiring, err := store.BeginRootFSWriterRetire(ctx, beginReq)
	require.NoError(t, err)
	assert.Equal(t, RootFSWriterGrantStateRetiring, retiring.State)
	retiringRetry, err := store.BeginRootFSWriterRetire(ctx, beginReq)
	require.NoError(t, err)
	assert.Equal(t, retiring.RetireStartedAt, retiringRetry.RetireStartedAt)

}

func TestRootFSWriterGrantCancelAndCAS(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)

	cancelReq := &CancelRootFSWriterGrantRequest{
		GrantID: issued.Grant.ID, WriterEpoch: 1,
		OperationID: issueReq.OperationID, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
	}
	canceled, err := store.CancelRootFSWriterGrant(ctx, cancelReq)
	require.NoError(t, err)
	assert.Equal(t, RootFSWriterGrantStateCanceled, canceled.State)
	cancelRetry, err := store.CancelRootFSWriterGrant(ctx, cancelReq)
	require.NoError(t, err)
	assert.Equal(t, canceled.CanceledAt, cancelRetry.CanceledAt)

	staleBinding := sha256.Sum256([]byte("stale-binding"))
	staleReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-stale", "claim-stale", "slot-stale", staleBinding[:])
	staleReq.ExpectedWriterEpoch = 0
	_, err = store.IssueRootFSWriterGrant(ctx, staleReq)
	require.ErrorIs(t, err, ErrRootFSWriterEpochConflict)

	nextReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-b", "claim-b", "slot-b", staleBinding[:])
	nextReq.ExpectedWriterEpoch = 1
	next, err := store.IssueRootFSWriterGrant(ctx, nextReq)
	require.NoError(t, err)
	assert.Equal(t, int64(2), next.Grant.WriterEpoch)
}

func TestRootFSWriterGrantRejectsBindingMismatchWithoutConsuming(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)

	differentBinding := sha256.Sum256([]byte("binding-b"))
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: differentBinding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "snapshotter-pod-a",
		LeaseTTL: time.Minute,
	})
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)

	stored, err := store.GetRootFSWriterGrant(ctx, issueReq.GrantID)
	require.NoError(t, err)
	assert.Equal(t, RootFSWriterGrantStateIssued, stored.State)
}

func TestRootFSWriterGrantConsumeCancelRaceHasOneWinner(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)

	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, consumeErr := store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
			GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
			RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
			ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "snapshotter-pod-a",
			LeaseTTL: time.Minute,
		})
		results <- consumeErr
	}()
	go func() {
		<-start
		_, cancelErr := store.CancelRootFSWriterGrant(ctx, &CancelRootFSWriterGrantRequest{
			GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
			OperationID: issueReq.OperationID, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		})
		results <- cancelErr
	}()
	close(start)

	successes := 0
	for i := 0; i < 2; i++ {
		racerErr := <-results
		if racerErr == nil {
			successes++
			continue
		}
		assert.ErrorIs(t, racerErr, ErrRootFSWriterGrantInvalidState)
	}
	assert.Equal(t, 1, successes)
	stored, err := store.GetRootFSWriterGrant(ctx, issueReq.GrantID)
	require.NoError(t, err)
	assert.Contains(t, []string{RootFSWriterGrantStateConsumed, RootFSWriterGrantStateCanceled}, stored.State)
}

func TestRootFSWriterGrantConsumeRetrySurvivesCtldPodRestart(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)
	first, err := store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "ctld-pod-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	retried, err := store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "ctld-pod-b", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	assert.Equal(t, first.LeaseExpiresAt, retried.LeaseExpiresAt)
	assert.Equal(t, "ctld-pod-a", retried.ConsumerCtldPodUID)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-b", ConsumerCtldPodUID: "ctld-pod-c", LeaseTTL: time.Minute,
	})
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)

	var auditPodUID string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT consumer_ctld_pod_uid
		FROM manager.rootfs_writer_grants
		WHERE grant_id = $1
	`, issueReq.GrantID).Scan(&auditPodUID))
	assert.Equal(t, "ctld-pod-a", auditPodUID)
}

func TestRenewRootFSWriterGrantUsesServerPolicyAndPreservesOwnerAudit(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)
	renewReq := &RenewRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		BindingVersion: RootFSWriterBindingVersion,
		BindingDigest:  binding[:], ConsumerNodeUID: "node-a",
	}
	policy := RootFSWriterLeaseRenewalPolicy{LeaseTTL: 2 * time.Minute}
	_, err = store.RenewRootFSWriterGrant(ctx, renewReq, policy)
	require.ErrorIs(t, err, ErrRootFSWriterGrantInvalidState)

	consumed, err := store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "ctld-pod-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	renewed, err := store.RenewRootFSWriterGrant(ctx, renewReq, policy)
	require.NoError(t, err)
	assert.True(t, renewed.LeaseExpiresAt.After(consumed.LeaseExpiresAt))
	assert.Equal(t, consumed.WriterEpoch, renewed.WriterEpoch)
	assert.Equal(t, "node-a", renewed.ConsumerNodeUID)
	assert.Equal(t, "ctld-pod-a", renewed.ConsumerCtldPodUID)

	wrongNode := *renewReq
	wrongNode.ConsumerNodeUID = "node-b"
	_, err = store.RenewRootFSWriterGrant(ctx, &wrongNode, policy)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	wrongDigest := *renewReq
	differentBinding := sha256.Sum256([]byte("different-binding"))
	wrongDigest.BindingDigest = differentBinding[:]
	_, err = store.RenewRootFSWriterGrant(ctx, &wrongDigest, policy)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	wrongVersion := *renewReq
	wrongVersion.BindingVersion = RootFSWriterBindingVersion + 1
	_, err = store.RenewRootFSWriterGrant(ctx, &wrongVersion, policy)
	require.ErrorContains(t, err, "unsupported binding_version")
	stored, err := store.GetRootFSWriterGrant(ctx, issueReq.GrantID)
	require.NoError(t, err)
	assert.Equal(t, renewed.LeaseExpiresAt, stored.LeaseExpiresAt)
	assert.Equal(t, "ctld-pod-a", stored.ConsumerCtldPodUID)

	_, err = store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: "retire-a", BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: binding[:], ExpectedOldHeadLayerID: "",
	})
	require.NoError(t, err)
	_, err = store.RenewRootFSWriterGrant(ctx, renewReq, policy)
	require.ErrorIs(t, err, ErrRootFSWriterGrantInvalidState)
}

func TestRenewRootFSWriterGrantsCommitsValidItemsWhenAnotherGrantIsStale(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	for _, sandboxID := range []string{"sandbox-a", "sandbox-b"} {
		require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord(sandboxID, "team-a")))
	}

	bindings := make([][sha256.Size]byte, 2)
	issues := make([]*IssueRootFSWriterGrantRequest, 2)
	consumed := make([]*RootFSWriterGrant, 2)
	requests := make([]*RenewRootFSWriterGrantRequest, 2)
	for index, sandboxID := range []string{"sandbox-a", "sandbox-b"} {
		bindings[index] = sha256.Sum256([]byte("batch-binding-" + sandboxID))
		issues[index] = rootFSWriterGrantTestIssueRequest(
			sandboxID, "grant-"+sandboxID, "claim-"+sandboxID, "slot-"+sandboxID, bindings[index][:],
		)
		issued, err := store.IssueRootFSWriterGrant(ctx, issues[index])
		require.NoError(t, err)
		consumed[index], err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
			GrantID: issues[index].GrantID, WriterEpoch: issued.Grant.WriterEpoch,
			RawToken: issues[index].RawToken, BindingVersion: RootFSWriterBindingVersion,
			BindingDigest: bindings[index][:], ConsumerNodeUID: "node-a",
			ConsumerCtldPodUID: "ctld-pod-a", LeaseTTL: time.Minute,
		})
		require.NoError(t, err)
		requests[index] = &RenewRootFSWriterGrantRequest{
			GrantID: issues[index].GrantID, WriterEpoch: issued.Grant.WriterEpoch,
			BindingVersion: RootFSWriterBindingVersion, BindingDigest: bindings[index][:],
			ConsumerNodeUID: "node-a",
		}
	}
	staleDigest := sha256.Sum256([]byte("stale-binding"))
	requests[1].BindingDigest = staleDigest[:]

	results, err := store.RenewRootFSWriterGrants(ctx, requests, RootFSWriterLeaseRenewalPolicy{LeaseTTL: 2 * time.Minute})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.NoError(t, results[0].Err)
	require.NotNil(t, results[0].Grant)
	assert.True(t, results[0].Grant.LeaseExpiresAt.After(consumed[0].LeaseExpiresAt))
	require.ErrorIs(t, results[1].Err, ErrRootFSWriterGrantConflict)
	require.Nil(t, results[1].Grant)

	storedStale, err := store.GetRootFSWriterGrant(ctx, issues[1].GrantID)
	require.NoError(t, err)
	assert.Equal(t, consumed[1].LeaseExpiresAt, storedStale.LeaseExpiresAt)
}

func TestRenewRootFSWriterGrantAllowsOnlyBoundedGraceWithoutFencing(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "ctld-pod-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	renewReq := &RenewRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		BindingVersion: RootFSWriterBindingVersion,
		BindingDigest:  binding[:], ConsumerNodeUID: "node-a",
	}

	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants
		SET lease_expires_at = NOW() - INTERVAL '100 milliseconds'
		WHERE grant_id = $1
	`, issueReq.GrantID)
	require.NoError(t, err)
	withinGrace, err := store.RenewRootFSWriterGrant(ctx, renewReq, RootFSWriterLeaseRenewalPolicy{
		LeaseTTL: time.Minute, GracePeriod: time.Second,
	})
	require.NoError(t, err)
	assert.Equal(t, RootFSWriterGrantStateConsumed, withinGrace.State)

	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants
		SET lease_expires_at = NOW() - INTERVAL '10 seconds'
		WHERE grant_id = $1
	`, issueReq.GrantID)
	require.NoError(t, err)
	_, err = store.RenewRootFSWriterGrant(ctx, renewReq, RootFSWriterLeaseRenewalPolicy{
		LeaseTTL: time.Minute, GracePeriod: RootFSWriterMaxRenewGrace,
	})
	require.ErrorIs(t, err, ErrRootFSWriterLeaseExpired)
	_, err = store.RenewRootFSWriterGrant(ctx, renewReq, RootFSWriterLeaseRenewalPolicy{
		LeaseTTL: time.Minute, GracePeriod: RootFSWriterMaxRenewGrace + time.Millisecond,
	})
	require.ErrorContains(t, err, "renew grace policy")
	stored, err := store.GetRootFSWriterGrant(ctx, issueReq.GrantID)
	require.NoError(t, err)
	assert.Equal(t, RootFSWriterGrantStateConsumed, stored.State)
}

func TestRootFSWriterGrantBindingVersionIsCheckedAcrossAllTransitions(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)
	setStoredRootFSWriterBindingVersion(t, ctx, pool, issueReq.GrantID, RootFSWriterBindingVersion+1)

	_, err = store.IssueRootFSWriterGrant(ctx, issueReq)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	consumeReq := &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "ctld-pod-a", LeaseTTL: time.Minute,
	}
	_, err = store.ConsumeRootFSWriterGrant(ctx, consumeReq)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)

	setStoredRootFSWriterBindingVersion(t, ctx, pool, issueReq.GrantID, RootFSWriterBindingVersion)
	_, err = store.ConsumeRootFSWriterGrant(ctx, consumeReq)
	require.NoError(t, err)
	setStoredRootFSWriterBindingVersion(t, ctx, pool, issueReq.GrantID, RootFSWriterBindingVersion+1)
	beginReq := &BeginRootFSWriterRetireRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: "retire-a", BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ExpectedOldHeadLayerID: "",
	}
	_, err = store.BeginRootFSWriterRetire(ctx, beginReq)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)

	stored, err := store.GetRootFSWriterGrant(ctx, issueReq.GrantID)
	require.NoError(t, err)
	assert.Equal(t, RootFSWriterGrantStateConsumed, stored.State)
}

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

func TestRootFSWriterGrantConcurrentIssueHasOneWinner(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))

	const contenders = 12
	start := make(chan struct{})
	results := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			binding := sha256.Sum256([]byte(fmt.Sprintf("binding-%d", index)))
			req := rootFSWriterGrantTestIssueRequest(
				"sandbox-a",
				fmt.Sprintf("grant-%d", index),
				fmt.Sprintf("claim-%d", index),
				fmt.Sprintf("slot-%d", index),
				binding[:],
			)
			_, err := store.IssueRootFSWriterGrant(ctx, req)
			results <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	successes := 0
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		assert.True(t,
			errors.Is(err, ErrRootFSWriterGrantConflict) || errors.Is(err, ErrRootFSWriterEpochConflict),
			"unexpected concurrent Issue error: %v", err,
		)
	}
	assert.Equal(t, 1, successes)
	var epoch int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT writer_epoch
		FROM manager.rootfs_filesystems
		WHERE filesystem_id = 'sandbox-a'
	`).Scan(&epoch))
	assert.Equal(t, int64(1), epoch)
}

func TestRootFSWriterGrantLiveClaimAndSlotAreUniqueAcrossFilesystems(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	for _, sandboxID := range []string{"sandbox-a", "sandbox-b"} {
		require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord(sandboxID, "team-a")))
	}

	bindingA := sha256.Sum256([]byte("binding-a"))
	first := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-shared", "slot-shared", bindingA[:])
	_, err := store.IssueRootFSWriterGrant(ctx, first)
	require.NoError(t, err)

	bindingB := sha256.Sum256([]byte("binding-b"))
	second := rootFSWriterGrantTestIssueRequest("sandbox-b", "grant-b", "claim-shared", "slot-other", bindingB[:])
	_, err = store.IssueRootFSWriterGrant(ctx, second)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)

	second.ClaimID = "claim-other"
	second.SlotID = "slot-shared"
	second.OperationID = "issue-b-slot"
	_, err = store.IssueRootFSWriterGrant(ctx, second)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
}

func TestExpiredRootFSWriterLeaseDoesNotChangeState(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "snapshotter-pod-a",
		LeaseTTL: time.Millisecond,
	})
	require.NoError(t, err)
	var expired []*RootFSWriterGrant
	require.Eventually(t, func() bool {
		var listErr error
		expired, listErr = store.ListExpiredRootFSWriterGrants(ctx, 10)
		return listErr == nil && len(expired) == 1
	}, time.Second, 5*time.Millisecond)
	assert.Equal(t, RootFSWriterGrantStateConsumed, expired[0].State)

	stored, err := store.GetRootFSWriterGrant(ctx, issueReq.GrantID)
	require.NoError(t, err)
	assert.Equal(t, RootFSWriterGrantStateConsumed, stored.State)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "snapshotter-pod-a",
		LeaseTTL: time.Minute,
	})
	require.ErrorIs(t, err, ErrRootFSWriterLeaseExpired)
	_, err = store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: "retire-a", BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ExpectedOldHeadLayerID: "",
	})
	require.ErrorIs(t, err, ErrRootFSWriterLeaseExpired)
	stored, err = store.GetRootFSWriterGrant(ctx, issueReq.GrantID)
	require.NoError(t, err)
	assert.Equal(t, RootFSWriterGrantStateConsumed, stored.State)
}

func TestLegacyRootFSHeadMutationsAreBlockedAfterWriterEpochStarts(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))
	require.NoError(t, store.SaveRootFSState(ctx,
		rootFSTestStoreState("sandbox-a", "team-a", "layer-old", "", 1, "old")))
	_, err := store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID: "sandbox-a", SnapshotID: "snapshot-old",
	})
	require.NoError(t, err)

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issueReq.InitialHeadLayerID = "layer-old"
	_, err = store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)

	legacyState := rootFSTestStoreState("sandbox-a", "team-a", "layer-legacy", "layer-old", 2, "legacy")
	err = store.SaveRootFSState(ctx, legacyState)
	require.ErrorIs(t, err, ErrRootFSHeadConflict)

	_, err = store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: "sandbox-a", SnapshotID: "snapshot-old", TeamID: "team-a",
	})
	require.ErrorIs(t, err, ErrRootFSFilesystemConflict)

	filesystem, err := store.GetRootFSFilesystem(ctx, "sandbox-a")
	require.NoError(t, err)
	require.NotNil(t, filesystem)
	assert.Equal(t, "layer-old", filesystem.HeadLayerID)
	var legacyLayerExists bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM manager.rootfs_layers WHERE layer_id = 'layer-legacy'
		)
	`).Scan(&legacyLayerExists))
	assert.False(t, legacyLayerExists)
}

func TestRootFSWriterPrelaunchAbortRetiresWithoutPublishingHead(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))
	require.NoError(t, store.SaveRootFSState(ctx,
		rootFSTestStoreState("sandbox-a", "team-a", "layer-old", "", 1, "old")))

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issueReq.InitialHeadLayerID = "layer-old"
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "ctld-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	beginReq := &BeginRootFSWriterRetireRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: "prelaunch-abort-a", BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: binding[:], ExpectedOldHeadLayerID: "layer-old",
	}
	err = store.WithSandboxLock(ctx, "sandbox-a", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		writerTx, ok := tx.(RootFSWriterGrantTx)
		require.True(t, ok)
		_, beginErr := writerTx.BeginRootFSWriterPrelaunchAbort(lockCtx, beginReq)
		return beginErr
	})
	require.NoError(t, err)
	_, err = store.BeginRootFSWriterRetire(ctx, beginReq)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)

	proof := sha256.Sum256([]byte("node-tombstone-proof"))
	completeReq := &CompleteRootFSWriterPrelaunchAbortRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: beginReq.OperationID, BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: binding[:], ProofDigest: proof[:], ExpectedOldHeadLayerID: "layer-old",
	}
	complete := func(request *CompleteRootFSWriterPrelaunchAbortRequest) (*RootFSWriterGrant, error) {
		var result *RootFSWriterGrant
		completeErr := store.WithSandboxLock(ctx, "sandbox-a", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
			writerTx, ok := tx.(RootFSWriterGrantTx)
			require.True(t, ok)
			var txErr error
			result, txErr = writerTx.CompleteRootFSWriterPrelaunchAbort(lockCtx, request)
			return txErr
		})
		return result, completeErr
	}
	retired, err := complete(completeReq)
	require.NoError(t, err)
	assert.Equal(t, RootFSWriterGrantStateRetired, retired.State)
	assert.Equal(t, RootFSWriterRetireKindPrelaunchAbort, retired.RetireKind)

	retried, err := complete(completeReq)
	require.NoError(t, err)
	assert.Equal(t, retired.RetiredAt, retried.RetiredAt)
	filesystem, err := store.GetRootFSFilesystem(ctx, "sandbox-a")
	require.NoError(t, err)
	require.NotNil(t, filesystem)
	assert.Equal(t, "layer-old", filesystem.HeadLayerID)
	assert.Equal(t, int64(1), filesystem.WriterEpoch)

	differentProof := sha256.Sum256([]byte("different-proof"))
	different := *completeReq
	different.ProofDigest = differentProof[:]
	_, err = complete(&different)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)

	nextBinding := sha256.Sum256([]byte("binding-b"))
	nextIssue := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-b", "claim-b", "slot-b", nextBinding[:])
	nextIssue.ExpectedWriterEpoch = 1
	nextIssue.InitialHeadLayerID = "layer-old"
	next, err := store.IssueRootFSWriterGrant(ctx, nextIssue)
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: nextIssue.GrantID, WriterEpoch: next.Grant.WriterEpoch,
		RawToken: nextIssue.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: nextBinding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "ctld-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{
		GrantID: nextIssue.GrantID, WriterEpoch: next.Grant.WriterEpoch,
		OperationID: "planned-b", BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: nextBinding[:], ExpectedOldHeadLayerID: "layer-old",
	})
	require.NoError(t, err)
	wrongKind := *completeReq
	wrongKind.GrantID = nextIssue.GrantID
	wrongKind.WriterEpoch = next.Grant.WriterEpoch
	wrongKind.OperationID = "planned-b"
	wrongKind.BindingDigest = nextBinding[:]
	_, err = complete(&wrongKind)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
}

func TestCompleteRootFSWriterRetireAndPublishSharesLifecycleTransaction(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))
	require.NoError(t, store.SaveRootFSState(ctx,
		rootFSTestStoreState("sandbox-a", "team-a", "layer-old", "", 1, "old")))

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issueReq.InitialHeadLayerID = "layer-old"
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "snapshotter-pod-a",
		LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: "retire-stale", BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: binding[:], ExpectedOldHeadLayerID: "layer-stale",
	})
	require.ErrorIs(t, err, ErrRootFSHeadConflict)
	_, err = store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: "retire-a", BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ExpectedOldHeadLayerID: "layer-old",
	})
	require.NoError(t, err)

	require.NoError(t, store.WithSandboxLock(ctx, "sandbox-a", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: "txn-a", SandboxID: "sandbox-a", Kind: SandboxLifecycleKindPause,
			Phase: SandboxLifecyclePhasePublishing, ExpectedHeadLayerID: "layer-old",
		})
	}))
	proof := sha256.Sum256([]byte("detach-and-seal-proof"))
	newState := rootFSTestStoreState("sandbox-a", "team-a", "layer-new", "layer-old", 2, "new")
	publishReq := &CompleteRootFSWriterRetireAndPublishRequest{
		LifecycleTxnID: "txn-a",
		GrantID:        issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: "retire-a", BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ProofDigest: proof[:],
		ExpectedOldHeadLayerID: "layer-old", RootFSState: newState,
	}
	missingLifecycle := *publishReq
	missingLifecycle.LifecycleTxnID = "txn-missing"
	_, err = completeRootFSWriterPublishWithLock(ctx, store, &missingLifecycle)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	setStoredRootFSWriterBindingVersion(t, ctx, pool, issueReq.GrantID, RootFSWriterBindingVersion+1)
	_, err = completeRootFSWriterPublishWithLock(ctx, store, publishReq)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	setStoredRootFSWriterBindingVersion(t, ctx, pool, issueReq.GrantID, RootFSWriterBindingVersion)

	rollbackErr := errors.New("force lifecycle rollback")
	err = store.WithSandboxLock(ctx, "sandbox-a", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		writerTx, ok := tx.(RootFSWriterGrantTx)
		require.True(t, ok)
		_, publishErr := writerTx.CompleteRootFSWriterRetireAndPublish(lockCtx, publishReq)
		if publishErr != nil {
			return publishErr
		}
		return rollbackErr
	})
	require.ErrorIs(t, err, rollbackErr)
	assertRootFSWriterPublishState(t, ctx, store, pool, "layer-old", RootFSWriterGrantStateRetiring, false)
	activeTxn, err := store.GetActiveLifecycleTxn(ctx, "sandbox-a")
	require.NoError(t, err)
	require.NotNil(t, activeTxn)
	assert.Equal(t, "txn-a", activeTxn.ID)
	assert.Equal(t, SandboxLifecyclePhasePublishing, activeTxn.Phase)

	err = store.WithSandboxLock(ctx, "sandbox-a", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		writerTx, ok := tx.(RootFSWriterGrantTx)
		require.True(t, ok)
		_, publishErr := writerTx.CompleteRootFSWriterRetireAndPublish(lockCtx, publishReq)
		return publishErr
	})
	require.NoError(t, err)
	assertRootFSWriterPublishState(t, ctx, store, pool, "layer-new", RootFSWriterGrantStateRetired, true)
	committedTxn, err := store.GetActiveLifecycleTxn(ctx, "sandbox-a")
	require.NoError(t, err)
	assert.Nil(t, committedTxn)

	retried, err := completeRootFSWriterPublishWithLock(ctx, store, publishReq)
	require.NoError(t, err)
	assert.Equal(t, RootFSWriterGrantStateRetired, retried.State)
	originalDiffID := publishReq.RootFSState.DiffID
	publishReq.RootFSState.DiffID = "sha256:different-diff-id"
	_, err = completeRootFSWriterPublishWithLock(ctx, store, publishReq)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	publishReq.RootFSState.DiffID = originalDiffID
	differentProof := sha256.Sum256([]byte("different-proof"))
	publishReq.ProofDigest = differentProof[:]
	_, err = completeRootFSWriterPublishWithLock(ctx, store, publishReq)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	assertRootFSWriterPublishState(t, ctx, store, pool, "layer-new", RootFSWriterGrantStateRetired, true)
}

func TestCompleteRootFSWriterRetireAndPublishRejectsStaleHeadAtomically(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-a")))
	require.NoError(t, store.SaveRootFSState(ctx,
		rootFSTestStoreState("sandbox-a", "team-a", "layer-old", "", 1, "old")))

	binding := sha256.Sum256([]byte("binding-a"))
	issueReq := rootFSWriterGrantTestIssueRequest("sandbox-a", "grant-a", "claim-a", "slot-a", binding[:])
	issueReq.InitialHeadLayerID = "layer-old"
	issued, err := store.IssueRootFSWriterGrant(ctx, issueReq)
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		RawToken: issueReq.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "snapshotter-pod-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{
		GrantID: issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: "retire-a", BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ExpectedOldHeadLayerID: "layer-old",
	})
	require.NoError(t, err)
	proof := sha256.Sum256([]byte("proof"))
	require.NoError(t, store.WithSandboxLock(ctx, "sandbox-a", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: "txn-stale", SandboxID: "sandbox-a", Kind: SandboxLifecycleKindPause,
			Phase: SandboxLifecyclePhasePublishing, ExpectedHeadLayerID: "layer-stale",
		})
	}))
	_, err = completeRootFSWriterPublishWithLock(ctx, store, &CompleteRootFSWriterRetireAndPublishRequest{
		LifecycleTxnID: "txn-stale",
		GrantID:        issueReq.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: "retire-a", BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ProofDigest: proof[:],
		ExpectedOldHeadLayerID: "layer-stale",
		RootFSState:            rootFSTestStoreState("sandbox-a", "team-a", "layer-new", "layer-old", 2, "new"),
	})
	require.ErrorIs(t, err, ErrRootFSHeadConflict)
	assertRootFSWriterPublishState(t, ctx, store, pool, "layer-old", RootFSWriterGrantStateRetiring, false)
}

func completeRootFSWriterPublishWithLock(
	ctx context.Context,
	store *PGSandboxStore,
	req *CompleteRootFSWriterRetireAndPublishRequest,
) (*RootFSWriterGrant, error) {
	var result *RootFSWriterGrant
	err := store.WithSandboxLock(ctx, req.RootFSState.SandboxID, func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		writerTx, ok := tx.(RootFSWriterGrantTx)
		if !ok {
			return fmt.Errorf("sandbox store tx does not implement RootFSWriterGrantTx")
		}
		var publishErr error
		result, publishErr = writerTx.CompleteRootFSWriterRetireAndPublish(lockCtx, req)
		return publishErr
	})
	return result, err
}

func assertRootFSWriterPublishState(
	t *testing.T,
	ctx context.Context,
	store *PGSandboxStore,
	pool *pgxpool.Pool,
	wantHead string,
	wantGrantState string,
	wantNewLayer bool,
) {
	t.Helper()
	filesystem, err := store.GetRootFSFilesystem(ctx, "sandbox-a")
	require.NoError(t, err)
	require.NotNil(t, filesystem)
	assert.Equal(t, wantHead, filesystem.HeadLayerID)
	grant, err := store.GetRootFSWriterGrant(ctx, "grant-a")
	require.NoError(t, err)
	assert.Equal(t, wantGrantState, grant.State)
	var layerExists bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM manager.rootfs_layers WHERE layer_id = 'layer-new'
		)
	`).Scan(&layerExists))
	assert.Equal(t, wantNewLayer, layerExists)
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
	require.Empty(t, record.CurrentPodNamespace)
	require.Empty(t, record.CurrentPodName)
	require.Equal(t, fixture.runtimeGeneration, record.RuntimeGeneration)
	var lifecyclePhase, lifecycleError, preparedHead string
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT phase, error, prepared_head_layer_id
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
			FromPodNamespace: "sandbox0", FromPodName: "sandbox-crash-abandon-pod",
			ExpectedHeadLayerID: fixture.initial.ID,
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
			current_pod_namespace = '',
			current_pod_name = '',
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
	require.Empty(t, record.CurrentPodNamespace)
	require.Empty(t, record.CurrentPodName)
	grant, err := fixture.store.GetRootFSWriterGrant(ctx, fixture.issued.Grant.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, grant.State)
	require.Equal(t, RootFSWriterRetireKindCrashAbandon, grant.RetireKind)
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
			FromPodNamespace: "sandbox0", FromPodName: "sandbox-crash-abandon-pod",
			ExpectedHeadLayerID: fixture.initial.ID,
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
	require.Empty(t, record.CurrentPodNamespace)
	require.Empty(t, record.CurrentPodName)
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
				FromGeneration: fixture.runtimeGeneration, FromPodNamespace: "sandbox0",
				FromPodName: "sandbox-crash-abandon-pod", ExpectedHeadLayerID: fixture.initial.ID,
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
			SET prepared_head_layer_id = 'unpublished-dirty-generation'
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
			BindingDigest:          fixture.beginRequest.BindingDigest,
			ExpectedOldHeadLayerID: fixture.initial.ID,
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
	record.CurrentPodNamespace = "sandbox0"
	record.CurrentPodName = "sandbox-crash-abandon-pod"
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
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "ctld-a", LeaseTTL: time.Minute,
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
			FromGeneration: runtimeGeneration, FromPodNamespace: record.CurrentPodNamespace,
			FromPodName: record.CurrentPodName, ExpectedHeadLayerID: initial.ID,
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
	require.Equal(t, "sandbox0", record.CurrentPodNamespace)
	require.Equal(t, "sandbox-crash-abandon-pod", record.CurrentPodName)
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
		NodeUID:           "node-a",
		NodeBootID:        "boot-a",
		PodNamespace:      "sandbox0",
		PodName:           sandboxID + "-pod",
		PodUID:            sandboxID + "-pod-uid",
		NodeName:          "node-a",
		GateParent:        "gate-" + slotID,
		RuntimeGeneration: "17",
		ConsumeExpiresAt:  time.Now().Add(time.Minute),
	}
}
