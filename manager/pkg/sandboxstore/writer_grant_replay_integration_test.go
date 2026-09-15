// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sandboxstore

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRootFSWriterConsumeReplayAndBatchFencingIntegration(t *testing.T) {
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	requests := make([]*RenewRootFSWriterGrantRequest, 4)
	consumes := make([]*ConsumeRootFSWriterGrantRequest, 4)
	filesystems := make([]string, 4)
	originalExpiries := make([]time.Time, 4)
	for i := range requests {
		id := fmt.Sprintf("batch-renew-%d", i)
		require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord(id, "team-a")))
		filesystem, initial, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
			SandboxID: id, TeamID: "team-a", SourceOCIRef: artifact.SourceOCIRef,
			SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
		})
		require.NoError(t, err)
		filesystems[i] = filesystem.ID
		binding := sha256.Sum256([]byte(id))
		issue := rootFSWriterGrantTestIssueRequest(id, "grant-"+id, "claim-"+id, "slot-"+id, binding[:])
		issue.ExpectedFilesystemID, issue.InitialGenerationID = filesystem.ID, initial.ID
		issued, err := store.IssueRootFSWriterGrant(ctx, issue)
		require.NoError(t, err)
		consumes[i] = &ConsumeRootFSWriterGrantRequest{
			GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken,
			BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ConsumerNodeUID: "node-a", ConsumerAgentUID: "ctld-a", LeaseTTL: time.Minute,
		}
		consumed, err := store.ConsumeRootFSWriterGrant(ctx, consumes[i])
		require.NoError(t, err)
		originalExpiries[i] = consumed.LeaseExpiresAt
		replay := *consumes[i]
		replay.LeaseTTL = 5 * time.Minute
		replayed, err := store.ConsumeRootFSWriterGrant(ctx, &replay)
		require.NoError(t, err)
		require.Equal(t, consumed.ConsumedAt, replayed.ConsumedAt)
		require.Equal(t, consumed.LeaseExpiresAt, replayed.LeaseExpiresAt, "consume replay must not renew the lease")
		requests[i] = &RenewRootFSWriterGrantRequest{GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ConsumerNodeUID: "node-a"}
	}
	requests[1].ConsumerNodeUID = "different-node"
	_, err = pool.Exec(ctx, "UPDATE manager.rootfs_filesystems SET writer_epoch=writer_epoch+1 WHERE filesystem_id=$1", filesystems[2])
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, consumes[2])
	require.ErrorIs(t, err, ErrRootFSWriterEpochConflict, "a consumed grant replay must still honor current fencing")
	requests[3].GrantID = "missing-grant"
	results, err := store.RenewRootFSWriterGrants(ctx, requests, RootFSWriterLeaseRenewalPolicy{LeaseTTL: 2 * time.Minute, GracePeriod: time.Second})
	require.NoError(t, err)
	require.Len(t, results, 4)
	require.NoError(t, results[0].Err)
	require.True(t, results[0].Grant.LeaseExpiresAt.After(originalExpiries[0]))
	require.ErrorIs(t, results[1].Err, ErrRootFSWriterGrantConflict)
	require.ErrorIs(t, results[2].Err, ErrRootFSWriterEpochConflict)
	require.ErrorIs(t, results[3].Err, ErrRootFSWriterGrantNotFound)
	for _, i := range []int{1, 2, 3} {
		var expiry time.Time
		require.NoError(t, pool.QueryRow(ctx, "SELECT lease_expires_at FROM manager.rootfs_writer_grants WHERE grant_id=$1", consumes[i].GrantID).Scan(&expiry))
		require.Equal(t, originalExpiries[i], expiry, "rejected batch item must not extend its writer lease")
	}
}
