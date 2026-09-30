package sandboxstore

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func releaseTestArtifact(version string) RuntimeReleaseArtifact {
	return RuntimeReleaseArtifact{SourceCommit: strings.Repeat(version, 40), SHA256: strings.Repeat(version, 64),
		ObjectKey: "sandbox0-nomad-runtime/" + version + ".tar.gz", OSSEndpoint: "https://isolated-runtime.invalid", OSSBucket: "runtime-test"}
}

func releaseTestReadySlot(t *testing.T, store *PGSandboxStore, name, node string) *RegisterRuntimeSlotRequest {
	t.Helper()
	r := runtimeSlotTestRegistration(name, "allocation-"+name)
	r.NodeID = "node-" + node
	r.NodeUID = "uid-" + node
	r.NodeBootID = "boot-" + node
	_, err := registerRuntimeSlotWithTestCapacity(t, t.Context(), store, r)
	require.NoError(t, err)
	_, err = store.ReportRuntimeSlotReady(t.Context(), &ReportRuntimeSlotReadyRequest{SlotID: r.SlotID, AllocationID: r.AllocationID,
		NodeUID: r.NodeUID, NodeBootID: r.NodeBootID, RuntimeReadyDigest: bytes.Repeat([]byte{1}, 32),
		NetworkReadyDigest: bytes.Repeat([]byte{2}, 32), StorageReadyDigest: bytes.Repeat([]byte{3}, 32), HeartbeatTTL: time.Minute})
	require.NoError(t, err)
	return r
}

func releaseTestClaim(t *testing.T, store *PGSandboxStore, id, digest string) *AcquireRuntimeSlotRequest {
	t.Helper()
	fs, generation := runtimeSlotTestGeneration(t, store, id, "operation-"+id)
	return &AcquireRuntimeSlotRequest{OperationID: "operation-" + id, ClaimID: "claim-" + id, SandboxID: id, FilesystemID: fs.ID,
		SourceGenerationID: generation.ID, CompatibilityDigest: digest, ClusterID: "cluster-a", RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute, Resources: runtimeSlotTestResources()}
}

func releaseTestActivation(operation string, artifact RuntimeReleaseArtifact, digest string, revision int64) ActivateRuntimeReleaseRequest {
	return ActivateRuntimeReleaseRequest{ClusterID: "cluster-a", SourceCommit: artifact.SourceCommit, BundleSHA256: artifact.SHA256,
		OperationID: operation, ExpectedRevision: revision, CompatibilityDigest: digest, MinimumReadyNodes: 1, MinimumReadySlots: 1}
}

func TestRuntimeHotReleasePreservesClaimsAndRollbackIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	old := releaseTestReadySlot(t, store, "old-active", "old")
	releaseTestReadySlot(t, store, "old-ready", "old")
	newSlot := releaseTestReadySlot(t, store, "new-ready", "new")
	releaseTestReadySlot(t, store, "new-spare", "new")
	for node, artifact := range map[string]RuntimeReleaseArtifact{"uid-old": releaseTestArtifact("1"), "uid-new": releaseTestArtifact("2")} {
		_, err := store.PinRuntimeNodeReleaseArtifact(t.Context(), "cluster-a", node, artifact)
		require.NoError(t, err)
	}
	oldRequest := releaseTestClaim(t, store, "old-guest", old.CompatibilityDigest)
	oldRequest.TargetNodeID, oldRequest.TargetNodeUID, oldRequest.TargetNodeBootID = old.NodeID, old.NodeUID, old.NodeBootID
	original, err := store.AcquireRuntimeSlot(t.Context(), oldRequest)
	require.NoError(t, err)
	policy, err := store.ActivateRuntimeRelease(t.Context(), releaseTestActivation("upgrade", releaseTestArtifact("2"), old.CompatibilityDigest, 0))
	require.NoError(t, err)
	require.EqualValues(t, 1, policy.Revision)
	retained, err := store.AcquireRuntimeSlot(t.Context(), oldRequest)
	require.NoError(t, err)
	require.Equal(t, original.ResourceLease, retained.ResourceLease)
	require.Equal(t, original.Revision, retained.Revision)
	require.Equal(t, original.ClaimedAt, retained.ClaimedAt)
	newRequest := releaseTestClaim(t, store, "new-guest", old.CompatibilityDigest)
	blocked := *newRequest
	blocked.TargetNodeID, blocked.TargetNodeUID, blocked.TargetNodeBootID = old.NodeID, old.NodeUID, old.NodeBootID
	_, err = store.AcquireRuntimeSlot(t.Context(), &blocked)
	require.ErrorIs(t, err, ErrRuntimeSlotUnavailable)
	claimed, err := store.AcquireRuntimeSlot(t.Context(), newRequest)
	require.NoError(t, err)
	require.Equal(t, newSlot.NodeUID, claimed.NodeUID)
	rollback, err := store.ActivateRuntimeRelease(t.Context(), releaseTestActivation("rollback", releaseTestArtifact("1"), old.CompatibilityDigest, 1))
	require.NoError(t, err)
	require.EqualValues(t, 2, rollback.Revision)
	retainedNew, err := store.AcquireRuntimeSlot(t.Context(), newRequest)
	require.NoError(t, err)
	require.Equal(t, claimed.ResourceLease, retainedNew.ResourceLease)
	require.Equal(t, claimed.Revision, retainedNew.Revision)
	require.Equal(t, claimed.ClaimedAt, retainedNew.ClaimedAt)
	// An old response retry must acknowledge revision 1 without undoing rollback.
	retry, err := store.ActivateRuntimeRelease(t.Context(), releaseTestActivation("upgrade", releaseTestArtifact("2"), old.CompatibilityDigest, 0))
	require.NoError(t, err)
	require.EqualValues(t, 1, retry.Revision)
	var source string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT source_commit FROM manager.runtime_release_policies`).Scan(&source))
	require.Equal(t, releaseTestArtifact("1").SourceCommit, source)
	var leases int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
	require.Equal(t, 2, leases)
}

func TestRuntimeHotReleaseRejectsUnreadyAndConflictingOperationsIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	slot := releaseTestReadySlot(t, store, "candidate", "new")
	_, err := store.PinRuntimeNodeReleaseArtifact(t.Context(), "cluster-a", slot.NodeUID, releaseTestArtifact("2"))
	require.NoError(t, err)
	r := releaseTestActivation("upgrade", releaseTestArtifact("2"), slot.CompatibilityDigest, 0)
	missing := r
	missing.BundleSHA256 = strings.Repeat("3", 64)
	_, err = store.ActivateRuntimeRelease(t.Context(), missing)
	require.ErrorContains(t, err, "lacks ready capacity")
	more := r
	more.MinimumReadyNodes = 2
	_, err = store.ActivateRuntimeRelease(t.Context(), more)
	require.ErrorContains(t, err, "lacks ready capacity")
	_, err = pool.Exec(t.Context(), `INSERT INTO manager.runtime_node_fences(cluster_id,node_id,node_uid,state,reason) VALUES($1,$2,$3,'warming','foreign operation')`, slot.ClusterID, slot.NodeID, slot.NodeUID)
	require.NoError(t, err)
	_, err = store.ActivateRuntimeRelease(t.Context(), r)
	require.ErrorContains(t, err, "lacks ready capacity")
	_, err = pool.Exec(t.Context(), `DELETE FROM manager.runtime_node_fences`)
	require.NoError(t, err)
	_, err = store.ActivateRuntimeRelease(t.Context(), r)
	require.NoError(t, err)
	_, err = store.ActivateRuntimeRelease(t.Context(), missing)
	require.ErrorIs(t, err, ErrRuntimeReleaseConflict)
	stale := r
	stale.OperationID = "other"
	_, err = store.ActivateRuntimeRelease(t.Context(), stale)
	require.ErrorIs(t, err, ErrRuntimeReleaseConflict)
}

func TestRuntimeHotReleaseCutoverWaitsForClaimsIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	slot := releaseTestReadySlot(t, store, "candidate", "new")
	_, err := store.PinRuntimeNodeReleaseArtifact(t.Context(), "cluster-a", slot.NodeUID, releaseTestArtifact("2"))
	require.NoError(t, err)
	tx, err := pool.Begin(t.Context())
	require.NoError(t, err)
	defer tx.Rollback(context.Background()) //nolint:errcheck
	require.NoError(t, lockRuntimeRelease(t.Context(), tx, "cluster-a", true))
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = store.ActivateRuntimeRelease(ctx, releaseTestActivation("blocked", releaseTestArtifact("2"), slot.CompatibilityDigest, 0))
	require.Error(t, err)
	var count int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.runtime_release_policies`).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, tx.Commit(t.Context()))
	_, err = store.ActivateRuntimeRelease(t.Context(), releaseTestActivation("retry", releaseTestArtifact("2"), slot.CompatibilityDigest, 0))
	require.NoError(t, err)
}

func TestRuntimeHotReleasePinsBootstrapArtifactIntegration(t *testing.T) {
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	first, err := store.PinRuntimeNodeReleaseArtifact(t.Context(), "cluster-a", "enrolling", releaseTestArtifact("1"))
	require.NoError(t, err)
	retry, err := store.PinRuntimeNodeReleaseArtifact(t.Context(), "cluster-a", "enrolling", releaseTestArtifact("2"))
	require.NoError(t, err)
	require.Equal(t, first, retry)
	bad := releaseTestArtifact("1")
	bad.OSSEndpoint = "http://runtime.invalid"
	_, err = store.PinRuntimeNodeReleaseArtifact(t.Context(), "cluster-a", "bad", bad)
	require.Error(t, err)
}

func TestRuntimeHotReleaseProbeCannotAdmitOrdinaryClaimsIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	old := releaseTestReadySlot(t, store, "old-spare", "old")
	candidate := releaseTestReadySlot(t, store, "candidate", "new")
	releaseTestReadySlot(t, store, "candidate-spare", "new")
	for uid, artifact := range map[string]RuntimeReleaseArtifact{"uid-old": releaseTestArtifact("1"), "uid-new": releaseTestArtifact("2")} {
		_, err := store.PinRuntimeNodeReleaseArtifact(t.Context(), "cluster-a", uid, artifact)
		require.NoError(t, err)
	}
	_, err := store.ActivateRuntimeRelease(t.Context(), releaseTestActivation("baseline", releaseTestArtifact("1"), old.CompatibilityDigest, 0))
	require.NoError(t, err)
	probe := releaseTestClaim(t, store, "probe", old.CompatibilityDigest)
	probe.TargetNodeID, probe.TargetNodeUID, probe.TargetNodeBootID = candidate.NodeID, candidate.NodeUID, candidate.NodeBootID
	_, err = store.AcquireRuntimeSlot(t.Context(), probe)
	require.ErrorIs(t, err, ErrRuntimeSlotUnavailable)
	grant := RuntimeReleaseProbe{OperationID: probe.OperationID, ClusterID: probe.ClusterID, NodeID: candidate.NodeID, NodeUID: candidate.NodeUID, NodeBootID: candidate.NodeBootID, SourceCommit: releaseTestArtifact("2").SourceCommit, BundleSHA256: releaseTestArtifact("2").SHA256}
	require.NoError(t, store.AuthorizeRuntimeReleaseProbe(t.Context(), grant))
	var expiry time.Time
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT expires_at FROM manager.runtime_release_probes WHERE operation_id=$1`, grant.OperationID).Scan(&expiry))
	require.NoError(t, store.AuthorizeRuntimeReleaseProbe(t.Context(), grant))
	var retryExpiry time.Time
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT expires_at FROM manager.runtime_release_probes WHERE operation_id=$1`, grant.OperationID).Scan(&retryExpiry))
	require.Equal(t, expiry, retryExpiry)
	changed := grant
	changed.NodeBootID = "other-boot"
	require.ErrorIs(t, store.AuthorizeRuntimeReleaseProbe(t.Context(), changed), ErrRuntimeReleaseConflict)
	claimed, err := store.AcquireRuntimeSlot(t.Context(), probe)
	require.NoError(t, err)
	require.Equal(t, candidate.NodeUID, claimed.NodeUID)
	ordinary := releaseTestClaim(t, store, "ordinary", old.CompatibilityDigest)
	ordinary.TargetNodeID, ordinary.TargetNodeUID, ordinary.TargetNodeBootID = candidate.NodeID, candidate.NodeUID, candidate.NodeBootID
	_, err = store.AcquireRuntimeSlot(t.Context(), ordinary)
	require.ErrorIs(t, err, ErrRuntimeSlotUnavailable)
	// A claiming slot is insufficient evidence for a successful guest command.
	require.ErrorIs(t, store.CompleteRuntimeReleaseProbe(t.Context(), grant.OperationID, strings.Repeat("a", 64)), ErrRuntimeReleaseConflict)
	expired := grant
	expired.OperationID = ordinary.OperationID
	require.NoError(t, store.AuthorizeRuntimeReleaseProbe(t.Context(), expired))
	_, err = pool.Exec(t.Context(), `UPDATE manager.runtime_release_probes SET expires_at=NOW()-INTERVAL '1 second' WHERE operation_id=$1`, expired.OperationID)
	require.NoError(t, err)
	_, err = store.AcquireRuntimeSlot(t.Context(), ordinary)
	require.ErrorIs(t, err, ErrRuntimeSlotUnavailable)
	require.ErrorIs(t, store.AuthorizeRuntimeReleaseProbe(t.Context(), expired), ErrRuntimeReleaseConflict)
	// Targeted probing never changed the region's normal admission policy.
	policy, err := store.GetRuntimeReleasePolicy(t.Context(), "cluster-a")
	require.NoError(t, err)
	require.Equal(t, releaseTestArtifact("1").SourceCommit, policy.SourceCommit)
}

func TestRuntimeHotReleaseDatabaseGuardRechecksAfterCutoverWaitIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	old := releaseTestReadySlot(t, store, "old-claim", "old")
	releaseTestReadySlot(t, store, "old-spare", "old")
	candidate := releaseTestReadySlot(t, store, "candidate", "new")
	for uid, artifact := range map[string]RuntimeReleaseArtifact{"uid-old": releaseTestArtifact("1"), "uid-new": releaseTestArtifact("2")} {
		_, err := store.PinRuntimeNodeReleaseArtifact(t.Context(), "cluster-a", uid, artifact)
		require.NoError(t, err)
	}
	claimed, err := store.AcquireRuntimeSlot(t.Context(), releaseTestClaim(t, store, "old-guest", old.CompatibilityDigest))
	require.NoError(t, err)
	tx, err := pool.Begin(t.Context())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	require.NoError(t, lockRuntimeRelease(t.Context(), tx, "cluster-a", false))
	lease := claimed.ResourceLease
	lease.LeaseID = "legacy-manager-test"
	lease.SlotID = "old-spare"
	lease.OperationID = "legacy-claim-test"
	lease.ClaimID = "legacy-claim-test"
	lease.CgroupName = "legacy-manager-test"
	result := make(chan error, 1)
	go func() {
		claimTx, e := pool.Begin(t.Context())
		if e != nil {
			result <- e
			return
		}
		defer claimTx.Rollback(context.Background())
		result <- insertRuntimeResourceLease(t.Context(), claimTx, lease, claimed.ResourceLeaseDigest)
	}()
	// Observe the actual PostgreSQL advisory wait, not a scheduling sleep.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE '%INSERT INTO manager.runtime_resource_leases%')`).Scan(&waiting))
		if waiting {
			break
		}
		require.True(t, time.Now().Before(deadline), "legacy claim did not reach the admission lock")
	}
	_, err = tx.Exec(t.Context(), `INSERT INTO manager.runtime_release_policies(cluster_id,source_commit,bundle_sha256,operation_id,revision) VALUES($1,$2,$3,'cutover',1)`, candidate.ClusterID, releaseTestArtifact("2").SourceCommit, releaseTestArtifact("2").SHA256)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(t.Context()))
	select {
	case err = <-result:
		require.ErrorContains(t, err, "runtime release does not admit new node leases")
	case <-time.After(5 * time.Second):
		t.Fatal("claim stayed blocked after cutover")
	}
	retained, err := store.AcquireRuntimeSlot(t.Context(), releaseTestClaim(t, store, "old-guest", old.CompatibilityDigest))
	require.NoError(t, err)
	require.Equal(t, claimed.ResourceLease, retained.ResourceLease)
}

func TestRuntimeHotReleaseRetiresOnlyIdleChosenPredecessorsIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	_, err := store.EnsureRuntimeNodePoolState(t.Context(), "elastic", "cluster-a")
	require.NoError(t, err)
	nodes := []string{"old", "busy", "new"}
	for index, node := range nodes {
		_, err = store.ReserveRuntimeNode(t.Context(), &ReserveRuntimeNodeRequest{PoolID: "elastic", ProviderInstanceID: "i-" + node, PoolKind: RuntimeNodePoolKindElastic, ClusterID: "cluster-a", NodeName: "s0-" + node, NodeUID: "uid-" + node, PrivateIP: fmt.Sprintf("10.0.1.%d", index+10), AllocationSupernet: "172.27.0.0/17", AllocationPrefix: 26})
		require.NoError(t, err)
		require.NoError(t, store.ActivateRuntimeNode(t.Context(), &ActivateRuntimeNodeRequest{PoolID: "elastic", ProviderInstanceID: "i-" + node, NomadNodeID: "node-" + node, AuthorityCommonName: "ctld-" + node, AgentUID: "ctld/test/" + node}))
		releaseTestReadySlot(t, store, node+"-ready", node)
		releaseTestReadySlot(t, store, node+"-spare", node)
		version := "1"
		if node == "new" {
			version = "2"
		}
		_, err = store.PinRuntimeNodeReleaseArtifact(t.Context(), "cluster-a", "uid-"+node, releaseTestArtifact(version))
		require.NoError(t, err)
	}
	// This isolated fixture represents completed provider admission.
	_, err = pool.Exec(t.Context(), `DELETE FROM manager.runtime_node_fences WHERE state='warming'`)
	require.NoError(t, err)
	inventory, err := store.GetRuntimeReleaseInventory(t.Context(), "cluster-a")
	require.NoError(t, err)
	digest := inventory[0].CompatibilityDigest
	_, err = store.ActivateRuntimeRelease(t.Context(), releaseTestActivation("baseline", releaseTestArtifact("1"), digest, 0))
	require.NoError(t, err)
	count, err := store.RetireIdlePredecessorRuntimeNodes(t.Context(), "elastic")
	require.NoError(t, err)
	require.Zero(t, count, "unactivated candidates must remain available for testing")
	busy := releaseTestClaim(t, store, "busy-guest", digest)
	busy.TargetNodeID, busy.TargetNodeUID, busy.TargetNodeBootID = "node-busy", "uid-busy", "boot-busy"
	_, err = store.AcquireRuntimeSlot(t.Context(), busy)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO manager.runtime_node_fences(cluster_id,node_id,node_uid,state,reason) VALUES('cluster-a','node-old','uid-old','warming','foreign maintenance')`)
	require.NoError(t, err)
	_, err = store.ActivateRuntimeRelease(t.Context(), releaseTestActivation("upgrade", releaseTestArtifact("2"), digest, 1))
	require.NoError(t, err)
	count, err = store.RetireIdlePredecessorRuntimeNodes(t.Context(), "elastic")
	require.NoError(t, err)
	require.Zero(t, count, "busy and foreign-fenced predecessors cannot retire")
	snapshot, err := store.GetRuntimeNodePoolSnapshot(t.Context(), "elastic")
	require.NoError(t, err)
	for _, node := range snapshot.Nodes {
		if node.NodeUID == "uid-busy" {
			require.True(t, node.PredecessorRelease)
			require.Equal(t, 1, node.ActiveLeases)
			require.Equal(t, 1, node.ReadySlots, "physical readiness remains usable for enrollment accounting")
		}
	}
	_, err = pool.Exec(t.Context(), `DELETE FROM manager.runtime_node_fences WHERE node_uid='uid-old' AND reason='foreign maintenance'`)
	require.NoError(t, err)
	count, err = store.RetireIdlePredecessorRuntimeNodes(t.Context(), "elastic")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	idle, err := store.GetRuntimeNodeDrainStatus(t.Context(), "elastic", "i-old")
	require.NoError(t, err)
	require.Equal(t, RuntimeNodeInstanceDraining, idle.Instance.State)
	require.Contains(t, idle.Instance.DrainReason, "audited-runtime-rollout:hot-retire:")
	occupied, err := store.GetRuntimeNodeDrainStatus(t.Context(), "elastic", "i-busy")
	require.NoError(t, err)
	require.Equal(t, RuntimeNodeInstanceActive, occupied.Instance.State)
	require.Equal(t, 1, occupied.Instance.ActiveLeases)
	count, err = store.RetireIdlePredecessorRuntimeNodes(t.Context(), "elastic")
	require.NoError(t, err)
	require.Zero(t, count)
}
