package sandboxstore

import (
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestCompatibleImportLeasePreservesOtherExecutableWork(t *testing.T) {
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	old, _, _ := rootFSImportTestFixture(t, "rollout-old")
	next, _, _ := rootFSImportTestFixture(t, "rollout-next")
	_, err := store.BeginRootFSImport(ctx, old)
	require.NoError(t, err)
	_, err = store.BeginRootFSImport(ctx, next)
	require.NoError(t, err)
	leased, err := store.LeaseNextCompatibleRootFSImport(ctx, "new-worker", time.Minute, next.Spec.ProcdProtocol, next.Spec.ProcdDigest)
	require.NoError(t, err)
	require.Equal(t, next.OperationID, leased.ID)
	pending, err := store.GetRootFSImportOperation(ctx, old.OperationID)
	require.NoError(t, err)
	require.Equal(t, RootFSImportStatePending, pending.State)
	require.Zero(t, pending.AttemptCount)
	_, err = pool.Exec(ctx, `UPDATE manager.rootfs_import_operations SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE operation_id=$1`, leased.ID)
	require.NoError(t, err)
	oldLease, err := store.LeaseNextCompatibleRootFSImport(ctx, "old-worker", time.Minute, old.Spec.ProcdProtocol, old.Spec.ProcdDigest)
	require.NoError(t, err)
	require.Equal(t, old.OperationID, oldLease.ID, "an expired different-version build must not be stolen")
	recovered, err := store.LeaseNextCompatibleRootFSImport(ctx, "new-worker-2", time.Minute, next.Spec.ProcdProtocol, next.Spec.ProcdDigest)
	require.NoError(t, err)
	require.Equal(t, leased.ID, recovered.ID)
	require.NotEqual(t, leased.LeaseToken, recovered.LeaseToken)
	require.Equal(t, 2, recovered.AttemptCount)
	for _, protocol := range []string{"sandbox0.procd.other", old.Spec.ProcdProtocol} {
		none, err := store.LeaseNextCompatibleRootFSImport(ctx, "unmatched", time.Minute, protocol, digest.FromString("absent").String())
		require.NoError(t, err)
		require.Nil(t, none)
	}
	_, err = store.LeaseNextCompatibleRootFSImport(ctx, "invalid", time.Minute, "", next.Spec.ProcdDigest)
	require.Error(t, err)
	_, err = store.LeaseNextCompatibleRootFSImport(ctx, "invalid", time.Minute, next.Spec.ProcdProtocol, "")
	require.Error(t, err)
}

func TestCommittedArtifactProcdSurvivesImagePolicyUpgrade(t *testing.T) {
	ctx := t.Context()
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	requirements := ReadyRootFSArtifactRequirements{
		FormatGeneration: artifact.FormatGeneration, LogicalSizeBytes: artifact.LogicalSizeBytes,
		ProcdProtocol: artifact.ProcdProtocol, ProcdDigest: digest.FromString("new-importer-procd").String(),
	}
	_, err = store.GetReadyRootFSBaseArtifactByDigest(ctx, artifact.ArtifactDigest, artifact.Platform, requirements)
	require.ErrorIs(t, err, ErrRootFSBaseArtifactNotFound)
	requirements.PreserveCommittedProcd = true
	selected, err := store.GetReadyRootFSBaseArtifactByDigest(ctx, artifact.ArtifactDigest, artifact.Platform, requirements)
	require.NoError(t, err)
	require.Equal(t, artifact.ProcdDigest, selected.ProcdDigest)
	_, err = store.GetReadyRootFSBaseArtifact(ctx, artifact.SourceOCIDigest, artifact.Platform, requirements)
	require.ErrorContains(t, err, "exact artifact digest")
	for _, mutate := range []func(*ReadyRootFSArtifactRequirements){
		func(r *ReadyRootFSArtifactRequirements) { r.ProcdProtocol = "sandbox0.procd.incompatible" },
		func(r *ReadyRootFSArtifactRequirements) { r.LogicalSizeBytes *= 2 },
		func(r *ReadyRootFSArtifactRequirements) { r.FormatGeneration++ },
	} {
		changed := requirements
		mutate(&changed)
		_, err = store.GetReadyRootFSBaseArtifactByDigest(ctx, artifact.ArtifactDigest, artifact.Platform, changed)
		require.ErrorIs(t, err, ErrRootFSBaseArtifactNotFound)
	}
	_, err = store.GetReadyRootFSBaseArtifactByDigest(ctx, digest.FromString("wrong-artifact").String(), artifact.Platform, requirements)
	require.ErrorIs(t, err, ErrRootFSBaseArtifactNotFound)
}
