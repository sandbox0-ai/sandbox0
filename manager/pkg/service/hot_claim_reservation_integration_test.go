package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

func TestHotClaimReservationPersistenceIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	reservation := &HotClaimReservation{
		SandboxID: "sandbox-hot",
		TeamID:    "team-1",
		ClusterID: "cluster-1",
		Namespace: "template-default",
		PodName:   "idle-pod",
		PodUID:    types.UID("pod-uid"),
	}

	reserved, err := store.TryReserveHotClaim(ctx, reservation)
	require.NoError(t, err)
	require.True(t, reserved)
	reserved, err = store.TryReserveHotClaim(ctx, &HotClaimReservation{
		SandboxID: "sandbox-conflict",
		TeamID:    "team-1",
		ClusterID: reservation.ClusterID,
		Namespace: reservation.Namespace,
		PodName:   reservation.PodName,
		PodUID:    reservation.PodUID,
	})
	require.NoError(t, err)
	require.False(t, reserved)
	otherCluster := &HotClaimReservation{
		SandboxID: "sandbox-other-cluster",
		TeamID:    reservation.TeamID,
		ClusterID: "cluster-2",
		Namespace: reservation.Namespace,
		PodName:   reservation.PodName,
		PodUID:    types.UID("other-pod-uid"),
	}
	reserved, err = store.TryReserveHotClaim(ctx, otherCluster)
	require.NoError(t, err)
	require.True(t, reserved)

	stored, err := store.GetHotClaimReservation(ctx, reservation.SandboxID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.True(t, stored.CommittedAt.IsZero())

	record := rootFSTestSandboxRecord(reservation.SandboxID, reservation.TeamID)
	record.CurrentPodNamespace = reservation.Namespace
	record.CurrentPodName = reservation.PodName
	record.ClusterID = reservation.ClusterID
	metadata := HotClaimPodMetadata{
		Labels:      map[string]string{"sandbox0.ai/sandbox-id": reservation.SandboxID},
		Annotations: map[string]string{"sandbox0.ai/team-id": reservation.TeamID},
		Finalizers:  []string{"sandbox0.ai/sandbox-cleanup"},
	}
	require.NoError(t, store.CommitHotClaim(ctx, record, reservation.PodUID, metadata))

	stored, err = store.GetHotClaimReservation(ctx, reservation.SandboxID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.False(t, stored.CommittedAt.IsZero())
	require.Equal(t, metadata, stored.Metadata)
	persistedRecord, err := store.GetSandbox(ctx, reservation.SandboxID)
	require.NoError(t, err)
	require.NotNil(t, persistedRecord)
	require.Equal(t, reservation.PodName, persistedRecord.CurrentPodName)

	reservations, err := store.ListHotClaimReservations(ctx, reservation.ClusterID, time.Now().Add(time.Second), 10)
	require.NoError(t, err)
	require.Len(t, reservations, 1)
	require.NoError(t, store.ReleaseHotClaimReservation(ctx, reservation.SandboxID, reservation.PodUID))
	require.NoError(t, store.ReleaseHotClaimReservation(ctx, otherCluster.SandboxID, otherCluster.PodUID))
	stored, err = store.GetHotClaimReservation(ctx, reservation.SandboxID)
	require.NoError(t, err)
	require.Nil(t, stored)
}
