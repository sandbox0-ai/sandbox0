package runtimeslot

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func restoreCPUReservationFixture(t *testing.T) MigrationRestoreCPUReservation {
	t.Helper()
	target := testNodeChannelTarget(true)
	resources, err := NewRuntimeResourceLease("migration-restore", "migration-claim", target.SlotID,
		target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID,
		RuntimeResourceRequest{Version: RuntimeResourceRequestVersion, CPUMillicores: 150,
			MemoryBytes: 512 << 20, PIDsLimit: DefaultRuntimePIDsLimit}, "0-3", "0")
	require.NoError(t, err)
	reservation, err := NewMigrationRestoreCPUReservation(target, resources, 500)
	require.NoError(t, err)
	return reservation
}

func TestMigrationRestoreCPUReservationPreservesBaseLeaseAndBindsRetries(t *testing.T) {
	r := restoreCPUReservationFixture(t)
	before, err := r.Resources.Digest()
	require.NoError(t, err)
	replayed, err := NewMigrationRestoreCPUReservation(r.Target, r.Resources, r.TotalCPUMillicores)
	require.NoError(t, err)
	require.Equal(t, r, replayed)
	require.Equal(t, int64(350), r.TotalCPUMillicores-r.Resources.CPUMillicores)
	require.Equal(t, int64(15000), r.Resources.CPUQuotaMicros)
	after, err := replayed.Resources.Digest()
	require.NoError(t, err)
	require.Equal(t, before, after, "temporary capacity must not rewrite the immutable base lease")
	for _, mutate := range []func(*MigrationRestoreCPUReservation){
		func(r *MigrationRestoreCPUReservation) { r.TotalCPUMillicores = 600 },
		func(r *MigrationRestoreCPUReservation) { r.Target.NodeBootID = "another-boot" },
		func(r *MigrationRestoreCPUReservation) { r.Resources.MemoryBytes++ },
		func(r *MigrationRestoreCPUReservation) { r.Version++ },
	} {
		changed := r
		mutate(&changed)
		require.Error(t, changed.Validate(), "a retry cannot mutate already-admitted capacity or identity")
	}
	other, err := NewMigrationRestoreCPUReservation(r.Target, r.Resources, 600)
	require.NoError(t, err)
	require.NotEqual(t, r.ReservationID, other.ReservationID)
}

func TestMigrationRestoreCPUReservationRejectsInvalidCapacity(t *testing.T) {
	r := restoreCPUReservationFixture(t)
	for _, total := range []int64{math.MinInt64, -1, 0, 149, 150, 151, 159, MaxRuntimeCPUMillicores + 1, math.MaxInt64} {
		_, err := NewMigrationRestoreCPUReservation(r.Target, r.Resources, total)
		require.Error(t, err)
	}
	_, err := NewMigrationRestoreCPUReservation(r.Target, r.Resources, 160)
	require.NoError(t, err)
	grant := MigrationRestoreCPUGrant{Reservation: r, RestoreRequestDigest: "not-a-digest"}
	require.Error(t, grant.Validate())
	grant.RestoreRequestDigest = strings.Repeat("a", 64)
	require.Error(t, grant.ValidateRestore(MigrationRestoreRequest{}), "a capacity descriptor is not execution authority")
}

func TestMigrationRestoreCPUResetProofRejectsRetainedQuotaAndCgroupReplacement(t *testing.T) {
	r := restoreCPUReservationFixture(t)
	grant := MigrationRestoreCPUGrant{Reservation: r, RestoreRequestDigest: strings.Repeat("a", 64)}
	digest, err := grant.Digest()
	require.NoError(t, err)
	proof := MigrationRestoreCPUResetProof{GrantDigest: digest, CgroupID: 123,
		CPUPeriodMicros: r.Resources.CPUPeriodMicros, CPUQuotaMicros: r.Resources.CPUQuotaMicros}
	require.NoError(t, proof.ValidateFor(grant, 123))
	for _, mutate := range []func(*MigrationRestoreCPUResetProof){
		func(p *MigrationRestoreCPUResetProof) { p.CPUQuotaMicros = 50000 },
		func(p *MigrationRestoreCPUResetProof) { p.CPUQuotaMicros = -1 },
		func(p *MigrationRestoreCPUResetProof) { p.CPUPeriodMicros = 200000 },
		func(p *MigrationRestoreCPUResetProof) { p.CgroupID++ },
		func(p *MigrationRestoreCPUResetProof) { p.GrantDigest = strings.Repeat("b", 64) },
	} {
		changed := proof
		mutate(&changed)
		require.Error(t, changed.ValidateFor(grant, 123))
	}
	require.Error(t, proof.ValidateFor(grant, 0), "missing durable cgroup identity cannot release capacity")
	other, err := NewMigrationRestoreCPUReservation(r.Target, r.Resources, 600)
	require.NoError(t, err)
	require.Error(t, proof.ValidateFor(MigrationRestoreCPUGrant{Reservation: other, RestoreRequestDigest: grant.RestoreRequestDigest}, 123), "an old reset receipt cannot release a newer reservation")
	otherGrant := grant
	otherGrant.RestoreRequestDigest = strings.Repeat("b", 64)
	require.NoError(t, otherGrant.Validate())
	require.Error(t, proof.ValidateFor(otherGrant, 123), "a reset receipt cannot cross restore bindings")
}
