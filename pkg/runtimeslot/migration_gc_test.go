package runtimeslot

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrationSourceGCChannelBindsRetentionAcknowledgement(t *testing.T) {
	r := MigrationSourceGCRequest{Target: migrationProtocolRequest().Target, FinalizationDigest: strings.Repeat("a", 64), CleanupProofDigest: strings.Repeat("b", 64), AllocationAbsenceDigest: strings.Repeat("c", 64)}
	c, err := NewNodeChannelMigrationSourceGCCommand(r)
	require.NoError(t, err)
	digest, err := r.Digest()
	require.NoError(t, err)
	result := NodeChannelResult{Version: NodeChannelVersion, RequestID: c.RequestID, Kind: c.Kind, MigrationSourceGC: &MigrationSourceGCAcknowledgement{RequestDigest: digest}}
	require.NoError(t, result.ValidateFor(c))
	for _, change := range []func(*MigrationSourceGCRequest){
		func(r *MigrationSourceGCRequest) { r.Target.NodeBootID = "other-boot" },
		func(r *MigrationSourceGCRequest) { r.Target.AllocationID = "other-allocation" },
		func(r *MigrationSourceGCRequest) { r.FinalizationDigest = strings.Repeat("d", 64) },
		func(r *MigrationSourceGCRequest) { r.CleanupProofDigest = strings.Repeat("d", 64) },
		func(r *MigrationSourceGCRequest) { r.AllocationAbsenceDigest = strings.Repeat("d", 64) },
	} {
		changed := r
		change(&changed)
		require.Error(t, result.MigrationSourceGC.ValidateFor(changed))
	}
	result.CleanupProof = &NodeCleanupControlProof{}
	require.Error(t, result.ValidateFor(c), "retention acknowledgement is not cleanup authority")
	c.MigrationFinalize = &MigrationSourceFinalizeRequest{}
	_, err = sealNodeChannelCommand(c)
	require.Error(t, err)
	r.AllocationAbsenceDigest = ""
	_, err = NewNodeChannelMigrationSourceGCCommand(r)
	require.Error(t, err)
}
