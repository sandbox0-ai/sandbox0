package runtimeslot

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/stretchr/testify/require"
)

func TestCaptureUploadGrantBindsSourceBudgetAndPreservesLegacyStagingDigest(t *testing.T) {
	source := migrationProtocolRequest()
	r := MigrationStagingRequest{Target: source.Target, Source: source,
		Destination: NodeChannelTarget{SlotID: "destination", ClusterID: source.Target.ClusterID, AllocationID: "destination-alloc",
			NodeID: "destination-node", NodeUID: "destination-uid", NodeBootID: "destination-boot", ControlEndpoint: "unix:///private/destination.sock"},
		DestinationResourceLeaseDigest: strings.Repeat("ab", 32), Bytes: 576 << 20, Inodes: 2048}
	legacy, err := json.Marshal(struct {
		Target                         NodeChannelTarget       `json:"target"`
		Source                         MigrationCaptureRequest `json:"source"`
		Destination                    NodeChannelTarget       `json:"destination"`
		DestinationResourceLeaseDigest string                  `json:"destination_resource_lease_digest"`
		Bytes                          int64                   `json:"bytes"`
		Inodes                         uint64                  `json:"inodes"`
	}{r.Target, r.Source, r.Destination, r.DestinationResourceLeaseDigest, r.Bytes, r.Inodes})
	require.NoError(t, err)
	payload, err := json.Marshal(r)
	require.NoError(t, err)
	require.Equal(t, string(legacy), string(payload), "zero upload grants must preserve existing signed commands")
	d := digest.FromString("profile").String()
	grant, err := NewMigrationCaptureUpload(source, "team", d, d, r.Bytes)
	require.NoError(t, err)
	r.CaptureUpload = grant
	require.NoError(t, r.Validate())
	scope, err := grant.Scope(source)
	require.NoError(t, err)
	require.Equal(t, source.OperationID, scope.OperationID())
	require.EqualValues(t, 3200<<20, grant.MaxBytes)
	require.Greater(t, grant.ReservedBytes(), grant.MaxBytes)
	for _, mutate := range []func(*MigrationStagingRequest){
		func(r *MigrationStagingRequest) { r.CaptureUpload.TeamID = "another-team" },
		func(r *MigrationStagingRequest) {
			r.CaptureUpload.CPUFeaturesDigest = digest.FromString("other").String()
		},
		func(r *MigrationStagingRequest) {
			r.CaptureUpload.CompatibilityDigest = digest.FromString("other").String()
		},
		func(r *MigrationStagingRequest) { r.CaptureUpload.MaxBytes += runtimecheckpoint.ChunkBytes },
		func(r *MigrationStagingRequest) { r.CaptureUpload.ScopeDigest = d },
		func(r *MigrationStagingRequest) { r.Source.BindingDigest = strings.Repeat("78", 32) },
		func(r *MigrationStagingRequest) { r.Source.AssignmentRevision = strings.Repeat("cd", 32) },
		func(r *MigrationStagingRequest) { r.Source.OperationID = "another-op" },
	} {
		changed := r
		mutate(&changed)
		require.Error(t, changed.Validate())
	}
	for _, bytes := range []int64{0, -1, runtimecheckpoint.MaxImageBytes, runtimecheckpoint.MaxImageBytes + 1} {
		_, err := MigrationCaptureUploadBytes(bytes)
		require.Error(t, err)
	}
}
